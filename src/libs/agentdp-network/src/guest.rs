use std::collections::VecDeque;

use crate::buffers::{BufferPool, FrameBuf};
use crate::drive::DriveBudget;
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorReady};
use crate::runtime::NetworkRuntime;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum TransportError {
    #[error("{operation} failed: {message}")]
    Operation { operation: &'static str, message: String },
}

impl TransportError {
    #[must_use]
    pub fn operation(operation: &'static str, error: impl std::fmt::Display) -> Self {
        Self::Operation {
            operation,
            message: error.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameRead {
    Frame,
    Blocked,
    Closed,
}

#[derive(Debug)]
pub enum ConnectStatus<S> {
    Connected(S),
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameWrite {
    Flushed,
    Blocked,
}

#[derive(Debug, Clone, Copy)]
pub enum GuestIoSource<'a> {
    #[cfg(unix)]
    Fd(std::os::fd::BorrowedFd<'a>),
    #[cfg(windows)]
    Socket(std::os::windows::io::BorrowedSocket<'a>),
    #[cfg(windows)]
    Handle(std::os::windows::io::BorrowedHandle<'a>),
}

impl<'a> From<agentdp_platform::socket::LocalSocketIoSource<'a>> for GuestIoSource<'a> {
    fn from(source: agentdp_platform::socket::LocalSocketIoSource<'a>) -> Self {
        match source {
            #[cfg(unix)]
            agentdp_platform::socket::LocalSocketIoSource::Fd(fd) => Self::Fd(fd),
            #[cfg(windows)]
            agentdp_platform::socket::LocalSocketIoSource::Socket(socket) => Self::Socket(socket),
            #[cfg(not(any(unix, target_os = "windows")))]
            agentdp_platform::socket::LocalSocketIoSource::Unsupported(_lifetime) => {
                unreachable!("local sockets are not supported on this host")
            }
        }
    }
}

pub trait GuestFrameTransport: 'static {
    type Session: GuestFrameSession;

    /// Attempts to connect one nonblocking guest frame session.
    ///
    /// # Errors
    ///
    /// Returns an error when the transport can determine that connection failed.
    fn try_connect(&mut self) -> Result<ConnectStatus<Self::Session>, TransportError>;

    /// Releases transport-owned resources after the network thread exits.
    ///
    /// # Errors
    ///
    /// Returns an error when cleanup fails.
    fn cleanup(self) -> Result<(), TransportError>;

    fn describe(&self) -> String;
}

pub trait GuestFrameSession: 'static {
    fn io_source(&mut self) -> GuestIoSource<'_>;

    /// Reads one nonblocking Ethernet frame into `frame`.
    ///
    /// # Errors
    ///
    /// Returns an error when the guest frame source fails.
    fn read_frame_into(&mut self, frame: &mut FrameBuf) -> Result<FrameRead, TransportError>;

    /// Writes one nonblocking Ethernet frame to the guest.
    ///
    /// # Errors
    ///
    /// Returns an error when the guest frame sink fails.
    fn write_frame(&mut self, frame: &[u8]) -> Result<FrameWrite, TransportError>;

    /// Shuts down writes to the guest frame session.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown fails.
    fn shutdown_write(&mut self) -> Result<(), TransportError>;
}

#[derive(Debug)]
pub(crate) enum GuestEvent {
    Frame {
        generation: u64,
        frame: FrameBuf,
    },
    Disconnected {
        generation: u64,
        result: Result<(), TransportError>,
    },
}

pub(crate) struct GuestIo<S: GuestFrameSession> {
    session: S,
    generation: u64,
    outbound: VecDeque<FrameBuf>,
    buffers: BufferPool,
    wants_write: bool,
}

impl<S> GuestIo<S>
where
    S: GuestFrameSession,
{
    pub(crate) fn register(
        mut session: S,
        generation: u64,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime,
    ) -> Result<Self, TransportError> {
        register_guest_io_source(&mut session, runtime.reactor_mut())?;
        Ok(Self {
            session,
            generation,
            outbound: VecDeque::new(),
            buffers: buffers.clone(),
            wants_write: false,
        })
    }

    pub(crate) fn send(&mut self, frame: FrameBuf, runtime: &impl NetworkRuntime) -> Result<(), TransportError> {
        self.outbound.push_back(frame);
        if !self.wants_write {
            self.wants_write = true;
            reregister_guest_io_source(&mut self.session, runtime.reactor(), true)?;
        }
        Ok(())
    }

    pub(crate) fn drive_queued(
        &mut self,
        budget: &mut DriveBudget,
        runtime: &impl NetworkRuntime,
    ) -> Result<bool, TransportError> {
        let mut made_progress = false;
        while let Some(frame) = self.outbound.front() {
            if !budget.step() || !budget.event(frame.len()) {
                break;
            }
            match self.session.write_frame(frame.as_slice())? {
                FrameWrite::Flushed => {
                    self.outbound.pop_front();
                    made_progress = true;
                }
                FrameWrite::Blocked => break,
            }
        }
        if self.outbound.is_empty() && self.wants_write {
            self.wants_write = false;
            reregister_guest_io_source(&mut self.session, runtime.reactor(), false)?;
        }
        Ok(made_progress)
    }

    pub(crate) fn drive_ready(
        &mut self,
        readiness: &[ReactorReady],
        events: &mut Vec<GuestEvent>,
        budget: &mut DriveBudget,
        runtime: &impl NetworkRuntime,
    ) -> Result<bool, TransportError> {
        let start_len = events.len();
        let mut made_progress = false;
        for ready in readiness {
            let ReactorReady::Io {
                item,
                readable,
                writable,
            } = *ready
            else {
                continue;
            };
            if item != ReactorItemId::Guest {
                continue;
            }
            if writable {
                made_progress |= self.drive_queued(budget, runtime)?;
            }
            if readable {
                while budget.can_continue() {
                    let Ok(mut frame) = self.buffers.try_frame() else {
                        break;
                    };
                    match self.session.read_frame_into(&mut frame)? {
                        FrameRead::Frame => {
                            let len = frame.len();
                            if !budget.event(len) {
                                break;
                            }
                            events.push(GuestEvent::Frame {
                                generation: self.generation,
                                frame,
                            });
                            made_progress = true;
                        }
                        FrameRead::Blocked => break,
                        FrameRead::Closed => {
                            events.push(GuestEvent::Disconnected {
                                generation: self.generation,
                                result: Ok(()),
                            });
                            made_progress = true;
                            break;
                        }
                    }
                }
            }
        }
        Ok(made_progress || events.len() > start_len)
    }

    pub(crate) fn shutdown(&mut self, runtime: &mut impl NetworkRuntime) {
        let _deregistered = deregister_guest_io_source(&mut self.session, runtime.reactor_mut());
        let _shutdown = self.session.shutdown_write();
    }
}

fn register_guest_io_source<S: GuestFrameSession>(
    session: &mut S,
    reactor: &mut impl ReactorBackend,
) -> Result<(), TransportError> {
    reactor.register_guest_source(session.io_source(), ReactorItemId::Guest)
}

fn reregister_guest_io_source<S: GuestFrameSession>(
    session: &mut S,
    reactor: &impl ReactorBackend,
    writable: bool,
) -> Result<(), TransportError> {
    reactor.reregister_guest_source(session.io_source(), ReactorItemId::Guest, writable)
}

fn deregister_guest_io_source<S: GuestFrameSession>(
    session: &mut S,
    reactor: &mut impl ReactorBackend,
) -> Result<(), TransportError> {
    reactor.deregister_guest_source(session.io_source(), ReactorItemId::Guest)
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};

    use crate::buffers::{BufferPool, FrameBuf};
    use crate::drive::DriveBudget;
    use crate::network::NetworkLimits;
    use crate::reactor::default_backend;
    use crate::test_support::unit::runtime_context;

    use super::{FrameRead, FrameWrite, GuestFrameSession, GuestIo, GuestIoSource, TransportError};

    #[test]
    fn flush_outbound_drains_all_queued_frames_before_reading() {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        let mut runtime = runtime_context(
            default_backend(NetworkLimits::default().reactor_event_capacity)
                .expect("unit-test reactor should initialize"),
        );
        let (reader, writer) = std::os::unix::net::UnixStream::pair().expect("stream pair should initialize");
        reader.set_nonblocking(true).expect("reader should become nonblocking");
        writer.set_nonblocking(true).expect("writer should become nonblocking");
        let mut guest = GuestIo::register(TestSession { reader, writer }, 1, &buffers, &mut runtime)
            .expect("guest session should register");

        guest
            .send(frame(&buffers, b"first"), &runtime)
            .expect("first frame should queue");
        guest
            .send(frame(&buffers, b"second"), &runtime)
            .expect("second frame should queue");
        let mut budget = DriveBudget::event_loop(&crate::network::NetworkLimits::default());
        guest
            .drive_queued(&mut budget, &runtime)
            .expect("queued frames should flush");

        let mut observed = [0_u8; 11];
        guest
            .session
            .reader
            .read_exact(&mut observed)
            .expect("peer should observe flushed frames");
        assert_eq!(&observed, b"firstsecond");
    }

    fn frame(buffers: &BufferPool, bytes: &[u8]) -> FrameBuf {
        let mut frame = buffers.try_frame().expect("prewarmed frame");
        frame.as_mut_vec().extend_from_slice(bytes);
        frame
    }

    struct TestSession {
        reader: std::os::unix::net::UnixStream,
        writer: std::os::unix::net::UnixStream,
    }

    impl GuestFrameSession for TestSession {
        fn io_source(&mut self) -> GuestIoSource<'_> {
            #[cfg(unix)]
            {
                use std::os::fd::AsFd as _;
                GuestIoSource::Fd(self.writer.as_fd())
            }
            #[cfg(windows)]
            {
                use std::os::windows::io::AsSocket as _;
                GuestIoSource::Socket(self.writer.as_socket())
            }
        }

        fn read_frame_into(&mut self, frame: &mut FrameBuf) -> Result<FrameRead, TransportError> {
            let mut buffer = [0_u8; 64];
            match self.writer.read(&mut buffer) {
                Ok(0) => Ok(FrameRead::Closed),
                Ok(len) => {
                    frame.as_mut_vec().extend_from_slice(&buffer[..len]);
                    Ok(FrameRead::Frame)
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(FrameRead::Blocked),
                Err(error) => Err(TransportError::operation("read test session", error)),
            }
        }

        fn write_frame(&mut self, frame: &[u8]) -> Result<FrameWrite, TransportError> {
            match self.writer.write(frame) {
                Ok(len) if len == frame.len() => Ok(FrameWrite::Flushed),
                Ok(_) => Err(TransportError::operation("write test session", "partial write")),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(FrameWrite::Blocked),
                Err(error) => Err(TransportError::operation("write test session", error)),
            }
        }

        fn shutdown_write(&mut self) -> Result<(), TransportError> {
            Ok(())
        }
    }
}
