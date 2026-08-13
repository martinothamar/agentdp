use std::collections::VecDeque;

use crate::buffers::{BufferPool, FrameBuf};
use crate::drive::{DriveApply, DriveGuestFrameRead, DriveGuestFrameReadStatus, DriveGuestFrameWrite, DriveTurn};
use crate::reactor::ReactorItemId;
use crate::reactor::ReactorReady;
use crate::reactor::RegisteredGuestSource;
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

pub(crate) enum GuestFrameEnqueue {
    Queued,
    Blocked(FrameBuf),
}

pub(crate) struct GuestIo<S: GuestFrameSession> {
    session: S,
    generation: u64,
    outbound: VecDeque<FrameBuf>,
    outbound_capacity: usize,
    buffers: BufferPool,
    io: RegisteredGuestSource,
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
        let io = RegisteredGuestSource::register(runtime.reactor_mut(), session.io_source(), ReactorItemId::Guest)?;
        Ok(Self {
            session,
            generation,
            outbound: VecDeque::new(),
            outbound_capacity: buffers.limits().frame_device_queue_capacity,
            buffers: buffers.clone(),
            io,
        })
    }

    pub(crate) fn enqueue(
        &mut self,
        frame: FrameBuf,
        drive: &mut DriveTurn<'_>,
        runtime: &impl NetworkRuntime,
    ) -> Result<GuestFrameEnqueue, TransportError> {
        let reservation = match drive.enqueue_guest_frame(self.outbound.len(), self.outbound_capacity, frame.len()) {
            crate::drive::DriveGuestFrameEnqueue::Reserved(reservation) => reservation,
            crate::drive::DriveGuestFrameEnqueue::Blocked => return Ok(GuestFrameEnqueue::Blocked(frame)),
        };
        reservation.push_queue(&mut self.outbound, frame);
        self.io.enable_write(runtime.reactor(), self.session.io_source())?;
        Ok(GuestFrameEnqueue::Queued)
    }

    pub(crate) fn flush_outbound(
        &mut self,
        drive: &mut DriveTurn<'_>,
        runtime: &impl NetworkRuntime,
    ) -> Result<(), TransportError> {
        while self.io.io().can_write()
            && let Some(frame) = self.outbound.front()
        {
            let frame_len = frame.len();
            match drive.write_guest_frame(frame_len, || {
                self.session
                    .write_frame(frame.as_slice())
                    .map(|status| matches!(status, FrameWrite::Flushed))
            })? {
                DriveGuestFrameWrite::Flushed => {
                    self.outbound.pop_front();
                }
                DriveGuestFrameWrite::WouldBlock => {
                    self.io.clear_write_after_would_block();
                    break;
                }
                DriveGuestFrameWrite::Budget => {
                    break;
                }
            }
        }
        if self.outbound.is_empty() && self.io.io().watches_write() {
            match drive.try_apply_state_change(|| self.io.disable_write(runtime.reactor(), self.session.io_source())) {
                DriveApply::Applied(()) => {}
                DriveApply::Failed(error) => return Err(error),
                DriveApply::Deferred => return Ok(()),
            }
        } else if !self.outbound.is_empty() && !self.io.io().can_write() {
            drive.wait_for_guest_send_capacity();
        }
        Ok(())
    }

    pub(crate) fn drive_ready(
        &mut self,
        readiness: &[ReactorReady],
        events: &mut Vec<GuestEvent>,
        drive: &mut DriveTurn<'_>,
        runtime: &impl NetworkRuntime,
    ) -> Result<(), TransportError> {
        let mut guest_ready = false;
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
            guest_ready = true;
            self.io.mark_reactor_ready(readable, writable);
        }
        if guest_ready {
            self.flush_outbound(drive, runtime)?;
            self.drain_ready_reads(events, drive)
        } else {
            Ok(())
        }
    }

    pub(crate) fn drain_ready_reads(
        &mut self,
        events: &mut Vec<GuestEvent>,
        drive: &mut DriveTurn<'_>,
    ) -> Result<(), TransportError> {
        while self.io.io().can_read() {
            match drive.read_guest_frame(&self.buffers, |frame| {
                self.session.read_frame_into(frame).map(|status| match status {
                    FrameRead::Frame => DriveGuestFrameReadStatus::Frame,
                    FrameRead::Blocked => DriveGuestFrameReadStatus::Blocked,
                    FrameRead::Closed => DriveGuestFrameReadStatus::Closed,
                })
            })? {
                DriveGuestFrameRead::Frame(frame) => {
                    events.push(GuestEvent::Frame {
                        generation: self.generation,
                        frame,
                    });
                }
                DriveGuestFrameRead::WouldBlock => {
                    self.io.clear_read_after_would_block();
                    return Ok(());
                }
                DriveGuestFrameRead::Blocked => return Ok(()),
                DriveGuestFrameRead::Closed => {
                    self.io.clear_read_after_would_block();
                    events.push(GuestEvent::Disconnected {
                        generation: self.generation,
                        result: Ok(()),
                    });
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn shutdown(&mut self, runtime: &mut impl NetworkRuntime) {
        self.io.deregister(runtime.reactor_mut(), self.session.io_source());
        let _shutdown = self.session.shutdown_write();
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};

    use crate::buffers::{BufferPool, FrameBuf};
    use crate::drive::{DriveBudget, DriveReport, DriveTurn};
    use crate::network::NetworkLimits;
    use crate::reactor::{ReactorItemId, ReactorReady, default_backend};
    use crate::test_support::unit::runtime_context;

    use super::{FrameRead, FrameWrite, GuestFrameEnqueue, GuestFrameSession, GuestIo, GuestIoSource, TransportError};

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
        let mut guest = GuestIo::register(TestSession::new(reader, writer), 1, &buffers, &mut runtime)
            .expect("guest session should register");
        let mut budget = DriveBudget::event_loop(&crate::network::NetworkLimits::default());
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        assert!(matches!(
            guest.enqueue(output_frame(&buffers, b"first"), &mut drive, &runtime),
            Ok(GuestFrameEnqueue::Queued)
        ));
        assert!(matches!(
            guest.enqueue(output_frame(&buffers, b"second"), &mut drive, &runtime),
            Ok(GuestFrameEnqueue::Queued)
        ));
        guest
            .flush_outbound(&mut drive, &runtime)
            .expect("queued frames should flush");

        let mut observed = [0_u8; 11];
        guest
            .session
            .reader
            .read_exact(&mut observed)
            .expect("peer should observe flushed frames");
        assert_eq!(&observed, b"firstsecond");
    }

    #[test]
    fn enqueue_blocks_when_guest_outbound_queue_is_full() {
        let limits = NetworkLimits {
            frame_device_queue_capacity: 1,
            ..NetworkLimits::default()
        };
        let buffers = BufferPool::new(limits.clone());
        buffers.prewarm_instance_network();
        let mut runtime = runtime_context(
            default_backend(limits.reactor_event_capacity).expect("unit-test reactor should initialize"),
        );
        let (reader, writer) = std::os::unix::net::UnixStream::pair().expect("stream pair should initialize");
        reader.set_nonblocking(true).expect("reader should become nonblocking");
        writer.set_nonblocking(true).expect("writer should become nonblocking");
        let mut guest = GuestIo::register(TestSession::new(reader, writer), 1, &buffers, &mut runtime)
            .expect("guest session should register");
        let mut budget = DriveBudget::event_loop(&limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        assert!(matches!(
            guest.enqueue(output_frame(&buffers, b"first"), &mut drive, &runtime),
            Ok(GuestFrameEnqueue::Queued)
        ));
        assert!(matches!(
            guest.enqueue(output_frame(&buffers, b"second"), &mut drive, &runtime),
            Ok(GuestFrameEnqueue::Blocked(_))
        ));
        assert!(report.wait().contains(crate::drive::DriveWait::GUEST_SEND_CAPACITY));
    }

    #[test]
    fn guest_read_would_block_clears_read_readiness() {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        let mut runtime = runtime_context(
            default_backend(NetworkLimits::default().reactor_event_capacity)
                .expect("unit-test reactor should initialize"),
        );
        let (reader, writer) = std::os::unix::net::UnixStream::pair().expect("stream pair should initialize");
        reader.set_nonblocking(true).expect("reader should become nonblocking");
        writer.set_nonblocking(true).expect("writer should become nonblocking");
        let mut guest = GuestIo::register(TestSession::new(reader, writer), 1, &buffers, &mut runtime)
            .expect("guest session should register");
        let mut events = Vec::new();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        guest
            .drive_ready(
                &[ReactorReady::Io {
                    item: ReactorItemId::Guest,
                    readable: true,
                    writable: false,
                }],
                &mut events,
                &mut drive,
                &runtime,
            )
            .expect("guest would-block read should not error");

        assert!(events.is_empty());
        assert!(report.wait().contains(crate::drive::DriveWait::GUEST_RECV));
        assert!(!guest.io.io().can_read());
    }

    #[test]
    fn saturated_guest_output_preserves_guest_input_progress() {
        let limits = NetworkLimits {
            frame_buffer_pool_capacity: 4,
            frame_device_queue_capacity: 4,
            ..NetworkLimits::default()
        };
        let buffers = BufferPool::new(limits.clone());
        buffers.prewarm_instance_network();
        let mut runtime = runtime_context(
            default_backend(limits.reactor_event_capacity).expect("unit-test reactor should initialize"),
        );
        let (mut peer, writer) = std::os::unix::net::UnixStream::pair().expect("stream pair should initialize");
        peer.set_nonblocking(true).expect("peer should become nonblocking");
        writer.set_nonblocking(true).expect("writer should become nonblocking");
        let mut guest = GuestIo::register(
            TestSession::with_blocked_writes(peer.try_clone().unwrap(), writer),
            1,
            &buffers,
            &mut runtime,
        )
        .expect("guest session should register");
        let mut budget = DriveBudget::event_loop(&limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        for payload in [b"one".as_slice(), b"two"] {
            assert!(matches!(
                guest.enqueue(output_frame(&buffers, payload), &mut drive, &runtime),
                Ok(GuestFrameEnqueue::Queued)
            ));
        }
        assert!(
            buffers
                .try_output_frame_with_capacity(limits.frame_buffer_capacity)
                .is_err(),
            "guest-bound output must preserve progress buffers"
        );
        peer.write_all(b"inbound").expect("peer should send a guest frame");
        let mut events = Vec::new();

        guest
            .drive_ready(
                &[ReactorReady::Io {
                    item: ReactorItemId::Guest,
                    readable: true,
                    writable: true,
                }],
                &mut events,
                &mut drive,
                &runtime,
            )
            .expect("guest input should remain readable while output is blocked");

        let [super::GuestEvent::Frame { frame, .. }] = events.as_slice() else {
            panic!("guest input must retain progress capacity");
        };
        assert_eq!(frame.as_slice(), b"inbound");
        let response = buffers
            .try_gateway_response_frame_with_capacity(limits.frame_buffer_capacity)
            .expect("the paired gateway response must retain progress capacity");
        drop(response);
    }

    fn output_frame(buffers: &BufferPool, bytes: &[u8]) -> FrameBuf {
        let mut frame = buffers
            .try_output_frame_with_capacity(bytes.len())
            .expect("prewarmed output frame");
        frame.as_mut_vec().extend_from_slice(bytes);
        frame
    }

    struct TestSession {
        reader: std::os::unix::net::UnixStream,
        writer: std::os::unix::net::UnixStream,
        block_writes: bool,
    }

    impl TestSession {
        const fn new(reader: std::os::unix::net::UnixStream, writer: std::os::unix::net::UnixStream) -> Self {
            Self {
                reader,
                writer,
                block_writes: false,
            }
        }

        const fn with_blocked_writes(
            reader: std::os::unix::net::UnixStream,
            writer: std::os::unix::net::UnixStream,
        ) -> Self {
            Self {
                reader,
                writer,
                block_writes: true,
            }
        }
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
            if self.block_writes {
                return Ok(FrameWrite::Blocked);
            }
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
