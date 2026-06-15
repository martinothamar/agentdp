use std::path::{Path, PathBuf};

use agentdp_network::{
    ConnectStatus, FrameBuf, FrameRead, FrameWrite, GuestFrameSession, GuestFrameTransport, GuestIoSource,
    TransportError,
};
use agentdp_platform::socket::LocalSocketIoSource;

pub mod stream;

use stream::{FrameStream, GuestFrameRead, GuestFrameWrite};

#[derive(Debug)]
pub struct QemuStreamTransport {
    socket: PathBuf,
}

impl QemuStreamTransport {
    /// Prepares the QEMU-owned stream socket path before a VM is launched.
    ///
    /// # Errors
    ///
    /// Returns an error when a stale socket cannot be removed or an active
    /// socket is already bound at the target path.
    pub async fn prepare_server_socket(socket: impl AsRef<Path>) -> Result<(), stream::Error> {
        stream::cleanup_socket(socket).await
    }

    #[must_use]
    pub fn connect(socket: impl Into<PathBuf>) -> Self {
        Self { socket: socket.into() }
    }
}

#[must_use]
pub fn stream_socket_path(runtime_dir: &Path) -> PathBuf {
    std::env::temp_dir()
        .join("agentdp-net")
        .join(stable_path_id(runtime_dir))
        .join("stream.sock")
}

fn stable_path_id(path: &Path) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in path.as_os_str().as_encoded_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

impl GuestFrameTransport for QemuStreamTransport {
    type Session = FrameStream;

    fn try_connect(&mut self) -> Result<ConnectStatus<Self::Session>, TransportError> {
        match FrameStream::connect(&self.socket) {
            Ok(session) => Ok(ConnectStatus::Connected(session)),
            Err(error) if connect_is_pending(&error) => Ok(ConnectStatus::Pending),
            Err(error) => Err(TransportError::operation("connect QEMU stream session", error)),
        }
    }

    fn cleanup(self) -> Result<(), TransportError> {
        Ok(())
    }

    fn describe(&self) -> String {
        format!("QEMU stream socket {}", self.socket.display())
    }
}

impl GuestFrameSession for FrameStream {
    fn io_source(&mut self) -> GuestIoSource<'_> {
        match Self::io_source(self) {
            #[cfg(unix)]
            LocalSocketIoSource::Fd(fd) => GuestIoSource::Fd(fd),
            #[cfg(windows)]
            LocalSocketIoSource::Socket(socket) => GuestIoSource::Socket(socket),
        }
    }

    fn read_frame_into(&mut self, frame: &mut FrameBuf) -> Result<FrameRead, TransportError> {
        self.try_read_frame_into(frame.as_mut_vec())
            .map(|read| match read {
                GuestFrameRead::Frame => FrameRead::Frame,
                GuestFrameRead::Blocked => FrameRead::Blocked,
                GuestFrameRead::Closed => FrameRead::Closed,
            })
            .map_err(|error| TransportError::operation("read QEMU stream frame", error))
    }

    fn write_frame(&mut self, frame: &[u8]) -> Result<FrameWrite, TransportError> {
        self.try_write_frame(frame)
            .map(|write| match write {
                GuestFrameWrite::Flushed => FrameWrite::Flushed,
                GuestFrameWrite::Blocked => FrameWrite::Blocked,
            })
            .map_err(|error| TransportError::operation("write QEMU stream frame", error))
    }

    fn shutdown_write(&mut self) -> Result<(), TransportError> {
        self.shutdown_write()
            .map_err(|error| TransportError::operation("shut down QEMU stream writer", error))
    }
}

fn connect_is_pending(error: &stream::Error) -> bool {
    match error {
        stream::Error::Connect { source, .. } => matches!(
            source.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::WouldBlock
        ),
        _ => false,
    }
}
