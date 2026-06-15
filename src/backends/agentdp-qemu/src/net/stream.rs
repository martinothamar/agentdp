use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use agentdp_platform::socket::{self, LocalSocket};

const FRAME_LENGTH_BYTES: usize = 4;
const MAX_ETHERNET_FRAME_BYTES: u32 = 65_535;
const SOCKET_CLOSE_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const SOCKET_CLOSE_POLL_DELAY: Duration = Duration::from_millis(10);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read QEMU stream frame: {0}")]
    Read(#[source] std::io::Error),
    #[error("failed to write QEMU stream frame: {0}")]
    Write(#[source] std::io::Error),
    #[error("failed to shut down QEMU stream writer: {0}")]
    Shutdown(#[source] std::io::Error),
    #[error("QEMU stream frame length {length} exceeds maximum {maximum}; length prefix bytes: {length_prefix}")]
    OversizedFrame {
        length: u32,
        maximum: u32,
        length_prefix: String,
    },
    #[error("failed to remove stale QEMU stream socket {path}: {source}")]
    RemoveStaleSocket {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("refusing to remove active QEMU stream socket {path}")]
    ActiveSocket { path: PathBuf },
    #[error("failed to connect QEMU stream socket {path}: {source}")]
    Connect {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn checked_incoming_frame_len(length_prefix: [u8; FRAME_LENGTH_BYTES]) -> Result<usize, Error> {
    let length = u32::from_be_bytes(length_prefix);
    if length > MAX_ETHERNET_FRAME_BYTES {
        return Err(Error::OversizedFrame {
            length,
            maximum: MAX_ETHERNET_FRAME_BYTES,
            length_prefix: hex_prefix(&length_prefix, FRAME_LENGTH_BYTES),
        });
    }
    Ok(length as usize)
}

fn checked_outgoing_frame_len(frame: &[u8]) -> Result<u32, Error> {
    let length = u32::try_from(frame.len()).map_err(|_| Error::OversizedFrame {
        length: u32::MAX,
        maximum: MAX_ETHERNET_FRAME_BYTES,
        length_prefix: "<outgoing frame too large>".to_owned(),
    })?;
    if length > MAX_ETHERNET_FRAME_BYTES {
        return Err(Error::OversizedFrame {
            length,
            maximum: MAX_ETHERNET_FRAME_BYTES,
            length_prefix: hex_prefix(&length.to_be_bytes(), FRAME_LENGTH_BYTES),
        });
    }
    Ok(length)
}

/// Removes a stale QEMU stream socket.
///
/// # Errors
///
/// Returns an error when the socket exists but cannot be removed.
pub async fn cleanup_socket(path: impl AsRef<Path>) -> Result<(), Error> {
    remove_stale_socket(path.as_ref()).await
}

/// Removes a QEMU stream socket after the owning listener has been closed.
///
/// Windows can keep a just-closed `AF_UNIX` listener connectable briefly. This
/// waits a bounded amount of time for that owned listener to become inactive
/// while still refusing to remove a genuinely active socket.
///
/// # Errors
///
/// Returns an error when the socket remains active or cannot be removed.
pub async fn cleanup_socket_after_close(path: impl AsRef<Path>) -> Result<(), Error> {
    cleanup_socket_after_close_with_timeout(path.as_ref(), SOCKET_CLOSE_WAIT_TIMEOUT).await
}

async fn cleanup_socket_after_close_with_timeout(path: &Path, timeout: Duration) -> Result<(), Error> {
    let deadline = Instant::now() + timeout;
    loop {
        match remove_stale_socket(path).await {
            Ok(()) => return Ok(()),
            Err(Error::ActiveSocket { .. }) if Instant::now() < deadline => {
                tokio::time::sleep(SOCKET_CLOSE_POLL_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn remove_stale_socket(path: &Path) -> Result<(), Error> {
    if !tokio::fs::try_exists(path)
        .await
        .map_err(|source| Error::RemoveStaleSocket {
            path: path.to_path_buf(),
            source,
        })?
    {
        return Ok(());
    }
    if socket::connect_local_socket(path).await.is_ok() {
        return Err(Error::ActiveSocket {
            path: path.to_path_buf(),
        });
    }
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::RemoveStaleSocket {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn local_socket_io_error(error: socket::LocalSocketError) -> std::io::Error {
    match error {
        socket::LocalSocketError::Io(error) => error,
        socket::LocalSocketError::Unsupported => std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "local sockets are not supported on this host",
        ),
    }
}

#[must_use]
pub fn hex_prefix(bytes: &[u8], max_len: usize) -> String {
    use std::fmt::Write as _;

    let prefix_len = bytes.len().min(max_len);
    let mut output = String::with_capacity(prefix_len.saturating_mul(3));
    for (index, byte) in bytes.iter().take(prefix_len).enumerate() {
        if index > 0 {
            output.push(' ');
        }
        let _ = write!(output, "{byte:02x}");
    }
    if bytes.len() > max_len {
        output.push_str(" ...");
    }
    output
}

#[derive(Debug)]
pub struct FrameStream {
    stream: LocalSocket,
    read_prefix: [u8; FRAME_LENGTH_BYTES],
    read_prefix_len: usize,
    read_payload: Vec<u8>,
    read_payload_len: usize,
    pending_write: Vec<u8>,
    pending_write_offset: usize,
}

impl FrameStream {
    /// Connects to a QEMU `-netdev stream` socket using nonblocking local socket IO.
    ///
    /// # Errors
    ///
    /// Returns an error when the socket cannot be opened.
    pub fn connect(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let stream = LocalSocket::connect(path).map_err(|error| Error::Connect {
            path: path.to_path_buf(),
            source: local_socket_io_error(error),
        })?;
        Ok(Self {
            stream,
            read_prefix: [0; FRAME_LENGTH_BYTES],
            read_prefix_len: 0,
            read_payload: Vec::with_capacity(MAX_ETHERNET_FRAME_BYTES as usize),
            read_payload_len: 0,
            pending_write: Vec::with_capacity(FRAME_LENGTH_BYTES + MAX_ETHERNET_FRAME_BYTES as usize),
            pending_write_offset: 0,
        })
    }

    #[must_use]
    pub fn io_source(&self) -> agentdp_platform::socket::LocalSocketIoSource<'_> {
        self.stream.io_source()
    }

    /// Reads one complete nonblocking QEMU frame into `frame`.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream fails or QEMU sends an oversized frame.
    pub fn try_read_frame_into(&mut self, frame: &mut Vec<u8>) -> Result<GuestFrameRead, Error> {
        while self.read_prefix_len < FRAME_LENGTH_BYTES {
            match self.stream.read(&mut self.read_prefix[self.read_prefix_len..]) {
                Ok(0) => return Ok(GuestFrameRead::Closed),
                Ok(read) => self.read_prefix_len += read,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(GuestFrameRead::Blocked),
                Err(error) => return Err(Error::Read(error)),
            }
        }

        if self.read_payload.is_empty() {
            let frame_len = checked_incoming_frame_len(self.read_prefix)?;
            self.read_payload.resize(frame_len, 0);
            self.read_payload_len = 0;
        }

        while self.read_payload_len < self.read_payload.len() {
            match self.stream.read(&mut self.read_payload[self.read_payload_len..]) {
                Ok(0) => return Ok(GuestFrameRead::Closed),
                Ok(read) => self.read_payload_len += read,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(GuestFrameRead::Blocked),
                Err(error) => return Err(Error::Read(error)),
            }
        }

        frame.clear();
        frame.extend_from_slice(&self.read_payload);
        self.read_prefix = [0; FRAME_LENGTH_BYTES];
        self.read_prefix_len = 0;
        self.read_payload.clear();
        self.read_payload_len = 0;
        Ok(GuestFrameRead::Frame)
    }

    /// Writes one complete nonblocking QEMU frame.
    ///
    /// # Errors
    ///
    /// Returns an error when the frame is too large or the stream cannot be written.
    pub fn try_write_frame(&mut self, frame: &[u8]) -> Result<GuestFrameWrite, Error> {
        if self.pending_write.is_empty() {
            let length = checked_outgoing_frame_len(frame)?;
            match self.stream.write(&length.to_be_bytes()) {
                Ok(FRAME_LENGTH_BYTES) => {}
                Ok(written) => {
                    self.pending_write.extend_from_slice(&length.to_be_bytes()[written..]);
                    self.pending_write.extend_from_slice(frame);
                    return self.flush_pending_write();
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    self.pending_write.extend_from_slice(&length.to_be_bytes());
                    self.pending_write.extend_from_slice(frame);
                    return Ok(GuestFrameWrite::Blocked);
                }
                Err(error) => return Err(Error::Write(error)),
            }
            match self.stream.write(frame) {
                Ok(written) if written == frame.len() => return Ok(GuestFrameWrite::Flushed),
                Ok(written) => self.pending_write.extend_from_slice(&frame[written..]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    self.pending_write.extend_from_slice(frame);
                    return Ok(GuestFrameWrite::Blocked);
                }
                Err(error) => return Err(Error::Write(error)),
            }
        }
        self.flush_pending_write()
    }

    /// Shuts down the writing side of the QEMU stream.
    ///
    /// # Errors
    ///
    /// Returns an error when socket shutdown fails.
    pub fn shutdown_write(&mut self) -> Result<(), Error> {
        self.stream.shutdown_write().map_err(Error::Shutdown)
    }

    fn flush_pending_write(&mut self) -> Result<GuestFrameWrite, Error> {
        while self.pending_write_offset < self.pending_write.len() {
            match self.stream.write(&self.pending_write[self.pending_write_offset..]) {
                Ok(0) => {
                    return Err(Error::Write(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "write zero",
                    )));
                }
                Ok(written) => self.pending_write_offset += written,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(GuestFrameWrite::Blocked),
                Err(error) => return Err(Error::Write(error)),
            }
        }
        self.pending_write.clear();
        self.pending_write_offset = 0;
        Ok(GuestFrameWrite::Flushed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuestFrameRead {
    Frame,
    Blocked,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuestFrameWrite {
    Flushed,
    Blocked,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agentdp_platform::socket;

    use super::{Error, FrameStream, GuestFrameRead, GuestFrameWrite};

    #[tokio::test(flavor = "current_thread")]
    async fn frame_stream_writes_local_socket_frames() {
        let socket = local_socket_path("roundtrip");
        let listener = bind_test_listener(&socket).await;

        let mut client = FrameStream::connect(&socket).unwrap();
        let accepted = listener.accept();
        let mut accepted = accepted.await.unwrap();

        assert_eq!(client.try_write_frame(b"from-qemu").unwrap(), GuestFrameWrite::Flushed);

        assert_eq!(
            read_async_frame(&mut accepted).await.unwrap(),
            Some(b"from-qemu".to_vec())
        );
        let _result = tokio::fs::remove_file(socket).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn frame_stream_reads_local_socket_frames() {
        let socket = local_socket_path("read");
        let listener = bind_test_listener(&socket).await;

        let mut client = FrameStream::connect(&socket).unwrap();
        let mut accepted = listener.accept().await.unwrap();
        write_async_frame(&mut accepted, b"from-qemu").await.unwrap();

        let mut frame = Vec::with_capacity(64);
        let read = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match client.try_read_frame_into(&mut frame).unwrap() {
                    GuestFrameRead::Frame => return GuestFrameRead::Frame,
                    GuestFrameRead::Blocked => tokio::task::yield_now().await,
                    GuestFrameRead::Closed => return GuestFrameRead::Closed,
                }
            }
        })
        .await
        .expect("frame stream did not read async server frame");

        assert_eq!(read, GuestFrameRead::Frame);
        assert_eq!(frame, b"from-qemu");
        let _result = tokio::fs::remove_file(socket).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_removes_stale_socket_path() {
        let socket = local_socket_path("stale");
        tokio::fs::write(&socket, b"stale").await.unwrap();

        super::cleanup_socket(&socket).await.unwrap();

        assert!(!tokio::fs::try_exists(socket).await.unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_refuses_to_remove_active_socket_path() {
        let socket = local_socket_path("active");
        let _listener = bind_test_listener(&socket).await;

        let error = super::cleanup_socket(&socket).await.unwrap_err();

        assert!(matches!(error, Error::ActiveSocket { .. }));
        let _result = tokio::fs::remove_file(socket).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_after_close_waits_for_listener_to_stop_accepting() {
        let socket = local_socket_path("closing");
        super::cleanup_socket(&socket).await.unwrap();
        let listener = bind_test_listener(&socket).await;
        let cleanup = super::cleanup_socket_after_close_with_timeout(&socket, Duration::from_secs(1));
        let release = async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            drop(listener);
        };

        let (result, ()) = tokio::join!(cleanup, release);

        result.unwrap();
        assert!(!tokio::fs::try_exists(socket).await.unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_oversized_frames() {
        let socket = local_socket_path("oversized");
        let listener = bind_test_listener(&socket).await;
        let mut reader = FrameStream::connect(&socket).unwrap();
        let mut writer = listener.accept().await.unwrap();

        writer.write_all(&65_536_u32.to_be_bytes()).await.unwrap();
        writer.flush().await.unwrap();
        let mut frame = Vec::new();
        let error = reader.try_read_frame_into(&mut frame).unwrap_err();

        assert!(matches!(
            error,
            Error::OversizedFrame {
                length: 65_536,
                maximum: 65_535,
                ..
            }
        ));
        assert!(error.to_string().contains("00 01 00 00"));
        let _result = tokio::fs::remove_file(socket).await;
    }

    #[test]
    fn formats_hex_prefix() {
        assert_eq!(super::hex_prefix(&[0x52, 0x54, 0x00, 0x73, 0x01], 4), "52 54 00 73 ...");
    }

    fn local_socket_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "agentdp-qemu-stream-{name}-{}-{}.sock",
            std::process::id(),
            unique_suffix()
        ))
    }

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    async fn bind_test_listener(socket: &std::path::Path) -> socket::AsyncLocalSocketListener {
        super::cleanup_socket(socket).await.unwrap();
        socket::bind_local_socket(socket).await.unwrap()
    }

    async fn write_async_frame(
        stream: &mut socket::AsyncLocalSocket,
        frame: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let length = u32::try_from(frame.len())?;
        stream.write_all(&length.to_be_bytes()).await?;
        stream.write_all(frame).await?;
        stream.flush().await?;
        Ok(())
    }

    async fn read_async_frame(
        stream: &mut socket::AsyncLocalSocket,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut length = [0_u8; super::FRAME_LENGTH_BYTES];
        match stream.read_exact(&mut length).await {
            Ok(()) => {}
            Err(source) if source.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(source) => return Err(source.into()),
        }
        let mut frame = vec![0; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut frame).await?;
        Ok(Some(frame))
    }
}
