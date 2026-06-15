use std::path::Path;
#[cfg(any(unix, target_os = "windows"))]
use std::pin::Pin;
#[cfg(target_os = "windows")]
use std::sync::Arc;
#[cfg(any(unix, target_os = "windows"))]
use std::task::{Context, Poll};
#[cfg(target_os = "windows")]
use std::time::Duration;

use thiserror::Error;
#[cfg(any(unix, target_os = "windows"))]
use tokio::io::{AsyncRead, ReadBuf};
#[cfg(unix)]
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
#[cfg(unix)]
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use super::fs::{SocketStatus, local_socket_status};

#[cfg(unix)]
mod unix;
#[cfg(not(any(unix, target_os = "windows")))]
mod unsupported;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(unix)]
use unix as sys;
#[cfg(not(any(unix, target_os = "windows")))]
use unsupported as sys;
#[cfg(target_os = "windows")]
use windows as sys;

#[cfg(target_os = "windows")]
const WINDOWS_SOCKET_RETRY_DELAY: Duration = Duration::from_millis(5);

#[derive(Debug, Error)]
pub enum LocalSocketError {
    #[error("local sockets are not supported on this host")]
    Unsupported,
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct LocalSocket {
    inner: sys::LocalSocket,
}

pub enum LocalSocketIoSource<'a> {
    #[cfg(unix)]
    Fd(std::os::fd::BorrowedFd<'a>),
    #[cfg(target_os = "windows")]
    Socket(std::os::windows::io::BorrowedSocket<'a>),
    #[cfg(not(any(unix, target_os = "windows")))]
    Unsupported(std::marker::PhantomData<&'a ()>),
}

impl LocalSocket {
    /// Creates a connected pair of nonblocking local sockets.
    ///
    /// # Errors
    ///
    /// Returns an error when local sockets are unsupported or the socket pair cannot be created.
    pub fn pair() -> Result<(Self, Self), LocalSocketError> {
        let (left, right) = sys::LocalSocket::pair()?;
        Ok((Self { inner: left }, Self { inner: right }))
    }

    /// Connects to a local socket using nonblocking synchronous IO.
    ///
    /// # Errors
    ///
    /// Returns an error when local sockets are unsupported or the connection fails.
    pub fn connect(path: &Path) -> Result<Self, LocalSocketError> {
        sys::LocalSocket::connect(path).map(|inner| Self { inner })
    }

    /// Reads bytes from the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when reading fails.
    pub fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buffer)
    }

    /// Writes bytes to the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when writing fails.
    pub fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.inner.write(bytes)
    }

    /// Shuts down the writing side of the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown fails.
    pub fn shutdown_write(&mut self) -> std::io::Result<()> {
        self.inner.shutdown_write()
    }

    #[must_use]
    pub fn io_source(&self) -> LocalSocketIoSource<'_> {
        self.inner.io_source()
    }
}

#[derive(Debug)]
pub struct LocalWake {
    reader: Option<LocalWakeReader>,
    writer: LocalSocket,
}

#[derive(Debug)]
pub struct LocalWakeReader {
    socket: LocalSocket,
}

impl LocalWake {
    /// Creates a pollable local wake source.
    ///
    /// # Errors
    ///
    /// Returns an error when the platform cannot create a local socket pair.
    pub fn new() -> Result<Self, LocalSocketError> {
        let (reader, writer) = LocalSocket::pair()?;
        Ok(Self {
            reader: Some(LocalWakeReader { socket: reader }),
            writer,
        })
    }

    #[must_use]
    pub const fn take_reader(&mut self) -> Option<LocalWakeReader> {
        self.reader.take()
    }

    /// Wakes the paired reader.
    ///
    /// # Errors
    ///
    /// Returns an error when writing to the wake socket fails.
    pub fn wake(&mut self) -> std::io::Result<()> {
        match self.writer.write(&[1]) {
            Ok(_written) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl LocalWakeReader {
    #[must_use]
    pub fn io_source(&self) -> LocalSocketIoSource<'_> {
        self.socket.io_source()
    }

    /// Drains pending wake bytes from the reader.
    ///
    /// # Errors
    ///
    /// Returns an error when reading from the wake socket fails.
    pub fn drain(&mut self) -> std::io::Result<()> {
        let mut buffer = [0_u8; 64];
        loop {
            match self.socket.read(&mut buffer) {
                Ok(0) => return Ok(()),
                Ok(_read) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(unix)]
impl std::os::fd::AsFd for LocalSocket {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.inner)
    }
}

#[cfg(target_os = "windows")]
impl std::os::windows::io::AsSocket for LocalSocket {
    fn as_socket(&self) -> std::os::windows::io::BorrowedSocket<'_> {
        std::os::windows::io::AsSocket::as_socket(&self.inner)
    }
}

#[cfg(unix)]
#[derive(Debug)]
pub struct AsyncLocalSocket {
    inner: tokio::net::UnixStream,
}

#[cfg(target_os = "windows")]
#[derive(Debug)]
pub struct AsyncLocalSocket {
    inner: Arc<crate::windows_uds::UnixStream>,
}

#[cfg(unix)]
#[derive(Debug)]
pub struct AsyncLocalSocketReader {
    inner: OwnedReadHalf,
}

#[cfg(target_os = "windows")]
#[derive(Debug)]
pub struct AsyncLocalSocketReader {
    inner: Arc<crate::windows_uds::UnixStream>,
}

#[cfg(unix)]
#[derive(Debug)]
pub struct AsyncLocalSocketWriter {
    inner: OwnedWriteHalf,
}

#[cfg(target_os = "windows")]
#[derive(Debug)]
pub struct AsyncLocalSocketWriter {
    inner: Arc<crate::windows_uds::UnixStream>,
}

#[cfg(unix)]
impl AsyncRead for AsyncLocalSocket {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buffer)
    }
}

#[cfg(unix)]
impl AsyncRead for AsyncLocalSocketReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buffer)
    }
}

#[cfg(target_os = "windows")]
impl AsyncRead for AsyncLocalSocket {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        poll_read_windows(&self.get_mut().inner, cx, buffer)
    }
}

#[cfg(target_os = "windows")]
impl AsyncRead for AsyncLocalSocketReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        poll_read_windows(&self.get_mut().inner, cx, buffer)
    }
}

impl AsyncLocalSocket {
    #[cfg(unix)]
    const fn new(inner: tokio::net::UnixStream) -> Self {
        Self { inner }
    }

    #[cfg(target_os = "windows")]
    fn new(inner: crate::windows_uds::UnixStream) -> Self {
        Self { inner: Arc::new(inner) }
    }

    #[must_use]
    pub fn split(self) -> (AsyncLocalSocketReader, AsyncLocalSocketWriter) {
        #[cfg(unix)]
        {
            let (reader, writer) = self.inner.into_split();
            (
                AsyncLocalSocketReader { inner: reader },
                AsyncLocalSocketWriter { inner: writer },
            )
        }
        #[cfg(target_os = "windows")]
        {
            (
                AsyncLocalSocketReader {
                    inner: Arc::clone(&self.inner),
                },
                AsyncLocalSocketWriter { inner: self.inner },
            )
        }
    }

    /// Reads bytes from the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when reading fails.
    pub async fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(unix)]
        {
            self.inner.read(buffer).await
        }
        #[cfg(target_os = "windows")]
        {
            read_windows(&self.inner, buffer).await
        }
    }

    /// Reads exactly enough bytes to fill `buffer`.
    ///
    /// # Errors
    ///
    /// Returns an error when the socket closes early or reading fails.
    pub async fn read_exact(&mut self, buffer: &mut [u8]) -> std::io::Result<()> {
        read_exact_from_socket(self, buffer).await
    }

    /// Reads until EOF and appends bytes to `output`.
    ///
    /// # Errors
    ///
    /// Returns an error when reading fails.
    pub async fn read_to_end(&mut self, output: &mut Vec<u8>) -> std::io::Result<usize> {
        let mut total = 0;
        let mut buffer = [0_u8; 8192];
        loop {
            let read_len = self.read(&mut buffer).await?;
            if read_len == 0 {
                return Ok(total);
            }
            output.extend_from_slice(&buffer[..read_len]);
            total += read_len;
        }
    }

    /// Writes all bytes to the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when writing fails.
    pub async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.inner.write_all(bytes).await
        }
        #[cfg(target_os = "windows")]
        {
            write_all_windows(&self.inner, bytes).await
        }
    }

    /// Flushes the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when flushing fails.
    #[allow(clippy::unused_async)]
    pub async fn flush(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.inner.flush().await
        }
        #[cfg(target_os = "windows")]
        {
            let _inner = Arc::clone(&self.inner);
            Ok(())
        }
    }

    /// Shuts down the writing side of the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown fails.
    #[allow(clippy::unused_async)]
    pub async fn shutdown_write(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.inner.shutdown().await
        }
        #[cfg(target_os = "windows")]
        {
            self.inner.shutdown_write()
        }
    }
}

impl AsyncLocalSocketReader {
    /// Reads bytes from the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when reading fails.
    pub async fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(unix)]
        {
            self.inner.read(buffer).await
        }
        #[cfg(target_os = "windows")]
        {
            read_windows(&self.inner, buffer).await
        }
    }

    /// Reads exactly enough bytes to fill `buffer`.
    ///
    /// # Errors
    ///
    /// Returns an error when the socket closes early or reading fails.
    pub async fn read_exact(&mut self, buffer: &mut [u8]) -> std::io::Result<()> {
        read_exact_from_reader(self, buffer).await
    }
}

impl AsyncLocalSocketWriter {
    /// Writes all bytes to the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when writing fails.
    pub async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.inner.write_all(bytes).await
        }
        #[cfg(target_os = "windows")]
        {
            write_all_windows(&self.inner, bytes).await
        }
    }

    /// Flushes the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when flushing fails.
    #[allow(clippy::unused_async)]
    pub async fn flush(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.inner.flush().await
        }
        #[cfg(target_os = "windows")]
        {
            let _inner = Arc::clone(&self.inner);
            Ok(())
        }
    }

    /// Shuts down the writing side of the local socket.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown fails.
    #[allow(clippy::unused_async)]
    pub async fn shutdown_write(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.inner.shutdown().await
        }
        #[cfg(target_os = "windows")]
        {
            self.inner.shutdown_write()
        }
    }
}

#[cfg(target_os = "windows")]
fn poll_read_windows(
    inner: &crate::windows_uds::UnixStream,
    cx: &mut Context<'_>,
    buffer: &mut ReadBuf<'_>,
) -> Poll<std::io::Result<()>> {
    let destination = buffer.initialize_unfilled();
    if destination.is_empty() {
        return Poll::Ready(Ok(()));
    }
    match inner.read(destination) {
        Ok(read) => {
            buffer.advance(read);
            Poll::Ready(Ok(()))
        }
        Err(error) if crate::windows_uds::is_would_block(&error) => {
            wake_after_socket_retry_delay(cx);
            Poll::Pending
        }
        Err(error) => Poll::Ready(Err(error)),
    }
}

#[cfg(target_os = "windows")]
fn wake_after_socket_retry_delay(cx: &mut Context<'_>) {
    let waker = cx.waker().clone();
    std::mem::drop(tokio::spawn(async move {
        tokio::time::sleep(WINDOWS_SOCKET_RETRY_DELAY).await;
        waker.wake();
    }));
}

async fn read_exact_from_socket(stream: &mut AsyncLocalSocket, mut buffer: &mut [u8]) -> std::io::Result<()> {
    while !buffer.is_empty() {
        let read_len = stream.read(buffer).await?;
        if read_len == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "local socket closed before enough bytes were read",
            ));
        }
        let (_read, remaining) = buffer.split_at_mut(read_len);
        buffer = remaining;
    }
    Ok(())
}

async fn read_exact_from_reader(reader: &mut AsyncLocalSocketReader, mut buffer: &mut [u8]) -> std::io::Result<()> {
    while !buffer.is_empty() {
        let read_len = reader.read(buffer).await?;
        if read_len == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "local socket closed before enough bytes were read",
            ));
        }
        let (_read, remaining) = buffer.split_at_mut(read_len);
        buffer = remaining;
    }
    Ok(())
}

#[cfg(target_os = "windows")]
async fn read_windows(inner: &crate::windows_uds::UnixStream, buffer: &mut [u8]) -> std::io::Result<usize> {
    loop {
        match inner.read(buffer) {
            Ok(read_len) => return Ok(read_len),
            Err(error) if crate::windows_uds::is_would_block(&error) => {
                tokio::time::sleep(WINDOWS_SOCKET_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(target_os = "windows")]
async fn write_all_windows(inner: &crate::windows_uds::UnixStream, mut bytes: &[u8]) -> std::io::Result<()> {
    while !bytes.is_empty() {
        let written = match inner.write(bytes) {
            Ok(written) => written,
            Err(error) if crate::windows_uds::is_would_block(&error) => {
                tokio::time::sleep(WINDOWS_SOCKET_RETRY_DELAY).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write bytes to local socket",
            ));
        }
        bytes = &bytes[written..];
    }
    Ok(())
}

#[cfg(unix)]
#[derive(Debug)]
pub struct AsyncLocalSocketListener {
    inner: tokio::net::UnixListener,
}

#[cfg(target_os = "windows")]
#[derive(Debug)]
pub struct AsyncLocalSocketListener {
    inner: Arc<crate::windows_uds::UnixListener>,
}

impl AsyncLocalSocketListener {
    #[cfg(unix)]
    const fn new(inner: tokio::net::UnixListener) -> Self {
        Self { inner }
    }

    #[cfg(unix)]
    /// Creates an async local socket listener from an already-bound std Unix
    /// listener.
    ///
    /// # Errors
    ///
    /// Returns an error when the listener cannot be placed in nonblocking mode
    /// or registered with Tokio.
    pub(crate) fn from_std_unix_listener(listener: std::os::unix::net::UnixListener) -> std::io::Result<Self> {
        listener.set_nonblocking(true)?;
        Ok(Self::new(tokio::net::UnixListener::from_std(listener)?))
    }

    #[cfg(target_os = "windows")]
    fn new(inner: crate::windows_uds::UnixListener) -> Self {
        Self { inner: Arc::new(inner) }
    }

    /// Accepts one local socket connection asynchronously.
    ///
    /// # Errors
    ///
    /// Returns an error when accepting from the underlying listener fails.
    pub async fn accept(&self) -> std::io::Result<AsyncLocalSocket> {
        #[cfg(unix)]
        {
            let (stream, _address) = self.inner.accept().await?;
            Ok(AsyncLocalSocket::new(stream))
        }
        #[cfg(target_os = "windows")]
        {
            loop {
                match self.inner.accept().map(AsyncLocalSocket::new) {
                    Ok(stream) => return Ok(stream),
                    Err(error) if crate::windows_uds::is_would_block(&error) => {
                        tokio::time::sleep(WINDOWS_SOCKET_RETRY_DELAY).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
}

/// Copies bytes from one local socket to another until EOF.
///
/// # Errors
///
/// Returns an error if reading or writing fails.
#[cfg(any(unix, target_os = "windows"))]
pub async fn copy_local_socket(reader: &mut AsyncLocalSocket, writer: &mut AsyncLocalSocket) -> std::io::Result<u64> {
    let mut total = 0_u64;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(total);
        }
        writer.write_all(&buffer[..read]).await?;
        total += u64::try_from(read).unwrap_or(u64::MAX);
    }
}

/// Copies bytes in both directions between two local sockets until both sides
/// reach EOF.
///
/// # Errors
///
/// Returns an error if either direction fails while reading, writing, or
/// shutting down the opposite write side.
#[cfg(any(unix, target_os = "windows"))]
pub async fn copy_bidirectional_local_socket(
    left: AsyncLocalSocket,
    right: AsyncLocalSocket,
) -> std::io::Result<(u64, u64)> {
    let (left_reader, left_writer) = left.split();
    let (right_reader, right_writer) = right.split();
    tokio::try_join!(
        copy_local_socket_half(left_reader, right_writer),
        copy_local_socket_half(right_reader, left_writer),
    )
}

#[cfg(any(unix, target_os = "windows"))]
async fn copy_local_socket_half(
    mut reader: AsyncLocalSocketReader,
    mut writer: AsyncLocalSocketWriter,
) -> std::io::Result<u64> {
    let mut total = 0_u64;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            writer.shutdown_write().await?;
            return Ok(total);
        }
        writer.write_all(&buffer[..read]).await?;
        total += u64::try_from(read).unwrap_or(u64::MAX);
    }
}

/// Connects to a local user socket asynchronously.
///
/// # Errors
///
/// Returns an error when local sockets are unsupported or the connection fails.
#[cfg(any(unix, target_os = "windows"))]
pub async fn connect_local_socket(path: &Path) -> Result<AsyncLocalSocket, LocalSocketError> {
    #[cfg(unix)]
    {
        Ok(AsyncLocalSocket::new(tokio::net::UnixStream::connect(path).await?))
    }
    #[cfg(target_os = "windows")]
    {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || crate::windows_uds::UnixStream::connect(&path).map(AsyncLocalSocket::new))
            .await
            .map_err(|source| LocalSocketError::Io(std::io::Error::other(source)))?
            .map_err(LocalSocketError::Io)
    }
}

/// Connects to a local user socket asynchronously.
///
/// # Errors
///
/// Returns an error because local sockets are unsupported on this host.
#[cfg(not(any(unix, target_os = "windows")))]
pub async fn connect_local_socket(_path: &Path) -> Result<AsyncLocalSocket, LocalSocketError> {
    Err(LocalSocketError::Unsupported)
}

/// Binds a local user socket asynchronously.
///
/// # Errors
///
/// Returns an error when local sockets are unsupported or binding fails.
#[cfg(any(unix, target_os = "windows"))]
pub async fn bind_local_socket(path: &Path) -> Result<AsyncLocalSocketListener, LocalSocketError> {
    prepare_socket_path(path).await?;
    #[cfg(unix)]
    {
        bind_prepared_local_socket(path)
    }
    #[cfg(target_os = "windows")]
    {
        bind_prepared_local_socket(path).await
    }
}

#[cfg(unix)]
fn bind_prepared_local_socket(path: &Path) -> Result<AsyncLocalSocketListener, LocalSocketError> {
    Ok(AsyncLocalSocketListener::new(tokio::net::UnixListener::bind(path)?))
}

#[cfg(target_os = "windows")]
async fn bind_prepared_local_socket(path: &Path) -> Result<AsyncLocalSocketListener, LocalSocketError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        crate::windows_uds::UnixListener::bind(&path).map(AsyncLocalSocketListener::new)
    })
    .await
    .map_err(|source| LocalSocketError::Io(std::io::Error::other(source)))?
    .map_err(LocalSocketError::Io)
}

/// Binds a local user socket asynchronously.
///
/// # Errors
///
/// Returns an error because local sockets are unsupported on this host.
#[cfg(not(any(unix, target_os = "windows")))]
pub async fn bind_local_socket(_path: &Path) -> Result<AsyncLocalSocketListener, LocalSocketError> {
    Err(LocalSocketError::Unsupported)
}

#[cfg(any(unix, target_os = "windows"))]
async fn prepare_socket_path(path: &Path) -> Result<(), LocalSocketError> {
    prepare_socket_parent(path).await?;

    match local_socket_status(path).await? {
        SocketStatus::Connected => {}
        SocketStatus::Missing | SocketStatus::Unavailable(_) | SocketStatus::Unsupported => {
            remove_socket_path_if_exists(path).await?;
        }
    }
    Ok(())
}

#[cfg(any(unix, target_os = "windows"))]
async fn prepare_socket_parent(path: &Path) -> Result<(), LocalSocketError> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    Ok(())
}

#[cfg(any(unix, target_os = "windows"))]
async fn remove_socket_path_if_exists(path: &Path) -> Result<(), LocalSocketError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{bind_local_socket, connect_local_socket};

    #[tokio::test(flavor = "current_thread")]
    async fn local_socket_roundtrips_bytes() {
        let path = local_socket_path("roundtrip");
        let listener = bind_local_socket(&path).await.unwrap();
        let client = connect_local_socket(&path);
        let server = listener.accept();
        let (client, server) = tokio::join!(client, server);
        let mut client = client.unwrap();
        let mut server = server.unwrap();
        let mut received = [0_u8; 4];

        client.write_all(b"ping").await.unwrap();
        server.read_exact(&mut received).await.unwrap();

        assert_eq!(&received, b"ping");
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn split_writer_shutdown_sends_eof_to_peer() {
        let path = local_socket_path("writer-shutdown");
        let listener = bind_local_socket(&path).await.unwrap();
        let client = connect_local_socket(&path);
        let server = listener.accept();
        let (client, server) = tokio::join!(client, server);
        let (_client_reader, mut client_writer) = client.unwrap().split();
        let mut server = server.unwrap();
        let mut received = [0_u8; 4];
        let mut eof = [0_u8; 1];

        client_writer.write_all(b"ping").await.unwrap();
        server.read_exact(&mut received).await.unwrap();
        client_writer.shutdown_write().await.unwrap();
        let read_len = tokio::time::timeout(Duration::from_secs(1), server.read(&mut eof))
            .await
            .expect("peer did not observe writer shutdown")
            .unwrap();

        assert_eq!(&received, b"ping");
        assert_eq!(read_len, 0);
        let _ = tokio::fs::remove_file(path).await;
    }

    #[cfg(target_os = "windows")]
    #[tokio::test(flavor = "current_thread")]
    async fn pending_windows_read_can_be_cancelled_without_consuming_later_bytes() {
        let path = local_socket_path("cancel-read");
        let listener = bind_local_socket(&path).await.unwrap();
        let client = connect_local_socket(&path);
        let server = listener.accept();
        let (client, server) = tokio::join!(client, server);
        let mut client = client.unwrap();
        let mut server = server.unwrap();
        let mut byte = [0_u8; 1];

        let timed_out = tokio::time::timeout(Duration::from_millis(25), server.read(&mut byte)).await;
        assert!(timed_out.is_err());

        client.write_all(b"x").await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), server.read_exact(&mut byte))
            .await
            .expect("cancelled read consumed the next byte")
            .unwrap();

        assert_eq!(byte, [b'x']);
        let _ = tokio::fs::remove_file(path).await;
    }

    fn local_socket_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "agentdp-local-socket-{name}-{}-{}.sock",
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
}
