#[cfg(any(unix, target_os = "windows"))]
use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use thiserror::Error;

#[cfg(any(unix, target_os = "windows"))]
use super::{SocketStatus, local_socket_status};

#[derive(Debug, Error)]
pub enum LocalSocketError {
    #[error("local sockets are not supported on this host")]
    Unsupported,
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

pub trait ReadWrite: Read + Write {}

impl<T> ReadWrite for T where T: Read + Write {}

pub struct LocalSocket {
    inner: Box<dyn ReadWrite + Send>,
}

impl LocalSocket {
    #[cfg(any(unix, target_os = "windows"))]
    fn new(inner: impl ReadWrite + Send + 'static) -> Self {
        Self { inner: Box::new(inner) }
    }
}

impl Read for LocalSocket {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buffer)
    }
}

impl Write for LocalSocket {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub struct LocalSocketListener {
    inner: Box<dyn Listener + Send>,
}

impl LocalSocketListener {
    #[cfg(any(unix, target_os = "windows"))]
    fn new(inner: impl Listener + Send + 'static) -> Self {
        Self { inner: Box::new(inner) }
    }

    /// Accepts one local socket connection.
    ///
    /// # Errors
    ///
    /// Returns an error when accepting from the underlying listener fails.
    pub fn accept(&self) -> std::io::Result<LocalSocket> {
        self.inner.accept()
    }

    /// Sets whether accepting from this listener should block.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying listener cannot change blocking
    /// mode.
    pub fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()> {
        self.inner.set_nonblocking(nonblocking)
    }
}

trait Listener {
    fn accept(&self) -> std::io::Result<LocalSocket>;
    fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()>;
}

/// Connects to a local user socket.
///
/// # Errors
///
/// Returns an error when local sockets are unsupported or the connection fails.
#[cfg(any(unix, target_os = "windows"))]
pub fn connect_local_socket(path: &Path) -> Result<LocalSocket, LocalSocketError> {
    connect_local_socket_impl(path)
}

/// Connects to a local user socket.
///
/// # Errors
///
/// Returns an error because local sockets are unsupported on this host.
#[cfg(not(any(unix, target_os = "windows")))]
pub const fn connect_local_socket(path: &Path) -> Result<LocalSocket, LocalSocketError> {
    connect_local_socket_impl(path)
}

/// Binds a local user socket.
///
/// # Errors
///
/// Returns an error when local sockets are unsupported or binding fails.
#[cfg(any(unix, target_os = "windows"))]
pub fn bind_local_socket(path: &Path) -> Result<LocalSocketListener, LocalSocketError> {
    bind_local_socket_impl(path)
}

/// Binds a local user socket.
///
/// # Errors
///
/// Returns an error because local sockets are unsupported on this host.
#[cfg(not(any(unix, target_os = "windows")))]
pub const fn bind_local_socket(path: &Path) -> Result<LocalSocketListener, LocalSocketError> {
    bind_local_socket_impl(path)
}

#[cfg(unix)]
fn connect_local_socket_impl(path: &Path) -> Result<LocalSocket, LocalSocketError> {
    Ok(LocalSocket::new(std::os::unix::net::UnixStream::connect(path)?))
}

#[cfg(target_os = "windows")]
fn connect_local_socket_impl(path: &Path) -> Result<LocalSocket, LocalSocketError> {
    Ok(LocalSocket::new(agentdp_windows_uds::UnixStream::connect(path)?))
}

#[cfg(not(any(unix, target_os = "windows")))]
const fn connect_local_socket_impl(_path: &Path) -> Result<LocalSocket, LocalSocketError> {
    Err(LocalSocketError::Unsupported)
}

#[cfg(any(unix, target_os = "windows"))]
fn bind_local_socket_impl(path: &Path) -> Result<LocalSocketListener, LocalSocketError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    match local_socket_status(path) {
        SocketStatus::Connected => {}
        SocketStatus::Missing | SocketStatus::Unavailable(_) | SocketStatus::Unsupported => {
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
    }

    Ok(LocalSocketListener::new(UnixListener {
        #[cfg(unix)]
        inner: std::os::unix::net::UnixListener::bind(path)?,
        #[cfg(target_os = "windows")]
        inner: agentdp_windows_uds::UnixListener::bind(path)?,
    }))
}

#[cfg(not(any(unix, target_os = "windows")))]
const fn bind_local_socket_impl(_path: &Path) -> Result<LocalSocketListener, LocalSocketError> {
    Err(LocalSocketError::Unsupported)
}

#[cfg(unix)]
struct UnixListener {
    inner: std::os::unix::net::UnixListener,
}

#[cfg(target_os = "windows")]
struct UnixListener {
    inner: agentdp_windows_uds::UnixListener,
}

#[cfg(any(unix, target_os = "windows"))]
impl Listener for UnixListener {
    fn accept(&self) -> std::io::Result<LocalSocket> {
        #[cfg(unix)]
        {
            let (stream, _address) = self.inner.accept()?;
            Ok(LocalSocket::new(stream))
        }
        #[cfg(target_os = "windows")]
        {
            Ok(LocalSocket::new(self.inner.accept()?))
        }
    }

    fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()> {
        self.inner.set_nonblocking(nonblocking)
    }
}
