use std::path::Path;

use super::{LocalSocketError, LocalSocketIoSource};

#[derive(Debug)]
pub(super) struct LocalSocket;

impl LocalSocket {
    pub(super) fn pair() -> Result<(Self, Self), LocalSocketError> {
        Err(LocalSocketError::Unsupported)
    }

    pub(super) fn connect(_path: &Path) -> Result<Self, LocalSocketError> {
        Err(LocalSocketError::Unsupported)
    }

    pub(super) fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "local sockets are not supported on this host",
        ))
    }

    pub(super) fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "local sockets are not supported on this host",
        ))
    }

    pub(super) fn shutdown_write(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "local sockets are not supported on this host",
        ))
    }

    pub(super) fn io_source(&self) -> LocalSocketIoSource<'_> {
        LocalSocketIoSource::Unsupported(std::marker::PhantomData)
    }
}
