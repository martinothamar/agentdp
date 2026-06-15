use std::io::{Read as _, Write as _};
use std::path::Path;

use super::{LocalSocketError, LocalSocketIoSource};

#[derive(Debug)]
pub(super) struct LocalSocket {
    inner: std::os::unix::net::UnixStream,
}

impl LocalSocket {
    pub(super) fn pair() -> Result<(Self, Self), LocalSocketError> {
        let (left, right) = std::os::unix::net::UnixStream::pair()?;
        left.set_nonblocking(true)?;
        right.set_nonblocking(true)?;
        Ok((Self { inner: left }, Self { inner: right }))
    }

    pub(super) fn connect(path: &Path) -> Result<Self, LocalSocketError> {
        let stream = std::os::unix::net::UnixStream::connect(path)?;
        stream.set_nonblocking(true)?;
        Ok(Self { inner: stream })
    }

    pub(super) fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buffer)
    }

    pub(super) fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.inner.write(bytes)
    }

    pub(super) fn shutdown_write(&self) -> std::io::Result<()> {
        self.inner.shutdown(std::net::Shutdown::Write)
    }

    pub(super) fn io_source(&self) -> LocalSocketIoSource<'_> {
        LocalSocketIoSource::Fd(std::os::fd::AsFd::as_fd(self))
    }
}

impl std::os::fd::AsFd for LocalSocket {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.inner)
    }
}
