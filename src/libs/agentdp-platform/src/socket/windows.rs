use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::{LocalSocketError, LocalSocketIoSource, WINDOWS_SOCKET_RETRY_DELAY};

static LOCAL_SOCKET_PAIR_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(super) struct LocalSocket {
    inner: crate::windows_uds::UnixStream,
}

impl LocalSocket {
    pub(super) fn pair() -> Result<(Self, Self), LocalSocketError> {
        let path = socket_pair_path();
        let listener = crate::windows_uds::UnixListener::bind(&path)?;
        let client = crate::windows_uds::UnixStream::connect(&path)?;
        let server = loop {
            match listener.accept() {
                Ok(server) => break server,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(WINDOWS_SOCKET_RETRY_DELAY);
                }
                Err(error) => return Err(error.into()),
            }
        };
        let _removed = std::fs::remove_file(&path);
        Ok((Self { inner: server }, Self { inner: client }))
    }

    pub(super) fn connect(path: &Path) -> Result<Self, LocalSocketError> {
        Ok(Self {
            inner: crate::windows_uds::UnixStream::connect(path)?,
        })
    }

    pub(super) fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buffer)
    }

    pub(super) fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.inner.write(bytes)
    }

    pub(super) fn shutdown_write(&mut self) -> std::io::Result<()> {
        self.inner.shutdown_write()
    }

    pub(super) fn io_source(&self) -> LocalSocketIoSource<'_> {
        LocalSocketIoSource::Socket(std::os::windows::io::AsSocket::as_socket(self))
    }
}

impl std::os::windows::io::AsSocket for LocalSocket {
    fn as_socket(&self) -> std::os::windows::io::BorrowedSocket<'_> {
        std::os::windows::io::AsSocket::as_socket(&self.inner)
    }
}

fn socket_pair_path() -> PathBuf {
    let counter = LOCAL_SOCKET_PAIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "agentdp-local-socket-pair-{}-{counter}.sock",
        std::process::id()
    ))
}
