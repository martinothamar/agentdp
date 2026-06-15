use std::io;
use std::path::Path;
use std::time::Duration;

use agentdp_platform::socket::{self, AsyncLocalSocket};

use super::local_protocol::{Request, Response, read_response, write_request};
use super::paths::RuntimePaths;
use crate::{Error, Result};
use agentdp_platform::fs::{path_exists, remove_file};

const DAEMON_CONNECT_TIMEOUT: Duration = Duration::from_mins(1);
const DAEMON_CONNECT_RETRY_DELAY: Duration = Duration::from_millis(250);

pub(crate) async fn client_request(request: Request) -> Result<Response> {
    let paths = RuntimePaths::discover()?;
    let mut stream = connect_guest_daemon(&paths.socket).await?;
    write_request(&mut stream, &request).await?;
    read_response(&mut stream).await
}

async fn connect_guest_daemon(path: &Path) -> Result<AsyncLocalSocket> {
    let started = tokio::time::Instant::now();
    loop {
        match socket::connect_local_socket(path).await {
            Ok(stream) => return Ok(stream),
            Err(error) if should_retry_daemon_connect(&error) && started.elapsed() < DAEMON_CONNECT_TIMEOUT => {
                tokio::time::sleep(DAEMON_CONNECT_RETRY_DELAY).await;
            }
            Err(error) if should_retry_daemon_connect(&error) => {
                return Err(Error::Message(format!(
                    "guest daemon socket {} did not become ready within {}s",
                    path.display(),
                    DAEMON_CONNECT_TIMEOUT.as_secs()
                )));
            }
            Err(error) => {
                return Err(Error::ConnectSocket {
                    path: path.to_path_buf(),
                    source: local_socket_io_error(error),
                });
            }
        }
    }
}

pub(crate) fn local_socket_io_error(error: socket::LocalSocketError) -> io::Error {
    match error {
        socket::LocalSocketError::Io(error) => error,
        socket::LocalSocketError::Unsupported => io::Error::new(
            io::ErrorKind::Unsupported,
            "agentdp guest local sockets are not supported on this platform",
        ),
    }
}

pub(crate) fn should_retry_daemon_connect(error: &socket::LocalSocketError) -> bool {
    matches!(
        error,
        socket::LocalSocketError::Io(source)
            if matches!(
                source.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused | io::ErrorKind::AddrNotAvailable
            )
    )
}

pub(crate) async fn remove_stale_socket(path: &Path) -> Result<()> {
    if !path_exists(path).await? {
        return Ok(());
    }
    if socket::connect_local_socket(path).await.is_ok() {
        return Err(Error::Message(format!(
            "agentdp guest daemon socket is already active at {}",
            path.display()
        )));
    }
    remove_file(path).await.map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use std::io;

    use agentdp_platform::socket::LocalSocketError;

    use super::should_retry_daemon_connect;

    #[test]
    fn daemon_connect_retries_startup_race_errors_only() {
        for kind in [
            io::ErrorKind::NotFound,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::AddrNotAvailable,
        ] {
            assert!(should_retry_daemon_connect(&LocalSocketError::Io(io::Error::from(
                kind
            ))));
        }

        assert!(!should_retry_daemon_connect(&LocalSocketError::Io(io::Error::from(
            io::ErrorKind::PermissionDenied
        ))));
        assert!(!should_retry_daemon_connect(&LocalSocketError::Unsupported));
    }
}
