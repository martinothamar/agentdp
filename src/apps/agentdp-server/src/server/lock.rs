use std::path::{Path, PathBuf};

use agentdp_platform as platform;
use tokio::io::AsyncWriteExt;

use super::Error;

#[derive(Debug)]
pub(super) struct ServerLock {
    path: PathBuf,
    pid: u32,
}

impl ServerLock {
    pub(super) async fn release(self) -> Result<(), Error> {
        let contents = match tokio::fs::read_to_string(&self.path).await {
            Ok(contents) => contents,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(Error::ReadLock {
                    path: self.path,
                    source,
                });
            }
        };
        if lock_owner_pid_from_contents(&contents) == Some(self.pid) {
            tokio::fs::remove_file(&self.path)
                .await
                .map_err(|source| Error::WriteLock {
                    path: self.path,
                    source,
                })?;
        }
        Ok(())
    }
}

pub(super) async fn acquire_server_lock(socket_path: &Path) -> Result<ServerLock, Error> {
    let lock_path = server_lock_path(socket_path);
    if let Some(parent) = lock_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| Error::WriteLock {
                path: lock_path.clone(),
                source,
            })?;
    }
    let pid = std::process::id();

    loop {
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .await
        {
            Ok(mut file) => {
                file.write_all(format!("pid={pid}\n").as_bytes())
                    .await
                    .map_err(|source| Error::WriteLock {
                        path: lock_path.clone(),
                        source,
                    })?;
                file.flush().await.map_err(|source| Error::WriteLock {
                    path: lock_path.clone(),
                    source,
                })?;
                return Ok(ServerLock { path: lock_path, pid });
            }
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Some(owner) = server_lock_owner(&lock_path).await? {
                    match platform::process::process_status(owner)
                        .await
                        .map_err(|source| Error::ProcessStatus { pid: owner, source })?
                    {
                        platform::process::ProcessStatus::Running => {
                            return Err(Error::AlreadyRunning {
                                path: lock_path,
                                pid: owner,
                            });
                        }
                        platform::process::ProcessStatus::NotFound => {
                            let _result = tokio::fs::remove_file(&lock_path).await;
                        }
                    }
                } else {
                    let _result = tokio::fs::remove_file(&lock_path).await;
                }
            }
            Err(source) => {
                return Err(Error::WriteLock {
                    path: lock_path,
                    source,
                });
            }
        }
    }
}

fn server_lock_path(socket_path: &Path) -> PathBuf {
    socket_path.with_extension("lock")
}

async fn server_lock_owner(path: &Path) -> Result<Option<u32>, Error> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .map_err(|source| Error::ReadLock {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(lock_owner_pid_from_contents(&contents))
}

fn lock_owner_pid_from_contents(contents: &str) -> Option<u32> {
    contents
        .lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|pid| pid.parse::<u32>().ok())
}

#[cfg(test)]
mod tests {
    use agentdp_platform::time;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{acquire_server_lock, server_lock_path};
    use crate::server::Error;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test(flavor = "local")]
    async fn server_lock_rejects_running_owner() {
        let temp = TestTempDir::create("server-lock-running");
        let socket = temp.path.join("agentdp-server.sock");
        let lock = server_lock_path(&socket);
        fs::write(&lock, format!("pid={}\n", std::process::id())).unwrap();

        let error = acquire_server_lock(&socket).await.unwrap_err();

        assert!(matches!(error, Error::AlreadyRunning { .. }));
    }

    #[tokio::test(flavor = "local")]
    async fn server_lock_replaces_stale_owner() {
        let temp = TestTempDir::create("server-lock-stale");
        let socket = temp.path.join("agentdp-server.sock");
        let lock = server_lock_path(&socket);
        fs::write(&lock, "pid=999999\n").unwrap();

        let guard = acquire_server_lock(&socket).await.unwrap();

        assert_eq!(guard.pid, std::process::id());
        assert_eq!(fs::read_to_string(&lock).unwrap(), format!("pid={}\n", guard.pid));
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn create(name: &str) -> Self {
            let timestamp = time::unix_nanos();
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("agentdp-{name}-{}-{timestamp}-{id}", std::process::id()));
            fs::create_dir(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _result = fs::remove_dir_all(&self.path);
        }
    }
}
