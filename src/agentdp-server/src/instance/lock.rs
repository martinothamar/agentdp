use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use agentdp_core::platform::{self, ProcessStatus};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("instance path has no parent directory: {0}")]
    MissingParent(PathBuf),
    #[error("instance path has no final component: {0}")]
    MissingName(PathBuf),
    #[error("failed to create instance lock parent directory {path}: {source}")]
    CreateParent {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("instance is locked by another operation: {path}")]
    Locked { path: PathBuf },
    #[error("instance is locked by another operation pid {pid}: {path}")]
    LockedByPid { path: PathBuf, pid: u32 },
    #[error("failed to acquire instance lock {path}: {source}")]
    Acquire {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write instance lock {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read instance lock {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to check owner of instance lock {path}: {source}")]
    ProcessStatus {
        path: PathBuf,
        #[source]
        source: platform::ProcessStatusError,
    },
    #[error("failed to remove stale instance lock {path}: {source}")]
    RemoveStale {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug)]
pub struct InstanceLock {
    path: PathBuf,
}

impl InstanceLock {
    pub fn acquire(instance_dir: &Path) -> Result<Self, Error> {
        let path = lock_path(instance_dir)?;
        let parent = path.parent().ok_or_else(|| Error::MissingParent(path.clone()))?;
        fs::create_dir_all(parent).map_err(|source| Error::CreateParent {
            path: parent.to_path_buf(),
            source,
        })?;

        loop {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    writeln!(file, "pid={}", std::process::id()).map_err(|source| Error::Write {
                        path: path.clone(),
                        source,
                    })?;
                    return Ok(Self { path });
                }
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => match lock_owner_pid(&path)? {
                    Some(pid) => match platform::process_status(pid).map_err(|source| Error::ProcessStatus {
                        path: path.clone(),
                        source,
                    })? {
                        ProcessStatus::Running => {
                            return Err(Error::LockedByPid {
                                path: path.clone(),
                                pid,
                            });
                        }
                        ProcessStatus::NotFound => {
                            fs::remove_file(&path).map_err(|source| Error::RemoveStale {
                                path: path.clone(),
                                source,
                            })?;
                        }
                    },
                    None => return Err(Error::Locked { path: path.clone() }),
                },
                Err(source) => {
                    return Err(Error::Acquire {
                        path: path.clone(),
                        source,
                    });
                }
            }
        }
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        let _result = fs::remove_file(&self.path);
    }
}

fn lock_path(instance_dir: &Path) -> Result<PathBuf, Error> {
    let parent = instance_dir
        .parent()
        .ok_or_else(|| Error::MissingParent(instance_dir.to_path_buf()))?;
    let name = instance_dir
        .file_name()
        .ok_or_else(|| Error::MissingName(instance_dir.to_path_buf()))?;
    Ok(parent.join(format!("{}.lock", name.to_string_lossy())))
}

fn lock_owner_pid(path: &Path) -> Result<Option<u32>, Error> {
    let contents = fs::read_to_string(path).map_err(|source| Error::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(contents
        .lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|pid| pid.parse::<u32>().ok()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::InstanceLock;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn lock_serializes_instance_mutation() {
        let temp = TestTempDir::create("instance-lock");
        let instance = temp.path.join("instances/altinn-studio/pr-0");

        let first = InstanceLock::acquire(&instance).unwrap();
        let second = InstanceLock::acquire(&instance);

        assert!(second.is_err());
        drop(first);
        assert!(InstanceLock::acquire(&instance).is_ok());
    }

    #[test]
    fn stale_lock_is_removed() {
        let temp = TestTempDir::create("instance-stale-lock");
        let instance = temp.path.join("instances/altinn-studio/pr-0");
        fs::create_dir_all(instance.parent().unwrap()).unwrap();
        fs::write(instance.parent().unwrap().join("pr-0.lock"), "pid=4294967295\n").unwrap();

        assert!(InstanceLock::acquire(&instance).is_ok());
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn create(name: &str) -> Self {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
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
