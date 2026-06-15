use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agentdp_core::Context;
use agentdp_platform as platform;
use thiserror::Error;
use tokio::process::Command;
use tokio::time::Instant;

use crate::command;

pub const QEMU_SYSTEM_PATH_ENV: &str = "AGENTDP_QEMU_SYSTEM_PATH";
const PID_FILE_TIMEOUT: Duration = Duration::from_secs(10);
const PID_FILE_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QemuSystem {
    binary: PathBuf,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("qemu-system-x86_64 was not found; install qemu-system-x86_64 or set {QEMU_SYSTEM_PATH_ENV}")]
    MissingQemuSystem,
    #[error("failed to create QEMU runtime directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to remove stale QEMU runtime file {path}: {source}")]
    RemoveStaleFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to remove stale QEMU runtime directory {path}: {source}")]
    RemoveStaleDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to run qemu-system-x86_64: {0}")]
    Run(#[source] std::io::Error),
    #[error("failed to spawn detached qemu-system-x86_64: {0}")]
    DetachedSpawn(#[from] platform::process::DetachedSpawnError),
    #[error("qemu-system-x86_64 failed: {stderr}")]
    StartFailed { stderr: String },
    #[error("qemu-system-x86_64 did not write a valid pid file {path} within {timeout:?}; last error: {last_error}")]
    PidFileTimeout {
        path: PathBuf,
        timeout: Duration,
        last_error: String,
    },
    #[error("failed to read QEMU pid file {path}: {source}")]
    ReadPid {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("QEMU pid file {path} did not contain a valid process id: {contents}")]
    InvalidPid { path: PathBuf, contents: String },
}

impl QemuSystem {
    #[must_use]
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self { binary: binary.into() }
    }

    /// Resolves the configured or PATH-discovered `qemu-system-x86_64` executable.
    ///
    /// # Errors
    ///
    /// Returns an error when `AGENTDP_QEMU_SYSTEM_PATH` is unset and
    /// `qemu-system-x86_64` cannot be found on `PATH` or in the default Windows
    /// installation path.
    pub async fn resolve() -> Result<Self, Error> {
        if let Some(path) = std::env::var_os(QEMU_SYSTEM_PATH_ENV).filter(|value| !value.is_empty()) {
            return Ok(Self::new(path));
        }
        let binary = platform::host::find_binary("qemu-system-x86_64")
            .await
            .or(default_windows_qemu_system().await)
            .ok_or(Error::MissingQemuSystem)?;
        Ok(Self::new(binary))
    }

    /// Starts a QEMU VM from a rendered command specification.
    ///
    /// # Errors
    ///
    /// Returns an error when runtime paths cannot be prepared, QEMU cannot be
    /// started, QEMU exits unsuccessfully, or a daemonized launch does not write
    /// a valid pid file.
    pub async fn start(&self, context: &Context, spec: &command::CommandSpec) -> Result<u32, Error> {
        prepare_runtime_paths(spec).await?;
        let args = command::args(spec);
        context
            .logger()
            .verbose_with(|| format!("starting QEMU with arguments: {}", args.join(" ")));
        start_qemu(&self.binary, &args, spec).await
    }
}

async fn start_qemu(binary: &Path, args: &[String], spec: &command::CommandSpec) -> Result<u32, Error> {
    if spec.daemonize {
        let mut command = Command::new(binary);
        command.args(args.iter().map(OsString::from));
        platform::command::hide_child_window(&mut command);
        let output = command.output().await.map_err(Error::Run)?;
        if !output.status.success() {
            return Err(Error::StartFailed {
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        return wait_for_pid_file(&spec.pid_file, &spec.qemu_log, PID_FILE_TIMEOUT).await;
    }

    let args = args.iter().map(OsString::from).collect::<Vec<_>>();
    platform::process::spawn_detached_with_output(binary, &args, &spec.qemu_log).await?;
    wait_for_pid_file(&spec.pid_file, &spec.qemu_log, PID_FILE_TIMEOUT).await
}

async fn default_windows_qemu_system() -> Option<PathBuf> {
    if !cfg!(windows) {
        return None;
    }
    let path = PathBuf::from(r"C:\Program Files\qemu\qemu-system-x86_64.exe");
    tokio::fs::metadata(&path)
        .await
        .is_ok_and(|metadata| metadata.is_file())
        .then_some(path)
}

/// Reads a QEMU pid file asynchronously.
///
/// # Errors
///
/// Returns an error when the pid file cannot be read or does not contain a
/// valid process id.
pub async fn read_pid_file(path: &Path) -> Result<u32, Error> {
    let contents = tokio::fs::read_to_string(path).await.map_err(|source| Error::ReadPid {
        path: path.to_path_buf(),
        source,
    })?;
    contents.trim().parse::<u32>().map_err(|_| Error::InvalidPid {
        path: path.to_path_buf(),
        contents: contents.trim().to_owned(),
    })
}

async fn wait_for_pid_file(path: &Path, qemu_log: &Path, timeout: Duration) -> Result<u32, Error> {
    let deadline = Instant::now() + timeout;
    loop {
        match read_pid_file(path).await {
            Ok(pid) => return Ok(pid),
            Err(error) if pid_file_can_still_appear(&error) => {
                if Instant::now() >= deadline {
                    return Err(Error::PidFileTimeout {
                        path: path.to_path_buf(),
                        timeout,
                        last_error: pid_file_wait_error(&error, qemu_log).await,
                    });
                }
                tokio::time::sleep(PID_FILE_POLL_INTERVAL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn pid_file_wait_error(error: &Error, qemu_log: &Path) -> String {
    let mut message = error.to_string();
    if let Ok(log) = tokio::fs::read_to_string(qemu_log).await {
        let log = log.trim();
        if !log.is_empty() {
            message.push_str("; qemu log tail: ");
            message.push_str(&tail(log, 4096));
        }
    }
    message
}

fn tail(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let tail = value
        .char_indices()
        .nth_back(max_chars.saturating_sub(1))
        .map_or(value, |(index, _)| &value[index..]);
    tail.trim_start().to_owned()
}

fn pid_file_can_still_appear(error: &Error) -> bool {
    match error {
        Error::ReadPid { source, .. } => source.kind() == std::io::ErrorKind::NotFound,
        Error::InvalidPid { .. } => true,
        _ => false,
    }
}

/// Removes stale QEMU runtime files and empty runtime directories asynchronously.
///
/// # Errors
///
/// Returns an error when a stale file or empty ancestor directory cannot be
/// removed.
pub async fn cleanup_runtime_files(
    pid_file: &Path,
    monitor_socket: &Path,
    qmp_socket: &Path,
    guest_control_socket: &Path,
) -> Result<(), Error> {
    for path in [pid_file, monitor_socket, qmp_socket, guest_control_socket] {
        remove_stale_file(path).await?;
    }
    remove_empty_ancestors(pid_file, 3).await?;
    Ok(())
}

async fn prepare_runtime_paths(spec: &command::CommandSpec) -> Result<(), Error> {
    for path in [
        &spec.pid_file,
        &spec.monitor_socket,
        &spec.qmp_socket,
        &spec.guest_control_socket,
        &spec.serial_log,
        &spec.qemu_log,
    ] {
        create_parent(path).await?;
    }
    if let command::NetworkBackend::Stream { socket, .. } = &spec.network {
        create_parent(socket).await?;
    }
    for path in [
        &spec.pid_file,
        &spec.monitor_socket,
        &spec.qmp_socket,
        &spec.guest_control_socket,
    ] {
        remove_stale_file(path).await?;
    }
    Ok(())
}

async fn create_parent(path: &Path) -> Result<(), Error> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|source| Error::CreateDirectory {
            path: parent.to_path_buf(),
            source,
        })
}

async fn remove_stale_file(path: &Path) -> Result<(), Error> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::RemoveStaleFile {
            path: path.to_path_buf(),
            source,
        }),
    }
}

async fn remove_empty_ancestors(path: &Path, max_depth: usize) -> Result<(), Error> {
    let mut current = path.parent();
    for _ in 0..max_depth {
        let Some(directory) = current else {
            return Ok(());
        };
        match tokio::fs::remove_dir(directory).await {
            Ok(()) => {}
            Err(source)
                if matches!(
                    source.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                ) =>
            {
                return Ok(());
            }
            Err(source) => {
                return Err(Error::RemoveStaleDirectory {
                    path: directory.to_path_buf(),
                    source,
                });
            }
        }
        current = directory.parent();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{cleanup_runtime_files, pid_file_wait_error, read_pid_file, tail, wait_for_pid_file};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn reads_pid_file() {
        let temp = TestTempDir::create("qemu-system-pid");
        let pid_file = temp.path.join("qemu.pid");
        fs::write(&pid_file, "4242\n").unwrap();

        assert_eq!(read_pid_file(&pid_file).await.unwrap(), 4242);
    }

    #[tokio::test]
    async fn waits_for_pid_file_to_appear() {
        let temp = TestTempDir::create("qemu-system-pid-wait");
        let pid_file = temp.path.join("qemu.pid");
        let writer_pid_file = pid_file.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            tokio::fs::write(writer_pid_file, "4242\n").await.unwrap();
        });

        let qemu_log = temp.path.join("qemu.log");

        assert_eq!(
            wait_for_pid_file(&pid_file, &qemu_log, std::time::Duration::from_secs(1))
                .await
                .unwrap(),
            4242
        );
    }

    #[tokio::test]
    async fn pid_file_wait_error_includes_qemu_log_tail() {
        let temp = TestTempDir::create("qemu-system-pid-log");
        let pid_file = temp.path.join("qemu.pid");
        let qemu_log = temp.path.join("qemu.log");
        fs::write(&qemu_log, "line 1\nline 2\n").unwrap();
        let error = read_pid_file(&pid_file).await.unwrap_err();

        let message = pid_file_wait_error(&error, &qemu_log).await;

        assert!(message.contains("qemu log tail: line 1\nline 2"));
    }

    #[test]
    fn tail_limits_to_requested_chars() {
        assert_eq!(tail("abcdef", 3), "def");
    }

    #[tokio::test]
    async fn cleanup_runtime_files_removes_empty_runtime_directory() {
        let temp = TestTempDir::create("qemu-system-cleanup");
        let manifest_dir = temp.path.join("instances/altinn-studio");
        let instance_dir = manifest_dir.join("pr-0");
        let qemu_dir = instance_dir.join("qemu");
        fs::create_dir_all(&qemu_dir).unwrap();
        let pid_file = qemu_dir.join("qemu.pid");
        let monitor_socket = qemu_dir.join("monitor.sock");
        let qmp_socket = qemu_dir.join("qmp.sock");
        let guest_control_socket = qemu_dir.join("guest-control.sock");
        fs::write(&pid_file, "4242\n").unwrap();
        fs::write(&monitor_socket, "").unwrap();
        fs::write(&qmp_socket, "").unwrap();
        fs::write(&guest_control_socket, "").unwrap();

        cleanup_runtime_files(&pid_file, &monitor_socket, &qmp_socket, &guest_control_socket)
            .await
            .unwrap();

        assert!(!qemu_dir.exists());
        assert!(!instance_dir.exists());
        assert!(!manifest_dir.exists());
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
