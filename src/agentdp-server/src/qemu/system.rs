use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use agentdp_core::Context;
use agentdp_core::platform;
use thiserror::Error;

use super::command;

pub(super) const QEMU_SYSTEM_PATH_ENV: &str = "AGENTDP_QEMU_SYSTEM_PATH";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QemuSystem {
    binary: PathBuf,
}

#[derive(Debug, Error)]
pub(super) enum Error {
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
    #[error("qemu-system-x86_64 failed: {stderr}")]
    StartFailed { stderr: String },
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
    pub(super) fn new(binary: impl Into<PathBuf>) -> Self {
        Self { binary: binary.into() }
    }

    pub(super) fn resolve() -> Result<Self, Error> {
        if let Some(path) = std::env::var_os(QEMU_SYSTEM_PATH_ENV).filter(|value| !value.is_empty()) {
            return Ok(Self::new(path));
        }
        let binary = platform::find_binary("qemu-system-x86_64")
            .or_else(default_windows_qemu_system)
            .ok_or(Error::MissingQemuSystem)?;
        Ok(Self::new(binary))
    }

    pub(super) fn start(&self, context: &Context, spec: &command::CommandSpec) -> Result<u32, Error> {
        prepare_runtime_paths(spec)?;
        let args = command::args(spec);
        context
            .logger()
            .verbose_with(|| format!("starting QEMU with arguments: {}", args.join(" ")));
        start_qemu(&self.binary, &args, spec)
    }
}

fn start_qemu(binary: &Path, args: &[String], spec: &command::CommandSpec) -> Result<u32, Error> {
    if spec.daemonize {
        let mut command = Command::new(binary);
        command.args(args.iter().map(OsString::from));
        let output = platform::hide_child_window(&mut command).output().map_err(Error::Run)?;
        if !output.status.success() {
            return Err(Error::StartFailed {
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        return read_pid_file(&spec.pid_file);
    }

    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&spec.qemu_log)
        .map_err(Error::Run)?;
    let stderr = log.try_clone().map_err(Error::Run)?;
    let mut command = Command::new(binary);
    command
        .args(args.iter().map(OsString::from))
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));
    let child = platform::hide_child_window(&mut command).spawn().map_err(Error::Run)?;
    Ok(child.id())
}

fn default_windows_qemu_system() -> Option<PathBuf> {
    cfg!(windows)
        .then(|| PathBuf::from(r"C:\Program Files\qemu\qemu-system-x86_64.exe"))
        .filter(|path| path.is_file())
}

pub(super) fn read_pid_file(path: &Path) -> Result<u32, Error> {
    let contents = fs::read_to_string(path).map_err(|source| Error::ReadPid {
        path: path.to_path_buf(),
        source,
    })?;
    contents.trim().parse::<u32>().map_err(|_| Error::InvalidPid {
        path: path.to_path_buf(),
        contents: contents.trim().to_owned(),
    })
}

pub(super) fn cleanup_runtime_files(pid_file: &Path, monitor_socket: &Path, qmp_socket: &Path) -> Result<(), Error> {
    for path in [pid_file, monitor_socket, qmp_socket] {
        remove_stale_file(path)?;
    }
    remove_empty_ancestors(pid_file, 3)?;
    Ok(())
}

fn prepare_runtime_paths(spec: &command::CommandSpec) -> Result<(), Error> {
    for path in [
        &spec.pid_file,
        &spec.monitor_socket,
        &spec.qmp_socket,
        &spec.serial_log,
        &spec.qemu_log,
    ] {
        create_parent(path)?;
    }
    for path in [&spec.pid_file, &spec.monitor_socket, &spec.qmp_socket] {
        remove_stale_file(path)?;
    }
    Ok(())
}

fn create_parent(path: &Path) -> Result<(), Error> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    fs::create_dir_all(parent).map_err(|source| Error::CreateDirectory {
        path: parent.to_path_buf(),
        source,
    })
}

fn remove_stale_file(path: &Path) -> Result<(), Error> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::RemoveStaleFile {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn remove_empty_ancestors(path: &Path, max_depth: usize) -> Result<(), Error> {
    let mut current = path.parent();
    for _ in 0..max_depth {
        let Some(directory) = current else {
            return Ok(());
        };
        match fs::remove_dir(directory) {
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

    use super::{cleanup_runtime_files, read_pid_file};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn reads_pid_file() {
        let temp = TestTempDir::create("qemu-system-pid");
        let pid_file = temp.path.join("qemu.pid");
        fs::write(&pid_file, "4242\n").unwrap();

        assert_eq!(read_pid_file(&pid_file).unwrap(), 4242);
    }

    #[test]
    fn cleanup_runtime_files_removes_empty_runtime_directory() {
        let temp = TestTempDir::create("qemu-system-cleanup");
        let manifest_dir = temp.path.join("instances/altinn-studio");
        let instance_dir = manifest_dir.join("pr-0");
        let qemu_dir = instance_dir.join("qemu");
        fs::create_dir_all(&qemu_dir).unwrap();
        let pid_file = qemu_dir.join("qemu.pid");
        let monitor_socket = qemu_dir.join("monitor.sock");
        let qmp_socket = qemu_dir.join("qmp.sock");
        fs::write(&pid_file, "4242\n").unwrap();
        fs::write(&monitor_socket, "").unwrap();
        fs::write(&qmp_socket, "").unwrap();

        cleanup_runtime_files(&pid_file, &monitor_socket, &qmp_socket).unwrap();

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
