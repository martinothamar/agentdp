use std::ffi::OsString;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::process::Stdio;
use std::time::{Duration, Instant};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DetachedSpawnError {
    #[error("detached process spawning is not supported on this host")]
    Unsupported,
    #[error("failed to spawn detached process: {0}")]
    Io(#[from] std::io::Error),
    #[error("detacher exited unsuccessfully: {0}")]
    Failed(std::process::ExitStatus),
}

#[derive(Debug, Error)]
pub enum TerminateProcessError {
    #[error("process termination is not supported on this host")]
    Unsupported,
    #[error("failed to terminate process {pid}: {source}")]
    Io {
        pid: u32,
        #[source]
        source: std::io::Error,
    },
    #[error("process terminator exited unsuccessfully for {pid}: {status}")]
    Failed { pid: u32, status: std::process::ExitStatus },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStatus {
    Running,
    NotFound,
}

#[derive(Debug, Error)]
pub enum ProcessStatusError {
    #[error("process status checks are not supported on this host")]
    Unsupported,
    #[error("failed to check process {pid}: {source}")]
    Io {
        pid: u32,
        #[source]
        source: std::io::Error,
    },
    #[error("process status checker exited unsuccessfully for {pid}: {status}")]
    Failed { pid: u32, status: std::process::ExitStatus },
}

/// Spawns a process detached from the current CLI process.
///
/// On first-cut Linux/WSL2 hosts this uses `setsid -f`, so the target process
/// runs in its own session and is not tied to the CLI process lifetime.
///
/// # Errors
///
/// Returns an error when detached spawning is unsupported or the detacher fails.
#[cfg(target_os = "linux")]
pub fn spawn_detached(program: &Path, args: &[OsString]) -> Result<(), DetachedSpawnError> {
    spawn_detached_impl(program, args)
}

/// Spawns a process detached from the current CLI process.
///
/// # Errors
///
/// Returns an error because detached spawning is unsupported on this host.
#[cfg(not(target_os = "linux"))]
pub const fn spawn_detached(program: &Path, args: &[OsString]) -> Result<(), DetachedSpawnError> {
    spawn_detached_impl(program, args)
}

/// Requests termination of a process by PID.
///
/// # Errors
///
/// Returns an error when process termination is unsupported or the host
/// terminator command fails.
pub fn terminate_process(pid: u32) -> Result<(), TerminateProcessError> {
    terminate_process_impl(pid)
}

/// Checks whether a process appears to still be running.
///
/// # Errors
///
/// Returns an error when process status checks are unsupported or the platform
/// status probe fails.
pub fn process_status(pid: u32) -> Result<ProcessStatus, ProcessStatusError> {
    process_status_impl(pid)
}

/// Waits until a process is no longer running.
///
/// # Errors
///
/// Returns an error when the process status probe fails.
pub fn wait_for_process_exit(pid: u32, timeout: Duration) -> Result<bool, ProcessStatusError> {
    let deadline = Instant::now() + timeout;
    loop {
        if process_status(pid)? == ProcessStatus::NotFound {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(target_os = "linux")]
fn spawn_detached_impl(program: &Path, args: &[OsString]) -> Result<(), DetachedSpawnError> {
    let status = std::process::Command::new("setsid")
        .arg("-f")
        .arg(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;

    if status.success() {
        Ok(())
    } else {
        Err(DetachedSpawnError::Failed(status))
    }
}

#[cfg(not(target_os = "linux"))]
const fn spawn_detached_impl(_program: &Path, _args: &[OsString]) -> Result<(), DetachedSpawnError> {
    Err(DetachedSpawnError::Unsupported)
}

#[cfg(target_os = "linux")]
fn process_status_impl(pid: u32) -> Result<ProcessStatus, ProcessStatusError> {
    let stat_path = format!("/proc/{pid}/stat");
    let stat = match std::fs::read_to_string(&stat_path) {
        Ok(stat) => stat,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(ProcessStatus::NotFound),
        Err(source) => return Err(ProcessStatusError::Io { pid, source }),
    };

    let state = stat
        .rsplit_once(") ")
        .and_then(|(_prefix, suffix)| suffix.chars().next());
    if state == Some('Z') {
        Ok(ProcessStatus::NotFound)
    } else {
        Ok(ProcessStatus::Running)
    }
}

#[cfg(target_os = "windows")]
fn process_status_impl(pid: u32) -> Result<ProcessStatus, ProcessStatusError> {
    let filter = format!("PID eq {pid}");
    let output = std::process::Command::new("tasklist")
        .args(["/FI", &filter, "/FO", "CSV", "/NH"])
        .output()
        .map_err(|source| ProcessStatusError::Io { pid, source })?;
    if !output.status.success() {
        return Err(ProcessStatusError::Failed {
            pid,
            status: output.status,
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    if stdout.contains(&format!(",\"{pid}\"")) {
        Ok(ProcessStatus::Running)
    } else {
        Ok(ProcessStatus::NotFound)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
fn process_status_impl(_pid: u32) -> Result<ProcessStatus, ProcessStatusError> {
    Err(ProcessStatusError::Unsupported)
}

#[cfg(target_os = "linux")]
fn terminate_process_impl(pid: u32) -> Result<(), TerminateProcessError> {
    let status = std::process::Command::new("kill")
        .arg(pid.to_string())
        .status()
        .map_err(|source| TerminateProcessError::Io { pid, source })?;
    if status.success() {
        Ok(())
    } else {
        Err(TerminateProcessError::Failed { pid, status })
    }
}

#[cfg(target_os = "windows")]
fn terminate_process_impl(pid: u32) -> Result<(), TerminateProcessError> {
    let status = std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T"])
        .status()
        .map_err(|source| TerminateProcessError::Io { pid, source })?;
    if status.success() {
        Ok(())
    } else {
        Err(TerminateProcessError::Failed { pid, status })
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
fn terminate_process_impl(_pid: u32) -> Result<(), TerminateProcessError> {
    Err(TerminateProcessError::Unsupported)
}
