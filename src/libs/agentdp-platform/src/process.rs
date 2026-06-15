use std::ffi::OsString;
use std::path::Path;
use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DetachedSpawnError {
    #[error("detached process spawning is not supported on this host")]
    Unsupported,
    #[error("failed to spawn detached process: {0}")]
    Io(#[from] std::io::Error),
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
}

/// Spawns a process detached from the current process asynchronously.
///
/// # Errors
///
/// Returns an error when detached spawning is unsupported or the native
/// platform call fails.
pub async fn spawn_detached(program: &Path, args: &[OsString]) -> Result<(), DetachedSpawnError> {
    spawn_command::spawn_detached(program, args, None).await
}

/// Spawns a detached process with stdout and stderr appended to a file.
///
/// # Errors
///
/// Returns an error when detached spawning is unsupported, the output file
/// cannot be opened, or the native platform call fails.
pub async fn spawn_detached_with_output(
    program: &Path,
    args: &[OsString],
    output: &Path,
) -> Result<(), DetachedSpawnError> {
    spawn_command::spawn_detached(program, args, Some(output)).await
}

/// Checks asynchronously whether a process appears to still be running.
///
/// # Errors
///
/// Returns an error when process status checks are unsupported or the native
/// platform call fails.
pub async fn process_status(pid: u32) -> Result<ProcessStatus, ProcessStatusError> {
    tokio::task::spawn_blocking(move || native::process_status(pid))
        .await
        .map_err(|source| ProcessStatusError::Io {
            pid,
            source: std::io::Error::other(source),
        })?
}

/// Requests termination of exactly one process by PID asynchronously.
///
/// Descendant processes are not terminated. Callers that need tree semantics
/// must use a separate tree-aware operation at the call site.
///
/// # Errors
///
/// Returns an error when process termination is unsupported or the native
/// platform call fails.
pub async fn terminate_process(pid: u32) -> Result<(), TerminateProcessError> {
    if pid == 0 {
        return Err(TerminateProcessError::Io {
            pid,
            source: std::io::Error::new(std::io::ErrorKind::InvalidInput, "process id 0 is not valid"),
        });
    }
    tokio::task::spawn_blocking(move || native::terminate_process(pid))
        .await
        .map_err(|source| TerminateProcessError::Io {
            pid,
            source: std::io::Error::other(source),
        })?
}

/// Waits asynchronously until a process is no longer running.
///
/// # Errors
///
/// Returns an error when the process status probe fails.
pub async fn wait_for_process_exit(pid: u32, timeout: Duration) -> Result<bool, ProcessStatusError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if process_status(pid).await? == ProcessStatus::NotFound {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(target_os = "linux")]
mod native {
    #![allow(unsafe_code)]

    use std::io::{Error, ErrorKind};

    use crate::process::{ProcessStatus, ProcessStatusError, TerminateProcessError};

    pub(super) fn process_status(pid: u32) -> Result<ProcessStatus, ProcessStatusError> {
        let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => stat,
            Err(source) if is_missing_process_error(&source) => return Ok(ProcessStatus::NotFound),
            Err(source) => return Err(ProcessStatusError::Io { pid, source }),
        };
        Ok(process_status_from_linux_stat(&stat))
    }

    pub(super) fn terminate_process(pid: u32) -> Result<(), TerminateProcessError> {
        let native_pid = native_pid(pid).map_err(|source| TerminateProcessError::Io { pid, source })?;
        let result = unsafe { libc::kill(native_pid, libc::SIGTERM) };
        if result == 0 {
            Ok(())
        } else {
            Err(TerminateProcessError::Io {
                pid,
                source: Error::last_os_error(),
            })
        }
    }

    fn is_missing_process_error(error: &std::io::Error) -> bool {
        error.kind() == ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
    }

    fn process_status_from_linux_stat(stat: &str) -> ProcessStatus {
        let state = stat
            .rsplit_once(") ")
            .and_then(|(_prefix, suffix)| suffix.chars().next());
        if state == Some('Z') {
            ProcessStatus::NotFound
        } else {
            ProcessStatus::Running
        }
    }

    fn native_pid(pid: u32) -> std::io::Result<libc::pid_t> {
        i32::try_from(pid)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, format!("process id {pid} exceeds pid_t range")))
    }
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
mod spawn_command {
    #![cfg_attr(target_os = "linux", allow(unsafe_code))]

    use std::ffi::OsString;
    use std::path::Path;
    use std::process::Stdio;

    use crate::process::DetachedSpawnError;
    use tokio::process::Command;

    #[cfg(target_os = "windows")]
    const DETACHED_FLAGS: u32 = DETACHED_FLAGS_WITHOUT_JOB_BREAKAWAY | CREATE_BREAKAWAY_FROM_JOB;
    #[cfg(target_os = "windows")]
    const DETACHED_FLAGS_WITHOUT_JOB_BREAKAWAY: u32 = CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW | DETACHED_PROCESS;

    #[cfg(target_os = "windows")]
    use windows_sys::Win32::System::Threading::{
        CREATE_BREAKAWAY_FROM_JOB, CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW, DETACHED_PROCESS,
    };

    pub(super) async fn spawn_detached(
        program: &Path,
        args: &[OsString],
        output: Option<&Path>,
    ) -> Result<(), DetachedSpawnError> {
        #[cfg(target_os = "windows")]
        {
            let result = spawn_with_flags(program, args, output, DETACHED_FLAGS).await;
            if matches!(&result, Err(DetachedSpawnError::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied)
            {
                // Some Windows job objects deny CREATE_BREAKAWAY_FROM_JOB. Keep the no-window,
                // detached process behavior and fall back to staying in that job when required.
                spawn_with_flags(program, args, output, DETACHED_FLAGS_WITHOUT_JOB_BREAKAWAY).await
            } else {
                result
            }
        }
        #[cfg(not(target_os = "windows"))]
        {
            spawn_with_detach(program, args, output).await
        }
    }

    #[cfg(target_os = "windows")]
    async fn spawn_with_flags(
        program: &Path,
        args: &[OsString],
        output: Option<&Path>,
        creation_flags: u32,
    ) -> Result<(), DetachedSpawnError> {
        let mut command = Command::new(program);
        command.args(args);
        configure_detach(&mut command, creation_flags);
        configure_stdio(&mut command, output).await?;
        drop(command.spawn()?);
        Ok(())
    }

    #[cfg(not(target_os = "windows"))]
    async fn spawn_with_detach(
        program: &Path,
        args: &[OsString],
        output: Option<&Path>,
    ) -> Result<(), DetachedSpawnError> {
        let mut command = Command::new(program);
        command.args(args);
        configure_detach(&mut command);
        configure_stdio(&mut command, output).await?;
        drop(command.spawn()?);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn configure_detach(command: &mut Command) {
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() < 0 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
    }

    #[cfg(target_os = "windows")]
    fn configure_detach(command: &mut Command, creation_flags: u32) {
        command.creation_flags(creation_flags);
        command.kill_on_drop(false);
    }

    async fn configure_stdio(command: &mut Command, output: Option<&Path>) -> std::io::Result<()> {
        command.stdin(Stdio::null());
        let Some(output) = output else {
            command.stdout(Stdio::null());
            command.stderr(Stdio::null());
            return Ok(());
        };
        let output = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(output)
            .await?
            .into_std()
            .await;
        let stderr = output.try_clone()?;
        command.stdout(Stdio::from(output));
        command.stderr(Stdio::from(stderr));
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
mod spawn_command {
    use std::ffi::OsString;
    use std::path::Path;

    use crate::process::DetachedSpawnError;

    pub(super) async fn spawn_detached(
        _program: &Path,
        _args: &[OsString],
        _output: Option<&Path>,
    ) -> Result<(), DetachedSpawnError> {
        Err(DetachedSpawnError::Unsupported)
    }
}

#[cfg(target_os = "windows")]
mod native {
    #![allow(unsafe_code)]

    use std::io::{Error, ErrorKind};

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_TERMINATE, TerminateProcess,
    };

    use crate::process::{ProcessStatus, ProcessStatusError, TerminateProcessError};

    const ERROR_INVALID_PARAMETER: i32 = 87;
    const STILL_ACTIVE: u32 = 259;

    pub(super) fn process_status(pid: u32) -> Result<ProcessStatus, ProcessStatusError> {
        let Some(process) = Handle::open(pid, PROCESS_QUERY_LIMITED_INFORMATION)
            .map_err(|source| ProcessStatusError::Io { pid, source })?
        else {
            return Ok(ProcessStatus::NotFound);
        };
        let mut exit_code = 0;
        let result = unsafe { GetExitCodeProcess(process.raw, std::ptr::addr_of_mut!(exit_code)) };
        if result == 0 {
            return Err(ProcessStatusError::Io {
                pid,
                source: Error::last_os_error(),
            });
        }
        if exit_code == STILL_ACTIVE {
            Ok(ProcessStatus::Running)
        } else {
            Ok(ProcessStatus::NotFound)
        }
    }

    pub(super) fn terminate_process(pid: u32) -> Result<(), TerminateProcessError> {
        let Some(process) =
            Handle::open(pid, PROCESS_TERMINATE).map_err(|source| TerminateProcessError::Io { pid, source })?
        else {
            return Err(TerminateProcessError::Io {
                pid,
                source: Error::new(ErrorKind::NotFound, "process was not found"),
            });
        };
        let result = unsafe { TerminateProcess(process.raw, 1) };
        if result == 0 {
            Err(TerminateProcessError::Io {
                pid,
                source: Error::last_os_error(),
            })
        } else {
            Ok(())
        }
    }

    struct Handle {
        raw: HANDLE,
    }

    impl Handle {
        fn open(pid: u32, access: u32) -> std::io::Result<Option<Self>> {
            let raw = unsafe { OpenProcess(access, 0, pid) };
            if raw.is_null() {
                let error = Error::last_os_error();
                if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER) {
                    return Ok(None);
                }
                return Err(error);
            }
            Ok(Some(Self { raw }))
        }
    }

    impl Drop for Handle {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.raw);
            }
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
mod native {
    use crate::process::{ProcessStatus, ProcessStatusError, TerminateProcessError};

    pub(super) fn process_status(_pid: u32) -> Result<ProcessStatus, ProcessStatusError> {
        Err(ProcessStatusError::Unsupported)
    }

    pub(super) fn terminate_process(_pid: u32) -> Result<(), TerminateProcessError> {
        Err(TerminateProcessError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    #[tokio::test]
    async fn terminate_process_rejects_pid_zero() {
        let error = super::terminate_process(0).await.unwrap_err();
        let super::TerminateProcessError::Io { source, .. } = error else {
            panic!("expected pid 0 to fail with an I/O error");
        };
        assert_eq!(source.kind(), ErrorKind::InvalidInput);
    }
}
