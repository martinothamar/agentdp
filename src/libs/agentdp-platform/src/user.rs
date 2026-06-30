#![allow(unsafe_code)]

use std::path::PathBuf;

use thiserror::Error;
use tokio::process::Command;

#[derive(Debug, Error)]
pub enum UserBinDirError {
    #[error("HOME is not set; cannot resolve user-local binary directory")]
    MissingHome,
    #[cfg(target_os = "windows")]
    #[error("LOCALAPPDATA is not set; cannot resolve user-local binary directory")]
    MissingLocalAppData,
}

#[derive(Debug, Error)]
pub enum CommandUserError {
    #[error("running commands as another user is not supported on this host")]
    Unsupported,
    #[error("user name contains an interior NUL byte")]
    InvalidUser,
    #[error("failed to resolve user {user}: {source}")]
    ResolveUser {
        user: String,
        #[source]
        source: std::io::Error,
    },
    #[error("current process is not privileged enough to run commands as {user}")]
    PermissionDenied { user: String },
}

/// Resolves the user-local directory for command-line binaries.
///
/// # Errors
///
/// Returns an error when the host cannot resolve a user-local binary directory.
pub fn user_bin_dir() -> Result<PathBuf, UserBinDirError> {
    user_bin_dir_impl()
}

#[cfg(unix)]
#[derive(Debug, Clone)]
pub struct UnixUser {
    name: std::ffi::CString,
    uid: u32,
    gid: u32,
}

#[cfg(unix)]
impl UnixUser {
    /// Resolves a Unix user through the platform user database.
    ///
    /// # Errors
    ///
    /// Returns an error when the user name is invalid or the user cannot be
    /// resolved.
    pub fn resolve(user: &str) -> Result<Self, CommandUserError> {
        unix::resolve_user(user)
    }

    #[must_use]
    pub const fn uid(&self) -> u32 {
        self.uid
    }

    #[must_use]
    pub const fn gid(&self) -> u32 {
        self.gid
    }
}

/// Configures a command to run as the resolved Unix user, including
/// supplementary groups.
///
/// # Errors
///
/// Returns an error when the user cannot be resolved on this platform.
#[cfg(unix)]
pub fn run_as_user(command: &mut Command, user: &str) -> Result<(), CommandUserError> {
    let user = UnixUser::resolve(user)?;
    if !can_switch_user(&user)? {
        return Ok(());
    }
    configure_unix_user(command, user);
    Ok(())
}

/// Configures a command to run as a Windows user.
///
/// Windows process user switching requires an existing token or credentials.
/// This function supports the current user as a no-op and rejects attempts to
/// switch users with a clear permission error.
///
/// # Errors
///
/// Returns an error when the requested user is not the current Windows user.
#[cfg(windows)]
pub fn run_as_user(_command: &mut Command, user: &str) -> Result<(), CommandUserError> {
    if windows::is_current_user(user)? {
        Ok(())
    } else {
        Err(CommandUserError::PermissionDenied { user: user.to_owned() })
    }
}

/// Configures a command to run as a user.
///
/// # Errors
///
/// This platform does not support user switching.
#[cfg(not(any(unix, windows)))]
pub fn run_as_user(_command: &mut Command, _user: &str) -> Result<(), CommandUserError> {
    Err(CommandUserError::Unsupported)
}

#[cfg(unix)]
fn can_switch_user(user: &UnixUser) -> Result<bool, CommandUserError> {
    let euid = unsafe { libc::geteuid() };
    if euid == 0 {
        return Ok(true);
    }
    if euid == user.uid {
        return Ok(false);
    }
    Err(CommandUserError::PermissionDenied {
        user: user.name.to_string_lossy().into_owned(),
    })
}

#[cfg(not(target_os = "windows"))]
fn user_bin_dir_impl() -> Result<PathBuf, UserBinDirError> {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|home| home.join(".local/bin"))
        .ok_or(UserBinDirError::MissingHome)
}

#[cfg(target_os = "windows")]
fn user_bin_dir_impl() -> Result<PathBuf, UserBinDirError> {
    std::env::var_os("LOCALAPPDATA")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|path| path.join("agentdp/bin"))
        .ok_or(UserBinDirError::MissingLocalAppData)
}

#[cfg(unix)]
fn configure_unix_user(command: &mut Command, user: UnixUser) {
    unsafe {
        command.pre_exec(move || {
            // SAFETY: pre_exec runs in the child after fork and before exec. The
            // CString is owned by the closure and has a stable NUL terminator.
            if libc::initgroups(user.name.as_ptr(), user.gid) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::setgid(user.gid) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::setuid(user.uid) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(unix)]
mod unix {
    use std::ffi::{CStr, CString};
    use std::io::ErrorKind;

    use super::{CommandUserError, UnixUser};

    pub(super) fn resolve_user(user: &str) -> Result<UnixUser, CommandUserError> {
        let name = CString::new(user).map_err(|_| CommandUserError::InvalidUser)?;
        let passwd = lookup_passwd(&name).map_err(|source| CommandUserError::ResolveUser {
            user: user.to_owned(),
            source,
        })?;
        Ok(UnixUser {
            name,
            uid: passwd.uid,
            gid: passwd.gid,
        })
    }

    fn lookup_passwd(name: &CStr) -> std::io::Result<Passwd> {
        let mut buffer = vec![0_u8; initial_buffer_size()];
        loop {
            let mut passwd = unsafe { std::mem::zeroed::<libc::passwd>() };
            let mut result = std::ptr::null_mut();
            let status = unsafe {
                libc::getpwnam_r(
                    name.as_ptr(),
                    &raw mut passwd,
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    &raw mut result,
                )
            };
            if status == 0 {
                if result.is_null() {
                    return Err(std::io::Error::new(ErrorKind::NotFound, "user was not found"));
                }
                return Ok(Passwd {
                    uid: passwd.pw_uid,
                    gid: passwd.pw_gid,
                });
            }
            if status != libc::ERANGE {
                return Err(std::io::Error::from_raw_os_error(status));
            }
            buffer.resize(buffer.len().saturating_mul(2), 0);
        }
    }

    fn initial_buffer_size() -> usize {
        let size = unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) };
        if size > 0 {
            usize::try_from(size).unwrap_or(16 * 1024)
        } else {
            16 * 1024
        }
    }

    struct Passwd {
        uid: u32,
        gid: u32,
    }
}

#[cfg(windows)]
mod windows {
    use super::CommandUserError;

    pub(super) fn is_current_user(user: &str) -> Result<bool, CommandUserError> {
        let current = std::env::var("USERNAME").map_err(|source| CommandUserError::ResolveUser {
            user: user.to_owned(),
            source: std::io::Error::other(source),
        })?;
        Ok(matches_user(user, &current))
    }

    fn matches_user(requested: &str, current: &str) -> bool {
        let requested = requested.rsplit(['\\', '/']).next().unwrap_or(requested);
        requested.eq_ignore_ascii_case(current)
    }

    #[cfg(test)]
    mod tests {
        use super::matches_user;

        #[test]
        fn matches_plain_and_domain_qualified_user_names() {
            assert!(matches_user("agent", "agent"));
            assert!(matches_user("DOMAIN\\agent", "agent"));
            assert!(matches_user("domain/agent", "AGENT"));
            assert!(!matches_user("other", "agent"));
        }
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::UnixUser;

    #[test]
    fn resolves_current_user() {
        let user = std::env::var("USER").expect("USER");
        let resolved = UnixUser::resolve(&user).expect("resolve current user");

        assert_eq!(resolved.name.to_string_lossy(), user);
    }
}
