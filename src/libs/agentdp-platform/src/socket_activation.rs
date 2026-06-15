#![allow(unsafe_code)]

use std::io;
use std::os::fd::{FromRawFd as _, RawFd};
use std::os::unix::net::UnixListener;

pub struct ListenFds {
    fds: Vec<Option<RawFd>>,
}

impl ListenFds {
    #[must_use]
    pub fn from_env() -> Self {
        if !listen_pid_matches() {
            return Self { fds: Vec::new() };
        }
        let Some(count) = std::env::var("LISTEN_FDS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
        else {
            return Self { fds: Vec::new() };
        };
        let first_fd = std::env::var("LISTEN_FDS_FIRST_FD")
            .ok()
            .and_then(|value| value.parse::<RawFd>().ok())
            .unwrap_or(3);
        // Match sd_listen_fds behavior: after reading, unset these so child
        // processes do not accidentally inherit and consume the same fds.
        unsafe {
            std::env::remove_var("LISTEN_PID");
            std::env::remove_var("LISTEN_FDS");
            std::env::remove_var("LISTEN_FDS_FIRST_FD");
        }
        let fds = (0..count)
            .map(|offset| {
                let offset = RawFd::try_from(offset).ok()?;
                first_fd.checked_add(offset).map(Some)
            })
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        Self { fds }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.fds.len()
    }

    /// Returns true if no socket activation fds are present.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.fds.is_empty()
    }

    /// Takes a Unix stream listener at the given activation fd index.
    ///
    /// # Errors
    ///
    /// Returns an error if the fd exists but is not a Unix stream socket, or
    /// if validating or configuring the fd fails.
    pub fn take_unix_listener(&mut self, index: usize) -> io::Result<Option<UnixListener>> {
        self.take(index, libc::AF_UNIX, libc::SOCK_STREAM, "unix stream socket", |fd| {
            // Ownership of the activated fd transfers into UnixListener.
            unsafe { UnixListener::from_raw_fd(fd) }
        })
    }

    /// Takes a local socket listener at the given activation fd index.
    ///
    /// # Errors
    ///
    /// Returns an error if the fd exists but is not a Unix stream socket, or
    /// if validating or configuring the fd fails.
    pub fn take_local_socket_listener(
        &mut self,
        index: usize,
    ) -> io::Result<Option<crate::socket::AsyncLocalSocketListener>> {
        self.take_unix_listener(index)?
            .map(crate::socket::AsyncLocalSocketListener::from_std_unix_listener)
            .transpose()
    }

    fn take<T>(
        &mut self,
        index: usize,
        family: libc::c_int,
        socket_type: libc::c_int,
        label: &str,
        convert: impl FnOnce(RawFd) -> T,
    ) -> io::Result<Option<T>> {
        let Some(slot) = self.fds.get_mut(index) else {
            return Ok(None);
        };
        let Some(fd) = *slot else {
            return Ok(None);
        };
        validate_socket(fd, family, socket_type, label)?;
        set_cloexec(fd)?;
        *slot = None;
        Ok(Some(convert(fd)))
    }
}

fn listen_pid_matches() -> bool {
    match std::env::var("LISTEN_PID") {
        Ok(value) if !value.is_empty() => value.parse::<u32>().ok() == Some(std::process::id()),
        _ => false,
    }
}

fn validate_socket(fd: RawFd, family: libc::c_int, socket_type: libc::c_int, label: &str) -> io::Result<()> {
    let stat = fstat(fd)?;
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFSOCK {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fd {fd} is not a socket"),
        ));
    }
    let actual_type = socket_type_of(fd)?;
    let actual_family = socket_family_of(fd)?;
    if actual_type != socket_type || !family_matches(actual_family, family) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fd {fd} is not a valid {label}"),
        ));
    }
    Ok(())
}

fn fstat(fd: RawFd) -> io::Result<libc::stat> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let status = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if status < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { stat.assume_init() })
    }
}

fn socket_type_of(fd: RawFd) -> io::Result<libc::c_int> {
    let mut value = std::mem::MaybeUninit::<libc::c_int>::uninit();
    let mut len = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "socklen_t overflow"))?;
    let status = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            value.as_mut_ptr().cast(),
            &raw mut len,
        )
    };
    if status < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { value.assume_init() })
    }
}

fn socket_family_of(fd: RawFd) -> io::Result<libc::c_int> {
    let mut storage = std::mem::MaybeUninit::<libc::sockaddr_storage>::uninit();
    let mut len = libc::socklen_t::try_from(std::mem::size_of::<libc::sockaddr_storage>())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "socklen_t overflow"))?;
    let status = unsafe { libc::getsockname(fd, storage.as_mut_ptr().cast(), &raw mut len) };
    if status < 0 {
        Err(io::Error::last_os_error())
    } else {
        let storage = unsafe { storage.assume_init() };
        Ok(storage.ss_family.into())
    }
}

const fn family_matches(actual: libc::c_int, expected: libc::c_int) -> bool {
    actual == expected || (actual == libc::AF_INET6 && expected == libc::AF_INET)
}

fn set_cloexec(fd: RawFd) -> io::Result<()> {
    let status = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    if status < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::ListenFds;

    static ENV_LOCK: Mutex<()> = Mutex::new(());
    const ENV_KEYS: &[&str] = &["LISTEN_PID", "LISTEN_FDS", "LISTEN_FDS_FIRST_FD"];

    #[test]
    fn missing_listen_pid_disables_socket_activation() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_env();

        unsafe {
            std::env::set_var("LISTEN_FDS", "1");
        }

        let fds = ListenFds::from_env();

        assert!(fds.is_empty());
        assert_eq!(std::env::var("LISTEN_FDS").as_deref(), Ok("1"));
        clear_env();
    }

    #[test]
    fn matching_listen_pid_consumes_activation_env() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_env();

        unsafe {
            std::env::set_var("LISTEN_PID", std::process::id().to_string());
            std::env::set_var("LISTEN_FDS", "2");
            std::env::set_var("LISTEN_FDS_FIRST_FD", "10");
        }

        let fds = ListenFds::from_env();

        assert_eq!(fds.len(), 2);
        for key in ENV_KEYS {
            assert!(std::env::var(key).is_err(), "{key} should be unset");
        }
    }

    fn clear_env() {
        for key in ENV_KEYS {
            unsafe {
                std::env::remove_var(key);
            }
        }
    }
}
