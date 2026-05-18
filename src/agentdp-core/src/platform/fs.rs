use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketStatus {
    Missing,
    Connected,
    Unavailable(String),
    Unsupported,
}

/// Ensures a directory exists and can be written by the current user.
///
/// # Errors
///
/// Returns an error when the directory cannot be created, a probe file cannot
/// be written, or the probe file cannot be removed.
pub fn ensure_writable_directory(path: &Path) -> std::io::Result<()> {
    fs::create_dir_all(path)?;
    let probe = path.join(format!(".agentdp-write-test-{}", std::process::id()));
    fs::write(&probe, b"agentdp doctor\n")?;
    fs::remove_file(probe)?;
    Ok(())
}

#[must_use]
pub fn local_socket_status(path: &Path) -> SocketStatus {
    if !path.exists() {
        return SocketStatus::Missing;
    }

    #[cfg(unix)]
    {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(_) => SocketStatus::Connected,
            Err(error) => SocketStatus::Unavailable(error.to_string()),
        }
    }

    #[cfg(not(unix))]
    {
        SocketStatus::Unsupported
    }
}

/// Applies executable permissions where the host platform requires them.
///
/// # Errors
///
/// Returns an error when permissions cannot be read or updated.
#[cfg(unix)]
pub fn set_executable(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)
}

/// Applies executable permissions where the host platform requires them.
///
/// # Errors
///
/// This platform does not require executable permission changes, so this
/// function always succeeds.
#[cfg(not(unix))]
pub const fn set_executable(_path: &Path) -> std::io::Result<()> {
    Ok(())
}
