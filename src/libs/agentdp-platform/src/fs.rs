use std::path::{Path, PathBuf};

use thiserror::Error;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketStatus {
    Missing,
    Connected,
    Unavailable(String),
    Unsupported,
}

#[derive(Debug, Error)]
pub enum PrivatePathError {
    #[error("failed to create directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to inspect path {path}: {source}")]
    Metadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("refusing world-accessible {kind} path {path}; expected mode without other permissions")]
    WorldAccessiblePath { kind: &'static str, path: PathBuf },
}

#[derive(Debug, Error)]
pub enum UserOwnedFileError {
    #[error("{0}")]
    User(#[from] crate::user::CommandUserError),
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// Ensures a private directory exists and is not world-accessible.
///
/// # Errors
///
/// Returns an error when the directory cannot be created, inspected, made
/// private, or still has permissions that allow access to other users.
pub async fn ensure_private_dir(path: &Path, kind: &'static str) -> Result<(), PrivatePathError> {
    if path_exists(path).await.map_err(|source| PrivatePathError::Metadata {
        path: path.to_path_buf(),
        source,
    })? {
        reject_world_accessible(path, kind).await?;
        return Ok(());
    }
    tokio::fs::create_dir_all(path)
        .await
        .map_err(|source| PrivatePathError::CreateDirectory {
            path: path.to_path_buf(),
            source,
        })?;
    set_private_dir(path).await?;
    reject_world_accessible(path, kind).await
}

async fn reject_world_accessible(path: &Path, kind: &'static str) -> Result<(), PrivatePathError> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|source| PrivatePathError::Metadata {
            path: path.to_path_buf(),
            source,
        })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        if metadata.permissions().mode() & 0o007 != 0 {
            return Err(PrivatePathError::WorldAccessiblePath {
                kind,
                path: path.to_path_buf(),
            });
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (&metadata, kind);
    }
    Ok(())
}

#[cfg_attr(
    not(unix),
    allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps, clippy::unused_async)
)]
async fn set_private_dir(path: &Path) -> Result<(), PrivatePathError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|source| PrivatePathError::WriteFile {
                path: path.to_path_buf(),
                source,
            })?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Restricts a private file to the current user where the host platform
/// supports Unix-style permissions.
///
/// # Errors
///
/// Returns an error when platform permissions cannot be updated.
#[cfg_attr(
    not(unix),
    allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps, clippy::unused_async)
)]
pub async fn set_private_file(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Applies a Unix file mode where the host platform supports Unix-style
/// permissions.
///
/// # Errors
///
/// Returns an error when platform permissions cannot be updated.
#[cfg_attr(
    not(unix),
    allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps, clippy::unused_async)
)]
pub async fn set_file_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).await?;
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
    Ok(())
}

/// Writes a file atomically by creating a sibling temporary file and renaming it
/// into place.
///
/// # Errors
///
/// Returns an error when the parent directory cannot be created, the temporary
/// file cannot be written, permissions cannot be set, or the rename fails.
pub async fn write_atomic(path: &Path, contents: &[u8], mode: u32) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = atomic_temp_path(path);
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)
        .await?;
    file.write_all(contents).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode)).await?;
    }
    #[cfg(not(unix))]
    {
        let _ = mode;
    }
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&tmp, path).await?;
    sync_parent_directory(path)?;
    Ok(())
}

/// Writes a user-owned file atomically when contents, mode, or owner differ.
///
/// # Errors
///
/// Returns an error when the user cannot be resolved, the current process cannot
/// create or replace the file, or platform ownership/permission updates fail.
pub async fn write_user_owned_file(
    path: &Path,
    contents: &[u8],
    file_mode: u32,
    directory_mode: u32,
    user: &str,
) -> Result<bool, UserOwnedFileError> {
    let owner = ResolvedFileOwner::resolve(user)?;
    if user_owned_file_matches(path, contents, file_mode, owner).await? {
        return Ok(false);
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
        set_file_mode(parent, directory_mode).await?;
        chown_if_needed(parent, owner).await?;
    }
    let tmp = atomic_temp_path(path);
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)
        .await?;
    file.write_all(contents).await?;
    set_file_mode(&tmp, file_mode).await?;
    chown_if_needed(&tmp, owner).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&tmp, path).await?;
    sync_parent_directory(path)?;
    Ok(true)
}

async fn user_owned_file_matches(
    path: &Path,
    contents: &[u8],
    file_mode: u32,
    owner: ResolvedFileOwner,
) -> std::io::Result<bool> {
    let existing = match tokio::fs::read(path).await {
        Ok(existing) => existing,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if existing != contents {
        return Ok(false);
    }
    let metadata = tokio::fs::metadata(path).await?;
    Ok(file_mode_matches(&metadata, file_mode) && file_owner_matches(&metadata, owner))
}

#[derive(Clone, Copy)]
struct ResolvedFileOwner {
    #[cfg(unix)]
    uid: u32,
    #[cfg(unix)]
    gid: u32,
}

impl ResolvedFileOwner {
    #[cfg(unix)]
    fn resolve(user: &str) -> Result<Self, crate::user::CommandUserError> {
        let user = crate::user::UnixUser::resolve(user)?;
        Ok(Self {
            uid: user.uid(),
            gid: user.gid(),
        })
    }

    #[cfg(not(unix))]
    fn resolve(_user: &str) -> Result<Self, crate::user::CommandUserError> {
        Ok(Self {})
    }
}

#[cfg(unix)]
fn file_mode_matches(metadata: &std::fs::Metadata, mode: u32) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o777 == mode
}

#[cfg(not(unix))]
fn file_mode_matches(_metadata: &std::fs::Metadata, _mode: u32) -> bool {
    true
}

#[cfg(unix)]
fn file_owner_matches(metadata: &std::fs::Metadata, owner: ResolvedFileOwner) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    metadata.uid() == owner.uid && metadata.gid() == owner.gid
}

#[cfg(not(unix))]
fn file_owner_matches(_metadata: &std::fs::Metadata, _owner: ResolvedFileOwner) -> bool {
    true
}

#[cfg(unix)]
async fn chown_if_needed(path: &Path, owner: ResolvedFileOwner) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = tokio::fs::metadata(path).await?;
    if metadata.uid() == owner.uid && metadata.gid() == owner.gid {
        return Ok(());
    }
    std::os::unix::fs::chown(path, Some(owner.uid), Some(owner.gid))
}

#[cfg(not(unix))]
async fn chown_if_needed(_path: &Path, _owner: ResolvedFileOwner) -> std::io::Result<()> {
    Ok(())
}

fn atomic_temp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("agentdp-atomic");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    path.with_file_name(format!(".{file_name}.{}.{nonce}.tmp", std::process::id()))
}

#[cfg_attr(not(unix), allow(clippy::unnecessary_wraps))]
fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Reads UTF-8 text from a file, returning `None` when it does not exist.
///
/// # Errors
///
/// Returns an error when the file exists but cannot be read.
pub async fn read_optional_text(path: &Path) -> std::io::Result<Option<String>> {
    match tokio::fs::read_to_string(path).await {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Lists direct child files with the requested extension, returning an empty
/// list when the directory does not exist.
///
/// # Errors
///
/// Returns an error when the directory cannot be read.
pub async fn files_with_extension(path: &Path, extension: &str) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut entries = match tokio::fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(files),
        Err(error) => return Err(error),
    };
    while let Some(entry) = entries.next_entry().await? {
        let file = entry.path();
        if file.extension().and_then(|value| value.to_str()) == Some(extension) {
            files.push(file);
        }
    }
    Ok(files)
}

/// Removes a file.
///
/// # Errors
///
/// Returns an error when the file cannot be removed.
pub async fn remove_file(path: &Path) -> std::io::Result<()> {
    tokio::fs::remove_file(path).await
}

/// Returns whether a path exists.
///
/// # Errors
///
/// Returns an error when the path cannot be inspected.
pub async fn path_exists(path: &Path) -> std::io::Result<bool> {
    tokio::fs::try_exists(path).await
}

/// Ensures a directory exists and can be written by the current user.
///
/// # Errors
///
/// Returns an error when the directory cannot be created, a probe file cannot
/// be written, or the probe file cannot be removed.
pub async fn ensure_writable_directory(path: &Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(path).await?;
    let probe = path.join(format!(".agentdp-write-test-{}", std::process::id()));
    tokio::fs::write(&probe, b"agentdp doctor\n").await?;
    tokio::fs::remove_file(probe).await?;
    Ok(())
}

/// Restricts a private file so OpenSSH accepts it as key material.
///
/// # Errors
///
/// Returns an error when platform permissions cannot be updated.
#[cfg(unix)]
pub async fn restrict_private_file_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = tokio::fs::metadata(path).await?.permissions();
    permissions.set_mode(0o600);
    tokio::fs::set_permissions(path, permissions).await
}

/// Restricts a private file so OpenSSH accepts it as key material.
///
/// # Errors
///
/// Returns an error when platform permissions cannot be updated.
#[cfg(target_os = "windows")]
pub async fn restrict_private_file_permissions(path: &Path) -> std::io::Result<()> {
    use std::process::Stdio;

    let user = std::env::var("USERNAME").map_err(std::io::Error::other)?;
    let mut command = tokio::process::Command::new("icacls");
    command
        .arg(path)
        .arg("/inheritance:r")
        .arg("/grant:r")
        .arg(format!("{user}:F"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = crate::command::hide_child_window(&mut command).status().await?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "icacls failed for {} with {status}",
            path.display()
        )))
    }
}

/// Restricts a private file so OpenSSH accepts it as key material.
///
/// # Errors
///
/// This platform does not require permission changes, so this function always
/// succeeds.
#[cfg(not(any(unix, target_os = "windows")))]
pub async fn restrict_private_file_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Checks whether a local socket path is connectable.
///
/// # Errors
///
/// Returns an error when the socket path cannot be inspected.
pub async fn local_socket_status(path: &Path) -> std::io::Result<SocketStatus> {
    if !tokio::fs::try_exists(path).await? {
        return Ok(SocketStatus::Missing);
    }

    Ok(connect_for_status(path).await)
}

#[cfg(unix)]
async fn connect_for_status(path: &Path) -> SocketStatus {
    match tokio::net::UnixStream::connect(path).await {
        Ok(_) => SocketStatus::Connected,
        Err(error) => SocketStatus::Unavailable(error.to_string()),
    }
}

#[cfg(target_os = "windows")]
async fn connect_for_status(path: &Path) -> SocketStatus {
    let path = path.to_path_buf();
    match tokio::task::spawn_blocking(move || crate::windows_uds::UnixStream::connect(&path)).await {
        Ok(Ok(_)) => SocketStatus::Connected,
        Ok(Err(error)) => SocketStatus::Unavailable(error.to_string()),
        Err(error) => SocketStatus::Unavailable(error.to_string()),
    }
}

#[cfg(not(any(unix, target_os = "windows")))]
async fn connect_for_status(_path: &Path) -> SocketStatus {
    SocketStatus::Unsupported
}

/// Applies executable permissions where the host platform requires them.
///
/// # Errors
///
/// Returns an error when permissions cannot be read or updated.
#[cfg(unix)]
pub async fn set_executable(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = tokio::fs::metadata(path).await?.permissions();
    permissions.set_mode(0o755);
    tokio::fs::set_permissions(path, permissions).await
}

/// Applies executable permissions where the host platform requires them.
///
/// # Errors
///
/// This platform does not require executable permission changes, so this
/// function always succeeds.
#[cfg(not(unix))]
#[allow(clippy::unused_async)]
pub async fn set_executable(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::write_atomic;

    #[tokio::test(flavor = "current_thread")]
    async fn write_atomic_replaces_existing_file() {
        let temp = TestTemp::new("platform-write-atomic-replace");
        let path = temp.path.join("state.json");
        tokio::fs::write(&path, b"old").await.expect("write old file");

        write_atomic(&path, b"new", 0o600).await.expect("atomic write");

        assert_eq!(
            tokio::fs::read_to_string(&path).await.expect("read replaced file"),
            "new"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_atomic_creates_parent_directory() {
        let temp = TestTemp::new("platform-write-atomic-create-parent");
        let path = temp.path.join("nested").join("state.json");

        write_atomic(&path, b"created", 0o600).await.expect("atomic write");

        assert_eq!(
            tokio::fs::read_to_string(&path).await.expect("read created file"),
            "created"
        );
    }

    struct TestTemp {
        path: std::path::PathBuf,
    }

    impl TestTemp {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TestTemp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
