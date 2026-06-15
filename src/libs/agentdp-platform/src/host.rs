use std::env;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostTarget {
    Linux,
    Wsl2,
    Windows,
    Unsupported(&'static str),
}

impl HostTarget {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Linux => "Linux",
            Self::Wsl2 => "WSL2",
            Self::Windows => "windows",
            Self::Unsupported(name) => name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvmStatus {
    Usable,
    Missing,
    Unusable(String),
    Unsupported(&'static str),
}

#[must_use]
pub async fn host_target() -> HostTarget {
    host_target_impl().await
}

#[cfg(target_os = "linux")]
pub async fn kvm_status() -> KvmStatus {
    kvm_status_impl().await
}

#[cfg(not(target_os = "linux"))]
#[allow(clippy::unused_async)]
pub async fn kvm_status() -> KvmStatus {
    KvmStatus::Unsupported(env::consts::OS)
}

pub async fn find_binary(name: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    for directory in env::split_paths(&path) {
        if let Some(binary) = find_binary_in_directory(&directory, name).await {
            return Some(binary);
        }
    }
    None
}

#[cfg(windows)]
fn executable_extensions() -> Vec<String> {
    env::var_os("PATHEXT")
        .map(|extensions| {
            env::split_paths(&extensions)
                .map(|extension| extension.display().to_string())
                .collect()
        })
        .filter(|extensions: &Vec<String>| !extensions.is_empty())
        .unwrap_or_else(|| {
            vec![
                ".COM".to_owned(),
                ".EXE".to_owned(),
                ".BAT".to_owned(),
                ".CMD".to_owned(),
            ]
        })
}

#[cfg(windows)]
async fn find_binary_in_directory(directory: &Path, name: &str) -> Option<PathBuf> {
    let direct = directory.join(name);
    if is_file(&direct).await {
        return Some(direct);
    }

    if Path::new(name).extension().is_some() {
        return None;
    }

    for extension in executable_extensions() {
        let candidate = directory.join(format!("{name}{extension}"));
        if is_file(&candidate).await {
            return Some(candidate);
        }
    }
    None
}

#[cfg(not(windows))]
async fn find_binary_in_directory(directory: &Path, name: &str) -> Option<PathBuf> {
    let candidate = directory.join(name);
    is_file(&candidate).await.then_some(candidate)
}

async fn is_file(path: &Path) -> bool {
    tokio::fs::metadata(path).await.is_ok_and(|metadata| metadata.is_file())
}

#[cfg(target_os = "linux")]
async fn host_target_impl() -> HostTarget {
    if is_wsl2().await {
        HostTarget::Wsl2
    } else {
        HostTarget::Linux
    }
}

#[cfg(all(not(target_os = "linux"), not(target_os = "windows")))]
async fn host_target_impl() -> HostTarget {
    HostTarget::Unsupported(env::consts::OS)
}

#[cfg(target_os = "windows")]
#[allow(clippy::unused_async)]
async fn host_target_impl() -> HostTarget {
    HostTarget::Windows
}

#[cfg(target_os = "linux")]
async fn kvm_status_impl() -> KvmStatus {
    use std::path::Path;

    let path = Path::new("/dev/kvm");
    match tokio::fs::try_exists(path).await {
        Ok(true) => {}
        Ok(false) => return KvmStatus::Missing,
        Err(error) => return KvmStatus::Unusable(error.to_string()),
    }

    match tokio::fs::OpenOptions::new().read(true).write(true).open(path).await {
        Ok(_) => KvmStatus::Usable,
        Err(error) => KvmStatus::Unusable(error.to_string()),
    }
}

#[cfg(target_os = "linux")]
async fn is_wsl2() -> bool {
    let os_release = tokio::fs::read_to_string("/proc/sys/kernel/osrelease")
        .await
        .unwrap_or_default();
    let proc_version = tokio::fs::read_to_string("/proc/version").await.unwrap_or_default();
    let joined = format!("{os_release}\n{proc_version}").to_ascii_lowercase();
    joined.contains("microsoft") || joined.contains("wsl2")
}
