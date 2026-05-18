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
#[cfg(target_os = "linux")]
pub fn host_target() -> HostTarget {
    host_target_impl()
}

#[must_use]
#[cfg(all(not(target_os = "linux"), not(target_os = "windows")))]
pub const fn host_target() -> HostTarget {
    host_target_impl()
}

#[must_use]
#[cfg(target_os = "windows")]
pub const fn host_target() -> HostTarget {
    HostTarget::Windows
}

#[must_use]
#[cfg(target_os = "linux")]
pub fn kvm_status() -> KvmStatus {
    kvm_status_impl()
}

#[must_use]
#[cfg(not(target_os = "linux"))]
pub const fn kvm_status() -> KvmStatus {
    kvm_status_impl()
}

#[must_use]
pub fn find_binary(name: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path).find_map(|directory| find_binary_in_directory(&directory, name))
}

#[cfg(windows)]
fn find_binary_in_directory(directory: &Path, name: &str) -> Option<PathBuf> {
    let direct = directory.join(name);
    if direct.is_file() {
        return Some(direct);
    }

    if Path::new(name).extension().is_some() {
        return None;
    }

    for extension in executable_extensions() {
        let candidate = directory.join(format!("{name}{extension}"));
        if candidate.is_file() {
            return Some(candidate);
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

#[cfg(not(windows))]
fn find_binary_in_directory(directory: &Path, name: &str) -> Option<PathBuf> {
    let candidate = directory.join(name);
    candidate.is_file().then_some(candidate)
}

#[cfg(target_os = "linux")]
fn host_target_impl() -> HostTarget {
    if is_wsl2() { HostTarget::Wsl2 } else { HostTarget::Linux }
}

#[cfg(all(not(target_os = "linux"), not(target_os = "windows")))]
const fn host_target_impl() -> HostTarget {
    HostTarget::Unsupported(env::consts::OS)
}

#[cfg(target_os = "linux")]
fn kvm_status_impl() -> KvmStatus {
    use std::fs::OpenOptions;
    use std::path::Path;

    let path = Path::new("/dev/kvm");
    if !path.exists() {
        return KvmStatus::Missing;
    }

    match OpenOptions::new().read(true).write(true).open(path) {
        Ok(_) => KvmStatus::Usable,
        Err(error) => KvmStatus::Unusable(error.to_string()),
    }
}

#[cfg(not(target_os = "linux"))]
const fn kvm_status_impl() -> KvmStatus {
    KvmStatus::Unsupported(env::consts::OS)
}

#[cfg(target_os = "linux")]
fn is_wsl2() -> bool {
    let os_release = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default();
    let proc_version = std::fs::read_to_string("/proc/version").unwrap_or_default();
    let joined = format!("{os_release}\n{proc_version}").to_ascii_lowercase();
    joined.contains("microsoft") || joined.contains("wsl2")
}
