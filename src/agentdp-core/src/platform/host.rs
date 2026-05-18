use std::env;
use std::path::PathBuf;

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

    #[must_use]
    pub const fn is_supported_first_cut(self) -> bool {
        matches!(self, Self::Linux | Self::Wsl2)
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
    env::split_paths(&path)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
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
