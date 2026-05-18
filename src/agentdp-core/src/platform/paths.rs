use std::env;
use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum Error {
    #[error("HOME is not set; cannot resolve user-local agentdp paths")]
    MissingHome,
    #[error("LOCALAPPDATA is not set; cannot resolve user-local agentdp paths")]
    MissingLocalAppData,
    #[error("APPDATA is not set; cannot resolve user-local agentdp config path")]
    MissingAppData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformPaths {
    pub data: PathBuf,
    pub config: PathBuf,
    pub cache: PathBuf,
    pub runtime: PathBuf,
    pub logs: PathBuf,
}

impl PlatformPaths {
    /// Resolves the user-local agentdp directory set for the current host.
    ///
    /// # Errors
    ///
    /// Returns an error when required per-OS user-local path environment
    /// variables are unavailable.
    pub fn resolve() -> Result<Self, Error> {
        platform_paths()
    }

    #[must_use]
    pub fn socket_path(&self) -> PathBuf {
        self.runtime.join("agentdp-server.sock")
    }

    pub(crate) fn entries(&self) -> [(&'static str, &std::path::Path); 5] {
        [
            ("data", self.data.as_path()),
            ("config", self.config.as_path()),
            ("cache", self.cache.as_path()),
            ("runtime", self.runtime.as_path()),
            ("logs", self.logs.as_path()),
        ]
    }
}

/// Resolves the user-local directory for command-line binaries.
///
/// # Errors
///
/// Returns an error when required per-OS user-local path environment variables
/// are unavailable.
pub fn user_bin_dir() -> Result<PathBuf, Error> {
    user_bin_dir_impl()
}

#[cfg(target_os = "linux")]
fn platform_paths() -> Result<PlatformPaths, Error> {
    let home = home_dir()?;
    let state_base = xdg_path("XDG_STATE_HOME").unwrap_or_else(|| home.join(".local/state"));

    Ok(PlatformPaths {
        data: xdg_path("XDG_DATA_HOME")
            .unwrap_or_else(|| home.join(".local/share"))
            .join("agentdp"),
        config: xdg_path("XDG_CONFIG_HOME")
            .unwrap_or_else(|| home.join(".config"))
            .join("agentdp"),
        cache: xdg_path("XDG_CACHE_HOME")
            .unwrap_or_else(|| home.join(".cache"))
            .join("agentdp"),
        runtime: xdg_path("XDG_RUNTIME_DIR")
            .map_or_else(|| state_base.join("agentdp/run"), |path| path.join("agentdp")),
        logs: state_base.join("agentdp"),
    })
}

#[cfg(target_os = "linux")]
fn user_bin_dir_impl() -> Result<PathBuf, Error> {
    Ok(home_dir()?.join(".local/bin"))
}

#[cfg(target_os = "macos")]
fn platform_paths() -> Result<PlatformPaths, Error> {
    let home = home_dir()?;
    let application_support = home.join("Library/Application Support/agentdp");
    Ok(PlatformPaths {
        data: application_support.clone(),
        config: application_support.clone(),
        cache: home.join("Library/Caches/agentdp"),
        runtime: application_support.join("run"),
        logs: home.join("Library/Logs/agentdp"),
    })
}

#[cfg(target_os = "macos")]
fn user_bin_dir_impl() -> Result<PathBuf, Error> {
    Ok(home_dir()?.join(".local/bin"))
}

#[cfg(target_os = "windows")]
fn platform_paths() -> Result<PlatformPaths, Error> {
    let local_app_data = env_path("LOCALAPPDATA").ok_or(Error::MissingLocalAppData)?;
    let app_data = env_path("APPDATA").ok_or(Error::MissingAppData)?;
    Ok(PlatformPaths {
        data: local_app_data.join("agentdp"),
        config: app_data.join("agentdp"),
        cache: local_app_data.join("agentdp").join("cache"),
        runtime: local_app_data.join("agentdp").join("run"),
        logs: local_app_data.join("agentdp").join("logs"),
    })
}

#[cfg(target_os = "windows")]
fn user_bin_dir_impl() -> Result<PathBuf, Error> {
    Ok(env_path("LOCALAPPDATA")
        .ok_or(Error::MissingLocalAppData)?
        .join("agentdp/bin"))
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn platform_paths() -> Result<PlatformPaths, Error> {
    let home = home_dir()?;
    let state_base = home.join(".local/state");
    Ok(PlatformPaths {
        data: home.join(".local/share/agentdp"),
        config: home.join(".config/agentdp"),
        cache: home.join(".cache/agentdp"),
        runtime: state_base.join("agentdp/run"),
        logs: state_base.join("agentdp"),
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn user_bin_dir_impl() -> Result<PathBuf, Error> {
    Ok(home_dir()?.join(".local/bin"))
}

#[cfg(not(target_os = "windows"))]
fn home_dir() -> Result<PathBuf, Error> {
    env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or(Error::MissingHome)
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name).filter(|value| !value.is_empty()).map(PathBuf::from)
}

#[cfg(target_os = "linux")]
fn xdg_path(name: &str) -> Option<PathBuf> {
    env_path(name)
}
