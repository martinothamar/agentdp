use std::env;
use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum Error {
    #[error("HOME is not set; cannot resolve agentdp layout root")]
    MissingHome,
    #[cfg(target_os = "windows")]
    #[error("USERPROFILE is not set; cannot resolve agentdp layout root")]
    MissingUserProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentdpLayout {
    root: PathBuf,
}

impl AgentdpLayout {
    /// Resolves the user-local agentdp filesystem layout.
    ///
    /// # Errors
    ///
    /// Returns an error when the current user's home directory cannot be resolved.
    pub fn resolve() -> Result<Self, Error> {
        Ok(Self::from_root(default_root()?))
    }

    #[must_use]
    pub fn from_root(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn socket_path(&self) -> PathBuf {
        self.root.join("server.sock")
    }

    #[must_use]
    pub fn server_log(&self) -> PathBuf {
        self.root.join("server.log")
    }

    #[must_use]
    pub fn config_dir(&self) -> PathBuf {
        self.root.join("config")
    }

    #[must_use]
    pub fn cache_dir(&self) -> PathBuf {
        self.root.join("cache")
    }

    #[must_use]
    pub fn image_cache_dir(&self) -> PathBuf {
        self.cache_dir().join("images")
    }

    #[must_use]
    pub fn agents_dir(&self) -> PathBuf {
        self.root.join("agents")
    }

    #[must_use]
    pub fn writable_directories(&self) -> [(&'static str, PathBuf); 4] {
        [
            ("root", self.root.clone()),
            ("config", self.config_dir()),
            ("cache", self.cache_dir()),
            ("agents", self.agents_dir()),
        ]
    }
}

#[cfg(not(target_os = "windows"))]
fn default_root() -> Result<PathBuf, Error> {
    env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|home| home.join(".agentdp"))
        .ok_or(Error::MissingHome)
}

#[cfg(target_os = "windows")]
fn default_root() -> Result<PathBuf, Error> {
    env::var_os("USERPROFILE")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|home| home.join(".agentdp"))
        .ok_or(Error::MissingUserProfile)
}
