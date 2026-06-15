use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt as _;

const DEFAULT_WEB_PORT: u16 = 2788;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to create control-plane config directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read control-plane config {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse control-plane config {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to serialize control-plane config: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("failed to write control-plane config {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ServerConfig {
    pub web: WebConfig,
    pub tailscale: TailscaleConfig,
}

impl ServerConfig {
    #[must_use]
    pub fn conservative_default() -> Self {
        Self {
            web: WebConfig::default(),
            tailscale: TailscaleConfig::default(),
        }
    }

    #[must_use]
    pub fn from_tailscale_detection(detection: &TailscaleDetection) -> Self {
        let mut config = Self::conservative_default();
        config.tailscale.installed = detection.installed;
        config.tailscale.authenticated = detection.authenticated;
        config
            .tailscale
            .magic_dns_suffix
            .clone_from(&detection.magic_dns_suffix);
        config.tailscale.https_available = detection.https_available;
        config.tailscale.enabled = detection.installed && detection.authenticated;
        config.tailscale.expose_web =
            config.tailscale.enabled && detection.magic_dns_suffix.is_some() && detection.https_available;
        config
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::conservative_default()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct WebConfig {
    pub enabled: bool,
    pub bind_address: String,
    pub port: u16,
    pub allowed_origins: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_address: "127.0.0.1".to_owned(),
            port: DEFAULT_WEB_PORT,
            allowed_origins: vec![
                format!("http://127.0.0.1:{DEFAULT_WEB_PORT}"),
                format!("http://localhost:{DEFAULT_WEB_PORT}"),
            ],
            auth_token: None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TailscaleConfig {
    pub enabled: bool,
    pub expose_web: bool,
    pub installed: bool,
    pub authenticated: bool,
    pub https_available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub magic_dns_suffix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_host: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TailscaleDetection {
    pub installed: bool,
    pub authenticated: bool,
    pub https_available: bool,
    pub magic_dns_suffix: Option<String>,
}

#[must_use]
pub fn config_path(config_dir: &Path) -> PathBuf {
    config_dir.join("server.json")
}

/// Loads server control-plane config from `config_dir`, returning conservative defaults when absent.
///
/// # Errors
///
/// Returns an error when an existing config file cannot be read or parsed.
pub async fn load_or_default(config_dir: &Path) -> Result<ServerConfig, Error> {
    let path = config_path(config_dir);
    match tokio::fs::read_to_string(&path).await {
        Ok(contents) => serde_json::from_str(&contents).map_err(|source| Error::Parse { path, source }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(ServerConfig::default()),
        Err(source) => Err(Error::Read { path, source }),
    }
}

/// Writes a server control-plane config only when one does not already exist.
///
/// # Errors
///
/// Returns an error when the config directory cannot be created or the config cannot be written.
pub async fn write_if_missing(config_dir: &Path, config: &ServerConfig) -> Result<bool, Error> {
    let path = config_path(config_dir);
    if tokio::fs::try_exists(&path).await.map_err(|source| Error::Read {
        path: path.clone(),
        source,
    })? {
        return Ok(false);
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| Error::CreateDirectory {
                path: parent.to_path_buf(),
                source,
            })?;
    }
    let contents = serde_json::to_vec_pretty(config).map_err(Error::Serialize)?;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await
        .map_err(|source| Error::Write {
            path: path.clone(),
            source,
        })?;
    file.write_all(&contents).await.map_err(|source| Error::Write {
        path: path.clone(),
        source,
    })?;
    file.write_all(b"\n").await.map_err(|source| Error::Write {
        path: path.clone(),
        source,
    })?;
    file.flush().await.map_err(|source| Error::Write { path, source })?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::{ServerConfig, TailscaleDetection};

    #[test]
    fn conservative_default_binds_web_to_localhost() {
        let config = ServerConfig::default();

        assert!(config.web.enabled);
        assert_eq!(config.web.bind_address, "127.0.0.1");
        assert!(!config.tailscale.expose_web);
    }

    #[test]
    fn tailscale_detection_enables_exposure_only_when_https_is_available() {
        let config = ServerConfig::from_tailscale_detection(&TailscaleDetection {
            installed: true,
            authenticated: true,
            https_available: false,
            magic_dns_suffix: Some("tailnet.ts.net".to_owned()),
        });

        assert!(config.tailscale.enabled);
        assert!(!config.tailscale.expose_web);
    }
}
