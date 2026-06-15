use std::env;
use std::path::{Path, PathBuf};

use agentdp_core::{Context, control_plane, layout::AgentdpLayout};
use agentdp_platform as platform;
use thiserror::Error;

const GUEST_TOOL_DIR_ENV: &str = "AGENTDP_GUEST_TOOL_DIR";
const INSTALLED_GUEST_TOOL_DIR: &str = "agentdp-guest-tools";
const GUESTD: &str = "guestd";
const GUESTCTL: &str = "guestctl";

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(transparent)]
    UserBinDir(#[from] platform::user::UserBinDirError),
    #[error(transparent)]
    AgentdpLayout(#[from] agentdp_core::layout::Error),
    #[error("failed to resolve current executable path: {0}")]
    CurrentExecutable(std::io::Error),
    #[error("{name} binary was not found beside agentctl at {path}")]
    MissingSiblingBinary { name: &'static str, path: PathBuf },
    #[error(
        "Linux guest tool binary {name} was not found at {path}; build extensionless Linux guest tools or set AGENTDP_GUEST_TOOL_DIR to a directory containing guestd and guestctl"
    )]
    MissingGuestTool { name: &'static str, path: PathBuf },
    #[error("failed to create install directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to copy {source} to {destination}: {error}")]
    Copy {
        source: PathBuf,
        destination: PathBuf,
        #[source]
        error: std::io::Error,
    },
    #[error("failed to replace {destination} with {source}: {error}")]
    Replace {
        source: PathBuf,
        destination: PathBuf,
        #[source]
        error: std::io::Error,
    },
    #[error("failed to set executable permissions on {path}: {source}")]
    SetPermissions {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    ControlPlaneConfig(#[from] control_plane::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstallationPaths {
    pub bin_dir: PathBuf,
    pub guest_tool_dir: PathBuf,
    pub agentctl: PathBuf,
    pub agentdp_server: PathBuf,
    pub guestd: PathBuf,
    pub guestctl: PathBuf,
}

impl InstallationPaths {
    /// Resolves the first-cut user-local installation paths.
    ///
    /// # Errors
    ///
    /// Returns an error when platform path resolution cannot determine the
    /// current user's home directory.
    pub(crate) fn resolve() -> Result<Self, Error> {
        let bin_dir = platform::user::user_bin_dir()?;
        let guest_tool_dir = bin_dir.join(INSTALLED_GUEST_TOOL_DIR);
        Ok(Self {
            agentctl: bin_dir.join(agentctl_file_name()),
            agentdp_server: bin_dir.join(agentdp_server_file_name()),
            guestd: guest_tool_dir.join(GUESTD),
            guestctl: guest_tool_dir.join(GUESTCTL),
            guest_tool_dir,
            bin_dir,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstallArtifact {
    pub name: &'static str,
    pub source: PathBuf,
    pub destination: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstallResult {
    pub artifacts: Vec<InstallArtifact>,
}

impl InstallResult {
    #[must_use]
    pub(crate) fn agentdp_server_destination(&self) -> Option<&Path> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.name == "agentdp-server")
            .map(|artifact| artifact.destination.as_path())
    }
}

/// Installs the currently running `agentctl` executable.
///
/// # Errors
///
/// Returns an error when the current executable cannot be resolved or the
/// executable cannot be copied into the user-local bin directory.
pub(crate) async fn install_current_agentctl(context: &Context) -> Result<InstallResult, Error> {
    let agentctl_source = env::current_exe().map_err(Error::CurrentExecutable)?;
    let agentdp_server_source = sibling_binary(&agentctl_source, &agentdp_server_file_name());
    let guestd_source = guest_tool_source(&agentctl_source, GUESTD);
    let guestctl_source = guest_tool_source(&agentctl_source, GUESTCTL);
    if !tokio::fs::try_exists(&agentdp_server_source).await.unwrap_or(false) {
        return Err(Error::MissingSiblingBinary {
            name: "agentdp-server",
            path: agentdp_server_source,
        });
    }
    for (name, source) in [(GUESTD, &guestd_source), (GUESTCTL, &guestctl_source)] {
        if !tokio::fs::try_exists(source).await.unwrap_or(false) {
            return Err(Error::MissingGuestTool {
                name,
                path: source.clone(),
            });
        }
    }
    install_from_sources(
        context,
        agentctl_source,
        agentdp_server_source,
        guestd_source,
        guestctl_source,
    )
    .await
}

/// Installs agentdp host and guest helper binaries from the supplied source paths.
///
/// # Errors
///
/// Returns an error when the destination directory cannot be created, the
/// binary cannot be copied, or executable permissions cannot be applied.
pub(crate) async fn install_from_sources(
    context: &Context,
    agentctl_source: impl AsRef<Path>,
    agentdp_server_source: impl AsRef<Path>,
    guestd_source: impl AsRef<Path>,
    guestctl_source: impl AsRef<Path>,
) -> Result<InstallResult, Error> {
    let paths = InstallationPaths::resolve()?;
    context
        .logger()
        .verbose_with(|| format!("resolved install bin directory {}", paths.bin_dir.display()));
    tokio::fs::create_dir_all(&paths.bin_dir)
        .await
        .map_err(|source_error| Error::CreateDirectory {
            path: paths.bin_dir.clone(),
            source: source_error,
        })?;
    tokio::fs::create_dir_all(&paths.guest_tool_dir)
        .await
        .map_err(|source_error| Error::CreateDirectory {
            path: paths.guest_tool_dir.clone(),
            source: source_error,
        })?;
    write_default_control_plane_config(context).await?;

    Ok(InstallResult {
        artifacts: vec![
            install_binary(context, "agentctl", agentctl_source.as_ref(), &paths.agentctl).await?,
            install_binary(
                context,
                "agentdp-server",
                agentdp_server_source.as_ref(),
                &paths.agentdp_server,
            )
            .await?,
            install_binary(context, "guestd", guestd_source.as_ref(), &paths.guestd).await?,
            install_binary(context, "guestctl", guestctl_source.as_ref(), &paths.guestctl).await?,
        ],
    })
}

async fn write_default_control_plane_config(context: &Context) -> Result<(), Error> {
    let layout = AgentdpLayout::resolve()?;
    let detection = detect_tailscale(context).await;
    let config = control_plane::ServerConfig::from_tailscale_detection(&detection);
    let config_dir = layout.config_dir();
    let created = control_plane::write_if_missing(&config_dir, &config).await?;
    if created {
        context.logger().verbose_with(|| {
            format!(
                "wrote default control-plane config {}",
                control_plane::config_path(&config_dir).display()
            )
        });
    }
    Ok(())
}

async fn detect_tailscale(context: &Context) -> control_plane::TailscaleDetection {
    let Ok(version) = tokio::process::Command::new("tailscale").arg("version").output().await else {
        return control_plane::TailscaleDetection::default();
    };
    let mut detection = control_plane::TailscaleDetection {
        installed: version.status.success(),
        ..control_plane::TailscaleDetection::default()
    };
    if !detection.installed {
        return detection;
    }

    match tokio::process::Command::new("tailscale")
        .args(["status", "--json"])
        .output()
        .await
    {
        Ok(output) if output.status.success() => {
            parse_tailscale_status(&output.stdout, &mut detection);
        }
        Ok(output) => {
            context.logger().verbose_with(|| {
                format!(
                    "tailscale status --json exited with status {} while detecting control-plane defaults",
                    output.status
                )
            });
        }
        Err(error) => {
            context.logger().verbose_with(|| {
                format!("failed to run tailscale status --json while detecting control-plane defaults: {error}")
            });
        }
    }
    detection
}

fn parse_tailscale_status(stdout: &[u8], detection: &mut control_plane::TailscaleDetection) {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(stdout) else {
        return;
    };
    detection.authenticated = value
        .get("Self")
        .and_then(|self_status| self_status.get("Online"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || value
            .get("Self")
            .and_then(|self_status| self_status.get("ID"))
            .is_some();
    detection.magic_dns_suffix = value
        .get("MagicDNSSuffix")
        .and_then(serde_json::Value::as_str)
        .filter(|suffix| !suffix.is_empty())
        .map(ToOwned::to_owned);
    detection.https_available = false;
}

async fn install_binary(
    context: &Context,
    name: &'static str,
    source: &Path,
    destination: &Path,
) -> Result<InstallArtifact, Error> {
    context
        .logger()
        .verbose_with(|| format!("installing {name} to {}", destination.display()));
    let source = source.canonicalize().unwrap_or_else(|_| source.to_path_buf());
    if source == destination {
        context.logger().verbose_with(|| {
            format!(
                "source {} is already the install destination; skipping copy",
                source.display()
            )
        });
        platform::fs::set_executable(destination)
            .await
            .map_err(|source| Error::SetPermissions {
                path: destination.to_path_buf(),
                source,
            })?;
    } else {
        let temp_destination = temp_install_path(destination);
        context
            .logger()
            .verbose_with(|| format!("copying {} to {}", source.display(), temp_destination.display()));
        tokio::fs::copy(&source, &temp_destination)
            .await
            .map_err(|error| Error::Copy {
                source: source.clone(),
                destination: temp_destination.clone(),
                error,
            })?;
        platform::fs::set_executable(&temp_destination)
            .await
            .map_err(|source| Error::SetPermissions {
                path: temp_destination.clone(),
                source,
            })?;
        tokio::fs::rename(&temp_destination, destination)
            .await
            .map_err(|error| Error::Replace {
                source: temp_destination,
                destination: destination.to_path_buf(),
                error,
            })?;
    }

    context
        .logger()
        .verbose_with(|| format!("installed executable permissions on {}", destination.display()));

    Ok(InstallArtifact {
        name,
        source,
        destination: destination.to_path_buf(),
    })
}

fn temp_install_path(destination: &Path) -> PathBuf {
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("agentdp-install");
    destination.with_file_name(format!(".{file_name}.install-{}.tmp", std::process::id()))
}

fn sibling_binary(agentctl_source: &Path, file_name: &str) -> PathBuf {
    agentctl_source
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(file_name)
}

fn guest_tool_source(agentctl_source: &Path, file_name: &str) -> PathBuf {
    std::env::var_os(GUEST_TOOL_DIR_ENV)
        .filter(|value| !value.is_empty())
        .map_or_else(
            || sibling_binary(agentctl_source, file_name),
            |dir| PathBuf::from(dir).join(file_name),
        )
}

fn agentctl_file_name() -> String {
    format!("agentctl{}", std::env::consts::EXE_SUFFIX)
}

fn agentdp_server_file_name() -> String {
    format!("agentdp-server{}", std::env::consts::EXE_SUFFIX)
}

#[cfg(test)]
mod tests {
    use agentdp_core::control_plane::TailscaleDetection;

    use super::{InstallationPaths, agentctl_file_name, parse_tailscale_status};

    #[test]
    fn resolves_user_local_bin_path() {
        let paths = InstallationPaths::resolve().unwrap();
        assert_eq!(paths.agentctl.file_name().unwrap(), agentctl_file_name().as_str());
        assert!(paths.agentctl.starts_with(&paths.bin_dir));
    }

    #[test]
    fn parses_tailscale_status_for_install_defaults() {
        let mut detection = TailscaleDetection {
            installed: true,
            ..TailscaleDetection::default()
        };

        parse_tailscale_status(
            br#"{"Self":{"ID":"node-id","Online":true},"MagicDNSSuffix":"example.ts.net"}"#,
            &mut detection,
        );

        assert!(detection.authenticated);
        assert_eq!(detection.magic_dns_suffix.as_deref(), Some("example.ts.net"));
        assert!(!detection.https_available);
    }
}
