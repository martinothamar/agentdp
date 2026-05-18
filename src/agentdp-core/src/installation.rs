use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::Context;
use crate::platform;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Platform(#[from] platform::Error),
    #[error("failed to resolve current executable path: {0}")]
    CurrentExecutable(std::io::Error),
    #[error("agentdp-server binary was not found beside agentctl at {path}")]
    MissingAgentdpServer { path: PathBuf },
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallationPaths {
    pub bin_dir: PathBuf,
    pub agentctl: PathBuf,
    pub agentdp_server: PathBuf,
}

impl InstallationPaths {
    /// Resolves the first-cut user-local installation paths.
    ///
    /// # Errors
    ///
    /// Returns an error when platform path resolution cannot determine the
    /// current user's home directory.
    pub fn resolve() -> Result<Self, Error> {
        let bin_dir = platform::user_bin_dir()?;
        Ok(Self {
            agentctl: bin_dir.join(agentctl_file_name()),
            agentdp_server: bin_dir.join(agentdp_server_file_name()),
            bin_dir,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallArtifact {
    pub name: &'static str,
    pub source: PathBuf,
    pub destination: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallResult {
    pub artifacts: Vec<InstallArtifact>,
}

impl InstallResult {
    #[must_use]
    pub fn agentdp_server_destination(&self) -> Option<&Path> {
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
pub fn install_current_agentctl(context: &Context) -> Result<InstallResult, Error> {
    let agentctl_source = env::current_exe().map_err(Error::CurrentExecutable)?;
    let agentdp_server_source = sibling_agentdp_server(&agentctl_source);
    if !agentdp_server_source.is_file() {
        return Err(Error::MissingAgentdpServer {
            path: agentdp_server_source,
        });
    }
    install_from_sources(context, agentctl_source, agentdp_server_source)
}

/// Installs `agentctl` and `agentdp-server` binaries from the supplied source paths.
///
/// # Errors
///
/// Returns an error when the destination directory cannot be created, the
/// binary cannot be copied, or executable permissions cannot be applied.
pub fn install_from_sources(
    context: &Context,
    agentctl_source: impl AsRef<Path>,
    agentdp_server_source: impl AsRef<Path>,
) -> Result<InstallResult, Error> {
    let paths = InstallationPaths::resolve()?;
    context
        .logger()
        .verbose_with(|| format!("resolved install bin directory {}", paths.bin_dir.display()));
    fs::create_dir_all(&paths.bin_dir).map_err(|source_error| Error::CreateDirectory {
        path: paths.bin_dir.clone(),
        source: source_error,
    })?;

    Ok(InstallResult {
        artifacts: vec![
            install_binary(context, "agentctl", agentctl_source.as_ref(), &paths.agentctl)?,
            install_binary(
                context,
                "agentdp-server",
                agentdp_server_source.as_ref(),
                &paths.agentdp_server,
            )?,
        ],
    })
}

fn install_binary(
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
        platform::set_executable(destination).map_err(|source| Error::SetPermissions {
            path: destination.to_path_buf(),
            source,
        })?;
    } else {
        let temp_destination = temp_install_path(destination);
        context
            .logger()
            .verbose_with(|| format!("copying {} to {}", source.display(), temp_destination.display()));
        fs::copy(&source, &temp_destination).map_err(|error| Error::Copy {
            source: source.clone(),
            destination: temp_destination.clone(),
            error,
        })?;
        platform::set_executable(&temp_destination).map_err(|source| Error::SetPermissions {
            path: temp_destination.clone(),
            source,
        })?;
        fs::rename(&temp_destination, destination).map_err(|error| Error::Replace {
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

fn sibling_agentdp_server(agentctl_source: &Path) -> PathBuf {
    agentctl_source
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(agentdp_server_file_name())
}

fn agentctl_file_name() -> String {
    format!("agentctl{}", std::env::consts::EXE_SUFFIX)
}

fn agentdp_server_file_name() -> String {
    format!("agentdp-server{}", std::env::consts::EXE_SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::{InstallationPaths, agentctl_file_name};

    #[test]
    fn resolves_user_local_bin_path() {
        let paths = InstallationPaths::resolve().unwrap();
        assert_eq!(paths.agentctl.file_name().unwrap(), agentctl_file_name().as_str());
        assert!(paths.agentctl.starts_with(&paths.bin_dir));
    }
}
