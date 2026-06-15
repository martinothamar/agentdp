use std::path::{Path, PathBuf};

use agentdp_core::Context;
use agentdp_platform as platform;
use thiserror::Error;
use tokio::process::Command;

pub const QEMU_IMG_PATH_ENV: &str = "AGENTDP_QEMU_IMG_PATH";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QemuImg {
    binary: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskCreateSpec {
    pub disk: PathBuf,
    pub backing_image: PathBuf,
    pub backing_format: String,
    pub size: String,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("qemu-img was not found; install qemu-img or set {QEMU_IMG_PATH_ENV}")]
    MissingQemuImg,
    #[error("instance disk already exists: {0}")]
    DiskExists(PathBuf),
    #[error("failed to create instance disk directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to run qemu-img: {0}")]
    Run(#[source] std::io::Error),
    #[error("qemu-img create failed: {stderr}")]
    CreateFailed { stderr: String },
}

impl QemuImg {
    #[must_use]
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self { binary: binary.into() }
    }

    /// Resolves the configured or PATH-discovered `qemu-img` executable.
    ///
    /// # Errors
    ///
    /// Returns an error when `AGENTDP_QEMU_IMG_PATH` is unset and `qemu-img`
    /// cannot be found on `PATH` or in the default Windows installation path.
    pub async fn resolve() -> Result<Self, Error> {
        if let Some(path) = std::env::var_os(QEMU_IMG_PATH_ENV).filter(|value| !value.is_empty()) {
            return Ok(Self::new(path));
        }
        let binary = platform::host::find_binary("qemu-img")
            .await
            .or(default_windows_qemu_img().await)
            .ok_or(Error::MissingQemuImg)?;
        Ok(Self::new(binary))
    }

    /// Creates a qcow2 overlay disk from a backing image.
    ///
    /// # Errors
    ///
    /// Returns an error when the target disk already exists, its parent
    /// directory cannot be created, `qemu-img` cannot be started, or
    /// `qemu-img` exits unsuccessfully.
    pub async fn create_overlay(&self, context: &Context, spec: &DiskCreateSpec) -> Result<(), Error> {
        if tokio::fs::try_exists(&spec.disk)
            .await
            .map_err(|source| Error::CreateDirectory {
                path: spec.disk.clone(),
                source,
            })?
        {
            return Err(Error::DiskExists(spec.disk.clone()));
        }
        if let Some(parent) = spec.disk.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| Error::CreateDirectory {
                    path: parent.to_path_buf(),
                    source,
                })?;
        }

        let args = create_overlay_args(spec);
        context
            .logger()
            .verbose_with(|| format!("creating QEMU instance disk {}", spec.disk.display()));
        let mut command = Command::new(&self.binary);
        command.args(&args);
        command.kill_on_drop(true);
        platform::command::hide_child_window(&mut command);
        let output = command.output().await.map_err(Error::Run)?;
        if !output.status.success() {
            return Err(Error::CreateFailed {
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        Ok(())
    }
}

fn create_overlay_args(spec: &DiskCreateSpec) -> Vec<String> {
    vec![
        "create".to_owned(),
        "-f".to_owned(),
        "qcow2".to_owned(),
        "-F".to_owned(),
        spec.backing_format.clone(),
        "-b".to_owned(),
        path_text(&spec.backing_image),
        path_text(&spec.disk),
        spec.size.clone(),
    ]
}

fn path_text(path: &Path) -> String {
    path.display().to_string()
}

async fn default_windows_qemu_img() -> Option<PathBuf> {
    if !cfg!(windows) {
        return None;
    }
    let path = PathBuf::from(r"C:\Program Files\qemu\qemu-img.exe");
    tokio::fs::metadata(&path)
        .await
        .is_ok_and(|metadata| metadata.is_file())
        .then_some(path)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{DiskCreateSpec, create_overlay_args};

    #[test]
    fn builds_qemu_img_overlay_create_args() {
        let spec = DiskCreateSpec {
            disk: PathBuf::from("/instances/altinn-studio/pr-0/disk.qcow2"),
            backing_image: PathBuf::from("/cache/images/archlinux.qcow2"),
            backing_format: "qcow2".to_owned(),
            size: "80G".to_owned(),
        };

        assert_eq!(
            create_overlay_args(&spec),
            [
                "create",
                "-f",
                "qcow2",
                "-F",
                "qcow2",
                "-b",
                "/cache/images/archlinux.qcow2",
                "/instances/altinn-studio/pr-0/disk.qcow2",
                "80G",
            ]
        );
    }
}
