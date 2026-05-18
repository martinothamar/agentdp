use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use agentdp_core::Context;
use agentdp_core::platform;
use thiserror::Error;

pub(super) const QEMU_IMG_PATH_ENV: &str = "AGENTDP_QEMU_IMG_PATH";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QemuImg {
    binary: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DiskCreateSpec {
    pub(super) disk: PathBuf,
    pub(super) backing_image: PathBuf,
    pub(super) backing_format: String,
    pub(super) size: String,
}

#[derive(Debug, Error)]
pub(super) enum Error {
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
    pub(super) fn new(binary: impl Into<PathBuf>) -> Self {
        Self { binary: binary.into() }
    }

    pub(super) fn resolve() -> Result<Self, Error> {
        if let Some(path) = std::env::var_os(QEMU_IMG_PATH_ENV).filter(|value| !value.is_empty()) {
            return Ok(Self::new(path));
        }
        let binary = platform::find_binary("qemu-img")
            .or_else(default_windows_qemu_img)
            .ok_or(Error::MissingQemuImg)?;
        Ok(Self::new(binary))
    }

    pub(super) fn create_overlay(&self, context: &Context, spec: &DiskCreateSpec) -> Result<(), Error> {
        if spec.disk.exists() {
            return Err(Error::DiskExists(spec.disk.clone()));
        }
        if let Some(parent) = spec.disk.parent() {
            fs::create_dir_all(parent).map_err(|source| Error::CreateDirectory {
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
        let output = platform::hide_child_window(&mut command).output().map_err(Error::Run)?;
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

fn default_windows_qemu_img() -> Option<PathBuf> {
    cfg!(windows)
        .then(|| PathBuf::from(r"C:\Program Files\qemu\qemu-img.exe"))
        .filter(|path| path.is_file())
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
