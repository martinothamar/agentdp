use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use agentdp_protocol::ReadinessResult;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::runtime::BackendState;

#[derive(Debug, Error)]
pub enum Error {
    #[error("instance already exists: {path}")]
    AlreadyExists { path: PathBuf },
    #[error("instance does not exist: {path}")]
    NotFound { path: PathBuf },
    #[error("failed to create instance directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to copy manifest from {source_path} to {destination_path}: {source}")]
    CopyManifest {
        source_path: PathBuf,
        destination_path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize instance state: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("failed to read instance state {path}: {source}")]
    ReadState {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse instance state {path}: {source}")]
    ParseState {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to write instance state {path}: {source}")]
    WriteState {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to remove instance directory {path}: {source}")]
    RemoveDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceFiles {
    pub instance_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub manifest: PathBuf,
    pub runtime: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceState {
    pub version: u16,
    pub manifest_name: String,
    pub instance: String,
    pub status: InstanceStatus,
    pub manifest: ManifestState,
    pub network: NetworkState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_access: Option<GuestAccessState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<ReadinessState>,
    pub backend: BackendState,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ManifestState {
    pub source: String,
    pub copy: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct NetworkState {
    pub mode: NetworkModeState,
    pub ports: BTreeMap<String, PortMappingState>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InstanceStatus {
    Created,
    Running,
    Stopped,
}

impl std::fmt::Display for InstanceStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Stopped => "stopped",
        })
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkModeState {
    User,
}

impl std::fmt::Display for NetworkModeState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::User => "user",
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PortMappingState {
    pub guest: u16,
    pub host: u16,
    pub protocol: PortProtocolState,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PortProtocolState {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuestAccessState {
    #[serde(rename = "ssh_user")]
    pub user: String,
    #[serde(rename = "ssh_private_key")]
    pub private_key: String,
    #[serde(rename = "ssh_public_key")]
    pub public_key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReadinessState {
    pub ready: bool,
    pub last_success_unix_seconds: u64,
    pub result: ReadinessResult,
}

pub(super) fn files(instance_dir: PathBuf) -> InstanceFiles {
    InstanceFiles {
        logs_dir: instance_dir.join("logs"),
        manifest: instance_dir.join("manifest.yaml"),
        runtime: instance_dir.join("runtime.json"),
        instance_dir,
    }
}

pub(super) fn build(
    manifest_path: &Path,
    manifest_name: String,
    instance: String,
    files: &InstanceFiles,
    ports: BTreeMap<String, PortMappingState>,
    guest_access: Option<GuestAccessState>,
    backend: BackendState,
) -> InstanceState {
    InstanceState {
        version: 1,
        manifest_name,
        instance,
        status: InstanceStatus::Created,
        manifest: ManifestState {
            source: path_text(manifest_path),
            copy: path_text(&files.manifest),
        },
        network: NetworkState {
            mode: NetworkModeState::User,
            ports,
        },
        guest_access,
        readiness: None,
        backend,
    }
}

pub(super) fn ensure_absent(files: &InstanceFiles) -> Result<(), Error> {
    if files.runtime.exists() {
        return Err(Error::AlreadyExists {
            path: files.runtime.clone(),
        });
    }
    Ok(())
}

pub(super) fn write(manifest_path: &Path, files: &InstanceFiles, state: &InstanceState) -> Result<(), Error> {
    fs::create_dir_all(&files.instance_dir).map_err(|source| Error::CreateDirectory {
        path: files.instance_dir.clone(),
        source,
    })?;
    fs::create_dir_all(&files.logs_dir).map_err(|source| Error::CreateDirectory {
        path: files.logs_dir.clone(),
        source,
    })?;
    fs::copy(manifest_path, &files.manifest).map_err(|source| Error::CopyManifest {
        source_path: manifest_path.to_path_buf(),
        destination_path: files.manifest.clone(),
        source,
    })?;

    write_state_file(&files.runtime, state)
}

pub(super) fn read(files: &InstanceFiles) -> Result<InstanceState, Error> {
    let contents = fs::read_to_string(&files.runtime).map_err(|source| Error::ReadState {
        path: files.runtime.clone(),
        source,
    })?;
    serde_json::from_str(&contents).map_err(|source| Error::ParseState {
        path: files.runtime.clone(),
        source,
    })
}

pub(super) fn write_runtime(files: &InstanceFiles, state: &InstanceState) -> Result<(), Error> {
    write_state_file(&files.runtime, state)
}

fn write_state_file(path: &Path, state: &InstanceState) -> Result<(), Error> {
    let temporary = path.with_extension(format!("json.{}.tmp", std::process::id()));
    let mut runtime = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)
        .map_err(|source| Error::WriteState {
            path: temporary.clone(),
            source,
        })?;
    let contents = serde_json::to_vec_pretty(state).map_err(Error::Serialize)?;
    runtime.write_all(&contents).map_err(|source| Error::WriteState {
        path: temporary.clone(),
        source,
    })?;
    runtime.write_all(b"\n").map_err(|source| Error::WriteState {
        path: temporary.clone(),
        source,
    })?;
    drop(runtime);
    fs::rename(&temporary, path).map_err(|source| Error::WriteState {
        path: path.to_path_buf(),
        source,
    })
}

pub(super) fn remove(files: &InstanceFiles) -> Result<(), Error> {
    if !files.instance_dir.exists() {
        return Err(Error::NotFound {
            path: files.instance_dir.clone(),
        });
    }
    fs::remove_dir_all(&files.instance_dir).map_err(|source| Error::RemoveDirectory {
        path: files.instance_dir.clone(),
        source,
    })
}

fn path_text(path: &Path) -> String {
    path.display().to_string()
}
