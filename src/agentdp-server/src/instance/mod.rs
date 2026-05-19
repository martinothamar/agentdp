use std::path::{Path, PathBuf};

use agentdp_core::manifest::{self, AgentManifest};
use agentdp_core::platform;
use agentdp_protocol::{
    GuestAccessResult, ManifestResult, NetworkResult, PortMappingResult, PortProtocolResult, ReadinessStateResult,
};
use thiserror::Error;

use crate::runtime;

mod clone;
mod create;
mod down;
mod exec;
mod loader;
mod lock;
mod logs;
mod ports;
pub mod provisioning;
pub mod ps;
mod readiness;
mod rm;
pub mod state;
mod status;
mod up;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Provisioning(#[from] provisioning::Error),
    #[error("{0}")]
    Manifest(#[from] manifest::Error),
    #[error("{0}")]
    State(#[from] state::Error),
    #[error("{0}")]
    Lock(#[from] lock::Error),
    #[error("{0}")]
    Ports(#[from] ports::Error),
    #[error("{0}")]
    Backend(#[from] runtime::Error),
    #[error("{0}")]
    Readiness(#[from] readiness::Error),
    #[error("instance {name} cannot transition from status {status}")]
    InvalidStatus { name: String, status: String },
    #[error("instance {name} is {status}; run `agentctl up {instance}` before connecting")]
    NotRunningForSsh {
        name: String,
        instance: String,
        status: String,
    },
    #[error("instance {name} is running; run `agentctl down {instance}` before removing it")]
    RemoveRunning { name: String, instance: String },
    #[error("source and target instance names must be different: {instance}")]
    CloneSameInstance { instance: String },
    #[error("instance {name} is running; run `agentctl down {instance}` before cloning it")]
    CloneRunning { name: String, instance: String },
    #[error("failed to copy instance directory from {source_path} to {destination_path}: {source}")]
    CopyInstanceDirectory {
        source_path: PathBuf,
        destination_path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to copy instance file from {source_path} to {destination_path}: {source}")]
    CopyInstanceFile {
        source_path: PathBuf,
        destination_path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to restrict cloned SSH private key permissions {path}: {source}")]
    RestrictClonedPrivateKeyPermissions {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    Terminate(#[from] platform::TerminateProcessError),
    #[error("{0}")]
    ProcessStatus(#[from] platform::ProcessStatusError),
    #[error("log line count must be greater than zero")]
    InvalidLogLines,
    #[error("exec command must not be empty")]
    EmptyExecCommand,
    #[error("exec timeout must be greater than zero")]
    InvalidExecTimeout,
    #[error("failed to read instance directory {path}: {source}")]
    ReadInstanceDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read log file {path}: {source}")]
    ReadLog {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub struct Instance {
    manifest: AgentManifest,
    files: state::InstanceFiles,
    state: state::InstanceState,
}

impl Instance {
    fn name(&self) -> String {
        format!("{}/{}", self.state.manifest_name, self.state.instance)
    }

    const fn backend(&self) -> runtime::Backend {
        runtime::Backend::from_state(&self.state.backend)
    }

    fn runtime_path(&self) -> String {
        path_text(&self.files.runtime)
    }

    fn write_runtime(&self) -> Result<(), Error> {
        state::write_runtime(&self.files, &self.state).map_err(Error::State)
    }

    fn acquire_lock(&self) -> Result<lock::InstanceLock, Error> {
        lock::InstanceLock::acquire(&self.files.instance_dir).map_err(Error::Lock)
    }

    fn reload_state(&mut self) -> Result<(), Error> {
        self.state = state::read(&self.files)?;
        Ok(())
    }

    fn ensure_running_for_ssh(&self) -> Result<(), Error> {
        if self.state.status == state::InstanceStatus::Running {
            return Ok(());
        }
        Err(Error::NotRunningForSsh {
            name: self.name(),
            instance: self.state.instance.clone(),
            status: self.state.status.to_string(),
        })
    }
}

fn path_text(path: &Path) -> String {
    path.display().to_string()
}

fn manifest_result(state: &state::InstanceState) -> ManifestResult {
    ManifestResult {
        source: state.manifest.source.clone(),
        copy: state.manifest.copy.clone(),
    }
}

fn network_result(network: &state::NetworkState) -> NetworkResult {
    NetworkResult {
        mode: network.mode.to_string(),
        ports: network
            .ports
            .iter()
            .map(|(name, port)| {
                (
                    name.clone(),
                    PortMappingResult {
                        guest: port.guest,
                        host: port.host,
                        protocol: match port.protocol {
                            state::PortProtocolState::Tcp => PortProtocolResult::Tcp,
                            state::PortProtocolState::Udp => PortProtocolResult::Udp,
                        },
                    },
                )
            })
            .collect(),
    }
}

fn guest_access_result(access: Option<&state::GuestAccessState>) -> GuestAccessResult {
    GuestAccessResult {
        ssh_user: access.map(|access| access.user.clone()),
        ssh_private_key: access.map(|access| access.private_key.clone()),
        ssh_public_key: access.map(|access| access.public_key.clone()),
    }
}

fn readiness_result(readiness: &state::ReadinessState) -> ReadinessStateResult {
    ReadinessStateResult {
        ready: readiness.ready,
        last_success_unix_seconds: readiness.last_success_unix_seconds,
        result: readiness.result.clone(),
    }
}

#[cfg(test)]
mod tests;
