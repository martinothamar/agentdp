use std::path::PathBuf;
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::backend::BackendKind;
use agentdp_core::doctor::DoctorReport;
use agentdp_core::manifest::AgentManifest;
use agentdp_core::platform::ssh::CommandOutput;
use agentdp_core::platform::{self, PlatformPaths, ProcessStatus};
use agentdp_protocol::{
    BackendCreateResult, BackendRuntimeResult, BackendStatusResult, HostCommandResult, LogFile, ProcessResult,
    ProvisioningPlanResult,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::instance::state::{GuestAccessState, InstanceFiles, InstanceState, InstanceStatus};
use crate::progress::Progress;
use crate::qemu;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Qemu(#[from] qemu::runtime::Error),
}

impl Error {
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::Qemu(error) => error.is_retryable(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Qemu,
}

impl Backend {
    #[must_use]
    pub const fn from_kind(kind: BackendKind) -> Self {
        match kind {
            BackendKind::Qemu => Self::Qemu,
        }
    }

    #[must_use]
    pub const fn for_manifest(manifest: &AgentManifest) -> Self {
        Self::from_kind(BackendKind::for_manifest(manifest))
    }

    #[must_use]
    pub const fn from_state(state: &BackendState) -> Self {
        Self::from_kind(state.kind())
    }

    pub fn check_prerequisites(self, context: &Context, report: &mut DoctorReport) {
        match self {
            Self::Qemu => qemu::doctor::check_prerequisites(context, report),
        }
    }

    pub fn ensure_absent(self, files: &InstanceFiles) -> Result<(), Error> {
        match self {
            Self::Qemu => qemu::runtime::ensure_absent(files).map_err(Error::Qemu),
        }
    }

    pub fn plan(
        self,
        context: &Context,
        manifest_path: PathBuf,
        manifest: AgentManifest,
        instance: String,
        paths: &PlatformPaths,
    ) -> Result<ProvisioningPlanResult, Error> {
        match self {
            Self::Qemu => qemu::runtime::plan(context, manifest_path, manifest, instance, paths).map_err(Error::Qemu),
        }
    }

    pub fn create(self, context: &Context, input: CreateInput) -> Result<CreateOutput, Error> {
        match self {
            Self::Qemu => qemu::runtime::QemuCreateBackend::resolve()?
                .create(context, input)
                .map_err(Error::Qemu),
        }
    }

    pub fn create_details(self, backend_state: &BackendState) -> BackendCreateResult {
        match (self, backend_state) {
            (Self::Qemu, BackendState::Qemu(qemu_state)) => qemu::runtime::create_details(qemu_state),
        }
    }

    #[must_use]
    pub fn clone_state(
        self,
        source_state: &BackendState,
        files: &InstanceFiles,
        paths: &PlatformPaths,
        manifest_name: &str,
        instance: &str,
    ) -> BackendState {
        match (self, source_state) {
            (Self::Qemu, BackendState::Qemu(qemu_state)) => BackendState::Qemu(qemu::runtime::clone_state(
                qemu_state,
                files,
                paths,
                manifest_name,
                instance,
            )),
        }
    }

    pub fn start(
        self,
        context: &Context,
        manifest: &AgentManifest,
        state: &mut InstanceState,
    ) -> Result<StartOutput, Error> {
        let manifest_name = state.manifest_name.clone();
        let instance = state.instance.clone();
        let network = state.network.clone();
        match (self, &mut state.backend) {
            (Self::Qemu, BackendState::Qemu(qemu_state)) => qemu::runtime::QemuRuntimeBackend::resolve()?
                .start(context, manifest, &manifest_name, &instance, &network, qemu_state)
                .map_err(Error::Qemu),
        }
    }

    pub fn run_readiness_command(
        self,
        context: &Context,
        state: &InstanceState,
        command: &str,
        timeout: Duration,
    ) -> Result<CommandOutput, Error> {
        match (self, &state.backend) {
            (Self::Qemu, BackendState::Qemu(_)) => {
                qemu::runtime::run_readiness_command(context, state, command, timeout).map_err(Error::Qemu)
            }
        }
    }

    pub fn wait_provisioned(
        self,
        context: &Context,
        state: &InstanceState,
        progress: &mut dyn Progress,
    ) -> Result<(), Error> {
        match (self, &state.backend) {
            (Self::Qemu, BackendState::Qemu(_)) => {
                qemu::runtime::wait_provisioned(context, state, progress).map_err(Error::Qemu)
            }
        }
    }

    pub fn down_with_process_control(
        self,
        context: &Context,
        input: DownInput<'_>,
        backend_state: &mut BackendState,
        process_status: impl FnMut(u32) -> Result<ProcessStatus, platform::ProcessStatusError>,
        terminate: impl FnMut(u32) -> Result<(), platform::TerminateProcessError>,
        wait_for_exit: impl FnMut(u32) -> Result<bool, platform::ProcessStatusError>,
    ) -> Result<DownOutput, Error> {
        match (self, backend_state) {
            (Self::Qemu, BackendState::Qemu(qemu_state)) => qemu::runtime::down_with_process_control(
                context,
                input,
                qemu_state,
                process_status,
                terminate,
                wait_for_exit,
            )
            .map_err(Error::Qemu),
        }
    }

    pub fn status_with_process_status(
        self,
        status: InstanceStatus,
        backend_state: &BackendState,
        process_status: impl FnMut(u32) -> Result<ProcessStatus, platform::ProcessStatusError>,
    ) -> StatusOutput {
        match (self, backend_state) {
            (Self::Qemu, BackendState::Qemu(qemu_state)) => {
                qemu::runtime::status_with_process_status(status, qemu_state, process_status)
            }
        }
    }

    pub const fn runtime_summary(self, backend_state: &BackendState) -> RuntimeSummary {
        match (self, backend_state) {
            (Self::Qemu, BackendState::Qemu(qemu_state)) => qemu::runtime::runtime_summary(qemu_state),
        }
    }

    pub fn runtime_details(self, backend_state: &BackendState) -> BackendRuntimeResult {
        match (self, backend_state) {
            (Self::Qemu, BackendState::Qemu(qemu_state)) => qemu::runtime::runtime_details(qemu_state),
        }
    }

    pub fn log_path(self, backend_state: &BackendState, file: LogFile) -> PathBuf {
        match (self, backend_state) {
            (Self::Qemu, BackendState::Qemu(qemu_state)) => qemu::runtime::log_path(qemu_state, file),
        }
    }

    pub fn shell_command(self, state: &InstanceState) -> Result<HostCommandResult, Error> {
        match (self, &state.backend) {
            (Self::Qemu, BackendState::Qemu(_)) => qemu::runtime::shell_command(state).map_err(Error::Qemu),
        }
    }

    pub fn run_user_command(
        self,
        context: &Context,
        state: &InstanceState,
        command: &[String],
        timeout: Duration,
    ) -> Result<CommandOutput, Error> {
        match (self, &state.backend) {
            (Self::Qemu, BackendState::Qemu(_)) => {
                qemu::runtime::run_user_command(context, state, command, timeout).map_err(Error::Qemu)
            }
        }
    }

    pub fn cleanup(self, backend_state: &BackendState) -> Result<(), Error> {
        match (self, backend_state) {
            (Self::Qemu, BackendState::Qemu(qemu_state)) => {
                qemu::runtime::cleanup_runtime_files(qemu_state).map_err(Error::Qemu)
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum BackendState {
    #[serde(rename = "qemu")]
    Qemu(qemu::runtime::State),
}

impl BackendState {
    #[must_use]
    pub const fn kind(&self) -> BackendKind {
        match self {
            Self::Qemu(_) => BackendKind::Qemu,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub const fn qemu(&self) -> &qemu::runtime::State {
        match self {
            Self::Qemu(state) => state,
        }
    }

    #[cfg(test)]
    pub const fn qemu_mut(&mut self) -> &mut qemu::runtime::State {
        match self {
            Self::Qemu(state) => state,
        }
    }
}

pub struct CreateInput<'a> {
    pub manifest_path: PathBuf,
    pub manifest: AgentManifest,
    pub instance: String,
    pub paths: &'a PlatformPaths,
    pub files: &'a InstanceFiles,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CreateOutput {
    pub state: BackendState,
    pub guest_access: Option<GuestAccessState>,
    pub details: BackendCreateResult,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StartOutput {
    pub process: ProcessResult,
    pub details: BackendRuntimeResult,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct DownInput<'a> {
    pub name: &'a str,
    pub status: InstanceStatus,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DownOutput {
    pub process_status: &'static str,
    pub terminated_pid: Option<u32>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StatusOutput {
    pub stale: bool,
    pub process: ProcessResult,
    pub details: BackendStatusResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeSummary {
    pub pid: Option<u32>,
}
