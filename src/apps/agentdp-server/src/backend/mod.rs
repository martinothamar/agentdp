use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::agent::{
    AgentInstanceCredentialState, AgentInstanceDocument, AgentInstancePhase, BackendState, BootstrapEvent,
    GuestAccessState, PortMappingState, ProcessStatus,
};
use agentdp_core::doctor::DoctorReport;
use agentdp_core::manifest::AgentManifest;
use agentdp_core::provisioning::bootstrap::RenderedBootstrapPlan;
use agentdp_core::provisioning::image::{CatalogImage, ImageCatalog, ImageRequest};
use agentdp_core::provisioning::{ProvisioningPlan, SeedFile};
use agentdp_ds::local::spsc;
use agentdp_platform::ssh::{CommandOutput, OutputSink};
use agentdp_protocol::client_server::{BackendKind, HostCommandResult, LogFile};
use serde::Serialize;
use thiserror::Error;

use crate::agent::AgentManifestContext;
use crate::agent::{AgentBaseFiles, AgentBaseKey, AgentInstanceFiles, AgentName, InstanceName};
use crate::qemu;
use crate::services::InstanceNetwork;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Qemu(#[from] qemu::Error),
    #[error("no backend supports image {os} {architecture} {variant}")]
    UnsupportedManifestImage {
        os: &'static str,
        architecture: &'static str,
        variant: &'static str,
    },
}

pub(crate) type BackendRef = Rc<dyn Backend>;
pub(crate) type InstanceControl = qemu::InstanceControl;
pub(crate) type BackendFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + 'a>>;
pub(crate) type BackendValueFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub(crate) trait BootstrapEventSink {
    fn emit(&mut self, event: BootstrapEvent);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BootstrapOutcome {
    Passed { attempt_epoch: u64 },
    Failed { attempt_epoch: u64, error: String },
}

impl BootstrapOutcome {
    pub(crate) const fn attempt_epoch(&self) -> u64 {
        match self {
            Self::Passed { attempt_epoch } | Self::Failed { attempt_epoch, .. } => *attempt_epoch,
        }
    }
}

impl BootstrapEventSink for spsc::Sender<BootstrapEvent> {
    fn emit(&mut self, event: BootstrapEvent) {
        let _sent = self.try_send(event);
    }
}

pub(crate) trait Backend: fmt::Debug {
    fn supports_image(&self, image: CatalogImage) -> bool;

    fn check_prerequisites<'a>(
        &'a self,
        context: &'a Context,
        report: &'a mut DoctorReport,
    ) -> BackendValueFuture<'a, ()>;

    fn base_image_identity<'a>(&'a self, manifest: &'a AgentManifest) -> BackendFuture<'a, BackendBaseImageIdentity>;

    fn create_base<'a>(
        &'a self,
        context: &'a Context,
        input: CreateBaseInput<'a>,
    ) -> BackendFuture<'a, CreateBaseOutput>;

    fn start_base<'a>(
        &'a self,
        context: &'a Context,
        manifest: &'a AgentManifestContext,
        state: &'a mut AgentInstanceDocument,
    ) -> BackendFuture<'a, StartOutput>;

    fn stop_base<'a>(
        &'a self,
        context: &'a Context,
        state: &'a mut AgentInstanceDocument,
        control: &'a mut Option<InstanceControl>,
    ) -> BackendFuture<'a, StopOutput>;

    fn stop_base_runtime<'a>(
        &'a self,
        context: &'a Context,
        agent: &'a AgentName,
        key: &'a AgentBaseKey,
        files: &'a AgentBaseFiles,
    ) -> BackendFuture<'a, StopOutput>;

    fn create_instance<'a>(
        &'a self,
        context: &'a Context,
        input: CreateInstanceInput<'a>,
    ) -> BackendFuture<'a, CreateInstanceOutput>;

    fn start_instance<'a>(
        &'a self,
        context: &'a Context,
        network: &'a InstanceNetwork,
        manifest: &'a AgentManifestContext,
        state: &'a mut AgentInstanceDocument,
    ) -> BackendFuture<'a, StartOutput>;

    fn exec<'a>(
        &'a self,
        context: &'a Context,
        state: &'a AgentInstanceDocument,
        command: &'a str,
        timeout: Duration,
        output: &'a mut dyn OutputSink,
    ) -> BackendFuture<'a, CommandOutput>;

    fn wait_bootstrap<'a>(
        &'a self,
        context: &'a Context,
        state: &'a AgentInstanceDocument,
        control: &'a mut Option<InstanceControl>,
        retry_epoch: Option<u64>,
        bootstrap_events: Option<&'a mut dyn BootstrapEventSink>,
    ) -> BackendFuture<'a, BootstrapOutcome>;

    fn stop_instance<'a>(
        &'a self,
        context: &'a Context,
        network: &'a InstanceNetwork,
        input: StopInstanceInput<'a>,
        backend_state: &'a mut BackendState,
        control: &'a mut Option<InstanceControl>,
    ) -> BackendFuture<'a, StopOutput>;

    fn reconcile_instance<'a>(
        &'a self,
        context: &'a Context,
        network: &'a InstanceNetwork,
        manifest: &'a AgentManifestContext,
        state: &'a mut AgentInstanceDocument,
    ) -> BackendFuture<'a, ReconcileOutput>;

    fn reconcile_runtime_secrets<'a>(
        &'a self,
        context: &'a Context,
        network: &'a InstanceNetwork,
        manifest: &'a AgentManifestContext,
        state: &'a mut AgentInstanceDocument,
    ) -> BackendFuture<'a, ReconcileRuntimeSecretsOutput>;

    fn reconcile_host_inputs<'a>(
        &'a self,
        context: &'a Context,
        network: &'a InstanceNetwork,
        manifest: &'a AgentManifestContext,
        state: &'a AgentInstanceDocument,
        secret_files: &'a [SeedFile],
        control: &'a mut Option<InstanceControl>,
    ) -> BackendFuture<'a, ReconcileHostInputsOutput>;

    fn ensure_attached<'a>(
        &'a self,
        context: &'a Context,
        network: &'a InstanceNetwork,
        manifest: &'a AgentManifestContext,
        state: &'a AgentInstanceDocument,
    ) -> BackendFuture<'a, ()>;

    fn log_path(&self, backend_state: &BackendState, file: LogFile) -> PathBuf;
    fn shell_command<'a>(&'a self, state: &'a AgentInstanceDocument) -> BackendFuture<'a, HostCommandResult>;
}

#[must_use]
pub(crate) fn resolve_for_kind(kind: BackendKind) -> BackendRef {
    match kind {
        BackendKind::Qemu => Rc::new(qemu::QemuBackend::for_host()),
    }
}

pub(crate) fn resolve_for_manifest(manifest: &AgentManifest) -> Result<BackendRef, Error> {
    ensure_manifest_supported(manifest, resolve_for_kind(BackendKind::Qemu))
}

pub(crate) fn ensure_manifest_supported(manifest: &AgentManifest, backend: BackendRef) -> Result<BackendRef, Error> {
    let image = ImageCatalog::resolve(ImageRequest {
        os: manifest.spec.image.os,
    });
    if backend.supports_image(image) {
        return Ok(backend);
    }
    Err(Error::UnsupportedManifestImage {
        os: image.os_name(),
        architecture: image.architecture_name(),
        variant: image.variant_name(),
    })
}

pub(crate) struct CreateInstanceInput<'a> {
    pub manifest: AgentManifestContext,
    pub instance: String,
    pub provisioning_plan: &'a ProvisioningPlan,
    pub rendered_bootstrap: &'a RenderedBootstrapPlan,
    pub image_cache_dir: &'a Path,
    pub agent_base: &'a AgentBaseFiles,
    pub files: &'a AgentInstanceFiles,
}

pub(crate) struct CreateBaseInput<'a> {
    pub manifest: AgentManifestContext,
    pub instance: &'a InstanceName,
    pub provisioning_plan: &'a ProvisioningPlan,
    pub rendered_bootstrap: &'a RenderedBootstrapPlan,
    pub image_cache_dir: &'a Path,
    pub files: &'a AgentBaseFiles,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackendBaseImageIdentity {
    pub base_key_schema: &'static str,
    pub os: &'static str,
    pub architecture: &'static str,
    pub variant: &'static str,
    pub cache_key: &'static str,
    pub url: &'static str,
    pub format: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateBaseOutput {
    pub state: BackendState,
    pub image_cache_key: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct CreateInstanceOutput {
    pub state: BackendState,
    pub guest_access: Option<GuestAccessState>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct StartOutput {
    pub process: ProcessStatus,
    pub host_ports: std::collections::BTreeMap<String, PortMappingState>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub(crate) struct StopInstanceInput<'a> {
    pub name: &'a str,
    pub agent: &'a AgentName,
    pub instance: &'a InstanceName,
    pub status: AgentInstancePhase,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub(crate) struct StopOutput {
    pub process_status: &'static str,
    pub terminated_pid: Option<u32>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ReconcileOutput {
    pub stale: bool,
    pub mark_stopped: bool,
    pub backend_changed: bool,
    pub process: ProcessStatus,
    pub host_ports: std::collections::BTreeMap<String, PortMappingState>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReconcileRuntimeSecretsOutput {
    pub secret_files: Vec<SeedFile>,
    pub credentials: std::collections::BTreeMap<String, AgentInstanceCredentialState>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub(crate) struct ReconcileHostInputsOutput {
    pub files_updated: u16,
    pub file_failures: u16,
    pub file_errors: Vec<GuestFileReconcileError>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct GuestFileReconcileError {
    pub path: String,
    pub error: String,
}

#[cfg(test)]
mod tests {
    use agentdp_core::manifest::AgentManifest;

    #[test]
    fn selects_qemu_for_supported_manifest_images() {
        for os in ["archlinux", "rocky9"] {
            let manifest = serde_yaml::from_str::<AgentManifest>(&format!(
                r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: smoke
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: {os}
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: user
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {{}}
    secrets: []
    plugins: {{}}
"
            ))
            .unwrap();

            assert!(super::resolve_for_manifest(&manifest).is_ok());
        }
    }
}
