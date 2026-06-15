mod control;
mod error;
mod lifecycle;
mod network;
mod provisioning;

pub(crate) use agentdp_core::agent::{
    QemuImageState as ImageState, QemuMediatedCaState as MediatedCaState, QemuState as State,
};
pub(crate) use error::Error;
use std::path::PathBuf;
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::agent::{AgentInstanceDocument, BackendState};
use agentdp_core::doctor::DoctorReport;
use agentdp_core::provisioning::image::CatalogImage;
use agentdp_platform::ssh::{CommandOutput, OutputSink, SshKeygen};
use agentdp_protocol::client_server::{HostCommandResult, LogFile};
use agentdp_qemu as qemu_backend;
use agentdp_qemu::{disk, system};

use crate::agent::{AgentBaseFiles, AgentBaseKey, AgentManifestContext};
use crate::backend::{
    Backend, BackendBaseImageIdentity, BackendFuture, BackendValueFuture, BootstrapEventSink, CreateBaseInput,
    CreateBaseOutput, CreateInstanceInput, CreateInstanceOutput, Error as BackendError, ReconcileOutput, StartOutput,
    StopInstanceInput, StopOutput,
};
use crate::host::{HostSshError, execute_host_shell_command, interactive_host_shell_command};
use crate::services::InstanceNetwork;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct QemuBackend {
    qemu_img: Option<disk::QemuImg>,
    qemu_system: Option<system::QemuSystem>,
    ssh_keygen: Option<SshKeygen>,
}

impl QemuBackend {
    #[must_use]
    pub(crate) const fn for_host() -> Self {
        Self {
            qemu_img: None,
            qemu_system: None,
            ssh_keygen: None,
        }
    }

    async fn qemu_img(&self) -> Result<disk::QemuImg, Error> {
        match &self.qemu_img {
            Some(qemu_img) => Ok(qemu_img.clone()),
            None => Ok(disk::QemuImg::resolve().await?),
        }
    }

    async fn qemu_system(&self) -> Result<system::QemuSystem, Error> {
        match &self.qemu_system {
            Some(qemu_system) => Ok(qemu_system.clone()),
            None => Ok(system::QemuSystem::resolve().await?),
        }
    }

    async fn ssh_keygen(&self) -> Result<SshKeygen, Error> {
        match &self.ssh_keygen {
            Some(ssh_keygen) => Ok(ssh_keygen.clone()),
            None => SshKeygen::resolve()
                .await
                .map_err(HostSshError::from)
                .map_err(Error::from),
        }
    }
}

const fn qemu_state(backend_state: &BackendState) -> &State {
    match backend_state {
        BackendState::Qemu(state) => state,
    }
}

const fn qemu_state_mut(backend_state: &mut BackendState) -> &mut State {
    match backend_state {
        BackendState::Qemu(state) => state,
    }
}

impl Backend for QemuBackend {
    fn supports_image(&self, image: CatalogImage) -> bool {
        qemu_backend::image::supports_image(image)
    }

    fn check_prerequisites<'a>(
        &'a self,
        context: &'a Context,
        report: &'a mut DoctorReport,
    ) -> BackendValueFuture<'a, ()> {
        Box::pin(async move {
            qemu_backend::doctor::check_prerequisites(context, report).await;
        })
    }

    fn base_image_identity<'a>(
        &'a self,
        manifest: &'a agentdp_core::manifest::AgentManifest,
    ) -> BackendFuture<'a, BackendBaseImageIdentity> {
        Box::pin(async move { lifecycle::base_image_identity(manifest).map_err(BackendError::Qemu) })
    }

    fn create_base<'a>(
        &'a self,
        context: &'a Context,
        input: CreateBaseInput<'a>,
    ) -> BackendFuture<'a, CreateBaseOutput> {
        Box::pin(async move {
            let qemu_img = self.qemu_img().await.map_err(BackendError::Qemu)?;
            lifecycle::create_base(context, input, &qemu_img)
                .await
                .map_err(BackendError::Qemu)
        })
    }

    fn start_base<'a>(
        &'a self,
        context: &'a Context,
        manifest: &'a AgentManifestContext,
        state: &'a mut AgentInstanceDocument,
    ) -> BackendFuture<'a, StartOutput> {
        Box::pin(async move {
            let qemu_system = self.qemu_system().await.map_err(BackendError::Qemu)?;
            lifecycle::start_base(
                context,
                &qemu_system,
                manifest,
                state.metadata.agent.as_str(),
                state.metadata.name.as_str(),
                &state.status.network,
                qemu_state_mut(&mut state.status.backend),
            )
            .await
            .map_err(BackendError::Qemu)
        })
    }

    fn stop_base<'a>(
        &'a self,
        context: &'a Context,
        state: &'a mut AgentInstanceDocument,
    ) -> BackendFuture<'a, StopOutput> {
        Box::pin(async move {
            lifecycle::stop_base(
                context,
                &state.metadata.agent,
                &state.metadata.name,
                qemu_state_mut(&mut state.status.backend),
            )
            .await
            .map_err(BackendError::Qemu)
        })
    }

    fn stop_base_runtime<'a>(
        &'a self,
        context: &'a Context,
        agent: &'a agentdp_core::agent::AgentName,
        key: &'a AgentBaseKey,
        files: &'a AgentBaseFiles,
    ) -> BackendFuture<'a, StopOutput> {
        Box::pin(async move {
            lifecycle::stop_base_runtime(context, agent, key, files)
                .await
                .map_err(BackendError::Qemu)
        })
    }

    fn create_instance<'a>(
        &'a self,
        context: &'a Context,
        input: CreateInstanceInput<'a>,
    ) -> BackendFuture<'a, CreateInstanceOutput> {
        Box::pin(async move {
            let qemu_img = self.qemu_img().await.map_err(BackendError::Qemu)?;
            let ssh_keygen = self.ssh_keygen().await.map_err(BackendError::Qemu)?;
            lifecycle::create_instance(context, input, &qemu_img, &ssh_keygen)
                .await
                .map_err(BackendError::Qemu)
        })
    }

    fn start_instance<'a>(
        &'a self,
        context: &'a Context,
        instance_network: &'a InstanceNetwork,
        manifest: &'a AgentManifestContext,
        state: &'a mut AgentInstanceDocument,
    ) -> BackendFuture<'a, StartOutput> {
        Box::pin(async move {
            let qemu_system = self.qemu_system().await.map_err(BackendError::Qemu)?;
            let agent = state.metadata.agent.clone();
            let instance = state.metadata.name.clone();
            let network = state.status.network.clone();
            lifecycle::start_instance(
                &qemu_system,
                lifecycle::StartInstanceInput {
                    context,
                    instance_network,
                    manifest,
                    agent: agent.as_str(),
                    instance: instance.as_str(),
                    network: &network,
                },
                qemu_state_mut(&mut state.status.backend),
            )
            .await
            .map_err(BackendError::Qemu)
        })
    }

    fn exec<'a>(
        &'a self,
        context: &'a Context,
        state: &'a AgentInstanceDocument,
        command: &'a str,
        timeout: Duration,
        output: &'a mut dyn OutputSink,
    ) -> BackendFuture<'a, CommandOutput> {
        Box::pin(async move {
            Box::pin(execute_host_shell_command(context, state, command, timeout, output))
                .await
                .map_err(Error::from)
                .map_err(BackendError::Qemu)
        })
    }

    fn wait_bootstrap<'a>(
        &'a self,
        context: &'a Context,
        state: &'a AgentInstanceDocument,
        bootstrap_events: Option<&'a mut dyn BootstrapEventSink>,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            control::wait_bootstrap(context, state, bootstrap_events)
                .await
                .map_err(BackendError::Qemu)
        })
    }

    fn stop_instance<'a>(
        &'a self,
        context: &'a Context,
        instance_network: &'a InstanceNetwork,
        input: StopInstanceInput<'a>,
        backend_state: &'a mut BackendState,
    ) -> BackendFuture<'a, StopOutput> {
        Box::pin(async move {
            lifecycle::stop_instance(context, instance_network, input, qemu_state_mut(backend_state))
                .await
                .map_err(BackendError::Qemu)
        })
    }

    fn reconcile_instance<'a>(
        &'a self,
        context: &'a Context,
        instance_network: &'a InstanceNetwork,
        manifest: &'a AgentManifestContext,
        state: &'a mut AgentInstanceDocument,
    ) -> BackendFuture<'a, ReconcileOutput> {
        Box::pin(async move {
            lifecycle::reconcile(
                lifecycle::RuntimeInput {
                    context,
                    instance_network,
                    instance_status: state.status.phase,
                    agent: state.metadata.agent.as_str(),
                    instance: state.metadata.name.as_str(),
                    network: &state.status.network,
                    manifest,
                },
                qemu_state_mut(&mut state.status.backend),
            )
            .await
            .map_err(BackendError::Qemu)
        })
    }

    fn ensure_attached<'a>(
        &'a self,
        context: &'a Context,
        instance_network: &'a InstanceNetwork,
        manifest: &'a AgentManifestContext,
        state: &'a AgentInstanceDocument,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            lifecycle::ensure_attached(
                lifecycle::RuntimeInput {
                    context,
                    instance_network,
                    instance_status: state.status.phase,
                    agent: state.metadata.agent.as_str(),
                    instance: state.metadata.name.as_str(),
                    network: &state.status.network,
                    manifest,
                },
                qemu_state(&state.status.backend),
            )
            .await
            .map_err(BackendError::Qemu)
        })
    }

    fn log_path(&self, backend_state: &BackendState, file: LogFile) -> PathBuf {
        lifecycle::log_path(qemu_state(backend_state), file)
    }

    fn shell_command<'a>(&'a self, state: &'a AgentInstanceDocument) -> BackendFuture<'a, HostCommandResult> {
        Box::pin(async move {
            interactive_host_shell_command(state)
                .await
                .map_err(Error::from)
                .map_err(BackendError::Qemu)
        })
    }
}
