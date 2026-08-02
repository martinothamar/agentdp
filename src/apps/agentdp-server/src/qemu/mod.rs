mod control;
mod error;
mod lifecycle;
mod network;
mod provisioning;

pub(crate) use agentdp_core::agent::{
    QemuImageState as ImageState, QemuMediatedCaState as MediatedCaState, QemuState as State,
};
pub(crate) use control::Session as InstanceControl;
pub(crate) use error::Error;
use std::path::PathBuf;
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::agent::{AgentInstanceDocument, BackendState};
use agentdp_core::doctor::DoctorReport;
use agentdp_core::provisioning::SeedFile;
use agentdp_core::provisioning::image::CatalogImage;
use agentdp_platform::ssh::{CommandOutput, OutputSink, SshKeygen};
use agentdp_protocol::client_server::{HostCommandResult, LogFile};
use agentdp_qemu as qemu_backend;
use agentdp_qemu::{disk, system};

use crate::agent::{AgentBaseFiles, AgentBaseKey, AgentManifestContext};
use crate::backend::{
    Backend, BackendBaseImageIdentity, BackendFuture, BackendValueFuture, BootstrapEventSink, BootstrapOutcome,
    CreateBaseInput, CreateBaseOutput, CreateInstanceInput, CreateInstanceOutput, Error as BackendError,
    ReconcileHostInputsOutput, ReconcileOutput, ReconcileRuntimeSecretsOutput, StartOutput, StopInstanceInput,
    StopOutput,
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
        control: &'a mut Option<InstanceControl>,
    ) -> BackendFuture<'a, StopOutput> {
        Box::pin(async move {
            lifecycle::stop_base(
                context,
                &state.metadata.agent,
                &state.metadata.name,
                qemu_state_mut(&mut state.status.backend),
                control,
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
        control: &'a mut Option<InstanceControl>,
        retry_epoch: Option<u64>,
        bootstrap_events: Option<&'a mut dyn BootstrapEventSink>,
    ) -> BackendFuture<'a, BootstrapOutcome> {
        Box::pin(async move {
            control::wait_bootstrap(
                context,
                state,
                control,
                retry_epoch,
                bootstrap_events,
                control::BOOTSTRAP_WAIT_TIMEOUT,
            )
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
        control: &'a mut Option<InstanceControl>,
    ) -> BackendFuture<'a, StopOutput> {
        Box::pin(async move {
            lifecycle::stop_instance(context, instance_network, input, qemu_state_mut(backend_state), control)
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

    fn reconcile_runtime_secrets<'a>(
        &'a self,
        context: &'a Context,
        instance_network: &'a InstanceNetwork,
        manifest: &'a AgentManifestContext,
        state: &'a mut AgentInstanceDocument,
    ) -> BackendFuture<'a, ReconcileRuntimeSecretsOutput> {
        Box::pin(async move {
            lifecycle::reconcile_runtime_secrets(
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

    fn reconcile_host_inputs<'a>(
        &'a self,
        context: &'a Context,
        instance_network: &'a InstanceNetwork,
        manifest: &'a AgentManifestContext,
        state: &'a AgentInstanceDocument,
        secret_files: &'a [SeedFile],
        control: &'a mut Option<InstanceControl>,
    ) -> BackendFuture<'a, ReconcileHostInputsOutput> {
        Box::pin(async move {
            control::wait_bootstrap(context, state, control, None, None, control::CONTROL_RECONNECT_TIMEOUT)
                .await
                .map_err(BackendError::Qemu)?;
            let agent = state.metadata.agent.clone();
            let instance = state.metadata.name.clone();
            let network = state.status.network.clone();
            lifecycle::reconcile_host_inputs(
                lifecycle::RuntimeInput {
                    context,
                    instance_network,
                    instance_status: state.status.phase,
                    agent: agent.as_str(),
                    instance: instance.as_str(),
                    network: &network,
                    manifest,
                },
                qemu_state(&state.status.backend),
                secret_files,
                control,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use agentdp_core::Context;
    use agentdp_core::agent::{
        AgentInstanceDocument, AgentInstanceId, AgentInstancePhase, AgentInstanceTarget, BackendState,
        NetworkAllowState, NetworkIpv6State, NetworkModeState, NetworkState, QemuImageState,
    };
    use agentdp_core::manifest::AgentManifest;
    use agentdp_core::provisioning::SeedFile;
    use agentdp_core::provisioning::secrets::SecretBindings;
    use agentdp_ds::local::spsc;
    use agentdp_platform::socket;
    use agentdp_protocol::jsonl::JsonLineReader;
    use agentdp_protocol::server_guest::{
        BootstrapFinished, BootstrapLifecycleStatus, BootstrapStatusReport, BootstrapStepPhase,
        GUEST_CONTROL_PROTOCOL_VERSION, GuestCommandResult, GuestHello, GuestMessage, GuestMessageKind, GuestdRole,
        HostMessageKind, WRITE_USER_FILE_COMMAND, decode_host_message_line, encode_guest_message_line,
    };

    use crate::agent::{AgentBaseKey, AgentManifestContext, AgentName, InstanceName};
    use crate::backend::Backend as _;
    use crate::services::InstanceNetwork;

    use super::{MediatedCaState, QemuBackend, State};

    #[tokio::test]
    async fn host_input_retry_reconnects_after_lost_retained_session() {
        let temp = std::env::temp_dir().join(format!(
            "agentdp-host-input-reconnect-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&temp).unwrap();
        let socket_path = temp.join("guest-control.sock");
        let listener = socket::bind_local_socket(&socket_path).await.unwrap();
        let manifest = serde_yaml::from_str::<AgentManifest>(agentdp_test_support::manifest::minimal()).unwrap();
        let manifest_context =
            AgentManifestContext::from_existing_value(&temp.join("agent.yaml"), manifest.clone()).unwrap();
        let state = instance_document(&manifest, &socket_path);
        let (events, _event_rx) = spsc::bounded(4);
        let network = InstanceNetwork::new(events);
        let backend = QemuBackend::for_host();
        let context = Context::quiet();
        let mut control = None;
        let host_files = [SeedFile {
            path: "/data/home/.codex/auth.json".to_owned(),
            contents: b"auth\n".to_vec(),
            permissions: "0600".to_owned(),
            owner: Some("agent".to_owned()),
        }];

        let first_guest = async {
            let mut guest = listener.accept().await.unwrap();
            write_terminal_replay(&mut guest).await;
            let command = read_host_command(&mut guest).await;
            assert_eq!(command, WRITE_USER_FILE_COMMAND);
        };
        let first_host =
            backend.reconcile_host_inputs(&context, &network, &manifest_context, &state, &host_files, &mut control);
        let (first, ()) = tokio::join!(first_host, first_guest);
        let first = first.expect("first reconciliation reports the lost command session");
        assert_eq!(first.file_failures, 1);
        assert!(control.is_none());

        let second_guest = async {
            let mut guest = listener.accept().await.unwrap();
            write_terminal_replay(&mut guest).await;
            let mut reader = JsonLineReader::default();
            let mut frame = Vec::new();
            assert!(reader.read_line(&mut guest, &mut frame).await.unwrap());
            let request = decode_host_message_line(&frame).unwrap();
            let HostMessageKind::Command(command) = request.kind;
            assert_eq!(command.command, WRITE_USER_FILE_COMMAND);
            guest
                .write_all(
                    &encode_guest_message_line(&GuestMessage::new(
                        request.id,
                        GuestMessageKind::CommandResult(GuestCommandResult {
                            command: WRITE_USER_FILE_COMMAND.to_owned(),
                            updated: false,
                        }),
                    ))
                    .unwrap(),
                )
                .await
                .unwrap();
        };
        let second_host =
            backend.reconcile_host_inputs(&context, &network, &manifest_context, &state, &host_files, &mut control);
        let (second, ()) = tokio::join!(second_host, second_guest);
        let second = second.expect("next reconciliation reconnects through terminal replay");

        assert_eq!(second.file_failures, 0);
        assert!(control.is_some());
        drop(listener);
        let _removed = std::fs::remove_dir_all(temp);
    }

    fn instance_document(manifest: &AgentManifest, socket_path: &std::path::Path) -> AgentInstanceDocument {
        AgentInstanceDocument::new(
            AgentName::new("altinn-studio"),
            AgentInstanceId::new(0),
            InstanceName::new("altinn-studio-0"),
            1,
            AgentBaseKey::new("sha256:test"),
            manifest.spec.template.clone(),
            AgentInstanceTarget::Active,
            AgentInstancePhase::Running,
            NetworkState {
                mode: NetworkModeState::Mediated,
                allow: NetworkAllowState::default(),
                ipv6: NetworkIpv6State::default(),
                ports: BTreeMap::new(),
                runtime: None,
            },
            Vec::new(),
            None,
            BackendState::Qemu(State {
                image: QemuImageState {
                    os: "test".to_owned(),
                    architecture: "x86_64".to_owned(),
                    variant: "cloud".to_owned(),
                    source_url: String::new(),
                    cache_key: String::new(),
                    cache_path: String::new(),
                    download_path: String::new(),
                    format: String::new(),
                },
                disk: String::new(),
                work_dir: String::new(),
                seed_media: String::new(),
                seed_meta_data: String::new(),
                seed_network_config: String::new(),
                seed_user_data: String::new(),
                monitor_socket: String::new(),
                qmp_socket: String::new(),
                guest_control_socket: socket_path.display().to_string(),
                pid_file: String::new(),
                serial_log: String::new(),
                qemu_log: String::new(),
                instance_network: None,
                mediated_secrets: SecretBindings::default(),
                mediated_ca: MediatedCaState::default(),
                pid: None,
                last_start_unix_seconds: None,
            }),
        )
    }

    async fn write_terminal_replay(guest: &mut agentdp_platform::socket::AsyncLocalSocket) {
        let messages = [
            GuestMessage::new(
                "hello",
                GuestMessageKind::Hello(GuestHello {
                    protocol_version: GUEST_CONTROL_PROTOCOL_VERSION,
                    guestd_role: GuestdRole::System,
                    guestd_version: "test".to_owned(),
                    manifest: "altinn-studio".to_owned(),
                    instance: "altinn-studio-0".to_owned(),
                    os: "linux".to_owned(),
                    hostname: "altinn-studio-0".to_owned(),
                    user: "agent".to_owned(),
                }),
            ),
            GuestMessage::new(
                "status",
                GuestMessageKind::BootstrapStatus(BootstrapStatusReport {
                    plan_id: "altinn-studio/altinn-studio-0".to_owned(),
                    plan_hash: "sha256:test".to_owned(),
                    attempt_epoch: 0,
                    phase: BootstrapStepPhase::User,
                    status: BootstrapLifecycleStatus::Passed,
                    current_step: None,
                    completed_steps: Vec::new(),
                    failed_step: None,
                    pending_steps: Vec::new(),
                }),
            ),
            GuestMessage::new(
                "finished",
                GuestMessageKind::BootstrapFinished(BootstrapFinished {
                    plan_hash: "sha256:test".to_owned(),
                    attempt_epoch: 0,
                }),
            ),
        ];
        for message in messages {
            guest
                .write_all(&encode_guest_message_line(&message).unwrap())
                .await
                .unwrap();
        }
    }

    async fn read_host_command(guest: &mut agentdp_platform::socket::AsyncLocalSocket) -> String {
        let mut reader = JsonLineReader::default();
        let mut frame = Vec::new();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), reader.read_line(guest, &mut frame))
                .await
                .unwrap()
                .unwrap()
        );
        let request = decode_host_message_line(&frame).unwrap();
        let HostMessageKind::Command(command) = request.kind;
        command.command
    }
}
