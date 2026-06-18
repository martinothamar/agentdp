use std::cell::RefCell;
use std::fmt;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use agentdp_core::Context;
use agentdp_core::agent::{
    AgentBaseKey, AgentDocument, AgentEvent, AgentInstanceBootstrapState, AgentInstanceBootstrapStepStatus,
    AgentInstanceDocument, AgentInstanceEvent, AgentInstanceId, AgentInstancePhase, AgentInstanceTarget,
    AgentStatusPhase, BackendState, BootstrapEvent, InstanceName, NetworkAllowState, NetworkIpv6State,
    NetworkModeState, NetworkState, ProcessStatus, QemuImageState, QemuInstanceNetworkState, QemuMediatedCaState,
    QemuState, assign_port_mappings,
};
use agentdp_core::doctor::DoctorReport;
use agentdp_core::manifest::{AgentManifest, AgentPhase, GuestPort, NetworkProtocol};
use agentdp_core::provisioning::image::CatalogImage;
use agentdp_core::provisioning::secrets::SecretBindings;
use agentdp_ds::local::{oneshot, spsc};
use agentdp_platform::ssh::{CommandOutput, OutputSink};
use agentdp_protocol::client_server::{
    AgentInstanceExecParams, AgentInstanceExecResult, AgentInstanceLogsParams, HostCommandResult, LogFile,
};
use agentdp_protocol::server_guest::{BootstrapLifecycleStatus, BootstrapStepPhase, BootstrapStepStatus};

use crate::agent::{AgentBaseFiles, AgentManifestContext, AgentName, AgentdpLayout};
use crate::backend;
use crate::host::tailscale::TailscaleService;
use crate::services::InstanceNetwork;

use super::{Agent, AgentCommand, AgentError as Error, AgentStreamItem};

static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(1);
const STREAM_CAPACITY: usize = 256;

fn tailscale_service() -> Rc<TailscaleService> {
    Rc::new(TailscaleService::new())
}

#[tokio::test(flavor = "local")]
async fn cold_apply_reaches_ready_without_child_actors() {
    let (_runtime, mut stream) = start_agent(&manifest_with(1)).await;

    let document = wait_for_document(&mut stream, |document| {
        document.status.phase == AgentStatusPhase::Running
            && document.status.replicas.ready == 1
            && document.status.observed_generation == document.generation()
    })
    .await;

    assert_eq!(document.status.instances.len(), 1);
    assert_eq!(document.status.replicas.active, 1);
}

#[tokio::test(flavor = "local")]
async fn scale_up_down_and_back_up_keeps_control_flow_local_to_agent() {
    let (agent, mut stream) = start_agent(&manifest_with(1)).await;
    let first = wait_for_ready(&mut stream, 1).await;

    let scaled = scale(&agent, 2).await;
    assert_eq!(scaled.replicas(), 2);
    assert_eq!(scaled.status.replicas.desired, 2);
    let second = wait_for_ready(&mut stream, 2).await;
    assert_eq!(second.status.replicas.ready, 2);

    let stopped = scale(&agent, 0).await;
    assert_eq!(stopped.replicas(), 0);
    assert_eq!(stopped.status.replicas.desired, 0);
    let stopped = wait_for_document(&mut stream, |document| {
        document.generation() == first.generation() + 2
            && document.status.replicas.active == 0
            && document.status.replicas.stopped >= 2
    })
    .await;
    assert_eq!(stopped.replicas(), 0);

    let restarted = scale(&agent, 1).await;
    assert_eq!(restarted.replicas(), 1);
    let restarted = wait_for_ready(&mut stream, 1).await;
    assert_eq!(restarted.status.replicas.ready, 1);
}

#[tokio::test(flavor = "local")]
async fn paused_apply_is_not_observed_until_instances_stop() {
    let (agent, mut stream) = start_agent(&manifest_with(1)).await;
    let ready = wait_for_ready(&mut stream, 1).await;
    let mut paused = manifest_with(1);
    paused.spec.phase = AgentPhase::Paused;

    let accepted = apply(&agent, manifest_context(paused)).await;

    assert_eq!(accepted.generation(), ready.generation() + 1);
    assert_eq!(accepted.status.phase, AgentStatusPhase::Paused);
    assert!(accepted.status.reconciling);
    assert_ne!(accepted.status.observed_generation, accepted.generation());
    assert_eq!(accepted.status.replicas.active, 1);

    let stopped = wait_for_document(&mut stream, |document| {
        document.generation() == accepted.generation()
            && document.status.observed_generation == document.generation()
            && !document.status.reconciling
            && document.status.replicas.active == 0
            && document.status.replicas.stopped >= 1
    })
    .await;
    assert_eq!(stopped.status.phase, AgentStatusPhase::Paused);
}

#[tokio::test(flavor = "local")]
async fn delete_while_base_is_building_converges_to_deleted() {
    let (agent, mut stream) = start_agent(&manifest_with(1)).await;
    wait_for_event(&mut stream, |event| {
        matches!(event, AgentEvent::AgentBaseStarted { .. })
    })
    .await;

    let accepted = delete(&agent).await;
    assert!(accepted.deletion_requested());
    assert_eq!(accepted.status.phase, AgentStatusPhase::Deleting);
    let deleted = wait_for_document(&mut stream, |document| document.status.deleted).await;

    assert_eq!(deleted.status.phase, AgentStatusPhase::Deleted);
    assert!(deleted.status.instances.is_empty());
}

#[tokio::test(flavor = "local")]
async fn apply_changed_manifest_rebuilds_base_and_reconciles_instances() {
    let (agent, mut stream) = start_agent(&manifest_with(1)).await;
    let initial = wait_for_ready(&mut stream, 1).await;
    let mut changed = manifest_with(1);
    changed.spec.bootstrap.packages.push("htop".to_owned());

    let accepted = apply(&agent, manifest_context(changed)).await;
    assert_eq!(accepted.generation(), initial.generation() + 1);

    let reconciled = wait_for_ready(&mut stream, 1).await;
    assert_eq!(reconciled.generation(), initial.generation() + 1);
    assert_ne!(reconciled.ready_agent_base_key(), initial.ready_agent_base_key());
    assert_eq!(reconciled.ready_agent_base_key(), reconciled.desired_agent_base_key());
}

#[tokio::test(flavor = "local")]
async fn generation_reconcile_preserves_configured_host_ports() {
    let (agent, mut stream) = start_agent(&manifest_with_code_server_host(1, 4090)).await;
    let initial = wait_for_ready(&mut stream, 1).await;
    assert_eq!(
        initial.status.instances[&AgentInstanceId::new(0)].network.ports["code_server"].host,
        Some(4090)
    );

    let mut changed = manifest_with_code_server_host(1, 4090);
    changed.spec.bootstrap.packages.push("htop".to_owned());
    apply(&agent, manifest_context(changed)).await;

    let reconciled = wait_for_ready(&mut stream, 1).await;
    assert_eq!(
        reconciled.status.instances[&AgentInstanceId::new(0)].network.ports["code_server"].host,
        Some(4090)
    );
}

#[tokio::test(flavor = "local")]
async fn late_watch_receives_current_document_then_live_events() {
    let (agent, mut initial_stream) = start_agent(&manifest_with(1)).await;
    let ready = wait_for_ready(&mut initial_stream, 1).await;

    let mut late_stream = watch(&agent).await;
    let first = recv_stream_item(&mut late_stream).await;
    let AgentStreamItem::Document(document) = first else {
        panic!("first watch item should be current document");
    };
    assert_eq!(document.generation(), ready.generation());

    let scaled = scale(&agent, 2).await;
    assert_eq!(scaled.replicas(), 2);
    wait_for_event(&mut late_stream, |event| {
        matches!(event, AgentEvent::ScaleAccepted { replicas: 2, .. })
    })
    .await;
    let document = wait_for_ready(&mut late_stream, 2).await;
    assert_eq!(document.status.replicas.ready, 2);
}

#[tokio::test(flavor = "local")]
async fn persistence_writes_agent_instances_and_events_from_commit_tail() {
    let (layout, agent_name) = test_layout("persistence_writes_agent_instances_and_events_from_commit_tail").await;
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name.clone(),
        layout.clone(),
        Rc::new(FakeBackend::default()),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;
    apply(&agent, manifest_context(manifest_with(1))).await;

    let ready = wait_for_ready(&mut stream, 1).await;

    let agent_yaml = tokio::fs::read_to_string(layout.agent_document(&agent_name))
        .await
        .expect("agent document written");
    let persisted_agent: AgentDocument = serde_yaml::from_str(&agent_yaml).expect("valid agent document");
    assert_eq!(persisted_agent.generation(), ready.generation());
    assert_eq!(persisted_agent.status.replicas.ready, 1);

    let instance_yaml =
        tokio::fs::read_to_string(layout.instance(&agent_name, AgentInstanceId::new(0)).instance_state())
            .await
            .expect("instance document written");
    let persisted_instance: AgentInstanceDocument =
        serde_yaml::from_str(&instance_yaml).expect("valid instance document");
    assert_eq!(
        persisted_instance.status.phase,
        agentdp_core::agent::AgentInstancePhase::Running
    );

    let events = tokio::fs::read_to_string(layout.agent_events(&agent_name))
        .await
        .expect("event log written");
    assert!(events.contains("\"kind\":\"agent_base_ready\""));
    assert!(events.contains("\"kind\":\"instance_created\""));
}

#[tokio::test(flavor = "local")]
async fn mediated_network_instances_publish_configured_host_ports() {
    let (_runtime, mut stream) = start_agent(&manifest_with(1)).await;

    let ready = wait_for_ready(&mut stream, 1).await;
    let instance = ready
        .status
        .instances
        .get(&AgentInstanceId::new(0))
        .expect("instance status");
    assert_eq!(instance.network.mode, NetworkModeState::Mediated);
    assert_eq!(instance.network.ports["ssh"].guest, 22);
    assert_eq!(instance.network.ports["ssh"].host, None);
}

#[tokio::test(flavor = "local")]
async fn instance_creation_passes_instance_name_not_agent_qualified_identity() {
    let (_agent, mut stream, backend) = start_agent_with_backend(&manifest_with(1)).await;

    let ready = wait_for_ready(&mut stream, 1).await;

    assert_eq!(ready.status.replicas.ready, 1);
    assert_eq!(
        backend.created_instances.borrow().as_slice(),
        ["replica-0"],
        "backend instance identity must match AgentInstanceDocument metadata"
    );
}

#[tokio::test(flavor = "local")]
async fn foreground_exec_status_and_logs_share_the_agent_runtime_queue() {
    let (agent, mut stream) = start_agent(&manifest_with(1)).await;
    wait_for_ready(&mut stream, 1).await;
    let id = AgentInstanceId::new(0);

    let status = status(&agent, id).await.expect("instance status");
    assert_eq!(status.status.phase, agentdp_core::agent::AgentInstancePhase::Running);

    let result = exec(&agent, id, "echo hello").await.expect("exec succeeds");
    assert_eq!(result.exit_status, 0);
    assert_eq!(result.stdout, "executed: 'echo hello'");

    let logs = logs(&agent, id).await;
    assert!(
        logs.iter().any(|line| line.contains("session_finished")),
        "event log should include session completion: {logs:?}"
    );
}

#[tokio::test(flavor = "local")]
async fn delete_waits_for_foreground_session_before_cleanup() {
    let (agent, mut stream) = start_agent(&manifest_with(1)).await;
    wait_for_ready(&mut stream, 1).await;
    let id = AgentInstanceId::new(0);

    let exec_task = tokio::task::spawn_local({
        let agent = agent.clone();
        async move { exec(&agent, id, "sleep 1").await }
    });
    wait_for_event(&mut stream, |event| {
        matches!(
            event,
            AgentEvent::InstanceEvent { event }
                if matches!(event.event, AgentInstanceEvent::SessionStarted { .. })
        )
    })
    .await;

    let accepted = delete(&agent).await;
    assert!(accepted.deletion_requested());
    let result = exec_task.await.expect("exec task joined").expect("exec completed");
    assert_eq!(result.stdout, "executed: 'sleep 1'");
    let deleted = wait_for_document(&mut stream, |document| document.status.deleted).await;
    assert!(deleted.status.instances.is_empty());
}

#[tokio::test(flavor = "local")]
async fn changed_manifest_restarts_children_without_synthetic_network_state() {
    let (agent, mut stream) = start_agent(&manifest_with(1)).await;
    wait_for_ready(&mut stream, 1).await;
    let mut changed = manifest_with(1);
    changed.spec.template.resources.memory = "3G".to_owned();
    let accepted = apply(&agent, manifest_context(changed)).await;

    let ready = wait_for_ready(&mut stream, 1).await;
    let instance = ready
        .status
        .instances
        .get(&AgentInstanceId::new(0))
        .expect("instance status");
    assert_eq!(ready.generation(), accepted.generation());
    assert_eq!(instance.network.mode, NetworkModeState::Mediated);
    assert_eq!(instance.network.ports["ssh"].guest, 22);
    assert_eq!(instance.network.ports["ssh"].host, None);
}

#[tokio::test(flavor = "local")]
async fn bootstrap_progress_is_structured_agent_events() {
    let (_runtime, mut stream) = start_agent(&manifest_with(1)).await;

    wait_for_instance_event(&mut stream, |event| {
        matches!(event, AgentInstanceEvent::BootstrapStarted)
    })
    .await;
    wait_for_bootstrap_event(&mut stream, |event| {
        matches!(
            event,
            BootstrapEvent::StepStarted { step } if step.step == "system.packages"
        )
    })
    .await;
    wait_for_bootstrap_event(&mut stream, |event| {
        matches!(
            event,
            BootstrapEvent::StepFinished { step, .. } if step == "system.packages"
        )
    })
    .await;
    wait_for_instance_event(&mut stream, |event| {
        matches!(
            event,
            AgentInstanceEvent::BootstrapFinished {
                result: agentdp_core::agent::OperationResult::Succeeded
            }
        )
    })
    .await;

    wait_for_ready(&mut stream, 1).await;
}

#[tokio::test(flavor = "local")]
async fn wait_style_stream_sees_document_command_events_and_readiness() {
    let (agent, mut stream) = start_agent(&manifest_with(0)).await;
    wait_for_ready(&mut stream, 0).await;

    let mut wait_stream = watch(&agent).await;
    let AgentStreamItem::Document(document) = recv_stream_item(&mut wait_stream).await else {
        panic!("watch should start with current document");
    };
    assert_eq!(document.status.replicas.desired, 0);

    let accepted = scale(&agent, 1).await;
    wait_for_event(&mut wait_stream, |event| {
        matches!(event, AgentEvent::ScaleAccepted { generation, replicas: 1 } if *generation == accepted.generation())
    })
    .await;
    wait_for_instance_event(&mut wait_stream, |event| {
        matches!(event, AgentInstanceEvent::BootstrapStarted)
    })
    .await;
    let ready = wait_for_ready(&mut wait_stream, 1).await;
    assert_eq!(ready.generation(), accepted.generation());
}

#[tokio::test(flavor = "local")]
async fn delete_continues_after_bounded_base_stop_timeout() {
    let (agent, mut stream, backend) = start_agent_with_backend(&manifest_with(1)).await;
    wait_for_ready(&mut stream, 1).await;
    backend.fail_next_base_stop();

    let accepted = delete(&agent).await;
    assert!(accepted.deletion_requested());
    wait_for_event(&mut stream, |event| {
        matches!(
            event,
            AgentEvent::Diagnostic { message, .. } if message.contains("agent base stop timed out")
        )
    })
    .await;
    wait_for_document(&mut stream, |document| document.status.deleted).await;
}

#[tokio::test(flavor = "local")]
async fn delete_continues_after_bounded_instance_delete_timeout() {
    let (agent, mut stream, backend) = start_agent_with_backend(&manifest_with(1)).await;
    wait_for_ready(&mut stream, 1).await;
    backend.fail_next_instance_delete();

    let accepted = delete(&agent).await;
    assert!(accepted.deletion_requested());
    wait_for_event(&mut stream, |event| {
        matches!(
            event,
            AgentEvent::Diagnostic { message, .. } if message.contains("instance delete timed out")
        )
    })
    .await;
    let deleted = wait_for_document(&mut stream, |document| document.status.deleted).await;
    assert!(deleted.status.instances.is_empty());
}

#[tokio::test(flavor = "local")]
async fn delete_stops_running_instance_before_removing_files() {
    let (agent, mut stream, backend) = start_agent_with_backend(&manifest_with(1)).await;
    wait_for_ready(&mut stream, 1).await;

    delete(&agent).await;
    wait_for_document(&mut stream, |document| document.status.deleted).await;

    assert_eq!(*backend.instance_stops.borrow(), 1);
}

#[tokio::test(flavor = "local")]
async fn deleted_agent_removes_state_and_can_be_recreated() {
    let manifest = manifest_with(1);
    let backend: backend::BackendRef = Rc::new(FakeBackend::default());
    let (layout, agent_name) = unique_layout(&manifest);
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name.clone(),
        layout.clone(),
        Rc::clone(&backend),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;
    apply(&agent, manifest_context(manifest.clone())).await;
    wait_for_ready(&mut stream, 1).await;
    delete(&agent).await;
    wait_for_document(&mut stream, |document| document.status.deleted).await;
    assert!(
        !tokio::fs::try_exists(layout.agent_document(&agent_name))
            .await
            .expect("inspect agent document")
    );

    let recreated = Agent::spawn(Context::quiet(), agent_name, layout, backend, tailscale_service());
    let mut recreated_stream = watch(&recreated).await;
    apply(&recreated, manifest_context(manifest)).await;
    wait_for_ready(&mut recreated_stream, 1).await;
}

#[tokio::test(flavor = "local")]
async fn pending_instance_creation_is_not_published_as_fake_status() {
    let manifest = manifest_with(1);
    let backend = Rc::new(FakeBackend::default());
    let release_create = backend.pause_next_instance_create();
    let agent_backend: backend::BackendRef = backend;
    let (layout, agent_name) = unique_layout(&manifest);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    let accepted = apply(&agent, manifest_context(manifest)).await;
    assert!(accepted.status.instances.is_empty());
    assert!(matches!(
        status(&agent, AgentInstanceId::new(0)).await,
        Err(Error::InstanceNotFound { .. })
    ));

    release_create.try_send(());
    wait_for_ready(&mut stream, 1).await;
}

#[tokio::test(flavor = "local")]
async fn persisted_bootstrap_retry_wakes_without_external_input() {
    let manifest = manifest_with(1);
    let (layout, agent_name) = unique_layout(&manifest);
    persist_retrying_running_instance(&layout, &agent_name, &manifest).await;
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        Rc::new(FakeBackend::default()),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    let ready = wait_for_ready(&mut stream, 1).await;
    assert_eq!(ready.status.replicas.ready, 1);
}

#[tokio::test(flavor = "local")]
async fn persisted_ready_instance_reconciles_after_agent_restart() {
    let manifest = manifest_with(1);
    let (layout, agent_name) = unique_layout(&manifest);
    persist_ready_running_instance(&layout, &agent_name, &manifest).await;
    let backend = Rc::new(FakeBackend::default());
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    let loaded = recv_stream_item(&mut stream).await;
    let AgentStreamItem::Document(document) = loaded else {
        panic!("first stream item should be loaded document");
    };
    let instance = &document.status.instances[&AgentInstanceId::new(0)];
    assert!(instance.readiness.is_none());
    assert!(instance.network.runtime.is_none());

    wait_for_ready(&mut stream, 1).await;
    assert_eq!(*backend.instance_reconciles.borrow(), 1);
}

#[tokio::test(flavor = "local")]
async fn commit_persistence_failure_answers_pending_command() {
    let manifest = manifest_with(0);
    let (layout, agent_name) = unique_layout(&manifest);
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name.clone(),
        layout.clone(),
        Rc::new(FakeBackend::default()),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;
    apply(&agent, manifest_context(manifest)).await;
    wait_for_ready(&mut stream, 0).await;

    let document_path = layout.agent_document(&agent_name);
    tokio::fs::remove_file(&document_path)
        .await
        .expect("remove agent document");
    tokio::fs::create_dir_all(&document_path)
        .await
        .expect("create directory at agent document path");

    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::Scale { replicas: 1, respond })
        .expect("agent accepts scale command");
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), receive)
        .await
        .expect("scale response should not hang")
        .expect("scale response sender should complete");

    assert!(matches!(result, Err(Error::PersistState { .. })));
}

async fn wait_for_ready(
    stream: &mut spsc::Receiver<AgentStreamItem>,
    replicas: u16,
) -> agentdp_core::agent::AgentDocument {
    wait_for_document(stream, |document| {
        document.status.replicas.ready == replicas
            && document.status.observed_generation == document.generation()
            && !document.status.reconciling
    })
    .await
}

async fn wait_for_document(
    stream: &mut spsc::Receiver<AgentStreamItem>,
    mut matches: impl FnMut(&agentdp_core::agent::AgentDocument) -> bool,
) -> agentdp_core::agent::AgentDocument {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for document");
        let item = recv_stream_item(stream).await;
        if let AgentStreamItem::Document(document) = item
            && matches(&document)
        {
            return *document;
        }
    }
}

async fn wait_for_event(
    stream: &mut spsc::Receiver<AgentStreamItem>,
    mut matches: impl FnMut(&AgentEvent) -> bool,
) -> AgentEvent {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for event");
        let item = recv_stream_item(stream).await;
        if let AgentStreamItem::Event(envelope) = item
            && matches(&envelope.event)
        {
            return envelope.event;
        }
    }
}

async fn wait_for_instance_event(
    stream: &mut spsc::Receiver<AgentStreamItem>,
    mut matches: impl FnMut(&AgentInstanceEvent) -> bool,
) -> AgentInstanceEvent {
    let event = wait_for_event(
        stream,
        |event| matches!(event, AgentEvent::InstanceEvent { event } if matches(&event.event)),
    )
    .await;
    let AgentEvent::InstanceEvent { event } = event else {
        unreachable!("wait_for_event matched an instance event");
    };
    event.event
}

async fn wait_for_bootstrap_event(
    stream: &mut spsc::Receiver<AgentStreamItem>,
    mut matches: impl FnMut(&BootstrapEvent) -> bool,
) -> BootstrapEvent {
    let event = wait_for_event(
        stream,
        |event| matches!(event, AgentEvent::BootstrapEvent { event } if matches(event)),
    )
    .await;
    let AgentEvent::BootstrapEvent { event } = event else {
        unreachable!("wait_for_event matched a bootstrap event");
    };
    event
}

async fn recv_stream_item(stream: &mut spsc::Receiver<AgentStreamItem>) -> AgentStreamItem {
    tokio::time::timeout(std::time::Duration::from_secs(2), stream.recv())
        .await
        .expect("timed out waiting for stream item")
        .expect("stream open")
}

async fn apply(agent: &Agent, manifest: AgentManifestContext) -> AgentDocument {
    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::Apply {
            manifest: Box::new(manifest),
            respond,
        })
        .expect("agent accepts apply command");
    receive.await.expect("apply response").expect("apply succeeds")
}

async fn scale(agent: &Agent, replicas: u16) -> AgentDocument {
    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::Scale { replicas, respond })
        .expect("agent accepts scale command");
    receive.await.expect("scale response").expect("scale succeeds")
}

async fn delete(agent: &Agent) -> AgentDocument {
    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::Delete { respond })
        .expect("agent accepts delete command");
    receive.await.expect("delete response").expect("delete succeeds")
}

async fn watch(agent: &Agent) -> spsc::Receiver<AgentStreamItem> {
    let (items, receiver) = spsc::bounded(STREAM_CAPACITY);
    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::OpenStream {
            replay_from_generation: None,
            items,
            respond,
        })
        .expect("agent accepts watch command");
    receive.await.expect("watch response").expect("watch succeeds");
    receiver
}

async fn status(agent: &Agent, id: AgentInstanceId) -> Result<AgentInstanceDocument, Error> {
    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::InstanceStatus { instance: id, respond })
        .expect("agent accepts status command");
    receive.await.expect("status response")
}

async fn exec(
    agent: &Agent,
    id: AgentInstanceId,
    command: impl Into<String>,
) -> Result<AgentInstanceExecResult, Error> {
    let (respond, receive) = oneshot::channel();
    let (output, _output_rx) = spsc::bounded(8);
    agent
        .send(AgentCommand::InstanceExec {
            context: Context::quiet(),
            instance: id,
            params: AgentInstanceExecParams {
                agent: agent.agent().to_string(),
                instance_id: id.as_u32(),
                command: vec![command.into()],
                timeout_seconds: Some(30),
            },
            output,
            respond,
        })
        .expect("agent accepts exec command");
    receive.await.expect("exec response")
}

async fn logs(agent: &Agent, id: AgentInstanceId) -> Vec<String> {
    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::InstanceLogs {
            instance: id,
            params: AgentInstanceLogsParams {
                agent: agent.agent().to_string(),
                instance_id: id.as_u32(),
                file: LogFile::Events,
                lines: 100,
            },
            respond,
        })
        .expect("agent accepts logs command");
    receive
        .await
        .expect("logs response")
        .expect("logs succeed")
        .contents
        .lines()
        .map(str::to_owned)
        .collect()
}

fn manifest_with(replicas: u16) -> AgentManifest {
    let mut manifest: AgentManifest = serde_yaml::from_str(agentdp_test_support::manifest::minimal()).unwrap();
    manifest.spec.replicas = replicas;
    manifest.spec.phase = AgentPhase::Running;
    manifest
}

fn manifest_with_code_server_host(replicas: u16, host: u16) -> AgentManifest {
    let mut manifest = manifest_with(replicas);
    manifest.spec.network.ports.insert(
        "code_server".to_owned(),
        GuestPort {
            guest: 4090,
            host: Some(host),
            protocol: NetworkProtocol::Tcp,
        },
    );
    manifest
}

fn manifest_context(manifest: AgentManifest) -> AgentManifestContext {
    let source_path = PathBuf::from(format!("/tmp/{}.yaml", manifest.name()));
    AgentManifestContext::from_existing_value(&source_path, manifest).expect("valid test manifest context")
}

async fn start_agent(manifest: &AgentManifest) -> (Agent, spsc::Receiver<AgentStreamItem>) {
    let backend: backend::BackendRef = Rc::new(FakeBackend::default());
    let (layout, agent_name) = unique_layout(manifest);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, backend, tailscale_service());
    let stream = watch(&agent).await;
    apply(&agent, manifest_context(manifest.clone())).await;
    (agent, stream)
}

async fn start_agent_with_backend(
    manifest: &AgentManifest,
) -> (Agent, spsc::Receiver<AgentStreamItem>, Rc<FakeBackend>) {
    let backend = Rc::new(FakeBackend::default());
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(manifest);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let stream = watch(&agent).await;
    apply(&agent, manifest_context(manifest.clone())).await;
    (agent, stream, backend)
}

fn unique_layout(manifest: &AgentManifest) -> (AgentdpLayout, AgentName) {
    let id = NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir()
        .join("agentdp-agent-loop-tests")
        .join(format!("{}-{id}", std::process::id()));
    (AgentdpLayout::from_root(root), AgentName::new(manifest.name()))
}

async fn test_layout(name: &str) -> (AgentdpLayout, AgentName) {
    let root = std::env::temp_dir().join("agentdp-agent-loop-tests").join(name);
    let _result = tokio::fs::remove_dir_all(&root).await;
    let manifest = manifest_with(1);
    (AgentdpLayout::from_root(root), AgentName::new(manifest.name()))
}

async fn persist_retrying_running_instance(layout: &AgentdpLayout, agent: &AgentName, manifest: &AgentManifest) {
    let mut instance = persisted_running_instance(layout, agent, manifest).await;
    let now = agentdp_platform::time::unix_seconds();
    instance.status.record_bootstrap_failure(AgentInstanceBootstrapState {
        failure_count: 1,
        last_failure_unix_seconds: now,
        next_retry_unix_seconds: now.saturating_add(1),
        last_error: "previous bootstrap failure".to_owned(),
    });
    write_persisted_instance(layout, agent, AgentInstanceId::new(0), &instance).await;
}

async fn persist_ready_running_instance(layout: &AgentdpLayout, agent: &AgentName, manifest: &AgentManifest) {
    let mut instance = persisted_running_instance(layout, agent, manifest).await;
    instance.status.mark_ready(agentdp_core::agent::ReadinessState {
        ready: true,
        last_success_unix_seconds: agentdp_platform::time::unix_seconds(),
        result: agentdp_core::agent::ReadinessResult {
            ready: true,
            services: std::collections::BTreeMap::new(),
            healthchecks: Vec::new(),
        },
    });
    instance.status.network.runtime = Some(agentdp_core::agent::AgentInstanceNetworkStatus::default());
    write_persisted_instance(layout, agent, AgentInstanceId::new(0), &instance).await;
}

async fn persisted_running_instance(
    layout: &AgentdpLayout,
    agent: &AgentName,
    manifest: &AgentManifest,
) -> AgentInstanceDocument {
    let base_key = AgentBaseKey::new("sha256-test");
    let mut document =
        AgentDocument::from_manifest("/tmp/retrying.yaml", agent.clone(), manifest).expect("agent document");
    document.mark_agent_base_ready(base_key.clone());
    let agent_path = layout.agent_document(agent);
    tokio::fs::create_dir_all(agent_path.parent().expect("agent document parent"))
        .await
        .expect("create agent dir");
    tokio::fs::write(
        &agent_path,
        serde_yaml::to_string(&document).expect("serialize agent document"),
    )
    .await
    .expect("write agent document");

    let id = AgentInstanceId::new(0);
    let backend = fake_mediated_backend_state();
    let ports = assign_port_mappings(&document.manifest(), id).expect("port mappings");
    let network = NetworkState::new(
        &backend,
        NetworkAllowState::from(&manifest.spec.network.allow),
        NetworkIpv6State::default(),
        ports,
    );
    AgentInstanceDocument::new(
        agent.clone(),
        id,
        InstanceName::new("replica-0"),
        document.generation(),
        base_key,
        document.template().clone(),
        AgentInstanceTarget::Active,
        AgentInstancePhase::Running,
        network,
        Vec::new(),
        None,
        backend,
    )
}

async fn write_persisted_instance(
    layout: &AgentdpLayout,
    agent: &AgentName,
    id: AgentInstanceId,
    instance: &AgentInstanceDocument,
) {
    let instance_path = layout.instance(agent, id).instance_state();
    tokio::fs::create_dir_all(instance_path.parent().expect("instance document parent"))
        .await
        .expect("create instance dir");
    tokio::fs::write(
        &instance_path,
        serde_yaml::to_string(&instance).expect("serialize instance document"),
    )
    .await
    .expect("write instance document");
}

#[derive(Default)]
struct FakeBackend {
    fail_next_base_stop: RefCell<bool>,
    fail_next_instance_delete: RefCell<bool>,
    pause_next_instance_create: RefCell<Option<oneshot::Receiver<()>>>,
    created_instances: RefCell<Vec<String>>,
    instance_reconciles: RefCell<u32>,
    instance_stops: RefCell<u32>,
}

impl FakeBackend {
    fn fail_next_base_stop(&self) {
        *self.fail_next_base_stop.borrow_mut() = true;
    }

    fn fail_next_instance_delete(&self) {
        *self.fail_next_instance_delete.borrow_mut() = true;
    }

    fn pause_next_instance_create(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.pause_next_instance_create.borrow_mut() = Some(wait);
        release
    }

    fn take_base_stop_failure(&self) -> bool {
        std::mem::take(&mut *self.fail_next_base_stop.borrow_mut())
    }

    fn take_instance_delete_failure(&self) -> bool {
        std::mem::take(&mut *self.fail_next_instance_delete.borrow_mut())
    }

    fn take_instance_create_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.pause_next_instance_create.borrow_mut().take()
    }
}

impl fmt::Debug for FakeBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("FakeBackend").finish_non_exhaustive()
    }
}

impl backend::Backend for FakeBackend {
    fn supports_image(&self, _image: CatalogImage) -> bool {
        true
    }

    fn check_prerequisites<'a>(
        &'a self,
        _context: &'a Context,
        _report: &'a mut DoctorReport,
    ) -> backend::BackendValueFuture<'a, ()> {
        Box::pin(async {})
    }

    fn base_image_identity<'a>(
        &'a self,
        _manifest: &'a AgentManifest,
    ) -> backend::BackendFuture<'a, backend::BackendBaseImageIdentity> {
        Box::pin(async {
            Ok(backend::BackendBaseImageIdentity {
                base_key_schema: "test",
                os: "archlinux",
                architecture: "x86_64",
                variant: "cloud",
                cache_key: "image",
                url: "https://example.invalid/image.qcow2",
                format: "qcow2",
            })
        })
    }

    fn create_base<'a>(
        &'a self,
        _context: &'a Context,
        _input: backend::CreateBaseInput<'a>,
    ) -> backend::BackendFuture<'a, backend::CreateBaseOutput> {
        Box::pin(async {
            Ok(backend::CreateBaseOutput {
                state: BackendState::Qemu(fake_qemu_state()),
                image_cache_key: "image".to_owned(),
            })
        })
    }

    fn start_base<'a>(
        &'a self,
        _context: &'a Context,
        _manifest: &'a AgentManifestContext,
        _state: &'a mut AgentInstanceDocument,
    ) -> backend::BackendFuture<'a, backend::StartOutput> {
        Box::pin(async { Ok(start_output()) })
    }

    fn stop_base<'a>(
        &'a self,
        _context: &'a Context,
        _state: &'a mut AgentInstanceDocument,
    ) -> backend::BackendFuture<'a, backend::StopOutput> {
        Box::pin(async { Ok(stop_output()) })
    }

    fn stop_base_runtime<'a>(
        &'a self,
        _context: &'a Context,
        _agent: &'a crate::agent::AgentName,
        _key: &'a crate::agent::AgentBaseKey,
        _files: &'a AgentBaseFiles,
    ) -> backend::BackendFuture<'a, backend::StopOutput> {
        let fail = self.take_base_stop_failure();
        Box::pin(async move {
            if fail {
                Err(fake_backend_error())
            } else {
                Ok(stop_output())
            }
        })
    }

    fn create_instance<'a>(
        &'a self,
        _context: &'a Context,
        input: backend::CreateInstanceInput<'a>,
    ) -> backend::BackendFuture<'a, backend::CreateInstanceOutput> {
        let pause = self.take_instance_create_pause();
        self.created_instances.borrow_mut().push(input.instance);
        Box::pin(async move {
            if let Some(release) = pause {
                let _result = release.await;
            }
            Ok(backend::CreateInstanceOutput {
                state: fake_mediated_backend_state(),
                guest_access: None,
            })
        })
    }

    fn start_instance<'a>(
        &'a self,
        _context: &'a Context,
        _network: &'a InstanceNetwork,
        _manifest: &'a AgentManifestContext,
        state: &'a mut AgentInstanceDocument,
    ) -> backend::BackendFuture<'a, backend::StartOutput> {
        Box::pin(async {
            Ok(backend::StartOutput {
                process: process_status("running"),
                host_ports: state.status.network.ports.clone(),
            })
        })
    }

    fn exec<'a>(
        &'a self,
        _context: &'a Context,
        _state: &'a AgentInstanceDocument,
        command: &'a str,
        _timeout: std::time::Duration,
        output: &'a mut dyn OutputSink,
    ) -> backend::BackendFuture<'a, CommandOutput> {
        Box::pin(async move {
            let stdout = format!("executed: {command}");
            output.output(agentdp_platform::ssh::OutputStream::Stdout, stdout.as_bytes());
            Ok(CommandOutput {
                status: 0,
                stdout,
                stderr: String::new(),
            })
        })
    }

    fn wait_bootstrap<'a>(
        &'a self,
        _context: &'a Context,
        _state: &'a AgentInstanceDocument,
        bootstrap_events: Option<&'a mut dyn backend::BootstrapEventSink>,
    ) -> backend::BackendFuture<'a, ()> {
        Box::pin(async move {
            if let Some(events) = bootstrap_events {
                for step in ["system.packages", "system.guest_tooling"] {
                    events.emit(BootstrapEvent::StepStarted {
                        step: bootstrap_step_status(step, BootstrapLifecycleStatus::Running),
                    });
                    events.emit(BootstrapEvent::StepFinished {
                        step: step.to_owned(),
                        status: BootstrapStepStatus::Passed,
                        exit_status: 0,
                        duration_ms: 5,
                    });
                }
            }
            Ok(())
        })
    }

    fn stop_instance<'a>(
        &'a self,
        _context: &'a Context,
        _network: &'a InstanceNetwork,
        _input: backend::StopInstanceInput<'a>,
        _backend_state: &'a mut BackendState,
    ) -> backend::BackendFuture<'a, backend::StopOutput> {
        *self.instance_stops.borrow_mut() += 1;
        let fail = self.take_instance_delete_failure();
        Box::pin(async move {
            if fail {
                Err(fake_backend_error())
            } else {
                Ok(stop_output())
            }
        })
    }

    fn reconcile_instance<'a>(
        &'a self,
        _context: &'a Context,
        _network: &'a InstanceNetwork,
        _manifest: &'a AgentManifestContext,
        state: &'a mut AgentInstanceDocument,
    ) -> backend::BackendFuture<'a, backend::ReconcileOutput> {
        *self.instance_reconciles.borrow_mut() += 1;
        Box::pin(async move {
            Ok(backend::ReconcileOutput {
                stale: false,
                mark_stopped: false,
                backend_changed: false,
                process: process_status("running"),
                host_ports: state.status.network.ports.clone(),
            })
        })
    }

    fn ensure_attached<'a>(
        &'a self,
        _context: &'a Context,
        _network: &'a InstanceNetwork,
        _manifest: &'a AgentManifestContext,
        _state: &'a AgentInstanceDocument,
    ) -> backend::BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn log_path(&self, _backend_state: &BackendState, _file: LogFile) -> PathBuf {
        "/tmp/agentdp-agent-prototype.log".into()
    }

    fn shell_command<'a>(&'a self, _state: &'a AgentInstanceDocument) -> backend::BackendFuture<'a, HostCommandResult> {
        Box::pin(async {
            Ok(HostCommandResult {
                program: "ssh".to_owned(),
                args: Vec::new(),
            })
        })
    }
}

fn bootstrap_step_status(step: &str, status: BootstrapLifecycleStatus) -> AgentInstanceBootstrapStepStatus {
    AgentInstanceBootstrapStepStatus {
        step: step.to_owned(),
        label: Some(step.replace('.', " ")),
        phase: Some(BootstrapStepPhase::System),
        current: None,
        total: None,
        status: Some(status),
    }
}

fn start_output() -> backend::StartOutput {
    backend::StartOutput {
        process: process_status("running"),
        host_ports: std::collections::BTreeMap::new(),
    }
}

fn stop_output() -> backend::StopOutput {
    backend::StopOutput {
        process_status: "stopped",
        terminated_pid: Some(1),
    }
}

fn process_status(status: &str) -> ProcessStatus {
    ProcessStatus {
        pid: Some(1),
        status: status.to_owned(),
        message: None,
    }
}

fn fake_mediated_backend_state() -> BackendState {
    let profile = agentdp_core::mediated_network::DEFAULT_PROFILE;
    let mut state = fake_qemu_state();
    state.instance_network = Some(QemuInstanceNetworkState {
        addresses: agentdp_network::InstanceAddresses {
            gateway: agentdp_network::Ipv4AddressText::from_std(profile.gateway_ipv4),
            address: agentdp_network::Ipv4AddressText::from_std(profile.guest_ipv4),
            cidr_prefix: profile.ipv4_cidr_prefix,
        },
        stream_socket: "/tmp/agentdp-agent-test.sock".to_owned(),
    });
    BackendState::Qemu(state)
}

fn fake_qemu_state() -> QemuState {
    QemuState {
        image: QemuImageState {
            os: "archlinux".to_owned(),
            architecture: "x86_64".to_owned(),
            variant: "default".to_owned(),
            source_url: "https://example.invalid/image.qcow2".to_owned(),
            cache_key: "image".to_owned(),
            cache_path: "/tmp/image.qcow2".to_owned(),
            download_path: "/tmp/image.download".to_owned(),
            format: "qcow2".to_owned(),
        },
        disk: "/tmp/disk.qcow2".to_owned(),
        work_dir: "/tmp/work".to_owned(),
        seed_media: "/tmp/seed.img".to_owned(),
        seed_meta_data: "/tmp/meta-data".to_owned(),
        seed_network_config: "/tmp/network-config".to_owned(),
        seed_user_data: "/tmp/user-data".to_owned(),
        monitor_socket: "/tmp/monitor.sock".to_owned(),
        qmp_socket: "/tmp/qmp.sock".to_owned(),
        guest_control_socket: "/tmp/guest.sock".to_owned(),
        pid_file: "/tmp/qemu.pid".to_owned(),
        serial_log: "/tmp/serial.log".to_owned(),
        qemu_log: "/tmp/qemu.log".to_owned(),
        instance_network: None,
        mediated_secrets: SecretBindings::default(),
        mediated_ca: QemuMediatedCaState::default(),
        pid: None,
        last_start_unix_seconds: None,
    }
}

fn fake_backend_error() -> backend::Error {
    backend::Error::UnsupportedManifestImage {
        os: "test",
        architecture: "test",
        variant: "test",
    }
}
