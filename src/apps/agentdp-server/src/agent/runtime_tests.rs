use std::cell::RefCell;
use std::fmt;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use agentdp_core::Context;
use agentdp_core::agent::{
    AgentBaseKey, AgentDocument, AgentEvent, AgentInstanceBootstrapState, AgentInstanceBootstrapStepStatus,
    AgentInstanceDocument, AgentInstanceEvent, AgentInstanceEventEnvelope, AgentInstanceEventSource,
    AgentInstanceHostInputsPhase, AgentInstanceId, AgentInstanceNetworkEvent, AgentInstanceNetworkEventKind,
    AgentInstancePhase, AgentInstanceTarget, AgentStatusPhase, BackendState, BootstrapEvent, EventLevel, InstanceName,
    NetworkAllowState, NetworkIpv6State, NetworkModeState, NetworkState, OperationResult, PortProtocolState,
    ProcessStatus, QemuImageState, QemuInstanceNetworkState, QemuMediatedCaState, QemuState, ReconciliationState,
    assign_port_mappings,
};
use agentdp_core::doctor::DoctorReport;
use agentdp_core::manifest::plugins::codex::Codex;
use agentdp_core::manifest::plugins::{AuthMode, codex};
use agentdp_core::manifest::{AgentManifest, AgentPhase, GuestPort, NetworkMode, NetworkProtocol, Secret};
use agentdp_core::provisioning::SeedFile;
use agentdp_core::provisioning::image::CatalogImage;
use agentdp_core::provisioning::secrets::SecretBindings;
use agentdp_ds::local::{oneshot, spsc};
use agentdp_platform::ssh::{CommandOutput, OutputSink};
use agentdp_protocol::client_server::{
    AgentInstanceExecParams, AgentInstanceExecResult, AgentInstanceLogsParams, AgentInstanceLogsResult,
    HostCommandResult, LogFile, LogFilter, NetworkLogKind,
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
async fn malformed_persisted_agent_state_quarantines_agent() {
    let manifest = manifest_with(1);
    let (layout, agent_name) = unique_layout(&manifest);
    let path = layout.agent_document(&agent_name);
    let malformed = b"metadata: [invalid\n";
    tokio::fs::create_dir_all(path.parent().expect("agent document parent"))
        .await
        .expect("create agent directory");
    tokio::fs::write(&path, malformed)
        .await
        .expect("write malformed agent state");

    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        Rc::new(FakeBackend::default()),
        tailscale_service(),
    );
    let error = apply_result(&agent, manifest_context(manifest))
        .await
        .expect_err("apply must be rejected after persisted state fails to load");
    assert!(matches!(
        error,
        Error::PersistedStateUnavailable { message, .. } if message.contains("failed to parse agent document")
    ));
    assert!(
        !agent.is_finished(),
        "load failure must remain quarantined in the registry"
    );
    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::ListItems { respond })
        .expect("quarantined agent accepts list command");
    assert!(matches!(
        receive.await.expect("list response"),
        Err(Error::PersistedStateUnavailable { .. })
    ));
    assert_eq!(tokio::fs::read(path).await.expect("read agent state"), malformed);
}

#[tokio::test(flavor = "local")]
async fn malformed_persisted_instance_state_quarantines_agent() {
    let manifest = manifest_with(1);
    let (layout, agent_name) = unique_layout(&manifest);
    let _document = persisted_running_instance(&layout, &agent_name, &manifest).await;
    let path = layout.instance(&agent_name, AgentInstanceId::new(0)).instance_state();
    let agent_events = layout.agent_events(&agent_name);
    let instance_events = layout.instance(&agent_name, AgentInstanceId::new(0)).instance_events();
    let malformed = b"metadata: [invalid\n";
    tokio::fs::create_dir_all(path.parent().expect("instance document parent"))
        .await
        .expect("create instance directory");
    tokio::fs::write(&path, malformed)
        .await
        .expect("write malformed instance state");

    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        Rc::new(FakeBackend::default()),
        tailscale_service(),
    );
    let error = apply_result(&agent, manifest_context(manifest))
        .await
        .expect_err("apply must be rejected after persisted instance state fails to load");
    assert!(matches!(
        error,
        Error::PersistedStateUnavailable { message, .. } if message.contains("failed to parse instance document")
    ));
    assert!(
        !agent.is_finished(),
        "load failure must remain quarantined in the registry"
    );
    assert_eq!(tokio::fs::read(path).await.expect("read instance state"), malformed);
    assert!(
        !tokio::fs::try_exists(agent_events)
            .await
            .expect("inspect agent event log"),
        "document validation must finish before the agent event writer starts"
    );
    assert!(
        !tokio::fs::try_exists(instance_events)
            .await
            .expect("inspect instance event log"),
        "document validation must finish before instance event writers start"
    );
}

#[tokio::test(flavor = "local")]
async fn persisted_event_logs_are_all_validated_before_any_repair() {
    let manifest = manifest_with(2);
    let (layout, agent_name) = unique_layout(&manifest);
    let first = persisted_running_instance(&layout, &agent_name, &manifest).await;
    write_persisted_instance(&layout, &agent_name, AgentInstanceId::new(0), &first).await;
    let mut second = first.clone();
    second.metadata.id = AgentInstanceId::new(1);
    second.metadata.name = InstanceName::new("replica-1");
    write_persisted_instance(&layout, &agent_name, AgentInstanceId::new(1), &second).await;
    let first_events = layout.instance(&agent_name, AgentInstanceId::new(0)).instance_events();
    let second_events = layout.instance(&agent_name, AgentInstanceId::new(1)).instance_events();
    let torn = b"{\"sequence\":1}\n{\"sequence\":2";
    tokio::fs::write(&first_events, torn)
        .await
        .expect("write repairable event log");
    tokio::fs::write(&second_events, b"not-json\n")
        .await
        .expect("write corrupt event log");

    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        Rc::new(FakeBackend::default()),
        tailscale_service(),
    );
    let error = apply_result(&agent, manifest_context(manifest))
        .await
        .expect_err("apply must be rejected when an event log is corrupt");

    assert!(matches!(
        error,
        Error::PersistedStateUnavailable { message, .. }
            if message.contains("failed to find a valid event sequence")
    ));
    assert_eq!(
        tokio::fs::read(first_events).await.expect("read repairable event log"),
        torn,
        "a later validation failure must prevent earlier repair plans from being applied"
    );
}

#[tokio::test(flavor = "local")]
async fn manifest_change_during_bootstrap_does_not_wedge_instance_work() {
    let initial = manifest_with(1);
    let backend = Rc::new(FakeBackend::default());
    let release_bootstrap = backend.pause_next_instance_bootstrap();
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&initial);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(initial)).await;
    wait_for_instance_event(&mut stream, |event| {
        matches!(event, AgentInstanceEvent::BootstrapStarted)
    })
    .await;
    let changed = apply(&agent, manifest_context(manifest_with_code_server_host(1, 4090))).await;
    release_bootstrap.try_send(());

    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == changed.generation() && document.status.replicas.ready == 1
    })
    .await;
    assert_eq!(ready.status.observed_generation, changed.generation());
}

#[tokio::test(flavor = "local")]
async fn mediated_runtime_secrets_refresh_while_bootstrap_is_running() {
    let manifest = manifest_with_codex_mediated_auth(1);
    let backend = Rc::new(FakeBackend::default());
    let release_bootstrap = backend.pause_next_instance_bootstrap();
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&manifest);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(manifest)).await;
    wait_for_instance_event(&mut stream, |event| {
        matches!(event, AgentInstanceEvent::BootstrapStarted)
    })
    .await;

    wait_for_count(
        &backend.runtime_secret_reconciles,
        1,
        "runtime secret refresh during bootstrap",
    )
    .await;
    assert_eq!(
        *backend.host_input_reconciles.borrow(),
        0,
        "guest files must wait until bootstrap releases the control session"
    );

    release_bootstrap.try_send(());
    wait_for_ready(&mut stream, 1).await;
}

#[tokio::test(flavor = "local")]
async fn backend_state_reconcile_waits_for_runtime_secrets() {
    let manifest = manifest_with_codex_mediated_auth(1);
    let (layout, agent_name) = unique_layout(&manifest);
    persist_ready_running_instance(&layout, &agent_name, &manifest).await;
    let backend = Rc::new(FakeBackend::default());
    let release_secrets = backend.pause_next_runtime_secret_reconcile();
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    let AgentStreamItem::Document(_) = recv_stream_item(&mut stream).await else {
        panic!("first stream item should be loaded document");
    };
    wait_for_count(&backend.runtime_secret_reconciles, 1, "runtime secret refresh").await;
    assert_eq!(
        *backend.instance_reconciles.borrow(),
        0,
        "backend state reconciliation must not race runtime secret refresh"
    );

    release_secrets.try_send(());
    wait_for_count(&backend.instance_reconciles, 1, "backend state reconciliation").await;
}

#[tokio::test(flavor = "local")]
async fn runtime_secret_failure_is_reported_and_triggers_backend_reconciliation() {
    let manifest = manifest_with_codex_mediated_auth(1);
    let backend = Rc::new(FakeBackend::default());
    backend.fail_runtime_secret_reconciles(1);
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&manifest);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let _stream = watch(&agent).await;

    apply(&agent, manifest_context(manifest)).await;
    wait_for_count(&backend.runtime_secret_reconciles, 1, "failed runtime secret refresh").await;
    wait_for_count(
        &backend.instance_reconciles,
        1,
        "backend reconciliation after runtime secret failure",
    )
    .await;
    let degraded = document(&agent).await;
    let degraded_status = degraded.status.instances.get(&AgentInstanceId::new(0)).unwrap();
    assert_eq!(degraded.status.replicas.ready, 0);
    assert!(degraded.status.reconciling);
    assert_eq!(degraded_status.host_inputs.phase, AgentInstanceHostInputsPhase::Failed);
    assert_eq!(degraded_status.host_inputs.observed_generation, degraded.generation());
    assert!(
        degraded_status
            .host_inputs
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("runtime secret refresh failed"))
    );
}

#[tokio::test(flavor = "local")]
async fn failed_runtime_secret_repair_preserves_running_vm_during_backoff() {
    let manifest = manifest_with_codex_mediated_auth(1);
    let backend = Rc::new(FakeBackend::default());
    backend.fail_runtime_secret_reconciles(1);
    backend.fail_instance_reconciles(1);
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&manifest);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let _stream = watch(&agent).await;

    apply(&agent, manifest_context(manifest)).await;
    wait_for_count(&backend.instance_reconciles, 1, "failed backend repair").await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(
        *backend.instance_starts.borrow(),
        1,
        "failed ancillary repair must not restart a still-running VM"
    );
    assert_eq!(
        *backend.runtime_secret_reconciles.borrow(),
        1,
        "runtime-secret backoff must govern the next repair attempt"
    );
}

#[tokio::test(flavor = "local")]
async fn persisted_running_instance_preserves_runtime_repair_backoff() {
    let manifest = manifest_with_codex_mediated_auth(1);
    let (layout, agent_name) = unique_layout(&manifest);
    persist_ready_running_instance(&layout, &agent_name, &manifest).await;
    let backend = Rc::new(FakeBackend::default());
    backend.fail_runtime_secret_reconciles(1);
    backend.fail_instance_reconciles(2);
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    let AgentStreamItem::Document(_) = recv_stream_item(&mut stream).await else {
        panic!("first stream item should be loaded document");
    };
    wait_for_count(&backend.runtime_secret_reconciles, 1, "failed runtime secret refresh").await;
    wait_for_count(&backend.instance_reconciles, 1, "failed runtime repair").await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(
        *backend.instance_reconciles.borrow(),
        1,
        "ordinary lifecycle reconciliation must remain suppressed during repair backoff"
    );
    assert_eq!(
        *backend.instance_starts.borrow(),
        0,
        "failed repair must not start another VM for a restored running instance"
    );
}

#[tokio::test(flavor = "local")]
async fn guest_file_reconcile_uses_runtime_secret_snapshot() {
    let manifest = manifest_with_codex_mediated_auth(1);
    let backend = Rc::new(FakeBackend::default());
    let secret_file = SeedFile {
        path: "/data/home/.codex/auth.json".to_owned(),
        contents: br#"{"tokens":{"future_token":"snapshot-placeholder"}}"#.to_vec(),
        permissions: "0600".to_owned(),
        owner: None,
    };
    backend.set_runtime_secret_files(vec![secret_file.clone()]);
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&manifest);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(manifest)).await;
    wait_for_ready(&mut stream, 1).await;
    wait_for_host_input_reconcile_count(&backend, 1).await;

    assert_eq!(backend.reconciled_secret_files.borrow().as_slice(), &[secret_file]);
}

#[tokio::test(flavor = "local")]
async fn guest_files_reconcile_after_terminal_bootstrap_failure() {
    let manifest = manifest_with(1);
    let backend = Rc::new(FakeBackend::default());
    backend.fail_instance_bootstraps(1);
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&manifest);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(manifest)).await;
    wait_for_instance_event(&mut stream, |event| {
        matches!(
            event,
            AgentInstanceEvent::BootstrapFinished {
                result: OperationResult::Failed { .. }
            }
        )
    })
    .await;
    wait_for_host_input_reconcile_count(&backend, 1).await;

    assert_eq!(*backend.host_input_reconciles.borrow(), 1);
}

#[tokio::test(flavor = "local")]
async fn template_change_during_instance_create_discards_obsolete_instance() {
    let initial = manifest_with(1);
    let backend = Rc::new(FakeBackend::default());
    let release_create = backend.pause_next_instance_create();
    let release_cleanup = backend.pause_next_instance_stop();
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&initial);
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name.clone(),
        layout.clone(),
        agent_backend,
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(initial)).await;
    wait_for_created_instances(&backend, 1).await;
    let mut changed = manifest_with(1);
    changed.spec.template.resources.memory = "3G".to_owned();
    let changed = apply(&agent, manifest_context(changed)).await;
    assert!(release_create.send(()).is_ok());
    wait_for_count(&backend.instance_stops, 1, "obsolete instance cleanup").await;
    let deleting = status(&agent, AgentInstanceId::new(0))
        .await
        .expect("obsolete instance remains addressable during cleanup");
    assert_eq!(deleting.spec.target, AgentInstanceTarget::Active);
    assert_eq!(deleting.status.phase, AgentInstancePhase::Deleting);
    let persisted = tokio::fs::read_to_string(layout.instance(&agent_name, AgentInstanceId::new(0)).instance_state())
        .await
        .expect("read persisted cleanup marker");
    let persisted: AgentInstanceDocument = serde_yaml::from_str(&persisted).expect("parse persisted cleanup marker");
    assert_eq!(persisted.status.phase, AgentInstancePhase::Deleting);
    assert!(release_cleanup.send(()).is_ok());

    wait_for_document(&mut stream, |document| {
        document.generation() == changed.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 1
    })
    .await;
    assert_eq!(backend.created_instances.borrow().len(), 2);
    assert_eq!(*backend.instance_stops.borrow(), 1);
}

#[tokio::test(flavor = "local")]
async fn scale_up_during_instance_create_keeps_compatible_materialization() {
    let initial = manifest_with(1);
    let backend = Rc::new(FakeBackend::default());
    let release_create = backend.pause_next_instance_create();
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&initial);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(initial)).await;
    wait_for_created_instances(&backend, 1).await;
    let scaled = scale(&agent, 2).await;
    assert!(release_create.send(()).is_ok());

    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == scaled.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 2
    })
    .await;
    assert_eq!(ready.status.replicas.active, 2);
    assert_eq!(backend.created_instances.borrow().len(), 2);
    assert_eq!(*backend.instance_stops.borrow(), 0);
}

#[tokio::test(flavor = "local")]
async fn stale_instance_create_failure_is_not_published_for_latest_generation() {
    let initial = manifest_with(1);
    let backend = Rc::new(FakeBackend::default());
    backend.fail_instance_creates(1);
    let release_create = backend.pause_next_instance_create();
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&initial);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(initial)).await;
    wait_for_created_instances(&backend, 1).await;
    let mut changed = manifest_with(1);
    changed.spec.template.resources.memory = "3G".to_owned();
    let changed = apply(&agent, manifest_context(changed)).await;
    assert!(release_create.send(()).is_ok());

    let mut stale_failure_published = false;
    loop {
        match recv_stream_item(&mut stream).await {
            AgentStreamItem::Event(event) => {
                stale_failure_published |= matches!(
                    event.event,
                    AgentEvent::Diagnostic { ref message, .. } if message.contains("failed to create instance")
                );
            }
            AgentStreamItem::Document(document)
                if document.generation() == changed.generation()
                    && document.status.observed_generation == document.generation()
                    && document.status.replicas.ready == 1 =>
            {
                break;
            }
            AgentStreamItem::Document(_) => {}
        }
    }
    assert!(!stale_failure_published);
    assert_eq!(backend.created_instances.borrow().len(), 2);
}

#[tokio::test(flavor = "local")]
async fn persisted_deleting_instance_is_cleaned_before_latest_recreation() {
    let initial = manifest_with(1);
    let mut changed = manifest_with(1);
    changed.spec.template.resources.memory = "3G".to_owned();
    let (layout, agent_name) = unique_layout(&initial);
    let initial_document =
        AgentDocument::from_manifest("/tmp/initial.yaml", agent_name.clone(), &initial).expect("initial document");
    let mut current_document = AgentDocument::from_manifest_after_existing(
        "/tmp/changed.yaml",
        agent_name.clone(),
        &changed,
        &initial_document,
    )
    .expect("changed document");
    let current_base = AgentBaseKey::new("sha256-current");
    current_document.mark_agent_base_ready(current_base);
    let agent_path = layout.agent_document(&agent_name);
    tokio::fs::create_dir_all(agent_path.parent().expect("agent document parent"))
        .await
        .expect("create agent directory");
    tokio::fs::write(
        &agent_path,
        serde_yaml::to_string(&current_document).expect("serialize agent document"),
    )
    .await
    .expect("persist agent document");

    let id = AgentInstanceId::new(0);
    let backend_state = fake_mediated_backend_state();
    let network = NetworkState::new(
        &backend_state,
        NetworkAllowState::from(&initial.spec.network.allow),
        NetworkIpv6State::default(),
        assign_port_mappings(&initial, id).expect("old port mappings"),
    );
    let obsolete = AgentInstanceDocument::new(
        agent_name.clone(),
        id,
        InstanceName::new("replica-0"),
        initial_document.generation(),
        AgentBaseKey::new("sha256-obsolete"),
        initial.spec.template,
        AgentInstanceTarget::Active,
        AgentInstancePhase::Deleting,
        network,
        Vec::new(),
        None,
        backend_state,
    );
    write_persisted_instance(&layout, &agent_name, id, &obsolete).await;

    let backend = Rc::new(FakeBackend::default());
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;
    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == current_document.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 1
    })
    .await;

    assert_eq!(ready.generation(), 2);
    assert_eq!(*backend.instance_stops.borrow(), 1);
    assert_eq!(backend.created_instances.borrow().len(), 1);
}

#[tokio::test(flavor = "local")]
async fn persisted_deleted_instance_finishes_file_removal_without_repeating_stop() {
    let manifest = manifest_with(1);
    let (layout, agent_name) = unique_layout(&manifest);
    let mut deleted = persisted_running_instance(&layout, &agent_name, &manifest).await;
    deleted.status.phase = AgentInstancePhase::Deleted;
    deleted.status.clear_readiness();
    write_persisted_instance(&layout, &agent_name, AgentInstanceId::new(0), &deleted).await;

    let backend = Rc::new(FakeBackend::default());
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    wait_for_ready(&mut stream, 1).await;

    assert_eq!(*backend.instance_stops.borrow(), 0);
    assert_eq!(backend.created_instances.borrow().len(), 1);
}

#[tokio::test(flavor = "local")]
async fn persisted_deleted_inactive_instance_does_not_recreate_its_document() {
    let manifest = manifest_with(0);
    let (layout, agent_name) = unique_layout(&manifest);
    let mut deleted = persisted_running_instance(&layout, &agent_name, &manifest).await;
    deleted.status.phase = AgentInstancePhase::Deleted;
    deleted.status.clear_readiness();
    let id = AgentInstanceId::new(0);
    write_persisted_instance(&layout, &agent_name, id, &deleted).await;

    let agent = Agent::spawn(
        Context::quiet(),
        agent_name.clone(),
        layout.clone(),
        Rc::new(FakeBackend::default()),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    wait_for_event(&mut stream, |event| {
        matches!(
            event,
            AgentEvent::InstanceDeleted { instance_id } if *instance_id == id
        )
    })
    .await;
    wait_for_document(&mut stream, |document| {
        document.status.instances.is_empty() && !document.status.reconciling
    })
    .await;

    assert!(
        !tokio::fs::try_exists(layout.instance(&agent_name, id).instance_state())
            .await
            .expect("inspect removed instance document")
    );
}

#[tokio::test(flavor = "local")]
async fn stale_base_failure_is_not_published_for_latest_generation() {
    let initial = manifest_with(1);
    let backend = Rc::new(FakeBackend::default());
    backend.fail_base_creates(1);
    let release_base = backend.pause_next_base_create();
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&initial);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(initial)).await;
    wait_for_count(&backend.created_bases, 1, "obsolete agent base create").await;
    let mut changed = manifest_with(1);
    changed.spec.bootstrap.packages.push("htop".to_owned());
    let changed = apply(&agent, manifest_context(changed)).await;
    assert!(release_base.send(()).is_ok());

    let mut stale_failure_published = false;
    loop {
        match recv_stream_item(&mut stream).await {
            AgentStreamItem::Event(event) => {
                stale_failure_published |= matches!(event.event, AgentEvent::AgentBaseFailed { .. });
            }
            AgentStreamItem::Document(document)
                if document.generation() == changed.generation()
                    && document.status.observed_generation == document.generation()
                    && document.status.replicas.ready == 1 =>
            {
                assert!(document.status.agent_base.last_error.is_none());
                break;
            }
            AgentStreamItem::Document(_) => {}
        }
    }
    assert!(!stale_failure_published);
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
async fn scale_back_up_while_stop_is_running_converges_to_latest_generation() {
    let (agent, mut stream, backend) = start_agent_with_backend(&manifest_with(1)).await;
    wait_for_ready(&mut stream, 1).await;
    let release_stop = backend.pause_next_instance_stop();

    scale(&agent, 0).await;
    wait_for_count(&backend.instance_stops, 1, "instance stop").await;
    let restarted = scale(&agent, 1).await;
    release_stop.try_send(());

    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == restarted.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 1
    })
    .await;
    assert_eq!(ready.status.replicas.active, 1);
}

#[tokio::test(flavor = "local")]
async fn stale_stop_completion_preserves_latest_desired_port_mapping() {
    let initial = manifest_with_code_server_host(1, 4090);
    let (agent, mut stream, backend) = start_agent_with_backend(&initial).await;
    wait_for_ready(&mut stream, 1).await;
    let release_stop = backend.pause_next_instance_stop();

    scale(&agent, 0).await;
    wait_for_count(&backend.instance_stops, 1, "instance stop").await;
    let changed = apply(&agent, manifest_context(manifest_with_code_server_host(1, 4091))).await;
    release_stop.try_send(());

    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == changed.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 1
    })
    .await;
    assert_eq!(
        ready.status.instances[&AgentInstanceId::new(0)].network.ports["code_server"].host,
        Some(4091)
    );
}

#[tokio::test(flavor = "local")]
async fn stale_start_completion_preserves_latest_desired_port_mapping() {
    let initial = manifest_with_code_server_host(1, 4090);
    let backend = Rc::new(FakeBackend::default());
    let release_start = backend.pause_next_instance_start();
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&initial);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(initial)).await;
    wait_for_count(&backend.instance_starts, 1, "instance start").await;
    let changed = apply(&agent, manifest_context(manifest_with_code_server_host(1, 4091))).await;
    assert!(release_start.send(()).is_ok());

    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == changed.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 1
    })
    .await;
    assert_eq!(
        ready.status.instances[&AgentInstanceId::new(0)].network.ports["code_server"].host,
        Some(4091)
    );
    assert_eq!(*backend.instance_starts.borrow(), 2);
    assert_eq!(*backend.instance_stops.borrow(), 1);
    assert_eq!(backend.created_instances.borrow().len(), 2);
}

#[tokio::test(flavor = "local")]
async fn scale_up_during_instance_start_keeps_compatible_materialization() {
    let initial = manifest_with(1);
    let backend = Rc::new(FakeBackend::default());
    let release_start = backend.pause_next_instance_start();
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&initial);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(initial)).await;
    wait_for_count(&backend.instance_starts, 1, "instance start").await;
    let scaled = scale(&agent, 2).await;
    assert!(release_start.send(()).is_ok());

    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == scaled.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 2
    })
    .await;
    assert_eq!(ready.status.replicas.active, 2);
    assert_eq!(backend.created_instances.borrow().len(), 2);
    assert_eq!(*backend.instance_starts.borrow(), 2);
    assert_eq!(*backend.instance_stops.borrow(), 0);
}

#[tokio::test(flavor = "local")]
async fn stale_start_cleanup_does_not_wait_for_replacement_base() {
    let initial = manifest_with(1);
    let backend = Rc::new(FakeBackend::default());
    let release_start = backend.pause_next_instance_start();
    let agent_backend: backend::BackendRef = backend.clone();
    let (layout, agent_name) = unique_layout(&initial);
    let agent = Agent::spawn(Context::quiet(), agent_name, layout, agent_backend, tailscale_service());
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(initial)).await;
    wait_for_count(&backend.instance_starts, 1, "obsolete instance start").await;
    let release_base = backend.pause_next_base_create();
    let mut changed_manifest = manifest_with(1);
    changed_manifest.spec.bootstrap.packages.push("htop".to_owned());
    let changed = apply(&agent, manifest_context(changed_manifest)).await;
    wait_for_count(&backend.created_bases, 2, "replacement agent base build").await;

    assert!(release_start.send(()).is_ok());
    wait_for_count(
        &backend.instance_stops,
        1,
        "obsolete instance cleanup before replacement base",
    )
    .await;
    assert!(release_base.send(()).is_ok());

    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == changed.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 1
    })
    .await;
    assert_eq!(ready.ready_agent_base_key(), ready.desired_agent_base_key());
}

#[tokio::test(flavor = "local")]
async fn changed_inactive_instance_deletes_stale_materialization() {
    let initial = manifest_with_code_server_host(1, 4090);
    let (agent, mut stream) = start_agent(&initial).await;
    wait_for_ready(&mut stream, 1).await;
    let scaled = scale(&agent, 0).await;
    wait_for_document(&mut stream, |document| {
        document.generation() == scaled.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.active == 0
    })
    .await;

    let mut changed_manifest = manifest_with_code_server_host(0, 4091);
    changed_manifest.spec.template.resources.memory = "3G".to_owned();
    let changed = apply(&agent, manifest_context(changed_manifest.clone())).await;

    let observed = wait_for_document(&mut stream, |document| {
        document.generation() == changed.generation()
            && document.status.observed_generation == document.generation()
            && document.status.instances.is_empty()
            && !document.status.reconciling
    })
    .await;
    assert_eq!(observed.status.replicas.desired, 0);
    assert!(status(&agent, AgentInstanceId::new(0)).await.is_err());
}

#[tokio::test(flavor = "local")]
async fn reapply_while_instance_delete_is_running_recreates_latest_generation() {
    let manifest = manifest_with(1);
    let (agent, mut stream, backend) = start_agent_with_backend(&manifest).await;
    wait_for_ready(&mut stream, 1).await;
    let release_stop = backend.pause_next_instance_stop();

    delete(&agent).await;
    wait_for_count(&backend.instance_stops, 1, "instance delete").await;
    let reapplied = apply(&agent, manifest_context(manifest)).await;
    release_stop.try_send(());

    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == reapplied.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 1
    })
    .await;
    assert!(!ready.deletion_requested());
}

#[tokio::test(flavor = "local")]
async fn reapply_while_base_stop_is_running_rebuilds_latest_generation() {
    let manifest = manifest_with(1);
    let (agent, mut stream, backend) = start_agent_with_backend(&manifest).await;
    wait_for_ready(&mut stream, 1).await;
    let release_base_stop = backend.pause_next_base_stop();

    delete(&agent).await;
    wait_for_count(&backend.base_stops, 1, "agent base stop").await;
    let reapplied = apply(&agent, manifest_context(manifest)).await;
    release_base_stop.try_send(());

    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == reapplied.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 1
    })
    .await;
    assert!(!ready.deletion_requested());
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
async fn apply_changed_manifest_rebuilds_base_and_replaces_instances() {
    let (agent, mut stream, backend) = start_agent_with_backend(&manifest_with(1)).await;
    let initial = wait_for_ready(&mut stream, 1).await;
    let mut changed = manifest_with(1);
    changed.spec.bootstrap.packages.push("htop".to_owned());

    let accepted = apply(&agent, manifest_context(changed)).await;
    assert_eq!(accepted.generation(), initial.generation() + 1);

    let reconciled = wait_for_ready(&mut stream, 1).await;
    assert_eq!(reconciled.generation(), initial.generation() + 1);
    assert_ne!(reconciled.ready_agent_base_key(), initial.ready_agent_base_key());
    assert_eq!(reconciled.ready_agent_base_key(), reconciled.desired_agent_base_key());
    assert_eq!(backend.created_instances.borrow().len(), 2);
    assert_eq!(*backend.instance_stops.borrow(), 1);
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
async fn apply_rejects_mediated_auth_binding_change_after_provisioning() {
    let manifest = manifest_with_codex_mediated_auth(1);
    let (agent, mut stream) = start_agent(&manifest).await;
    wait_for_ready(&mut stream, 1).await;
    let mut changed = manifest_with(1);
    changed.metadata.name.clone_from(&manifest.metadata.name);
    changed.spec.template.network.mode = NetworkMode::Mediated;

    let error = apply_result(&agent, manifest_context(changed))
        .await
        .expect_err("mediated auth binding change should be rejected");

    assert!(error.to_string().contains("mediated auth inputs cannot change"));
}

#[tokio::test(flavor = "local")]
async fn apply_allows_mediated_auth_host_policy_change_after_provisioning() {
    let manifest = manifest_with_top_level_mediated_secret(1, &["api.github.com"]);
    let (agent, mut stream) = start_agent(&manifest).await;
    wait_for_ready(&mut stream, 1).await;
    let changed = manifest_with_top_level_mediated_secret(1, &["api.github.com", "uploads.github.com"]);

    let document = apply_result(&agent, manifest_context(changed))
        .await
        .expect("host policy change should be accepted");

    assert_eq!(document.manifest().spec.template.secrets[0].allow_hosts.len(), 2);
}

#[tokio::test(flavor = "local")]
async fn apply_allows_top_level_mediated_secret_reordering_after_provisioning() {
    let manifest = manifest_with_top_level_mediated_secrets(
        1,
        &[
            ("ALTINN_DEV_KEY", &["api.github.com"][..]),
            ("ALTINN_REFRESH_TOKEN", &["chatgpt.com"][..]),
        ],
    );
    let (agent, mut stream) = start_agent(&manifest).await;
    wait_for_ready(&mut stream, 1).await;
    let changed = manifest_with_top_level_mediated_secrets(
        1,
        &[
            ("ALTINN_REFRESH_TOKEN", &["chatgpt.com"][..]),
            ("ALTINN_DEV_KEY", &["api.github.com"][..]),
        ],
    );

    let document = apply_result(&agent, manifest_context(changed))
        .await
        .expect("reordering same mediated secrets should be accepted");

    assert_eq!(document.manifest().spec.template.secrets[0].env, "ALTINN_REFRESH_TOKEN");
    assert_eq!(document.manifest().spec.template.secrets[1].env, "ALTINN_DEV_KEY");
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
async fn failed_instance_creation_does_not_retry_in_a_busy_loop() {
    let backend = Rc::new(FakeBackend::default());
    backend.fail_instance_creates(1);
    let agent = Agent::spawn(
        Context::quiet(),
        AgentName::new("altinn-studio"),
        unique_layout(&manifest_with(1)).0,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(manifest_with(1))).await;
    wait_for_event(&mut stream, |event| {
        matches!(
            event,
            AgentEvent::Diagnostic { message, .. } if message.contains("failed to create instance")
        )
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(
        backend.created_instances.borrow().len(),
        1,
        "creation failure must occupy the instance slot until the retry deadline"
    );
}

#[tokio::test(flavor = "local")]
async fn failed_base_creation_does_not_retry_in_a_busy_loop() {
    let backend = Rc::new(FakeBackend::default());
    backend.fail_base_creates(1);
    let manifest = manifest_with(1);
    let agent = Agent::spawn(
        Context::quiet(),
        AgentName::new(manifest.name()),
        unique_layout(&manifest).0,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(manifest)).await;
    wait_for_event(&mut stream, |event| matches!(event, AgentEvent::AgentBaseFailed { .. })).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(
        *backend.created_bases.borrow(),
        1,
        "base setup failure must not retry until desired state changes"
    );
}

#[tokio::test(flavor = "local")]
async fn failed_host_input_reconcile_does_not_retry_in_a_busy_loop() {
    let backend = Rc::new(FakeBackend::default());
    backend.fail_host_input_reconciles(1);
    let (layout, agent_name) = unique_layout(&manifest_with(1));
    persist_ready_running_instance(&layout, &agent_name, &manifest_with(1)).await;
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name.clone(),
        layout.clone(),
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    wait_for_event(&mut stream, |event| {
        matches!(
            event,
            AgentEvent::Diagnostic { message, .. } if message.contains("failed to reconcile host inputs")
        )
    })
    .await;
    let degraded = document(&agent).await;
    assert_eq!(degraded.status.replicas.ready, 0);
    assert!(degraded.status.reconciling);
    let degraded_status = degraded.status.instances.get(&AgentInstanceId::new(0)).unwrap();
    assert_eq!(degraded_status.host_inputs.phase, AgentInstanceHostInputsPhase::Failed);
    assert_eq!(degraded_status.host_inputs.observed_generation, degraded.generation());
    assert!(
        degraded_status
            .host_inputs
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("host input reconciliation failed"))
    );
    let persisted_path = layout.instance(&agent_name, AgentInstanceId::new(0)).instance_state();
    let persisted: AgentInstanceDocument = serde_yaml::from_str(
        &tokio::fs::read_to_string(&persisted_path)
            .await
            .expect("read persisted degraded instance"),
    )
    .expect("parse persisted degraded instance");
    assert_eq!(persisted.status.host_inputs, degraded_status.host_inputs);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(
        *backend.host_input_reconciles.borrow(),
        1,
        "host input failure must wait for retry delay before re-reading host files"
    );

    drop(agent);
    drop(stream);
    let recovered_backend = Rc::new(FakeBackend::default());
    let release_recovery = recovered_backend.pause_next_runtime_secret_reconcile();
    let recovered = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        recovered_backend.clone(),
        tailscale_service(),
    );
    let mut recovered_stream = watch(&recovered).await;
    wait_for_count(
        &recovered_backend.runtime_secret_reconciles,
        1,
        "runtime secret recovery after restart",
    )
    .await;
    let restored = status(&recovered, AgentInstanceId::new(0))
        .await
        .expect("restored instance status");
    assert_eq!(restored.status.host_inputs, persisted.status.host_inputs);

    assert!(release_recovery.send(()).is_ok());
    let recovered_document = wait_for_ready(&mut recovered_stream, 1).await;
    let recovered_status = recovered_document
        .status
        .instances
        .get(&AgentInstanceId::new(0))
        .unwrap();
    assert_eq!(recovered_status.host_inputs.phase, AgentInstanceHostInputsPhase::Ready);
    assert_eq!(
        recovered_status.host_inputs.observed_generation,
        recovered_document.generation()
    );
    assert!(recovered_status.host_inputs.last_error.is_none());
}

#[tokio::test(flavor = "local")]
async fn scale_to_zero_waits_for_in_flight_host_input_reconcile() {
    let backend = Rc::new(FakeBackend::default());
    let release_host_inputs = backend.pause_next_host_input_reconcile();
    let manifest = manifest_with(1);
    let (layout, agent_name) = unique_layout(&manifest);
    persist_ready_running_instance(&layout, &agent_name, &manifest).await;
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    wait_for_host_input_reconcile_count(&backend, 1).await;
    scale(&agent, 0).await;
    assert!(release_host_inputs.send(()).is_ok());
    let stopped = wait_for_document(&mut stream, |document| document.status.replicas.stopped == 1).await;

    assert_eq!(stopped.status.replicas.stopped, 1);
    assert_eq!(*backend.host_input_side_effects.borrow(), 1);
}

#[tokio::test(flavor = "local")]
async fn delete_waits_for_in_flight_host_input_reconcile() {
    let backend = Rc::new(FakeBackend::default());
    let release_host_inputs = backend.pause_next_host_input_reconcile();
    let manifest = manifest_with(1);
    let (layout, agent_name) = unique_layout(&manifest);
    persist_ready_running_instance(&layout, &agent_name, &manifest).await;
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    wait_for_host_input_reconcile_count(&backend, 1).await;
    delete(&agent).await;
    assert!(release_host_inputs.send(()).is_ok());
    let deleted = wait_for_document(&mut stream, |document| document.status.deleted).await;

    assert!(deleted.status.instances.is_empty());
    assert_eq!(*backend.host_input_side_effects.borrow(), 1);
}

#[tokio::test(flavor = "local")]
async fn superseded_host_input_completion_does_not_replace_newer_attempt() {
    let backend = Rc::new(FakeBackend::default());
    let release_old_host_inputs = backend.pause_next_host_input_reconcile();
    let release_new_host_inputs = backend.pause_next_host_input_reconcile();
    let manifest = manifest_with(1);
    let (layout, agent_name) = unique_layout(&manifest);
    persist_ready_running_instance(&layout, &agent_name, &manifest).await;
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    wait_for_host_input_reconcile_count(&backend, 1).await;
    scale(&agent, 0).await;
    wait_for_ready(&mut stream, 0).await;
    scale(&agent, 1).await;
    assert!(release_old_host_inputs.send(()).is_ok());
    wait_for_ready(&mut stream, 1).await;
    wait_for_host_input_reconcile_count(&backend, 2).await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        *backend.host_input_reconciles.borrow(),
        2,
        "the superseded completion must not admit a concurrent third reconcile"
    );
    assert!(release_new_host_inputs.send(()).is_ok());
}

#[tokio::test(flavor = "local")]
async fn manifest_change_during_host_input_reconcile_starts_new_generation() {
    let backend = Rc::new(FakeBackend::default());
    let release_host_inputs = backend.pause_next_host_input_reconcile();
    let initial = manifest_with(1);
    let (layout, agent_name) = unique_layout(&initial);
    persist_ready_running_instance(&layout, &agent_name, &initial).await;
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    wait_for_host_input_reconcile_count(&backend, 1).await;
    let changed = apply(&agent, manifest_context(manifest_with_code_server_host(1, 4090))).await;
    let projected = status(&agent, AgentInstanceId::new(0))
        .await
        .expect("instance remains addressable while stale work finishes");
    assert_eq!(projected.spec.desired_generation, changed.generation());
    assert_eq!(projected.status.phase, AgentInstancePhase::Deleting);
    assert!(
        release_host_inputs.send(()).is_ok(),
        "superseded host-input work must finish instead of being cancelled"
    );

    wait_for_host_input_reconcile_count(&backend, 2).await;
    assert_eq!(
        *backend.host_input_side_effects.borrow(),
        2,
        "the completed old effect must be followed by reconciliation of the current generation"
    );
    let ready = wait_for_document(&mut stream, |document| {
        document.generation() == changed.generation()
            && document.status.replicas.ready == 1
            && document.status.observed_generation == document.generation()
    })
    .await;
    assert_eq!(ready.generation(), changed.generation());
}

#[tokio::test(flavor = "local")]
async fn manifest_change_finishes_host_inputs_after_partial_side_effect() {
    let backend = Rc::new(FakeBackend::default());
    let release_old_host_inputs = backend.pause_next_host_input_after_side_effect();
    let initial = manifest_with(1);
    let (layout, agent_name) = unique_layout(&initial);
    persist_ready_running_instance(&layout, &agent_name, &initial).await;
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    wait_for_count(&backend.host_input_side_effects, 1, "partial host-input side effect").await;
    let changed = apply(&agent, manifest_context(manifest_with_code_server_host(1, 4090))).await;
    assert!(release_old_host_inputs.send(()).is_ok());

    wait_for_host_input_reconcile_count(&backend, 2).await;
    wait_for_count(
        &backend.host_input_completed_side_effects,
        2,
        "superseded and current host-input completion",
    )
    .await;
    wait_for_document(&mut stream, |document| {
        document.generation() == changed.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 1
    })
    .await;
    assert_eq!(*backend.host_input_side_effects.borrow(), 2);
    assert_eq!(*backend.host_input_completed_side_effects.borrow(), 2);
}

#[tokio::test(flavor = "local")]
async fn deleted_agent_reapply_does_not_reuse_host_input_attempt_owner() {
    let backend = Rc::new(FakeBackend::default());
    let release_old_host_inputs = backend.pause_next_host_input_reconcile();
    let release_new_host_inputs = backend.pause_next_host_input_reconcile();
    let manifest = manifest_with(1);
    let (layout, agent_name) = unique_layout(&manifest);
    persist_ready_running_instance(&layout, &agent_name, &manifest).await;
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    wait_for_host_input_reconcile_count(&backend, 1).await;
    delete(&agent).await;
    assert!(release_old_host_inputs.send(()).is_ok());
    wait_for_document(&mut stream, |document| document.status.deleted).await;
    apply(&agent, manifest_context(manifest)).await;
    wait_for_host_input_reconcile_count(&backend, 2).await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        *backend.host_input_reconciles.borrow(),
        2,
        "the deleted instance completion must not replace the recreated instance attempt"
    );
    assert!(release_new_host_inputs.send(()).is_ok());
    wait_for_ready(&mut stream, 1).await;
}

#[tokio::test(flavor = "local")]
async fn apply_after_failed_instance_creation_retries_new_generation() {
    let backend = Rc::new(FakeBackend::default());
    backend.fail_instance_creates(1);
    let initial = manifest_with(1);
    let agent = Agent::spawn(
        Context::quiet(),
        AgentName::new(initial.name()),
        unique_layout(&initial).0,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(initial)).await;
    wait_for_event(&mut stream, |event| {
        matches!(
            event,
            AgentEvent::Diagnostic { message, .. } if message.contains("failed to create instance")
        )
    })
    .await;
    let mut changed = manifest_with(1);
    changed.spec.bootstrap.packages.push("htop".to_owned());
    apply(&agent, manifest_context(changed)).await;

    let ready = wait_for_ready(&mut stream, 1).await;

    assert_eq!(ready.status.replicas.ready, 1);
    assert_eq!(backend.created_instances.borrow().len(), 2);
}

#[tokio::test(flavor = "local")]
async fn delete_after_failed_instance_creation_converges_without_waiting_for_retry() {
    let backend = Rc::new(FakeBackend::default());
    backend.fail_instance_creates(1);
    let manifest = manifest_with(1);
    let agent = Agent::spawn(
        Context::quiet(),
        AgentName::new(manifest.name()),
        unique_layout(&manifest).0,
        backend,
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(manifest)).await;
    wait_for_event(&mut stream, |event| {
        matches!(
            event,
            AgentEvent::Diagnostic { message, .. } if message.contains("failed to create instance")
        )
    })
    .await;
    delete(&agent).await;

    let deleted = wait_for_document(&mut stream, |document| document.status.deleted).await;
    assert!(deleted.status.instances.is_empty());
}

#[tokio::test(flavor = "local")]
async fn scale_to_zero_after_failed_instance_creation_converges_without_waiting_for_retry() {
    let backend = Rc::new(FakeBackend::default());
    backend.fail_instance_creates(1);
    let manifest = manifest_with(1);
    let agent = Agent::spawn(
        Context::quiet(),
        AgentName::new(manifest.name()),
        unique_layout(&manifest).0,
        backend,
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(manifest)).await;
    wait_for_event(&mut stream, |event| {
        matches!(
            event,
            AgentEvent::Diagnostic { message, .. } if message.contains("failed to create instance")
        )
    })
    .await;
    let accepted = scale(&agent, 0).await;

    let stopped = wait_for_document(&mut stream, |document| {
        document.generation() == accepted.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.active == 0
            && !document.status.reconciling
    })
    .await;
    assert_eq!(stopped.replicas(), 0);
}

#[tokio::test(flavor = "local")]
async fn scale_down_after_failed_extra_instance_creation_drops_retry_placeholder() {
    let backend = Rc::new(FakeBackend::default());
    backend.fail_instance_creates(2);
    let manifest = manifest_with(2);
    let agent = Agent::spawn(
        Context::quiet(),
        AgentName::new(manifest.name()),
        unique_layout(&manifest).0,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    apply(&agent, manifest_context(manifest)).await;
    for _ in 0..2 {
        wait_for_event(&mut stream, |event| {
            matches!(
                event,
                AgentEvent::Diagnostic { message, .. } if message.contains("failed to create instance")
            )
        })
        .await;
    }
    scale(&agent, 1).await;

    let ready = wait_for_ready(&mut stream, 1).await;

    assert_eq!(ready.status.replicas.ready, 1);
    assert_eq!(backend.created_instances.borrow().len(), 3);
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

    let logs = logs(&agent, id, 100).await;
    assert!(
        logs.iter().any(|line| line.contains("session_finished")),
        "event log should include session completion: {logs:?}"
    );
}

#[tokio::test(flavor = "local")]
async fn manifest_change_while_exec_is_running_completes_session_and_latest_generation() {
    let (agent, mut stream, backend) = start_agent_with_backend(&manifest_with(1)).await;
    wait_for_ready(&mut stream, 1).await;
    let release_exec = backend.pause_next_exec();
    let id = AgentInstanceId::new(0);
    let exec_task = tokio::task::spawn_local({
        let agent = agent.clone();
        async move { exec(&agent, id, "sleep until released").await }
    });
    wait_for_event(&mut stream, |event| {
        matches!(
            event,
            AgentEvent::InstanceEvent { event }
                if matches!(event.event, AgentInstanceEvent::SessionStarted { .. })
        )
    })
    .await;

    let changed = apply(&agent, manifest_context(manifest_with_code_server_host(1, 4090))).await;
    release_exec.try_send(());

    let result = exec_task.await.expect("exec task joined").expect("exec completed");
    assert_eq!(result.stdout, "executed: 'sleep until released'");
    wait_for_document(&mut stream, |document| {
        document.generation() == changed.generation()
            && document.status.observed_generation == document.generation()
            && document.status.replicas.ready == 1
    })
    .await;
}

#[tokio::test(flavor = "local")]
async fn instance_logs_tail_reads_only_requested_lines() {
    let manifest = manifest_with(1);
    let backend: backend::BackendRef = Rc::new(FakeBackend::default());
    let (layout, agent_name) = unique_layout(&manifest);
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name.clone(),
        layout.clone(),
        backend,
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;
    apply(&agent, manifest_context(manifest)).await;
    wait_for_ready(&mut stream, 1).await;

    let path = layout.instance(&agent_name, AgentInstanceId::new(0)).instance_events();
    tokio::fs::create_dir_all(path.parent().expect("event log parent"))
        .await
        .expect("create event log directory");
    let mut contents = String::new();
    for index in 0..10_000 {
        writeln!(contents, "line-{index}").expect("write test log line");
    }
    tokio::fs::write(&path, contents).await.expect("write event log");

    let tail = logs(&agent, AgentInstanceId::new(0), 3).await;
    assert_eq!(tail, ["line-9997", "line-9998", "line-9999"]);
}

#[tokio::test(flavor = "local")]
async fn network_logs_are_filtered_before_tail_lines_are_applied() {
    let manifest = manifest_with(1);
    let backend: backend::BackendRef = Rc::new(FakeBackend::default());
    let (layout, agent_name) = unique_layout(&manifest);
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name.clone(),
        layout.clone(),
        backend,
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;
    apply(&agent, manifest_context(manifest)).await;
    wait_for_ready(&mut stream, 1).await;

    let path = layout.instance(&agent_name, AgentInstanceId::new(0)).instance_events();
    tokio::fs::create_dir_all(path.parent().expect("event log parent"))
        .await
        .expect("create event log directory");
    let mut contents = String::new();
    push_diagnostic_log_lines(&mut contents, 0..2_000);
    contents.push_str(&network_event_log_line(
        10_001,
        AgentInstanceNetworkEventKind::LifecycleStateChanged {
            state: "connected".to_owned(),
        },
    ));
    push_diagnostic_log_lines(&mut contents, 2_000..4_000);
    contents.push_str(&network_event_log_line(
        10_002,
        AgentInstanceNetworkEventKind::EgressError {
            protocol: "tcp".to_owned(),
            proxy: Some(7),
            destination: None,
            upstream: None,
            authority: None,
            route: None,
            phase: Some("write".to_owned()),
            message: "synthetic failure".to_owned(),
        },
    ));
    push_diagnostic_log_lines(&mut contents, 4_000..6_000);
    contents.push_str(&network_event_log_line(
        10_003,
        AgentInstanceNetworkEventKind::HostPortBound {
            name: "code_server".to_owned(),
            protocol: PortProtocolState::Tcp,
            guest: 4090,
            host: 4090,
        },
    ));
    push_diagnostic_log_lines(&mut contents, 6_000..8_000);
    tokio::fs::write(&path, contents).await.expect("write event log");

    let network = log_result(
        &agent,
        AgentInstanceId::new(0),
        2,
        Some(LogFilter::Network {
            errors: false,
            event_kind: None,
        }),
    )
    .await;
    assert_eq!(network_event_sequences(&network.contents), [10_002, 10_003]);

    let errors = log_result(
        &agent,
        AgentInstanceId::new(0),
        1,
        Some(LogFilter::Network {
            errors: true,
            event_kind: None,
        }),
    )
    .await;
    assert_eq!(network_event_sequences(&errors.contents), [10_002]);

    let host_ports = log_result(
        &agent,
        AgentInstanceId::new(0),
        1,
        Some(LogFilter::Network {
            errors: false,
            event_kind: Some(NetworkLogKind::HostPort),
        }),
    )
    .await;
    assert_eq!(network_event_sequences(&host_ports.contents), [10_003]);
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
    let (agent, mut stream, backend) = start_agent_with_backend(&manifest_with(1)).await;
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
    assert_eq!(backend.created_instances.borrow().len(), 2);
    assert_eq!(*backend.instance_stops.borrow(), 1);
    let instance = status(&agent, AgentInstanceId::new(0)).await.expect("instance status");
    assert_eq!(instance.spec.template.resources.memory, "3G");
    assert_eq!(instance.status.materialized_template, instance.spec.template);
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
async fn delete_retries_after_bounded_instance_delete_timeout() {
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
    wait_for_count(&backend.instance_stops, 2, "retried instance delete").await;
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
    let backend = Rc::new(FakeBackend::default());
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    let ready = wait_for_ready(&mut stream, 1).await;
    assert_eq!(ready.status.replicas.ready, 1);
    assert_eq!(backend.bootstrap_retry_epochs.borrow().as_slice(), &[Some(1)]);
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
    wait_for_host_input_reconcile_count(&backend, 1).await;
}

#[tokio::test(flavor = "local")]
async fn persisted_stale_stopped_active_instance_starts_after_agent_restart() {
    let manifest = manifest_with(1);
    let (layout, agent_name) = unique_layout(&manifest);
    persist_stale_stopped_active_instance(&layout, &agent_name, &manifest).await;
    let backend = Rc::new(FakeBackend::default());
    let agent = Agent::spawn(
        Context::quiet(),
        agent_name,
        layout,
        backend.clone(),
        tailscale_service(),
    );
    let mut stream = watch(&agent).await;

    wait_for_ready(&mut stream, 1).await;

    assert_eq!(*backend.instance_starts.borrow(), 1);
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
    apply_result(agent, manifest).await.expect("apply succeeds")
}

async fn apply_result(agent: &Agent, manifest: AgentManifestContext) -> Result<AgentDocument, Error> {
    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::Apply {
            manifest: Box::new(manifest),
            respond,
        })
        .expect("agent accepts apply command");
    receive.await.expect("apply response")
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

async fn document(agent: &Agent) -> AgentDocument {
    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::Document { respond })
        .expect("agent accepts document command");
    receive.await.expect("document response").expect("document succeeds")
}

async fn wait_for_host_input_reconcile_count(backend: &FakeBackend, expected: u32) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for host input reconciliation"
        );
        if *backend.host_input_reconciles.borrow() >= expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn wait_for_count(count: &RefCell<u32>, expected: u32, operation: &str) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {operation}"
        );
        if *count.borrow() >= expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn wait_for_created_instances(backend: &FakeBackend, expected: usize) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for instance creation"
        );
        if backend.created_instances.borrow().len() >= expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
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

async fn logs(agent: &Agent, id: AgentInstanceId, lines: usize) -> Vec<String> {
    log_result(agent, id, lines, None)
        .await
        .contents
        .lines()
        .map(str::to_owned)
        .collect()
}

async fn log_result(
    agent: &Agent,
    id: AgentInstanceId,
    lines: usize,
    filter: Option<LogFilter>,
) -> AgentInstanceLogsResult {
    let (respond, receive) = oneshot::channel();
    agent
        .send(AgentCommand::InstanceLogs {
            instance: id,
            params: AgentInstanceLogsParams {
                agent: agent.agent().to_string(),
                instance_id: id.as_u32(),
                file: LogFile::Events,
                lines,
                filter,
            },
            respond,
        })
        .expect("agent accepts logs command");
    receive.await.expect("logs response").expect("logs succeed")
}

fn manifest_with(replicas: u16) -> AgentManifest {
    let mut manifest: AgentManifest = serde_yaml::from_str(agentdp_test_support::manifest::minimal()).unwrap();
    manifest.spec.replicas = replicas;
    manifest.spec.phase = AgentPhase::Running;
    manifest
}

fn manifest_with_codex_mediated_auth(replicas: u16) -> AgentManifest {
    let mut manifest = manifest_with(replicas);
    manifest.metadata.name = "codex-auth".to_owned();
    manifest.spec.replicas = replicas;
    manifest.spec.template.network.mode = NetworkMode::Mediated;
    manifest.spec.template.plugins.codex = Some(Codex {
        yolo: false,
        auth: AuthMode::Mediated,
        auth_source: Some(codex::CodexAuthSource::HostAuth),
    });
    manifest
}

fn manifest_with_top_level_mediated_secret(replicas: u16, hosts: &[&str]) -> AgentManifest {
    manifest_with_top_level_mediated_secrets(replicas, &[("ALTINN_DEV_KEY", hosts)])
}

fn manifest_with_top_level_mediated_secrets(replicas: u16, secrets: &[(&str, &[&str])]) -> AgentManifest {
    let mut manifest = manifest_with(replicas);
    manifest.metadata.name = "top-level-secret".to_owned();
    manifest.spec.template.network.mode = NetworkMode::Mediated;
    manifest.spec.template.secrets = secrets
        .iter()
        .map(|(env, hosts)| Secret {
            env: (*env).to_owned(),
            from_env: None,
            allow_hosts: hosts.iter().map(|host| (*host).to_owned()).collect(),
        })
        .collect();
    manifest
}

fn push_diagnostic_log_lines(contents: &mut String, range: std::ops::Range<u64>) {
    for sequence in range {
        let line = AgentInstanceEventEnvelope {
            sequence,
            timestamp: "2026-01-01T00:00:00Z".to_owned(),
            generation: 1,
            work_epoch: None,
            source: AgentInstanceEventSource::Instance,
            event: AgentInstanceEvent::Diagnostic {
                level: EventLevel::Info,
                message: format!("diagnostic-{sequence}"),
            },
        };
        contents.push_str(&serde_json::to_string(&line).expect("serialize diagnostic event"));
        contents.push('\n');
    }
}

fn network_event_log_line(sequence: u64, event: AgentInstanceNetworkEventKind) -> String {
    let line = AgentInstanceEventEnvelope {
        sequence,
        timestamp: "2026-01-01T00:00:00Z".to_owned(),
        generation: 1,
        work_epoch: None,
        source: AgentInstanceEventSource::Network,
        event: AgentInstanceEvent::NetworkEvent(AgentInstanceNetworkEvent {
            sequence,
            unix_millis: sequence,
            dropped_events_before: 0,
            event,
        }),
    };
    format!("{}\n", serde_json::to_string(&line).expect("serialize network event"))
}

fn network_event_sequences(contents: &str) -> Vec<u64> {
    contents
        .lines()
        .map(|line| serde_json::from_str::<AgentInstanceEventEnvelope>(line).expect("parse network event"))
        .map(|envelope| {
            let AgentInstanceEvent::NetworkEvent(event) = envelope.event else {
                panic!("expected network event");
            };
            event.sequence
        })
        .collect()
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
        attempt_epoch: Some(0),
        failure_count: 1,
        last_failure_unix_seconds: now,
        next_retry_unix_seconds: now.saturating_add(1),
        last_error: "previous bootstrap failure".to_owned(),
    });
    write_persisted_instance(layout, agent, AgentInstanceId::new(0), &instance).await;
}

async fn persist_ready_running_instance(layout: &AgentdpLayout, agent: &AgentName, manifest: &AgentManifest) {
    let mut instance = persisted_running_instance(layout, agent, manifest).await;
    instance.status.host_inputs.mark_ready(instance.spec.desired_generation);
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

async fn persist_stale_stopped_active_instance(layout: &AgentdpLayout, agent: &AgentName, manifest: &AgentManifest) {
    let mut instance = persisted_running_instance(layout, agent, manifest).await;
    instance.status.phase = AgentInstancePhase::Stopped;
    instance.status.clear_readiness();
    instance.status.reconciliation = Some(ReconciliationState {
        stale: true,
        observed_status: "missing".to_owned(),
        observed_pid: Some(3_940_761),
        reason: Some("runtime status is running but QEMU pid 3940761 is not running".to_owned()),
    });
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
    fail_base_creates: RefCell<u32>,
    fail_next_instance_delete: RefCell<bool>,
    fail_instance_creates: RefCell<u32>,
    pause_next_instance_create: RefCell<Option<oneshot::Receiver<()>>>,
    pause_next_base_create: RefCell<Option<oneshot::Receiver<()>>>,
    pause_next_instance_bootstrap: RefCell<Option<oneshot::Receiver<()>>>,
    fail_instance_bootstraps: RefCell<u32>,
    bootstrap_retry_epochs: RefCell<Vec<Option<u64>>>,
    pause_next_instance_start: RefCell<Option<oneshot::Receiver<()>>>,
    pause_next_instance_stop: RefCell<Option<oneshot::Receiver<()>>>,
    pause_next_runtime_secret_reconcile: RefCell<Option<oneshot::Receiver<()>>>,
    pause_next_base_stop: RefCell<Option<oneshot::Receiver<()>>>,
    pause_next_exec: RefCell<Option<oneshot::Receiver<()>>>,
    paused_host_input_reconciles: RefCell<std::collections::VecDeque<oneshot::Receiver<()>>>,
    pause_next_host_input_after_side_effect: RefCell<Option<oneshot::Receiver<()>>>,
    created_bases: RefCell<u32>,
    created_instances: RefCell<Vec<String>>,
    instance_starts: RefCell<u32>,
    instance_reconciles: RefCell<u32>,
    fail_instance_reconciles: RefCell<u32>,
    runtime_secret_reconciles: RefCell<u32>,
    fail_runtime_secret_reconciles: RefCell<u32>,
    runtime_secret_files: RefCell<Vec<SeedFile>>,
    host_input_reconciles: RefCell<u32>,
    reconciled_secret_files: RefCell<Vec<SeedFile>>,
    host_input_side_effects: RefCell<u32>,
    host_input_completed_side_effects: RefCell<u32>,
    fail_host_input_reconciles: RefCell<u32>,
    instance_stops: RefCell<u32>,
    base_stops: RefCell<u32>,
}

impl FakeBackend {
    fn fail_next_base_stop(&self) {
        *self.fail_next_base_stop.borrow_mut() = true;
    }

    fn fail_base_creates(&self, count: u32) {
        *self.fail_base_creates.borrow_mut() = count;
    }

    fn fail_next_instance_delete(&self) {
        *self.fail_next_instance_delete.borrow_mut() = true;
    }

    fn fail_instance_creates(&self, count: u32) {
        *self.fail_instance_creates.borrow_mut() = count;
    }

    fn fail_instance_bootstraps(&self, count: u32) {
        *self.fail_instance_bootstraps.borrow_mut() = count;
    }

    fn fail_instance_reconciles(&self, count: u32) {
        *self.fail_instance_reconciles.borrow_mut() = count;
    }

    fn fail_host_input_reconciles(&self, count: u32) {
        *self.fail_host_input_reconciles.borrow_mut() = count;
    }

    fn fail_runtime_secret_reconciles(&self, count: u32) {
        *self.fail_runtime_secret_reconciles.borrow_mut() = count;
    }

    fn set_runtime_secret_files(&self, files: Vec<SeedFile>) {
        *self.runtime_secret_files.borrow_mut() = files;
    }

    fn pause_next_instance_create(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.pause_next_instance_create.borrow_mut() = Some(wait);
        release
    }

    fn pause_next_base_create(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.pause_next_base_create.borrow_mut() = Some(wait);
        release
    }

    fn pause_next_instance_bootstrap(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.pause_next_instance_bootstrap.borrow_mut() = Some(wait);
        release
    }

    fn pause_next_instance_start(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.pause_next_instance_start.borrow_mut() = Some(wait);
        release
    }

    fn pause_next_instance_stop(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.pause_next_instance_stop.borrow_mut() = Some(wait);
        release
    }

    fn pause_next_runtime_secret_reconcile(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.pause_next_runtime_secret_reconcile.borrow_mut() = Some(wait);
        release
    }

    fn pause_next_base_stop(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.pause_next_base_stop.borrow_mut() = Some(wait);
        release
    }

    fn pause_next_exec(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.pause_next_exec.borrow_mut() = Some(wait);
        release
    }

    fn pause_next_host_input_reconcile(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        self.paused_host_input_reconciles.borrow_mut().push_back(wait);
        release
    }

    fn pause_next_host_input_after_side_effect(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.pause_next_host_input_after_side_effect.borrow_mut() = Some(wait);
        release
    }

    fn take_base_stop_failure(&self) -> bool {
        std::mem::take(&mut *self.fail_next_base_stop.borrow_mut())
    }

    fn take_base_create_failure(&self) -> bool {
        let mut failures = self.fail_base_creates.borrow_mut();
        if *failures == 0 {
            return false;
        }
        *failures -= 1;
        true
    }

    fn take_instance_delete_failure(&self) -> bool {
        std::mem::take(&mut *self.fail_next_instance_delete.borrow_mut())
    }

    fn take_instance_create_failure(&self) -> bool {
        let mut failures = self.fail_instance_creates.borrow_mut();
        if *failures == 0 {
            return false;
        }
        *failures -= 1;
        true
    }

    fn take_instance_bootstrap_failure(&self) -> bool {
        let mut failures = self.fail_instance_bootstraps.borrow_mut();
        if *failures == 0 {
            return false;
        }
        *failures -= 1;
        true
    }

    fn take_instance_reconcile_failure(&self) -> bool {
        let mut failures = self.fail_instance_reconciles.borrow_mut();
        if *failures == 0 {
            return false;
        }
        *failures -= 1;
        true
    }

    fn take_host_input_reconcile_failure(&self) -> bool {
        let mut failures = self.fail_host_input_reconciles.borrow_mut();
        if *failures == 0 {
            return false;
        }
        *failures -= 1;
        true
    }

    fn take_runtime_secret_reconcile_failure(&self) -> bool {
        let mut failures = self.fail_runtime_secret_reconciles.borrow_mut();
        if *failures == 0 {
            return false;
        }
        *failures -= 1;
        true
    }

    fn take_instance_create_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.pause_next_instance_create.borrow_mut().take()
    }

    fn take_base_create_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.pause_next_base_create.borrow_mut().take()
    }

    fn take_instance_bootstrap_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.pause_next_instance_bootstrap.borrow_mut().take()
    }

    fn take_instance_start_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.pause_next_instance_start.borrow_mut().take()
    }

    fn take_instance_stop_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.pause_next_instance_stop.borrow_mut().take()
    }

    fn take_runtime_secret_reconcile_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.pause_next_runtime_secret_reconcile.borrow_mut().take()
    }

    fn take_base_stop_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.pause_next_base_stop.borrow_mut().take()
    }

    fn take_exec_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.pause_next_exec.borrow_mut().take()
    }

    fn take_host_input_reconcile_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.paused_host_input_reconciles.borrow_mut().pop_front()
    }

    fn take_host_input_after_side_effect_pause(&self) -> Option<oneshot::Receiver<()>> {
        self.pause_next_host_input_after_side_effect.borrow_mut().take()
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
        *self.created_bases.borrow_mut() += 1;
        let pause = self.take_base_create_pause();
        let fail = self.take_base_create_failure();
        Box::pin(async move {
            if let Some(release) = pause {
                let _result = release.await;
            }
            if fail {
                return Err(fake_backend_error());
            }
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
        _control: &'a mut Option<backend::InstanceControl>,
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
        *self.base_stops.borrow_mut() += 1;
        let pause = self.take_base_stop_pause();
        let fail = self.take_base_stop_failure();
        Box::pin(async move {
            if let Some(release) = pause {
                let _result = release.await;
            }
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
        let fail = self.take_instance_create_failure();
        self.created_instances.borrow_mut().push(input.instance);
        Box::pin(async move {
            if let Some(release) = pause {
                let _result = release.await;
            }
            if fail {
                return Err(fake_backend_error());
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
        *self.instance_starts.borrow_mut() += 1;
        let pause = self.take_instance_start_pause();
        let host_ports = state.status.network.ports.clone();
        Box::pin(async move {
            if let Some(release) = pause {
                let _result = release.await;
            }
            Ok(backend::StartOutput {
                process: process_status("running"),
                host_ports,
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
        let pause = self.take_exec_pause();
        Box::pin(async move {
            if let Some(release) = pause {
                let _result = release.await;
            }
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
        state: &'a AgentInstanceDocument,
        _control: &'a mut Option<backend::InstanceControl>,
        retry_epoch: Option<u64>,
        bootstrap_events: Option<&'a mut dyn backend::BootstrapEventSink>,
    ) -> backend::BackendFuture<'a, backend::BootstrapOutcome> {
        let pause = (state.metadata.name.as_str() != crate::agent::AGENT_BASE_INSTANCE)
            .then(|| self.take_instance_bootstrap_pause())
            .flatten();
        let instance_bootstrap = state.metadata.name.as_str() != crate::agent::AGENT_BASE_INSTANCE;
        if instance_bootstrap {
            self.bootstrap_retry_epochs.borrow_mut().push(retry_epoch);
        }
        let fail = instance_bootstrap && self.take_instance_bootstrap_failure();
        Box::pin(async move {
            if let Some(release) = pause {
                let _result = release.await;
            }
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
            if fail {
                return Ok(backend::BootstrapOutcome::Failed {
                    attempt_epoch: retry_epoch.unwrap_or(0),
                    error: "fake bootstrap failure".to_owned(),
                });
            }
            Ok(backend::BootstrapOutcome::Passed {
                attempt_epoch: retry_epoch.unwrap_or(0),
            })
        })
    }

    fn stop_instance<'a>(
        &'a self,
        _context: &'a Context,
        _network: &'a InstanceNetwork,
        _input: backend::StopInstanceInput<'a>,
        _backend_state: &'a mut BackendState,
        _control: &'a mut Option<backend::InstanceControl>,
    ) -> backend::BackendFuture<'a, backend::StopOutput> {
        *self.instance_stops.borrow_mut() += 1;
        let pause = self.take_instance_stop_pause();
        let fail = self.take_instance_delete_failure();
        Box::pin(async move {
            if let Some(release) = pause {
                let _result = release.await;
            }
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
        let fail = self.take_instance_reconcile_failure();
        Box::pin(async move {
            if fail {
                return Err(fake_backend_error());
            }
            Ok(backend::ReconcileOutput {
                stale: false,
                mark_stopped: false,
                backend_changed: false,
                process: process_status("running"),
                host_ports: state.status.network.ports.clone(),
            })
        })
    }

    fn reconcile_runtime_secrets<'a>(
        &'a self,
        _context: &'a Context,
        _network: &'a InstanceNetwork,
        _manifest: &'a AgentManifestContext,
        _state: &'a mut AgentInstanceDocument,
    ) -> backend::BackendFuture<'a, backend::ReconcileRuntimeSecretsOutput> {
        *self.runtime_secret_reconciles.borrow_mut() += 1;
        let secret_files = self.runtime_secret_files.borrow().clone();
        let pause = self.take_runtime_secret_reconcile_pause();
        let fail = self.take_runtime_secret_reconcile_failure();
        Box::pin(async move {
            if let Some(release) = pause {
                let _result = release.await;
            }
            if fail {
                return Err(fake_backend_error());
            }
            Ok(backend::ReconcileRuntimeSecretsOutput { secret_files })
        })
    }

    fn reconcile_host_inputs<'a>(
        &'a self,
        _context: &'a Context,
        _network: &'a InstanceNetwork,
        _manifest: &'a AgentManifestContext,
        _state: &'a AgentInstanceDocument,
        secret_files: &'a [SeedFile],
        _control: &'a mut Option<backend::InstanceControl>,
    ) -> backend::BackendFuture<'a, backend::ReconcileHostInputsOutput> {
        *self.host_input_reconciles.borrow_mut() += 1;
        *self.reconciled_secret_files.borrow_mut() = secret_files.to_vec();
        let pause = self.take_host_input_reconcile_pause();
        let pause_after_side_effect = self.take_host_input_after_side_effect_pause();
        let fail = self.take_host_input_reconcile_failure();
        Box::pin(async move {
            if let Some(release) = pause {
                let _result = release.await;
            }
            *self.host_input_side_effects.borrow_mut() += 1;
            if let Some(release) = pause_after_side_effect {
                let _result = release.await;
            }
            *self.host_input_completed_side_effects.borrow_mut() += 1;
            if fail {
                return Err(fake_backend_error());
            }
            Ok(backend::ReconcileHostInputsOutput {
                files_updated: 0,
                file_failures: 0,
                file_errors: Vec::new(),
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
