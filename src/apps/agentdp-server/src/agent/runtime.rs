//! Single-loop per-agent runtime.
//!
//! The agent owns the whole agent graph directly: desired document, base state,
//! instance state, event streams, and child task admission. Base and instances
//! are owned state machines inside this loop, not independent actors.

use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::agent::{
    AgentBaseKey, AgentDocument, AgentEvent, AgentEventEnvelope, AgentEventSource, AgentInstanceBootstrapState,
    AgentInstanceBootstrapWorkPhase, AgentInstanceBootstrapWorkStatus, AgentInstanceDocument, AgentInstanceEvent,
    AgentInstanceEventEnvelope, AgentInstanceEventSource, AgentInstanceId, AgentInstanceNetworkEvent,
    AgentInstanceNetworkEventKind, AgentInstanceNetworkStatus, AgentInstancePhase, AgentInstanceSessionsWorkStatus,
    AgentInstanceTarget, AgentInstanceTransitionKind, AgentInstanceTransitionWorkStatus, AgentInstanceWorkStatus,
    AgentName, BackendState, BootstrapEvent, EventLevel, InstanceName, NetworkAllowState, NetworkIpv6State,
    NetworkState, OperationResult, PortMappingState, PortRequestError, ReadinessResult, ReadinessState, ServiceStatus,
    SessionKind, SessionResultSummary, TailscaleServeState, assign_port_mappings,
};
use agentdp_core::manifest::{AgentPhase, ValidationErrors};
use agentdp_core::provisioning::bootstrap::BootstrapGraphError;
use agentdp_core::provisioning::{ProvisioningOptions, ProvisioningPlan, SeedFile};
use agentdp_ds::local::{inbox, oneshot, spsc};
use agentdp_platform::ssh::{CommandOutput, OutputSink, OutputStream, shell_join};
use agentdp_platform::text::Utf8Stream;
use agentdp_platform::time;
use agentdp_protocol::client_server::{
    AgentInstanceExecParams, AgentInstanceExecResult, AgentInstanceListItem, AgentInstanceLogsParams,
    AgentInstanceLogsResult, AgentInstanceShellResult, LogFile, LogFilter, NetworkLogKind,
};
use thiserror::Error as ThisError;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::agent::{AgentContextError, AgentManifestContext, AgentdpLayout, AgentdpLayoutError, IdentityError};
use crate::backend;
use crate::host;
use crate::host::HostSeedError;
use crate::host::tailscale::{TailscaleServeDesired, TailscaleService};
use crate::services::InstanceNetwork;

use super::base::{AgentBaseDiskPhase, AgentBasePreparation, ensure_agent_base_ready};
use super::documents::{AgentDocuments, AgentInstanceDocuments};
use super::event_log::{
    Error as EventLogError, EventLogWriter, SequencePlan, inspect_sequence as inspect_event_sequence,
};

const INPUT_CAPACITY: usize = 256;
const RECENT_EVENT_CAPACITY: usize = 128;
const AGENT_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const HOST_INPUT_RECONCILE_INTERVAL: Duration = Duration::from_mins(15);
const HOST_INPUT_RECONCILE_RETRY_DELAY: Duration = Duration::from_secs(15);
const HOST_INPUT_RECONCILE_RETRY_MAX_DELAY: Duration = Duration::from_mins(5);
const INSTANCE_CREATE_RETRY_DELAY: Duration = Duration::from_secs(15);
const INSTANCE_CREATE_RETRY_MAX_DELAY: Duration = Duration::from_mins(5);
const INSTANCE_DELETE_RETRY_DELAY: Duration = Duration::from_secs(1);
const INSTANCE_BOOTSTRAP_RETRY_DELAY: Duration = Duration::from_secs(15);
const INSTANCE_BOOTSTRAP_RETRY_MAX_DELAY: Duration = Duration::from_mins(5);
const INSTANCE_READY_PROBE_TIMEOUT: Duration = Duration::from_secs(30);
const LOG_TAIL_CHUNK_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AgentStreamItem {
    Document(Box<AgentDocument>),
    Event(AgentEventEnvelope),
}

#[derive(Debug, ThisError)]
pub(crate) enum Error {
    #[error("{0}")]
    AgentContext(#[from] AgentContextError),
    #[error("{0}")]
    Identity(#[from] IdentityError),
    #[error("{0}")]
    Layout(#[from] AgentdpLayoutError),
    #[error("{0}")]
    Backend(#[from] backend::Error),
    #[error("{0}")]
    EventLog(#[from] EventLogError),
    #[error("{0}")]
    TailscaleServe(#[from] host::tailscale::Error),
    #[error("{0}")]
    HostSeed(#[from] HostSeedError),
    #[error("{0}")]
    BootstrapGraph(#[from] BootstrapGraphError),
    #[error("{0}")]
    PortRequest(#[from] PortRequestError),
    #[error("failed to read agent document {path}: {source}")]
    ReadAgentDocument {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse agent document {path}: {source}")]
    ParseAgentDocument {
        path: std::path::PathBuf,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("agent document {path} is invalid:\n{errors}")]
    InvalidAgentDocument {
        path: std::path::PathBuf,
        errors: ValidationErrors,
    },
    #[error("persisted state {path} identifies {actual}; expected {expected}")]
    PersistedStateIdentityMismatch {
        path: std::path::PathBuf,
        expected: String,
        actual: String,
    },
    #[error("persisted state for agent {name} could not be loaded: {message}")]
    PersistedStateUnavailable { name: String, message: String },
    #[error("failed to read instance document {path}: {source}")]
    ReadInstanceDocument {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse instance document {path}: {source}")]
    ParseInstanceDocument {
        path: std::path::PathBuf,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("failed to serialize agent base key material: {0}")]
    SerializeBaseKeyMaterial(#[source] serde_json::Error),
    #[error("agent base {key} is {phase:?}")]
    AgentBaseNotReady {
        key: AgentBaseKey,
        phase: AgentBaseDiskPhase,
    },
    #[error("failed to create agent base directory {path}: {source}")]
    CreateAgentBaseDirectory {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to inspect agent base disk {path}: {source}")]
    InspectAgentBaseDisk {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read agent base document {path}: {source}")]
    ReadAgentBaseDocument {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse agent base document {path}: {source}")]
    ParseAgentBaseDocument {
        path: std::path::PathBuf,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("failed to serialize agent base document: {0}")]
    SerializeAgentBaseDocument(#[source] serde_yaml::Error),
    #[error("failed to write agent base document {path}: {source}")]
    WriteAgentBaseDocument {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to persist agent state: {message}")]
    PersistState { message: String },
    #[error("readiness probe failed for {name}: exit status {status}")]
    ReadinessProbeFailed { name: String, status: i32 },
    #[error("agent base guest bootstrap failed: {error}")]
    AgentBaseBootstrapFailed { error: String },
    #[error("failed to read instance log {path}: {source}")]
    ReadLog {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("instance {name} is already running operation {operation}")]
    OperationInProgress { name: String, operation: &'static str },
    #[error("instance {name} is not available")]
    InstanceUnavailable { name: String },
    #[error("instance {name} does not exist")]
    InstanceNotFound { name: String },
    #[error("instance {name} is {status}; expected running")]
    InvalidStatus { name: String, status: String },
    #[error("log line count must be greater than zero")]
    InvalidLogLines,
    #[error("log filters are only supported for the instance event log")]
    InvalidLogFilter,
    #[error("exec command must not be empty")]
    EmptyExecCommand,
    #[error("exec timeout must be greater than zero")]
    InvalidExecTimeout,
    #[error(
        "mediated auth inputs cannot change after provisioning; credential values and host policy may change, but changing auth mode, source, guest path, or transform requires deleting and recreating the agent"
    )]
    MediatedAuthBindingsChanged,
}

pub(crate) enum AgentInstanceSessionOutput {
    Stdout(String),
    Stderr(String),
}

#[derive(Clone)]
pub(crate) struct Agent {
    name: AgentName,
    input: inbox::Sender<AgentInput>,
    task: Rc<JoinHandle<()>>,
}

impl Drop for Agent {
    fn drop(&mut self) {
        if Rc::strong_count(&self.task) == 1 {
            self.task.abort();
        }
    }
}

impl Agent {
    pub(crate) fn spawn(
        context: Context,
        agent: AgentName,
        layout: AgentdpLayout,
        backend: backend::BackendRef,
        tailscale: Rc<TailscaleService>,
    ) -> Self {
        let (input, receiver) = inbox::bounded(INPUT_CAPACITY);
        let task = spawn_agent_loop(
            AgentState::Starting(StartingAgentState {
                agent: agent.clone(),
                context,
                layout,
                backend,
                tailscale,
                pending_streams: Vec::new(),
                input: input.clone(),
            }),
            receiver,
        );
        let _result = input.try_send(AgentInput::Load);
        Self {
            name: agent,
            input,
            task: Rc::new(task),
        }
    }

    pub(crate) const fn agent(&self) -> &AgentName {
        &self.name
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub(crate) fn send(&self, command: AgentCommand) -> Result<(), Error> {
        if self.task.is_finished() {
            return Err(Error::InstanceUnavailable {
                name: self.name.to_string(),
            });
        }
        self.input
            .try_send(AgentInput::Command(command))
            .map_err(|error| match error {
                inbox::TrySendError::Full(_) => Error::OperationInProgress {
                    name: self.name.to_string(),
                    operation: "agent command queue",
                },
                inbox::TrySendError::Disconnected(_) => Error::InstanceUnavailable {
                    name: self.name.to_string(),
                },
            })
    }
}

enum AgentInput {
    Load,
    Command(AgentCommand),
    Work(Box<WorkCompletion>),
}

pub(crate) enum AgentCommand {
    Apply {
        manifest: Box<AgentManifestContext>,
        respond: oneshot::Sender<Result<AgentDocument, Error>>,
    },
    Scale {
        replicas: u16,
        respond: oneshot::Sender<Result<AgentDocument, Error>>,
    },
    Delete {
        respond: oneshot::Sender<Result<AgentDocument, Error>>,
    },
    OpenStream {
        replay_from_generation: Option<u64>,
        items: spsc::Sender<AgentStreamItem>,
        respond: oneshot::Sender<Result<(), Error>>,
    },
    InstanceStatus {
        instance: AgentInstanceId,
        respond: oneshot::Sender<Result<AgentInstanceDocument, Error>>,
    },
    InstanceLogs {
        instance: AgentInstanceId,
        params: AgentInstanceLogsParams,
        respond: oneshot::Sender<Result<AgentInstanceLogsResult, Error>>,
    },
    InstanceExec {
        context: Context,
        instance: AgentInstanceId,
        params: AgentInstanceExecParams,
        output: spsc::Sender<AgentInstanceSessionOutput>,
        respond: oneshot::Sender<Result<AgentInstanceExecResult, Error>>,
    },
    InstanceShell {
        context: Context,
        instance: AgentInstanceId,
        respond: oneshot::Sender<Result<AgentInstanceShellResult, Error>>,
    },
    ListItems {
        respond: oneshot::Sender<Result<Vec<AgentInstanceListItem>, Error>>,
    },
    Document {
        respond: oneshot::Sender<Result<AgentDocument, Error>>,
    },
}

enum WorkCompletion {
    BasePrepared {
        work_id: u64,
        generation: u64,
        preparation: Box<AgentBasePreparation>,
    },
    BaseBuilt {
        work_id: u64,
        generation: u64,
        key: AgentBaseKey,
    },
    BaseFailed {
        work_id: u64,
        generation: u64,
        error: String,
    },
    BaseStopped {
        work_id: u64,
    },
    BaseStopTimedOut {
        work_id: u64,
    },
    InstanceCreated {
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        document: Box<Result<AgentInstanceDocument, String>>,
    },
    InstanceStarted {
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        outcome: InstanceBackendOutcome<backend::StartOutput>,
    },
    InstanceReconciled {
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        outcome: InstanceBackendOutcome<backend::ReconcileOutput>,
    },
    BootstrapStarted {
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
    },
    BootstrapEvent {
        source: BootstrapSource,
        work_id: u64,
        generation: u64,
        event: BootstrapEvent,
    },
    BootstrapFinished {
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        control: Option<backend::InstanceControl>,
        result: Result<Option<TailscaleServeState>, BootstrapTaskFailure>,
    },
    TailscaleServeReconciled {
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        result: Result<Option<TailscaleServeState>, String>,
    },
    RuntimeSecretsReconciled {
        id: AgentInstanceId,
        generation: u64,
        work_id: u64,
        outcome: InstanceBackendOutcome<backend::ReconcileRuntimeSecretsOutput>,
    },
    HostInputsReconciled {
        id: AgentInstanceId,
        generation: u64,
        work_id: u64,
        control: Option<backend::InstanceControl>,
        result: Result<backend::ReconcileHostInputsOutput, String>,
    },
    InstanceStopped {
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        control: Option<backend::InstanceControl>,
        outcome: InstanceStopOutcome,
    },
    InstanceDeleteRuntimeStopped {
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        control: Option<backend::InstanceControl>,
        outcome: InstanceStopOutcome,
    },
    InstanceDeleted {
        id: AgentInstanceId,
        work_id: u64,
    },
    InstanceDeleteTimedOut {
        id: AgentInstanceId,
        work_id: u64,
        error: String,
    },
    ExecFinished {
        id: AgentInstanceId,
        work_id: u64,
        command: Vec<String>,
        output: CommandOutput,
    },
}

struct BootstrapTaskFailure {
    attempt_epoch: Option<u64>,
    error: String,
}

impl BootstrapTaskFailure {
    fn unobserved(error: impl std::fmt::Display) -> Self {
        Self {
            attempt_epoch: None,
            error: error.to_string(),
        }
    }

    const fn observed(attempt_epoch: u64, error: String) -> Self {
        Self {
            attempt_epoch: Some(attempt_epoch),
            error,
        }
    }
}

struct InstanceBackendOutcome<T> {
    backend: BackendState,
    result: Result<T, String>,
}

struct InstanceStopOutcome {
    backend: BackendState,
    tailscale_serve: Option<TailscaleServeState>,
    result: Result<backend::StopOutput, String>,
}

#[derive(Debug, Clone, Copy)]
enum BootstrapSource {
    AgentBase,
    Instance { id: AgentInstanceId },
}

enum AgentState {
    Starting(StartingAgentState),
    LoadFailed(LoadFailedAgentState),
    Running(Box<RunningAgentState>),
}

#[derive(Clone)]
struct AgentWorkServices {
    input: inbox::Sender<AgentInput>,
    backend: backend::BackendRef,
    tailscale: Rc<TailscaleService>,
}

struct StartingAgentState {
    agent: AgentName,
    context: Context,
    layout: AgentdpLayout,
    backend: backend::BackendRef,
    tailscale: Rc<TailscaleService>,
    pending_streams: Vec<PendingOpenStream>,
    input: inbox::Sender<AgentInput>,
}

struct LoadFailedAgentState {
    agent: AgentName,
    message: String,
}

struct RunningAgentState {
    agent: AgentName,
    context: Context,
    documents: AgentDocuments,
    pending_responses: Vec<PendingResponse>,
    recent_events: VecDeque<AgentEventEnvelope>,
    event_sequence: u64,
    events: EventLogWriter<AgentEventEnvelope>,
    layout: AgentdpLayout,
    backend: backend::BackendRef,
    tailscale: Rc<TailscaleService>,
    base: AgentBaseState,
    instances: BTreeMap<AgentInstanceId, AgentInstanceState>,
    streams: Vec<spsc::Sender<AgentStreamItem>>,
    input: inbox::Sender<AgentInput>,
    next_reconcile: Instant,
    next_work_id: u64,
}

enum PendingResponse {
    AgentDocument(oneshot::Sender<Result<AgentDocument, Error>>),
    InstanceDocument {
        id: AgentInstanceId,
        respond: oneshot::Sender<Result<AgentInstanceDocument, Error>>,
    },
    ListItems(oneshot::Sender<Result<Vec<AgentInstanceListItem>, Error>>),
    OpenStream {
        replay_from_generation: Option<u64>,
        items: spsc::Sender<AgentStreamItem>,
        respond: Option<oneshot::Sender<Result<(), Error>>>,
    },
}

struct PendingOpenStream {
    replay_from_generation: Option<u64>,
    items: spsc::Sender<AgentStreamItem>,
}

pub(super) enum AgentBaseState {
    Missing,
    Preparing {
        work_id: u64,
    },
    Building {
        work_id: u64,
        generation: u64,
        key: AgentBaseKey,
    },
    Ready {
        key: AgentBaseKey,
    },
    Failed {
        generation: u64,
        key: Option<AgentBaseKey>,
    },
    Stopping {
        work_id: u64,
    },
    Stopped,
}

impl AgentBaseState {
    const fn has_provisioned_resources(&self) -> bool {
        matches!(
            self,
            Self::Building { .. }
                | Self::Ready { .. }
                | Self::Stopping { .. }
                | Self::Stopped
                | Self::Failed { key: Some(_), .. }
        )
    }
}

pub(super) struct StartingAgentInstanceState {
    work_id: u64,
    generation: u64,
    retry_at: Option<Instant>,
    failure_count: u16,
    event_sequence: u64,
    events: EventLogWriter<AgentInstanceEventEnvelope>,
    network_runtime: Rc<InstanceNetwork>,
    network_events: spsc::Receiver<AgentInstanceNetworkEvent>,
}

impl StartingAgentInstanceState {
    fn new(
        context: &Context,
        layout: &AgentdpLayout,
        agent: &AgentName,
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
    ) -> Self {
        let instance_layout = layout.instance(agent, id);
        let events = EventLogWriter::spawn(context, instance_layout.instance_events());
        let (network_events, network_event_receiver) = spsc::bounded(1024);
        Self {
            work_id,
            generation,
            retry_at: None,
            failure_count: 0,
            event_sequence: 1,
            events,
            network_runtime: Rc::new(InstanceNetwork::new(network_events)),
            network_events: network_event_receiver,
        }
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "running instances are stored directly in the agent table; boxing would add an allocation to every live instance to optimize the short-lived starting creation state"
)]
pub(super) enum AgentInstanceState {
    Starting(StartingAgentInstanceState),
    Running(RunningAgentInstanceState),
}

impl AgentInstanceState {
    fn running(
        document: AgentInstanceDocument,
        persisted: Option<AgentInstanceDocument>,
        event_sequence: u64,
        events: EventLogWriter<AgentInstanceEventEnvelope>,
        network_runtime: Rc<InstanceNetwork>,
        network_events: spsc::Receiver<AgentInstanceNetworkEvent>,
    ) -> Self {
        Self::Running(RunningAgentInstanceState::new(
            document,
            persisted,
            event_sequence,
            events,
            network_runtime,
            network_events,
        ))
    }

    const fn running_ref(&self) -> Option<&RunningAgentInstanceState> {
        match self {
            Self::Starting(_) => None,
            Self::Running(running) => Some(running),
        }
    }

    pub(super) const fn running_mut(&mut self) -> Option<&mut RunningAgentInstanceState> {
        match self {
            Self::Starting(_) => None,
            Self::Running(running) => Some(running),
        }
    }
}

pub(super) struct RunningAgentInstanceState {
    pub(super) documents: AgentInstanceDocuments,
    event_sequence: u64,
    events: EventLogWriter<AgentInstanceEventEnvelope>,
    bootstrap_retry: Option<Instant>,
    cleanup_retry: Option<Instant>,
    work: Option<AgentInstanceWork>,
    network_runtime: Rc<InstanceNetwork>,
    network_events: spsc::Receiver<AgentInstanceNetworkEvent>,
    control: Option<backend::InstanceControl>,
    runtime_secrets: ReconcileSchedule,
    runtime_repair: RuntimeRepairState,
    secret_host_files: Option<Vec<SeedFile>>,
    host_inputs: ReconcileSchedule,
    session: Option<ForegroundSession>,
}

impl RunningAgentInstanceState {
    fn new(
        document: AgentInstanceDocument,
        persisted: Option<AgentInstanceDocument>,
        event_sequence: u64,
        events: EventLogWriter<AgentInstanceEventEnvelope>,
        network_runtime: Rc<InstanceNetwork>,
        network_events: spsc::Receiver<AgentInstanceNetworkEvent>,
    ) -> Self {
        let bootstrap_retry = bootstrap_retry_deadline(&document);
        Self {
            documents: AgentInstanceDocuments::new(document, persisted),
            event_sequence,
            events,
            bootstrap_retry,
            cleanup_retry: None,
            work: None,
            network_runtime,
            network_events,
            control: None,
            runtime_secrets: ReconcileSchedule::new(),
            runtime_repair: RuntimeRepairState::Idle,
            secret_host_files: None,
            host_inputs: ReconcileSchedule::new(),
            session: None,
        }
    }

    const fn blocks_instance_transition(&self) -> bool {
        self.work.is_some()
            || self.runtime_secrets.active_work_id.is_some()
            || self.host_inputs.active_work_id.is_some()
            || self.session.is_some()
    }

    fn bootstrap_retry_wake(&self) -> Option<Instant> {
        if self.blocks_instance_transition()
            || !self.runtime_repair.allows_auxiliary_work()
            || self.documents.private.spec.target != AgentInstanceTarget::Active
            || self
                .documents
                .private
                .status
                .readiness
                .as_ref()
                .is_some_and(|state| state.ready)
        {
            return None;
        }
        self.bootstrap_retry
    }
}

struct ReconcileSchedule {
    next_at: Instant,
    active_work_id: Option<u64>,
    failure_count: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeRepairState {
    Idle,
    Due,
    Backoff,
}

impl RuntimeRepairState {
    const fn allows_auxiliary_work(self) -> bool {
        matches!(self, Self::Idle)
    }
}

impl ReconcileSchedule {
    fn new() -> Self {
        Self {
            next_at: Instant::now(),
            active_work_id: None,
            failure_count: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AgentInstanceWork {
    id: u64,
    kind: AgentInstanceWorkKind,
    input_generation: u64,
}

#[derive(Clone, Copy)]
struct TaskAttempt {
    work_id: u64,
    generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentInstanceWorkKind {
    Reconciling,
    Repairing,
    Starting,
    Bootstrapping,
    Stopping,
    Deleting,
}

struct ForegroundSession {
    work_id: u64,
    respond: oneshot::Sender<Result<AgentInstanceExecResult, Error>>,
}

fn spawn_agent_loop(mut state: AgentState, mut inputs: inbox::Receiver<AgentInput>) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        // This is the main loop of the agent representing the controller/control plane
        // of the agent and the state machines within. The state of the agent and agent instances
        // are represented by AgentDocument and AgentInstanceDocument. They look like typical k8s
        // manifests with a spec/template and status objects which together describe the state of the agent.
        //
        // Notes on lifecycle and behavior:
        // - Initial state of Agent is "Starting" (preexisting state may or may not be on disk)
        // - For the starting state
        //   - Load command is always processed first, which reads any preexisting state on disk (comes from Agent::spawn)
        //     state transitions to Running if there was preexisting state (it has been applied before)
        //   - Apply may follow (e.g. if this is the first spawn for an agent) with AgentManifestContext
        //     state transitions to running (if it wasnt already due to Load) and response is queued
        // - Once running, the normal order of operation is that the loop waits for inputs, reconciliation intervals or retries, example:
        //   1. Apply is processed (see `running.handle_command()`), which means the document (the desired state of the agent) is updated in memory
        //   2. Reconcile (see `running.reconcile()`) takes the desired states and spawns async reconciliation tasks "driving" infra as needed
        //     2.1. The tasks are the side effects and will queue events and completion results during execution, all this happens async
        //   3. Commit (see `running.commit_state()`) commits state to disk
        //   4. WorkCompletions originating from async tasks completing are eventually processed as input in the loop and update the status of the documents
        //      and may in turn spawn further async tasks, which in turn produce more work completions and so on.

        // Loop invariants:
        // - Only the loop code mutates agent state, async (spawned) tasks communicate through events and enqueued completions
        // - Only initialization (starting -> running), input recv, timer waits and commit_state are async. This is important to keep control plane responsive
        // - Responses to commands (like list, wait/stream, status) are enqueued in memory and dispatched at the end of the loop (commit stage)
        loop {
            let input = match &state {
                AgentState::Starting(_) | AgentState::LoadFailed(_) => match inputs.recv().await {
                    Ok(input) => Some(input),
                    Err(inbox::TryRecvError::Empty) => continue,
                    Err(inbox::TryRecvError::Disconnected) => break,
                },
                AgentState::Running(running) => {
                    let next_wake = running.next_wake();
                    tokio::select! {
                        input = inputs.recv() => match input {
                            Ok(input) => Some(input),
                            Err(inbox::TryRecvError::Empty) => continue,
                            Err(inbox::TryRecvError::Disconnected) => break,
                        },
                        () = async move {
                            match next_wake {
                                Some(deadline) => tokio::time::sleep_until(deadline).await,
                                None => std::future::pending::<()>().await,
                            }
                        } => None,
                    }
                }
            };
            if let Some(input) = input {
                match &mut state {
                    AgentState::Starting(starting) => match starting.handle_input(input).await {
                        Ok(Some(running)) => state = AgentState::Running(Box::new(running)),
                        Ok(None) => {}
                        Err(error) => {
                            starting
                                .context
                                .logger()
                                .error(format!("failed to load persisted agent {}: {error}", starting.agent));
                            state = AgentState::LoadFailed(LoadFailedAgentState {
                                agent: starting.agent.clone(),
                                message: error.to_string(),
                            });
                        }
                    },
                    AgentState::LoadFailed(failed) => failed.handle_input(input),
                    AgentState::Running(running) => match input {
                        AgentInput::Load => {}
                        AgentInput::Command(command) => running.handle_command(command),
                        AgentInput::Work(completion) => running.handle_work_completion(*completion),
                    },
                }
            }
            let AgentState::Running(running) = &mut state else {
                continue;
            };
            running.refresh_runtime_observations();
            running.reconcile();
            if running.commit_state().await {
                break;
            }
        }
    })
}

impl StartingAgentState {
    async fn handle_input(&mut self, input: AgentInput) -> Result<Option<RunningAgentState>, Error> {
        match input {
            AgentInput::Load => self.load().await,
            AgentInput::Command(AgentCommand::Apply { manifest, respond }) => {
                let document = match AgentDocument::from_manifest(
                    manifest.source_path().display().to_string(),
                    self.agent.clone(),
                    manifest.value(),
                ) {
                    Ok(document) => document,
                    Err(error) => {
                        respond.try_send(Err(error.into()));
                        return Ok(None);
                    }
                };
                match self.start(document, false).await {
                    Ok(mut running) => {
                        running.emit(
                            AgentEventSource::Controller,
                            AgentEvent::DesiredStateAccepted {
                                generation: running.documents.private.generation(),
                            },
                        );
                        running.pending_responses.push(PendingResponse::AgentDocument(respond));
                        Ok(Some(running))
                    }
                    Err(error) => {
                        respond.try_send(Err(error));
                        Ok(None)
                    }
                }
            }
            AgentInput::Command(AgentCommand::OpenStream {
                replay_from_generation,
                items,
                respond,
            }) => {
                self.pending_streams.push(PendingOpenStream {
                    replay_from_generation,
                    items,
                });
                respond.try_send(Ok(()));
                Ok(None)
            }
            AgentInput::Command(command) => {
                match command {
                    AgentCommand::Scale { respond, .. } | AgentCommand::Delete { respond } => drop(respond),
                    AgentCommand::InstanceStatus { instance, respond } => {
                        respond.try_send(Err(instance_not_found(&self.agent, instance)));
                    }
                    AgentCommand::InstanceLogs { instance, respond, .. } => {
                        respond.try_send(Err(instance_not_found(&self.agent, instance)));
                    }
                    AgentCommand::InstanceExec { instance, respond, .. } => {
                        respond.try_send(Err(instance_not_found(&self.agent, instance)));
                    }
                    AgentCommand::InstanceShell { instance, respond, .. } => {
                        respond.try_send(Err(instance_not_found(&self.agent, instance)));
                    }
                    AgentCommand::ListItems { respond } => respond.try_send(Ok(Vec::new())),
                    AgentCommand::Document { respond } => {
                        respond.try_send(Err(Error::InstanceNotFound {
                            name: self.agent.to_string(),
                        }));
                    }
                    AgentCommand::Apply { .. } | AgentCommand::OpenStream { .. } => {
                        unreachable!("handled by starting state")
                    }
                }
                Ok(None)
            }
            AgentInput::Work(_) => Ok(None),
        }
    }

    async fn load(&mut self) -> Result<Option<RunningAgentState>, Error> {
        let path = self.layout.agent_document(&self.agent);
        let Some(document) = try_read_agent_document(&path).await? else {
            return Ok(None);
        };
        if document.agent() != &self.agent {
            return Err(Error::PersistedStateIdentityMismatch {
                path,
                expected: self.agent.to_string(),
                actual: document.agent().to_string(),
            });
        }
        self.start(document, true).await.map(Some)
    }

    async fn start(&mut self, document: AgentDocument, persisted: bool) -> Result<RunningAgentState, Error> {
        let events_path = self.layout.agent_events(document.agent());
        let (event_plan, inspected_instances) = if persisted {
            let instance_documents = load_instance_documents(&self.layout, document.agent()).await?;
            let inspected_instances =
                inspect_instance_states(&self.layout, document.agent(), instance_documents).await?;
            (Some(inspect_event_sequence(&events_path).await?), inspected_instances)
        } else {
            (None, BTreeMap::new())
        };
        let event_sequence = match event_plan {
            Some(plan) => plan.apply().await?,
            None => 1,
        };
        let prepared_instances = apply_instance_event_repairs(inspected_instances).await?;
        let events = EventLogWriter::spawn(&self.context, events_path);
        let instances = start_instance_states(&self.context, &self.layout, document.agent(), prepared_instances);
        let documents = AgentDocuments::new(document.clone(), persisted.then(|| document.clone()));
        let base = if persisted {
            document
                .ready_agent_base_key()
                .cloned()
                .map_or(AgentBaseState::Missing, |key| AgentBaseState::Ready { key })
        } else {
            AgentBaseState::Missing
        };
        Ok(RunningAgentState {
            agent: self.agent.clone(),
            context: self.context.clone(),
            documents,
            pending_responses: std::mem::take(&mut self.pending_streams)
                .into_iter()
                .map(|stream| PendingResponse::OpenStream {
                    replay_from_generation: stream.replay_from_generation,
                    items: stream.items,
                    respond: None,
                })
                .collect(),
            recent_events: VecDeque::new(),
            event_sequence,
            events,
            layout: self.layout.clone(),
            backend: self.backend.clone(),
            tailscale: Rc::clone(&self.tailscale),
            base,
            instances,
            streams: Vec::new(),
            input: self.input.clone(),
            next_reconcile: Instant::now(),
            next_work_id: 0,
        })
    }
}

impl LoadFailedAgentState {
    fn handle_input(&self, input: AgentInput) {
        let error = || Error::PersistedStateUnavailable {
            name: self.agent.to_string(),
            message: self.message.clone(),
        };
        match input {
            AgentInput::Load | AgentInput::Work(_) => {}
            AgentInput::Command(command) => match command {
                AgentCommand::Apply { respond, .. }
                | AgentCommand::Scale { respond, .. }
                | AgentCommand::Delete { respond }
                | AgentCommand::Document { respond } => respond.try_send(Err(error())),
                AgentCommand::OpenStream { respond, .. } => respond.try_send(Err(error())),
                AgentCommand::InstanceStatus { respond, .. } => respond.try_send(Err(error())),
                AgentCommand::InstanceLogs { respond, .. } => respond.try_send(Err(error())),
                AgentCommand::InstanceExec { respond, .. } => respond.try_send(Err(error())),
                AgentCommand::InstanceShell { respond, .. } => respond.try_send(Err(error())),
                AgentCommand::ListItems { respond } => respond.try_send(Err(error())),
            },
        }
    }
}

impl RunningAgentState {
    fn allocate_work_id(&mut self) -> u64 {
        let work_id = self.next_work_id;
        let Some(next_work_id) = self.next_work_id.checked_add(1) else {
            std::process::abort();
        };
        self.next_work_id = next_work_id;
        work_id
    }

    fn work_services(&self) -> AgentWorkServices {
        AgentWorkServices {
            input: self.input.clone(),
            backend: self.backend.clone(),
            tailscale: Rc::clone(&self.tailscale),
        }
    }

    fn next_wake(&self) -> Option<Instant> {
        let mut deadline = Some(self.next_reconcile);
        for instance in self.instances.values() {
            let Some(instance) = instance.running_ref() else {
                if let AgentInstanceState::Starting(starting) = instance
                    && let Some(retry) = starting.retry_at
                {
                    deadline = Some(deadline.map_or(retry, |current| current.min(retry)));
                }
                continue;
            };
            if let Some(retry) = instance.bootstrap_retry_wake() {
                deadline = Some(deadline.map_or(retry, |current| current.min(retry)));
            }
            if should_reconcile_runtime_secrets(instance) {
                deadline = Some(deadline.map_or(instance.runtime_secrets.next_at, |current| {
                    current.min(instance.runtime_secrets.next_at)
                }));
            }
            if should_reconcile_host_inputs(instance) {
                deadline = Some(deadline.map_or(instance.host_inputs.next_at, |current| {
                    current.min(instance.host_inputs.next_at)
                }));
            }
            if !instance.blocks_instance_transition()
                && matches!(
                    instance.documents.private.status.phase,
                    AgentInstancePhase::Deleting | AgentInstancePhase::Deleted
                )
                && cleanup_phase_persisted(instance, instance.documents.private.status.phase)
            {
                let retry = instance.cleanup_retry.unwrap_or_else(Instant::now);
                deadline = Some(deadline.map_or(retry, |current| current.min(retry)));
            }
        }
        deadline
    }

    fn handle_command(&mut self, command: AgentCommand) {
        match command {
            AgentCommand::Apply { manifest, respond } => {
                if let Err(error) = self.apply_manifest(&manifest) {
                    respond.try_send(Err(error));
                } else {
                    self.pending_responses.push(PendingResponse::AgentDocument(respond));
                }
            }
            AgentCommand::OpenStream {
                replay_from_generation,
                items,
                respond,
            } => {
                self.pending_responses.push(PendingResponse::OpenStream {
                    replay_from_generation,
                    items,
                    respond: Some(respond),
                });
            }
            AgentCommand::InstanceStatus { instance, respond } => {
                self.pending_responses
                    .push(PendingResponse::InstanceDocument { id: instance, respond });
            }
            AgentCommand::InstanceLogs {
                instance,
                params,
                respond,
            } => self.start_instance_logs(instance, &params, respond),
            AgentCommand::InstanceExec {
                context,
                instance,
                params,
                output,
                respond,
            } => self.start_instance_exec(context, instance, params, output, respond),
            AgentCommand::InstanceShell {
                context,
                instance,
                respond,
            } => self.start_instance_shell(context, instance, respond),
            AgentCommand::ListItems { respond } => {
                self.pending_responses.push(PendingResponse::ListItems(respond));
            }
            AgentCommand::Document { respond } => {
                self.pending_responses.push(PendingResponse::AgentDocument(respond));
            }
            AgentCommand::Scale { replicas, respond } => {
                if let Err(error) = self.documents.private.set_replicas_if_changed(replicas) {
                    respond.try_send(Err(error.into()));
                    return;
                }
                let generation = self.documents.private.generation();
                self.emit(
                    AgentEventSource::Controller,
                    AgentEvent::ScaleAccepted { generation, replicas },
                );
                self.pending_responses.push(PendingResponse::AgentDocument(respond));
            }
            AgentCommand::Delete { respond } => {
                self.documents.private.mark_deletion_requested_if_changed();
                let generation = self.documents.private.generation();
                self.emit(AgentEventSource::Controller, AgentEvent::DeleteAccepted { generation });
                self.pending_responses.push(PendingResponse::AgentDocument(respond));
            }
        }
    }

    fn instance_document(&self, id: AgentInstanceId) -> Result<AgentInstanceDocument, Error> {
        self.instances
            .get(&id)
            .and_then(AgentInstanceState::running_ref)
            .map(|instance| instance.documents.public.clone())
            .ok_or_else(|| instance_not_found(&self.agent, id))
    }

    fn open_stream(&mut self, replay_from_generation: Option<u64>, mut items: spsc::Sender<AgentStreamItem>) {
        let _result = items.try_send(AgentStreamItem::Document(Box::new(self.documents.public.clone())));
        if let Some(generation) = replay_from_generation {
            for event in self.recent_events.iter().filter(|event| event.generation >= generation) {
                let _result = items.try_send(AgentStreamItem::Event(event.clone()));
            }
        }
        self.streams.push(items);
    }

    fn start_instance_logs(
        &self,
        id: AgentInstanceId,
        params: &AgentInstanceLogsParams,
        respond: oneshot::Sender<Result<AgentInstanceLogsResult, Error>>,
    ) {
        let file = params.file;
        let lines = params.lines;
        if lines == 0 {
            respond.try_send(Err(Error::InvalidLogLines));
            return;
        }
        if params.filter.is_some() && file != LogFile::Events {
            respond.try_send(Err(Error::InvalidLogFilter));
            return;
        }
        let Some(instance) = self.instances.get(&id).and_then(AgentInstanceState::running_ref) else {
            respond.try_send(Err(instance_not_found(&self.agent, id)));
            return;
        };
        let path = match file {
            LogFile::Events => self
                .layout
                .instance(self.documents.private.agent(), id)
                .instance_events(),
            LogFile::Serial | LogFile::Qemu => self.backend.log_path(&instance.documents.private.status.backend, file),
        };
        let document = instance.documents.private.clone();
        let filter = params.filter;
        tokio::task::spawn_local(async move {
            let contents = match filter {
                Some(LogFilter::Network { errors, event_kind }) => {
                    read_network_event_log_tail(&path, lines, errors, event_kind).await
                }
                None => read_log_tail(&path, lines).await,
            };
            let result = contents.map(|contents| AgentInstanceLogsResult {
                name: document.name(),
                file: file.as_str().to_owned(),
                path: path.display().to_string(),
                lines,
                contents,
            });
            respond.try_send(result);
        });
    }

    fn start_instance_shell(
        &self,
        context: Context,
        id: AgentInstanceId,
        respond: oneshot::Sender<Result<AgentInstanceShellResult, Error>>,
    ) {
        let Some(instance) = self.instances.get(&id).and_then(AgentInstanceState::running_ref) else {
            respond.try_send(Err(instance_not_found(&self.agent, id)));
            return;
        };
        let document = instance.documents.private.clone();
        let network = Rc::clone(&instance.network_runtime);
        let backend = self.backend.clone();
        let manifest = match manifest_context(&self.documents.private) {
            Ok(manifest) => manifest,
            Err(error) => {
                respond.try_send(Err(error));
                return;
            }
        };
        tokio::task::spawn_local(async move {
            let result = async {
                backend
                    .ensure_attached(&context, &network, &manifest, &document)
                    .await?;
                let command = backend.shell_command(&document).await?;
                Ok(AgentInstanceShellResult {
                    name: document.name(),
                    command,
                })
            }
            .await;
            respond.try_send(result);
        });
    }

    fn list_items(&self) -> Vec<AgentInstanceListItem> {
        self.instances
            .values()
            .filter_map(AgentInstanceState::running_ref)
            .map(|instance| {
                let document = &instance.documents.public;
                let reconciliation = document.status.reconciliation.as_ref();
                let stale = reconciliation.is_some_and(|state| state.stale);
                AgentInstanceListItem {
                    name: document.name(),
                    agent: document.metadata.agent.to_string(),
                    instance: document.metadata.name.to_string(),
                    instance_id: document.metadata.id.as_u32(),
                    status: document.status.phase.to_string(),
                    stale,
                    stale_reason: reconciliation
                        .and_then(|state| stale.then(|| state.reason.clone()))
                        .flatten(),
                    process_status: reconciliation.map(|state| state.observed_status.clone()),
                    process_message: reconciliation.and_then(|state| state.reason.clone()),
                    pid: reconciliation.and_then(|state| state.observed_pid),
                    ready: document.status.readiness.as_ref().map(|readiness| {
                        readiness.ready
                            && document
                                .status
                                .host_inputs
                                .is_ready_for(document.spec.desired_generation)
                    }),
                }
            })
            .collect()
    }

    fn start_instance_exec(
        &mut self,
        context: Context,
        id: AgentInstanceId,
        params: AgentInstanceExecParams,
        output: spsc::Sender<AgentInstanceSessionOutput>,
        respond: oneshot::Sender<Result<AgentInstanceExecResult, Error>>,
    ) {
        if params.command.is_empty() {
            respond.try_send(Err(Error::EmptyExecCommand));
            return;
        }
        let Some(timeout) = params.timeout_seconds.map(std::time::Duration::from_secs) else {
            self.start_exec_command(
                context,
                id,
                params.command,
                std::time::Duration::from_mins(5),
                Some(output),
                respond,
            );
            return;
        };
        if timeout.is_zero() {
            respond.try_send(Err(Error::InvalidExecTimeout));
            return;
        }
        self.start_exec_command(context, id, params.command, timeout, Some(output), respond);
    }

    fn start_exec_command(
        &mut self,
        context: Context,
        id: AgentInstanceId,
        command: Vec<String>,
        timeout: std::time::Duration,
        output: Option<spsc::Sender<AgentInstanceSessionOutput>>,
        respond: oneshot::Sender<Result<AgentInstanceExecResult, Error>>,
    ) {
        let manifest = match manifest_context(&self.documents.private) {
            Ok(manifest) => manifest,
            Err(error) => {
                respond.try_send(Err(error));
                return;
            }
        };
        let shell_command = shell_join(&command);
        let work_id = self.allocate_work_id();
        let (instance_document, network) = {
            let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
                respond.try_send(Err(instance_not_found(&self.agent, id)));
                return;
            };
            if instance.session.is_some() {
                respond.try_send(Err(Error::OperationInProgress {
                    name: format!("{}/{}", self.agent, id),
                    operation: "foreground session",
                }));
                return;
            }
            if instance.documents.private.status.phase != AgentInstancePhase::Running {
                respond.try_send(Err(Error::InvalidStatus {
                    name: format!("{}/{}", self.agent, id),
                    status: instance.documents.private.status.phase.to_string(),
                }));
                return;
            }
            let instance_document = instance.documents.private.clone();
            let network = Rc::clone(&instance.network_runtime);
            instance.session = Some(ForegroundSession { work_id, respond });
            instance.documents.private.status.work.sessions.active = 1;
            (instance_document, network)
        };
        self.emit_instance_event(
            id,
            AgentInstanceEventSource::Session {
                kind: SessionKind::Exec,
            },
            AgentInstanceEvent::SessionStarted {
                session: SessionKind::Exec,
            },
        );
        spawn_exec(ExecTask {
            input: self.input.clone(),
            backend: self.backend.clone(),
            context,
            manifest,
            network,
            document: instance_document,
            id,
            work_id,
            command,
            shell_command,
            output,
            timeout,
        });
    }

    fn apply_manifest(&mut self, manifest: &AgentManifestContext) -> Result<(), Error> {
        self.validate_mediated_auth_bindings(manifest.value())?;
        let source_path = manifest.source_path().display().to_string();
        let previous_generation = self.documents.private.generation();
        let updated = if self.documents.private.status.deleted {
            AgentDocument::from_manifest(source_path, self.agent.clone(), manifest.value())?
        } else {
            AgentDocument::from_manifest_after_existing(
                source_path,
                self.agent.clone(),
                manifest.value(),
                &self.documents.private,
            )?
        };
        if self.documents.private == updated {
            return Ok(());
        }
        self.documents.write(updated.clone(), &self.base, &mut self.instances);
        let generation_changed = updated.generation() != previous_generation;
        if generation_changed {
            for instance in self.instances.values_mut().filter_map(AgentInstanceState::running_mut) {
                instance.runtime_secrets.next_at = Instant::now();
                instance.runtime_secrets.failure_count = 0;
                instance.secret_host_files = None;
                instance.host_inputs.next_at = Instant::now();
                instance.host_inputs.failure_count = 0;
            }
        }
        if (generation_changed
            || (updated.desired_agent_base_key().is_none() && updated.ready_agent_base_key().is_none()))
            && !matches!(
                self.base,
                AgentBaseState::Preparing { .. } | AgentBaseState::Building { .. } | AgentBaseState::Stopping { .. }
            )
        {
            self.base = AgentBaseState::Missing;
        }
        self.emit(
            AgentEventSource::Controller,
            AgentEvent::DesiredStateAccepted {
                generation: updated.generation(),
            },
        );
        Ok(())
    }

    fn validate_mediated_auth_bindings(&self, next: &agentdp_core::manifest::AgentManifest) -> Result<(), Error> {
        if self.documents.private.status.deleted || !self.has_provisioned_state() {
            return Ok(());
        }
        let current = self.documents.private.manifest().host_input_requirements();
        let next = next.host_input_requirements();
        if current.has_same_mediated_auth_bindings(&next) {
            return Ok(());
        }
        Err(Error::MediatedAuthBindingsChanged)
    }

    fn has_provisioned_state(&self) -> bool {
        !self.instances.is_empty() || self.base.has_provisioned_resources()
    }

    fn handle_work_completion(&mut self, completion: WorkCompletion) {
        match completion {
            WorkCompletion::BasePrepared { .. }
            | WorkCompletion::BaseBuilt { .. }
            | WorkCompletion::BaseFailed { .. }
            | WorkCompletion::BaseStopped { .. }
            | WorkCompletion::BaseStopTimedOut { .. } => self.handle_base_completion(completion),
            WorkCompletion::InstanceCreated { .. }
            | WorkCompletion::InstanceReconciled { .. }
            | WorkCompletion::InstanceStarted { .. }
            | WorkCompletion::InstanceStopped { .. }
            | WorkCompletion::InstanceDeleteRuntimeStopped { .. }
            | WorkCompletion::InstanceDeleted { .. }
            | WorkCompletion::InstanceDeleteTimedOut { .. } => self.handle_instance_completion(completion),
            WorkCompletion::BootstrapStarted { .. }
            | WorkCompletion::BootstrapEvent { .. }
            | WorkCompletion::BootstrapFinished { .. } => self.handle_bootstrap_completion(completion),
            WorkCompletion::TailscaleServeReconciled { .. } => self.handle_tailscale_completion(&completion),
            WorkCompletion::RuntimeSecretsReconciled { .. } => self.handle_runtime_secrets_completion(completion),
            WorkCompletion::HostInputsReconciled { .. } => self.handle_host_inputs_completion(completion),
            WorkCompletion::ExecFinished { .. } => self.handle_exec_completion(&completion),
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "base state transitions are kept together so prepare/build/failure/delete races are readable"
    )]
    fn handle_base_completion(&mut self, completion: WorkCompletion) {
        match completion {
            WorkCompletion::BasePrepared {
                work_id,
                generation,
                preparation,
            } => {
                if !matches!(self.base, AgentBaseState::Preparing { work_id: active, .. } if active == work_id) {
                    return;
                }
                if generation != self.documents.private.generation() {
                    self.base = AgentBaseState::Missing;
                    return;
                }
                let key = preparation.key().clone();
                self.documents.private.mark_agent_base_desired(key.clone());
                let document = self.documents.private.clone();
                self.base = AgentBaseState::Building {
                    work_id,
                    generation,
                    key: key.clone(),
                };
                spawn_build_base(
                    self.input.clone(),
                    self.context.clone(),
                    self.backend.clone(),
                    self.layout.clone(),
                    document,
                    TaskAttempt { work_id, generation },
                    *preparation,
                );
                self.emit(AgentEventSource::AgentBase, AgentEvent::AgentBaseStarted { key });
            }
            WorkCompletion::BaseBuilt {
                work_id,
                generation,
                key,
            } => {
                if !matches!(
                    &self.base,
                    AgentBaseState::Building {
                        work_id: active_work,
                        generation: active,
                        key: active_key,
                    } if *active_work == work_id && *active == generation && active_key == &key
                ) {
                    return;
                }
                if self.documents.private.deletion_requested() {
                    self.documents.private.mark_agent_base_ready(key.clone());
                    self.base = AgentBaseState::Ready { key };
                    return;
                }
                if generation != self.documents.private.generation() {
                    self.base = AgentBaseState::Missing;
                    return;
                }
                self.documents.private.mark_agent_base_ready(key.clone());
                self.base = AgentBaseState::Ready { key: key.clone() };
                self.emit(AgentEventSource::AgentBase, AgentEvent::AgentBaseReady { key });
            }
            WorkCompletion::BaseFailed {
                work_id,
                generation,
                error,
            } => {
                let failed_key = match &self.base {
                    AgentBaseState::Preparing { work_id: active, .. } if *active == work_id => {
                        self.documents.private.desired_agent_base_key().cloned()
                    }
                    AgentBaseState::Building {
                        work_id: active, key, ..
                    } if *active == work_id => Some(key.clone()),
                    _ => return,
                };
                if generation != self.documents.private.generation() {
                    self.base = AgentBaseState::Missing;
                    return;
                }
                self.base = AgentBaseState::Failed {
                    generation,
                    key: failed_key.clone(),
                };
                self.documents.private.mark_agent_base_failed(error.clone());
                self.emit(
                    AgentEventSource::AgentBase,
                    AgentEvent::AgentBaseFailed {
                        key: failed_key.unwrap_or_else(|| AgentBaseKey::new("unknown")),
                        error,
                    },
                );
            }
            WorkCompletion::BaseStopped { work_id } => {
                if matches!(self.base, AgentBaseState::Stopping { work_id: active } if active == work_id) {
                    self.base = AgentBaseState::Stopped;
                }
            }
            WorkCompletion::BaseStopTimedOut { work_id } => {
                if matches!(self.base, AgentBaseState::Stopping { work_id: active } if active == work_id) {
                    self.base = AgentBaseState::Stopped;
                    self.emit(
                        AgentEventSource::AgentBase,
                        AgentEvent::Diagnostic {
                            level: EventLevel::Warn,
                            message: "agent base stop timed out; delete continued after bounded cleanup".to_owned(),
                        },
                    );
                }
            }
            WorkCompletion::InstanceCreated { .. }
            | WorkCompletion::InstanceReconciled { .. }
            | WorkCompletion::InstanceStarted { .. }
            | WorkCompletion::BootstrapStarted { .. }
            | WorkCompletion::BootstrapEvent { .. }
            | WorkCompletion::BootstrapFinished { .. }
            | WorkCompletion::TailscaleServeReconciled { .. }
            | WorkCompletion::RuntimeSecretsReconciled { .. }
            | WorkCompletion::HostInputsReconciled { .. }
            | WorkCompletion::InstanceStopped { .. }
            | WorkCompletion::InstanceDeleteRuntimeStopped { .. }
            | WorkCompletion::InstanceDeleted { .. }
            | WorkCompletion::InstanceDeleteTimedOut { .. }
            | WorkCompletion::ExecFinished { .. } => {}
        }
    }

    fn handle_instance_completion(&mut self, completion: WorkCompletion) {
        match completion {
            WorkCompletion::InstanceCreated {
                id,
                work_id,
                generation,
                document,
            } => self.complete_instance_create(id, work_id, generation, *document),
            WorkCompletion::InstanceStarted {
                id,
                work_id,
                generation,
                outcome,
            } => self.complete_instance_start(id, work_id, generation, outcome),
            WorkCompletion::InstanceReconciled {
                id,
                work_id,
                generation,
                outcome,
            } => self.complete_instance_reconcile(id, work_id, generation, outcome),
            WorkCompletion::InstanceStopped {
                id,
                work_id,
                generation,
                control,
                outcome,
            } => self.complete_instance_stop(id, work_id, generation, control, outcome),
            WorkCompletion::InstanceDeleteRuntimeStopped {
                id,
                work_id,
                generation,
                control,
                outcome,
            } => self.complete_instance_delete_runtime(id, work_id, generation, control, outcome),
            WorkCompletion::InstanceDeleted { id, work_id } => self.complete_instance_delete(id, work_id, None),
            WorkCompletion::InstanceDeleteTimedOut { id, work_id, error } => {
                self.complete_instance_delete(id, work_id, Some(error));
            }
            WorkCompletion::BasePrepared { .. }
            | WorkCompletion::BaseBuilt { .. }
            | WorkCompletion::BaseFailed { .. }
            | WorkCompletion::BaseStopped { .. }
            | WorkCompletion::BaseStopTimedOut { .. }
            | WorkCompletion::BootstrapStarted { .. }
            | WorkCompletion::BootstrapEvent { .. }
            | WorkCompletion::BootstrapFinished { .. }
            | WorkCompletion::TailscaleServeReconciled { .. }
            | WorkCompletion::RuntimeSecretsReconciled { .. }
            | WorkCompletion::HostInputsReconciled { .. }
            | WorkCompletion::ExecFinished { .. } => {}
        }
    }

    fn complete_instance_delete_runtime(
        &mut self,
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        control: Option<backend::InstanceControl>,
        outcome: InstanceStopOutcome,
    ) {
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        if !instance.work.is_some_and(|work| {
            work.id == work_id && work.kind == AgentInstanceWorkKind::Deleting && work.input_generation == generation
        }) {
            return;
        }
        instance.control = control;
        instance.documents.private.status.backend = outcome.backend;
        instance.documents.private.status.tailscale_serve = outcome.tailscale_serve;
        match outcome.result {
            Ok(_) => {
                clear_instance_work(instance);
                instance.cleanup_retry = None;
                instance.documents.private.status.phase = AgentInstancePhase::Deleted;
            }
            Err(error) => {
                clear_instance_work(instance);
                instance.cleanup_retry = Some(Instant::now() + INSTANCE_DELETE_RETRY_DELAY);
                self.emit(
                    AgentEventSource::Instance { id },
                    AgentEvent::Diagnostic {
                        level: EventLevel::Warn,
                        message: format!("{id}: instance delete timed out; cleanup will retry: {error}"),
                    },
                );
            }
        }
    }

    fn complete_instance_delete(&mut self, id: AgentInstanceId, work_id: u64, error: Option<String>) {
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        if !instance
            .work
            .is_some_and(|work| work.id == work_id && work.kind == AgentInstanceWorkKind::Deleting)
        {
            return;
        }
        if let Some(error) = error {
            clear_instance_work(instance);
            instance.cleanup_retry = Some(Instant::now() + INSTANCE_DELETE_RETRY_DELAY);
            self.emit(
                AgentEventSource::Instance { id },
                AgentEvent::Diagnostic {
                    level: EventLevel::Warn,
                    message: format!("{id}: instance delete timed out; cleanup will retry: {error}"),
                },
            );
            return;
        }
        self.instances.remove(&id);
        self.emit(
            AgentEventSource::Controller,
            AgentEvent::InstanceDeleted { instance_id: id },
        );
    }

    fn complete_instance_create(
        &mut self,
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        document: Result<AgentInstanceDocument, String>,
    ) {
        if !matches!(
            self.instances.get(&id),
            Some(AgentInstanceState::Starting(pending)) if pending.work_id == work_id
        ) {
            return;
        }
        let Some(AgentInstanceState::Starting(pending)) = self.instances.remove(&id) else {
            return;
        };
        match document {
            Ok(document) => {
                self.instances
                    .insert(id, running_instance_from_created_document(document, pending));
                if generation == self.documents.private.generation() {
                    self.emit(
                        AgentEventSource::Controller,
                        AgentEvent::InstanceCreated { instance_id: id },
                    );
                }
            }
            Err(_) if generation != self.documents.private.generation() => {}
            Err(error) => {
                let mut pending = pending;
                pending.failure_count = pending.failure_count.saturating_add(1);
                pending.retry_at = Some(Instant::now() + instance_create_retry_delay(pending.failure_count));
                self.instances.insert(id, AgentInstanceState::Starting(pending));
                self.emit(
                    AgentEventSource::Controller,
                    AgentEvent::Diagnostic {
                        level: EventLevel::Warn,
                        message: format!("{id}: failed to create instance: {error}"),
                    },
                );
            }
        }
    }

    fn complete_instance_start(
        &mut self,
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        outcome: InstanceBackendOutcome<backend::StartOutput>,
    ) {
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        if !instance.work.is_some_and(|work| {
            work.id == work_id && work.kind == AgentInstanceWorkKind::Starting && work.input_generation == generation
        }) {
            return;
        }
        let relevant = generation == instance.documents.private.spec.desired_generation;
        let cleaning_up = instance.documents.private.spec.target == AgentInstanceTarget::Deleting
            || matches!(
                instance.documents.private.status.phase,
                AgentInstancePhase::Deleting | AgentInstancePhase::Deleted
            );
        instance.documents.private.status.backend = outcome.backend;
        match outcome.result {
            Ok(started) if !cleaning_up => {
                if relevant {
                    apply_bound_host_ports(&mut instance.documents.private.status.network, &started.host_ports);
                }
                instance.documents.private.status.phase = AgentInstancePhase::Running;
                instance.documents.private.status.clear_readiness();
                instance.documents.private.status.reconciliation =
                    relevant.then_some(agentdp_core::agent::ReconciliationState {
                        stale: false,
                        observed_status: started.process.status,
                        observed_pid: started.process.pid,
                        reason: started.process.message,
                    });
            }
            Ok(_) => {
                instance.documents.private.status.clear_readiness();
                instance.documents.private.status.reconciliation = None;
                instance.cleanup_retry = None;
            }
            Err(error) if relevant && !cleaning_up => mark_instance_transition_failed(instance, error),
            Err(_) => {}
        }
        clear_instance_work(instance);
    }

    fn complete_instance_reconcile(
        &mut self,
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        outcome: InstanceBackendOutcome<backend::ReconcileOutput>,
    ) {
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        let Some(work) = instance.work.filter(|work| {
            work.id == work_id
                && matches!(
                    work.kind,
                    AgentInstanceWorkKind::Reconciling | AgentInstanceWorkKind::Repairing
                )
                && work.input_generation == generation
        }) else {
            return;
        };
        let repairing = work.kind == AgentInstanceWorkKind::Repairing;
        let relevant = generation == instance.documents.private.spec.desired_generation;
        instance.documents.private.status.backend = outcome.backend;
        if repairing && outcome.result.is_ok() {
            instance.runtime_repair = RuntimeRepairState::Idle;
        }
        match outcome.result {
            Ok(reconciled) if relevant => {
                apply_bound_host_ports(&mut instance.documents.private.status.network, &reconciled.host_ports);
                instance.documents.private.status.reconciliation = Some(agentdp_core::agent::ReconciliationState {
                    stale: reconciled.stale,
                    observed_status: reconciled.process.status,
                    observed_pid: reconciled.process.pid,
                    reason: reconciled.process.message,
                });
                if reconciled.mark_stopped {
                    instance.documents.private.status.phase = AgentInstancePhase::Stopped;
                    instance.documents.private.status.clear_readiness();
                }
            }
            Ok(reconciled) => {
                if reconciled.mark_stopped {
                    instance.documents.private.status.phase = AgentInstancePhase::Stopped;
                    instance.documents.private.status.clear_readiness();
                }
                instance.documents.private.status.reconciliation = None;
            }
            Err(error) if relevant && !repairing => mark_instance_transition_failed(instance, error),
            Err(_) => {}
        }
        clear_instance_work(instance);
    }

    fn complete_instance_stop(
        &mut self,
        id: AgentInstanceId,
        work_id: u64,
        generation: u64,
        control: Option<backend::InstanceControl>,
        outcome: InstanceStopOutcome,
    ) {
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        if !instance.work.is_some_and(|work| {
            work.id == work_id && work.kind == AgentInstanceWorkKind::Stopping && work.input_generation == generation
        }) {
            return;
        }
        let relevant = generation == instance.documents.private.spec.desired_generation;
        instance.control = control;
        instance.documents.private.status.backend = outcome.backend;
        instance.documents.private.status.tailscale_serve = outcome.tailscale_serve;
        match outcome.result {
            Ok(stopped) => {
                instance.documents.private.status.phase = AgentInstancePhase::Stopped;
                instance.documents.private.status.clear_readiness();
                instance.documents.private.status.reconciliation = Some(agentdp_core::agent::ReconciliationState {
                    stale: false,
                    observed_status: stopped.process_status.to_owned(),
                    observed_pid: None,
                    reason: None,
                });
            }
            Err(error) if relevant => mark_instance_transition_failed(instance, error),
            Err(_) => {}
        }
        clear_instance_work(instance);
    }

    #[allow(
        clippy::too_many_lines,
        reason = "bootstrap state transitions are kept together so success, failure, and event emission remain visible in one place"
    )]
    fn handle_bootstrap_completion(&mut self, completion: WorkCompletion) {
        match completion {
            WorkCompletion::BootstrapStarted {
                id,
                work_id,
                generation,
            } => {
                let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
                    return;
                };
                if generation == instance.documents.private.spec.desired_generation
                    && instance.work.is_some_and(|work| {
                        work.id == work_id
                            && work.kind == AgentInstanceWorkKind::Bootstrapping
                            && work.input_generation == generation
                    })
                {
                    instance.documents.private.status.work.bootstrap = Some(AgentInstanceBootstrapWorkStatus {
                        phase: AgentInstanceBootstrapWorkPhase::Running,
                        current_step: None,
                        last_error: None,
                        failure_count: None,
                        next_retry_unix_seconds: None,
                    });
                    self.emit_instance_event(
                        id,
                        AgentInstanceEventSource::Bootstrap,
                        AgentInstanceEvent::BootstrapStarted,
                    );
                }
            }
            WorkCompletion::BootstrapEvent {
                source,
                work_id,
                generation,
                event,
            } => self.apply_bootstrap_event(source, work_id, generation, event),
            WorkCompletion::BootstrapFinished {
                id,
                work_id,
                generation,
                control,
                result,
            } => {
                let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
                    return;
                };
                if !instance.work.is_some_and(|work| {
                    work.id == work_id
                        && work.kind == AgentInstanceWorkKind::Bootstrapping
                        && work.input_generation == generation
                }) {
                    return;
                }
                instance.control = control;
                if generation != instance.documents.private.spec.desired_generation {
                    if let Ok(tailscale_serve) = result {
                        instance.documents.private.status.tailscale_serve = tailscale_serve;
                    }
                    clear_instance_work(instance);
                    instance.bootstrap_retry = None;
                    return;
                }
                let tailscale_serve = match result {
                    Ok(tailscale_serve) => tailscale_serve,
                    Err(failure) => {
                        let BootstrapTaskFailure { attempt_epoch, error } = failure;
                        let now = time::unix_seconds();
                        let failure_count = instance
                            .documents
                            .private
                            .status
                            .bootstrap
                            .as_ref()
                            .map_or(1, |state| state.failure_count.saturating_add(1));
                        let observed_attempt_epoch = attempt_epoch.or_else(|| {
                            instance
                                .documents
                                .private
                                .status
                                .bootstrap
                                .as_ref()
                                .and_then(|state| state.attempt_epoch)
                        });
                        let retry_delay = bootstrap_retry_delay(failure_count);
                        let next_retry_unix_seconds = now.saturating_add(retry_delay.as_secs());
                        instance
                            .documents
                            .private
                            .status
                            .record_bootstrap_failure(AgentInstanceBootstrapState {
                                attempt_epoch: observed_attempt_epoch,
                                failure_count,
                                last_failure_unix_seconds: now,
                                next_retry_unix_seconds,
                                last_error: error.clone(),
                            });
                        instance.bootstrap_retry = Some(Instant::now() + retry_delay);
                        instance.documents.private.status.work.bootstrap = Some(AgentInstanceBootstrapWorkStatus {
                            phase: AgentInstanceBootstrapWorkPhase::BackingOff,
                            current_step: None,
                            last_error: Some(error.clone()),
                            failure_count: Some(failure_count),
                            next_retry_unix_seconds: Some(next_retry_unix_seconds),
                        });
                        instance.work = None;
                        self.emit_instance_event(
                            id,
                            AgentInstanceEventSource::Bootstrap,
                            AgentInstanceEvent::BootstrapFinished {
                                result: OperationResult::Failed { error },
                            },
                        );
                        return;
                    }
                };
                instance.documents.private.status.tailscale_serve = tailscale_serve;
                let readiness_result = ready_result(&instance.documents.private);
                instance.documents.private.status.mark_ready(ReadinessState {
                    ready: true,
                    last_success_unix_seconds: time::unix_seconds(),
                    result: readiness_result,
                });
                instance.bootstrap_retry = None;
                clear_instance_work(instance);
                self.emit_instance_event(
                    id,
                    AgentInstanceEventSource::Bootstrap,
                    AgentInstanceEvent::BootstrapFinished {
                        result: OperationResult::Succeeded,
                    },
                );
            }
            WorkCompletion::BasePrepared { .. }
            | WorkCompletion::BaseBuilt { .. }
            | WorkCompletion::BaseFailed { .. }
            | WorkCompletion::BaseStopped { .. }
            | WorkCompletion::BaseStopTimedOut { .. }
            | WorkCompletion::InstanceCreated { .. }
            | WorkCompletion::InstanceReconciled { .. }
            | WorkCompletion::InstanceStarted { .. }
            | WorkCompletion::InstanceStopped { .. }
            | WorkCompletion::InstanceDeleteRuntimeStopped { .. }
            | WorkCompletion::InstanceDeleted { .. }
            | WorkCompletion::InstanceDeleteTimedOut { .. }
            | WorkCompletion::TailscaleServeReconciled { .. }
            | WorkCompletion::RuntimeSecretsReconciled { .. }
            | WorkCompletion::HostInputsReconciled { .. }
            | WorkCompletion::ExecFinished { .. } => {}
        }
    }

    fn handle_tailscale_completion(&mut self, completion: &WorkCompletion) {
        let WorkCompletion::TailscaleServeReconciled {
            id,
            work_id,
            generation,
            result,
        } = completion
        else {
            return;
        };
        let mut warning = None;
        {
            let Some(instance) = self.instances.get_mut(id).and_then(AgentInstanceState::running_mut) else {
                return;
            };
            if !instance.work.is_some_and(|work| {
                work.id == *work_id
                    && work.kind == AgentInstanceWorkKind::Reconciling
                    && work.input_generation == *generation
            }) {
                return;
            }
            match result {
                Ok(tailscale_serve) => {
                    instance
                        .documents
                        .private
                        .status
                        .tailscale_serve
                        .clone_from(tailscale_serve);
                }
                Err(error) if *generation == instance.documents.private.spec.desired_generation => {
                    warning = Some(format!("{id}: failed to reconcile Tailscale serve: {error}"));
                }
                Err(_) => {}
            }
            clear_instance_work(instance);
        }
        if let Some(message) = warning {
            self.emit(
                AgentEventSource::Instance { id: *id },
                AgentEvent::Diagnostic {
                    level: EventLevel::Warn,
                    message,
                },
            );
        }
    }

    fn handle_runtime_secrets_completion(&mut self, completion: WorkCompletion) {
        let WorkCompletion::RuntimeSecretsReconciled {
            id,
            generation,
            work_id,
            outcome,
        } = completion
        else {
            return;
        };
        let mut diagnostic = None;
        {
            let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
                return;
            };
            if instance.runtime_secrets.active_work_id != Some(work_id) {
                return;
            }
            instance.runtime_secrets.active_work_id.take();
            if generation != instance.documents.private.spec.desired_generation {
                instance.runtime_secrets.next_at = Instant::now();
                return;
            }
            match outcome.result {
                Ok(output) => {
                    instance.documents.private.status.backend = outcome.backend;
                    instance.runtime_repair = RuntimeRepairState::Idle;
                    instance.secret_host_files = Some(output.secret_files);
                    instance.runtime_secrets.failure_count = 0;
                    instance.runtime_secrets.next_at = Instant::now() + HOST_INPUT_RECONCILE_INTERVAL;
                    instance.host_inputs.next_at = Instant::now();
                }
                Err(error) => {
                    instance.runtime_repair = RuntimeRepairState::Due;
                    instance.runtime_secrets.failure_count = instance.runtime_secrets.failure_count.saturating_add(1);
                    instance.runtime_secrets.next_at =
                        Instant::now() + host_input_reconcile_retry_delay(instance.runtime_secrets.failure_count);
                    instance
                        .documents
                        .private
                        .status
                        .host_inputs
                        .record_failure(generation, format!("runtime secret refresh failed: {error}"));
                    diagnostic = Some(format!(
                        "{id}: failed to refresh runtime secrets (attempt {}): {error}",
                        instance.runtime_secrets.failure_count
                    ));
                }
            }
        }
        if let Some(message) = diagnostic {
            self.emit(
                AgentEventSource::Instance { id },
                AgentEvent::Diagnostic {
                    level: EventLevel::Warn,
                    message,
                },
            );
        }
    }

    fn handle_host_inputs_completion(&mut self, completion: WorkCompletion) {
        let WorkCompletion::HostInputsReconciled {
            id,
            generation,
            work_id,
            control,
            result,
        } = completion
        else {
            return;
        };
        let mut diagnostic = None;
        {
            let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
                return;
            };
            if instance.host_inputs.active_work_id != Some(work_id) {
                return;
            }
            instance.host_inputs.active_work_id.take();
            instance.control = control;
            match result {
                Ok(output) => {
                    if generation != instance.documents.private.spec.desired_generation {
                        instance.host_inputs.next_at = Instant::now();
                        return;
                    }
                    if output.file_failures > 0 {
                        instance.host_inputs.failure_count = instance.host_inputs.failure_count.saturating_add(1);
                        instance.host_inputs.next_at =
                            Instant::now() + host_input_reconcile_retry_delay(instance.host_inputs.failure_count);
                        let error = output
                            .file_errors
                            .iter()
                            .map(|failure| format!("{}: {}", failure.path, failure.error))
                            .collect::<Vec<_>>()
                            .join("; ");
                        instance
                            .documents
                            .private
                            .status
                            .host_inputs
                            .record_failure(generation, format!("guest file writes failed: {error}"));
                        diagnostic = Some((
                            EventLevel::Warn,
                            format!(
                                "{id}: host input guest file writes failed: {}: {}",
                                output.file_failures, error
                            ),
                        ));
                    } else {
                        instance.host_inputs.failure_count = 0;
                        instance.host_inputs.next_at = Instant::now() + HOST_INPUT_RECONCILE_INTERVAL;
                        instance.documents.private.status.host_inputs.mark_ready(generation);
                        if output.files_updated > 0 {
                            diagnostic = Some((
                                EventLevel::Info,
                                format!("{id}: wrote host input file updates: {}", output.files_updated),
                            ));
                        }
                    }
                }
                Err(error) => {
                    if generation != instance.documents.private.spec.desired_generation {
                        instance.host_inputs.next_at = Instant::now();
                        return;
                    }
                    instance.host_inputs.failure_count = instance.host_inputs.failure_count.saturating_add(1);
                    instance.host_inputs.next_at =
                        Instant::now() + host_input_reconcile_retry_delay(instance.host_inputs.failure_count);
                    instance
                        .documents
                        .private
                        .status
                        .host_inputs
                        .record_failure(generation, format!("host input reconciliation failed: {error}"));
                    diagnostic = Some((
                        EventLevel::Warn,
                        format!(
                            "{id}: failed to reconcile host inputs (attempt {}): {error}",
                            instance.host_inputs.failure_count
                        ),
                    ));
                }
            }
        }
        if let Some((level, message)) = diagnostic {
            self.emit(
                AgentEventSource::Instance { id },
                AgentEvent::Diagnostic { level, message },
            );
        }
    }

    fn apply_bootstrap_event(&mut self, source: BootstrapSource, work_id: u64, generation: u64, event: BootstrapEvent) {
        match source {
            BootstrapSource::AgentBase => {
                let owns_event = matches!(
                    self.base,
                    AgentBaseState::Preparing { work_id: active, .. }
                        | AgentBaseState::Building { work_id: active, .. }
                        if active == work_id
                );
                if owns_event && generation == self.documents.private.generation() {
                    self.emit(AgentEventSource::AgentBase, AgentEvent::BootstrapEvent { event });
                }
            }
            BootstrapSource::Instance { id } => {
                let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
                    return;
                };
                if generation != instance.documents.private.spec.desired_generation
                    || !instance.work.is_some_and(|work| {
                        work.id == work_id
                            && work.kind == AgentInstanceWorkKind::Bootstrapping
                            && work.input_generation == generation
                    })
                {
                    return;
                }
                match &event {
                    BootstrapEvent::Diagnostic { .. } => {}
                    BootstrapEvent::StepStarted { step } => {
                        instance.documents.private.status.work.bootstrap = Some(AgentInstanceBootstrapWorkStatus {
                            phase: AgentInstanceBootstrapWorkPhase::Running,
                            current_step: Some(step.clone()),
                            last_error: None,
                            failure_count: None,
                            next_retry_unix_seconds: None,
                        });
                    }
                    BootstrapEvent::StepFinished { .. } => {
                        if let Some(bootstrap) = &mut instance.documents.private.status.work.bootstrap {
                            bootstrap.current_step = None;
                        }
                    }
                    BootstrapEvent::StepFailed { message, .. } => {
                        let failure_count = instance
                            .documents
                            .private
                            .status
                            .bootstrap
                            .as_ref()
                            .map_or(1, |state| state.failure_count.saturating_add(1));
                        instance.documents.private.status.work.bootstrap = Some(AgentInstanceBootstrapWorkStatus {
                            phase: AgentInstanceBootstrapWorkPhase::Failed,
                            current_step: None,
                            last_error: Some(message.clone()),
                            failure_count: Some(failure_count),
                            next_retry_unix_seconds: None,
                        });
                    }
                }
                self.emit(AgentEventSource::Instance { id }, AgentEvent::BootstrapEvent { event });
            }
        }
    }

    fn handle_exec_completion(&mut self, completion: &WorkCompletion) {
        let WorkCompletion::ExecFinished {
            id,
            work_id,
            ref command,
            ref output,
        } = *completion
        else {
            return;
        };
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        let Some(session) = instance.session.take_if(|session| session.work_id == work_id) else {
            return;
        };
        instance.documents.private.status.work.sessions.active = 0;
        let result = AgentInstanceExecResult {
            name: instance.documents.private.name(),
            command: command.clone(),
            exit_status: u64::try_from(output.status).unwrap_or(1),
            stdout: output.stdout.clone(),
            stderr: output.stderr.clone(),
        };
        session.respond.try_send(Ok(result));
        self.emit_instance_event(
            id,
            AgentInstanceEventSource::Session {
                kind: SessionKind::Exec,
            },
            AgentInstanceEvent::SessionFinished {
                session: SessionKind::Exec,
                result: SessionResultSummary {
                    exit_status: Some(u64::try_from(output.status).unwrap_or(1)),
                    result: if output.status == 0 {
                        OperationResult::Succeeded
                    } else {
                        OperationResult::Failed {
                            error: format!("command exited with status {}", output.status),
                        }
                    },
                },
            },
        );
    }

    fn reconcile(&mut self) {
        let now = Instant::now();
        if now >= self.next_reconcile {
            self.next_reconcile = now + AGENT_RECONCILE_INTERVAL;
        }
        self.project_instance_desired_states();
        self.reconcile_runtime_secrets();
        if self.documents.private.deletion_requested() {
            self.reconcile_deletion();
        } else if self.documents.private.phase() == AgentPhase::Paused || self.documents.private.replicas() == 0 {
            self.reconcile_inactive_instances();
        } else {
            self.reconcile_cleanup_instances();
            self.reconcile_base();
            if self.agent_base_ready_for_document() {
                self.reconcile_active_instances();
            }
        }
        self.reconcile_host_inputs();
    }

    fn agent_base_ready_for_document(&self) -> bool {
        matches!(
            (
                &self.base,
                self.documents.private.ready_agent_base_key(),
                self.documents.private.desired_agent_base_key(),
            ),
            (
                AgentBaseState::Ready {
                    key: ready_key,
                    ..
                },
                Some(document_key),
                Some(desired_key),
            ) if ready_key == document_key && document_key == desired_key
        )
    }

    fn project_instance_desired_states(&mut self) {
        let document = self.documents.private.clone();
        let manifest = document.manifest();
        let inactive = document.phase() == AgentPhase::Paused || document.replicas() == 0;
        let deleting = document.deletion_requested();
        let ids = self.instances.keys().copied().collect::<Vec<_>>();
        let mut warnings = Vec::new();
        for id in ids {
            let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
                continue;
            };
            let target = if deleting {
                AgentInstanceTarget::Deleting
            } else if inactive || id.as_u32() >= u32::from(document.replicas()) {
                AgentInstanceTarget::Inactive
            } else {
                AgentInstanceTarget::Active
            };
            if let Some(warning) = project_instance_desired_state(instance, &document, &manifest, id, target) {
                warnings.push((id, warning));
            }
            refresh_instance_observed_generation(instance);
        }
        for (id, message) in warnings {
            self.emit(
                AgentEventSource::Instance { id },
                AgentEvent::Diagnostic {
                    level: EventLevel::Warn,
                    message,
                },
            );
        }
    }

    fn reconcile_deletion(&mut self) {
        self.drop_retrying_starting_instances(|_| true);
        let ids = self.instances.keys().copied().collect::<Vec<_>>();
        for id in ids {
            if let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut)
                && !matches!(
                    instance.documents.private.status.phase,
                    AgentInstancePhase::Deleting | AgentInstancePhase::Deleted
                )
            {
                instance.documents.private.status.phase = AgentInstancePhase::Deleting;
                instance.documents.private.status.clear_readiness();
                instance.documents.private.status.reconciliation = None;
            }
            self.reconcile_instance(id);
        }
        if self.instances.is_empty() {
            let work_id = self.allocate_work_id();
            match &self.base {
                AgentBaseState::Ready { .. } => {
                    self.base = AgentBaseState::Stopping { work_id };
                    spawn_stop_base(
                        self.input.clone(),
                        self.backend.clone(),
                        self.documents.private.clone(),
                        self.layout.clone(),
                        work_id,
                    );
                }
                AgentBaseState::Missing | AgentBaseState::Stopped => {
                    self.documents.private.refresh_status_projection(true);
                    self.documents.private.mark_observed_generation_if_changed();
                }
                AgentBaseState::Preparing { .. }
                | AgentBaseState::Building { .. }
                | AgentBaseState::Failed { .. }
                | AgentBaseState::Stopping { .. } => {}
            }
        }
    }

    fn reconcile_inactive_instances(&mut self) {
        self.drop_retrying_starting_instances(|_| true);
        let ids = self.instances.keys().copied().collect::<Vec<_>>();
        for id in ids {
            self.reconcile_instance(id);
        }
    }

    fn reconcile_cleanup_instances(&mut self) {
        let ids = self
            .instances
            .iter()
            .filter_map(|(id, state)| {
                state.running_ref().and_then(|instance| {
                    matches!(
                        instance.documents.private.status.phase,
                        AgentInstancePhase::Deleting | AgentInstancePhase::Deleted
                    )
                    .then_some(*id)
                })
            })
            .collect::<Vec<_>>();
        for id in ids {
            self.reconcile_instance(id);
        }
    }

    fn reconcile_base(&mut self) {
        let document = self.documents.private.clone();
        let work_id = self.allocate_work_id();
        match self.base {
            AgentBaseState::Missing | AgentBaseState::Stopped => {
                let generation = document.generation();
                self.base = AgentBaseState::Preparing { work_id };
                spawn_prepare_base(self.input.clone(), self.context.clone(), document, work_id, generation);
            }
            AgentBaseState::Failed {
                generation: failed,
                ref key,
            } if failed != document.generation() || key.as_ref() != document.desired_agent_base_key() => {
                let generation = document.generation();
                self.base = AgentBaseState::Preparing { work_id };
                spawn_prepare_base(self.input.clone(), self.context.clone(), document, work_id, generation);
            }
            AgentBaseState::Ready { .. }
            | AgentBaseState::Preparing { .. }
            | AgentBaseState::Building { .. }
            | AgentBaseState::Failed { .. }
            | AgentBaseState::Stopping { .. } => {}
        }
    }

    fn reconcile_active_instances(&mut self) {
        let document = self.documents.private.clone();
        let Some(agent_base) = document.ready_agent_base_key().cloned() else {
            return;
        };
        let now = Instant::now();
        for slot in 0..document.replicas() {
            let id = AgentInstanceId::new(u32::from(slot));
            let work_id = self.allocate_work_id();
            match self.instances.get_mut(&id) {
                Some(AgentInstanceState::Running(_)) => continue,
                Some(AgentInstanceState::Starting(starting)) => {
                    if starting.generation != document.generation() {
                        if starting.retry_at.is_none() {
                            continue;
                        }
                        starting.generation = document.generation();
                        starting.work_id = work_id;
                        starting.retry_at = None;
                        starting.failure_count = 0;
                    } else if starting.retry_at.is_none_or(|retry| now < retry) {
                        continue;
                    }
                    starting.retry_at = None;
                    starting.work_id = work_id;
                }
                None => {
                    let pending = StartingAgentInstanceState::new(
                        &self.context,
                        &self.layout,
                        document.agent(),
                        id,
                        work_id,
                        document.generation(),
                    );
                    self.instances.insert(id, AgentInstanceState::Starting(pending));
                }
            }
            spawn_create_instance(
                self.input.clone(),
                self.backend.clone(),
                document.clone(),
                self.layout.clone(),
                id,
                agent_base.clone(),
                TaskAttempt {
                    work_id,
                    generation: document.generation(),
                },
            );
        }
        self.drop_retrying_starting_instances(|id| id.as_u32() >= u32::from(document.replicas()));
        let ids = self.instances.keys().copied().collect::<Vec<_>>();
        for id in ids {
            self.reconcile_instance(id);
        }
    }

    fn drop_retrying_starting_instances(&mut self, mut should_drop: impl FnMut(AgentInstanceId) -> bool) {
        // A retrying Starting slot is an admission placeholder only. It owns no VM yet, so
        // desired-state changes can drop it instead of waiting for the retry deadline.
        self.instances.retain(|id, state| {
            !(should_drop(*id)
                && matches!(state, AgentInstanceState::Starting(starting) if starting.retry_at.is_some()))
        });
    }

    fn reconcile_instance(&mut self, id: AgentInstanceId) {
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        if instance.blocks_instance_transition() {
            return;
        }
        if matches!(
            instance.documents.private.status.phase,
            AgentInstancePhase::Deleting | AgentInstancePhase::Deleted
        ) {
            self.reconcile_deleting_instance(id);
            return;
        }
        match instance.documents.private.spec.target {
            AgentInstanceTarget::Active => self.reconcile_active_instance(id),
            AgentInstanceTarget::Inactive => self.reconcile_inactive_instance(id),
            AgentInstanceTarget::Deleting => self.reconcile_deleting_instance(id),
        }
    }

    fn reconcile_active_instance(&mut self, id: AgentInstanceId) {
        let work_id = self.allocate_work_id();
        let document = self.documents.private.clone();
        let services = self.work_services();
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        let generation = instance.documents.private.spec.desired_generation;
        if instance.runtime_repair == RuntimeRepairState::Due
            && instance.documents.private.status.phase == AgentInstancePhase::Running
        {
            instance.runtime_repair = RuntimeRepairState::Backoff;
            admit_instance_work(instance, work_id, generation, AgentInstanceWorkKind::Repairing);
            spawn_reconcile_instance(
                self.input.clone(),
                self.backend.clone(),
                Rc::clone(&instance.network_runtime),
                document,
                instance.documents.private.clone(),
                id,
                TaskAttempt { work_id, generation },
            );
            return;
        }
        if instance.runtime_repair == RuntimeRepairState::Backoff
            && instance.documents.private.status.phase == AgentInstancePhase::Running
        {
            return;
        }
        if instance.documents.private.status.phase == AgentInstancePhase::Running
            && instance.documents.private.status.reconciliation.is_none()
        {
            admit_instance_work(instance, work_id, generation, AgentInstanceWorkKind::Reconciling);
            spawn_reconcile_instance(
                self.input.clone(),
                self.backend.clone(),
                Rc::clone(&instance.network_runtime),
                document,
                instance.documents.private.clone(),
                id,
                TaskAttempt { work_id, generation },
            );
        } else if instance.documents.private.status.phase != AgentInstancePhase::Running {
            instance.control.take();
            admit_instance_work(instance, work_id, generation, AgentInstanceWorkKind::Starting);
            spawn_start_instance(
                self.input.clone(),
                self.backend.clone(),
                Rc::clone(&instance.network_runtime),
                document,
                instance.documents.private.clone(),
                id,
                TaskAttempt { work_id, generation },
            );
        } else if instance
            .documents
            .private
            .status
            .readiness
            .as_ref()
            .is_none_or(|state| !state.ready)
        {
            self.reconcile_instance_bootstrap(id);
        } else if instance.documents.private.status.host_inputs.is_ready_for(generation)
            && instance.documents.private.status.observed_generation != generation
        {
            admit_instance_work(instance, work_id, generation, AgentInstanceWorkKind::Reconciling);
            spawn_reconcile_tailscale_serve(
                services,
                self.context.clone(),
                self.layout.clone(),
                document,
                instance.documents.private.clone(),
                id,
                TaskAttempt { work_id, generation },
            );
        }
    }

    fn reconcile_runtime_secrets(&mut self) {
        let agent_document = self.documents.private.clone();
        let ids = self.instances.keys().copied().collect::<Vec<_>>();
        for id in ids {
            let work_id = self.allocate_work_id();
            let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
                continue;
            };
            if !should_reconcile_runtime_secrets(instance) || Instant::now() < instance.runtime_secrets.next_at {
                continue;
            }
            spawn_reconcile_runtime_secrets(ReconcileRuntimeSecretsTask {
                input: self.input.clone(),
                backend: self.backend.clone(),
                network: Rc::clone(&instance.network_runtime),
                agent_document: agent_document.clone(),
                document: instance.documents.private.clone(),
                id,
                generation: instance.documents.private.spec.desired_generation,
                work_id,
            });
            instance.runtime_secrets.active_work_id = Some(work_id);
        }
    }

    fn reconcile_instance_host_inputs(&mut self, id: AgentInstanceId) {
        let work_id = self.allocate_work_id();
        let document = self.documents.private.clone();
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        if !should_reconcile_host_inputs(instance) || Instant::now() < instance.host_inputs.next_at {
            return;
        }
        let Some(secret_files) = instance.secret_host_files.clone() else {
            return;
        };
        let control = instance.control.take();
        spawn_reconcile_host_inputs(ReconcileHostInputsTask {
            input: self.input.clone(),
            backend: self.backend.clone(),
            network: Rc::clone(&instance.network_runtime),
            control,
            agent_document: document,
            document: instance.documents.private.clone(),
            secret_files,
            id,
            generation: instance.documents.private.spec.desired_generation,
            work_id,
        });
        instance.host_inputs.active_work_id = Some(work_id);
    }

    fn reconcile_host_inputs(&mut self) {
        let ids = self.instances.keys().copied().collect::<Vec<_>>();
        for id in ids {
            self.reconcile_instance_host_inputs(id);
        }
    }

    fn reconcile_instance_bootstrap(&mut self, id: AgentInstanceId) {
        let work_id = self.allocate_work_id();
        let services = self.work_services();
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        if let Some(retry_at) = instance.bootstrap_retry {
            if Instant::now() < retry_at {
                return;
            }
            instance.bootstrap_retry = None;
        }
        let ssh_port_ready = instance
            .documents
            .private
            .status
            .network
            .ports
            .get("ssh")
            .is_some_and(|mapping| mapping.host.is_some());
        if instance.documents.private.status.guest_access.is_some() && !ssh_port_ready {
            return;
        }
        let generation = instance.documents.private.spec.desired_generation;
        admit_instance_work(instance, work_id, generation, AgentInstanceWorkKind::Bootstrapping);
        let control = instance.control.take();
        spawn_bootstrap_instance(
            services,
            self.layout.clone(),
            self.documents.private.clone(),
            control,
            instance.documents.private.clone(),
            id,
            TaskAttempt { work_id, generation },
        );
    }

    fn reconcile_inactive_instance(&mut self, id: AgentInstanceId) {
        let work_id = self.allocate_work_id();
        let document = self.documents.private.clone();
        let services = self.work_services();
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        let generation = instance.documents.private.spec.desired_generation;
        if instance.documents.private.status.phase == AgentInstancePhase::Stopped {
            refresh_instance_observed_generation(instance);
        } else {
            admit_instance_work(instance, work_id, generation, AgentInstanceWorkKind::Stopping);
            let control = instance.control.take();
            spawn_stop_instance(
                services,
                Rc::clone(&instance.network_runtime),
                control,
                document.agent().clone(),
                instance.documents.private.clone(),
                id,
                TaskAttempt { work_id, generation },
            );
        }
    }

    fn reconcile_deleting_instance(&mut self, id: AgentInstanceId) {
        let work_id = self.allocate_work_id();
        let services = self.work_services();
        let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
            return;
        };
        let cleanup_phase = instance.documents.private.status.phase;
        if !cleanup_phase_persisted(instance, cleanup_phase)
            || instance.cleanup_retry.is_some_and(|retry| Instant::now() < retry)
        {
            return;
        }
        instance.cleanup_retry = None;
        let generation = instance.documents.private.spec.desired_generation;
        if cleanup_phase == AgentInstancePhase::Deleting {
            admit_instance_work(instance, work_id, generation, AgentInstanceWorkKind::Deleting);
            let control = instance.control.take();
            spawn_stop_instance_for_delete(
                services,
                Rc::clone(&instance.network_runtime),
                control,
                instance.documents.private.clone(),
                id,
                work_id,
            );
        } else {
            // `Deleted` is the durable barrier for removing the instance directory.
            // File removal is in-memory work only: persisting it would recreate the
            // directory while the removal task owns it.
            instance.work = Some(AgentInstanceWork {
                id: work_id,
                kind: AgentInstanceWorkKind::Deleting,
                input_generation: generation,
            });
            spawn_remove_instance_files(self.input.clone(), self.layout.clone(), self.agent.clone(), id, work_id);
        }
    }

    async fn commit_state(&mut self) -> bool {
        // This is the only state publication boundary for the running agent loop.
        // Everything above this point mutates private documents and child state; here we
        // derive public documents, persist idempotently, publish stream updates, and only
        // then answer document/status/list/open-stream requests with the committed view.
        let document = self.documents.private.clone();
        self.documents.write(document, &self.base, &mut self.instances);
        let document_changed = self.documents.dirty();
        if self.documents.private.status.deleted {
            let agent_document = self.layout.agent_document(self.documents.private.agent());
            let removed = async {
                let Some(agent_dir) = agent_document.parent() else {
                    return Err(format!(
                        "agent document path has no parent: {}",
                        agent_document.display()
                    ));
                };
                match tokio::fs::remove_dir_all(agent_dir).await {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(format!("remove {}: {error}", agent_dir.display())),
                }
            }
            .await;
            if let Err(error) = removed {
                self.context.logger().warn(format!(
                    "failed to remove deleted agent state for {}: {error}",
                    self.agent
                ));
            }
            self.documents.persisted = Some(self.documents.private.clone());
        } else if let Err(error) = self.documents.persist_to(&self.layout, &mut self.instances).await {
            self.emit(
                AgentEventSource::Controller,
                AgentEvent::Diagnostic {
                    level: EventLevel::Error,
                    message: format!("failed to persist agent state: {error}"),
                },
            );
            self.answer_pending_responses(Some(&error));
            return false;
        }
        if document_changed {
            self.publish(&AgentStreamItem::Document(Box::new(self.documents.public.clone())));
            self.emit(
                AgentEventSource::Controller,
                AgentEvent::DocumentChanged {
                    document: Box::new(self.documents.public.clone()),
                },
            );
        }
        self.answer_pending_responses(None);
        false
    }

    fn answer_pending_responses(&mut self, persist_error: Option<&str>) {
        for response in std::mem::take(&mut self.pending_responses) {
            match response {
                PendingResponse::AgentDocument(respond) => {
                    respond.try_send(committed_result(persist_error, || Ok(self.documents.public.clone())));
                }
                PendingResponse::InstanceDocument { id, respond } => {
                    respond.try_send(committed_result(persist_error, || self.instance_document(id)));
                }
                PendingResponse::ListItems(respond) => {
                    respond.try_send(committed_result(persist_error, || Ok(self.list_items())));
                }
                PendingResponse::OpenStream {
                    replay_from_generation,
                    items,
                    respond,
                } => {
                    if persist_error.is_none() {
                        self.open_stream(replay_from_generation, items);
                    }
                    if let Some(respond) = respond {
                        respond.try_send(committed_result(persist_error, || Ok(())));
                    }
                }
            }
        }
    }

    fn refresh_runtime_observations(&mut self) {
        let ids = self.instances.keys().copied().collect::<Vec<_>>();
        for id in ids {
            let mut events = Vec::new();
            if let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) {
                instance.network_events.drain(|event| events.push(event));
            }
            for event in events {
                if let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) {
                    apply_network_event(&mut instance.documents.private, &event);
                }
                self.emit_instance_event(
                    id,
                    AgentInstanceEventSource::Network,
                    AgentInstanceEvent::NetworkEvent(event),
                );
            }
            let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut) else {
                continue;
            };
            if let Some(observation) = instance.network_runtime.observation() {
                let status = observation.status();
                apply_bound_host_ports(&mut instance.documents.private.status.network, &status.host_ports);
                instance.documents.private.status.network.runtime = Some(status);
            }
        }
    }

    fn emit(&mut self, source: AgentEventSource, event: AgentEvent) {
        if matches!(event, AgentEvent::DocumentChanged { .. }) {
            return;
        }
        let envelope = AgentEventEnvelope {
            sequence: self.next_event_sequence(),
            timestamp: time::rfc3339_utc_now(),
            generation: self.documents.private.generation(),
            source,
            event,
        };
        if let Err(error) = self.events.append(envelope.clone()) {
            self.context
                .logger()
                .warn(format!("failed to append agent event for {}: {error}", self.agent));
        }
        self.recent_events.push_back(envelope.clone());
        while self.recent_events.len() > RECENT_EVENT_CAPACITY {
            self.recent_events.pop_front();
        }
        self.publish(&AgentStreamItem::Event(envelope));
    }

    fn emit_instance_event(
        &mut self,
        id: AgentInstanceId,
        source: AgentInstanceEventSource,
        event: AgentInstanceEvent,
    ) {
        let Some(sequence) = self.next_instance_event_sequence(id) else {
            return;
        };
        let envelope = AgentInstanceEventEnvelope {
            sequence,
            timestamp: time::rfc3339_utc_now(),
            generation: self.documents.private.generation(),
            work_epoch: None,
            source,
            event,
        };
        if let Some(instance) = self.instances.get_mut(&id).and_then(AgentInstanceState::running_mut)
            && let Err(error) = instance.events.append(envelope.clone())
        {
            self.context.logger().warn(format!(
                "failed to append agent instance event for {}/{}: {error}",
                self.agent, id
            ));
        }
        if !should_forward_instance_event(&envelope.event) {
            return;
        }
        self.emit(
            AgentEventSource::Instance { id },
            AgentEvent::InstanceEvent {
                event: Box::new(envelope),
            },
        );
    }

    const fn next_event_sequence(&mut self) -> u64 {
        let sequence = self.event_sequence;
        self.event_sequence = self.event_sequence.saturating_add(1);
        sequence
    }

    fn next_instance_event_sequence(&mut self, id: AgentInstanceId) -> Option<u64> {
        self.instances
            .get_mut(&id)
            .and_then(AgentInstanceState::running_mut)
            .map(|instance| {
                let sequence = instance.event_sequence;
                instance.event_sequence = instance.event_sequence.saturating_add(1);
                sequence
            })
    }

    fn publish(&mut self, item: &AgentStreamItem) {
        self.streams.retain_mut(|stream| stream.try_send(item.clone()).is_ok());
    }
}

async fn read_log_tail(path: &Path, lines: usize) -> Result<String, Error> {
    let mut file = tokio::fs::File::open(path).await.map_err(|source| Error::ReadLog {
        path: path.to_path_buf(),
        source,
    })?;
    let mut offset = file
        .metadata()
        .await
        .map_err(|source| Error::ReadLog {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    let mut chunks = Vec::new();
    let mut newline_count = 0usize;
    while offset > 0 && newline_count <= lines {
        let chunk_len = offset.min(LOG_TAIL_CHUNK_BYTES);
        offset -= chunk_len;
        let mut chunk = vec![0; chunk_len as usize];
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|source| Error::ReadLog {
                path: path.to_path_buf(),
                source,
            })?;
        file.read_exact(&mut chunk).await.map_err(|source| Error::ReadLog {
            path: path.to_path_buf(),
            source,
        })?;
        newline_count += count_byte(&chunk, b'\n');
        chunks.push(chunk);
    }
    let total_len = chunks.iter().map(Vec::len).sum();
    let mut bytes = Vec::with_capacity(total_len);
    for chunk in chunks.iter().rev() {
        bytes.extend_from_slice(chunk);
    }
    let start = tail_start(&bytes, lines);
    String::from_utf8(bytes[start..].to_vec()).map_err(|source| Error::ReadLog {
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })
}

async fn read_network_event_log_tail(
    path: &Path,
    lines: usize,
    errors: bool,
    kind: Option<NetworkLogKind>,
) -> Result<String, Error> {
    let mut file = tokio::fs::File::open(path).await.map_err(|source| Error::ReadLog {
        path: path.to_path_buf(),
        source,
    })?;
    let mut offset = file
        .metadata()
        .await
        .map_err(|source| Error::ReadLog {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    let mut leading_fragment = Vec::new();
    let mut matched_lines = Vec::new();
    while offset > 0 && matched_lines.len() < lines {
        let chunk_len = offset.min(LOG_TAIL_CHUNK_BYTES);
        offset -= chunk_len;
        let mut chunk = vec![0; chunk_len as usize];
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|source| Error::ReadLog {
                path: path.to_path_buf(),
                source,
            })?;
        file.read_exact(&mut chunk).await.map_err(|source| Error::ReadLog {
            path: path.to_path_buf(),
            source,
        })?;
        chunk.extend_from_slice(&leading_fragment);

        let complete_lines = if offset > 0 {
            let Some(first_newline) = chunk.iter().position(|byte| *byte == b'\n') else {
                leading_fragment = chunk;
                continue;
            };
            leading_fragment.clear();
            leading_fragment.extend_from_slice(&chunk[..first_newline]);
            &chunk[first_newline + 1..]
        } else {
            leading_fragment.clear();
            &chunk[..]
        };

        for line in complete_lines.rsplit(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            if network_event_line_matches(path, line, errors, kind)? {
                matched_lines.push(line.to_vec());
                if matched_lines.len() == lines {
                    break;
                }
            }
        }
    }

    matched_lines.reverse();
    let mut bytes = Vec::with_capacity(matched_lines.iter().map(Vec::len).sum::<usize>() + matched_lines.len());
    for line in matched_lines {
        bytes.extend_from_slice(&line);
        bytes.push(b'\n');
    }
    String::from_utf8(bytes).map_err(|source| Error::ReadLog {
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })
}

fn network_event_line_matches(
    path: &Path,
    line: &[u8],
    errors: bool,
    kind: Option<NetworkLogKind>,
) -> Result<bool, Error> {
    let envelope = serde_json::from_slice::<AgentInstanceEventEnvelope>(line).map_err(|source| Error::ReadLog {
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })?;
    let AgentInstanceEvent::NetworkEvent(event) = &envelope.event else {
        return Ok(false);
    };
    if errors && !network_event_is_error(&event.event) {
        return Ok(false);
    }
    if let Some(kind) = kind
        && network_log_kind(&event.event) != kind
    {
        return Ok(false);
    }
    Ok(true)
}

const fn network_log_kind(event: &AgentInstanceNetworkEventKind) -> NetworkLogKind {
    match event {
        AgentInstanceNetworkEventKind::LifecycleStateChanged { .. } => NetworkLogKind::Lifecycle,
        AgentInstanceNetworkEventKind::TelemetrySnapshot { .. } => NetworkLogKind::Telemetry,
        AgentInstanceNetworkEventKind::TransportConnectFailed { .. }
        | AgentInstanceNetworkEventKind::TransportGuestConnected { .. }
        | AgentInstanceNetworkEventKind::TransportGuestDisconnected { .. }
        | AgentInstanceNetworkEventKind::TransportRegisterFailed { .. } => NetworkLogKind::Transport,
        AgentInstanceNetworkEventKind::EgressError { .. } | AgentInstanceNetworkEventKind::EgressProxyClosed { .. } => {
            NetworkLogKind::Egress
        }
        AgentInstanceNetworkEventKind::DnsResolved { .. } => NetworkLogKind::Dns,
        AgentInstanceNetworkEventKind::HostPortBound { .. } | AgentInstanceNetworkEventKind::HostPortError { .. } => {
            NetworkLogKind::HostPort
        }
        AgentInstanceNetworkEventKind::ReactorError { .. } => NetworkLogKind::Reactor,
    }
}

fn network_event_is_error(event: &AgentInstanceNetworkEventKind) -> bool {
    match event {
        AgentInstanceNetworkEventKind::LifecycleStateChanged { state } => state == "backoff" || state == "failed",
        AgentInstanceNetworkEventKind::TransportConnectFailed { .. }
        | AgentInstanceNetworkEventKind::TransportGuestDisconnected { .. }
        | AgentInstanceNetworkEventKind::TransportRegisterFailed { .. }
        | AgentInstanceNetworkEventKind::EgressError { .. }
        | AgentInstanceNetworkEventKind::HostPortError { .. }
        | AgentInstanceNetworkEventKind::ReactorError { .. } => true,
        AgentInstanceNetworkEventKind::TelemetrySnapshot { .. }
        | AgentInstanceNetworkEventKind::TransportGuestConnected { .. }
        | AgentInstanceNetworkEventKind::EgressProxyClosed { .. }
        | AgentInstanceNetworkEventKind::DnsResolved { .. }
        | AgentInstanceNetworkEventKind::HostPortBound { .. } => false,
    }
}

fn tail_start(bytes: &[u8], lines: usize) -> usize {
    let mut seen = 0usize;
    let mut index = bytes.len();
    while index > 0 {
        index -= 1;
        if bytes[index] == b'\n' {
            if index + 1 == bytes.len() {
                continue;
            }
            seen += 1;
            if seen == lines {
                return index + 1;
            }
        }
    }
    0
}

fn count_byte(bytes: &[u8], needle: u8) -> usize {
    bytes.iter().fold(0, |count, byte| count + usize::from(*byte == needle))
}

fn spawn_prepare_base(
    input: inbox::Sender<AgentInput>,
    context: Context,
    document: AgentDocument,
    work_id: u64,
    generation: u64,
) {
    tokio::task::spawn_local(async move {
        queue_bootstrap_event(
            &input,
            BootstrapSource::AgentBase,
            work_id,
            generation,
            BootstrapEvent::Diagnostic {
                level: EventLevel::Info,
                message: "preparing agent base files".to_owned(),
            },
        );
        let completion = match AgentBasePreparation::from_document(&context, document).await {
            Ok(preparation) => WorkCompletion::BasePrepared {
                work_id,
                generation,
                preparation: Box::new(preparation),
            },
            Err(error) => WorkCompletion::BaseFailed {
                work_id,
                generation,
                error: error.to_string(),
            },
        };
        queue_completion(&input, completion).await;
    });
}

fn spawn_build_base(
    input: inbox::Sender<AgentInput>,
    context: Context,
    backend: backend::BackendRef,
    layout: AgentdpLayout,
    document: AgentDocument,
    attempt: TaskAttempt,
    preparation: AgentBasePreparation,
) {
    let TaskAttempt { work_id, generation } = attempt;
    tokio::task::spawn_local(async move {
        let key = preparation.key().clone();
        queue_bootstrap_event(
            &input,
            BootstrapSource::AgentBase,
            work_id,
            generation,
            BootstrapEvent::Diagnostic {
                level: EventLevel::Info,
                message: "creating agent base disk".to_owned(),
            },
        );
        let mut events = BaseBootstrapEvents {
            input: input.clone(),
            work_id,
            generation,
        };
        let result = ensure_agent_base_ready(
            &context,
            &layout,
            document.agent(),
            &preparation,
            backend.as_ref(),
            &mut events,
        )
        .await;
        queue_completion(
            &input,
            match result {
                Ok(()) => WorkCompletion::BaseBuilt {
                    work_id,
                    generation,
                    key,
                },
                Err(error) => WorkCompletion::BaseFailed {
                    work_id,
                    generation,
                    error: error.to_string(),
                },
            },
        )
        .await;
    });
}

fn spawn_stop_base(
    input: inbox::Sender<AgentInput>,
    backend: backend::BackendRef,
    document: AgentDocument,
    layout: AgentdpLayout,
    work_id: u64,
) {
    tokio::task::spawn_local(async move {
        let context = Context::quiet();
        let keys = [document.desired_agent_base_key(), document.ready_agent_base_key()]
            .into_iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        if keys.is_empty() {
            queue_completion(&input, WorkCompletion::BaseStopped { work_id }).await;
            return;
        }
        let mut result: Result<(), backend::Error> = Ok(());
        for key in keys {
            let files = layout.agent_base(document.agent(), &key).files();
            if let Err(error) = backend
                .stop_base_runtime(&context, document.agent(), &key, &files)
                .await
            {
                result = Err(error);
                break;
            }
        }
        let completion = if result.is_ok() {
            WorkCompletion::BaseStopped { work_id }
        } else {
            WorkCompletion::BaseStopTimedOut { work_id }
        };
        queue_completion(&input, completion).await;
    });
}

fn spawn_create_instance(
    input: inbox::Sender<AgentInput>,
    backend: backend::BackendRef,
    agent_document: AgentDocument,
    layout: AgentdpLayout,
    id: AgentInstanceId,
    agent_base: AgentBaseKey,
    attempt: TaskAttempt,
) {
    let TaskAttempt { work_id, generation } = attempt;
    tokio::task::spawn_local(async move {
        let context = Context::quiet();
        let result = async {
            let manifest = manifest_context(&agent_document).map_err(|error| error.to_string())?;
            let instance = InstanceName::new(format!("replica-{id}"));
            let provisioning = ProvisioningPlan::from_manifest(
                manifest.value(),
                &ProvisioningOptions {
                    hostname: Some(instance.to_string()),
                },
            );
            let rendered = provisioning
                .render_instance_bootstrap(manifest.value())
                .map_err(|error| error.to_string())?;
            let files = layout.instance(agent_document.agent(), id).files();
            let base_files = layout.agent_base(agent_document.agent(), &agent_base).files();
            let created = backend
                .create_instance(
                    &context,
                    backend::CreateInstanceInput {
                        manifest,
                        instance: instance.to_string(),
                        provisioning_plan: &provisioning,
                        rendered_bootstrap: &rendered,
                        image_cache_dir: &layout.image_cache_dir(),
                        agent_base: &base_files,
                        files: &files,
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
            let ports = assign_port_mappings(&agent_document.manifest(), id).map_err(|error| error.to_string())?;
            let network = NetworkState::new(
                &created.state,
                NetworkAllowState::from(&agent_document.manifest().spec.network.allow),
                NetworkIpv6State::from_manifest(agent_document.manifest().spec.network.ipv6).await,
                ports,
            );
            let document = AgentInstanceDocument::new(
                agent_document.agent().clone(),
                id,
                instance,
                generation,
                agent_base,
                agent_document.template().clone(),
                AgentInstanceTarget::Active,
                AgentInstancePhase::Materialized,
                network,
                rendered.healthchecks.clone(),
                created.guest_access,
                created.state,
            );
            Ok::<_, String>(document)
        }
        .await;
        queue_completion(
            &input,
            WorkCompletion::InstanceCreated {
                id,
                work_id,
                generation,
                document: Box::new(result),
            },
        )
        .await;
    });
}

fn spawn_start_instance(
    input: inbox::Sender<AgentInput>,
    backend: backend::BackendRef,
    network: Rc<InstanceNetwork>,
    agent_document: AgentDocument,
    mut document: AgentInstanceDocument,
    id: AgentInstanceId,
    attempt: TaskAttempt,
) {
    let TaskAttempt { work_id, generation } = attempt;
    tokio::task::spawn_local(async move {
        let context = Context::quiet();
        let result = async {
            let manifest = manifest_context(&agent_document).map_err(|error| error.to_string())?;
            let started = backend
                .start_instance(&context, &network, &manifest, &mut document)
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(started)
        }
        .await;
        queue_completion(
            &input,
            WorkCompletion::InstanceStarted {
                id,
                work_id,
                generation,
                outcome: InstanceBackendOutcome {
                    backend: document.status.backend,
                    result,
                },
            },
        )
        .await;
    });
}

fn spawn_reconcile_instance(
    input: inbox::Sender<AgentInput>,
    backend: backend::BackendRef,
    network: Rc<InstanceNetwork>,
    agent_document: AgentDocument,
    mut document: AgentInstanceDocument,
    id: AgentInstanceId,
    attempt: TaskAttempt,
) {
    let TaskAttempt { work_id, generation } = attempt;
    tokio::task::spawn_local(async move {
        let context = Context::quiet();
        let result = async {
            let manifest = manifest_context(&agent_document).map_err(|error| error.to_string())?;
            let output = backend
                .reconcile_instance(&context, &network, &manifest, &mut document)
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(output)
        }
        .await;
        queue_completion(
            &input,
            WorkCompletion::InstanceReconciled {
                id,
                work_id,
                generation,
                outcome: InstanceBackendOutcome {
                    backend: document.status.backend,
                    result,
                },
            },
        )
        .await;
    });
}

struct ReconcileRuntimeSecretsTask {
    input: inbox::Sender<AgentInput>,
    backend: backend::BackendRef,
    network: Rc<InstanceNetwork>,
    agent_document: AgentDocument,
    document: AgentInstanceDocument,
    id: AgentInstanceId,
    generation: u64,
    work_id: u64,
}

fn spawn_reconcile_runtime_secrets(task: ReconcileRuntimeSecretsTask) {
    let _task = tokio::task::spawn_local(async move {
        let ReconcileRuntimeSecretsTask {
            input,
            backend,
            network,
            agent_document,
            mut document,
            id,
            generation,
            work_id,
        } = task;
        let context = Context::quiet();
        let result = match manifest_context(&agent_document) {
            Ok(manifest) => backend
                .reconcile_runtime_secrets(&context, &network, &manifest, &mut document)
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        queue_completion(
            &input,
            WorkCompletion::RuntimeSecretsReconciled {
                id,
                generation,
                work_id,
                outcome: InstanceBackendOutcome {
                    backend: document.status.backend,
                    result,
                },
            },
        )
        .await;
    });
}

struct ReconcileHostInputsTask {
    input: inbox::Sender<AgentInput>,
    backend: backend::BackendRef,
    network: Rc<InstanceNetwork>,
    control: Option<backend::InstanceControl>,
    agent_document: AgentDocument,
    document: AgentInstanceDocument,
    secret_files: Vec<SeedFile>,
    id: AgentInstanceId,
    generation: u64,
    work_id: u64,
}

fn spawn_reconcile_host_inputs(task: ReconcileHostInputsTask) {
    let _task = tokio::task::spawn_local(async move {
        let ReconcileHostInputsTask {
            input,
            backend,
            network,
            mut control,
            agent_document,
            document,
            secret_files,
            id,
            generation,
            work_id,
        } = task;
        let context = Context::quiet();
        let result = match manifest_context(&agent_document) {
            Ok(manifest) => backend
                .reconcile_host_inputs(&context, &network, &manifest, &document, &secret_files, &mut control)
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        queue_completion(
            &input,
            WorkCompletion::HostInputsReconciled {
                id,
                generation,
                work_id,
                control,
                result,
            },
        )
        .await;
    });
}

fn spawn_stop_instance(
    services: AgentWorkServices,
    network: Rc<InstanceNetwork>,
    mut control: Option<backend::InstanceControl>,
    agent: AgentName,
    mut document: AgentInstanceDocument,
    id: AgentInstanceId,
    attempt: TaskAttempt,
) {
    let TaskAttempt { work_id, generation } = attempt;
    tokio::task::spawn_local(async move {
        let AgentWorkServices {
            input,
            backend,
            tailscale,
        } = services;
        let context = Context::quiet();
        let name = document.name();
        let instance = document.metadata.name.clone();
        let status = document.status.phase;
        let result = async {
            document.status.tailscale_serve = tailscale
                .reconcile(
                    &context,
                    TailscaleServeDesired::Absent {
                        observed: document.status.tailscale_serve.as_ref(),
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
            let stopped = backend
                .stop_instance(
                    &context,
                    &network,
                    backend::StopInstanceInput {
                        name: &name,
                        agent: &agent,
                        instance: &instance,
                        status,
                    },
                    &mut document.status.backend,
                    &mut control,
                )
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(stopped)
        }
        .await;
        queue_completion(
            &input,
            WorkCompletion::InstanceStopped {
                id,
                work_id,
                generation,
                control,
                outcome: InstanceStopOutcome {
                    backend: document.status.backend,
                    tailscale_serve: document.status.tailscale_serve,
                    result,
                },
            },
        )
        .await;
    });
}

fn spawn_stop_instance_for_delete(
    services: AgentWorkServices,
    network: Rc<InstanceNetwork>,
    mut control: Option<backend::InstanceControl>,
    mut document: AgentInstanceDocument,
    id: AgentInstanceId,
    work_id: u64,
) {
    let generation = document.spec.desired_generation;
    tokio::task::spawn_local(async move {
        let AgentWorkServices {
            input,
            backend,
            tailscale,
        } = services;
        let name = document.name();
        let agent = document.metadata.agent.clone();
        let instance = document.metadata.name.clone();
        let status = document.status.phase;
        let context = Context::quiet();
        let result = async {
            document.status.tailscale_serve = tailscale
                .reconcile(
                    &context,
                    TailscaleServeDesired::Absent {
                        observed: document.status.tailscale_serve.as_ref(),
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
            backend
                .stop_instance(
                    &context,
                    &network,
                    backend::StopInstanceInput {
                        name: &name,
                        agent: &agent,
                        instance: &instance,
                        status,
                    },
                    &mut document.status.backend,
                    &mut control,
                )
                .await
                .map_err(|error| error.to_string())
        }
        .await;
        queue_completion(
            &input,
            WorkCompletion::InstanceDeleteRuntimeStopped {
                id,
                work_id,
                generation,
                control,
                outcome: InstanceStopOutcome {
                    backend: document.status.backend,
                    tailscale_serve: document.status.tailscale_serve,
                    result,
                },
            },
        )
        .await;
    });
}

fn spawn_remove_instance_files(
    input: inbox::Sender<AgentInput>,
    layout: AgentdpLayout,
    agent: AgentName,
    id: AgentInstanceId,
    work_id: u64,
) {
    tokio::task::spawn_local(async move {
        let files = layout.instance(&agent, id).files();
        let completion = match tokio::fs::remove_dir_all(&files.instance_dir).await {
            Ok(()) => WorkCompletion::InstanceDeleted { id, work_id },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                WorkCompletion::InstanceDeleted { id, work_id }
            }
            Err(error) => WorkCompletion::InstanceDeleteTimedOut {
                id,
                work_id,
                error: format!("remove {}: {error}", files.instance_dir.display()),
            },
        };
        queue_completion(&input, completion).await;
    });
}

fn spawn_bootstrap_instance(
    services: AgentWorkServices,
    layout: AgentdpLayout,
    agent_document: AgentDocument,
    mut control: Option<backend::InstanceControl>,
    mut document: AgentInstanceDocument,
    id: AgentInstanceId,
    attempt: TaskAttempt,
) {
    let TaskAttempt { work_id, generation } = attempt;
    tokio::task::spawn_local(async move {
        let AgentWorkServices {
            input,
            backend,
            tailscale,
        } = services;
        let context = Context::quiet();
        queue_completion(
            &input,
            WorkCompletion::BootstrapStarted {
                id,
                work_id,
                generation,
            },
        )
        .await;
        queue_bootstrap_event(
            &input,
            BootstrapSource::Instance { id },
            work_id,
            generation,
            BootstrapEvent::Diagnostic {
                level: EventLevel::Info,
                message: "waiting for bootstrap".to_owned(),
            },
        );
        let mut events = RuntimeBootstrapEvents {
            input: input.clone(),
            id,
            work_id,
            generation,
        };
        let retry_epoch = document
            .status
            .bootstrap
            .as_ref()
            .and_then(|state| state.attempt_epoch)
            .and_then(|epoch| epoch.checked_add(1));
        let result = async {
            match backend
                .wait_bootstrap(&context, &document, &mut control, retry_epoch, Some(&mut events))
                .await
                .map_err(BootstrapTaskFailure::unobserved)?
            {
                backend::BootstrapOutcome::Passed { .. } => {}
                backend::BootstrapOutcome::Failed { attempt_epoch, error } => {
                    return Err(BootstrapTaskFailure::observed(attempt_epoch, error));
                }
            }
            queue_bootstrap_event(
                &input,
                BootstrapSource::Instance { id },
                work_id,
                generation,
                BootstrapEvent::Diagnostic {
                    level: EventLevel::Info,
                    message: "checking guest access".to_owned(),
                },
            );
            probe_guest_access(&context, &backend, &document)
                .await
                .map_err(BootstrapTaskFailure::unobserved)?;
            let manifest = manifest_context(&agent_document).map_err(BootstrapTaskFailure::unobserved)?;
            let config_dir = layout.config_dir();
            document.status.tailscale_serve = tailscale
                .reconcile(
                    &context,
                    TailscaleServeDesired::Instance {
                        config_dir: &config_dir,
                        manifest: manifest.value(),
                        document: &document,
                    },
                )
                .await
                .map_err(BootstrapTaskFailure::unobserved)?;
            Ok(document.status.tailscale_serve)
        }
        .await;
        queue_completion(
            &input,
            WorkCompletion::BootstrapFinished {
                id,
                work_id,
                generation,
                control,
                result,
            },
        )
        .await;
    });
}

fn spawn_reconcile_tailscale_serve(
    services: AgentWorkServices,
    context: Context,
    layout: AgentdpLayout,
    agent_document: AgentDocument,
    mut document: AgentInstanceDocument,
    id: AgentInstanceId,
    attempt: TaskAttempt,
) {
    let TaskAttempt { work_id, generation } = attempt;
    tokio::task::spawn_local(async move {
        let AgentWorkServices { input, tailscale, .. } = services;
        let result = async {
            let manifest = manifest_context(&agent_document)?;
            let config_dir = layout.config_dir();
            document.status.tailscale_serve = tailscale
                .reconcile(
                    &context,
                    TailscaleServeDesired::Instance {
                        config_dir: &config_dir,
                        manifest: manifest.value(),
                        document: &document,
                    },
                )
                .await?;
            Ok::<_, Error>(document.status.tailscale_serve)
        }
        .await
        .map_err(|error| error.to_string());
        queue_completion(
            &input,
            WorkCompletion::TailscaleServeReconciled {
                id,
                work_id,
                generation,
                result,
            },
        )
        .await;
    });
}

struct ExecTask {
    input: inbox::Sender<AgentInput>,
    backend: backend::BackendRef,
    context: Context,
    manifest: AgentManifestContext,
    network: Rc<InstanceNetwork>,
    document: AgentInstanceDocument,
    id: AgentInstanceId,
    work_id: u64,
    command: Vec<String>,
    shell_command: String,
    output: Option<spsc::Sender<AgentInstanceSessionOutput>>,
    timeout: std::time::Duration,
}

fn spawn_exec(task: ExecTask) {
    tokio::task::spawn_local(async move {
        let ExecTask {
            input,
            backend,
            context,
            manifest,
            network,
            document,
            id,
            work_id,
            command,
            shell_command,
            output,
            timeout,
        } = task;
        let mut captured = CapturedOutput::default();
        let mut streaming = output.map(StreamingOutput::new);
        let output: &mut dyn OutputSink = match streaming.as_mut() {
            Some(output) => output,
            None => &mut captured,
        };
        let result = async {
            backend
                .ensure_attached(&context, &network, &manifest, &document)
                .await?;
            backend.exec(&context, &document, &shell_command, timeout, output).await
        }
        .await
        .unwrap_or(CommandOutput {
            status: 1,
            stdout: captured.stdout,
            stderr: captured.stderr,
        });
        queue_completion(
            &input,
            WorkCompletion::ExecFinished {
                id,
                work_id,
                command,
                output: result,
            },
        )
        .await;
    });
}

async fn queue_completion(input: &inbox::Sender<AgentInput>, completion: WorkCompletion) {
    let _result = input.send(AgentInput::Work(Box::new(completion))).await;
}

fn queue_bootstrap_event(
    input: &inbox::Sender<AgentInput>,
    source: BootstrapSource,
    work_id: u64,
    generation: u64,
    event: BootstrapEvent,
) {
    let _result = input.try_send(AgentInput::Work(Box::new(WorkCompletion::BootstrapEvent {
        source,
        work_id,
        generation,
        event,
    })));
}

fn manifest_context(document: &AgentDocument) -> Result<AgentManifestContext, Error> {
    Ok(AgentManifestContext::from_existing_value(
        &document.source_manifest(),
        document.manifest(),
    )?)
}

#[derive(Default)]
struct CapturedOutput {
    stdout: String,
    stderr: String,
}

impl OutputSink for CapturedOutput {
    fn output(&mut self, stream: OutputStream, chunk: &[u8]) {
        let text = String::from_utf8_lossy(chunk);
        match stream {
            OutputStream::Stdout => self.stdout.push_str(&text),
            OutputStream::Stderr => self.stderr.push_str(&text),
        }
    }
}

struct StreamingOutput {
    output: spsc::Sender<AgentInstanceSessionOutput>,
    stdout: Utf8Stream,
    stderr: Utf8Stream,
}

impl StreamingOutput {
    fn new(output: spsc::Sender<AgentInstanceSessionOutput>) -> Self {
        Self {
            output,
            stdout: Utf8Stream::default(),
            stderr: Utf8Stream::default(),
        }
    }

    fn send(&mut self, stream: OutputStream, chunk: String) {
        let chunk = match stream {
            OutputStream::Stdout => AgentInstanceSessionOutput::Stdout(chunk),
            OutputStream::Stderr => AgentInstanceSessionOutput::Stderr(chunk),
        };
        let _result = self.output.try_send(chunk);
    }
}

impl OutputSink for StreamingOutput {
    fn output(&mut self, stream: OutputStream, chunk: &[u8]) {
        let chunk = match stream {
            OutputStream::Stdout => self.stdout.push(chunk),
            OutputStream::Stderr => self.stderr.push(chunk),
        };
        if let Some(chunk) = chunk {
            self.send(stream, chunk);
        }
    }
}

impl Drop for StreamingOutput {
    fn drop(&mut self) {
        if let Some(chunk) = self.stdout.finish() {
            self.send(OutputStream::Stdout, chunk);
        }
        if let Some(chunk) = self.stderr.finish() {
            self.send(OutputStream::Stderr, chunk);
        }
    }
}

struct RuntimeBootstrapEvents {
    input: inbox::Sender<AgentInput>,
    id: AgentInstanceId,
    work_id: u64,
    generation: u64,
}

impl backend::BootstrapEventSink for RuntimeBootstrapEvents {
    fn emit(&mut self, event: BootstrapEvent) {
        queue_bootstrap_event(
            &self.input,
            BootstrapSource::Instance { id: self.id },
            self.work_id,
            self.generation,
            event,
        );
    }
}

struct BaseBootstrapEvents {
    input: inbox::Sender<AgentInput>,
    work_id: u64,
    generation: u64,
}

impl backend::BootstrapEventSink for BaseBootstrapEvents {
    fn emit(&mut self, event: BootstrapEvent) {
        queue_bootstrap_event(
            &self.input,
            BootstrapSource::AgentBase,
            self.work_id,
            self.generation,
            event,
        );
    }
}

fn instance_not_found(agent: &AgentName, id: AgentInstanceId) -> Error {
    Error::InstanceNotFound {
        name: format!("{agent}/{id}"),
    }
}

fn committed_result<T>(persist_error: Option<&str>, value: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
    persist_error.map_or_else(value, |error| {
        Err(Error::PersistState {
            message: error.to_owned(),
        })
    })
}

fn admit_instance_work(
    instance: &mut RunningAgentInstanceState,
    id: u64,
    input_generation: u64,
    kind: AgentInstanceWorkKind,
) {
    instance.work = Some(AgentInstanceWork {
        id,
        kind,
        input_generation,
    });
    instance.documents.private.status.work = work_status(kind, instance.session.is_some());
}

fn project_instance_desired_state(
    instance: &mut RunningAgentInstanceState,
    document: &AgentDocument,
    manifest: &agentdp_core::manifest::AgentManifest,
    id: AgentInstanceId,
    target: AgentInstanceTarget,
) -> Option<String> {
    if instance.documents.private.status.phase == AgentInstancePhase::Deleted {
        return None;
    }
    let agent_base = document
        .desired_agent_base_key()
        .or_else(|| document.ready_agent_base_key())
        .cloned()
        .unwrap_or_else(|| instance.documents.private.spec.agent_base.clone());
    let desired_changed = instance.documents.private.spec.desired_generation != document.generation()
        || instance.documents.private.spec.agent_base != agent_base
        || instance.documents.private.spec.template != *document.template()
        || instance.documents.private.spec.target != target;
    let stale_materialization = instance.documents.private.status.materialized_agent_base != agent_base
        || instance.documents.private.status.materialized_template != *document.template();
    let entering_cleanup = stale_materialization
        && !matches!(
            instance.documents.private.status.phase,
            AgentInstancePhase::Deleting | AgentInstancePhase::Deleted
        );
    if !desired_changed && !entering_cleanup {
        return None;
    }
    let generation_changed = instance.documents.private.spec.desired_generation != document.generation();
    if desired_changed {
        instance.runtime_repair = RuntimeRepairState::Idle;
        instance.documents.private.spec.desired_generation = document.generation();
        instance.documents.private.spec.agent_base = agent_base;
        instance.documents.private.spec.template.clone_from(document.template());
        instance.documents.private.spec.target = target;
    }
    if generation_changed {
        instance.runtime_secrets.next_at = Instant::now();
        instance.runtime_secrets.failure_count = 0;
        instance.secret_host_files = None;
        instance.host_inputs.next_at = Instant::now();
        instance.host_inputs.failure_count = 0;
        instance.documents.private.status.host_inputs.mark_pending();
    }
    if entering_cleanup {
        instance.documents.private.status.phase = AgentInstancePhase::Deleting;
        instance.cleanup_retry = None;
    }
    instance.documents.private.status.clear_readiness();
    instance.documents.private.status.reconciliation = None;
    instance.documents.private.status.network.allow = NetworkAllowState::from(&manifest.spec.network.allow);
    instance.documents.private.status.network.runtime = None;
    let warning = match assign_port_mappings(manifest, id) {
        Ok(ports) => {
            instance.documents.private.status.network.ports = ports;
            None
        }
        Err(error) => Some(format!("{id}: failed to assign host ports: {error}")),
    };
    instance.documents.private.status.observed_generation = instance
        .documents
        .private
        .status
        .observed_generation
        .min(document.generation().saturating_sub(1));
    instance.bootstrap_retry = None;
    warning
}

fn running_instance_from_created_document(
    document: AgentInstanceDocument,
    pending: StartingAgentInstanceState,
) -> AgentInstanceState {
    AgentInstanceState::running(
        document,
        None,
        pending.event_sequence,
        pending.events,
        pending.network_runtime,
        pending.network_events,
    )
}

fn clear_instance_work(instance: &mut RunningAgentInstanceState) {
    instance.work = None;
    instance.documents.private.status.work.transition = None;
    instance.documents.private.status.work.bootstrap = None;
    instance.documents.private.status.work.sessions.active = u16::from(instance.session.is_some());
}

fn refresh_instance_observed_generation(instance: &mut RunningAgentInstanceState) {
    if instance.work.is_some() {
        return;
    }
    let status = &instance.documents.private.status;
    let converged = match instance.documents.private.spec.target {
        AgentInstanceTarget::Active => {
            status.phase == AgentInstancePhase::Running
                && status
                    .reconciliation
                    .as_ref()
                    .is_some_and(|reconciliation| !reconciliation.stale)
                && status
                    .host_inputs
                    .is_ready_for(instance.documents.private.spec.desired_generation)
        }
        AgentInstanceTarget::Inactive => status.phase == AgentInstancePhase::Stopped,
        AgentInstanceTarget::Deleting => false,
    };
    if converged {
        instance
            .documents
            .private
            .status
            .mark_observed_generation(instance.documents.private.spec.desired_generation);
    }
}

fn cleanup_phase_persisted(instance: &RunningAgentInstanceState, phase: AgentInstancePhase) -> bool {
    instance
        .documents
        .persisted
        .as_ref()
        .is_some_and(|document| document.status.phase == phase)
}

fn mark_instance_transition_failed(instance: &mut RunningAgentInstanceState, error: String) {
    instance.documents.private.status.phase = AgentInstancePhase::Failed;
    instance.documents.private.status.clear_readiness();
    instance.documents.private.status.reconciliation = Some(agentdp_core::agent::ReconciliationState {
        stale: true,
        observed_status: "failed".to_owned(),
        observed_pid: None,
        reason: Some(error),
    });
}

fn should_reconcile_runtime_secrets(instance: &RunningAgentInstanceState) -> bool {
    instance.runtime_secrets.active_work_id.is_none()
        && instance.host_inputs.active_work_id.is_none()
        && instance
            .work
            .as_ref()
            .is_none_or(|work| work.kind == AgentInstanceWorkKind::Bootstrapping)
        && instance.documents.private.spec.target == AgentInstanceTarget::Active
        && instance.documents.private.status.phase == AgentInstancePhase::Running
}

fn should_reconcile_host_inputs(instance: &RunningAgentInstanceState) -> bool {
    if instance.host_inputs.active_work_id.is_some()
        || instance.runtime_secrets.active_work_id.is_some()
        || !instance.runtime_repair.allows_auxiliary_work()
        || instance.secret_host_files.is_none()
        || instance.work.is_some()
        || instance.session.is_some()
        || instance.documents.private.spec.target != AgentInstanceTarget::Active
        || instance.documents.private.status.phase != AgentInstancePhase::Running
    {
        return false;
    }
    true
}

fn host_input_reconcile_retry_delay(failure_count: u16) -> Duration {
    retry_delay(
        HOST_INPUT_RECONCILE_RETRY_DELAY,
        HOST_INPUT_RECONCILE_RETRY_MAX_DELAY,
        failure_count,
    )
}

fn instance_create_retry_delay(failure_count: u16) -> Duration {
    retry_delay(
        INSTANCE_CREATE_RETRY_DELAY,
        INSTANCE_CREATE_RETRY_MAX_DELAY,
        failure_count,
    )
}

fn bootstrap_retry_delay(failure_count: u32) -> Duration {
    let exponent = failure_count.saturating_sub(1).min(8);
    INSTANCE_BOOTSTRAP_RETRY_DELAY
        .saturating_mul(1 << exponent)
        .min(INSTANCE_BOOTSTRAP_RETRY_MAX_DELAY)
}

fn retry_delay(initial: Duration, max: Duration, failure_count: u16) -> Duration {
    let exponent = u32::from(failure_count.saturating_sub(1)).min(8);
    initial.saturating_mul(1 << exponent).min(max)
}

fn work_status(work: AgentInstanceWorkKind, session_active: bool) -> AgentInstanceWorkStatus {
    let mut status = AgentInstanceWorkStatus {
        sessions: AgentInstanceSessionsWorkStatus {
            active: u16::from(session_active),
        },
        ..AgentInstanceWorkStatus::default()
    };
    match work {
        AgentInstanceWorkKind::Reconciling | AgentInstanceWorkKind::Repairing => {
            status.transition = Some(transition_work(AgentInstanceTransitionKind::Reconcile));
        }
        AgentInstanceWorkKind::Starting => {
            status.transition = Some(transition_work(AgentInstanceTransitionKind::Start));
        }
        AgentInstanceWorkKind::Stopping => {
            status.transition = Some(transition_work(AgentInstanceTransitionKind::Stop));
        }
        AgentInstanceWorkKind::Deleting => {
            status.transition = Some(transition_work(AgentInstanceTransitionKind::Delete));
        }
        AgentInstanceWorkKind::Bootstrapping => {
            status.bootstrap = Some(AgentInstanceBootstrapWorkStatus {
                phase: AgentInstanceBootstrapWorkPhase::Running,
                current_step: None,
                last_error: None,
                failure_count: None,
                next_retry_unix_seconds: None,
            });
        }
    }
    status
}

fn transition_work(kind: AgentInstanceTransitionKind) -> AgentInstanceTransitionWorkStatus {
    AgentInstanceTransitionWorkStatus {
        kind,
        started_unix_seconds: Some(time::unix_seconds()),
        message: None,
    }
}

fn bootstrap_retry_deadline(document: &AgentInstanceDocument) -> Option<Instant> {
    if document
        .status
        .readiness
        .as_ref()
        .is_some_and(|readiness| readiness.ready)
    {
        return None;
    }
    let next_retry_unix_seconds = document.status.bootstrap.as_ref()?.next_retry_unix_seconds;
    let now_unix_seconds = time::unix_seconds();
    let delay = next_retry_unix_seconds.saturating_sub(now_unix_seconds);
    Some(Instant::now() + Duration::from_secs(delay))
}

fn apply_bound_host_ports(network: &mut NetworkState, host_ports: &BTreeMap<String, PortMappingState>) {
    for (name, mapping) in host_ports {
        if let Some(configured) = network.ports.get_mut(name) {
            configured.host = mapping.host;
        }
    }
}

fn apply_network_event(document: &mut AgentInstanceDocument, event: &AgentInstanceNetworkEvent) -> bool {
    let event_seconds = event.unix_millis / 1_000;
    let mut bound_port = None;
    if matches!(
        &event.event,
        AgentInstanceNetworkEventKind::TelemetrySnapshot { .. }
            | AgentInstanceNetworkEventKind::EgressProxyClosed { .. }
            | AgentInstanceNetworkEventKind::DnsResolved { .. }
    ) {
        return false;
    }
    {
        let status = document
            .status
            .network
            .runtime
            .get_or_insert_with(AgentInstanceNetworkStatus::default);
        status.network_event_drops = status.network_event_drops.saturating_add(event.dropped_events_before);
        match &event.event {
            AgentInstanceNetworkEventKind::LifecycleStateChanged { state } => {
                status.state.clone_from(state);
                status.ready = state == "traffic-observed";
                status.last_state_change_unix_seconds = event_seconds;
            }
            AgentInstanceNetworkEventKind::TelemetrySnapshot {
                started_unix_seconds,
                last_state_change_unix_seconds,
                last_transport_connect_unix_seconds,
                last_guest_frame_unix_seconds,
                last_host_frame_unix_seconds,
                guest_frames_received,
                guest_bytes_received,
                host_frames_sent,
                host_bytes_sent,
                session_disconnects,
                connect_errors,
                egress_errors,
                ..
            } => {
                status.started_unix_seconds = *started_unix_seconds;
                status.last_state_change_unix_seconds = *last_state_change_unix_seconds;
                status.last_transport_connect_unix_seconds = *last_transport_connect_unix_seconds;
                status.last_guest_frame_unix_seconds = *last_guest_frame_unix_seconds;
                status.last_host_frame_unix_seconds = *last_host_frame_unix_seconds;
                status.guest_frames_received = *guest_frames_received;
                status.guest_bytes_received = *guest_bytes_received;
                status.host_frames_sent = *host_frames_sent;
                status.host_bytes_sent = *host_bytes_sent;
                status.session_disconnects = *session_disconnects;
                status.connect_errors = *connect_errors;
                status.egress_errors = *egress_errors;
            }
            AgentInstanceNetworkEventKind::TransportGuestConnected { transport, generation } => {
                status.transport = Some(transport.clone());
                status.generation = Some(*generation);
                status.last_transport_connect_unix_seconds = Some(event_seconds);
            }
            AgentInstanceNetworkEventKind::TransportGuestDisconnected { generation, reason } => {
                status.generation = Some(*generation);
                status.last_error = Some(reason.clone());
            }
            AgentInstanceNetworkEventKind::TransportConnectFailed { error, .. }
            | AgentInstanceNetworkEventKind::TransportRegisterFailed { error, .. } => {
                status.last_error = Some(error.clone());
            }
            AgentInstanceNetworkEventKind::EgressError { message, .. }
            | AgentInstanceNetworkEventKind::HostPortError { message }
            | AgentInstanceNetworkEventKind::ReactorError { message } => {
                status.last_error = Some(message.clone());
            }
            AgentInstanceNetworkEventKind::HostPortBound {
                name,
                protocol,
                guest,
                host,
            } => {
                let mapping = PortMappingState {
                    guest: *guest,
                    host: Some(*host),
                    protocol: *protocol,
                };
                status.host_ports.insert(name.clone(), mapping.clone());
                bound_port = Some((name.clone(), mapping));
            }
            AgentInstanceNetworkEventKind::EgressProxyClosed { .. }
            | AgentInstanceNetworkEventKind::DnsResolved { .. } => {}
        }
    }
    if let Some((name, mapping)) = bound_port
        && let Some(configured) = document.status.network.ports.get_mut(&name)
    {
        configured.host = mapping.host;
    }
    true
}

const fn should_forward_instance_event(event: &AgentInstanceEvent) -> bool {
    !matches!(
        event,
        AgentInstanceEvent::NetworkEvent(_)
            | AgentInstanceEvent::SessionOutput { .. }
            | AgentInstanceEvent::DocumentChanged { .. }
    )
}

fn ready_result(document: &AgentInstanceDocument) -> ReadinessResult {
    ReadinessResult {
        ready: true,
        services: document
            .status
            .network
            .ports
            .iter()
            .filter_map(|(name, port)| {
                Some((
                    name.clone(),
                    ServiceStatus {
                        url: None,
                        host_port: port.host?,
                        guest_port: port.guest,
                    },
                ))
            })
            .collect(),
        healthchecks: Vec::new(),
    }
}

async fn probe_guest_access(
    context: &Context,
    backend: &backend::BackendRef,
    document: &AgentInstanceDocument,
) -> Result<(), Error> {
    let mut output = DiscardOutputSink;
    let result = backend
        .exec(context, document, "true", INSTANCE_READY_PROBE_TIMEOUT, &mut output)
        .await?;
    if result.status == 0 {
        return Ok(());
    }
    Err(Error::ReadinessProbeFailed {
        name: document.name(),
        status: result.status,
    })
}

struct DiscardOutputSink;

impl OutputSink for DiscardOutputSink {
    fn output(&mut self, _stream: OutputStream, _chunk: &[u8]) {}
}

async fn try_read_agent_document(path: &Path) -> Result<Option<AgentDocument>, Error> {
    let contents = match tokio::fs::read_to_string(path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(Error::ReadAgentDocument {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let document = serde_yaml::from_str::<AgentDocument>(&contents).map_err(|source| Error::ParseAgentDocument {
        path: path.to_path_buf(),
        source,
    })?;
    document
        .manifest()
        .validate()
        .map_err(|errors| Error::InvalidAgentDocument {
            path: path.to_path_buf(),
            errors,
        })?;
    Ok(Some(document))
}

async fn load_instance_documents(
    layout: &AgentdpLayout,
    agent: &AgentName,
) -> Result<BTreeMap<AgentInstanceId, AgentInstanceDocument>, Error> {
    let mut documents = BTreeMap::new();
    for (id, instance_layout) in layout.instance_layouts(agent).await? {
        let path = instance_layout.instance_state();
        let contents = match tokio::fs::read_to_string(&path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(Error::ReadInstanceDocument {
                    path: path.clone(),
                    source,
                });
            }
        };
        let mut document = serde_yaml::from_str::<AgentInstanceDocument>(&contents).map_err(|source| {
            Error::ParseInstanceDocument {
                path: path.clone(),
                source,
            }
        })?;
        if document.metadata.agent != *agent || document.metadata.id != id {
            return Err(Error::PersistedStateIdentityMismatch {
                path,
                expected: format!("{agent}/{id}"),
                actual: format!("{}/{}", document.metadata.agent, document.metadata.id),
            });
        }
        reset_loaded_instance_runtime_status(&mut document);
        documents.insert(id, document);
    }
    Ok(documents)
}

async fn inspect_instance_states(
    layout: &AgentdpLayout,
    agent: &AgentName,
    documents: BTreeMap<AgentInstanceId, AgentInstanceDocument>,
) -> Result<BTreeMap<AgentInstanceId, (AgentInstanceDocument, SequencePlan)>, Error> {
    let mut inspected = BTreeMap::new();
    for (id, document) in documents {
        let events_path = layout.instance(agent, id).instance_events();
        let event_plan = inspect_event_sequence(&events_path).await?;
        inspected.insert(id, (document, event_plan));
    }
    Ok(inspected)
}

async fn apply_instance_event_repairs(
    inspected: BTreeMap<AgentInstanceId, (AgentInstanceDocument, SequencePlan)>,
) -> Result<BTreeMap<AgentInstanceId, (AgentInstanceDocument, u64)>, Error> {
    let mut prepared = BTreeMap::new();
    for (id, (document, event_plan)) in inspected {
        prepared.insert(id, (document, event_plan.apply().await?));
    }
    Ok(prepared)
}

fn start_instance_states(
    context: &Context,
    layout: &AgentdpLayout,
    agent: &AgentName,
    prepared: BTreeMap<AgentInstanceId, (AgentInstanceDocument, u64)>,
) -> BTreeMap<AgentInstanceId, AgentInstanceState> {
    prepared
        .into_iter()
        .map(|(id, (document, event_sequence))| {
            let events_path = layout.instance(agent, id).instance_events();
            let (network_events, network_event_receiver) = spsc::bounded(1024);
            (
                id,
                AgentInstanceState::running(
                    document.clone(),
                    Some(document),
                    event_sequence,
                    EventLogWriter::spawn(context, events_path),
                    Rc::new(InstanceNetwork::new(network_events)),
                    network_event_receiver,
                ),
            )
        })
        .collect()
}

fn reset_loaded_instance_runtime_status(document: &mut AgentInstanceDocument) {
    document.status.work = AgentInstanceWorkStatus::default();
    if document.status.phase == AgentInstancePhase::Running {
        document.status.clear_readiness();
        document.status.reconciliation = None;
        document.status.network.runtime = None;
        document.status.tailscale_serve = None;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use agentdp_core::Context;
    use agentdp_core::agent::{
        AgentInstanceDocument, AgentInstanceId, AgentInstancePhase, AgentInstanceTarget, AgentName, BackendState,
        InstanceName, NetworkAllowState, NetworkIpv6State, NetworkState, QemuImageState, QemuMediatedCaState,
        QemuState,
    };
    use agentdp_core::manifest::AgentManifest;
    use agentdp_core::provisioning::secrets::SecretBindings;
    use agentdp_ds::local::spsc;

    use super::{
        AgentBaseKey, AgentBaseState, EventLogWriter, INSTANCE_BOOTSTRAP_RETRY_DELAY,
        INSTANCE_BOOTSTRAP_RETRY_MAX_DELAY, InstanceNetwork, RunningAgentInstanceState, RuntimeRepairState,
        bootstrap_retry_delay,
    };

    #[test]
    fn runtime_repair_blocks_auxiliary_work_until_recovered() {
        assert!(RuntimeRepairState::Idle.allows_auxiliary_work());
        assert!(!RuntimeRepairState::Due.allows_auxiliary_work());
        assert!(!RuntimeRepairState::Backoff.allows_auxiliary_work());
    }

    #[test]
    fn bootstrap_retry_delay_backs_off_to_cap() {
        assert_eq!(bootstrap_retry_delay(1), INSTANCE_BOOTSTRAP_RETRY_DELAY);
        assert_eq!(bootstrap_retry_delay(2), INSTANCE_BOOTSTRAP_RETRY_DELAY * 2);
        assert_eq!(bootstrap_retry_delay(u32::MAX), INSTANCE_BOOTSTRAP_RETRY_MAX_DELAY);
    }

    #[tokio::test(flavor = "local")]
    async fn active_auxiliary_work_excludes_bootstrap_retry_wake() {
        let mut instance = test_running_instance();
        let retry = tokio::time::Instant::now();
        instance.bootstrap_retry = Some(retry);
        assert_eq!(instance.bootstrap_retry_wake(), Some(retry));

        instance.host_inputs.active_work_id = Some(1);
        assert_eq!(instance.bootstrap_retry_wake(), None);

        instance.host_inputs.active_work_id = None;
        instance.runtime_secrets.active_work_id = Some(2);
        assert_eq!(instance.bootstrap_retry_wake(), None);
    }

    #[test]
    fn base_provisioned_resources_require_external_state_or_base_key() {
        assert!(!AgentBaseState::Missing.has_provisioned_resources());
        assert!(!AgentBaseState::Preparing { work_id: 1 }.has_provisioned_resources());
        assert!(
            !AgentBaseState::Failed {
                generation: 1,
                key: None,
            }
            .has_provisioned_resources()
        );

        let key = AgentBaseKey::new("sha256-test");
        assert!(
            AgentBaseState::Building {
                work_id: 1,
                generation: 1,
                key: key.clone()
            }
            .has_provisioned_resources()
        );
        assert!(AgentBaseState::Ready { key: key.clone() }.has_provisioned_resources());
        assert!(
            AgentBaseState::Failed {
                generation: 1,
                key: Some(key),
            }
            .has_provisioned_resources()
        );
        assert!(AgentBaseState::Stopping { work_id: 1 }.has_provisioned_resources());
        assert!(AgentBaseState::Stopped.has_provisioned_resources());
    }

    fn test_running_instance() -> RunningAgentInstanceState {
        let manifest: AgentManifest =
            serde_yaml::from_str(agentdp_test_support::manifest::minimal()).expect("minimal manifest");
        let agent = AgentName::new(manifest.name());
        let id = AgentInstanceId::new(0);
        let backend = test_backend_state();
        let network = NetworkState::new(
            &backend,
            NetworkAllowState::default(),
            NetworkIpv6State::default(),
            BTreeMap::new(),
        );
        let document = AgentInstanceDocument::new(
            agent,
            id,
            InstanceName::new("replica-0"),
            1,
            AgentBaseKey::new("sha256-test"),
            manifest.spec.template,
            AgentInstanceTarget::Active,
            AgentInstancePhase::Running,
            network,
            Vec::new(),
            None,
            backend,
        );
        let (network_events, network_event_receiver) = spsc::bounded(1);
        RunningAgentInstanceState::new(
            document,
            None,
            1,
            EventLogWriter::spawn(
                &Context::quiet(),
                std::env::temp_dir().join(format!("agentdp-wake-test-{}.jsonl", std::process::id())),
            ),
            Rc::new(InstanceNetwork::new(network_events)),
            network_event_receiver,
        )
    }

    fn test_backend_state() -> BackendState {
        BackendState::Qemu(QemuState {
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
        })
    }
}
