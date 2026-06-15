use serde::{Deserialize, Serialize};

use super::{
    AgentBaseKey, AgentDocument, AgentInstanceBootstrapStepStatus, AgentInstanceDocument, AgentInstanceId,
    AgentInstanceNetworkEvent, AgentInstanceTarget, AgentInstanceTransitionKind, AgentName, OperationResult,
};
use agentdp_protocol::server_guest::BootstrapStepStatus;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentEventEnvelope {
    pub sequence: u64,
    pub timestamp: String,
    pub generation: u64,
    pub source: AgentEventSource,
    pub event: AgentEvent,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentEventSource {
    Controller,
    AgentBase,
    Instance { id: AgentInstanceId },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentEvent {
    DesiredStateAccepted { generation: u64 },
    ScaleAccepted { generation: u64, replicas: u16 },
    DeleteAccepted { generation: u64 },
    AgentBaseStarted { key: AgentBaseKey },
    AgentBaseReady { key: AgentBaseKey },
    AgentBaseFailed { key: AgentBaseKey, error: String },
    InstanceCreated { instance_id: AgentInstanceId },
    InstanceDeleted { instance_id: AgentInstanceId },
    BootstrapEvent { event: BootstrapEvent },
    InstanceEvent { event: Box<AgentInstanceEventEnvelope> },
    DocumentChanged { document: Box<AgentDocument> },
    Diagnostic { level: EventLevel, message: String },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BootstrapEvent {
    Diagnostic {
        level: EventLevel,
        message: String,
    },
    StepStarted {
        step: AgentInstanceBootstrapStepStatus,
    },
    StepFinished {
        step: String,
        status: BootstrapStepStatus,
        exit_status: i32,
        duration_ms: u64,
    },
    StepFailed {
        step: String,
        status: BootstrapStepStatus,
        exit_status: i32,
        duration_ms: u64,
        message: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceEventEnvelope {
    pub sequence: u64,
    pub timestamp: String,
    pub generation: u64,
    #[serde(rename = "workEpoch", skip_serializing_if = "Option::is_none")]
    pub work_epoch: Option<u64>,
    pub source: AgentInstanceEventSource,
    pub event: AgentInstanceEvent,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentInstanceEventSource {
    Instance,
    Backend,
    Bootstrap,
    Network,
    Session { kind: SessionKind },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentInstanceEvent {
    SpecApplied {
        generation: u64,
        target: AgentInstanceTarget,
    },
    TransitionStarted {
        transition: AgentInstanceTransitionKind,
    },
    TransitionFinished {
        transition: AgentInstanceTransitionKind,
        result: OperationResult,
    },
    BootstrapStarted,
    BootstrapFinished {
        result: OperationResult,
    },
    SessionStarted {
        session: SessionKind,
    },
    SessionOutput {
        session: SessionKind,
        stream: OutputStream,
        chunk: String,
    },
    SessionFinished {
        session: SessionKind,
        result: SessionResultSummary,
    },
    NetworkEvent(AgentInstanceNetworkEvent),
    DocumentChanged {
        document: Box<AgentInstanceDocument>,
    },
    Diagnostic {
        level: EventLevel,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventLevel {
    Info,
    Warn,
    Error,
    Verbose,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    Exec,
    Shell,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionResultSummary {
    pub exit_status: Option<u64>,
    pub result: OperationResult,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentWaitConditionResult {
    Accepted,
    Observed,
    Ready,
    Paused,
    Stopped,
    Deleted,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentWaitStatusResult {
    Satisfied,
    Pending,
    Superseded,
    Timeout,
    Deleted,
    Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentWaitResult {
    pub agent: AgentName,
    pub generation: u64,
    pub condition: AgentWaitConditionResult,
    pub status: AgentWaitStatusResult,
    pub document: AgentDocument,
}
