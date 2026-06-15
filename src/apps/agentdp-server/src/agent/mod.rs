mod base;
mod context;
mod documents;
mod event_log;
mod layout;
mod registry;
mod runtime;
#[cfg(test)]
mod runtime_tests;
mod wait;

pub(crate) use agentdp_core::agent::{AgentBaseKey, AgentInstanceId, AgentName, IdentityError, InstanceName};
pub(crate) use context::{AgentContextError, AgentManifestContext};
pub(crate) use layout::{AgentBaseFiles, AgentInstanceFiles, AgentdpLayout, Error as AgentdpLayoutError};
pub(crate) use registry::AgentRegistry;
pub(crate) use runtime::{Agent, AgentCommand, AgentInstanceSessionOutput, AgentStreamItem, Error as AgentError};
pub(crate) use wait::{wait_condition_result, wait_status};

pub(crate) const AGENT_BASE_INSTANCE: &str = "agent-base";
