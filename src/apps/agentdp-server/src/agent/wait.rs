use agentdp_core::agent::{AgentDocument, AgentStatusPhase, AgentWaitConditionResult, AgentWaitStatusResult};
use agentdp_protocol::client_server::AgentWaitCondition;

pub(crate) const fn wait_condition_result(condition: AgentWaitCondition) -> AgentWaitConditionResult {
    match condition {
        AgentWaitCondition::Accepted => AgentWaitConditionResult::Accepted,
        AgentWaitCondition::Observed => AgentWaitConditionResult::Observed,
        AgentWaitCondition::Ready => AgentWaitConditionResult::Ready,
        AgentWaitCondition::Paused => AgentWaitConditionResult::Paused,
        AgentWaitCondition::Stopped => AgentWaitConditionResult::Stopped,
        AgentWaitCondition::Deleted => AgentWaitConditionResult::Deleted,
    }
}

pub(crate) fn wait_status(
    document: &AgentDocument,
    generation: u64,
    condition: AgentWaitCondition,
) -> AgentWaitStatusResult {
    if document.generation() > generation {
        return AgentWaitStatusResult::Superseded;
    }
    if document.generation() < generation {
        return AgentWaitStatusResult::Pending;
    }
    if condition_satisfied(document, generation, condition) {
        return AgentWaitStatusResult::Satisfied;
    }
    if document.status.deleted {
        return AgentWaitStatusResult::Deleted;
    }
    if failed_for_generation(document, generation, condition) {
        return AgentWaitStatusResult::Failed;
    }
    AgentWaitStatusResult::Pending
}

fn condition_satisfied(document: &AgentDocument, generation: u64, condition: AgentWaitCondition) -> bool {
    match condition {
        AgentWaitCondition::Accepted => document.generation() == generation,
        AgentWaitCondition::Observed => document.status.observed_generation == generation,
        AgentWaitCondition::Ready => {
            document.status.observed_generation == generation
                && document.status.replicas.ready == document.status.replicas.desired
                && !document.status.reconciling
        }
        AgentWaitCondition::Paused => {
            document.status.observed_generation == generation
                && document.status.phase == AgentStatusPhase::Paused
                && document.status.replicas.active == 0
                && !document.status.reconciling
        }
        AgentWaitCondition::Stopped => {
            document.status.observed_generation == generation
                && document.status.replicas.active == 0
                && !document.status.reconciling
        }
        AgentWaitCondition::Deleted => document.status.observed_generation == generation && document.status.deleted,
    }
}

fn failed_for_generation(document: &AgentDocument, generation: u64, condition: AgentWaitCondition) -> bool {
    if base_failure_blocks_condition(document, condition) {
        return true;
    }
    document
        .status
        .instances
        .values()
        .any(|instance| instance.spec_generation_failed(generation))
}

fn base_failure_blocks_condition(document: &AgentDocument, condition: AgentWaitCondition) -> bool {
    if document.status.agent_base.phase != agentdp_core::agent::AgentBasePhase::Failed {
        return false;
    }
    match condition {
        AgentWaitCondition::Ready => true,
        AgentWaitCondition::Observed => {
            document.status.phase == AgentStatusPhase::Running && document.status.replicas.desired > 0
        }
        AgentWaitCondition::Accepted
        | AgentWaitCondition::Paused
        | AgentWaitCondition::Stopped
        | AgentWaitCondition::Deleted => false,
    }
}

trait InstanceWaitStatus {
    fn spec_generation_failed(&self, generation: u64) -> bool;
}

impl InstanceWaitStatus for agentdp_core::agent::AgentInstanceStatus {
    fn spec_generation_failed(&self, generation: u64) -> bool {
        self.observed_generation == generation && self.phase == agentdp_core::agent::AgentInstancePhase::Failed
    }
}
