use std::collections::BTreeMap;
use std::path::Path;

use agentdp_core::agent::{
    AgentDocument, AgentInstanceDocument, AgentInstanceId, AgentInstancePhase, AgentInstanceTarget, AgentStatusPhase,
    ReplicaStatus,
};
use agentdp_core::manifest::AgentPhase;

use super::AgentdpLayout;
use super::runtime::{AgentBaseState, AgentInstanceState};

const REDACTED_PRIVATE_KEY: &str = "<redacted>";

pub(super) struct AgentDocuments {
    pub(super) private: AgentDocument,
    pub(super) persisted: Option<AgentDocument>,
    pub(super) public: AgentDocument,
}

impl AgentDocuments {
    pub(super) fn new(private: AgentDocument, persisted: Option<AgentDocument>) -> Self {
        let mut public = private.clone();
        for status in public.status.instances.values_mut() {
            if let Some(guest_access) = &mut status.guest_access {
                REDACTED_PRIVATE_KEY.clone_into(&mut guest_access.private_key);
            }
        }
        Self {
            private,
            persisted,
            public,
        }
    }

    pub(super) fn write(
        &mut self,
        mut private: AgentDocument,
        base: &AgentBaseState,
        instances: &mut BTreeMap<AgentInstanceId, AgentInstanceState>,
    ) {
        let mut instance_statuses = BTreeMap::new();
        let mut ready = 0u16;
        let mut active = 0u16;
        let mut stopped = 0u16;
        let mut deleting = 0u16;
        for (id, instance) in instances.iter_mut() {
            let Some(instance) = instance.running_mut() else {
                continue;
            };
            instance.documents.write(instance.documents.private.clone());
            let status = instance.documents.public.status.clone();
            if matches!(status.phase, AgentInstancePhase::Deleting | AgentInstancePhase::Deleted) {
                deleting = deleting.saturating_add(1);
            }
            if status.phase == AgentInstancePhase::Running {
                active = active.saturating_add(1);
            }
            if status.phase != AgentInstancePhase::Running {
                stopped = stopped.saturating_add(1);
            }
            if instance.documents.private.spec.target == AgentInstanceTarget::Active
                && status.observed_generation == private.generation()
                && status.readiness.as_ref().is_some_and(|state| state.ready)
                && status.host_inputs.is_ready_for(private.generation())
            {
                ready = ready.saturating_add(1);
            }
            instance_statuses.insert(*id, status);
        }
        let deleted = private.deletion_requested()
            && instances.is_empty()
            && matches!(
                base,
                AgentBaseState::Stopped | AgentBaseState::Missing | AgentBaseState::Failed { .. }
            );
        let instances_observed = instances.values().all(|instance| match instance {
            AgentInstanceState::Starting(_) => false,
            AgentInstanceState::Running(instance) => {
                instance.documents.private.status.observed_generation == private.generation()
                    && (instance.documents.private.spec.target != AgentInstanceTarget::Active
                        || instance
                            .documents
                            .private
                            .status
                            .host_inputs
                            .is_ready_for(private.generation()))
            }
        });
        let inactive_converged = (private.phase() == AgentPhase::Paused || private.replicas() == 0)
            && active == 0
            && deleting == 0
            && instances_observed;
        let running_converged = !private.deletion_requested()
            && private.phase() == AgentPhase::Running
            && active == private.replicas()
            && private.ready_agent_base_key() == private.desired_agent_base_key()
            && instances_observed;
        let observed = deleted || inactive_converged || running_converged;
        if observed {
            private.mark_observed_generation_if_changed();
        }
        private.status.phase = if deleted {
            AgentStatusPhase::Deleted
        } else if private.deletion_requested() {
            AgentStatusPhase::Deleting
        } else if private.phase() == AgentPhase::Paused {
            AgentStatusPhase::Paused
        } else {
            AgentStatusPhase::Running
        };
        private.status.deleted = deleted;
        private.status.reconciling = !observed;
        private.status.replicas = ReplicaStatus {
            desired: private.replicas(),
            active,
            ready,
            stopped,
            deleting,
        };
        private.status.instances = instance_statuses;

        let persisted = self.persisted.take();
        *self = Self::new(private, persisted);
    }

    pub(super) fn dirty(&self) -> bool {
        self.persisted.as_ref() != Some(&self.private)
    }

    pub(super) async fn persist_to(
        &mut self,
        layout: &AgentdpLayout,
        instances: &mut BTreeMap<AgentInstanceId, AgentInstanceState>,
    ) -> Result<(), String> {
        let agent = self.private.agent().clone();
        if self.dirty() {
            let path = layout.agent_document(&agent);
            let contents = serde_yaml::to_string(&self.private).map_err(|error| error.to_string())?;
            let should_write = match tokio::fs::read_to_string(&path).await {
                Ok(existing) => existing != contents,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
                Err(error) => return Err(format!("read {}: {error}", path.display())),
            };
            if should_write {
                let Some(parent) = path.parent() else {
                    return Err(format!("agent document path has no parent: {}", path.display()));
                };
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|error| format!("create {}: {error}", parent.display()))?;
                tokio::fs::write(&path, contents)
                    .await
                    .map_err(|error| format!("write {}: {error}", path.display()))?;
            }
            self.persisted = Some(self.private.clone());
        }
        for (id, instance) in instances {
            let Some(instance) = instance.running_mut() else {
                continue;
            };
            let path = layout.instance(&agent, *id).instance_state();
            instance.documents.persist_to(&path).await?;
        }
        Ok(())
    }
}

pub(super) struct AgentInstanceDocuments {
    pub(super) private: AgentInstanceDocument,
    pub(super) persisted: Option<AgentInstanceDocument>,
    pub(super) public: AgentInstanceDocument,
}

impl AgentInstanceDocuments {
    pub(super) fn new(private: AgentInstanceDocument, persisted: Option<AgentInstanceDocument>) -> Self {
        let mut public = private.clone();
        if let Some(guest_access) = &mut public.status.guest_access {
            REDACTED_PRIVATE_KEY.clone_into(&mut guest_access.private_key);
        }
        Self {
            private,
            persisted,
            public,
        }
    }

    pub(super) fn write(&mut self, private: AgentInstanceDocument) {
        let persisted = self.persisted.take();
        *self = Self::new(private, persisted);
    }

    fn dirty(&self) -> bool {
        self.persisted.as_ref() != Some(&self.private)
    }

    async fn persist_to(&mut self, path: &Path) -> Result<(), String> {
        if !self.dirty() {
            return Ok(());
        }
        let Some(parent) = path.parent() else {
            return Err(format!("instance document path has no parent: {}", path.display()));
        };
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
        let document_yaml = serde_yaml::to_string(&self.private).map_err(|error| error.to_string())?;
        tokio::fs::write(path, document_yaml)
            .await
            .map_err(|error| format!("write {}: {error}", path.display()))?;
        self.persisted = Some(self.private.clone());
        Ok(())
    }
}
