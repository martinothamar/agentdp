use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::manifest::{
    AgentDeploymentSpec, AgentManifest, AgentManifestKind, AgentMetadata, AgentPhase, AgentSpec, NetworkProtocol,
    ValidationErrors,
};

use super::{
    AgentBaseKey, AgentBasePhase, AgentBaseStatus, AgentInstanceId, AgentInstancePhase, AgentInstanceStatus,
    AgentInstanceTarget, AgentName, AgentStatus, AgentStatusPhase, BackendState, GuestAccessState, InstanceName,
    NetworkState, PortMappingState, PortProtocolState, ReplicaStatus,
};

pub const AGENTDP_API_VERSION: &str = "agentdp.dev/v1alpha1";

pub type AgentApplyResult = AgentDocument;
pub type AgentDeleteResult = AgentDocument;
pub type AgentScaleResult = AgentDocument;

#[derive(Debug, Error)]
pub enum PortRequestError {
    #[error("manifest host port plan is invalid:\n{0}")]
    Manifest(ValidationErrors),
    #[error("network port {name} host base {host} overflows for instance {instance}")]
    HostPortOverflow {
        name: String,
        host: u16,
        instance: AgentInstanceId,
    },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentDocument {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: AgentDocumentKind,
    pub metadata: AgentDocumentMetadata,
    pub spec: AgentDocumentSpec,
    pub status: AgentStatus,
}

impl<'de> Deserialize<'de> for AgentDocument {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            #[serde(rename = "apiVersion")]
            api_version: String,
            kind: AgentDocumentKind,
            metadata: AgentDocumentMetadata,
            spec: AgentDocumentSpec,
            status: AgentStatus,
        }

        let fields = Fields::deserialize(deserializer)?;
        if fields.api_version != AGENTDP_API_VERSION {
            return Err(D::Error::custom(format!(
                "unsupported apiVersion `{}` for Agent",
                fields.api_version
            )));
        }
        Ok(Self {
            api_version: fields.api_version,
            kind: fields.kind,
            metadata: fields.metadata,
            spec: fields.spec,
            status: fields.status,
        })
    }
}

impl AgentDocument {
    /// Builds a new persisted Agent document from a source manifest.
    ///
    /// # Errors
    ///
    /// Returns an error when requested host ports are invalid for the manifest.
    pub fn from_manifest(
        source_manifest: impl Into<String>,
        agent: AgentName,
        manifest: &AgentManifest,
    ) -> Result<Self, PortRequestError> {
        manifest.validate().map_err(PortRequestError::Manifest)?;
        Ok(Self {
            api_version: AGENTDP_API_VERSION.to_owned(),
            kind: AgentDocumentKind::Agent,
            metadata: AgentDocumentMetadata {
                name: agent,
                generation: 1,
                deletion_requested: false,
            },
            spec: AgentDocumentSpec {
                source_manifest: source_manifest.into(),
                deployment: manifest.spec.clone(),
            },
            status: AgentStatus {
                observed_generation: 0,
                phase: agent_status_phase(manifest.spec.phase, false, false),
                replicas: ReplicaStatus {
                    desired: manifest.spec.replicas,
                    ..ReplicaStatus::default()
                },
                reconciling: true,
                deleted: false,
                agent_base: AgentBaseStatus::default(),
                instances: BTreeMap::new(),
            },
        })
    }

    /// Builds an updated Agent document and increments generation when desired state changed.
    ///
    /// # Errors
    ///
    /// Returns an error when requested host ports are invalid for the manifest.
    pub fn from_manifest_after_existing(
        source_manifest: impl Into<String>,
        agent: AgentName,
        manifest: &AgentManifest,
        existing: &Self,
    ) -> Result<Self, PortRequestError> {
        let mut document = Self::from_manifest(source_manifest, agent, manifest)?;
        if existing.has_same_desired_state(&document) {
            document.metadata = existing.metadata.clone();
            document.status = existing.status.clone();
            return Ok(document);
        }
        let preserve_agent_base = existing.spec.deployment.template == document.spec.deployment.template;
        document.metadata.generation = existing.metadata.generation.saturating_add(1);
        document.status = existing.status.clone();
        document.status.observed_generation = existing.status.observed_generation;
        if !preserve_agent_base {
            document.status.agent_base = AgentBaseStatus::default();
        }
        document.refresh_status_projection(false);
        Ok(document)
    }

    #[must_use]
    pub fn source_manifest(&self) -> PathBuf {
        PathBuf::from(&self.spec.source_manifest)
    }

    #[must_use]
    pub const fn agent(&self) -> &AgentName {
        &self.metadata.name
    }

    #[must_use]
    pub const fn replicas(&self) -> u16 {
        self.spec.deployment.replicas
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.metadata.generation
    }

    #[must_use]
    pub const fn deletion_requested(&self) -> bool {
        self.metadata.deletion_requested
    }

    #[must_use]
    pub const fn observed_generation(&self) -> u64 {
        self.status.observed_generation
    }

    #[must_use]
    pub const fn phase(&self) -> AgentPhase {
        self.spec.deployment.phase
    }

    #[must_use]
    pub const fn template(&self) -> &AgentSpec {
        &self.spec.deployment.template
    }

    #[must_use]
    pub const fn desired_agent_base_key(&self) -> Option<&AgentBaseKey> {
        self.status.agent_base.desired_key.as_ref()
    }

    #[must_use]
    pub const fn ready_agent_base_key(&self) -> Option<&AgentBaseKey> {
        self.status.agent_base.ready_key.as_ref()
    }

    pub fn mark_agent_base_desired(&mut self, key: AgentBaseKey) -> bool {
        if self.status.agent_base.desired_key.as_ref() == Some(&key)
            && self.status.agent_base.phase == AgentBasePhase::Building
            && self.status.agent_base.last_error.is_none()
        {
            return false;
        }
        self.status.agent_base.desired_key = Some(key);
        self.status.agent_base.ready_key = None;
        self.status.agent_base.phase = AgentBasePhase::Building;
        self.status.agent_base.message = None;
        self.status.agent_base.last_error = None;
        true
    }

    pub fn mark_agent_base_ready(&mut self, key: AgentBaseKey) -> bool {
        let changed = self.status.agent_base.desired_key.as_ref() != Some(&key)
            || self.status.agent_base.ready_key.as_ref() != Some(&key)
            || self.status.agent_base.phase != AgentBasePhase::Ready
            || self.status.agent_base.last_error.is_some();
        if changed {
            self.status.agent_base.desired_key = Some(key.clone());
            self.status.agent_base.ready_key = Some(key);
            self.status.agent_base.phase = AgentBasePhase::Ready;
            self.status.agent_base.message = None;
            self.status.agent_base.last_error = None;
        }
        changed
    }

    pub fn mark_agent_base_failed(&mut self, message: String) -> bool {
        if self.status.agent_base.phase == AgentBasePhase::Failed
            && self.status.agent_base.last_error.as_ref() == Some(&message)
        {
            return false;
        }
        self.status.agent_base.phase = AgentBasePhase::Failed;
        self.status.agent_base.last_error = Some(message);
        true
    }

    #[must_use]
    pub fn has_same_desired_state(&self, other: &Self) -> bool {
        self.metadata.name == other.metadata.name
            && self.metadata.deletion_requested == other.metadata.deletion_requested
            && self.spec.deployment == other.spec.deployment
    }

    pub const fn mark_deletion_requested_if_changed(&mut self) -> bool {
        if self.metadata.deletion_requested {
            return false;
        }
        self.metadata.deletion_requested = true;
        self.metadata.generation = self.metadata.generation.saturating_add(1);
        true
    }

    /// Updates desired replica count and increments generation if it changed.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest host-port plan is invalid for the new replica count.
    pub fn set_replicas_if_changed(&mut self, replicas: u16) -> Result<bool, PortRequestError> {
        if self.spec.deployment.replicas == replicas {
            return Ok(false);
        }
        let mut manifest = self.manifest();
        manifest.spec.replicas = replicas;
        manifest.validate().map_err(PortRequestError::Manifest)?;
        self.spec.deployment.replicas = replicas;
        self.metadata.generation = self.metadata.generation.saturating_add(1);
        Ok(true)
    }

    pub const fn mark_observed_generation_if_changed(&mut self) -> bool {
        if self.status.observed_generation == self.metadata.generation {
            return false;
        }
        self.status.observed_generation = self.metadata.generation;
        true
    }

    #[must_use]
    pub fn manifest(&self) -> AgentManifest {
        AgentManifest {
            api_version: AGENTDP_API_VERSION.to_owned(),
            kind: AgentManifestKind::Agent,
            metadata: AgentMetadata {
                name: self.metadata.name.to_string(),
            },
            spec: self.spec.deployment.clone(),
        }
    }

    pub const fn refresh_status_projection(&mut self, deleted: bool) {
        self.status.phase = agent_status_phase(self.spec.deployment.phase, self.metadata.deletion_requested, deleted);
        self.status.deleted = deleted;
        self.status.replicas.desired = self.spec.deployment.replicas;
        self.status.reconciling = self.status.observed_generation != self.metadata.generation;
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum AgentDocumentKind {
    Agent,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentDocumentMetadata {
    pub name: AgentName,
    pub generation: u64,
    #[serde(rename = "deletionRequested")]
    pub deletion_requested: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentDocumentSpec {
    #[serde(rename = "sourceManifest")]
    pub source_manifest: String,
    #[serde(flatten)]
    pub deployment: AgentDeploymentSpec,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceDocument {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: AgentInstanceDocumentKind,
    pub metadata: AgentInstanceMetadata,
    pub spec: AgentInstanceSpec,
    pub status: AgentInstanceStatus,
}

impl AgentInstanceDocument {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        agent: AgentName,
        id: AgentInstanceId,
        instance: InstanceName,
        desired_generation: u64,
        agent_base: AgentBaseKey,
        template: AgentSpec,
        target: AgentInstanceTarget,
        phase: AgentInstancePhase,
        network: NetworkState,
        healthchecks: Vec<crate::provisioning::bootstrap::HealthcheckPlan>,
        guest_access: Option<GuestAccessState>,
        backend: BackendState,
    ) -> Self {
        let now = agentdp_platform::time::rfc3339_utc_now();
        Self {
            api_version: AGENTDP_API_VERSION.to_owned(),
            kind: AgentInstanceDocumentKind::AgentInstance,
            metadata: AgentInstanceMetadata {
                agent,
                id,
                name: instance,
            },
            spec: AgentInstanceSpec {
                desired_generation,
                agent_base,
                template,
                target,
            },
            status: AgentInstanceStatus {
                phase,
                observed_generation: 0,
                created_at: now,
                ready_at: None,
                bootstrap: None,
                network,
                healthchecks,
                guest_access,
                readiness: None,
                work: super::AgentInstanceWorkStatus::default(),
                reconciliation: None,
                tailscale_serve: None,
                backend,
            },
        }
    }

    #[must_use]
    pub fn name(&self) -> String {
        format!("{}/{}", self.metadata.agent, self.metadata.name)
    }
}

impl<'de> Deserialize<'de> for AgentInstanceDocument {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            #[serde(rename = "apiVersion")]
            api_version: String,
            kind: AgentInstanceDocumentKind,
            metadata: AgentInstanceMetadata,
            spec: AgentInstanceSpec,
            status: AgentInstanceStatus,
        }

        let fields = Fields::deserialize(deserializer)?;
        if fields.api_version != AGENTDP_API_VERSION {
            return Err(D::Error::custom(format!(
                "unsupported apiVersion `{}` for AgentInstance",
                fields.api_version
            )));
        }
        Ok(Self {
            api_version: fields.api_version,
            kind: fields.kind,
            metadata: fields.metadata,
            spec: fields.spec,
            status: fields.status,
        })
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum AgentInstanceDocumentKind {
    AgentInstance,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceMetadata {
    pub agent: AgentName,
    pub id: AgentInstanceId,
    pub name: InstanceName,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceSpec {
    #[serde(rename = "desiredGeneration")]
    pub desired_generation: u64,
    #[serde(rename = "agentBase")]
    pub agent_base: AgentBaseKey,
    pub template: AgentSpec,
    pub target: AgentInstanceTarget,
}

/// Assigns runtime host/guest port mappings from an Agent manifest.
///
/// # Errors
///
/// Returns an error when the per-instance host port plan overflows.
pub fn assign_port_mappings(
    manifest: &AgentManifest,
    instance_id: AgentInstanceId,
) -> Result<BTreeMap<String, PortMappingState>, PortRequestError> {
    manifest.validate().map_err(PortRequestError::Manifest)?;
    manifest
        .spec
        .network
        .ports
        .iter()
        .map(|(name, port)| {
            Ok((
                name.clone(),
                PortMappingState {
                    guest: port.guest,
                    host: assigned_host_port(name, port.host, instance_id)?,
                    protocol: port_protocol_state(port.protocol),
                },
            ))
        })
        .collect()
}

fn assigned_host_port(
    name: &str,
    host: Option<u16>,
    instance_id: AgentInstanceId,
) -> Result<Option<u16>, PortRequestError> {
    let Some(host) = host else {
        return Ok(None);
    };
    let assigned =
        u32::from(host)
            .checked_add(instance_id.as_u32())
            .ok_or_else(|| PortRequestError::HostPortOverflow {
                name: name.to_owned(),
                host,
                instance: instance_id,
            })?;
    u16::try_from(assigned)
        .map(Some)
        .map_err(|_| PortRequestError::HostPortOverflow {
            name: name.to_owned(),
            host,
            instance: instance_id,
        })
}

#[must_use]
pub const fn port_protocol_state(protocol: NetworkProtocol) -> PortProtocolState {
    match protocol {
        NetworkProtocol::Tcp => PortProtocolState::Tcp,
        NetworkProtocol::Udp => PortProtocolState::Udp,
    }
}

#[must_use]
pub const fn agent_status_phase(phase: AgentPhase, deleting: bool, deleted: bool) -> AgentStatusPhase {
    if deleted {
        return AgentStatusPhase::Deleted;
    }
    if deleting {
        return AgentStatusPhase::Deleting;
    }
    match phase {
        AgentPhase::Running => AgentStatusPhase::Running,
        AgentPhase::Paused => AgentStatusPhase::Paused,
    }
}
