use std::collections::BTreeMap;
use std::path::Path;

use agentdp_core::Context;
use agentdp_core::agent::{
    AGENTDP_API_VERSION, AgentBaseKey, AgentInstanceDocument, AgentInstanceId, AgentInstancePhase, AgentInstanceTarget,
    AgentName, BackendState, BootstrapEvent, EventLevel, InstanceName, NetworkAllowState, NetworkIpv6State,
    NetworkModeState, NetworkState,
};
use agentdp_core::manifest::AgentManifest;
use agentdp_core::provisioning::bootstrap::RenderedBootstrapPlan;
use agentdp_core::provisioning::{ProvisioningOptions, ProvisioningPlan};
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use crate::agent::{AgentManifestContext, AgentdpLayout};
use crate::backend;
use crate::host::collect_guest_tool_seeds;

use super::runtime::Error;

#[derive(Debug, Clone)]
pub(super) struct AgentBasePreparation {
    pub(super) manifest: AgentManifestContext,
    pub(super) key: AgentBaseKey,
    pub(super) provisioning_plan: ProvisioningPlan,
    pub(super) rendered_bootstrap: RenderedBootstrapPlan,
}

impl AgentBasePreparation {
    pub(super) async fn from_document(
        context: &Context,
        document: agentdp_core::agent::AgentDocument,
    ) -> Result<Self, Error> {
        let manifest = AgentManifestContext::from_existing_value(&document.source_manifest(), document.manifest())?;
        let backend = backend::resolve_for_manifest(manifest.value())?;
        let provisioning_plan = ProvisioningPlan::from_manifest(
            manifest.value(),
            &ProvisioningOptions {
                hostname: Some(format!("{}-base", manifest.agent())),
            },
        );
        let rendered_bootstrap = provisioning_plan.render_base_bootstrap(manifest.value())?;
        let key = agent_base_key(context, backend.as_ref(), manifest.value(), &rendered_bootstrap).await?;
        Ok(Self {
            manifest,
            key,
            provisioning_plan,
            rendered_bootstrap,
        })
    }

    pub(super) const fn key(&self) -> &AgentBaseKey {
        &self.key
    }
}

async fn agent_base_key(
    context: &Context,
    backend: &dyn backend::Backend,
    manifest: &AgentManifest,
    rendered_bootstrap: &RenderedBootstrapPlan,
) -> Result<AgentBaseKey, Error> {
    let image = backend.base_image_identity(manifest).await?;
    let bootstrap_key_material =
        serde_json::to_vec(&rendered_bootstrap.steps).map_err(Error::SerializeBaseKeyMaterial)?;
    let guest_tools = collect_guest_tool_seeds(context, manifest).await?;
    let mut hasher = sha2::Sha256::new();
    for value in [
        image.base_key_schema,
        image.os,
        image.architecture,
        image.variant,
        image.cache_key,
        image.url,
        image.format,
        manifest.spec.resources.storage.as_str(),
    ] {
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    hasher.update(bootstrap_key_material);
    hasher.update([0]);
    for tool in guest_tools {
        for value in [
            tool.path.as_str(),
            tool.permissions.as_str(),
            tool.owner.as_deref().unwrap_or(""),
        ] {
            hasher.update(value.as_bytes());
            hasher.update([0]);
        }
        hasher.update(&tool.contents);
        hasher.update([0]);
    }
    Ok(AgentBaseKey::new(format!("sha256-{}", hex(&hasher.finalize()))))
}

pub(super) async fn ensure_agent_base_ready(
    context: &Context,
    layout: &AgentdpLayout,
    agent: &AgentName,
    preparation: &AgentBasePreparation,
    backend: &dyn backend::Backend,
    events: &mut dyn backend::BootstrapEventSink,
) -> Result<(), Error> {
    let key = preparation.key();
    let base_layout = layout.agent_base(agent, key);
    let files = base_layout.files();
    if reuse_ready_agent_base(key, &files).await? {
        return Ok(());
    }

    create_agent_base_directories(&files).await?;
    let instance = InstanceName::new(super::AGENT_BASE_INSTANCE);
    let output = backend
        .create_base(
            context,
            backend::CreateBaseInput {
                manifest: preparation.manifest.clone(),
                instance: &instance,
                provisioning_plan: &preparation.provisioning_plan,
                rendered_bootstrap: &preparation.rendered_bootstrap,
                image_cache_dir: &layout.image_cache_dir(),
                files: &files,
            },
        )
        .await?;
    let mut runtime = base_bootstrap_runtime_document(agent, instance, key, &preparation.manifest, output.state);
    let mut control = None;
    runtime.status.phase = AgentInstancePhase::Starting;
    events.emit(BootstrapEvent::Diagnostic {
        level: EventLevel::Info,
        message: "starting agent base bootstrap VM".to_owned(),
    });
    backend.start_base(context, &preparation.manifest, &mut runtime).await?;
    runtime.status.phase = AgentInstancePhase::Running;

    events.emit(BootstrapEvent::Diagnostic {
        level: EventLevel::Info,
        message: "waiting for agent base bootstrap".to_owned(),
    });
    let bootstrap = backend
        .wait_bootstrap(context, &runtime, &mut control, None, Some(events))
        .await;
    runtime.status.phase = AgentInstancePhase::Stopping;
    events.emit(BootstrapEvent::Diagnostic {
        level: EventLevel::Info,
        message: "stopping agent base bootstrap VM".to_owned(),
    });
    let shutdown = backend.stop_base(context, &mut runtime, &mut control).await;
    runtime.status.phase = AgentInstancePhase::Stopped;
    match bootstrap? {
        backend::BootstrapOutcome::Passed { .. } => {}
        backend::BootstrapOutcome::Failed { error, .. } => {
            return Err(Error::AgentBaseBootstrapFailed { error });
        }
    }
    shutdown?;

    let now = agentdp_platform::time::rfc3339_utc_now();
    let document = AgentBaseDiskDocument::ready(agent.clone(), key.clone(), output.image_cache_key, now);
    write_agent_base_document(&files.document, &document).await
}

async fn reuse_ready_agent_base(key: &AgentBaseKey, files: &crate::agent::AgentBaseFiles) -> Result<bool, Error> {
    if !tokio::fs::try_exists(&files.document)
        .await
        .map_err(|source| Error::InspectAgentBaseDisk {
            path: files.document.clone(),
            source,
        })?
    {
        return Ok(false);
    }

    let mut document = read_agent_base_document(&files.document).await?;
    if document.phase() != AgentBaseDiskPhase::Ready || document.key() != key {
        return Err(Error::AgentBaseNotReady {
            key: key.clone(),
            phase: document.phase(),
        });
    }
    let exists = tokio::fs::try_exists(&files.disk)
        .await
        .map_err(|source| Error::InspectAgentBaseDisk {
            path: files.disk.clone(),
            source,
        })?;
    if !exists {
        return Ok(false);
    }
    document.mark_used(agentdp_platform::time::rfc3339_utc_now());
    write_agent_base_document(&files.document, &document).await?;
    Ok(true)
}

async fn create_agent_base_directories(files: &crate::agent::AgentBaseFiles) -> Result<(), Error> {
    for path in [&files.base_dir, &files.logs_dir, &files.run_dir, &files.seed_dir] {
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|source| Error::CreateAgentBaseDirectory {
                path: path.clone(),
                source,
            })?;
    }
    Ok(())
}

fn base_bootstrap_runtime_document(
    agent: &AgentName,
    instance: InstanceName,
    key: &AgentBaseKey,
    manifest: &AgentManifestContext,
    backend: BackendState,
) -> AgentInstanceDocument {
    AgentInstanceDocument::new(
        agent.clone(),
        AgentInstanceId::new(0),
        instance,
        1,
        key.clone(),
        manifest.value().spec.template.clone(),
        AgentInstanceTarget::Active,
        AgentInstancePhase::Materialized,
        NetworkState {
            mode: NetworkModeState::User,
            allow: NetworkAllowState::default(),
            ipv6: NetworkIpv6State { enabled: false },
            ports: BTreeMap::new(),
            runtime: None,
        },
        Vec::new(),
        None,
        backend,
    )
}

fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(TABLE[usize::from(byte >> 4)] as char);
        output.push(TABLE[usize::from(byte & 0x0f)] as char);
    }
    output
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AgentBaseDiskPhase {
    Ready,
    Failed,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AgentBaseDiskDocument {
    #[serde(rename = "apiVersion")]
    api_version: String,
    kind: AgentBaseDiskDocumentKind,
    metadata: AgentBaseDiskMetadata,
    spec: AgentBaseDiskSpec,
    status: AgentBaseDiskStatus,
}

impl AgentBaseDiskDocument {
    fn ready(agent: AgentName, key: AgentBaseKey, image_cache_key: String, timestamp: String) -> Self {
        Self {
            api_version: AGENTDP_API_VERSION.to_owned(),
            kind: AgentBaseDiskDocumentKind::AgentBase,
            metadata: AgentBaseDiskMetadata { agent, key },
            spec: AgentBaseDiskSpec { image_cache_key },
            status: AgentBaseDiskStatus {
                phase: AgentBaseDiskPhase::Ready,
                created_at: timestamp.clone(),
                ready_at: Some(timestamp.clone()),
                last_used_at: Some(timestamp),
            },
        }
    }

    const fn key(&self) -> &AgentBaseKey {
        &self.metadata.key
    }

    const fn phase(&self) -> AgentBaseDiskPhase {
        self.status.phase
    }

    fn mark_used(&mut self, timestamp: String) {
        self.status.last_used_at = Some(timestamp);
    }
}

impl<'de> Deserialize<'de> for AgentBaseDiskDocument {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            #[serde(rename = "apiVersion")]
            api_version: String,
            kind: AgentBaseDiskDocumentKind,
            metadata: AgentBaseDiskMetadata,
            spec: AgentBaseDiskSpec,
            status: AgentBaseDiskStatus,
        }

        let fields = Fields::deserialize(deserializer)?;
        if fields.api_version != AGENTDP_API_VERSION {
            return Err(serde::de::Error::custom(format!(
                "unsupported apiVersion `{}` for AgentBase",
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
enum AgentBaseDiskDocumentKind {
    AgentBase,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AgentBaseDiskMetadata {
    agent: AgentName,
    key: AgentBaseKey,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AgentBaseDiskSpec {
    #[serde(rename = "imageCacheKey")]
    image_cache_key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AgentBaseDiskStatus {
    phase: AgentBaseDiskPhase,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "readyAt")]
    ready_at: Option<String>,
    #[serde(rename = "lastUsedAt")]
    last_used_at: Option<String>,
}

async fn read_agent_base_document(path: &Path) -> Result<AgentBaseDiskDocument, Error> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .map_err(|source| Error::ReadAgentBaseDocument {
            path: path.to_path_buf(),
            source,
        })?;
    serde_yaml::from_str(&contents).map_err(|source| Error::ParseAgentBaseDocument {
        path: path.to_path_buf(),
        source,
    })
}

async fn write_agent_base_document(path: &Path, document: &AgentBaseDiskDocument) -> Result<(), Error> {
    let contents = serde_yaml::to_string(document).map_err(Error::SerializeAgentBaseDocument)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| Error::CreateAgentBaseDirectory {
                path: parent.to_path_buf(),
                source,
            })?;
    }
    if tokio::fs::read_to_string(path)
        .await
        .is_ok_and(|existing| existing == contents)
    {
        return Ok(());
    }
    tokio::fs::write(path, contents)
        .await
        .map_err(|source| Error::WriteAgentBaseDocument {
            path: path.to_path_buf(),
            source,
        })
}
