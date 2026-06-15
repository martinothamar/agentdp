use std::path::Path;

use agentdp_core::Context;
use agentdp_core::manifest::{self, AgentManifest, LoadedAgentManifest};
use thiserror::Error;

use super::{AgentName, AgentdpLayout, IdentityError};

#[derive(Debug, Error)]
pub(crate) enum AgentContextError {
    #[error("{0}")]
    Identity(#[from] IdentityError),
    #[error("{0}")]
    Layout(#[from] super::layout::Error),
    #[error("{0}")]
    Manifest(#[from] manifest::Error),
}

#[derive(Debug, Clone)]
pub(crate) struct AgentManifestContext {
    loaded: LoadedAgentManifest,
    agent: AgentName,
}

impl AgentManifestContext {
    pub(crate) async fn load(
        context: &Context,
        layout: &AgentdpLayout,
        source_path: &Path,
    ) -> Result<Self, AgentContextError> {
        let loaded = LoadedAgentManifest::load(context, source_path).await?;
        Self::from_loaded(layout, loaded).await
    }

    async fn from_loaded(layout: &AgentdpLayout, loaded: LoadedAgentManifest) -> Result<Self, AgentContextError> {
        let agent = AgentName::parse(loaded.agent_name())?;
        let _exists = layout.agent_exists(&agent).await?;
        Ok(Self::new(loaded, agent))
    }

    pub(crate) fn from_existing_value(source_path: &Path, value: AgentManifest) -> Result<Self, AgentContextError> {
        let loaded = LoadedAgentManifest::from_value(source_path, value)?;
        let agent = AgentName::parse(loaded.agent_name())?;
        Ok(Self::new(loaded, agent))
    }

    const fn new(loaded: LoadedAgentManifest, agent: AgentName) -> Self {
        Self { loaded, agent }
    }

    #[must_use]
    pub(crate) fn source_path(&self) -> &Path {
        self.loaded.source_path()
    }

    #[must_use]
    pub(crate) const fn agent(&self) -> &AgentName {
        &self.agent
    }

    #[must_use]
    pub(crate) const fn value(&self) -> &AgentManifest {
        self.loaded.value()
    }
}
