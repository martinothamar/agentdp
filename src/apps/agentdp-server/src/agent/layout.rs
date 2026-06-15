use std::path::{Path, PathBuf};

use agentdp_core::layout;
use thiserror::Error;

use super::{AgentBaseKey, AgentInstanceId, AgentName};

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Core(#[from] layout::Error),
    #[error("failed to read agent directory {path}: {source}")]
    ReadDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to inspect agent path {path}: {source}")]
    InspectPath {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct AgentdpLayout {
    inner: layout::AgentdpLayout,
}

impl AgentdpLayout {
    pub(crate) fn resolve() -> Result<Self, Error> {
        Ok(Self {
            inner: layout::AgentdpLayout::resolve()?,
        })
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn from_root(root: impl Into<PathBuf>) -> Self {
        Self {
            inner: layout::AgentdpLayout::from_root(root),
        }
    }

    #[must_use]
    pub(crate) fn socket_path(&self) -> PathBuf {
        self.inner.socket_path()
    }

    #[must_use]
    pub(crate) fn server_log(&self) -> PathBuf {
        self.inner.server_log()
    }

    #[must_use]
    pub(crate) fn config_dir(&self) -> PathBuf {
        self.inner.config_dir()
    }

    #[must_use]
    pub(crate) fn image_cache_dir(&self) -> PathBuf {
        self.inner.image_cache_dir()
    }

    #[must_use]
    pub(crate) fn agents_dir(&self) -> PathBuf {
        self.inner.agents_dir()
    }

    #[must_use]
    pub(crate) fn instance(&self, agent: &AgentName, id: AgentInstanceId) -> AgentInstanceLayout {
        self.agent(agent).instance(id)
    }

    #[must_use]
    pub(crate) fn agent_base(&self, agent: &AgentName, key: &AgentBaseKey) -> AgentBaseLayout {
        self.agent(agent).base(key)
    }

    #[must_use]
    pub(crate) fn agent_document(&self, agent: &AgentName) -> PathBuf {
        self.agent(agent).document()
    }

    #[must_use]
    pub(crate) fn agent_events(&self, agent: &AgentName) -> PathBuf {
        self.agent(agent).events()
    }

    pub(crate) async fn agent_exists(&self, agent: &AgentName) -> Result<bool, Error> {
        path_exists(&self.agent(agent).document()).await
    }

    #[must_use]
    fn agent(&self, agent: &AgentName) -> AgentLayout {
        AgentLayout {
            root: self.agents_dir().join(agent.as_str()),
        }
    }

    pub(crate) async fn deployed_agents(&self) -> Result<Vec<AgentName>, Error> {
        let root = self.agents_dir();
        if !path_exists(&root).await? {
            return Ok(Vec::new());
        }

        let mut agents = Vec::new();
        let mut entries = read_dir(&root).await?;
        while let Some(entry) = entries.next_entry().await.map_err(|source| Error::ReadDirectory {
            path: root.clone(),
            source,
        })? {
            let agent_dir = entry.path();
            if !entry
                .file_type()
                .await
                .map_err(|source| Error::InspectPath {
                    path: agent_dir.clone(),
                    source,
                })?
                .is_dir()
            {
                continue;
            }
            let Some(agent) = entry.file_name().to_str().map(AgentName::new) else {
                continue;
            };
            if path_exists(&self.agent(&agent).document()).await? {
                agents.push(agent);
            }
        }
        Ok(agents)
    }

    pub(crate) async fn instance_layouts(
        &self,
        agent: &AgentName,
    ) -> Result<Vec<(AgentInstanceId, AgentInstanceLayout)>, Error> {
        let root = self.agent(agent).instances_dir();
        if !path_exists(&root).await? {
            return Ok(Vec::new());
        }

        let mut instances = Vec::new();
        let mut entries = read_dir(&root).await?;
        while let Some(entry) = entries.next_entry().await.map_err(|source| Error::ReadDirectory {
            path: root.clone(),
            source,
        })? {
            let instance_dir = entry.path();
            if !entry
                .file_type()
                .await
                .map_err(|source| Error::InspectPath {
                    path: instance_dir.clone(),
                    source,
                })?
                .is_dir()
            {
                continue;
            }
            let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|value| AgentInstanceId::parse(value).ok())
            else {
                continue;
            };
            let layout = self.instance(agent, id);
            if path_exists(&layout.instance_state()).await? {
                instances.push((id, layout));
            }
        }
        Ok(instances)
    }
}

#[derive(Debug, Clone)]
struct AgentLayout {
    root: PathBuf,
}

impl AgentLayout {
    fn document(&self) -> PathBuf {
        self.root.join("agent.yaml")
    }

    fn events(&self) -> PathBuf {
        self.root.join("events.jsonl")
    }

    fn instances_dir(&self) -> PathBuf {
        self.root.join("instances")
    }

    fn bases_dir(&self) -> PathBuf {
        self.root.join("bases")
    }

    fn base(&self, key: &AgentBaseKey) -> AgentBaseLayout {
        AgentBaseLayout {
            key: key.clone(),
            root: self.bases_dir().join(key.as_str()),
        }
    }

    fn instance(&self, id: AgentInstanceId) -> AgentInstanceLayout {
        AgentInstanceLayout {
            agent_document: self.document(),
            root: self.instances_dir().join(id.path_component()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AgentBaseLayout {
    key: AgentBaseKey,
    root: PathBuf,
}

impl AgentBaseLayout {
    #[must_use]
    pub(crate) fn files(&self) -> AgentBaseFiles {
        AgentBaseFiles {
            base_dir: self.root.clone(),
            document: self.document(),
            disk: self.disk(),
            logs_dir: self.logs_dir(),
            run_dir: self.run_dir(),
            seed_dir: self.seed_dir(),
            seed_media: self.seed_media(),
        }
    }

    #[must_use]
    pub(crate) fn document(&self) -> PathBuf {
        self.root.join("agent-base.yaml")
    }

    #[must_use]
    pub(crate) fn disk(&self) -> PathBuf {
        self.root.join("disk.qcow2")
    }

    #[must_use]
    pub(crate) fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    #[must_use]
    pub(crate) fn run_dir(&self) -> PathBuf {
        let agent_root = self.root.parent().and_then(Path::parent).unwrap_or(&self.root);
        agent_root.join(".run").join("b").join(short_agent_base_key(&self.key))
    }

    #[must_use]
    pub(crate) fn seed_dir(&self) -> PathBuf {
        self.root.join("seed")
    }

    #[must_use]
    pub(crate) fn seed_media(&self) -> PathBuf {
        self.root.join("seed.img")
    }
}

fn short_agent_base_key(key: &AgentBaseKey) -> String {
    let digest = key.as_str().strip_prefix("sha256-").unwrap_or_else(|| key.as_str());
    digest.chars().take(12).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentBaseFiles {
    pub base_dir: PathBuf,
    pub document: PathBuf,
    pub disk: PathBuf,
    pub logs_dir: PathBuf,
    pub run_dir: PathBuf,
    pub seed_dir: PathBuf,
    pub seed_media: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentInstanceFiles {
    pub instance_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub run_dir: PathBuf,
    pub seed_dir: PathBuf,
    pub seed_media: PathBuf,
    pub disk: PathBuf,
    pub agent_document: PathBuf,
    pub state: PathBuf,
    pub events: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct AgentInstanceLayout {
    agent_document: PathBuf,
    root: PathBuf,
}

impl AgentInstanceLayout {
    #[must_use]
    pub(crate) fn files(&self) -> AgentInstanceFiles {
        AgentInstanceFiles {
            instance_dir: self.root.clone(),
            logs_dir: self.logs_dir(),
            run_dir: self.run_dir(),
            seed_dir: self.seed_dir(),
            seed_media: self.seed_media(),
            disk: self.disk(),
            agent_document: self.agent_document(),
            state: self.instance_state(),
            events: self.instance_events(),
        }
    }

    #[must_use]
    pub(crate) fn disk(&self) -> PathBuf {
        self.root.join("disk.qcow2")
    }

    #[must_use]
    pub(crate) fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    #[must_use]
    pub(crate) fn run_dir(&self) -> PathBuf {
        self.root.join("run")
    }

    #[must_use]
    pub(crate) fn seed_dir(&self) -> PathBuf {
        self.root.join("seed")
    }

    #[must_use]
    pub(crate) fn seed_media(&self) -> PathBuf {
        self.root.join("seed.img")
    }

    #[must_use]
    pub(crate) fn agent_document(&self) -> PathBuf {
        self.agent_document.clone()
    }

    #[must_use]
    pub(crate) fn instance_state(&self) -> PathBuf {
        self.root.join("instance.yaml")
    }

    #[must_use]
    pub(crate) fn instance_events(&self) -> PathBuf {
        self.root.join("events.jsonl")
    }
}

async fn path_exists(path: &Path) -> Result<bool, Error> {
    tokio::fs::try_exists(path).await.map_err(|source| Error::InspectPath {
        path: path.to_path_buf(),
        source,
    })
}

async fn read_dir(path: &Path) -> Result<tokio::fs::ReadDir, Error> {
    tokio::fs::read_dir(path).await.map_err(|source| Error::ReadDirectory {
        path: path.to_path_buf(),
        source,
    })
}
