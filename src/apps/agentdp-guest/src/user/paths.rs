use std::path::PathBuf;

use crate::{Error, Result};

const SOCKET_NAME: &str = "guestd.sock";

#[derive(Debug, Clone)]
pub(crate) struct RuntimePaths {
    pub socket_dir: PathBuf,
    pub socket: PathBuf,
    pub state_dir: PathBuf,
    pub registry: PathBuf,
    pub seen: PathBuf,
    pub queue_dir: PathBuf,
    pub pane_file: PathBuf,
    pub code_dir: PathBuf,
}

impl RuntimePaths {
    pub(crate) fn discover() -> Result<Self> {
        let runtime_dir = env_path("XDG_RUNTIME_DIR")
            .ok_or_else(|| Error::Message("XDG_RUNTIME_DIR must be set for agentdp guest tooling".to_owned()))?;
        let home = env_path("HOME").ok_or_else(|| Error::Message("HOME must be set".to_owned()))?;
        let state_dir = env_path("AGENTDP_STATE_DIR")
            .or_else(|| env_path("XDG_STATE_HOME").map(|path| path.join("agentdp")))
            .unwrap_or_else(|| home.join(".local/state/agentdp"));
        let socket_dir = runtime_dir.join("agentdp");
        let code_dir = env_path("AGENTDP_CODE_DIR").unwrap_or_else(|| home.join("code"));
        Ok(Self {
            socket: socket_dir.join(SOCKET_NAME),
            socket_dir,
            registry: env_path("AGENTDP_PR_REGISTRY").unwrap_or_else(|| state_dir.join("pr-watch.json")),
            seen: state_dir.join("pr-subscriber-seen.json"),
            queue_dir: state_dir.join("pr-subscriber-queue"),
            pane_file: env_path("AGENTDP_CODEX_PANE_FILE").unwrap_or_else(|| state_dir.join("codex-pane-id")),
            state_dir,
            code_dir,
        })
    }
}

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}
