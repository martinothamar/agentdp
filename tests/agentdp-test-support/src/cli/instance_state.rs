use std::fs;
use std::path::Path;

use agentdp_core::agent::{AgentInstanceDocument, BackendState, QemuState};

pub const fn qemu(document: &AgentInstanceDocument) -> &QemuState {
    match &document.status.backend {
        BackendState::Qemu(state) => state,
    }
}

pub const fn qemu_mut(document: &mut AgentInstanceDocument) -> &mut QemuState {
    match &mut document.status.backend {
        BackendState::Qemu(state) => state,
    }
}

pub fn read(path: &Path) -> AgentInstanceDocument {
    serde_yaml::from_str(&fs::read_to_string(path).expect("read instance state")).expect("parse instance state")
}

pub fn write(path: &Path, document: &AgentInstanceDocument) {
    fs::write(path, serde_yaml::to_string(document).expect("serialize instance state")).expect("write instance state");
}
