use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use agentdp_protocol::ReadinessResult;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeState {
    pub version: u16,
    pub manifest_name: String,
    pub instance: String,
    pub status: String,
    pub manifest: ManifestState,
    pub network: NetworkState,
    pub guest_access: Option<GuestAccessState>,
    pub readiness: Option<ReadinessState>,
    pub backend: BackendState,
}

impl RuntimeState {
    pub const fn qemu(&self) -> &QemuState {
        match &self.backend {
            BackendState::Qemu(state) => state,
        }
    }

    pub const fn qemu_mut(&mut self) -> &mut QemuState {
        match &mut self.backend {
            BackendState::Qemu(state) => state,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ManifestState {
    pub source: String,
    pub copy: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NetworkState {
    pub mode: String,
    pub ports: BTreeMap<String, PortMappingState>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PortMappingState {
    pub guest: u16,
    pub host: u16,
    pub protocol: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GuestAccessState {
    #[serde(rename = "ssh_user")]
    pub user: String,
    #[serde(rename = "ssh_private_key")]
    pub private_key: String,
    #[serde(rename = "ssh_public_key")]
    pub public_key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReadinessState {
    pub ready: bool,
    pub last_success_unix_seconds: u64,
    pub result: ReadinessResult,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind")]
pub enum BackendState {
    #[serde(rename = "qemu")]
    Qemu(QemuState),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QemuState {
    pub image: ImageState,
    pub disk: String,
    pub work_dir: String,
    pub seed_media: String,
    pub seed_meta_data: String,
    pub seed_user_data: String,
    pub bootstrap_script: String,
    pub monitor_socket: String,
    pub qmp_socket: String,
    pub pid_file: String,
    pub serial_log: String,
    pub qemu_log: String,
    pub pid: Option<u32>,
    pub last_start_unix_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ImageState {
    pub os: String,
    pub architecture: String,
    pub variant: String,
    pub source_url: String,
    pub cache_key: String,
    pub cache_path: String,
    pub download_path: String,
    pub format: String,
}

pub fn read(path: &Path) -> RuntimeState {
    serde_json::from_str(&fs::read_to_string(path).expect("read runtime")).expect("parse runtime")
}

pub fn write(path: &Path, state: &RuntimeState) {
    fs::write(path, serde_json::to_string_pretty(state).expect("serialize runtime")).expect("write runtime");
}
