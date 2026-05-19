use std::collections::BTreeMap;

use agentdp_core::backend::BackendKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PingResult {
    pub service: String,
    pub pid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ShutdownResult {
    pub shutdown: bool,
    pub pid: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ServerDoctorResult {
    pub backend: BackendKind,
    pub checks: Vec<DoctorCheckResult>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DoctorCheckResult {
    pub name: String,
    pub status: String,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProvisioningPlanResult {
    pub manifest: String,
    pub name: String,
    pub instance: String,
    pub image: ProvisioningImageResult,
    pub backend: BackendProvisioningResult,
    pub work_dir: String,
    pub seed: SeedResult,
    pub guest_access: GuestAccessResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProvisioningImageResult {
    pub os: String,
    pub architecture: String,
    pub variant: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum BackendProvisioningResult {
    #[serde(rename = "qemu")]
    Qemu(QemuProvisioningResult),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct QemuProvisioningResult {
    pub image: QemuImageResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct QemuImageResult {
    pub url: String,
    pub cache_key: String,
    pub format: String,
    pub cache_path: String,
    pub download_path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SeedResult {
    pub directory: String,
    pub meta_data: String,
    pub user_data: String,
    pub bootstrap_script: String,
    pub media: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceCreateResult {
    pub name: String,
    pub manifest: ManifestResult,
    pub state: String,
    pub backend: BackendCreateResult,
    pub network: NetworkResult,
    pub guest_access: GuestAccessResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceCloneResult {
    pub source: String,
    pub name: String,
    pub manifest: ManifestResult,
    pub state: String,
    pub backend: BackendCreateResult,
    pub network: NetworkResult,
    pub guest_access: GuestAccessResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ManifestResult {
    pub source: String,
    pub copy: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ImageResult {
    pub cache_path: String,
    pub download_path: String,
    pub source_url: String,
    pub format: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum BackendCreateResult {
    #[serde(rename = "qemu")]
    Qemu(QemuCreateResult),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct QemuCreateResult {
    pub image: ImageResult,
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
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuestAccessResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_private_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_public_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceUpResult {
    pub name: String,
    pub state: String,
    pub process: ProcessResult,
    pub readiness: ReadinessResult,
    pub backend: BackendRuntimeResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum BackendRuntimeResult {
    #[serde(rename = "qemu")]
    Qemu(QemuRuntimeResult),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct QemuRuntimeResult {
    pub monitor_socket: String,
    pub qmp_socket: String,
    pub pid_file: String,
    pub serial_log: String,
    pub qemu_log: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceDownResult {
    pub name: String,
    pub state: String,
    pub status: String,
    pub previous_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminated_pid: Option<u32>,
    pub process: ProcessResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceRmResult {
    pub name: String,
    pub removed: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceLogsResult {
    pub name: String,
    pub file: String,
    pub path: String,
    pub lines: usize,
    pub contents: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceExecResult {
    pub name: String,
    pub command: Vec<String>,
    pub exit_status: u64,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstancePsResult {
    pub instances: Vec<InstanceListItem>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceShellResult {
    pub name: String,
    pub command: HostCommandResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct HostCommandResult {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceListItem {
    pub name: String,
    pub manifest_name: String,
    pub instance: String,
    pub status: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceStatusResult {
    pub name: String,
    pub state: String,
    pub status: String,
    pub stale: bool,
    pub process: ProcessResult,
    pub backend: BackendStatusResult,
    pub network: NetworkResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<ReadinessStateResult>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum BackendStatusResult {
    #[serde(rename = "qemu")]
    Qemu(QemuStatusResult),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProcessResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct QemuStatusResult {
    pub disk: String,
    pub seed_media: String,
    pub pid_file: String,
    pub monitor_socket: String,
    pub qmp_socket: String,
    pub serial_log: String,
    pub qemu_log: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct NetworkResult {
    pub mode: String,
    pub ports: BTreeMap<String, PortMappingResult>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PortMappingResult {
    pub guest: u16,
    pub host: u16,
    pub protocol: PortProtocolResult,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PortProtocolResult {
    Tcp,
    Udp,
}

impl PortProtocolResult {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReadinessStateResult {
    pub ready: bool,
    pub last_success_unix_seconds: u64,
    pub result: ReadinessResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReadinessResult {
    pub ready: bool,
    pub services: BTreeMap<String, ServiceResult>,
    pub healthchecks: Vec<HealthcheckResult>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ServiceResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub host_port: u16,
    pub guest_port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct HealthcheckResult {
    pub name: String,
    pub kind: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_status: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default)]
    pub elapsed_ms: u128,
}
