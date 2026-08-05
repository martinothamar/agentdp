use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::manifest::{AgentSpec, NetworkAllow, NetworkIpv6};
use crate::provisioning::bootstrap::HealthcheckPlan;
use crate::provisioning::secrets::SecretBindings;
use agentdp_protocol::server_guest::{BootstrapLifecycleStatus, BootstrapStepPhase};

use super::{AgentBaseKey, AgentInstanceId};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentStatus {
    #[serde(rename = "observedGeneration")]
    pub observed_generation: u64,
    pub phase: AgentStatusPhase,
    pub replicas: ReplicaStatus,
    pub reconciling: bool,
    pub deleted: bool,
    #[serde(rename = "agentBase")]
    pub agent_base: AgentBaseStatus,
    pub instances: BTreeMap<AgentInstanceId, AgentInstanceStatus>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum AgentStatusPhase {
    Running,
    Paused,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicaStatus {
    pub desired: u16,
    pub ready: u16,
    pub active: u16,
    pub stopped: u16,
    pub deleting: u16,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentBaseStatus {
    #[serde(rename = "desiredKey")]
    pub desired_key: Option<AgentBaseKey>,
    #[serde(rename = "readyKey")]
    pub ready_key: Option<AgentBaseKey>,
    pub phase: AgentBasePhase,
    pub message: Option<String>,
    #[serde(rename = "lastError")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentBasePhase {
    #[default]
    Missing,
    Building,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceStatus {
    pub phase: AgentInstancePhase,
    #[serde(rename = "observedGeneration")]
    pub observed_generation: u64,
    #[serde(rename = "materializedAgentBase")]
    pub materialized_agent_base: AgentBaseKey,
    #[serde(rename = "materializedTemplate")]
    pub materialized_template: AgentSpec,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "readyAt")]
    pub ready_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<AgentInstanceBootstrapState>,
    pub network: NetworkState,
    pub healthchecks: Vec<HealthcheckPlan>,
    #[serde(rename = "guestAccess")]
    pub guest_access: Option<GuestAccessState>,
    pub readiness: Option<ReadinessState>,
    #[serde(rename = "hostInputs")]
    pub host_inputs: AgentInstanceHostInputsState,
    pub work: AgentInstanceWorkStatus,
    pub reconciliation: Option<ReconciliationState>,
    #[serde(rename = "tailscaleServe")]
    pub tailscale_serve: Option<TailscaleServeState>,
    pub backend: BackendState,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceHostInputsState {
    #[serde(rename = "observedGeneration")]
    pub observed_generation: u64,
    pub phase: AgentInstanceHostInputsPhase,
    #[serde(rename = "lastError", skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub credentials: BTreeMap<String, AgentInstanceCredentialState>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceCredentialState {
    pub phase: AgentInstanceCredentialPhase,
    #[serde(rename = "expiresAtUnixSeconds", skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_seconds: Option<u64>,
    #[serde(rename = "lastRefreshAt", skip_serializing_if = "Option::is_none")]
    pub last_refresh_at: Option<String>,
    #[serde(rename = "lastError", skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentInstanceCredentialPhase {
    Ready,
    RefreshFailed,
    Expired,
}

impl AgentInstanceCredentialPhase {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::RefreshFailed => "refresh-failed",
            Self::Expired => "expired",
        }
    }
}

impl AgentInstanceHostInputsState {
    #[must_use]
    pub fn is_ready_for(&self, generation: u64) -> bool {
        self.observed_generation == generation
            && matches!(self.phase, AgentInstanceHostInputsPhase::Ready)
            && self
                .credentials
                .values()
                .all(|credential| credential.phase != AgentInstanceCredentialPhase::Expired)
    }

    pub fn mark_pending(&mut self) {
        self.phase = AgentInstanceHostInputsPhase::Pending;
        self.last_error = None;
    }

    pub fn mark_ready(&mut self, generation: u64) {
        self.observed_generation = generation;
        self.phase = AgentInstanceHostInputsPhase::Ready;
        self.last_error = None;
    }

    pub fn record_failure(&mut self, generation: u64, error: String) {
        self.observed_generation = generation;
        self.phase = AgentInstanceHostInputsPhase::Failed;
        self.last_error = Some(error);
    }
}

#[cfg(test)]
mod tests {
    use super::{AgentInstanceCredentialPhase, AgentInstanceCredentialState, AgentInstanceHostInputsState};

    #[test]
    fn expired_credentials_block_host_input_readiness() {
        let mut state = AgentInstanceHostInputsState::default();
        state.mark_ready(3);
        state.credentials.insert(
            "codex".to_owned(),
            AgentInstanceCredentialState {
                phase: AgentInstanceCredentialPhase::Expired,
                expires_at_unix_seconds: Some(1_000),
                last_refresh_at: None,
                last_error: Some("expired".to_owned()),
            },
        );
        assert!(!state.is_ready_for(3));

        state.credentials.get_mut("codex").unwrap().phase = AgentInstanceCredentialPhase::RefreshFailed;
        assert!(state.is_ready_for(3));
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentInstanceHostInputsPhase {
    #[default]
    Pending,
    Ready,
    Failed,
}

impl AgentInstanceHostInputsPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }
}

impl AgentInstanceStatus {
    pub fn clear_readiness(&mut self) {
        self.readiness = None;
        self.ready_at = None;
    }

    pub fn clear_bootstrap_failure(&mut self) {
        self.bootstrap = None;
    }

    pub fn mark_ready(&mut self, readiness: ReadinessState) {
        self.ready_at = Some(agentdp_platform::time::rfc3339_utc_now());
        self.bootstrap = None;
        self.readiness = Some(readiness);
    }

    pub fn record_bootstrap_failure(&mut self, failure: AgentInstanceBootstrapState) {
        self.clear_readiness();
        self.bootstrap = Some(failure);
    }

    pub const fn mark_observed_generation(&mut self, generation: u64) -> bool {
        if self.observed_generation == generation {
            return false;
        }
        self.observed_generation = generation;
        true
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentInstancePhase {
    Materialized,
    Starting,
    Running,
    Stopping,
    Stopped,
    Deleting,
    Deleted,
    Failed,
}

impl fmt::Display for AgentInstancePhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AgentInstancePhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Materialized => "materialized",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentInstanceTarget {
    Active,
    Inactive,
    Deleting,
}

impl AgentInstanceTarget {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Inactive => "inactive",
            Self::Deleting => "deleting",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceBootstrapState {
    #[serde(rename = "attemptEpoch", skip_serializing_if = "Option::is_none")]
    pub attempt_epoch: Option<u64>,
    #[serde(rename = "failureCount")]
    pub failure_count: u32,
    #[serde(rename = "lastFailureUnixSeconds")]
    pub last_failure_unix_seconds: u64,
    #[serde(rename = "nextRetryUnixSeconds")]
    pub next_retry_unix_seconds: u64,
    #[serde(rename = "lastError")]
    pub last_error: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceWorkStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transition: Option<AgentInstanceTransitionWorkStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<AgentInstanceBootstrapWorkStatus>,
    pub sessions: AgentInstanceSessionsWorkStatus,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceTransitionWorkStatus {
    pub kind: AgentInstanceTransitionKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_unix_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceBootstrapWorkStatus {
    pub phase: AgentInstanceBootstrapWorkPhase,
    #[serde(rename = "currentStep", skip_serializing_if = "Option::is_none")]
    pub current_step: Option<AgentInstanceBootstrapStepStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_unix_seconds: Option<u64>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentInstanceBootstrapWorkPhase {
    Running,
    BackingOff,
    Failed,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceSessionsWorkStatus {
    pub active: u16,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentInstanceTransitionKind {
    Materialize,
    Reconcile,
    Start,
    Stop,
    Delete,
}

impl AgentInstanceTransitionKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Materialize => "materialize",
            Self::Reconcile => "reconcile",
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceBootstrapStepStatus {
    pub step: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<BootstrapStepPhase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<BootstrapLifecycleStatus>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NetworkState {
    pub mode: NetworkModeState,
    pub allow: NetworkAllowState,
    pub ipv6: NetworkIpv6State,
    pub ports: BTreeMap<String, PortMappingState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<AgentInstanceNetworkStatus>,
}

impl NetworkState {
    #[must_use]
    pub const fn new(
        backend_state: &BackendState,
        allow: NetworkAllowState,
        ipv6: NetworkIpv6State,
        ports: BTreeMap<String, PortMappingState>,
    ) -> Self {
        let mode = match backend_state {
            BackendState::Qemu(state) if state.instance_network.is_some() => NetworkModeState::Mediated,
            BackendState::Qemu(_) => NetworkModeState::User,
        };
        Self {
            mode,
            allow,
            ipv6,
            ports,
            runtime: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkModeState {
    User,
    Mediated,
}

impl fmt::Display for NetworkModeState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::User => formatter.write_str("user"),
            Self::Mediated => formatter.write_str("mediated"),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "hosts", rename_all = "lowercase")]
pub enum NetworkAllowState {
    All,
    #[default]
    Public,
    Hosts(Vec<String>),
}

impl From<&NetworkAllow> for NetworkAllowState {
    fn from(value: &NetworkAllow) -> Self {
        match value {
            NetworkAllow::All => Self::All,
            NetworkAllow::Hosts(hosts) if hosts.is_empty() => Self::Public,
            NetworkAllow::Hosts(hosts) => Self::Hosts(hosts.clone()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NetworkIpv6State {
    pub enabled: bool,
}

impl NetworkIpv6State {
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub async fn from_manifest(value: NetworkIpv6) -> Self {
        Self {
            enabled: value.enabled_for_host(agentdp_platform::net::has_ipv6_egress().await),
        }
    }
}

impl Default for NetworkIpv6State {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PortMappingState {
    pub guest: u16,
    pub host: Option<u16>,
    pub protocol: PortProtocolState,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PortProtocolState {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GuestAccessState {
    pub user: String,
    #[serde(rename = "privateKey")]
    pub private_key: String,
    #[serde(rename = "publicKey")]
    pub public_key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReadinessState {
    pub ready: bool,
    #[serde(rename = "lastSuccessUnixSeconds")]
    pub last_success_unix_seconds: u64,
    pub result: ReadinessResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReadinessResult {
    pub ready: bool,
    pub services: BTreeMap<String, ServiceStatus>,
    pub healthchecks: Vec<HealthcheckStatus>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub host_port: u16,
    pub guest_port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HealthcheckStatus {
    pub name: String,
    pub kind: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_status: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationState {
    pub stale: bool,
    #[serde(rename = "observedStatus")]
    pub observed_status: String,
    #[serde(rename = "observedPid")]
    pub observed_pid: Option<u32>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TailscaleServeState {
    pub routes: Vec<TailscaleServeRouteState>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TailscaleServeRouteState {
    pub service: String,
    pub mode: String,
    pub host: String,
    #[serde(rename = "httpsPort")]
    pub https_port: Option<u16>,
    pub path: String,
    pub url: String,
    pub target: String,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum BackendState {
    #[serde(rename = "qemu")]
    Qemu(QemuState),
}

impl BackendState {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Qemu(_) => "qemu",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProcessStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QemuState {
    pub image: QemuImageState,
    pub disk: String,
    pub work_dir: String,
    pub seed_media: String,
    pub seed_meta_data: String,
    pub seed_network_config: String,
    pub seed_user_data: String,
    pub monitor_socket: String,
    pub qmp_socket: String,
    pub guest_control_socket: String,
    pub pid_file: String,
    pub serial_log: String,
    pub qemu_log: String,
    pub instance_network: Option<QemuInstanceNetworkState>,
    pub mediated_secrets: SecretBindings,
    pub mediated_ca: QemuMediatedCaState,
    pub pid: Option<u32>,
    pub last_start_unix_seconds: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QemuMediatedCaState {
    pub cert_pem: String,
    pub key_path: String,
}

impl QemuMediatedCaState {
    #[must_use]
    pub const fn new(cert_pem: String, key_path: String) -> Self {
        Self { cert_pem, key_path }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QemuInstanceNetworkState {
    pub addresses: agentdp_network::InstanceAddresses,
    pub stream_socket: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QemuImageState {
    pub os: String,
    pub architecture: String,
    pub variant: String,
    pub source_url: String,
    pub cache_key: String,
    pub cache_path: String,
    pub download_path: String,
    pub format: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceNetworkStatus {
    pub state: String,
    pub ready: bool,
    pub host_ports: BTreeMap<String, PortMappingState>,
    pub transport: Option<String>,
    pub generation: Option<u64>,
    pub started_unix_seconds: u64,
    pub last_state_change_unix_seconds: u64,
    pub last_transport_connect_unix_seconds: Option<u64>,
    pub last_guest_frame_unix_seconds: Option<u64>,
    pub last_host_frame_unix_seconds: Option<u64>,
    pub guest_frames_received: u64,
    pub guest_bytes_received: u64,
    pub host_frames_sent: u64,
    pub host_bytes_sent: u64,
    pub session_disconnects: u64,
    pub connect_errors: u64,
    pub egress_errors: u64,
    pub network_event_drops: u64,
    pub last_error: Option<String>,
}

impl Default for AgentInstanceNetworkStatus {
    fn default() -> Self {
        Self {
            state: "starting".to_owned(),
            ready: false,
            host_ports: BTreeMap::new(),
            transport: None,
            generation: None,
            started_unix_seconds: 0,
            last_state_change_unix_seconds: 0,
            last_transport_connect_unix_seconds: None,
            last_guest_frame_unix_seconds: None,
            last_host_frame_unix_seconds: None,
            guest_frames_received: 0,
            guest_bytes_received: 0,
            host_frames_sent: 0,
            host_bytes_sent: 0,
            session_disconnects: 0,
            connect_errors: 0,
            egress_errors: 0,
            network_event_drops: 0,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceNetworkEvent {
    pub sequence: u64,
    pub unix_millis: u64,
    pub dropped_events_before: u64,
    pub event: AgentInstanceNetworkEventKind,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentInstanceNetworkEventKind {
    LifecycleStateChanged {
        state: String,
    },
    TelemetrySnapshot {
        started_unix_seconds: u64,
        last_state_change_unix_seconds: u64,
        last_transport_connect_unix_seconds: Option<u64>,
        last_guest_frame_unix_seconds: Option<u64>,
        last_host_frame_unix_seconds: Option<u64>,
        guest_frames_received: u64,
        guest_bytes_received: u64,
        host_frames_sent: u64,
        host_bytes_sent: u64,
        session_disconnects: u64,
        connect_errors: u64,
        egress_errors: u64,
        buffer_frame_available: u64,
        buffer_small_byte_available: u64,
        buffer_medium_byte_available: u64,
        buffer_tcp_byte_available: u64,
        tcp_proxy_active_slots: u64,
        tcp_proxy_upstream_read_ready: u64,
        tcp_proxy_upstream_read_masked: u64,
        tcp_proxy_guest_send_blocked: u64,
        tcp_proxy_pending_guest_bytes: u64,
    },
    TransportConnectFailed {
        transport: String,
        error: String,
    },
    TransportGuestConnected {
        transport: String,
        generation: u64,
    },
    TransportGuestDisconnected {
        generation: u64,
        reason: String,
    },
    TransportRegisterFailed {
        transport: String,
        error: String,
    },
    EgressError {
        protocol: String,
        proxy: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        destination: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        upstream: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        authority: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        route: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        phase: Option<String>,
        message: String,
    },
    EgressProxyClosed {
        protocol: String,
        proxy: Option<u64>,
    },
    DnsResolved {
        protocol: String,
        host: String,
        addresses: Vec<IpAddr>,
        ttl_millis: u64,
    },
    HostPortBound {
        name: String,
        protocol: PortProtocolState,
        guest: u16,
        host: u16,
    },
    HostPortError {
        message: String,
    },
    ReactorError {
        message: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum OperationResult {
    Succeeded,
    Failed { error: String },
}
