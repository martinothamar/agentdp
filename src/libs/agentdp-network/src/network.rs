use std::collections::VecDeque;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use agentdp_crypto::{TlsClientConfig, TlsServerConfig};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smoltcp::wire::{Ipv4Address, Ipv4Cidr};

use crate::buffers::ByteBuf;
use crate::clock::{NetworkClock, SystemClock};
use crate::guest::TransportError;
use crate::policy::{Authority, EgressPolicy, NetworkPolicy, RuntimeSecrets};
use crate::tls::TlsInterceptConfig;

const DEFAULT_MTU: usize = 1500;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InstanceAddresses {
    pub gateway: Ipv4AddressText,
    pub address: Ipv4AddressText,
    pub cidr_prefix: u8,
}

impl InstanceAddresses {
    #[must_use]
    pub const fn cidr(&self) -> Ipv4Cidr {
        Ipv4Cidr::new(self.address.0, self.cidr_prefix)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ipv4AddressText(pub Ipv4Address);

impl Ipv4AddressText {
    #[must_use]
    pub const fn from_std(address: Ipv4Addr) -> Self {
        let [a, b, c, d] = address.octets();
        Self(Ipv4Address::new(a, b, c, d))
    }

    #[must_use]
    pub const fn std(self) -> Ipv4Addr {
        let [a, b, c, d] = self.0.octets();
        Ipv4Addr::new(a, b, c, d)
    }
}

impl Serialize for Ipv4AddressText {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Ipv4AddressText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse::<Ipv4Addr>().map_or_else(
            |_| Err(serde::de::Error::custom("invalid IPv4 address")),
            |address| {
                let [a, b, c, d] = address.octets();
                Ok(Self(Ipv4Address::new(a, b, c, d)))
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstanceMacAddresses {
    pub gateway: MacAddress,
    pub guest: MacAddress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MacAddress([u8; 6]);

impl MacAddress {
    #[must_use]
    pub const fn new(octets: [u8; 6]) -> Self {
        Self(octets)
    }

    #[must_use]
    pub const fn octets(self) -> [u8; 6] {
        self.0
    }

    #[must_use]
    pub(crate) const fn smoltcp(self) -> smoltcp::wire::EthernetAddress {
        smoltcp::wire::EthernetAddress(self.0)
    }
}

impl fmt::Display for MacAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5]
        )
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum HostPortProtocol {
    Tcp,
    Udp,
}

impl std::fmt::Display for HostPortProtocol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp => formatter.write_str("TCP"),
            Self::Udp => formatter.write_str("UDP"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPortSpec {
    pub name: String,
    pub protocol: HostPortProtocol,
    pub guest: u16,
    pub host: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkLimits {
    pub command_inbox_capacity: usize,
    pub reactor_event_capacity: usize,
    pub component_output_batch_capacity: usize,
    pub component_event_batch_capacity: usize,
    pub frame_device_queue_capacity: usize,
    pub frame_buffer_capacity: usize,
    pub max_pooled_frame_capacity: usize,
    pub frame_buffer_pool_capacity: usize,
    pub small_byte_capacity: usize,
    pub medium_byte_capacity: usize,
    pub tcp_byte_capacity: usize,
    pub small_byte_pool_capacity: usize,
    pub medium_byte_pool_capacity: usize,
    pub tcp_byte_pool_capacity: usize,
    pub tcp_socket_buffer_capacity: usize,
    pub udp_datagram_buffer_capacity: usize,
    pub ingress_udp_datagram_buffer_capacity: usize,
    pub client_hello_limit: usize,
    pub tls_relay_buffer_capacity: usize,
    pub transport_connect_retry_delay: Duration,
    pub status_publish_interval: Duration,
    pub idle_poll_delay: Duration,
    pub udp_proxy_timeout: Duration,
    pub drive_event_budget: usize,
    pub drive_step_budget: usize,
    pub drive_byte_budget: usize,
    pub tcp_proxy_limit: usize,
    pub ingress_tcp_connection_limit: usize,
    pub ingress_udp_peer_limit: usize,
    pub udp_proxy_limit: usize,
    pub timer_queue_capacity: usize,
    pub telemetry_event_capacity: usize,
}

impl NetworkLimits {
    #[must_use]
    pub const fn default_local() -> Self {
        Self {
            command_inbox_capacity: 1024,
            reactor_event_capacity: 256,
            component_output_batch_capacity: 256,
            component_event_batch_capacity: 256,
            frame_device_queue_capacity: 512,
            frame_buffer_capacity: 1514,
            max_pooled_frame_capacity: 65_535,
            frame_buffer_pool_capacity: 256,
            small_byte_capacity: 2 * 1024,
            medium_byte_capacity: 16 * 1024,
            tcp_byte_capacity: 64 * 1024,
            small_byte_pool_capacity: 1024,
            medium_byte_pool_capacity: 256,
            tcp_byte_pool_capacity: 128,
            tcp_socket_buffer_capacity: 64 * 1024,
            udp_datagram_buffer_capacity: 65_536,
            ingress_udp_datagram_buffer_capacity: 65_536,
            client_hello_limit: 16_384,
            tls_relay_buffer_capacity: 16_384,
            transport_connect_retry_delay: Duration::from_millis(10),
            status_publish_interval: Duration::from_secs(10),
            idle_poll_delay: Duration::from_secs(1),
            udp_proxy_timeout: Duration::from_secs(30),
            drive_event_budget: 256,
            drive_step_budget: 8192,
            drive_byte_budget: 64 * 1024 * 1024,
            tcp_proxy_limit: 128,
            ingress_tcp_connection_limit: 128,
            ingress_udp_peer_limit: 128,
            udp_proxy_limit: 128,
            timer_queue_capacity: 1024,
            telemetry_event_capacity: 16,
        }
    }

    pub(crate) fn validate_for_event_loop(&self) -> Result<(), String> {
        if self.drive_event_budget == 0 {
            return Err("drive_event_budget must be greater than zero".to_owned());
        }
        if self.drive_step_budget == 0 {
            return Err("drive_step_budget must be greater than zero".to_owned());
        }
        if self.drive_byte_budget == 0 {
            return Err("drive_byte_budget must be greater than zero".to_owned());
        }
        for (name, capacity) in [
            ("frame_buffer_capacity", self.frame_buffer_capacity),
            ("udp_datagram_buffer_capacity", self.udp_datagram_buffer_capacity),
            (
                "ingress_udp_datagram_buffer_capacity",
                self.ingress_udp_datagram_buffer_capacity,
            ),
        ] {
            if self.drive_byte_budget < capacity {
                return Err(format!(
                    "drive_byte_budget {} must be at least {name} {capacity}",
                    self.drive_byte_budget
                ));
            }
        }
        Ok(())
    }
}

impl Default for NetworkLimits {
    fn default() -> Self {
        Self::default_local()
    }
}

#[must_use]
const fn dns_upstream_fallback() -> SocketAddr {
    SocketAddr::new(agentdp_platform::dns::fallback_dns_server(), 53)
}

#[derive(Debug)]
pub struct InstanceNetworkConfig {
    pub network: InstanceAddresses,
    pub mac: InstanceMacAddresses,
    pub policy: NetworkPolicy,
    pub host_ports: Vec<HostPortSpec>,
    pub dns_upstream: SocketAddr,
    pub tls: Option<TlsInterceptConfig>,
    pub limits: NetworkLimits,
    pub ipv6_enabled: bool,
    pub mtu: usize,
}

impl InstanceNetworkConfig {
    #[must_use]
    pub const fn new(network: InstanceAddresses, mac: InstanceMacAddresses, policy: EgressPolicy) -> Self {
        Self {
            network,
            mac,
            policy: NetworkPolicy::new(policy),
            host_ports: Vec::new(),
            dns_upstream: dns_upstream_fallback(),
            tls: None,
            limits: NetworkLimits::default_local(),
            ipv6_enabled: false,
            mtu: DEFAULT_MTU,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum InstanceNetworkState {
    Starting,
    Connecting {
        transport: String,
    },
    Connected {
        generation: u64,
    },
    TrafficObserved {
        generation: u64,
    },
    Backoff {
        generation: u64,
        reason: String,
        reconnect_after: Duration,
    },
    Stopping,
    Stopped,
    Failed {
        error: String,
    },
}

impl InstanceNetworkState {
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::TrafficObserved { .. })
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped | Self::Failed { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceNetworkStatus {
    pub state: InstanceNetworkState,
    pub host_ports: Vec<InstanceNetworkHostPort>,
    pub telemetry: InstanceNetworkTelemetry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceNetworkHostPort {
    pub name: String,
    pub protocol: HostPortProtocol,
    pub guest: u16,
    pub host: u16,
}

impl InstanceNetworkStatus {
    #[must_use]
    pub(crate) fn starting_with_limits(limits: &NetworkLimits, clock: &impl NetworkClock) -> Self {
        Self {
            state: InstanceNetworkState::Starting,
            host_ports: Vec::new(),
            telemetry: InstanceNetworkTelemetry::new(limits, clock),
        }
    }

    #[must_use]
    pub fn starting(limits: &NetworkLimits) -> Self {
        Self::starting_with_limits(limits, &SystemClock)
    }

    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.state.is_ready()
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    pub fn observe_event(&mut self, envelope: &crate::events::NetworkEventEnvelope) {
        use crate::events::{
            NetworkEgressEvent, NetworkEvent, NetworkHostPortEvent, NetworkLifecycleEvent, NetworkReactorEvent,
            NetworkTelemetryEvent, NetworkTransportEvent,
        };

        let unix_seconds = envelope.unix_millis / 1000;
        match &envelope.event {
            NetworkEvent::Lifecycle(NetworkLifecycleEvent::StateChanged { state }) => {
                self.state = state.to_state();
                self.telemetry.last_state_change_unix_seconds = unix_seconds;
                match &self.state {
                    InstanceNetworkState::Connected { .. } => {
                        self.telemetry.last_transport_connect_unix_seconds = Some(unix_seconds);
                    }
                    InstanceNetworkState::Backoff { reason, .. } => {
                        self.telemetry.session_disconnects = self.telemetry.session_disconnects.saturating_add(1);
                        self.telemetry
                            .push_error_at(InstanceNetworkErrorSource::State, reason.clone(), unix_seconds);
                    }
                    InstanceNetworkState::Failed { error } => {
                        self.telemetry
                            .push_error_at(InstanceNetworkErrorSource::State, error.clone(), unix_seconds);
                    }
                    _ => {}
                }
            }
            NetworkEvent::Telemetry(NetworkTelemetryEvent::Snapshot(snapshot)) => {
                (*snapshot).apply_to(&mut self.telemetry);
            }
            NetworkEvent::Transport(
                NetworkTransportEvent::ConnectFailed { error, .. }
                | NetworkTransportEvent::RegisterFailed { error, .. },
            ) => {
                self.telemetry.connect_errors = self.telemetry.connect_errors.saturating_add(1);
                self.telemetry.push_error_at(
                    InstanceNetworkErrorSource::Connect,
                    error.as_str().to_owned(),
                    unix_seconds,
                );
            }
            NetworkEvent::Egress(NetworkEgressEvent::Error(error)) => {
                self.telemetry.egress_errors = self.telemetry.egress_errors.saturating_add(1);
                self.telemetry.push_error_at(
                    InstanceNetworkErrorSource::Egress,
                    error.message.as_str().to_owned(),
                    unix_seconds,
                );
            }
            NetworkEvent::HostPort(NetworkHostPortEvent::Error { message })
            | NetworkEvent::Reactor(NetworkReactorEvent::Error { message }) => {
                self.telemetry.egress_errors = self.telemetry.egress_errors.saturating_add(1);
                self.telemetry.push_error_at(
                    InstanceNetworkErrorSource::Egress,
                    message.as_str().to_owned(),
                    unix_seconds,
                );
            }
            NetworkEvent::HostPort(NetworkHostPortEvent::Bound {
                name,
                protocol,
                guest,
                host,
            }) => {
                let bound = InstanceNetworkHostPort {
                    name: name.as_str().to_owned(),
                    protocol: *protocol,
                    guest: *guest,
                    host: *host,
                };
                match self.host_ports.iter_mut().find(|port| port.name == bound.name) {
                    Some(port) => *port = bound,
                    None => self.host_ports.push(bound),
                }
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceNetworkTelemetry {
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
    pub telemetry_events_dropped: u64,
    pub error_events: InstanceNetworkErrorEvents,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceNetworkErrorEvents {
    events: VecDeque<InstanceNetworkErrorEvent>,
    capacity: usize,
}

impl InstanceNetworkErrorEvents {
    #[must_use]
    fn new(capacity: usize) -> Self {
        Self {
            events: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn push(&mut self, event: InstanceNetworkErrorEvent) -> bool {
        if self.capacity == 0 {
            return true;
        }
        let dropped = if self.events.len() == self.capacity {
            let _oldest = self.events.pop_front();
            true
        } else {
            false
        };
        self.events.push_back(event);
        dropped
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &InstanceNetworkErrorEvent> {
        self.events.iter()
    }

    #[must_use]
    pub fn latest(&self) -> Option<&InstanceNetworkErrorEvent> {
        self.events.back()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceNetworkErrorEvent {
    pub unix_seconds: u64,
    pub source: InstanceNetworkErrorSource,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceNetworkErrorSource {
    Connect,
    Egress,
    State,
}

impl InstanceNetworkTelemetry {
    #[must_use]
    fn new(limits: &NetworkLimits, clock: &impl NetworkClock) -> Self {
        let now = clock.unix_seconds();
        Self {
            started_unix_seconds: now,
            last_state_change_unix_seconds: now,
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
            telemetry_events_dropped: 0,
            error_events: InstanceNetworkErrorEvents::new(limits.telemetry_event_capacity),
        }
    }

    pub(crate) fn record_state(&mut self, state: &InstanceNetworkState, clock: &impl NetworkClock) {
        self.last_state_change_unix_seconds = clock.unix_seconds();
        match state {
            InstanceNetworkState::Connected { .. } => {
                self.last_transport_connect_unix_seconds = Some(clock.unix_seconds());
            }
            InstanceNetworkState::Backoff { reason, .. } => {
                self.session_disconnects = self.session_disconnects.saturating_add(1);
                self.push_error(InstanceNetworkErrorSource::State, reason.clone(), clock);
            }
            InstanceNetworkState::Failed { error } => {
                self.push_error(InstanceNetworkErrorSource::State, error.clone(), clock);
            }
            _ => {}
        }
    }

    pub(crate) fn record_connect_error(&mut self, error: &str, clock: &impl NetworkClock) {
        self.connect_errors = self.connect_errors.saturating_add(1);
        self.push_error(InstanceNetworkErrorSource::Connect, error.to_owned(), clock);
    }

    pub(crate) fn record_egress_error(&mut self, error: String, clock: &impl NetworkClock) {
        self.egress_errors = self.egress_errors.saturating_add(1);
        self.push_error(InstanceNetworkErrorSource::Egress, error, clock);
    }

    fn push_error(&mut self, source: InstanceNetworkErrorSource, message: String, clock: &impl NetworkClock) {
        self.push_error_at(source, message, clock.unix_seconds());
    }

    fn push_error_at(&mut self, source: InstanceNetworkErrorSource, message: String, unix_seconds: u64) {
        let dropped = self.error_events.push(InstanceNetworkErrorEvent {
            unix_seconds,
            source,
            message,
        });
        if dropped {
            self.telemetry_events_dropped = self.telemetry_events_dropped.saturating_add(1);
        }
    }

    pub(crate) fn record_guest_frame(&mut self, bytes: usize, clock: &impl NetworkClock) {
        self.guest_frames_received = self.guest_frames_received.saturating_add(1);
        self.guest_bytes_received = self.guest_bytes_received.saturating_add(bytes_to_u64(bytes));
        self.last_guest_frame_unix_seconds = Some(clock.unix_seconds());
    }

    pub(crate) fn record_host_frame(&mut self, bytes: usize, clock: &impl NetworkClock) {
        self.host_frames_sent = self.host_frames_sent.saturating_add(1);
        self.host_bytes_sent = self.host_bytes_sent.saturating_add(bytes_to_u64(bytes));
        self.last_host_frame_unix_seconds = Some(clock.unix_seconds());
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum InstanceNetworkError {
    #[error("instance network transport `{transport}` failed while connecting a session: {source}")]
    TransportConnect {
        transport: String,
        #[source]
        source: TransportError,
    },
    #[error("instance network transport `{transport}` cleanup failed: {source}")]
    Cleanup {
        transport: String,
        #[source]
        source: TransportError,
    },
    #[error("instance network failed to bind host port: {message}")]
    HostPortBind { message: String },
    #[error("timed out after {timeout:?} waiting for instance network `{label}` to become ready")]
    ReadyTimeout { label: String, timeout: Duration },
    #[error("timed out after {timeout:?} stopping instance network `{label}`")]
    StopTimeout { label: String, timeout: Duration },
    #[error("instance network task for `{label}` failed: {message}")]
    TaskFailed { label: String, message: String },
    #[error("instance network `{label}` stopped before readiness")]
    StoppedBeforeReady { label: String },
}

pub struct InstanceNetworkSpec {
    pub label: String,
    pub config: InstanceNetworkConfig,
    pub reconnect_delay: Duration,
    pub write_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TcpProxyId(pub(crate) u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct HostConnectionId(pub(crate) u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct UdpProxyKey {
    pub(crate) guest_src: SocketAddr,
    pub(crate) guest_dst: SocketAddr,
    pub(crate) host_dst: SocketAddr,
}

pub(crate) struct EgressUdpSend {
    pub(crate) proxy: UdpProxyKey,
    pub(crate) bytes: ByteBuf,
    pub(crate) is_dns: bool,
}

pub(crate) enum IngressTcpOutput {
    Write {
        connection: HostConnectionId,
        bytes: ByteBuf,
    },
    FinishWrite {
        connection: HostConnectionId,
    },
    Close {
        connection: HostConnectionId,
    },
}

pub(crate) struct IngressUdpSend {
    pub(crate) port: u16,
    pub(crate) peer: SocketAddr,
    pub(crate) bytes: ByteBuf,
}

#[derive(Debug, Clone)]
pub(crate) enum TcpEgressRoute {
    Plain(TcpEgressPolicy),
    Dns { upstream: SocketAddr },
    Tls(TlsEgressPolicy),
}

#[derive(Debug, Clone)]
pub(crate) struct TcpEgressPolicy {
    pub(crate) decision: EgressDecision,
    pub(crate) reject_secret_placeholders: bool,
}

#[derive(Clone)]
pub(crate) struct TlsEgressPolicy {
    pub(crate) dst: SocketAddr,
    pub(crate) client_config: TlsClientConfig,
    pub(crate) bypass_hosts: Vec<String>,
    pub(crate) server_configs: Vec<(Authority, TlsServerConfig)>,
    pub(crate) decisions: Vec<(Authority, EgressDecision)>,
    pub(crate) fallback: EgressDecision,
}

impl TlsEgressPolicy {
    pub(crate) fn decision_for(&self, authority: &Authority) -> Option<&EgressDecision> {
        authority_value(&self.decisions, authority)
    }

    pub(crate) fn server_config_for(&self, authority: &Authority) -> Option<&TlsServerConfig> {
        authority_value(&self.server_configs, authority)
    }
}

fn authority_value<'a, T>(entries: &'a [(Authority, T)], authority: &Authority) -> Option<&'a T> {
    entries
        .iter()
        .find(|(candidate, _value)| candidate == authority)
        .map(|(_authority, value)| value)
}

impl std::fmt::Debug for TlsEgressPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TlsEgressPolicy")
            .field("dst", &self.dst)
            .field("bypass_hosts", &self.bypass_hosts)
            .field(
                "server_configs",
                &self
                    .server_configs
                    .iter()
                    .map(|(authority, _config)| authority)
                    .collect::<Vec<_>>(),
            )
            .field("decisions", &self.decisions)
            .field("fallback", &self.fallback)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EgressDecision {
    pub(crate) application: ApplicationPolicy,
}

#[derive(Debug, Clone)]
pub(crate) enum ApplicationPolicy {
    Raw,
    Http1 {
        authority: Authority,
        secrets: RuntimeSecrets,
    },
    Block {
        reason: BlockReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockReason {
    AuthorityNotAllowed,
    TlsInterceptUnavailable,
}

fn bytes_to_u64(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io::{Read as _, Write as _};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::os::fd::AsFd as _;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use crate::buffers::FrameBuf;
    use crate::clock::SystemClock;
    use crate::command::{NetworkCommand, NetworkCommandSource};
    use crate::event_loop::{EventLoop, NetworkExit};
    use crate::events::{
        NetworkEvent, NetworkEventEnvelope, NetworkEventSink, NetworkEventText, NetworkHostPortEvent,
        NetworkLifecycleEvent, NetworkStateEvent, NetworkTelemetryEvent, NetworkTransportEvent,
    };
    use crate::guest::{ConnectStatus, FrameRead, FrameWrite, GuestFrameSession, GuestFrameTransport, GuestIoSource};
    use crate::reactor::ProductionWake;
    use smoltcp::wire::{
        ArpOperation, ArpPacket, ArpRepr, ETHERNET_HEADER_LEN, EthernetAddress, EthernetFrame, EthernetProtocol,
        Ipv4Address,
    };
    use tokio::sync::watch;

    use super::{
        EgressPolicy, HostPortProtocol, InstanceAddresses, InstanceMacAddresses, InstanceNetworkConfig,
        InstanceNetworkError, InstanceNetworkErrorSource, InstanceNetworkSpec, InstanceNetworkState,
        InstanceNetworkStatus, InstanceNetworkTelemetry, Ipv4AddressText, MacAddress, NetworkLimits, TransportError,
        bytes_to_u64,
    };

    const TEST_MAC: InstanceMacAddresses = InstanceMacAddresses {
        gateway: MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]),
        guest: MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x02]),
    };

    const TEST_ADDRESSES: InstanceAddresses = InstanceAddresses {
        gateway: Ipv4AddressText(Ipv4Address::new(10, 73, 0, 1)),
        address: Ipv4AddressText(Ipv4Address::new(10, 73, 0, 10)),
        cidr_prefix: 24,
    };

    #[test]
    fn address_text_and_protocol_display_are_stable() {
        let addresses = TEST_ADDRESSES;

        assert_eq!(addresses.gateway.std(), Ipv4Addr::new(10, 73, 0, 1));
        assert_eq!(addresses.address.std(), Ipv4Addr::new(10, 73, 0, 10));
        assert_eq!(HostPortProtocol::Tcp.to_string(), "TCP");
        assert_eq!(HostPortProtocol::Udp.to_string(), "UDP");
        assert_eq!(
            Ipv4AddressText(smoltcp::wire::Ipv4Address::new(192, 0, 2, 1)).std(),
            Ipv4Addr::new(192, 0, 2, 1)
        );
    }

    #[test]
    fn telemetry_records_frames_states_and_ring_buffered_errors() {
        let limits = NetworkLimits {
            telemetry_event_capacity: 16,
            ..NetworkLimits::default()
        };
        let clock = SystemClock;
        let mut telemetry = InstanceNetworkTelemetry::new(&limits, &clock);

        telemetry.record_state(&InstanceNetworkState::Connected { generation: 7 }, &clock);
        telemetry.record_guest_frame(5, &clock);
        telemetry.record_host_frame(9, &clock);
        telemetry.record_connect_error("connect failed", &clock);
        for index in 0..(limits.telemetry_event_capacity + 2) {
            telemetry.record_egress_error(format!("egress failed {index}"), &clock);
        }
        telemetry.record_state(
            &InstanceNetworkState::Backoff {
                generation: 7,
                reason: "closed".to_owned(),
                reconnect_after: Duration::from_millis(10),
            },
            &clock,
        );

        assert_eq!(telemetry.guest_frames_received, 1);
        assert_eq!(telemetry.guest_bytes_received, 5);
        assert_eq!(telemetry.host_frames_sent, 1);
        assert_eq!(telemetry.host_bytes_sent, 9);
        assert_eq!(telemetry.connect_errors, 1);
        assert_eq!(telemetry.egress_errors, (limits.telemetry_event_capacity + 2) as u64);
        assert_eq!(telemetry.session_disconnects, 1);
        assert_eq!(telemetry.error_events.len(), limits.telemetry_event_capacity);
        assert_eq!(telemetry.telemetry_events_dropped, 4);
        assert_eq!(
            telemetry.error_events.latest().map(|event| event.source),
            Some(InstanceNetworkErrorSource::State)
        );
        assert!(
            telemetry
                .error_events
                .iter()
                .any(|event| event.message == "egress failed 3")
        );
        assert!(
            !telemetry
                .error_events
                .iter()
                .any(|event| event.message == "connect failed")
        );
    }

    #[test]
    fn status_readiness_and_terminal_helpers_follow_state() {
        let mut status = InstanceNetworkStatus::starting_with_limits(&NetworkLimits::default(), &SystemClock);
        assert!(!status.is_ready());
        assert!(!status.is_terminal());

        status.state = InstanceNetworkState::TrafficObserved { generation: 1 };
        assert!(status.is_ready());
        assert!(!status.is_terminal());

        status.state = InstanceNetworkState::Stopped;
        assert!(status.is_terminal());
    }

    #[test]
    fn status_observes_bound_host_ports_by_name() {
        let mut status = InstanceNetworkStatus::starting_with_limits(&NetworkLimits::default(), &SystemClock);

        status.observe_event(&NetworkEventEnvelope {
            sequence: 1,
            unix_millis: 1_000,
            dropped_events_before: 0,
            event: NetworkEvent::HostPort(NetworkHostPortEvent::Bound {
                name: NetworkEventText::from_str("web"),
                protocol: HostPortProtocol::Tcp,
                guest: 4090,
                host: 51111,
            }),
        });
        status.observe_event(&NetworkEventEnvelope {
            sequence: 2,
            unix_millis: 1_001,
            dropped_events_before: 0,
            event: NetworkEvent::HostPort(NetworkHostPortEvent::Bound {
                name: NetworkEventText::from_str("web"),
                protocol: HostPortProtocol::Tcp,
                guest: 4090,
                host: 52222,
            }),
        });

        assert_eq!(status.host_ports.len(), 1);
        assert_eq!(status.host_ports[0].name, "web");
        assert_eq!(status.host_ports[0].protocol, HostPortProtocol::Tcp);
        assert_eq!(status.host_ports[0].guest, 4090);
        assert_eq!(status.host_ports[0].host, 52222);
    }

    #[test]
    fn bytes_to_u64_saturates() {
        assert_eq!(bytes_to_u64(12), 12);
        assert_eq!(bytes_to_u64(usize::MAX), u64::try_from(usize::MAX).unwrap_or(u64::MAX));
    }

    #[test]
    fn event_loop_rejects_drive_byte_budget_smaller_than_whole_items() {
        let mut spec = spec();
        spec.config.limits.drive_byte_budget = spec.config.limits.udp_datagram_buffer_capacity - 1;
        let (status_tx, _status_rx) = watch::channel(InstanceNetworkStatus::starting(&spec.config.limits));
        let (_commands_tx, commands_rx) = mpsc::channel();

        let result = EventLoop::new(
            spec,
            FakeTransport::frames_then_wait([]),
            TestOutputSink {
                status: status_tx,
                events: None,
            },
            TestCommandSource { receiver: commands_rx },
        );

        match result {
            Ok(_) => panic!("event loop accepted impossible whole-item drive byte budget"),
            Err(InstanceNetworkError::TaskFailed { message, .. }) => {
                assert!(message.contains("drive_byte_budget"));
                assert!(message.contains("udp_datagram_buffer_capacity"));
            }
            Err(error) => panic!("unexpected error: {error}"),
        }
    }

    #[test]
    fn network_limits_reject_zero_drive_byte_budget_even_when_item_capacities_are_zero() {
        let limits = NetworkLimits {
            drive_byte_budget: 0,
            frame_buffer_capacity: 0,
            udp_datagram_buffer_capacity: 0,
            ingress_udp_datagram_buffer_capacity: 0,
            ..NetworkLimits::default()
        };

        let error = limits
            .validate_for_event_loop()
            .expect_err("zero byte budget must be rejected");

        assert!(error.contains("drive_byte_budget"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn network_records_transport_connect_errors() -> Result<(), Box<dyn std::error::Error>> {
        let handle = TestNetworkHandle::start(spec(), FakeTransport::fail(), None)?;

        wait_until(Duration::from_secs(2), || handle.status().telemetry.connect_errors > 0).await?;
        let status = handle.status();
        assert_eq!(status.telemetry.connect_errors, 1);
        assert!(
            status
                .telemetry
                .error_events
                .iter()
                .any(|event| event.source == InstanceNetworkErrorSource::Connect)
        );

        tokio::time::timeout(Duration::from_secs(1), handle.stop()).await??;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn network_emits_transport_connect_failed_event() -> Result<(), Box<dyn std::error::Error>> {
        let events = Arc::new(Mutex::new(Vec::new()));
        let handle = TestNetworkHandle::start(spec(), FakeTransport::fail(), Some(Arc::clone(&events)))?;

        wait_until(Duration::from_secs(2), || {
            events.lock().unwrap().iter().any(|event| {
                matches!(
                    &event.event,
                    NetworkEvent::Transport(NetworkTransportEvent::ConnectFailed { .. })
                )
            })
        })
        .await?;
        let event = events
            .lock()
            .unwrap()
            .iter()
            .find(|event| {
                matches!(
                    &event.event,
                    NetworkEvent::Transport(NetworkTransportEvent::ConnectFailed { .. })
                )
            })
            .cloned()
            .expect("connect failure event should be emitted");
        assert!(event.sequence > 0);
        assert!(event.unix_millis > 0);
        assert!(matches!(
            &event.event,
            NetworkEvent::Transport(NetworkTransportEvent::ConnectFailed { transport, error })
                if transport.as_str() == "fake transport" && !error.as_str().is_empty()
        ));

        tokio::time::timeout(Duration::from_secs(1), handle.stop()).await??;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn network_observes_guest_frame_then_stops() -> Result<(), Box<dyn std::error::Error>> {
        let handle =
            TestNetworkHandle::start(spec(), FakeTransport::frames([b"not an ethernet frame".to_vec()]), None)?;

        wait_until(Duration::from_secs(2), || {
            handle.status().telemetry.guest_frames_received == 1
        })
        .await?;
        let status = handle.status();
        assert_eq!(
            status.telemetry.guest_bytes_received,
            b"not an ethernet frame".len() as u64
        );

        handle.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dataplane_frames_do_not_publish_telemetry_before_status_interval() -> Result<(), Box<dyn std::error::Error>>
    {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut spec = spec();
        spec.config.limits.status_publish_interval = Duration::from_mins(1);
        let frames = (0..64).map(|_| b"not an ethernet frame".to_vec());
        let handle = TestNetworkHandle::start(spec, FakeTransport::frames_then_wait(frames), Some(events.clone()))?;

        wait_until(Duration::from_secs(2), || {
            events.lock().unwrap().iter().any(|event| {
                matches!(
                    &event.event,
                    NetworkEvent::Lifecycle(NetworkLifecycleEvent::StateChanged {
                        state: NetworkStateEvent::TrafficObserved { .. },
                    })
                )
            })
        })
        .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let telemetry_snapshots = events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                matches!(
                    &event.event,
                    NetworkEvent::Telemetry(NetworkTelemetryEvent::Snapshot(_))
                )
            })
            .count();

        assert!(
            telemetry_snapshots <= 4,
            "telemetry snapshot count should be bounded by state publications before the status interval; got {telemetry_snapshots}"
        );

        handle.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn network_answers_guest_arp_request() -> Result<(), Box<dyn std::error::Error>> {
        let transport = FakeTransport::frames_then_wait([arp_request()]);
        let writes = transport.writes();
        let handle = TestNetworkHandle::start(spec(), transport, None)?;

        wait_until(Duration::from_secs(2), || !writes.lock().unwrap().is_empty()).await?;
        let response = writes.lock().unwrap()[0].clone();

        assert_arp_reply_for_gateway(&response)?;
        handle.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn network_reconnects_after_guest_session_disconnect() -> Result<(), Box<dyn std::error::Error>> {
        let transport = FakeTransport::sessions([
            FakeFrames::closed_after([b"not an ethernet frame".to_vec()]),
            FakeFrames::waiting_after([arp_request()]),
        ]);
        let writes = transport.writes();
        let handle = TestNetworkHandle::start(spec_with_reconnect(Duration::from_millis(10)), transport, None)?;

        wait_until(Duration::from_secs(2), || {
            handle.status().telemetry.session_disconnects >= 1 && !writes.lock().unwrap().is_empty()
        })
        .await?;
        assert!(matches!(
            handle.status().state,
            InstanceNetworkState::TrafficObserved { generation: 2 }
        ));

        handle.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_ready_resolves_after_first_guest_frame() -> Result<(), Box<dyn std::error::Error>> {
        let handle = TestNetworkHandle::start(
            spec(),
            FakeTransport::frames_then_wait([b"not an ethernet frame".to_vec()]),
            None,
        )?;

        handle.wait_ready(Duration::from_secs(2)).await?;
        assert_eq!(handle.label(), "unit-test");
        assert!(handle.is_running());

        handle.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_ready_times_out_before_traffic_observed() -> Result<(), Box<dyn std::error::Error>> {
        let handle = TestNetworkHandle::start(spec(), FakeTransport::frames_then_wait([]), None)?;

        let result = handle.wait_ready(Duration::from_millis(20)).await;

        assert!(matches!(result, Err(InstanceNetworkError::ReadyTimeout { .. })));
        handle.stop().await?;
        Ok(())
    }

    struct TestNetworkHandle {
        label: String,
        commands: mpsc::Sender<NetworkCommand>,
        wake: ProductionWake,
        status: watch::Receiver<InstanceNetworkStatus>,
        finished: Arc<AtomicBool>,
        thread: JoinHandle<NetworkExit>,
    }

    struct TestCommandSource {
        receiver: mpsc::Receiver<NetworkCommand>,
    }

    struct TestOutputSink {
        status: watch::Sender<InstanceNetworkStatus>,
        events: Option<Arc<Mutex<Vec<NetworkEventEnvelope>>>>,
    }

    impl TestNetworkHandle {
        fn start(
            spec: InstanceNetworkSpec,
            transport: FakeTransport,
            events: Option<Arc<Mutex<Vec<NetworkEventEnvelope>>>>,
        ) -> Result<Self, InstanceNetworkError> {
            let label = spec.label.clone();
            let (status_tx, status_rx) = watch::channel(InstanceNetworkStatus::starting(&spec.config.limits));
            let (commands_tx, commands_rx) = mpsc::channel();
            let (started_tx, started_rx) = mpsc::sync_channel(1);
            let finished = Arc::new(AtomicBool::new(false));
            let thread_finished = Arc::clone(&finished);
            let thread_label = label.clone();
            let thread = std::thread::Builder::new()
                .name(format!("agentdp-network-unit-{thread_label}"))
                .spawn(move || {
                    let event_loop = match EventLoop::new(
                        spec,
                        transport,
                        TestOutputSink {
                            status: status_tx,
                            events,
                        },
                        TestCommandSource { receiver: commands_rx },
                    ) {
                        Ok(event_loop) => event_loop,
                        Err(error) => {
                            let _sent = started_tx.send(Err(error.clone()));
                            thread_finished.store(true, Ordering::Release);
                            return NetworkExit::Failed(error);
                        }
                    };
                    let wake = event_loop.wake_handle();
                    let _sent = started_tx.send(Ok(wake));
                    let exit = event_loop.run();
                    thread_finished.store(true, Ordering::Release);
                    exit
                })
                .map_err(|error| InstanceNetworkError::TaskFailed {
                    label: label.clone(),
                    message: format!("failed to spawn unit network thread: {error}"),
                })?;
            let wake = match started_rx.recv() {
                Ok(Ok(wake)) => wake,
                Ok(Err(error)) => return Err(error),
                Err(_disconnected) => {
                    return Err(InstanceNetworkError::TaskFailed {
                        label,
                        message: "unit network thread stopped during startup".to_owned(),
                    });
                }
            };
            Ok(Self {
                label,
                commands: commands_tx,
                wake,
                status: status_rx,
                finished,
                thread,
            })
        }

        fn label(&self) -> &str {
            &self.label
        }

        fn status(&self) -> InstanceNetworkStatus {
            self.status.borrow().clone()
        }

        fn is_running(&self) -> bool {
            !self.finished.load(Ordering::Acquire) && !self.status().is_terminal()
        }

        async fn wait_ready(&self, timeout: Duration) -> Result<(), InstanceNetworkError> {
            let mut status = self.status.clone();
            let label = self.label.clone();
            match tokio::time::timeout(timeout, async {
                loop {
                    let current = status.borrow().clone();
                    if current.is_ready() {
                        return Ok(());
                    }
                    match current.state {
                        InstanceNetworkState::Stopped => {
                            return Err(InstanceNetworkError::StoppedBeforeReady { label: label.clone() });
                        }
                        InstanceNetworkState::Failed { error } => {
                            return Err(InstanceNetworkError::TaskFailed {
                                label: label.clone(),
                                message: error,
                            });
                        }
                        _ => {}
                    }
                    if status.changed().await.is_err() {
                        return Err(InstanceNetworkError::StoppedBeforeReady { label: label.clone() });
                    }
                }
            })
            .await
            {
                Ok(result) => result,
                Err(_elapsed) => Err(InstanceNetworkError::ReadyTimeout { label, timeout }),
            }
        }

        async fn stop(self) -> Result<(), InstanceNetworkError> {
            let _sent = self.commands.send(NetworkCommand::Stop);
            let _woken = self.wake.wake();
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if self.thread.is_finished() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .map_err(|_elapsed| InstanceNetworkError::StopTimeout {
                label: self.label.clone(),
                timeout: Duration::from_secs(1),
            })?;
            match self.thread.join() {
                Ok(NetworkExit::Stopped) => Ok(()),
                Ok(NetworkExit::Failed(error)) => Err(error),
                Err(_panic) => Err(InstanceNetworkError::TaskFailed {
                    label: self.label,
                    message: "unit network thread panicked".to_owned(),
                }),
            }
        }
    }

    impl NetworkCommandSource for TestCommandSource {
        fn try_recv(&mut self) -> Option<NetworkCommand> {
            match self.receiver.try_recv() {
                Ok(command) => Some(command),
                Err(mpsc::TryRecvError::Empty) => None,
                Err(mpsc::TryRecvError::Disconnected) => Some(NetworkCommand::Stop),
            }
        }
    }

    impl NetworkEventSink for TestOutputSink {
        fn emit(&mut self, fill: impl FnOnce(&mut NetworkEventEnvelope)) {
            let mut event = NetworkEventEnvelope::default();
            fill(&mut event);
            let mut status = self.status.borrow().clone();
            status.observe_event(&event);
            let _sent = self.status.send(status);
            let Some(events) = &self.events else {
                return;
            };
            events.lock().unwrap().push(event);
        }

        fn flush(&mut self) {}
    }

    async fn wait_until(
        timeout: Duration,
        mut condition: impl FnMut() -> bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if condition() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err("timed out waiting for network test condition".into())
    }

    fn spec() -> InstanceNetworkSpec {
        spec_with_reconnect(Duration::from_mins(1))
    }

    fn spec_with_reconnect(reconnect_delay: Duration) -> InstanceNetworkSpec {
        let mut config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        config.limits.status_publish_interval = Duration::from_millis(10);
        InstanceNetworkSpec {
            label: "unit-test".to_owned(),
            config,
            reconnect_delay,
            write_timeout: Duration::from_millis(50),
        }
    }

    #[derive(Clone)]
    struct FakeTransport {
        state: Arc<Mutex<FakeTransportState>>,
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    struct FakeTransportState {
        sessions: VecDeque<FakeFrames>,
        fail_when_empty: bool,
    }

    #[derive(Default)]
    struct FakeFrames {
        frames: VecDeque<Vec<u8>>,
        wait_when_empty: bool,
    }

    impl FakeTransport {
        fn fail() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeTransportState {
                    sessions: VecDeque::new(),
                    fail_when_empty: true,
                })),
                writes: Arc::default(),
            }
        }

        fn frames(frames: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self::sessions([FakeFrames::closed_after(frames)])
        }

        fn frames_then_wait(frames: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self::sessions([FakeFrames::waiting_after(frames)])
        }

        fn sessions(sessions: impl IntoIterator<Item = FakeFrames>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeTransportState {
                    sessions: sessions.into_iter().collect(),
                    fail_when_empty: false,
                })),
                writes: Arc::default(),
            }
        }

        fn writes(&self) -> Arc<Mutex<Vec<Vec<u8>>>> {
            self.writes.clone()
        }
    }

    impl FakeFrames {
        fn closed_after(frames: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                frames: frames.into_iter().collect(),
                wait_when_empty: false,
            }
        }

        fn waiting_after(frames: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                frames: frames.into_iter().collect(),
                wait_when_empty: true,
            }
        }
    }

    impl GuestFrameTransport for FakeTransport {
        type Session = FakeSession;

        fn try_connect(&mut self) -> Result<ConnectStatus<Self::Session>, TransportError> {
            let session = match self.state.lock() {
                Ok(mut state) => match state.sessions.pop_front() {
                    Some(frames) => fake_session(frames, self.writes.clone()),
                    None if state.fail_when_empty => Err(TransportError::operation("connect fake transport", "failed")),
                    None => fake_session(FakeFrames::waiting_after([]), self.writes.clone()),
                },
                Err(error) => Err(TransportError::operation("lock fake transport", error)),
            }?;
            Ok(ConnectStatus::Connected(session))
        }

        fn cleanup(self) -> Result<(), TransportError> {
            Ok(())
        }

        fn describe(&self) -> String {
            "fake transport".to_owned()
        }
    }

    struct FakeSession {
        frames: VecDeque<Vec<u8>>,
        wait_when_empty: bool,
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
        wake: std::os::unix::net::UnixStream,
    }

    impl GuestFrameSession for FakeSession {
        fn io_source(&mut self) -> GuestIoSource<'_> {
            GuestIoSource::Fd(self.wake.as_fd())
        }

        fn read_frame_into(&mut self, frame: &mut FrameBuf) -> Result<FrameRead, TransportError> {
            drain_fake_wake(&mut self.wake)?;
            let Some(bytes) = self.frames.pop_front() else {
                return if self.wait_when_empty {
                    Ok(FrameRead::Blocked)
                } else {
                    Ok(FrameRead::Closed)
                };
            };
            frame.as_mut_vec().extend_from_slice(&bytes);
            Ok(FrameRead::Frame)
        }

        fn write_frame(&mut self, frame: &[u8]) -> Result<FrameWrite, TransportError> {
            self.writes
                .lock()
                .map_err(|error| TransportError::operation("record fake frame write", error))?
                .push(frame.to_vec());
            Ok(FrameWrite::Flushed)
        }

        fn shutdown_write(&mut self) -> Result<(), TransportError> {
            Ok(())
        }
    }

    fn fake_session(frames: FakeFrames, writes: Arc<Mutex<Vec<Vec<u8>>>>) -> Result<FakeSession, TransportError> {
        let (wake, mut signal) = std::os::unix::net::UnixStream::pair()
            .map_err(|error| TransportError::operation("open fake transport wake stream", error))?;
        wake.set_nonblocking(true)
            .map_err(|error| TransportError::operation("configure fake transport wake stream", error))?;
        signal
            .set_nonblocking(true)
            .map_err(|error| TransportError::operation("configure fake transport signal stream", error))?;
        if !frames.frames.is_empty() || !frames.wait_when_empty {
            let _sent = signal.write(&[1]);
        }
        Ok(FakeSession {
            frames: frames.frames,
            wait_when_empty: frames.wait_when_empty,
            writes,
            wake,
        })
    }

    fn drain_fake_wake(wake: &mut std::os::unix::net::UnixStream) -> Result<(), TransportError> {
        let mut buffer = [0_u8; 16];
        loop {
            match wake.read(&mut buffer) {
                Ok(0) => return Ok(()),
                Ok(_len) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => return Err(TransportError::operation("drain fake transport wake stream", error)),
            }
        }
    }

    fn arp_request() -> Vec<u8> {
        let repr = ArpRepr::EthernetIpv4 {
            operation: ArpOperation::Request,
            source_hardware_addr: TEST_MAC.guest.smoltcp(),
            source_protocol_addr: Ipv4Address::new(10, 73, 0, 10),
            target_hardware_addr: EthernetAddress([0, 0, 0, 0, 0, 0]),
            target_protocol_addr: Ipv4Address::new(10, 73, 0, 1),
        };
        let mut frame = vec![0; ETHERNET_HEADER_LEN + repr.buffer_len()];
        let mut ethernet = EthernetFrame::new_unchecked(frame.as_mut_slice());
        ethernet.set_src_addr(TEST_MAC.guest.smoltcp());
        ethernet.set_dst_addr(EthernetAddress::BROADCAST);
        ethernet.set_ethertype(EthernetProtocol::Arp);
        repr.emit(&mut ArpPacket::new_unchecked(ethernet.payload_mut()));
        frame
    }

    fn assert_arp_reply_for_gateway(frame: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let ethernet = EthernetFrame::new_checked(frame)?;
        assert_eq!(ethernet.ethertype(), EthernetProtocol::Arp);
        let arp = ArpPacket::new_checked(ethernet.payload())?;
        let ArpRepr::EthernetIpv4 {
            operation,
            source_hardware_addr,
            source_protocol_addr,
            target_hardware_addr,
            target_protocol_addr,
        } = ArpRepr::parse(&arp)?
        else {
            return Err("expected Ethernet IPv4 ARP reply".into());
        };
        assert_eq!(operation, ArpOperation::Reply);
        assert_eq!(source_hardware_addr, TEST_MAC.gateway.smoltcp());
        assert_eq!(source_protocol_addr, Ipv4Address::new(10, 73, 0, 1));
        assert_eq!(target_hardware_addr, TEST_MAC.guest.smoltcp());
        assert_eq!(target_protocol_addr, Ipv4Address::new(10, 73, 0, 10));
        Ok(())
    }

    #[test]
    fn config_defaults_are_conservative() {
        let config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());

        assert!(config.host_ports.is_empty());
        assert_eq!(config.limits.command_inbox_capacity, 1024);
        assert_eq!(config.limits.tcp_proxy_limit, 128);
        assert!(!config.ipv6_enabled);
        assert_eq!(config.mtu, 1500);
        assert_eq!(config.dns_upstream.port(), 53);
        assert_eq!(SocketAddr::new(IpAddr::V4(config.network.gateway.std()), 53).port(), 53);
    }
}
