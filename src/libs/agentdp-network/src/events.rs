use std::fmt::{self, Write as _};
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use serde::ser::SerializeSeq as _;
use serde::{Serialize, Serializer};

use crate::network::{HostPortProtocol, InstanceNetworkState, InstanceNetworkStatus, InstanceNetworkTelemetry};

const NETWORK_EVENT_TEXT_CAPACITY: usize = 96;
const NETWORK_EVENT_ADDRESS_CAPACITY: usize = 8;
const NETWORK_EVENT_ADDRESS_CAPACITY_U8: u8 = 8;

pub type NetworkEventText = InlineText<NETWORK_EVENT_TEXT_CAPACITY>;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct NetworkEventEnvelope {
    pub sequence: u64,
    pub unix_millis: u64,
    pub dropped_events_before: u64,
    pub event: NetworkEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "data")]
pub enum NetworkEvent {
    Lifecycle(NetworkLifecycleEvent),
    Telemetry(NetworkTelemetryEvent),
    Transport(NetworkTransportEvent),
    Egress(NetworkEgressEvent),
    Dns(NetworkDnsEvent),
    HostPort(NetworkHostPortEvent),
    Reactor(NetworkReactorEvent),
}

impl Default for NetworkEvent {
    fn default() -> Self {
        Self::Lifecycle(NetworkLifecycleEvent::default())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "data")]
pub enum NetworkLifecycleEvent {
    StateChanged { state: NetworkStateEvent },
}

impl Default for NetworkLifecycleEvent {
    fn default() -> Self {
        Self::StateChanged {
            state: NetworkStateEvent::Starting,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "data")]
pub enum NetworkStateEvent {
    Starting,
    Connecting {
        transport: NetworkEventText,
    },
    Connected {
        generation: u64,
    },
    TrafficObserved {
        generation: u64,
    },
    Backoff {
        generation: u64,
        reason: NetworkEventText,
        reconnect_after: Duration,
    },
    Stopping,
    Stopped,
    Failed {
        error: NetworkEventText,
    },
}

impl NetworkStateEvent {
    #[must_use]
    pub fn from_state(state: &InstanceNetworkState) -> Self {
        match state {
            InstanceNetworkState::Starting => Self::Starting,
            InstanceNetworkState::Connecting { transport } => Self::Connecting {
                transport: NetworkEventText::from_str(transport),
            },
            InstanceNetworkState::Connected { generation } => Self::Connected {
                generation: *generation,
            },
            InstanceNetworkState::TrafficObserved { generation } => Self::TrafficObserved {
                generation: *generation,
            },
            InstanceNetworkState::Backoff {
                generation,
                reason,
                reconnect_after,
            } => Self::Backoff {
                generation: *generation,
                reason: NetworkEventText::from_str(reason),
                reconnect_after: *reconnect_after,
            },
            InstanceNetworkState::Stopping => Self::Stopping,
            InstanceNetworkState::Stopped => Self::Stopped,
            InstanceNetworkState::Failed { error } => Self::Failed {
                error: NetworkEventText::from_str(error),
            },
        }
    }

    #[must_use]
    pub fn to_state(self) -> InstanceNetworkState {
        match self {
            Self::Starting => InstanceNetworkState::Starting,
            Self::Connecting { transport } => InstanceNetworkState::Connecting {
                transport: transport.as_str().to_owned(),
            },
            Self::Connected { generation } => InstanceNetworkState::Connected { generation },
            Self::TrafficObserved { generation } => InstanceNetworkState::TrafficObserved { generation },
            Self::Backoff {
                generation,
                reason,
                reconnect_after,
            } => InstanceNetworkState::Backoff {
                generation,
                reason: reason.as_str().to_owned(),
                reconnect_after,
            },
            Self::Stopping => InstanceNetworkState::Stopping,
            Self::Stopped => InstanceNetworkState::Stopped,
            Self::Failed { error } => InstanceNetworkState::Failed {
                error: error.as_str().to_owned(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "data")]
pub enum NetworkTelemetryEvent {
    Snapshot(NetworkTelemetrySnapshot),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct NetworkTelemetrySnapshot {
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
    pub buffer_frame_available: u64,
    pub buffer_small_byte_available: u64,
    pub buffer_medium_byte_available: u64,
    pub buffer_tcp_byte_available: u64,
    pub tcp_proxy_active_slots: u64,
    pub tcp_proxy_upstream_read_ready: u64,
    pub tcp_proxy_upstream_read_masked: u64,
    pub tcp_proxy_guest_send_blocked: u64,
    pub tcp_proxy_pending_guest_bytes: u64,
}

impl NetworkTelemetrySnapshot {
    #[must_use]
    pub const fn from_status(status: &InstanceNetworkStatus) -> Self {
        Self::from_telemetry(&status.telemetry)
    }

    #[must_use]
    pub const fn from_telemetry(telemetry: &InstanceNetworkTelemetry) -> Self {
        Self {
            started_unix_seconds: telemetry.started_unix_seconds,
            last_state_change_unix_seconds: telemetry.last_state_change_unix_seconds,
            last_transport_connect_unix_seconds: telemetry.last_transport_connect_unix_seconds,
            last_guest_frame_unix_seconds: telemetry.last_guest_frame_unix_seconds,
            last_host_frame_unix_seconds: telemetry.last_host_frame_unix_seconds,
            guest_frames_received: telemetry.guest_frames_received,
            guest_bytes_received: telemetry.guest_bytes_received,
            host_frames_sent: telemetry.host_frames_sent,
            host_bytes_sent: telemetry.host_bytes_sent,
            session_disconnects: telemetry.session_disconnects,
            connect_errors: telemetry.connect_errors,
            egress_errors: telemetry.egress_errors,
            telemetry_events_dropped: telemetry.telemetry_events_dropped,
            buffer_frame_available: telemetry.buffer_frame_available,
            buffer_small_byte_available: telemetry.buffer_small_byte_available,
            buffer_medium_byte_available: telemetry.buffer_medium_byte_available,
            buffer_tcp_byte_available: telemetry.buffer_tcp_byte_available,
            tcp_proxy_active_slots: telemetry.tcp_proxy_active_slots,
            tcp_proxy_upstream_read_ready: telemetry.tcp_proxy_upstream_read_ready,
            tcp_proxy_upstream_read_masked: telemetry.tcp_proxy_upstream_read_masked,
            tcp_proxy_guest_send_blocked: telemetry.tcp_proxy_guest_send_blocked,
            tcp_proxy_pending_guest_bytes: telemetry.tcp_proxy_pending_guest_bytes,
        }
    }

    pub const fn apply_to(self, telemetry: &mut InstanceNetworkTelemetry) {
        telemetry.started_unix_seconds = self.started_unix_seconds;
        telemetry.last_state_change_unix_seconds = self.last_state_change_unix_seconds;
        telemetry.last_transport_connect_unix_seconds = self.last_transport_connect_unix_seconds;
        telemetry.last_guest_frame_unix_seconds = self.last_guest_frame_unix_seconds;
        telemetry.last_host_frame_unix_seconds = self.last_host_frame_unix_seconds;
        telemetry.guest_frames_received = self.guest_frames_received;
        telemetry.guest_bytes_received = self.guest_bytes_received;
        telemetry.host_frames_sent = self.host_frames_sent;
        telemetry.host_bytes_sent = self.host_bytes_sent;
        telemetry.session_disconnects = self.session_disconnects;
        telemetry.connect_errors = self.connect_errors;
        telemetry.egress_errors = self.egress_errors;
        telemetry.telemetry_events_dropped = self.telemetry_events_dropped;
        telemetry.buffer_frame_available = self.buffer_frame_available;
        telemetry.buffer_small_byte_available = self.buffer_small_byte_available;
        telemetry.buffer_medium_byte_available = self.buffer_medium_byte_available;
        telemetry.buffer_tcp_byte_available = self.buffer_tcp_byte_available;
        telemetry.tcp_proxy_active_slots = self.tcp_proxy_active_slots;
        telemetry.tcp_proxy_upstream_read_ready = self.tcp_proxy_upstream_read_ready;
        telemetry.tcp_proxy_upstream_read_masked = self.tcp_proxy_upstream_read_masked;
        telemetry.tcp_proxy_guest_send_blocked = self.tcp_proxy_guest_send_blocked;
        telemetry.tcp_proxy_pending_guest_bytes = self.tcp_proxy_pending_guest_bytes;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "data")]
pub enum NetworkTransportEvent {
    ConnectFailed {
        transport: NetworkEventText,
        error: NetworkEventText,
    },
    GuestConnected {
        transport: NetworkEventText,
        generation: u64,
    },
    GuestDisconnected {
        generation: u64,
        reason: NetworkEventText,
    },
    RegisterFailed {
        transport: NetworkEventText,
        error: NetworkEventText,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum NetworkEgressProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "data")]
pub enum NetworkEgressEvent {
    Error(Box<NetworkEgressErrorEvent>),
    ProxyClosed {
        protocol: NetworkEgressProtocol,
        proxy: Option<u64>,
    },
}

impl NetworkEgressEvent {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn error(
        protocol: NetworkEgressProtocol,
        proxy: Option<u64>,
        destination: Option<NetworkEventText>,
        upstream: Option<NetworkEventText>,
        authority: Option<NetworkEventText>,
        route: Option<NetworkEventText>,
        phase: Option<NetworkEventText>,
        message: NetworkEventText,
    ) -> Self {
        Self::Error(Box::new(NetworkEgressErrorEvent {
            protocol,
            proxy,
            destination,
            upstream,
            authority,
            route,
            phase,
            message,
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct NetworkEgressErrorEvent {
    pub protocol: NetworkEgressProtocol,
    pub proxy: Option<u64>,
    pub destination: Option<NetworkEventText>,
    pub upstream: Option<NetworkEventText>,
    pub authority: Option<NetworkEventText>,
    pub route: Option<NetworkEventText>,
    pub phase: Option<NetworkEventText>,
    pub message: NetworkEventText,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "data")]
pub enum NetworkDnsEvent {
    Resolved {
        protocol: NetworkEgressProtocol,
        host: NetworkEventText,
        addresses: NetworkAddresses,
        ttl: Duration,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "data")]
pub enum NetworkHostPortEvent {
    Bound {
        name: NetworkEventText,
        protocol: HostPortProtocol,
        guest: u16,
        host: u16,
    },
    Error {
        message: NetworkEventText,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "data")]
pub enum NetworkReactorEvent {
    Error { message: NetworkEventText },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkAddresses {
    values: [IpAddr; NETWORK_EVENT_ADDRESS_CAPACITY],
    len: u8,
    truncated: bool,
}

impl Default for NetworkAddresses {
    fn default() -> Self {
        Self {
            values: [IpAddr::V4(Ipv4Addr::UNSPECIFIED); NETWORK_EVENT_ADDRESS_CAPACITY],
            len: 0,
            truncated: false,
        }
    }
}

impl NetworkAddresses {
    #[must_use]
    pub fn from_slice(addresses: &[IpAddr]) -> Self {
        let mut values = [IpAddr::V4(Ipv4Addr::UNSPECIFIED); NETWORK_EVENT_ADDRESS_CAPACITY];
        let len = addresses.len().min(NETWORK_EVENT_ADDRESS_CAPACITY);
        values[..len].copy_from_slice(&addresses[..len]);
        Self {
            values,
            len: u8::try_from(len).unwrap_or(NETWORK_EVENT_ADDRESS_CAPACITY_U8),
            truncated: addresses.len() > NETWORK_EVENT_ADDRESS_CAPACITY,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = IpAddr> + '_ {
        self.values[..usize::from(self.len)].iter().copied()
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

impl Serialize for NetworkAddresses {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(usize::from(self.len)))?;
        for address in self.iter() {
            seq.serialize_element(&address)?;
        }
        seq.end()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InlineText<const N: usize> {
    bytes: [u8; N],
    len: u16,
    truncated: bool,
}

impl<const N: usize> InlineText<N> {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            bytes: [0; N],
            len: 0,
            truncated: false,
        }
    }

    #[must_use]
    pub fn from_str(value: &str) -> Self {
        let mut text = Self::empty();
        text.push_str_lossy(value);
        text
    }

    #[must_use]
    pub fn from_display(value: impl fmt::Display) -> Self {
        let mut text = Self::empty();
        let _ = write!(&mut text, "{value}");
        text
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..usize::from(self.len)]).unwrap_or_default()
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }

    fn push_str_lossy(&mut self, value: &str) {
        for char in value.chars() {
            self.push_char(char);
        }
    }

    fn push_char(&mut self, char: char) {
        if self.truncated {
            return;
        }
        let mut buffer = [0; 4];
        let encoded = char.encode_utf8(&mut buffer);
        let len = usize::from(self.len);
        if len + encoded.len() > N {
            self.truncated = true;
            return;
        }
        self.bytes[len..len + encoded.len()].copy_from_slice(encoded.as_bytes());
        self.len = u16::try_from(len + encoded.len()).unwrap_or(u16::MAX);
    }
}

impl<const N: usize> Default for InlineText<N> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<const N: usize> fmt::Display for InlineText<N> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<const N: usize> fmt::Write for InlineText<N> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.push_str_lossy(value);
        Ok(())
    }
}

impl<const N: usize> Serialize for InlineText<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

pub trait NetworkEventSink {
    fn emit(&mut self, fill: impl FnOnce(&mut NetworkEventEnvelope));

    fn flush(&mut self);
}

#[cfg(test)]
mod tests {
    use super::{InlineText, NetworkAddresses};

    #[test]
    fn inline_text_truncates_on_valid_utf8_boundary() {
        let text = InlineText::<5>::from_str("abcdøef");

        assert_eq!(text.as_str(), "abcd");
        assert!(text.truncated());
    }

    #[test]
    fn network_addresses_are_inline_and_bounded() {
        let addresses = (1..=10)
            .map(|octet| std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, octet)))
            .collect::<Vec<_>>();

        let inline = NetworkAddresses::from_slice(&addresses);

        assert_eq!(inline.iter().count(), 8);
        assert!(inline.truncated());
    }
}
