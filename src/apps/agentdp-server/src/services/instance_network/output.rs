use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use agentdp_core::agent::{AgentInstanceNetworkEvent, AgentInstanceNetworkEventKind, PortProtocolState};
use agentdp_ds::{local, sync};
use agentdp_network::{
    HostPortProtocol, InstanceNetworkStatus, NetworkDnsEvent, NetworkEgressEvent, NetworkEgressProtocol, NetworkEvent,
    NetworkEventEnvelope, NetworkEventSink, NetworkHostPortEvent, NetworkLifecycleEvent, NetworkReactorEvent,
    NetworkStateEvent, NetworkTelemetryEvent, NetworkTransportEvent,
};
use tokio::sync::watch;

const NETWORK_EVENT_PUMP_IDLE_DELAY: Duration = Duration::from_millis(10);
const NETWORK_EVENT_PUMP_BATCH_SIZE: usize = 32;

use super::InstanceNetworkObservation;

#[derive(Debug)]
pub(super) struct NetworkEventProducer {
    producer: sync::ring::BufferedProducer<NetworkEventEnvelope>,
    dropped_events: u64,
}

pub(super) struct NetworkOutputDrain {
    consumer: sync::ring::Consumer<NetworkEventEnvelope>,
    events: Rc<RefCell<local::spsc::Sender<AgentInstanceNetworkEvent>>>,
    observation: InstanceNetworkObservation,
    observation_tx: watch::Sender<InstanceNetworkObservation>,
}

impl NetworkEventProducer {
    const fn new(producer: sync::ring::BufferedProducer<NetworkEventEnvelope>) -> Self {
        Self {
            producer,
            dropped_events: 0,
        }
    }
}

impl NetworkEventSink for NetworkEventProducer {
    fn emit(&mut self, fill: impl FnOnce(&mut NetworkEventEnvelope)) {
        let dropped_events = self.dropped_events;
        match self.producer.write_with(|slot| {
            fill(slot);
            slot.dropped_events_before = dropped_events;
        }) {
            Ok(()) => {
                self.dropped_events = 0;
            }
            Err(sync::ring::TryReserveError::Disconnected) => {}
            Err(sync::ring::TryReserveError::Full) => {
                self.dropped_events = self.dropped_events.saturating_add(1);
            }
        }
    }

    fn flush(&mut self) {
        self.producer.flush();
    }
}

pub(super) fn network_outputs(
    initial_status: InstanceNetworkStatus,
    channel_capacity: usize,
    events: Rc<RefCell<local::spsc::Sender<AgentInstanceNetworkEvent>>>,
) -> (
    NetworkEventProducer,
    watch::Receiver<InstanceNetworkObservation>,
    NetworkOutputDrain,
) {
    let (producer, consumer) = sync::ring::buffered(channel_capacity, NETWORK_EVENT_PUMP_BATCH_SIZE);
    let observation = InstanceNetworkObservation {
        status: initial_status,
        event_drops: 0,
    };
    let (observation_tx, observation_rx) = watch::channel(observation.clone());
    let drain = NetworkOutputDrain {
        consumer,
        events,
        observation,
        observation_tx,
    };
    (NetworkEventProducer::new(producer), observation_rx, drain)
}

pub(super) async fn drain_network_outputs(drain: NetworkOutputDrain) {
    let NetworkOutputDrain {
        mut consumer,
        events,
        mut observation,
        observation_tx,
    } = drain;
    let mut batch_events = vec![NetworkEventEnvelope::default(); NETWORK_EVENT_PUMP_BATCH_SIZE].into_boxed_slice();
    let mut actor_event_drops = 0_u64;
    loop {
        let mut disconnected = false;
        let mut drained = 0;
        loop {
            match consumer.try_read_batch(NETWORK_EVENT_PUMP_BATCH_SIZE) {
                Ok(batch) => {
                    let len = batch.len();
                    batch.for_each(|index, event| {
                        batch_events[index] = event.clone();
                    });
                    drop(batch);
                    drained += len;
                    for event in &batch_events[..len] {
                        observation.status.observe_event(event);
                        observation.event_drops = observation.event_drops.saturating_add(event.dropped_events_before);
                        let mut event = instance_network_event(event);
                        event.dropped_events_before = event.dropped_events_before.saturating_add(actor_event_drops);
                        match events.borrow_mut().try_send(event.clone()) {
                            Ok(()) => {
                                actor_event_drops = 0;
                            }
                            Err(local::spsc::TrySendError::Full(_event)) => {
                                actor_event_drops = actor_event_drops.saturating_add(1);
                                observation.event_drops = observation.event_drops.saturating_add(1);
                            }
                            Err(local::spsc::TrySendError::Disconnected(_event)) => {}
                        }
                    }
                    publish_observation(&observation, &observation_tx);
                }
                Err(sync::ring::TryReadError::Empty) => break,
                Err(sync::ring::TryReadError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        if disconnected {
            return;
        }
        if drained == 0 {
            tokio::time::sleep(NETWORK_EVENT_PUMP_IDLE_DELAY).await;
        } else {
            tokio::task::yield_now().await;
        }
    }
}

fn publish_observation(
    observation: &InstanceNetworkObservation,
    observation_tx: &watch::Sender<InstanceNetworkObservation>,
) {
    let _sent = observation_tx.send(observation.clone());
}

fn instance_network_event(event: &NetworkEventEnvelope) -> AgentInstanceNetworkEvent {
    AgentInstanceNetworkEvent {
        sequence: event.sequence,
        unix_millis: event.unix_millis,
        dropped_events_before: event.dropped_events_before,
        event: match &event.event {
            NetworkEvent::Lifecycle(NetworkLifecycleEvent::StateChanged { state }) => {
                AgentInstanceNetworkEventKind::LifecycleStateChanged {
                    state: instance_network_state_name(state).to_owned(),
                }
            }
            NetworkEvent::Telemetry(NetworkTelemetryEvent::Snapshot(snapshot)) => {
                AgentInstanceNetworkEventKind::TelemetrySnapshot {
                    started_unix_seconds: snapshot.started_unix_seconds,
                    last_state_change_unix_seconds: snapshot.last_state_change_unix_seconds,
                    last_transport_connect_unix_seconds: snapshot.last_transport_connect_unix_seconds,
                    last_guest_frame_unix_seconds: snapshot.last_guest_frame_unix_seconds,
                    last_host_frame_unix_seconds: snapshot.last_host_frame_unix_seconds,
                    guest_frames_received: snapshot.guest_frames_received,
                    guest_bytes_received: snapshot.guest_bytes_received,
                    host_frames_sent: snapshot.host_frames_sent,
                    host_bytes_sent: snapshot.host_bytes_sent,
                    session_disconnects: snapshot.session_disconnects,
                    connect_errors: snapshot.connect_errors,
                    egress_errors: snapshot.egress_errors,
                }
            }
            NetworkEvent::Transport(NetworkTransportEvent::ConnectFailed { transport, error }) => {
                AgentInstanceNetworkEventKind::TransportConnectFailed {
                    transport: transport.to_string(),
                    error: error.to_string(),
                }
            }
            NetworkEvent::Transport(NetworkTransportEvent::GuestConnected { transport, generation }) => {
                AgentInstanceNetworkEventKind::TransportGuestConnected {
                    transport: transport.to_string(),
                    generation: *generation,
                }
            }
            NetworkEvent::Transport(NetworkTransportEvent::GuestDisconnected { generation, reason }) => {
                AgentInstanceNetworkEventKind::TransportGuestDisconnected {
                    generation: *generation,
                    reason: reason.to_string(),
                }
            }
            NetworkEvent::Transport(NetworkTransportEvent::RegisterFailed { transport, error }) => {
                AgentInstanceNetworkEventKind::TransportRegisterFailed {
                    transport: transport.to_string(),
                    error: error.to_string(),
                }
            }
            NetworkEvent::Egress(NetworkEgressEvent::Error(error)) => AgentInstanceNetworkEventKind::EgressError {
                protocol: egress_protocol_name(error.protocol).to_owned(),
                proxy: error.proxy,
                destination: error.destination.map(|value| value.to_string()),
                upstream: error.upstream.map(|value| value.to_string()),
                authority: error.authority.map(|value| value.to_string()),
                route: error.route.map(|value| value.to_string()),
                phase: error.phase.map(|value| value.to_string()),
                message: error.message.to_string(),
            },
            NetworkEvent::Egress(NetworkEgressEvent::ProxyClosed { protocol, proxy }) => {
                AgentInstanceNetworkEventKind::EgressProxyClosed {
                    protocol: egress_protocol_name(*protocol).to_owned(),
                    proxy: *proxy,
                }
            }
            NetworkEvent::Dns(NetworkDnsEvent::Resolved {
                protocol,
                host,
                addresses,
                ttl,
            }) => AgentInstanceNetworkEventKind::DnsResolved {
                protocol: egress_protocol_name(*protocol).to_owned(),
                host: host.to_string(),
                addresses: addresses.iter().collect(),
                ttl_millis: u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX),
            },
            NetworkEvent::HostPort(NetworkHostPortEvent::Bound {
                name,
                protocol,
                guest,
                host,
            }) => AgentInstanceNetworkEventKind::HostPortBound {
                name: name.as_str().to_owned(),
                protocol: host_port_protocol_state(*protocol),
                guest: *guest,
                host: *host,
            },
            NetworkEvent::HostPort(NetworkHostPortEvent::Error { message }) => {
                AgentInstanceNetworkEventKind::HostPortError {
                    message: message.to_string(),
                }
            }
            NetworkEvent::Reactor(NetworkReactorEvent::Error { message }) => {
                AgentInstanceNetworkEventKind::ReactorError {
                    message: message.to_string(),
                }
            }
        },
    }
}

const fn instance_network_state_name(state: &NetworkStateEvent) -> &'static str {
    match state {
        NetworkStateEvent::Starting => "starting",
        NetworkStateEvent::Connecting { .. } => "connecting",
        NetworkStateEvent::Connected { .. } => "connected",
        NetworkStateEvent::TrafficObserved { .. } => "traffic-observed",
        NetworkStateEvent::Backoff { .. } => "backoff",
        NetworkStateEvent::Stopping => "stopping",
        NetworkStateEvent::Stopped => "stopped",
        NetworkStateEvent::Failed { .. } => "failed",
    }
}

const fn egress_protocol_name(protocol: NetworkEgressProtocol) -> &'static str {
    match protocol {
        NetworkEgressProtocol::Tcp => "tcp",
        NetworkEgressProtocol::Udp => "udp",
    }
}

const fn host_port_protocol_state(protocol: HostPortProtocol) -> PortProtocolState {
    match protocol {
        HostPortProtocol::Tcp => PortProtocolState::Tcp,
        HostPortProtocol::Udp => PortProtocolState::Udp,
    }
}

#[cfg(test)]
mod tests {
    use agentdp_core::agent::AgentInstanceNetworkEventKind;
    use agentdp_ds::local;
    use agentdp_network::{
        InstanceNetworkStatus, NetworkEvent, NetworkEventEnvelope, NetworkEventSink as _, NetworkEventText,
        NetworkTransportEvent,
    };

    #[tokio::test(flavor = "local")]
    async fn pumps_ringbuffer_events_to_recent_history() {
        let initial_status = InstanceNetworkStatus::starting(&test_network_config().limits);
        let (event_tx, mut event_rx) = local::spsc::bounded(8);
        let (mut sink, observation, drain) =
            super::network_outputs(initial_status, 2, std::rc::Rc::new(std::cell::RefCell::new(event_tx)));
        sink.emit(|slot| {
            *slot = NetworkEventEnvelope {
                sequence: 9,
                unix_millis: 44,
                dropped_events_before: 0,
                event: NetworkEvent::Transport(NetworkTransportEvent::ConnectFailed {
                    transport: NetworkEventText::from_str("qemu stream"),
                    error: NetworkEventText::from_str("closed"),
                }),
            };
        });
        drop(sink);
        Box::pin(super::drain_network_outputs(drain)).await;

        assert_eq!(observation.borrow().status.telemetry.connect_errors, 1);
        let event = event_rx.recv().await.unwrap();
        assert_eq!(event.sequence, 9);
        assert!(matches!(
            event.event,
            AgentInstanceNetworkEventKind::TransportConnectFailed { .. }
        ));
    }

    fn test_network_config() -> agentdp_network::InstanceNetworkConfig {
        agentdp_network::InstanceNetworkConfig::new(
            mediated_network_addresses(),
            mediated_network_mac(),
            agentdp_network::EgressPolicy::allow_all(),
        )
    }

    fn mediated_network_addresses() -> agentdp_network::InstanceAddresses {
        let profile = agentdp_core::mediated_network::DEFAULT_PROFILE;
        agentdp_network::InstanceAddresses {
            gateway: ipv4_address(profile.gateway_ipv4),
            address: ipv4_address(profile.guest_ipv4),
            cidr_prefix: profile.ipv4_cidr_prefix,
        }
    }

    fn mediated_network_mac() -> agentdp_network::InstanceMacAddresses {
        let profile = agentdp_core::mediated_network::DEFAULT_PROFILE;
        agentdp_network::InstanceMacAddresses {
            gateway: agentdp_network::MacAddress::new(profile.gateway_mac.octets()),
            guest: agentdp_network::MacAddress::new(profile.guest_mac.octets()),
        }
    }

    fn ipv4_address(address: std::net::Ipv4Addr) -> agentdp_network::Ipv4AddressText {
        let [a, b, c, d] = address.octets();
        agentdp_network::Ipv4AddressText(smoltcp::wire::Ipv4Address::new(a, b, c, d))
    }
}
