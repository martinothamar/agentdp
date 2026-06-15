use std::time::Duration;
use std::time::UNIX_EPOCH;

#[cfg(any(test, feature = "simulation"))]
use crate::buffers::BufferPoolSnapshot;
use crate::buffers::{BufferPool, FrameBuf};
use crate::clock::NetworkClock as _;
use crate::command::{NetworkCommand, NetworkCommandSource};
use crate::drive::DriveBudget;
use crate::egress::tcp::{TcpProxies, TcpProxyEvent};
use crate::egress::udp::{UdpProxies, UdpProxyEvent};
use crate::events::{
    NetworkAddresses, NetworkDnsEvent, NetworkEgressEvent, NetworkEgressProtocol, NetworkEvent, NetworkEventEnvelope,
    NetworkEventSink, NetworkEventText, NetworkHostPortEvent, NetworkLifecycleEvent, NetworkReactorEvent,
    NetworkStateEvent, NetworkTelemetryEvent, NetworkTelemetrySnapshot, NetworkTransportEvent,
};
use crate::gateway::Gateway;
use crate::guest::{ConnectStatus, GuestEvent, GuestFrameTransport, GuestIo, TransportError};
use crate::ingress::TcpConnections;
use crate::ingress::UdpPeers;
use crate::ingress::{HostPortEvent, HostPorts};
use crate::network::{
    EgressUdpSend, HostConnectionId, IngressTcpWrite, IngressUdpSend, InstanceNetworkError, InstanceNetworkSpec,
    InstanceNetworkState, InstanceNetworkStatus,
};
use crate::reactor::{ProductionWake, ReactorBackend, ReactorReady};
use crate::runtime::{NetworkRuntime, ProductionRuntime, production_runtime};
use crate::timer::{TIMER_QUEUE_REQUIRED_CAPACITY, TimerId, TimerQueue};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkExit {
    Stopped,
    Failed(InstanceNetworkError),
}

pub(crate) enum ConnectOutcome {
    Connected,
    Pending,
}

pub(crate) enum DriveOutcome {
    Continue,
    Stop,
    Reconnect,
}

pub struct EventLoop<T, O, C>
where
    T: GuestFrameTransport,
    O: NetworkEventSink,
    C: NetworkCommandSource,
{
    core: EventLoopCore<ProductionRuntime<T>, O, C>,
}

impl<T, O, C> EventLoop<T, O, C>
where
    T: GuestFrameTransport,
    O: NetworkEventSink,
    C: NetworkCommandSource,
{
    /// # Errors
    ///
    /// Returns an error when the production runtime cannot be created or the event loop cannot initialize.
    pub fn new(spec: InstanceNetworkSpec, transport: T, outputs: O, commands: C) -> Result<Self, InstanceNetworkError> {
        let runtime = production_runtime(transport, spec.config.limits.reactor_event_capacity).map_err(|error| {
            InstanceNetworkError::TaskFailed {
                label: spec.label.clone(),
                message: format!("failed to build instance network runtime: {error}"),
            }
        })?;
        EventLoopCore::new(spec, runtime, commands, outputs).map(|core| Self { core })
    }

    #[must_use]
    pub fn wake_handle(&self) -> ProductionWake {
        self.core.wake_handle()
    }

    pub fn run(self) -> NetworkExit {
        self.core.run()
    }
}

struct ComponentEvents {
    guest: Vec<GuestEvent>,
    host_ports: Vec<HostPortEvent>,
    egress_tcp: Vec<TcpProxyEvent>,
    egress_udp: Vec<UdpProxyEvent>,
}

impl ComponentEvents {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            guest: Vec::with_capacity(capacity),
            host_ports: Vec::with_capacity(capacity),
            egress_tcp: Vec::with_capacity(capacity),
            egress_udp: Vec::with_capacity(capacity),
        }
    }

    const fn is_empty(&self) -> bool {
        self.guest.is_empty() && self.host_ports.is_empty() && self.egress_tcp.is_empty() && self.egress_udp.is_empty()
    }
}

struct ComponentOutputQueues {
    // Components do not call each other directly while they are being driven.
    // They append side effects here, then the event loop applies the batch in a
    // fixed order after the current component phase completes.
    //
    // Direction names are from the guest's point of view:
    // egress leaves the guest toward an upstream server, ingress enters the
    // guest from a host port, and guest_frames are Ethernet frames to deliver
    // back to the guest transport.
    guest_frames: Vec<FrameBuf>,
    egress_udp_sends: Vec<EgressUdpSend>,
    ingress_tcp_writes: Vec<IngressTcpWrite>,
    ingress_tcp_closes: Vec<HostConnectionId>,
    ingress_udp_sends: Vec<IngressUdpSend>,
}

impl ComponentOutputQueues {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            guest_frames: Vec::with_capacity(capacity),
            egress_udp_sends: Vec::with_capacity(capacity),
            ingress_tcp_writes: Vec::with_capacity(capacity),
            ingress_tcp_closes: Vec::with_capacity(capacity),
            ingress_udp_sends: Vec::with_capacity(capacity),
        }
    }

    const fn is_empty(&self) -> bool {
        self.guest_frames.is_empty()
            && self.egress_udp_sends.is_empty()
            && self.ingress_tcp_writes.is_empty()
            && self.ingress_tcp_closes.is_empty()
            && self.ingress_udp_sends.is_empty()
    }
}

pub(crate) struct EventLoopCore<R, O, C>
where
    R: NetworkRuntime,
    O: NetworkEventSink,
    C: NetworkCommandSource,
{
    spec: InstanceNetworkSpec,
    runtime: R,
    outputs: O,
    event_sequence: u64,
    commands: C,
    guest: Option<GuestIo<<R::Transport as crate::guest::GuestFrameTransport>::Session>>,
    host_ports: Option<HostPorts<R::Reactor>>,
    egress_tcp: TcpProxies<R::Reactor>,
    ingress_udp: UdpPeers,
    ingress_tcp: TcpConnections,
    egress_udp: UdpProxies<R::Reactor>,
    gateway: Gateway<R::Clock>,
    buffers: BufferPool,
    component_outputs: ComponentOutputQueues,
    component_events: ComponentEvents,
    reactor_ready: Vec<ReactorReady>,
    timers: TimerQueue<R::Clock>,
    expired_timers: Vec<TimerId>,
    status: InstanceNetworkStatus,
    status_dirty: bool,
    generation: u64,
    guest_disconnect: Option<(u64, String)>,
}

impl<R, O, C> EventLoopCore<R, O, C>
where
    R: NetworkRuntime,
    O: NetworkEventSink,
    C: NetworkCommandSource,
{
    pub(crate) fn new(
        spec: InstanceNetworkSpec,
        runtime: R,
        commands: C,
        outputs: O,
    ) -> Result<Self, InstanceNetworkError> {
        let limits = spec.config.limits.clone();
        let buffers = BufferPool::new(limits.clone());
        buffers.prewarm_instance_network();
        let mut timers = TimerQueue::new(limits.timer_queue_capacity, runtime.clock().clone()).map_err(|error| {
            InstanceNetworkError::TaskFailed {
                label: spec.label.clone(),
                message: error.to_string(),
            }
        })?;
        timers.schedule_after(TimerId::StatusPublish, limits.status_publish_interval);
        let status = InstanceNetworkStatus::starting_with_limits(&limits, runtime.clock());
        Ok(Self {
            gateway: Gateway::new(&spec.config, buffers.clone(), runtime.clock().clone()),
            spec,
            runtime,
            outputs,
            event_sequence: 0,
            commands,
            guest: None,
            host_ports: None,
            egress_tcp: TcpProxies::new(&limits, &buffers),
            ingress_udp: UdpPeers::new(limits.ingress_udp_peer_limit),
            ingress_tcp: TcpConnections::new(limits.ingress_tcp_connection_limit, limits.tcp_socket_buffer_capacity),
            egress_udp: UdpProxies::new(&buffers),
            buffers,
            component_outputs: ComponentOutputQueues::with_capacity(limits.component_output_batch_capacity),
            component_events: ComponentEvents::with_capacity(limits.component_event_batch_capacity),
            reactor_ready: Vec::with_capacity(limits.component_event_batch_capacity),
            timers,
            expired_timers: Vec::with_capacity(TIMER_QUEUE_REQUIRED_CAPACITY),
            status,
            status_dirty: false,
            generation: 0,
            guest_disconnect: None,
        })
    }

    pub(crate) fn run(mut self) -> NetworkExit {
        self.set_state(InstanceNetworkState::Starting);
        let result = self.run_until_stopped();
        self.outputs.flush();
        let _cleanup = self.runtime.cleanup();
        result
    }

    pub(crate) fn wake_handle(&self) -> <R::Reactor as ReactorBackend>::Wake {
        self.runtime.reactor().wake_handle()
    }

    pub(crate) fn begin_connect(&mut self) -> Result<String, InstanceNetworkError> {
        self.reset_network()?;
        let transport_name = self.runtime.transport().describe();
        self.set_state(InstanceNetworkState::Connecting {
            transport: transport_name.clone(),
        });
        Ok(transport_name)
    }

    pub(crate) fn connect_once(&mut self, transport_name: &str) -> Result<ConnectOutcome, InstanceNetworkError> {
        let session = match self.runtime.transport_mut().try_connect() {
            Ok(ConnectStatus::Connected(session)) => session,
            Ok(ConnectStatus::Pending) => return Ok(ConnectOutcome::Pending),
            Err(source) => {
                self.emit_event(NetworkEvent::Transport(NetworkTransportEvent::ConnectFailed {
                    transport: NetworkEventText::from_str(transport_name),
                    error: NetworkEventText::from_display(&source),
                }));
                return Err(InstanceNetworkError::TransportConnect {
                    transport: transport_name.to_owned(),
                    source,
                });
            }
        };
        self.generation = self.generation.saturating_add(1);
        self.set_state(InstanceNetworkState::Connected {
            generation: self.generation,
        });
        self.emit_event(NetworkEvent::Transport(NetworkTransportEvent::GuestConnected {
            transport: NetworkEventText::from_str(transport_name),
            generation: self.generation,
        }));
        self.guest = Some(
            GuestIo::register(session, self.generation, &self.buffers, &mut self.runtime).map_err(|source| {
                self.emit_event(NetworkEvent::Transport(NetworkTransportEvent::RegisterFailed {
                    transport: NetworkEventText::from_str(transport_name),
                    error: NetworkEventText::from_display(&source),
                }));
                InstanceNetworkError::TransportConnect {
                    transport: transport_name.to_owned(),
                    source,
                }
            })?,
        );
        Ok(ConnectOutcome::Connected)
    }

    pub(crate) fn drive_once(&mut self, poll_timeout: Option<Duration>) -> DriveOutcome {
        // Commands are the only control-plane input in the hot connected loop.
        // The dataplane itself communicates by filling component_events and
        // component_outputs below.
        match self.commands.try_recv() {
            Some(command) if stop_requested(Some(&command)) => return DriveOutcome::Stop,
            Some(_) | None => {}
        }

        // First drain work that is already known without blocking in the
        // reactor: queued guest writes, queued host-port writes, UDP sends,
        // gateway poll work, and expired timers. This keeps write-heavy flows
        // moving even when no new readiness event is needed.
        let mut budget = DriveBudget::event_loop(&self.spec.config.limits);
        let mut made_progress = false;
        if let Some(guest) = &mut self.guest {
            match guest.drive_queued(&mut budget, &self.runtime) {
                Ok(result) => made_progress |= result,
                Err(error) => {
                    self.guest_disconnect = Some((self.generation, error.to_string()));
                }
            }
        }
        if let Some(host_ports) = &mut self.host_ports {
            made_progress |=
                host_ports.drive_queued(&mut self.component_events.host_ports, &mut budget, &mut self.runtime);
        }
        made_progress |=
            self.egress_udp
                .drive_queued(&mut self.component_events.egress_udp, &mut budget, &mut self.runtime);
        made_progress |= self.drive_queued_tcp(&mut budget);
        made_progress |= self.process_component_outputs();
        made_progress |= self.drive_expired_timers();
        if let Some((generation, reason)) = self.got_disconnected() {
            self.backoff_connected(generation, reason);
            return DriveOutcome::Reconnect;
        }
        // A non-blocking call is used by tests/simulation and by callers that
        // want one bounded turn. If we made progress, return now so the caller
        // can observe the intermediate state instead of immediately polling.
        if made_progress && poll_timeout.is_none() {
            return DriveOutcome::Continue;
        }

        // No local work is ready, so publish pending output, wait for reactor
        // readiness or the next timer, then drive the components that own those
        // ready items. Any new side effects produced by readiness are flushed at
        // the end of this same turn.
        self.refresh_timers();
        let timeout = poll_timeout.or_else(|| self.timers.next_timeout());
        if let Err(message) = self.wait_reactor(timeout) {
            self.record_reactor_error(message);
            self.publish();
            return DriveOutcome::Continue;
        }

        let readiness = std::mem::take(&mut self.reactor_ready);
        let mut budget = DriveBudget::event_loop(&self.spec.config.limits);
        if let Some(guest) = &mut self.guest
            && let Err(error) =
                guest.drive_ready(&readiness, &mut self.component_events.guest, &mut budget, &self.runtime)
        {
            self.guest_disconnect = Some((self.generation, error.to_string()));
        }
        if let Some(host_ports) = &mut self.host_ports {
            let _progress = host_ports.drive_ready(
                &readiness,
                &mut self.component_events.host_ports,
                &mut budget,
                &mut self.runtime,
            );
        }
        let _tcp_progress = self.drive_ready_tcp(&readiness, &mut budget);
        let _udp_progress = self.egress_udp.drive_ready(
            &readiness,
            &mut self.component_events.egress_udp,
            &mut budget,
            &mut self.runtime,
        );
        self.reactor_ready = readiness;
        let _timers = self.drive_expired_timers();
        let _outputs = self.process_component_outputs();
        if let Some((generation, reason)) = self.got_disconnected() {
            self.backoff_connected(generation, reason);
            return DriveOutcome::Reconnect;
        }
        DriveOutcome::Continue
    }

    #[cfg(any(test, feature = "simulation"))]
    pub(crate) fn buffer_snapshot(&self) -> BufferPoolSnapshot {
        self.buffers.snapshot()
    }

    #[cfg(any(test, feature = "simulation"))]
    pub(crate) fn tcp_snapshot(&self) -> String {
        self.egress_tcp.debug_snapshot(self.gateway.tcp_sockets())
    }

    #[cfg(any(test, feature = "simulation"))]
    pub(crate) fn active_tcp_proxy_slots(&self) -> usize {
        self.egress_tcp.active_proxy_slots()
    }

    fn run_until_stopped(&mut self) -> NetworkExit {
        loop {
            // Each outer-loop iteration owns one guest transport generation.
            // Reconnect drops all runtime-registered resources and starts a new
            // generation so stale guest events cannot affect the new session.
            let transport_name = match self.begin_connect() {
                Ok(transport_name) => transport_name,
                Err(error) => {
                    self.set_state(InstanceNetworkState::Failed {
                        error: error.to_string(),
                    });
                    return NetworkExit::Failed(error);
                }
            };
            loop {
                // try_connect may report Pending for transports that need an
                // external peer. While pending, only stop commands and timers
                // are processed; dataplane components are not registered yet.
                match self.connect_once(&transport_name) {
                    Ok(ConnectOutcome::Connected) => break,
                    Ok(ConnectOutcome::Pending) => {}
                    Err(error) => {
                        self.status
                            .telemetry
                            .record_connect_error(&error.to_string(), self.runtime.clock());
                        self.publish();
                        if self.backoff_or_stop(&error) {
                            return self.stop();
                        }
                    }
                }
                self.timers.schedule_after(
                    TimerId::ConnectRetry,
                    self.spec.config.limits.transport_connect_retry_delay,
                );
                if self.wait_until_timer_or_stop(TimerId::ConnectRetry) {
                    return self.stop();
                }
            }
            if self.run_connected() {
                return self.stop();
            }
        }
    }

    fn run_connected(&mut self) -> bool {
        loop {
            match self.drive_once(None) {
                DriveOutcome::Continue => {}
                DriveOutcome::Stop => return true,
                DriveOutcome::Reconnect => return false,
            }
        }
    }

    fn wait_reactor(&mut self, timeout: Option<Duration>) -> Result<(), String> {
        self.outputs.flush();
        self.runtime
            .reactor_mut()
            .ready_into(&mut self.reactor_ready, timeout)
            .map_err(|error| error.to_string())
    }

    fn drive_queued_tcp(&mut self, budget: &mut DriveBudget) -> bool {
        let made_progress = self.egress_tcp.drive_queued(
            &mut self.gateway,
            &mut self.component_events.egress_tcp,
            budget,
            &mut self.runtime,
        );
        if made_progress {
            self.gateway.poll(&mut self.component_outputs.guest_frames);
        }
        made_progress
    }

    fn drive_ready_tcp(&mut self, readiness: &[ReactorReady], budget: &mut DriveBudget) -> bool {
        let made_progress = self.egress_tcp.drive_ready(
            &mut self.gateway,
            readiness,
            &mut self.component_events.egress_tcp,
            budget,
            &mut self.runtime,
        );
        if made_progress {
            self.gateway.poll(&mut self.component_outputs.guest_frames);
        }
        made_progress
    }

    fn drive_tcp_with_gateway(&mut self) -> bool {
        let mut budget = DriveBudget::event_loop(&self.spec.config.limits);
        let made_progress = self.egress_tcp.drive_gateway(
            &mut self.gateway,
            &[],
            &mut self.component_events.egress_tcp,
            &mut budget,
            &mut self.runtime,
        );
        if made_progress {
            self.gateway.poll(&mut self.component_outputs.guest_frames);
        }
        made_progress
    }
    fn relay_ingress_tcp_from_gateway(&mut self) -> bool {
        let start_writes = self.component_outputs.ingress_tcp_writes.len();
        let start_closes = self.component_outputs.ingress_tcp_closes.len();
        let start_frames = self.component_outputs.guest_frames.len();
        self.gateway.relay_ingress_tcp_guest_bytes(
            &mut self.ingress_tcp,
            &mut self.component_outputs.ingress_tcp_writes,
            &mut self.component_outputs.ingress_tcp_closes,
            &mut self.component_outputs.guest_frames,
        );
        self.component_outputs.ingress_tcp_writes.len() > start_writes
            || self.component_outputs.ingress_tcp_closes.len() > start_closes
            || self.component_outputs.guest_frames.len() > start_frames
    }

    fn process_component_outputs(&mut self) -> bool {
        let made_progress = !self.component_events.is_empty() || !self.component_outputs.is_empty();
        self.process_component_events();
        self.send_guest_frames();
        self.send_egress_udp_datagrams();
        self.write_ingress_tcp_bytes();
        self.close_ingress_tcp_connections();
        self.send_ingress_udp_datagrams();
        self.flush_outputs();
        made_progress
    }

    fn process_component_events(&mut self) {
        // Handlers may enqueue more component events. Taking one queue at a time
        // keeps the current batch stable and makes re-entrant appends visible on
        // the next drive turn.
        let mut guest = std::mem::take(&mut self.component_events.guest);
        consume_events(&mut guest, |event| self.handle_guest_event(event));
        self.component_events.guest = guest;

        let mut host_ports = std::mem::take(&mut self.component_events.host_ports);
        consume_events(&mut host_ports, |event| self.handle_host_port_event(event));
        self.component_events.host_ports = host_ports;

        let mut egress_tcp = std::mem::take(&mut self.component_events.egress_tcp);
        consume_events(&mut egress_tcp, |event| self.handle_tcp_event(event));
        self.component_events.egress_tcp = egress_tcp;

        let mut egress_udp = std::mem::take(&mut self.component_events.egress_udp);
        consume_events(&mut egress_udp, |event| self.handle_udp_event(event));
        self.component_events.egress_udp = egress_udp;
    }

    fn flush_outputs(&mut self) {
        self.outputs.flush();
    }

    fn handle_guest_event(&mut self, event: GuestEvent) {
        match event {
            GuestEvent::Frame { generation, frame } if generation == self.generation => {
                // A frame from the guest may be egress traffic to an upstream
                // server, a response on a host-port ingress flow, or a local
                // gateway protocol packet. Gateway classifies it and appends the
                // resulting side effects to component_outputs.
                self.status
                    .telemetry
                    .record_guest_frame(frame.len(), self.runtime.clock());
                self.status_dirty = true;
                if matches!(self.status.state, InstanceNetworkState::Connected { generation: current } if current == generation)
                {
                    self.set_state(InstanceNetworkState::TrafficObserved { generation });
                }
                self.gateway.ingest_guest_frame(
                    &mut self.egress_tcp,
                    &self.ingress_udp,
                    frame,
                    &mut self.component_outputs.egress_udp_sends,
                    &mut self.component_outputs.ingress_udp_sends,
                    &mut self.component_outputs.guest_frames,
                );
                self.drive_tcp_with_gateway();
                self.relay_ingress_tcp_from_gateway();
            }
            GuestEvent::Disconnected { generation, result } if generation == self.generation => {
                self.guest_disconnect = Some((generation, disconnect_reason(result)));
            }
            _ => {}
        }
    }

    fn handle_host_port_event(&mut self, event: HostPortEvent) {
        match event {
            HostPortEvent::TcpAccepted { port, connection } => {
                self.gateway.accept_ingress_tcp(
                    &mut self.ingress_tcp,
                    port,
                    connection,
                    &mut self.component_outputs.ingress_tcp_closes,
                    &mut self.component_outputs.guest_frames,
                );
                self.relay_ingress_tcp_from_gateway();
            }
            HostPortEvent::TcpBytes { connection, bytes } => {
                self.gateway.write_ingress_tcp(
                    &mut self.ingress_tcp,
                    connection,
                    bytes,
                    &mut self.component_outputs.guest_frames,
                );
                self.relay_ingress_tcp_from_gateway();
            }
            HostPortEvent::TcpClosed { connection } => {
                self.gateway.close_ingress_tcp(
                    &mut self.ingress_tcp,
                    connection,
                    &mut self.component_outputs.guest_frames,
                );
                self.relay_ingress_tcp_from_gateway();
            }
            HostPortEvent::UdpDatagram { port, peer, bytes } => {
                self.gateway.ingest_ingress_udp_datagram(
                    &mut self.ingress_udp,
                    port,
                    peer,
                    &bytes,
                    &mut self.component_outputs.guest_frames,
                );
            }
            HostPortEvent::Error { message } => {
                self.emit_event(NetworkEvent::HostPort(NetworkHostPortEvent::Error {
                    message: NetworkEventText::from_str(&message),
                }));
                self.status.telemetry.record_egress_error(message, self.runtime.clock());
                self.publish();
            }
        }
    }

    fn handle_tcp_event(&mut self, event: TcpProxyEvent) {
        match event {
            TcpProxyEvent::Closed { proxy } => {
                self.emit_event(NetworkEvent::Egress(NetworkEgressEvent::ProxyClosed {
                    protocol: NetworkEgressProtocol::Tcp,
                    proxy: Some(proxy.0),
                }));
            }
            TcpProxyEvent::DnsResolved { host, addresses, ttl } => {
                self.emit_event(NetworkEvent::Dns(NetworkDnsEvent::Resolved {
                    protocol: NetworkEgressProtocol::Tcp,
                    host: NetworkEventText::from_str(&host),
                    addresses: NetworkAddresses::from_slice(&addresses),
                    ttl,
                }));
                self.gateway.record_dns_resolution(&host, addresses, ttl);
            }
            TcpProxyEvent::Error {
                proxy,
                context,
                message,
            } => {
                self.emit_event(NetworkEvent::Egress(NetworkEgressEvent::error(
                    NetworkEgressProtocol::Tcp,
                    Some(proxy.0),
                    context
                        .as_ref()
                        .map(|context| NetworkEventText::from_str(&context.destination)),
                    context
                        .as_ref()
                        .map(|context| NetworkEventText::from_str(&context.upstream)),
                    context
                        .as_ref()
                        .and_then(|context| context.authority.as_deref().map(NetworkEventText::from_str)),
                    context
                        .as_ref()
                        .map(|context| NetworkEventText::from_str(context.route)),
                    context
                        .as_ref()
                        .map(|context| NetworkEventText::from_str(context.phase)),
                    NetworkEventText::from_str(&message),
                )));
                self.status.telemetry.record_egress_error(message, self.runtime.clock());
                self.publish();
            }
        }
    }

    fn handle_udp_event(&mut self, event: UdpProxyEvent) {
        match event {
            UdpProxyEvent::Bytes { proxy, bytes, is_dns } => {
                self.gateway
                    .write_udp_response(proxy, &bytes, is_dns, &mut self.component_outputs.guest_frames);
            }
            UdpProxyEvent::Closed => {
                self.emit_event(NetworkEvent::Egress(NetworkEgressEvent::ProxyClosed {
                    protocol: NetworkEgressProtocol::Udp,
                    proxy: None,
                }));
            }
            UdpProxyEvent::DnsResolved { host, addresses, ttl } => {
                self.emit_event(NetworkEvent::Dns(NetworkDnsEvent::Resolved {
                    protocol: NetworkEgressProtocol::Udp,
                    host: NetworkEventText::from_str(&host),
                    addresses: NetworkAddresses::from_slice(&addresses),
                    ttl,
                }));
                self.gateway.record_dns_resolution(&host, addresses, ttl);
            }
            UdpProxyEvent::Error { message } => {
                self.emit_event(NetworkEvent::Egress(NetworkEgressEvent::error(
                    NetworkEgressProtocol::Udp,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    NetworkEventText::from_str(&message),
                )));
                self.status.telemetry.record_egress_error(message, self.runtime.clock());
                self.publish();
            }
        }
    }

    fn send_guest_frames(&mut self) {
        // All protocol paths that produce packets for the guest converge here:
        // upstream responses, host-port ingress traffic, DNS replies, ARP, and
        // TCP state-machine packets from smoltcp.
        let mut frames = take_fifo(&mut self.component_outputs.guest_frames);
        while let Some(frame) = frames.pop() {
            self.status
                .telemetry
                .record_host_frame(frame.len(), self.runtime.clock());
            self.status_dirty = true;
            if let Some(guest) = &mut self.guest
                && let Err(error) = guest.send(frame, &self.runtime)
            {
                self.guest_disconnect = Some((self.generation, error.to_string()));
            }
        }
        self.component_outputs.guest_frames = frames;
    }

    fn send_egress_udp_datagrams(&mut self) {
        let mut sends = take_fifo(&mut self.component_outputs.egress_udp_sends);
        while let Some(send) = sends.pop() {
            self.egress_udp.send(send.proxy, send.bytes, send.is_dns);
        }
        self.component_outputs.egress_udp_sends = sends;
    }

    fn write_ingress_tcp_bytes(&mut self) {
        let mut writes = take_fifo(&mut self.component_outputs.ingress_tcp_writes);
        while let Some(write) = writes.pop() {
            if let Some(host_ports) = &mut self.host_ports {
                host_ports.write_tcp(write.connection, write.bytes, &self.runtime);
            }
        }
        self.component_outputs.ingress_tcp_writes = writes;
    }

    fn close_ingress_tcp_connections(&mut self) {
        let mut closes = take_fifo(&mut self.component_outputs.ingress_tcp_closes);
        while let Some(connection) = closes.pop() {
            if let Some(host_ports) = &mut self.host_ports {
                host_ports.close_tcp(connection, &mut self.runtime);
            }
        }
        self.component_outputs.ingress_tcp_closes = closes;
    }

    fn send_ingress_udp_datagrams(&mut self) {
        let mut sends = take_fifo(&mut self.component_outputs.ingress_udp_sends);
        while let Some(send) = sends.pop() {
            if let Some(host_ports) = &mut self.host_ports {
                host_ports.send_udp(send.port, send.peer, send.bytes, &self.runtime);
            }
        }
        self.component_outputs.ingress_udp_sends = sends;
    }

    fn reset_network(&mut self) -> Result<(), InstanceNetworkError> {
        self.drop_runtime_resources();
        self.reset_dataplane();
        let host_ports = HostPorts::bind(self.spec.config.host_ports.clone(), &self.buffers, &mut self.runtime)
            .map_err(|error| InstanceNetworkError::HostPortBind {
                message: error.to_string(),
            })?;
        for port in host_ports.bound_ports() {
            self.emit_event(NetworkEvent::HostPort(NetworkHostPortEvent::Bound {
                name: NetworkEventText::from_str(port.name),
                protocol: port.protocol,
                guest: port.guest,
                host: port.host,
            }));
        }
        self.host_ports = Some(host_ports);
        self.refresh_timers();
        Ok(())
    }

    fn drop_runtime(&mut self) {
        self.drop_runtime_resources();
        self.reset_dataplane();
    }

    fn drop_runtime_resources(&mut self) {
        if let Some(mut guest) = self.guest.take() {
            guest.shutdown(&mut self.runtime);
        }
        if let Some(mut host_ports) = self.host_ports.take() {
            host_ports.shutdown(&mut self.runtime);
        }
        self.egress_tcp.shutdown(&mut self.runtime);
        self.egress_udp.shutdown(&mut self.runtime);
        self.guest_disconnect = None;
    }

    fn reset_dataplane(&mut self) {
        self.egress_tcp = TcpProxies::new(&self.spec.config.limits, &self.buffers);
        self.ingress_udp = UdpPeers::new(self.spec.config.limits.ingress_udp_peer_limit);
        self.ingress_tcp = TcpConnections::new(
            self.spec.config.limits.ingress_tcp_connection_limit,
            self.spec.config.limits.tcp_socket_buffer_capacity,
        );
        self.egress_udp = UdpProxies::new(&self.buffers);
        self.gateway = Gateway::new(&self.spec.config, self.buffers.clone(), self.runtime.clock().clone());
    }

    fn backoff_connected(&mut self, generation: u64, reason: String) {
        self.emit_event(NetworkEvent::Transport(NetworkTransportEvent::GuestDisconnected {
            generation,
            reason: NetworkEventText::from_str(&reason),
        }));
        self.set_state(InstanceNetworkState::Backoff {
            generation,
            reason,
            reconnect_after: self.spec.reconnect_delay,
        });
        self.drop_runtime();
    }

    fn backoff_or_stop(&mut self, error: &InstanceNetworkError) -> bool {
        self.set_state(InstanceNetworkState::Backoff {
            generation: self.generation,
            reason: error.to_string(),
            reconnect_after: self.spec.reconnect_delay,
        });
        self.timers
            .schedule_after(TimerId::Reconnect, self.spec.reconnect_delay);
        self.wait_until_timer_or_stop(TimerId::Reconnect)
    }

    fn set_state(&mut self, state: InstanceNetworkState) {
        self.status.telemetry.record_state(&state, self.runtime.clock());
        self.emit_event(NetworkEvent::Lifecycle(NetworkLifecycleEvent::StateChanged {
            state: NetworkStateEvent::from_state(&state),
        }));
        self.status.state = state;
        self.publish();
    }

    fn record_reactor_error(&mut self, message: String) {
        self.emit_event(NetworkEvent::Reactor(NetworkReactorEvent::Error {
            message: NetworkEventText::from_str(&message),
        }));
        self.status.telemetry.record_egress_error(message, self.runtime.clock());
    }

    fn emit_event(&mut self, event: NetworkEvent) {
        self.event_sequence = self.event_sequence.saturating_add(1);
        let sequence = self.event_sequence;
        let unix_millis = unix_millis(self.runtime.clock().system_time());
        self.outputs.emit(|slot| {
            *slot = NetworkEventEnvelope {
                sequence,
                unix_millis,
                dropped_events_before: 0,
                event,
            };
        });
    }

    fn publish(&mut self) {
        self.emit_event(NetworkEvent::Telemetry(NetworkTelemetryEvent::Snapshot(
            NetworkTelemetrySnapshot::from_status(&self.status),
        )));
        self.status_dirty = false;
    }

    fn refresh_timers(&mut self) {
        self.timers
            .schedule_after(TimerId::GatewayPoll, self.gateway.next_poll_delay());
        if let Some(deadline) = self.egress_udp.next_expiry() {
            self.timers.schedule_at(TimerId::UdpExpiry, deadline);
        } else {
            self.timers.clear(TimerId::UdpExpiry);
        }
    }

    fn drive_expired_timers(&mut self) -> bool {
        self.timers.pop_expired(&mut self.expired_timers);
        let mut made_progress = false;
        let expired = std::mem::take(&mut self.expired_timers);
        for timer in &expired {
            match timer {
                TimerId::GatewayPoll => {
                    // smoltcp advances TCP state from time as well as from
                    // packet input, so timer-driven gateway work can produce
                    // guest frames and proxy work even without reactor events.
                    self.component_outputs.guest_frames.clear();
                    self.gateway.poll(&mut self.component_outputs.guest_frames);
                    self.drive_tcp_with_gateway();
                    self.send_guest_frames();
                    self.send_egress_udp_datagrams();
                    self.write_ingress_tcp_bytes();
                    self.close_ingress_tcp_connections();
                    self.send_ingress_udp_datagrams();
                    made_progress = true;
                }
                TimerId::UdpExpiry => {
                    let mut budget = DriveBudget::event_loop(&self.spec.config.limits);
                    made_progress |= self.egress_udp.expire_due(
                        &mut self.component_events.egress_udp,
                        &mut budget,
                        &mut self.runtime,
                    );
                }
                TimerId::StatusPublish => {
                    if self.status_dirty {
                        self.publish();
                    }
                    self.timers
                        .schedule_after(TimerId::StatusPublish, self.spec.config.limits.status_publish_interval);
                }
                TimerId::ConnectRetry | TimerId::Reconnect => {}
            }
        }
        self.expired_timers = expired;
        self.refresh_timers();
        made_progress
    }

    fn wait_until_timer_or_stop(&mut self, target: TimerId) -> bool {
        loop {
            if stop_requested(self.commands.try_recv().as_ref()) {
                return true;
            }
            if let Err(message) = self.wait_reactor(self.timers.next_timeout()) {
                self.record_reactor_error(message);
                self.publish();
            }
            self.timers.pop_expired(&mut self.expired_timers);
            let expired = std::mem::take(&mut self.expired_timers);
            let target_expired = expired.contains(&target);
            for timer in &expired {
                if matches!(timer, TimerId::StatusPublish) {
                    if self.status_dirty {
                        self.publish();
                    }
                    self.timers
                        .schedule_after(TimerId::StatusPublish, self.spec.config.limits.status_publish_interval);
                }
            }
            self.expired_timers = expired;
            if target_expired {
                return false;
            }
        }
    }

    pub(crate) fn stop(&mut self) -> NetworkExit {
        self.set_state(InstanceNetworkState::Stopping);
        self.drop_runtime();
        self.set_state(InstanceNetworkState::Stopped);
        self.outputs.flush();
        NetworkExit::Stopped
    }

    fn got_disconnected(&mut self) -> Option<(u64, String)> {
        let (generation, reason) = self.guest_disconnect.take()?;
        (generation == self.generation).then_some((generation, reason))
    }
}

const fn stop_requested(command: Option<&NetworkCommand>) -> bool {
    matches!(command, Some(NetworkCommand::Stop))
}

fn consume_events<T>(events: &mut Vec<T>, mut collect: impl FnMut(T)) {
    // Component queues append at the tail. Reversing lets us pop while still
    // preserving FIFO order and avoids shifting the Vec on every item.
    events.reverse();
    while let Some(event) = events.pop() {
        collect(event);
    }
}

fn take_fifo<T>(queue: &mut Vec<T>) -> Vec<T> {
    // Callers consume with pop(), so reverse once to preserve FIFO order.
    let mut items = std::mem::take(queue);
    items.reverse();
    items
}

fn disconnect_reason(result: Result<(), TransportError>) -> String {
    match result {
        Ok(()) => "guest transport closed".to_owned(),
        Err(error) => error.to_string(),
    }
}

fn unix_millis(time: std::time::SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}
