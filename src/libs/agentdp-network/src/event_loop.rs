use std::time::Duration;
use std::time::UNIX_EPOCH;

#[cfg(any(test, feature = "simulation"))]
use crate::buffers::BufferPoolSnapshot;
use crate::buffers::{BufferPool, FrameBuf};
use crate::clock::NetworkClock as _;
use crate::command::{NetworkCommand, NetworkCommandSource};
use crate::drive::{DriveBudget, DriveReport, DriveTurn};
use crate::egress::tcp::{TcpProxies, TcpProxyEvent};
use crate::egress::udp::{UdpProxies, UdpProxyEvent};
use crate::events::{
    NetworkAddresses, NetworkDnsEvent, NetworkEgressEvent, NetworkEgressProtocol, NetworkEvent, NetworkEventEnvelope,
    NetworkEventSink, NetworkEventText, NetworkHostPortEvent, NetworkLifecycleEvent, NetworkReactorEvent,
    NetworkStateEvent, NetworkTelemetryEvent, NetworkTelemetrySnapshot, NetworkTransportEvent,
};
use crate::gateway::{Gateway, GatewayOutputs, GuestFrameIngest, UdpFrameWrite};
use crate::guest::{ConnectStatus, GuestEvent, GuestFrameEnqueue, GuestFrameTransport, GuestIo};
use crate::ingress::TcpConnections;
use crate::ingress::UdpPeers;
use crate::ingress::{HostPortEvent, HostPorts};
use crate::network::{
    EgressUdpSend, HostConnectionId, IngressTcpOutput, IngressUdpSend, InstanceNetworkError, InstanceNetworkSpec,
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
        spec.config
            .limits
            .validate_for_event_loop()
            .map_err(|message| InstanceNetworkError::TaskFailed {
                label: spec.label.clone(),
                message,
            })?;
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
    ingress_tcp: Vec<IngressTcpOutput>,
    ingress_udp_sends: Vec<IngressUdpSend>,
}

impl ComponentOutputQueues {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            guest_frames: Vec::with_capacity(capacity),
            egress_udp_sends: Vec::with_capacity(capacity),
            ingress_tcp: Vec::with_capacity(capacity),
            ingress_udp_sends: Vec::with_capacity(capacity),
        }
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
    ingress_tcp_blocked_reads: Vec<HostConnectionId>,
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
        limits
            .validate_for_event_loop()
            .map_err(|message| InstanceNetworkError::TaskFailed {
                label: spec.label.clone(),
                message,
            })?;
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
            ingress_tcp_blocked_reads: Vec::with_capacity(limits.ingress_tcp_connection_limit),
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
        if self.process_commands() {
            return DriveOutcome::Stop;
        }

        // First drain work that is already known without blocking in the
        // reactor: queued guest writes, queued host-port writes, UDP sends,
        // gateway poll work, and expired timers. This keeps write-heavy flows
        // moving even when no new readiness event is needed.
        let mut budget = DriveBudget::event_loop(&self.spec.config.limits);
        let mut queued_report = DriveReport::new();
        let (queued_timer_progress, queued_output_progress) = {
            let mut drive = DriveTurn::new(&mut budget, &mut queued_report);
            if let Some(guest) = &mut self.guest
                && let Err(error) = guest.drive_queued(&mut self.component_events.guest, &mut drive, &self.runtime)
            {
                self.guest_disconnect = Some((self.generation, error.to_string()));
            }
            if let Some(host_ports) = &mut self.host_ports {
                self.gateway
                    .ingress_tcp_guest_send_blocked(&self.ingress_tcp, &mut self.ingress_tcp_blocked_reads);
                host_ports.drive_queued(
                    &self.ingress_tcp_blocked_reads,
                    &mut self.component_events.host_ports,
                    &mut drive,
                    &mut self.runtime,
                );
            }
            self.egress_udp
                .drive_queued(&mut self.component_events.egress_udp, &mut drive, &mut self.runtime);
            self.drive_queued_tcp(&mut drive);
            let before_timers = drive.progress();
            self.drive_expired_timers(&mut drive);
            let timer_progress = drive.progress() != before_timers;
            let before_outputs = drive.progress();
            self.process_component_outputs(&mut drive);
            let output_progress = drive.progress() != before_outputs;
            (timer_progress, output_progress)
        };
        let followup_progress = queued_output_progress || queued_timer_progress;
        let made_progress = queued_report.made_progress() || followup_progress;
        if let Some((generation, reason)) = self.got_disconnected() {
            self.backoff_connected(generation, reason);
            return DriveOutcome::Reconnect;
        }
        // No local work is ready, so publish pending output, wait for reactor
        // readiness or the next timer, then drive the components that own those
        // ready items. Any new side effects produced by readiness are flushed at
        // the end of this same turn.
        self.refresh_timers();
        let timeout = if made_progress {
            Some(Duration::ZERO)
        } else {
            poll_timeout.or_else(|| self.timers.next_timeout())
        };
        if let Err(message) = self.wait_reactor(timeout) {
            self.record_reactor_error(message);
            self.publish();
            return DriveOutcome::Continue;
        }

        let readiness = std::mem::take(&mut self.reactor_ready);
        let mut budget = DriveBudget::event_loop(&self.spec.config.limits);
        let mut ready_report = DriveReport::new();
        let (ready_timer_progress, ready_output_progress) = {
            let mut drive = DriveTurn::new(&mut budget, &mut ready_report);
            if let Some(guest) = &mut self.guest
                && let Err(error) =
                    guest.drive_ready(&readiness, &mut self.component_events.guest, &mut drive, &self.runtime)
            {
                self.guest_disconnect = Some((self.generation, error.to_string()));
            }
            if let Some(host_ports) = &mut self.host_ports {
                self.gateway
                    .ingress_tcp_guest_send_blocked(&self.ingress_tcp, &mut self.ingress_tcp_blocked_reads);
                host_ports.drive_ready(
                    &readiness,
                    &self.ingress_tcp_blocked_reads,
                    &mut self.component_events.host_ports,
                    &mut drive,
                    &mut self.runtime,
                );
            }
            self.drive_ready_tcp(&readiness, &mut drive);
            self.egress_udp.drive_ready(
                &readiness,
                &mut self.component_events.egress_udp,
                &mut drive,
                &mut self.runtime,
            );
            self.reactor_ready = readiness;
            let before_timers = drive.progress();
            self.drive_expired_timers(&mut drive);
            let timer_progress = drive.progress() != before_timers;
            let before_outputs = drive.progress();
            self.process_component_outputs(&mut drive);
            let output_progress = drive.progress() != before_outputs;
            (timer_progress, output_progress)
        };
        if let Some((generation, reason)) = self.got_disconnected() {
            self.backoff_connected(generation, reason);
            return DriveOutcome::Reconnect;
        }
        if should_retry_local(&ready_report, ready_output_progress || ready_timer_progress) {
            return DriveOutcome::Continue;
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

    #[cfg(any(test, feature = "simulation"))]
    pub(crate) fn queue_guest_frame_for_test(&mut self, bytes: &[u8]) -> Result<(), String> {
        let mut frame = self
            .buffers
            .try_frame_with_capacity(bytes.len())
            .map_err(|error| error.to_string())?;
        frame.as_mut_vec().extend_from_slice(bytes);
        self.component_outputs.guest_frames.push(frame);
        Ok(())
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

    fn drive_queued_tcp(&mut self, drive: &mut DriveTurn<'_>) {
        self.egress_tcp.drive_queued(
            &mut self.gateway,
            &mut self.component_events.egress_tcp,
            drive,
            &mut self.runtime,
        );
        self.gateway.poll(&mut self.component_outputs.guest_frames, drive);
    }

    fn drive_ready_tcp(&mut self, readiness: &[ReactorReady], drive: &mut DriveTurn<'_>) {
        self.egress_tcp.drive_ready(
            &mut self.gateway,
            readiness,
            &mut self.component_events.egress_tcp,
            drive,
            &mut self.runtime,
        );
        self.gateway.poll(&mut self.component_outputs.guest_frames, drive);
    }

    fn drive_tcp_with_gateway(&mut self, drive: &mut DriveTurn<'_>) {
        self.egress_tcp.drive_gateway(
            &mut self.gateway,
            &[],
            &mut self.component_events.egress_tcp,
            drive,
            &mut self.runtime,
        );
        self.gateway.poll(&mut self.component_outputs.guest_frames, drive);
    }
    fn relay_ingress_tcp_from_gateway(&mut self, drive: &mut DriveTurn<'_>) -> bool {
        let start_ingress_tcp = self.component_outputs.ingress_tcp.len();
        let start_frames = self.component_outputs.guest_frames.len();
        self.gateway.relay_ingress_tcp_guest_bytes(
            &mut self.ingress_tcp,
            &mut self.component_outputs.ingress_tcp,
            &mut self.component_outputs.guest_frames,
            drive,
        );
        self.component_outputs.ingress_tcp.len() > start_ingress_tcp
            || self.component_outputs.guest_frames.len() > start_frames
    }

    fn process_component_outputs(&mut self, drive: &mut DriveTurn<'_>) {
        self.process_component_events(drive);
        self.send_guest_frames(drive);
        self.send_egress_udp_datagrams(drive);
        self.process_ingress_tcp_outputs(drive);
        self.send_ingress_udp_datagrams(drive);
        self.outputs.flush();
    }

    fn process_component_events(&mut self, drive: &mut DriveTurn<'_>) {
        // Handlers may enqueue more component events. Taking one queue at a time
        // keeps the current batch stable and makes re-entrant appends visible on
        // the next drive turn.
        let mut guest = take_fifo(&mut self.component_events.guest);
        while let Some(event) = guest.pop() {
            if let Some(event) = self.handle_guest_event(event, drive) {
                guest.push(event);
                break;
            }
        }
        guest.reverse();
        self.component_events.guest = guest;

        let mut host_ports = take_fifo(&mut self.component_events.host_ports);
        while let Some(event) = host_ports.pop() {
            if let Some(event) = self.handle_host_port_event(event, drive) {
                host_ports.push(event);
                break;
            }
        }
        host_ports.reverse();
        self.component_events.host_ports = host_ports;

        let mut egress_tcp = std::mem::take(&mut self.component_events.egress_tcp);
        consume_events(&mut egress_tcp, |event| self.handle_tcp_event(event));
        self.component_events.egress_tcp = egress_tcp;

        let mut egress_udp = take_fifo(&mut self.component_events.egress_udp);
        while let Some(event) = egress_udp.pop() {
            if let Some(event) = self.handle_udp_event(event, drive) {
                egress_udp.push(event);
                break;
            }
        }
        egress_udp.reverse();
        self.component_events.egress_udp = egress_udp;
    }

    fn handle_guest_event(&mut self, event: GuestEvent, drive: &mut DriveTurn<'_>) -> Option<GuestEvent> {
        match event {
            GuestEvent::Frame { generation, frame } if generation == self.generation => {
                // A frame from the guest may be egress traffic to an upstream
                // server, a response on a host-port ingress flow, or a local
                // gateway protocol packet. Gateway classifies it and appends the
                // resulting side effects to component_outputs.
                let frame_len = frame.len();
                {
                    let mut outputs = GatewayOutputs::new(
                        &mut self.component_outputs.egress_udp_sends,
                        &mut self.component_outputs.ingress_udp_sends,
                        &mut self.component_outputs.guest_frames,
                    );
                    let ingest = self.gateway.ingest_guest_frame(
                        &mut self.egress_tcp,
                        &self.ingress_udp,
                        frame,
                        &mut outputs,
                        drive,
                    );
                    if let GuestFrameIngest::Blocked(frame) = ingest {
                        return Some(GuestEvent::Frame { generation, frame });
                    }
                }
                self.status
                    .telemetry
                    .record_guest_frame(frame_len, self.runtime.clock());
                self.status_dirty = true;
                if matches!(self.status.state, InstanceNetworkState::Connected { generation: current } if current == generation)
                {
                    self.set_state(InstanceNetworkState::TrafficObserved { generation });
                }
                self.drive_tcp_with_gateway(drive);
                self.relay_ingress_tcp_from_gateway(drive);
                None
            }
            GuestEvent::Disconnected { generation, result } if generation == self.generation => {
                let reason = match result {
                    Ok(()) => "guest transport closed".to_owned(),
                    Err(error) => error.to_string(),
                };
                self.guest_disconnect = Some((generation, reason));
                None
            }
            _ => None,
        }
    }

    fn handle_host_port_event(&mut self, event: HostPortEvent, drive: &mut DriveTurn<'_>) -> Option<HostPortEvent> {
        match event {
            HostPortEvent::TcpAccepted { port, connection } => {
                let accepted = {
                    self.gateway.accept_ingress_tcp(
                        &mut self.ingress_tcp,
                        port,
                        connection,
                        &mut self.component_outputs.ingress_tcp,
                        &mut self.component_outputs.guest_frames,
                        drive,
                    )
                };
                if !accepted {
                    return Some(HostPortEvent::TcpAccepted { port, connection });
                }
                self.relay_ingress_tcp_from_gateway(drive);
                None
            }
            HostPortEvent::TcpBytes { connection, bytes } => {
                {
                    self.gateway.write_ingress_tcp(
                        &mut self.ingress_tcp,
                        connection,
                        bytes,
                        &mut self.component_outputs.guest_frames,
                        drive,
                    );
                }
                self.relay_ingress_tcp_from_gateway(drive);
                None
            }
            HostPortEvent::TcpClosed { connection } => {
                {
                    self.gateway.close_ingress_tcp(
                        &mut self.ingress_tcp,
                        connection,
                        &mut self.component_outputs.guest_frames,
                        drive,
                    );
                }
                self.relay_ingress_tcp_from_gateway(drive);
                None
            }
            HostPortEvent::UdpDatagram { port, peer, bytes } => {
                match self.gateway.ingest_ingress_udp_datagram(
                    &mut self.ingress_udp,
                    port,
                    peer,
                    &bytes,
                    &mut self.component_outputs.guest_frames,
                    drive,
                ) {
                    UdpFrameWrite::Queued | UdpFrameWrite::Dropped => None,
                    UdpFrameWrite::Blocked => Some(HostPortEvent::UdpDatagram { port, peer, bytes }),
                }
            }
            HostPortEvent::Error { message } => {
                self.emit_event(NetworkEvent::HostPort(NetworkHostPortEvent::Error {
                    message: NetworkEventText::from_str(&message),
                }));
                self.status.telemetry.record_egress_error(message, self.runtime.clock());
                self.publish();
                None
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

    fn handle_udp_event(&mut self, event: UdpProxyEvent, drive: &mut DriveTurn<'_>) -> Option<UdpProxyEvent> {
        match event {
            UdpProxyEvent::Bytes { proxy, bytes, is_dns } => {
                match self.gateway.write_udp_response(
                    proxy,
                    &bytes,
                    is_dns,
                    &mut self.component_outputs.guest_frames,
                    drive,
                ) {
                    UdpFrameWrite::Queued => None,
                    UdpFrameWrite::Blocked => Some(UdpProxyEvent::Bytes { proxy, bytes, is_dns }),
                    UdpFrameWrite::Dropped => unreachable!("egress UDP responses are not policy-dropped"),
                }
            }
            UdpProxyEvent::Closed => {
                self.emit_event(NetworkEvent::Egress(NetworkEgressEvent::ProxyClosed {
                    protocol: NetworkEgressProtocol::Udp,
                    proxy: None,
                }));
                None
            }
            UdpProxyEvent::DnsResolved { host, addresses, ttl } => {
                self.emit_event(NetworkEvent::Dns(NetworkDnsEvent::Resolved {
                    protocol: NetworkEgressProtocol::Udp,
                    host: NetworkEventText::from_str(&host),
                    addresses: NetworkAddresses::from_slice(&addresses),
                    ttl,
                }));
                self.gateway.record_dns_resolution(&host, addresses, ttl);
                None
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
                None
            }
        }
    }

    fn send_guest_frames(&mut self, drive: &mut DriveTurn<'_>) {
        // All protocol paths that produce packets for the guest converge here:
        // upstream responses, host-port ingress traffic, DNS replies, ARP, and
        // TCP state-machine packets from smoltcp.
        let mut frames = take_fifo(&mut self.component_outputs.guest_frames);
        while let Some(frame) = frames.pop() {
            let frame_len = frame.len();
            let Some(guest) = &mut self.guest else {
                continue;
            };
            match guest.enqueue(frame, drive, &self.runtime) {
                Ok(GuestFrameEnqueue::Queued) => {}
                Ok(GuestFrameEnqueue::Blocked(frame)) => {
                    frames.push(frame);
                    break;
                }
                Err(error) => {
                    self.guest_disconnect = Some((self.generation, error.to_string()));
                    break;
                }
            }
            self.status.telemetry.record_host_frame(frame_len, self.runtime.clock());
            self.status_dirty = true;
        }
        frames.reverse();
        self.component_outputs.guest_frames = frames;
    }

    fn send_egress_udp_datagrams(&mut self, drive: &mut DriveTurn<'_>) {
        let mut sends = take_fifo(&mut self.component_outputs.egress_udp_sends);
        while let Some(send) = sends.pop() {
            if let Err(send) = drive.apply_component_output(send, |send| {
                self.egress_udp.send(send.proxy, send.bytes, send.is_dns);
            }) {
                sends.push(send);
                break;
            }
        }
        sends.reverse();
        self.component_outputs.egress_udp_sends = sends;
    }

    fn process_ingress_tcp_outputs(&mut self, drive: &mut DriveTurn<'_>) {
        let mut outputs = take_fifo(&mut self.component_outputs.ingress_tcp);
        while let Some(output) = outputs.pop() {
            if let Err(output) = drive.apply_component_output(output, |output| match output {
                IngressTcpOutput::Write { connection, bytes } => {
                    if let Some(host_ports) = &mut self.host_ports {
                        host_ports.write_tcp(connection, bytes, &self.runtime);
                    }
                }
                IngressTcpOutput::FinishWrite { connection } => {
                    if let Some(host_ports) = &mut self.host_ports {
                        host_ports.finish_tcp_write(connection);
                    }
                }
                IngressTcpOutput::Close { connection } => {
                    if let Some(host_ports) = &mut self.host_ports {
                        host_ports.close_tcp(connection, &mut self.runtime);
                    }
                }
            }) {
                outputs.push(output);
                break;
            }
        }
        outputs.reverse();
        self.component_outputs.ingress_tcp = outputs;
    }

    fn send_ingress_udp_datagrams(&mut self, drive: &mut DriveTurn<'_>) {
        let mut sends = take_fifo(&mut self.component_outputs.ingress_udp_sends);
        while let Some(send) = sends.pop() {
            if let Err(send) = drive.apply_component_output(send, |send| {
                if let Some(host_ports) = &mut self.host_ports {
                    host_ports.send_udp(send.port, send.peer, send.bytes, &self.runtime);
                }
            }) {
                sends.push(send);
                break;
            }
        }
        sends.reverse();
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
        // Gateway polling is advisory. Repeated event-loop turns must not postpone an
        // existing smoltcp deadline, or socket timers can starve behind local work.
        self.timers
            .schedule_after_if_earlier(TimerId::GatewayPoll, self.gateway.next_poll_delay());
        if let Some(deadline) = self.egress_udp.next_expiry() {
            self.timers.schedule_at(TimerId::UdpExpiry, deadline);
        } else {
            self.timers.clear(TimerId::UdpExpiry);
        }
    }

    fn drive_expired_timers(&mut self, drive: &mut DriveTurn<'_>) {
        while let Some(timer) = self.timers.pop_next_expired() {
            if !drive.can_start_operation() {
                self.timers.schedule_at(timer, self.runtime.clock().now());
                break;
            }
            match timer {
                TimerId::GatewayPoll => {
                    // smoltcp advances TCP state from time as well as from
                    // packet input, so timer-driven gateway work can produce
                    // guest frames and proxy work even without reactor events.
                    self.gateway.poll(&mut self.component_outputs.guest_frames, drive);
                    self.drive_tcp_with_gateway(drive);
                }
                TimerId::UdpExpiry => {
                    self.egress_udp
                        .expire_due(&mut self.component_events.egress_udp, drive, &mut self.runtime);
                }
                TimerId::StatusPublish => {
                    if self.status_dirty && drive.apply_state_change(|| self.publish()).is_none() {
                        self.timers
                            .schedule_at(TimerId::StatusPublish, self.runtime.clock().now());
                        break;
                    }
                    self.timers
                        .schedule_after(TimerId::StatusPublish, self.spec.config.limits.status_publish_interval);
                }
                TimerId::ConnectRetry | TimerId::Reconnect => {}
            }
        }
        self.refresh_timers();
    }

    fn wait_until_timer_or_stop(&mut self, target: TimerId) -> bool {
        loop {
            if self.process_commands() {
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

    fn process_commands(&mut self) -> bool {
        while let Some(command) = self.commands.try_recv() {
            match command {
                NetworkCommand::UpdateSecrets(secrets) => {
                    if let Some(affected_authorities) = self.gateway.update_runtime_secrets(secrets)
                        && !affected_authorities.is_empty()
                    {
                        self.egress_tcp.retire_authorities(
                            self.gateway.tcp_sockets_mut(),
                            self.runtime.reactor_mut(),
                            &affected_authorities,
                        );
                        self.timers
                            .schedule_at(TimerId::GatewayPoll, self.runtime.clock().now());
                    }
                }
                NetworkCommand::Stop => return true,
            }
        }
        false
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

fn consume_events<T>(events: &mut Vec<T>, mut collect: impl FnMut(T)) {
    // Component queues append at the tail. Reversing lets us pop while still
    // preserving FIFO order and avoids shifting the Vec on every item.
    events.reverse();
    while let Some(event) = events.pop() {
        collect(event);
    }
}

const fn should_retry_local(report: &DriveReport, followup_progress: bool) -> bool {
    (report.budget_exhausted() && report.made_progress())
        || (!report.runnable().is_empty() && (report.made_progress() || followup_progress))
}

fn take_fifo<T>(queue: &mut Vec<T>) -> Vec<T> {
    // Callers consume with pop(), so reverse once to preserve FIFO order.
    let mut items = std::mem::take(queue);
    items.reverse();
    items
}

fn unix_millis(time: std::time::SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use crate::drive::{DriveBudget, DriveReport, DriveRunnable, DriveTurn};
    use crate::network::NetworkLimits;

    use super::should_retry_local;

    #[test]
    fn local_retry_requires_progress_or_budgeted_work() {
        let mut report = drive_report(|drive| {
            drive.wait_for_local_buffer_capacity_and_runnable(DriveRunnable::WRITE_UPSTREAM);
        });
        assert!(!should_retry_local(&report, false));

        report = drive_report(|drive| {
            record_progress(drive);
        });
        assert!(!should_retry_local(&report, false));

        report = drive_report(|drive| {
            record_progress(drive);
            drive.wait_for_local_buffer_capacity_and_runnable(DriveRunnable::WRITE_UPSTREAM);
        });
        assert!(should_retry_local(&report, false));
    }

    #[test]
    fn budget_exhaustion_retries_only_after_progress() {
        let mut report = drive_report_with(
            &NetworkLimits {
                drive_step_budget: 0,
                ..NetworkLimits::default()
            },
            |drive| {
                assert!(!drive.can_start_operation());
            },
        );
        assert!(!should_retry_local(&report, false));

        report = drive_report_with(
            &NetworkLimits {
                drive_event_budget: 1,
                ..NetworkLimits::default()
            },
            |drive| {
                record_progress(drive);
                assert!(drive.push_event(&mut Vec::new(), ()).is_err());
            },
        );
        assert!(should_retry_local(&report, false));
    }

    fn record_progress(drive: &mut DriveTurn<'_>) {
        assert!(drive.push_event(&mut Vec::new(), ()).is_ok());
    }

    fn drive_report(f: impl FnOnce(&mut DriveTurn<'_>)) -> DriveReport {
        drive_report_with(&NetworkLimits::default(), f)
    }

    fn drive_report_with(limits: &NetworkLimits, f: impl FnOnce(&mut DriveTurn<'_>)) -> DriveReport {
        let mut budget = DriveBudget::event_loop(limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        f(&mut drive);
        report
    }
}
