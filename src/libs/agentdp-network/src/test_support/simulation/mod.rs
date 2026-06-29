use std::cell::{Cell, RefCell};
use std::net::{Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use smoltcp::time::Instant as SmoltcpInstant;

use crate::clock::NetworkClock;
use crate::command::{NetworkCommand, NetworkCommandSource};
use crate::event_loop::{ConnectOutcome, EventLoopCore, NetworkExit};
use crate::events::{NetworkEventEnvelope, NetworkEventSink};
use crate::guest::{GuestFrameTransport, TransportError};
use crate::network::{InstanceNetworkError, InstanceNetworkSpec, InstanceNetworkStatus};
use crate::reactor::ReactorItemId;
use crate::runtime::{NetworkRuntime, RuntimeContext};
use tokio::sync::watch;

pub use self::reactor::{
    SimTcpHandler, SimTcpHandlerFn, SimTcpResponse, SimUdpHandler, SimUdpHandlerFn, SimUdpResponse,
};

use self::reactor::{SimEndpointRegistry, SimReactor, SimTcpConnector, SimUdpSocketFactory};

mod reactor;

type SimRuntime<T> = RuntimeContext<T, SimReactor, SimClock, SimTcpConnector, SimUdpSocketFactory>;

#[derive(Debug, Clone)]
struct SimClock {
    base_instant: Instant,
    elapsed_micros: Rc<Cell<u64>>,
}

impl SimClock {
    #[must_use]
    fn new() -> Self {
        Self {
            base_instant: Instant::now(),
            elapsed_micros: Rc::new(Cell::new(0)),
        }
    }

    fn advance(&self, duration: Duration) {
        let micros = duration_micros_saturating(duration);
        self.elapsed_micros
            .set(self.elapsed_micros.get().saturating_add(micros));
    }

    #[must_use]
    fn elapsed(&self) -> Duration {
        Duration::from_micros(self.elapsed_micros.get())
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkClock for SimClock {
    fn now(&self) -> Instant {
        self.base_instant
            .checked_add(self.elapsed())
            .unwrap_or(self.base_instant)
    }

    fn system_time(&self) -> SystemTime {
        UNIX_EPOCH.checked_add(self.elapsed()).unwrap_or(UNIX_EPOCH)
    }

    fn unix_seconds(&self) -> u64 {
        self.elapsed_micros.get() / 1_000_000
    }

    fn smoltcp_now(&self) -> SmoltcpInstant {
        SmoltcpInstant::from_micros(i64::try_from(self.elapsed_micros.get()).unwrap_or(i64::MAX))
    }
}

fn duration_micros_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[derive(Default)]
pub struct SimulationUpstreams {
    pub tcp_echo: bool,
    pub tcp_handlers: Vec<SimulationTcpHandler>,
    pub udp_echo: bool,
    pub udp_handlers: Vec<SimulationUdpHandler>,
    pub dns_a_endpoints: Vec<SimulationDnsAEndpoint>,
    pub dns_a: Option<Ipv4Addr>,
}

pub struct SimulationTcpHandler {
    pub addr: SocketAddr,
    pub handler: SimTcpHandler,
    pub write_limit: Option<usize>,
}

pub struct SimulationUdpHandler {
    pub addr: SocketAddr,
    pub handler: SimUdpHandler,
}

#[derive(Debug, Clone, Copy)]
pub struct SimulationDnsAEndpoint {
    pub addr: SocketAddr,
    pub address: Ipv4Addr,
}

impl std::fmt::Debug for SimulationUpstreams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SimulationUpstreams")
            .field("tcp_echo", &self.tcp_echo)
            .field(
                "tcp_handlers",
                &self
                    .tcp_handlers
                    .iter()
                    .map(|endpoint| endpoint.addr)
                    .collect::<Vec<_>>(),
            )
            .field("udp_echo", &self.udp_echo)
            .field(
                "udp_handlers",
                &self
                    .udp_handlers
                    .iter()
                    .map(|endpoint| endpoint.addr)
                    .collect::<Vec<_>>(),
            )
            .field("dns_a_endpoints", &self.dns_a_endpoints)
            .field("dns_a", &self.dns_a)
            .finish()
    }
}

impl std::fmt::Debug for SimulationTcpHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SimulationTcpHandler")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for SimulationUdpHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SimulationUdpHandler")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl SimulationUpstreams {
    #[must_use]
    pub fn with_tcp_handler(mut self, addr: SocketAddr, handler: SimTcpHandler) -> Self {
        self.tcp_handlers.push(SimulationTcpHandler {
            addr,
            handler,
            write_limit: None,
        });
        self
    }

    #[must_use]
    pub fn with_limited_tcp_handler(mut self, addr: SocketAddr, handler: SimTcpHandler, write_limit: usize) -> Self {
        self.tcp_handlers.push(SimulationTcpHandler {
            addr,
            handler,
            write_limit: Some(write_limit.max(1)),
        });
        self
    }

    #[must_use]
    pub fn with_udp_handler(mut self, addr: SocketAddr, handler: SimUdpHandler) -> Self {
        self.udp_handlers.push(SimulationUdpHandler { addr, handler });
        self
    }

    #[must_use]
    pub fn with_dns_a_endpoint(mut self, addr: SocketAddr, address: Ipv4Addr) -> Self {
        self.dns_a_endpoints.push(SimulationDnsAEndpoint { addr, address });
        self
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SimulationEndpointAddresses {
    pub tcp_echo: Option<SocketAddr>,
    pub udp_echo: Option<SocketAddr>,
    pub dns_a: Option<SocketAddr>,
}

pub struct RunningSim<T>
where
    T: GuestFrameTransport,
{
    event_loop: EventLoopCore<SimRuntime<T>, SimOutputSink, NoopCommandSource>,
    status: watch::Receiver<InstanceNetworkStatus>,
    events: Rc<RefCell<Vec<NetworkEventEnvelope>>>,
    reactor: SimReactor,
    clock: SimClock,
    endpoints: SimulationEndpointAddresses,
}

struct SimOutputSink {
    status: watch::Sender<InstanceNetworkStatus>,
    events: Rc<RefCell<Vec<NetworkEventEnvelope>>>,
}

impl NetworkEventSink for SimOutputSink {
    fn emit(&mut self, fill: impl FnOnce(&mut NetworkEventEnvelope)) {
        let mut envelope = NetworkEventEnvelope::default();
        fill(&mut envelope);
        let mut status = self.status.borrow().clone();
        status.observe_event(&envelope);
        let _sent = self.status.send(status);
        self.events.borrow_mut().push(envelope);
    }

    fn flush(&mut self) {}
}

struct NoopCommandSource;

impl NetworkCommandSource for NoopCommandSource {
    fn try_recv(&mut self) -> Option<NetworkCommand> {
        None
    }
}

impl<T> RunningSim<T>
where
    T: GuestFrameTransport,
{
    /// Starts a simulated network using the supplied guest transport and upstream endpoints.
    ///
    /// # Errors
    ///
    /// Returns an error when the network cannot be constructed or registered with the simulated reactor.
    pub fn start(
        spec: InstanceNetworkSpec,
        transport: T,
        upstreams: SimulationUpstreams,
    ) -> Result<Self, InstanceNetworkError> {
        let endpoints = SimEndpointRegistry::new();
        for endpoint in upstreams.tcp_handlers {
            endpoints.tcp_handler(endpoint.addr, endpoint.handler, endpoint.write_limit);
        }
        for endpoint in upstreams.udp_handlers {
            endpoints.udp_handler(endpoint.addr, endpoint.handler);
        }
        for endpoint in upstreams.dns_a_endpoints {
            endpoints.dns_a_at(endpoint.addr, endpoint.address);
        }
        let endpoint_addresses = SimulationEndpointAddresses {
            tcp_echo: upstreams.tcp_echo.then(|| endpoints.tcp_echo()),
            udp_echo: upstreams.udp_echo.then(|| endpoints.udp_echo()),
            dns_a: upstreams.dns_a.map(|address| endpoints.dns_a(address)),
        };
        let reactor = SimReactor::new();
        let clock = SimClock::new();
        let runtime = RuntimeContext::new(
            transport,
            reactor.clone(),
            clock.clone(),
            endpoints.tcp_connector(),
            endpoints.udp_socket_factory(),
        );
        let (status_tx, status_rx) = watch::channel(InstanceNetworkStatus::starting_with_limits(
            &spec.config.limits,
            runtime.clock(),
        ));
        let events = Rc::new(RefCell::new(Vec::new()));
        let mut event_loop = EventLoopCore::new(
            spec,
            runtime,
            NoopCommandSource,
            SimOutputSink {
                status: status_tx,
                events: Rc::clone(&events),
            },
        )?;
        let transport_name = event_loop.begin_connect()?;
        match event_loop.connect_once(&transport_name)? {
            ConnectOutcome::Connected => {}
            ConnectOutcome::Pending => {
                return Err(InstanceNetworkError::TransportConnect {
                    transport: transport_name,
                    source: TransportError::operation("connect guest transport", "simulated transport is pending"),
                });
            }
        }
        Ok(Self {
            event_loop,
            status: status_rx,
            events,
            reactor,
            clock,
            endpoints: endpoint_addresses,
        })
    }

    pub fn drive_once(&mut self) {
        self.reactor.push_ready(ReactorItemId::Guest, true, true);
        let _outcome = self.event_loop.drive_once(Some(Duration::ZERO));
    }

    pub fn drive_once_production_mode(&mut self) {
        let _outcome = self.event_loop.drive_once(None);
    }

    pub fn queue_guest_readiness(&self) {
        self.reactor.push_ready(ReactorItemId::Guest, true, false);
    }

    /// # Errors
    ///
    /// Returns an error when the simulated network cannot allocate the test frame.
    pub fn queue_network_to_guest_frame(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.event_loop.queue_guest_frame_for_test(bytes)
    }

    pub fn advance_clock(&self, duration: Duration) {
        self.clock.advance(duration);
    }

    #[must_use]
    pub fn elapsed_time(&self) -> Duration {
        self.clock.elapsed()
    }

    #[must_use]
    pub fn status(&self) -> InstanceNetworkStatus {
        self.status.borrow().clone()
    }

    #[must_use]
    pub fn events(&self) -> Vec<NetworkEventEnvelope> {
        self.events.borrow().clone()
    }

    #[must_use]
    pub fn buffer_snapshot(&self) -> String {
        format!("{:?}", self.event_loop.buffer_snapshot())
    }

    #[must_use]
    pub fn tcp_snapshot(&self) -> String {
        self.event_loop.tcp_snapshot()
    }

    #[must_use]
    pub fn active_tcp_proxy_slots(&self) -> usize {
        self.event_loop.active_tcp_proxy_slots()
    }

    #[must_use]
    pub fn pending_reactor_ready(&self) -> usize {
        self.reactor.pending_ready_len()
    }

    #[must_use]
    pub const fn endpoints(&self) -> &SimulationEndpointAddresses {
        &self.endpoints
    }

    /// Stops the simulated network and returns its final status and emitted events.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown fails while draining the network state.
    pub fn stop(mut self) -> Result<(InstanceNetworkStatus, Vec<NetworkEventEnvelope>), InstanceNetworkError> {
        match self.event_loop.stop() {
            NetworkExit::Stopped => Ok((self.status(), self.events())),
            NetworkExit::Failed(error) => Err(error),
        }
    }
}
