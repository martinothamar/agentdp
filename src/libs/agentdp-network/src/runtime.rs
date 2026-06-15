use crate::clock::{NetworkClock, SystemClock};
use crate::connectors::tcp::{ProductionTcpConnector, TcpConnector};
use crate::connectors::udp::{ProductionUdpSocketFactory, UdpSocketFactory};
use crate::guest::{GuestFrameTransport, TransportError};
use crate::reactor::{MioReactor, ReactorBackend, default_backend};

pub(crate) trait NetworkRuntime: 'static {
    type Transport: GuestFrameTransport;
    type Reactor: ReactorBackend + 'static;
    type Clock: NetworkClock;
    type TcpConnector: TcpConnector<Self::Reactor>;
    type UdpSocketFactory: UdpSocketFactory<Self::Reactor>;

    fn transport(&self) -> &Self::Transport;
    fn transport_mut(&mut self) -> &mut Self::Transport;
    fn reactor(&self) -> &Self::Reactor;
    fn reactor_mut(&mut self) -> &mut Self::Reactor;
    fn clock(&self) -> &Self::Clock;
    fn tcp_connector(&self) -> &Self::TcpConnector;
    fn udp_socket_factory(&self) -> &Self::UdpSocketFactory;
    fn cleanup(self) -> Result<(), TransportError>;
}

pub(crate) struct RuntimeContext<T, R, C, K, U> {
    transport: T,
    reactor: R,
    clock: C,
    tcp_connector: K,
    udp_socket_factory: U,
}

pub(crate) type ProductionRuntime<T> =
    RuntimeContext<T, MioReactor, SystemClock, ProductionTcpConnector, ProductionUdpSocketFactory>;

impl<T, R, C, K, U> RuntimeContext<T, R, C, K, U> {
    pub(crate) const fn new(transport: T, reactor: R, clock: C, tcp_connector: K, udp_socket_factory: U) -> Self {
        Self {
            transport,
            reactor,
            clock,
            tcp_connector,
            udp_socket_factory,
        }
    }
}

impl<T, R, C, K, U> NetworkRuntime for RuntimeContext<T, R, C, K, U>
where
    T: GuestFrameTransport,
    R: ReactorBackend + 'static,
    C: NetworkClock,
    K: TcpConnector<R>,
    U: UdpSocketFactory<R>,
{
    type Transport = T;
    type Reactor = R;
    type Clock = C;
    type TcpConnector = K;
    type UdpSocketFactory = U;

    fn transport(&self) -> &Self::Transport {
        &self.transport
    }

    fn transport_mut(&mut self) -> &mut Self::Transport {
        &mut self.transport
    }

    fn reactor(&self) -> &Self::Reactor {
        &self.reactor
    }

    fn reactor_mut(&mut self) -> &mut Self::Reactor {
        &mut self.reactor
    }

    fn clock(&self) -> &Self::Clock {
        &self.clock
    }

    fn tcp_connector(&self) -> &Self::TcpConnector {
        &self.tcp_connector
    }

    fn udp_socket_factory(&self) -> &Self::UdpSocketFactory {
        &self.udp_socket_factory
    }

    fn cleanup(self) -> Result<(), TransportError> {
        self.transport.cleanup()
    }
}

pub(crate) fn production_runtime<T>(
    transport: T,
    reactor_event_capacity: usize,
) -> std::io::Result<ProductionRuntime<T>>
where
    T: GuestFrameTransport,
{
    let reactor = default_backend(reactor_event_capacity)?;
    Ok(RuntimeContext::new(
        transport,
        reactor,
        SystemClock,
        ProductionTcpConnector,
        ProductionUdpSocketFactory,
    ))
}
