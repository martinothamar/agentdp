use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr};

use crate::buffers::{BufferPool, ByteBuf, WriteQueue};
use crate::drive::DriveBudget;
use crate::network::{HostConnectionId, HostPortProtocol, HostPortSpec};
use crate::reactor::ReactorItemId;
use crate::reactor::{
    ReactorBackend, ReactorInterest, ReactorReady, ReactorTcpListener, ReactorTcpStream, ReactorUdpSocket,
};
use crate::runtime::NetworkRuntime;
use agentdp_ds::fixed_table::FixedTable;

#[derive(Debug, thiserror::Error)]
#[error("failed to bind {protocol} host port {name} on 127.0.0.1:{host}: {source}")]
pub(crate) struct HostPortBindError {
    name: String,
    protocol: HostPortProtocol,
    host: u16,
    #[source]
    source: std::io::Error,
}

#[derive(Debug)]
pub(crate) enum HostPortEvent {
    TcpAccepted {
        port: u16,
        connection: HostConnectionId,
    },
    TcpBytes {
        connection: HostConnectionId,
        bytes: ByteBuf,
    },
    TcpClosed {
        connection: HostConnectionId,
    },
    UdpDatagram {
        port: u16,
        peer: SocketAddr,
        bytes: ByteBuf,
    },
    Error {
        message: String,
    },
}

pub(crate) struct HostPorts<R: ReactorBackend> {
    tcp: Vec<HostTcpPort<R>>,
    udp: Vec<HostUdpPort<R>>,
    connections: FixedTable<HostConnectionId, IngressTcpConnection<R>>,
    connection_scratch: Vec<HostConnectionId>,
    buffers: BufferPool,
    udp_buffer: Vec<u8>,
    max_tcp_connections: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BoundHostPort<'a> {
    pub(crate) name: &'a str,
    pub(crate) protocol: HostPortProtocol,
    pub(crate) guest: u16,
    pub(crate) host: u16,
}

impl<R> HostPorts<R>
where
    R: ReactorBackend,
{
    /// # Errors
    ///
    /// Returns an error when any configured host port cannot be bound.
    pub(crate) fn bind(
        specs: impl IntoIterator<Item = HostPortSpec>,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> Result<Self, HostPortBindError> {
        let mut ports = Self {
            tcp: Vec::new(),
            udp: Vec::new(),
            connections: FixedTable::with_capacity(buffers.limits().ingress_tcp_connection_limit),
            connection_scratch: Vec::with_capacity(buffers.limits().ingress_tcp_connection_limit),
            buffers: buffers.clone(),
            udp_buffer: vec![0; buffers.limits().ingress_udp_datagram_buffer_capacity],
            max_tcp_connections: buffers.limits().ingress_tcp_connection_limit,
        };
        for spec in specs {
            match spec.protocol {
                HostPortProtocol::Tcp => ports.bind_tcp(&spec, runtime.reactor_mut())?,
                HostPortProtocol::Udp => ports.bind_udp(&spec, runtime.reactor_mut())?,
            }
        }
        Ok(ports)
    }

    pub(crate) fn bound_ports(&self) -> impl Iterator<Item = BoundHostPort<'_>> {
        self.tcp
            .iter()
            .map(|port| BoundHostPort {
                name: &port.name,
                protocol: HostPortProtocol::Tcp,
                guest: port.guest,
                host: port.host,
            })
            .chain(self.udp.iter().map(|port| BoundHostPort {
                name: &port.name,
                protocol: HostPortProtocol::Udp,
                guest: port.guest,
                host: port.host,
            }))
    }

    pub(crate) fn write_tcp(
        &mut self,
        connection: HostConnectionId,
        bytes: ByteBuf,
        runtime: &impl NetworkRuntime<Reactor = R>,
    ) {
        if let Some(connection) = self.connections.get_mut(&connection) {
            connection.pending.push(bytes);
            if !connection.wants_write {
                connection.wants_write = true;
                if let Err(error) = runtime.reactor().reregister_tcp_stream(
                    &mut connection.stream,
                    ReactorItemId::IngressTcpConnection {
                        connection: connection.id,
                    },
                    ReactorInterest::ReadWrite,
                ) {
                    connection.pending.clear();
                    connection.wants_write = false;
                    connection.error = Some(error.to_string());
                }
            }
        }
    }

    pub(crate) fn close_tcp(&mut self, connection: HostConnectionId, runtime: &mut impl NetworkRuntime<Reactor = R>) {
        if let Some(mut connection) = self.connections.remove(&connection) {
            let _deregistered = runtime.reactor_mut().deregister_tcp_stream(
                &mut connection.stream,
                ReactorItemId::IngressTcpConnection {
                    connection: connection.id,
                },
            );
        }
    }

    pub(crate) fn send_udp(
        &mut self,
        port: u16,
        peer: SocketAddr,
        bytes: ByteBuf,
        runtime: &impl NetworkRuntime<Reactor = R>,
    ) {
        if let Some(socket) = self.udp.iter_mut().find(|socket| socket.guest == port) {
            socket.pending.push_back((peer, bytes));
            if !socket.wants_write {
                socket.wants_write = true;
                if let Err(error) = runtime.reactor().reregister_udp_socket(
                    &mut socket.socket,
                    ReactorItemId::IngressUdpSocket { port: socket.host },
                    ReactorInterest::ReadWrite,
                ) {
                    socket.pending.clear();
                    socket.wants_write = false;
                    socket.error = Some(error.to_string());
                }
            }
        }
    }

    pub(crate) fn shutdown(&mut self, runtime: &mut impl NetworkRuntime<Reactor = R>) {
        let mut connections = std::mem::take(&mut self.connection_scratch);
        self.connections.keys_into(&mut connections);
        while let Some(connection) = connections.pop() {
            let _removed = self.remove_connection(connection, runtime.reactor_mut());
        }
        self.connection_scratch = connections;

        for mut port in self.tcp.drain(..) {
            let _deregistered = runtime.reactor_mut().deregister_tcp_listener(
                &mut port.listener,
                ReactorItemId::IngressTcpListener { port: port.host },
            );
        }

        for mut port in self.udp.drain(..) {
            let _deregistered = runtime
                .reactor_mut()
                .deregister_udp_socket(&mut port.socket, ReactorItemId::IngressUdpSocket { port: port.host });
        }
    }

    fn bind_tcp(&mut self, spec: &HostPortSpec, reactor: &mut R) -> Result<(), HostPortBindError> {
        let mut listener =
            R::TcpListener::bind((Ipv4Addr::LOCALHOST, spec.host).into()).map_err(|source| bind_error(spec, source))?;
        let host = listener.local_addr().map_err(|source| bind_error(spec, source))?.port();
        listener
            .prevent_child_inheritance()
            .map_err(|source| bind_error(spec, source))?;
        reactor
            .register_tcp_listener(
                &mut listener,
                ReactorItemId::IngressTcpListener { port: host },
                ReactorInterest::Readable,
            )
            .map_err(|source| bind_error(spec, source))?;
        self.tcp.push(HostTcpPort {
            name: spec.name.clone(),
            host,
            guest: spec.guest,
            listener,
            next_connection: u64::from(spec.guest) << 32,
        });
        Ok(())
    }

    fn bind_udp(&mut self, spec: &HostPortSpec, reactor: &mut R) -> Result<(), HostPortBindError> {
        let mut socket =
            R::UdpSocket::bind((Ipv4Addr::LOCALHOST, spec.host).into()).map_err(|source| bind_error(spec, source))?;
        let host = socket.local_addr().map_err(|source| bind_error(spec, source))?.port();
        socket
            .prevent_child_inheritance()
            .map_err(|source| bind_error(spec, source))?;
        reactor
            .register_udp_socket(
                &mut socket,
                ReactorItemId::IngressUdpSocket { port: host },
                ReactorInterest::Readable,
            )
            .map_err(|source| bind_error(spec, source))?;
        self.udp.push(HostUdpPort {
            name: spec.name.clone(),
            host,
            guest: spec.guest,
            socket,
            pending: VecDeque::new(),
            wants_write: false,
            error: None,
        });
        Ok(())
    }

    #[cfg(test)]
    fn bound_tcp_host_port(&self, guest: u16) -> Option<u16> {
        self.tcp
            .iter()
            .find(|port| port.guest == guest)
            .and_then(|port| port.listener.local_addr().ok())
            .map(|addr| addr.port())
    }

    #[cfg(test)]
    fn bound_udp_host_port(&self, guest: u16) -> Option<u16> {
        self.udp
            .iter()
            .find(|port| port.guest == guest)
            .and_then(|port| port.socket.local_addr().ok())
            .map(|addr| addr.port())
    }

    pub(crate) fn drive_queued(
        &mut self,
        events: &mut Vec<HostPortEvent>,
        budget: &mut DriveBudget,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        let start_len = events.len();
        let mut made_progress = false;
        loop {
            if !budget.step() || !budget.can_continue() {
                break;
            }
            if let Some(event) = self.pop_pending_error() {
                push_event(events, budget, event);
                continue;
            }
            let tcp = self.try_write_tcp(runtime.reactor_mut());
            made_progress |= tcp.made_progress;
            if let Some(event) = tcp.event {
                push_event(events, budget, event);
                continue;
            }
            let udp = self.try_send_udp(runtime.reactor());
            made_progress |= udp.made_progress;
            if let Some(event) = udp.event {
                push_event(events, budget, event);
                continue;
            }
            break;
        }
        made_progress || events.len() > start_len
    }

    pub(crate) fn drive_ready(
        &mut self,
        readiness: &[ReactorReady],
        events: &mut Vec<HostPortEvent>,
        budget: &mut DriveBudget,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        let start_len = events.len();
        let mut made_progress = false;
        for ready in readiness {
            if !budget.step() || !budget.can_continue() {
                break;
            }
            let ReactorReady::Io {
                item,
                readable,
                writable,
            } = *ready
            else {
                continue;
            };
            match item {
                ReactorItemId::IngressTcpListener { port } if readable => {
                    let Some(index) = self.tcp.iter().position(|tcp| tcp.host == port) else {
                        continue;
                    };
                    self.accept_tcp_ready(index, events, budget, runtime.reactor_mut());
                }
                ReactorItemId::IngressTcpConnection { connection } => {
                    if writable {
                        let write = self.try_write_connection(connection, runtime.reactor_mut());
                        made_progress |= write.made_progress;
                        if let Some(event) = write.event {
                            push_event(events, budget, event);
                        }
                    }
                    if readable {
                        self.read_connection_ready(connection, events, budget, runtime.reactor_mut());
                    }
                }
                ReactorItemId::IngressUdpSocket { port } => {
                    let Some(index) = self.udp.iter().position(|udp| udp.host == port) else {
                        continue;
                    };
                    if writable {
                        let send = self.try_send_udp_socket(index, runtime.reactor());
                        made_progress |= send.made_progress;
                        if let Some(event) = send.event {
                            push_event(events, budget, event);
                        }
                    }
                    if readable {
                        self.recv_udp_ready(index, events, budget);
                    }
                }
                ReactorItemId::Guest
                | ReactorItemId::IngressTcpListener { .. }
                | ReactorItemId::TcpProxy { .. }
                | ReactorItemId::UdpProxy { .. } => {}
            }
        }
        made_progress |= self.drive_queued(events, budget, runtime);
        made_progress || events.len() > start_len
    }

    fn accept_tcp_ready(
        &mut self,
        index: usize,
        events: &mut Vec<HostPortEvent>,
        budget: &mut DriveBudget,
        reactor: &mut R,
    ) {
        while budget.can_continue() {
            let Some(event) = self.accept_tcp(index, reactor) else {
                break;
            };
            push_event(events, budget, event);
        }
    }

    fn accept_tcp(&mut self, index: usize, reactor: &mut R) -> Option<HostPortEvent> {
        let port = &mut self.tcp[index];
        match port.listener.accept() {
            Ok((mut stream, _peer)) => {
                if self.connections.len() >= self.max_tcp_connections {
                    return Some(HostPortEvent::Error {
                        message: format!(
                            "host TCP connection limit {} exceeded for guest port {}",
                            self.max_tcp_connections, port.guest
                        ),
                    });
                }
                if let Err(error) = stream.set_nodelay(true) {
                    return Some(HostPortEvent::Error {
                        message: error.to_string(),
                    });
                }
                if let Err(error) = stream.prevent_child_inheritance() {
                    return Some(HostPortEvent::Error {
                        message: error.to_string(),
                    });
                }
                let connection = HostConnectionId(port.next_connection);
                port.next_connection = port.next_connection.saturating_add(1);
                if let Err(error) = reactor.register_tcp_stream(
                    &mut stream,
                    ReactorItemId::IngressTcpConnection { connection },
                    ReactorInterest::Readable,
                ) {
                    return Some(HostPortEvent::Error {
                        message: error.to_string(),
                    });
                }
                if self
                    .connections
                    .insert(
                        connection,
                        IngressTcpConnection {
                            id: connection,
                            stream,
                            pending: WriteQueue::new(),
                            wants_write: false,
                            error: None,
                        },
                    )
                    .is_err()
                {
                    return Some(HostPortEvent::Error {
                        message: format!(
                            "host TCP connection limit {} exceeded for guest port {}",
                            self.max_tcp_connections, port.guest
                        ),
                    });
                }
                Some(HostPortEvent::TcpAccepted {
                    port: port.guest,
                    connection,
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(error) => Some(HostPortEvent::Error {
                message: error.to_string(),
            }),
        }
    }

    fn pop_pending_error(&mut self) -> Option<HostPortEvent> {
        for connection in self.connections.values_mut() {
            if let Some(message) = connection.error.take() {
                return Some(HostPortEvent::Error { message });
            }
        }
        for socket in &mut self.udp {
            if let Some(message) = socket.error.take() {
                return Some(HostPortEvent::Error { message });
            }
        }
        None
    }

    fn read_connection_ready(
        &mut self,
        connection: HostConnectionId,
        events: &mut Vec<HostPortEvent>,
        budget: &mut DriveBudget,
        reactor: &mut R,
    ) {
        while budget.can_continue() {
            let Some(event) = self.try_read_connection(connection, reactor) else {
                break;
            };
            let terminal = matches!(event, HostPortEvent::TcpClosed { .. } | HostPortEvent::Error { .. });
            push_event(events, budget, event);
            if terminal {
                break;
            }
        }
    }

    fn try_read_connection(&mut self, connection: HostConnectionId, reactor: &mut R) -> Option<HostPortEvent> {
        let Ok(mut bytes) = self.buffers.try_tcp_byte() else {
            return None;
        };
        bytes.resize_zeroed(self.buffers.tcp_byte_capacity());
        let read = {
            let connection_state = self.connections.get_mut(&connection)?;
            std::io::Read::read(&mut connection_state.stream, bytes.as_mut_slice())
        };
        match read {
            Ok(0) => self
                .remove_connection(connection, reactor)
                .map(|()| HostPortEvent::TcpClosed { connection }),
            Ok(len) => {
                bytes.truncate(len);
                Some(HostPortEvent::TcpBytes { connection, bytes })
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(error) => self
                .remove_connection(connection, reactor)
                .map(|()| HostPortEvent::Error {
                    message: error.to_string(),
                }),
        }
    }

    fn try_write_connection(&mut self, connection: HostConnectionId, reactor: &mut R) -> WriteStep {
        let Some(connection_state) = self.connections.get_mut(&connection) else {
            return WriteStep::blocked();
        };
        let made_progress = match connection_state.pending.flush_to_std(&mut connection_state.stream) {
            Ok(step) => step.made_progress(),
            Err(error) => {
                let message = error.to_string();
                self.remove_connection(connection, reactor);
                return WriteStep {
                    made_progress: false,
                    event: Some(HostPortEvent::Error { message }),
                };
            }
        };
        if connection_state.pending.is_empty() && connection_state.wants_write {
            connection_state.wants_write = false;
            if let Err(error) = reactor.reregister_tcp_stream(
                &mut connection_state.stream,
                ReactorItemId::IngressTcpConnection {
                    connection: connection_state.id,
                },
                ReactorInterest::Readable,
            ) {
                return WriteStep {
                    made_progress,
                    event: Some(HostPortEvent::Error {
                        message: error.to_string(),
                    }),
                };
            }
        }
        WriteStep {
            made_progress,
            event: None,
        }
    }

    fn try_write_tcp(&mut self, reactor: &mut R) -> WriteStep {
        let mut ids = std::mem::take(&mut self.connection_scratch);
        self.connections.keys_into(&mut ids);
        let mut result = WriteStep::blocked();
        let mut index = 0;
        while index < ids.len() {
            let id = ids[index];
            let step = self.try_write_connection(id, reactor);
            if step.made_progress || step.event.is_some() {
                result = step;
                break;
            }
            index += 1;
        }
        self.connection_scratch = ids;
        result
    }

    fn recv_udp_ready(&mut self, index: usize, events: &mut Vec<HostPortEvent>, budget: &mut DriveBudget) {
        while budget.can_continue() {
            let Some(event) = self.try_recv_udp_socket(index) else {
                break;
            };
            push_event(events, budget, event);
        }
    }

    fn try_recv_udp_socket(&mut self, index: usize) -> Option<HostPortEvent> {
        let port = &mut self.udp[index];
        match port.socket.recv_from(&mut self.udp_buffer) {
            Ok((len, peer)) => {
                let Ok(mut bytes) = self.buffers.try_byte_with_capacity(len) else {
                    return None;
                };
                bytes.extend_from_slice(&self.udp_buffer[..len]);
                Some(HostPortEvent::UdpDatagram {
                    port: port.guest,
                    peer,
                    bytes,
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(error) => Some(HostPortEvent::Error {
                message: error.to_string(),
            }),
        }
    }

    fn try_send_udp_socket(&mut self, index: usize, reactor: &R) -> WriteStep {
        let port = &mut self.udp[index];
        let mut made_progress = false;
        while let Some((peer, bytes)) = port.pending.front() {
            match port.socket.send_to(bytes.as_slice(), *peer) {
                Ok(_sent) => {
                    made_progress = true;
                    port.pending.pop_front();
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    port.pending.pop_front();
                    return WriteStep {
                        made_progress,
                        event: Some(HostPortEvent::Error {
                            message: error.to_string(),
                        }),
                    };
                }
            }
        }
        if port.pending.is_empty() && port.wants_write {
            port.wants_write = false;
            if let Err(error) = reactor.reregister_udp_socket(
                &mut port.socket,
                ReactorItemId::IngressUdpSocket { port: port.host },
                ReactorInterest::Readable,
            ) {
                return WriteStep {
                    made_progress,
                    event: Some(HostPortEvent::Error {
                        message: error.to_string(),
                    }),
                };
            }
        }
        WriteStep {
            made_progress,
            event: None,
        }
    }

    fn try_send_udp(&mut self, reactor: &R) -> WriteStep {
        for index in 0..self.udp.len() {
            let step = self.try_send_udp_socket(index, reactor);
            if step.made_progress || step.event.is_some() {
                return step;
            }
        }
        WriteStep::blocked()
    }

    fn remove_connection(&mut self, connection: HostConnectionId, reactor: &mut R) -> Option<()> {
        let mut connection = self.connections.remove(&connection)?;
        let _deregistered = reactor.deregister_tcp_stream(
            &mut connection.stream,
            ReactorItemId::IngressTcpConnection {
                connection: connection.id,
            },
        );
        Some(())
    }
}

fn push_event(events: &mut Vec<HostPortEvent>, budget: &mut DriveBudget, event: HostPortEvent) {
    let bytes = match &event {
        HostPortEvent::TcpBytes { bytes, .. } | HostPortEvent::UdpDatagram { bytes, .. } => bytes.len(),
        HostPortEvent::TcpAccepted { .. } | HostPortEvent::TcpClosed { .. } | HostPortEvent::Error { .. } => 1,
    };
    if budget.event(bytes) {
        events.push(event);
    }
}

fn bind_error(spec: &HostPortSpec, source: std::io::Error) -> HostPortBindError {
    HostPortBindError {
        name: spec.name.clone(),
        protocol: spec.protocol,
        host: spec.host,
        source,
    }
}

#[derive(Debug)]
struct HostTcpPort<R: ReactorBackend> {
    name: String,
    host: u16,
    guest: u16,
    listener: R::TcpListener,
    next_connection: u64,
}

#[derive(Debug)]
struct HostUdpPort<R: ReactorBackend> {
    name: String,
    host: u16,
    guest: u16,
    socket: R::UdpSocket,
    pending: VecDeque<(SocketAddr, ByteBuf)>,
    wants_write: bool,
    error: Option<String>,
}

#[derive(Debug)]
struct IngressTcpConnection<R: ReactorBackend> {
    id: HostConnectionId,
    stream: R::TcpStream,
    pending: WriteQueue,
    wants_write: bool,
    error: Option<String>,
}

struct WriteStep {
    made_progress: bool,
    event: Option<HostPortEvent>,
}

impl WriteStep {
    const fn blocked() -> Self {
        Self {
            made_progress: false,
            event: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::net::Ipv4Addr;
    use std::time::Duration;

    use crate::buffers::BufferPool;
    use crate::drive::DriveBudget;
    use crate::network::{HostPortProtocol, HostPortSpec, NetworkLimits};
    use crate::reactor::{ReactorBackend, default_backend};
    use crate::runtime::NetworkRuntime;
    use crate::test_support::unit::runtime_context;

    use super::{HostPortEvent, HostPorts};

    fn test_buffers() -> BufferPool {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        buffers
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mio_host_ports_accept_read_write_and_udp_are_readiness_driven() -> Result<(), Box<dyn std::error::Error>> {
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let buffers = test_buffers();
        let mut host_ports = HostPorts::bind(
            [
                host_port("tcp", HostPortProtocol::Tcp, 3000),
                host_port("udp", HostPortProtocol::Udp, 5353),
            ],
            &buffers,
            &mut runtime,
        )?;

        let tcp_host = host_ports
            .bound_tcp_host_port(3000)
            .ok_or("TCP host port was not bound")?;
        let udp_host = host_ports
            .bound_udp_host_port(5353)
            .ok_or("UDP host port was not bound")?;
        let mut tcp_peer = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;
        tcp_peer.set_nodelay(true)?;
        let events = wait_until_host_event(&mut runtime, &mut host_ports, |event| {
            matches!(event, HostPortEvent::TcpAccepted { .. })
        })?;
        let connection = match events.as_slice() {
            [HostPortEvent::TcpAccepted { connection, .. }] => *connection,
            _ => return Err("expected accepted connection".into()),
        };

        tcp_peer.write_all(b"from-peer")?;
        let events = wait_until_host_event(&mut runtime, &mut host_ports, |event| {
            matches!(event, HostPortEvent::TcpBytes { .. })
        })?;
        match events.as_slice() {
            [
                HostPortEvent::TcpBytes {
                    connection: event_connection,
                    bytes,
                },
            ] => {
                assert_eq!(*event_connection, connection);
                assert_eq!(bytes.as_slice(), b"from-peer");
            }
            _ => return Err("expected tcp bytes event".into()),
        }

        host_ports.write_tcp(connection, io_buffer(&buffers, b"to-peer"), &runtime);
        assert!(drive_queued(&mut runtime, &mut host_ports)?);
        let mut observed = [0_u8; 7];
        tcp_peer.read_exact(&mut observed)?;
        assert_eq!(&observed, b"to-peer");

        let udp_peer = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        udp_peer.connect((Ipv4Addr::LOCALHOST, udp_host))?;
        udp_peer.send(b"udp-in")?;
        let events = wait_until_host_event(&mut runtime, &mut host_ports, |event| {
            matches!(event, HostPortEvent::UdpDatagram { .. })
        })?;
        let peer_addr = udp_peer.local_addr()?;
        match events.as_slice() {
            [HostPortEvent::UdpDatagram { port, peer, bytes }] => {
                assert_eq!(*port, 5353);
                assert_eq!(*peer, peer_addr);
                assert_eq!(bytes.as_slice(), b"udp-in");
            }
            _ => return Err("expected udp datagram event".into()),
        }

        host_ports.send_udp(5353, peer_addr, io_buffer(&buffers, b"udp-out"), &runtime);
        assert!(drive_queued(&mut runtime, &mut host_ports)?);
        let mut observed = [0_u8; 7];
        let len = udp_peer.recv(&mut observed)?;
        assert_eq!(&observed[..len], b"udp-out");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mio_host_ports_drain_readiness_until_would_block() -> Result<(), Box<dyn std::error::Error>> {
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let buffers = test_buffers();
        let mut host_ports = HostPorts::bind(
            [
                host_port("tcp", HostPortProtocol::Tcp, 3000),
                host_port("udp", HostPortProtocol::Udp, 5353),
            ],
            &buffers,
            &mut runtime,
        )?;
        let tcp_host = host_ports
            .bound_tcp_host_port(3000)
            .ok_or("TCP host port was not bound")?;
        let udp_host = host_ports
            .bound_udp_host_port(5353)
            .ok_or("UDP host port was not bound")?;

        let _tcp_a = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;
        let _tcp_b = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;
        let events = wait_until_host_event_count(&mut runtime, &mut host_ports, 2, |event| {
            matches!(event, HostPortEvent::TcpAccepted { .. })
        })?;
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, HostPortEvent::TcpAccepted { .. }))
                .count(),
            2
        );

        let udp_peer = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        udp_peer.connect((Ipv4Addr::LOCALHOST, udp_host))?;
        udp_peer.send(b"one")?;
        udp_peer.send(b"two")?;
        let events = wait_until_host_event_count(&mut runtime, &mut host_ports, 2, |event| {
            matches!(event, HostPortEvent::UdpDatagram { .. })
        })?;
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, HostPortEvent::UdpDatagram { .. }))
                .count(),
            2
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tcp_accept_reports_limit_exhaustion() -> Result<(), Box<dyn std::error::Error>> {
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let buffers = BufferPool::new(NetworkLimits {
            ingress_tcp_connection_limit: 0,
            ..NetworkLimits::default()
        });
        buffers.prewarm_instance_network();
        let mut host_ports = HostPorts::bind([host_port("tcp", HostPortProtocol::Tcp, 3000)], &buffers, &mut runtime)?;
        let tcp_host = host_ports
            .bound_tcp_host_port(3000)
            .ok_or("TCP host port was not bound")?;
        let _tcp_peer = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;

        let events = wait_until_host_event(&mut runtime, &mut host_ports, |event| {
            matches!(event, HostPortEvent::Error { .. })
        })?;

        match events.as_slice() {
            [HostPortEvent::Error { message }] => {
                assert!(message.contains("host TCP connection limit 0 exceeded"));
            }
            _ => return Err("expected host TCP connection limit event".into()),
        }
        Ok(())
    }

    fn wait_until_host_event<N>(
        runtime: &mut N,
        host_ports: &mut HostPorts<N::Reactor>,
        done: impl FnMut(&HostPortEvent) -> bool,
    ) -> Result<Vec<HostPortEvent>, Box<dyn std::error::Error>>
    where
        N: NetworkRuntime,
    {
        wait_until_host_event_count(runtime, host_ports, 1, done)
    }

    fn wait_until_host_event_count<N>(
        runtime: &mut N,
        host_ports: &mut HostPorts<N::Reactor>,
        count: usize,
        mut matches_event: impl FnMut(&HostPortEvent) -> bool,
    ) -> Result<Vec<HostPortEvent>, Box<dyn std::error::Error>>
    where
        N: NetworkRuntime,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut output = Vec::new();
        let mut readiness = Vec::new();
        while tokio::time::Instant::now() < deadline {
            runtime
                .reactor_mut()
                .ready_into(&mut readiness, Some(deadline - tokio::time::Instant::now()))?;
            let mut budget = DriveBudget::event_loop(&crate::network::NetworkLimits::default());
            host_ports.drive_ready(&readiness, &mut output, &mut budget, runtime);
            if output.iter().filter(|event| matches_event(event)).count() >= count {
                return Ok(output);
            }
            output.clear();
        }
        Err("timed out waiting for mio host port readiness".into())
    }

    fn drive_queued<N>(
        runtime: &mut N,
        host_ports: &mut HostPorts<N::Reactor>,
    ) -> Result<bool, Box<dyn std::error::Error>>
    where
        N: NetworkRuntime,
    {
        let mut events = Vec::new();
        let mut budget = DriveBudget::event_loop(&crate::network::NetworkLimits::default());
        let result = host_ports.drive_queued(&mut events, &mut budget, runtime);
        if events.is_empty() {
            Ok(result)
        } else {
            Err(format!("unexpected host port events while draining queued writes: {events:?}").into())
        }
    }

    fn host_port(name: &str, protocol: HostPortProtocol, guest: u16) -> HostPortSpec {
        HostPortSpec {
            name: name.to_owned(),
            protocol,
            guest,
            host: 0,
        }
    }

    fn io_buffer(buffers: &BufferPool, bytes: &[u8]) -> crate::buffers::ByteBuf {
        let mut buffer = buffers
            .try_byte_with_capacity(bytes.len())
            .expect("prewarmed byte buffer");
        buffer.extend_from_slice(bytes);
        buffer
    }
}
