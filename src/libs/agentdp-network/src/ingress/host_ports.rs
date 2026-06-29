use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr};

use crate::buffers::{BufferPool, ByteBuf, WriteQueue};
use crate::drive::{DriveApply, DriveDatagramRecvFrom, DriveDatagramSend, DriveRunnable, DriveStreamRead, DriveTurn};
use crate::network::{HostConnectionId, HostPortProtocol, HostPortSpec};
use crate::reactor::ReactorItemId;
use crate::reactor::{
    ReactorBackend, ReactorInterest, ReactorReady, ReactorTcpListener, ReactorTcpStream, ReactorUdpSocket,
    RegisteredTcpListener, RegisteredTcpStream, RegisteredUdpSocket, RegisteringTcpListener, RegisteringTcpStream,
    RegisteringUdpSocket,
};
use crate::runtime::NetworkRuntime;
use agentdp_ds::fixed_table::{FixedTable, FixedTableReserveError};

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

impl HostPortEvent {
    fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }
}

pub(crate) struct HostPorts<R: ReactorBackend> {
    tcp: Vec<HostTcpPort<R>>,
    udp: Vec<HostUdpPort<R>>,
    connections: FixedTable<HostConnectionId, IngressTcpConnection<R>>,
    connection_scratch: Vec<HostConnectionId>,
    port_scratch: Vec<u16>,
    pending_events: VecDeque<HostPortEvent>,
    buffers: BufferPool,
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
            port_scratch: Vec::new(),
            pending_events: VecDeque::new(),
            buffers: buffers.clone(),
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
            if connection.write_state != HostWriteState::Open {
                return;
            }
            connection.pending.push(bytes);
            if !connection.stream.io().watches_write() {
                match connection
                    .stream
                    .reregister(runtime.reactor(), ReactorInterest::ReadWrite)
                {
                    Ok(()) => {}
                    Err(error) => {
                        connection.pending.clear();
                        connection.error = Some(error.to_string());
                    }
                }
            }
        }
    }

    pub(crate) fn finish_tcp_write(&mut self, connection: HostConnectionId) {
        let Some(connection) = self.connections.get_mut(&connection) else {
            return;
        };
        if connection.write_state == HostWriteState::Open {
            connection.write_state = HostWriteState::FinishRequested;
        }
        if let Some(error) = finish_write_if_ready(connection) {
            connection.error = Some(error);
        }
    }

    pub(crate) fn close_tcp(&mut self, connection: HostConnectionId, runtime: &mut impl NetworkRuntime<Reactor = R>) {
        let should_close = if let Some(connection) = self.connections.get_mut(&connection) {
            connection.close_requested = true;
            connection.pending.is_empty()
        } else {
            false
        };
        if should_close {
            self.remove_connection(connection, runtime.reactor_mut());
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
            if !socket.socket.io().watches_write() {
                match socket.socket.reregister(runtime.reactor(), ReactorInterest::ReadWrite) {
                    Ok(()) => {}
                    Err(error) => {
                        socket.pending.clear();
                        socket.error = Some(error.to_string());
                    }
                }
            }
        }
    }

    pub(crate) fn shutdown(&mut self, runtime: &mut impl NetworkRuntime<Reactor = R>) {
        self.pending_events.clear();
        self.port_scratch.clear();
        let mut connections = std::mem::take(&mut self.connection_scratch);
        self.connections.keys_into(&mut connections);
        while let Some(connection) = connections.pop() {
            let _removed = self.remove_connection(connection, runtime.reactor_mut());
        }
        self.connection_scratch = connections;

        for mut port in self.tcp.drain(..) {
            port.listener.deregister(runtime.reactor_mut());
        }

        for mut port in self.udp.drain(..) {
            port.socket.deregister(runtime.reactor_mut());
        }
    }

    fn bind_tcp(&mut self, spec: &HostPortSpec, reactor: &mut R) -> Result<(), HostPortBindError> {
        let listener =
            R::TcpListener::bind((Ipv4Addr::LOCALHOST, spec.host).into()).map_err(|source| bind_error(spec, source))?;
        let host = listener.local_addr().map_err(|source| bind_error(spec, source))?.port();
        listener
            .prevent_child_inheritance()
            .map_err(|source| bind_error(spec, source))?;
        let listener = RegisteringTcpListener::new(
            reactor,
            listener,
            ReactorItemId::IngressTcpListener { port: host },
            ReactorInterest::Readable,
        )
        .map_err(|source| bind_error(spec, source))?
        .commit();
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
        let socket =
            R::UdpSocket::bind((Ipv4Addr::LOCALHOST, spec.host).into()).map_err(|source| bind_error(spec, source))?;
        let host = socket.local_addr().map_err(|source| bind_error(spec, source))?.port();
        socket
            .prevent_child_inheritance()
            .map_err(|source| bind_error(spec, source))?;
        let socket = RegisteringUdpSocket::new(
            reactor,
            socket,
            ReactorItemId::IngressUdpSocket { port: host },
            ReactorInterest::Readable,
        )
        .map_err(|source| bind_error(spec, source))?
        .commit();
        self.udp.push(HostUdpPort {
            name: spec.name.clone(),
            host,
            guest: spec.guest,
            socket,
            pending: VecDeque::new(),
            error: None,
        });
        Ok(())
    }

    #[cfg(test)]
    fn bound_tcp_host_port(&self, guest: u16) -> Option<u16> {
        self.tcp
            .iter()
            .find(|port| port.guest == guest)
            .and_then(|port| port.listener.source().local_addr().ok())
            .map(|addr| addr.port())
    }

    #[cfg(test)]
    fn bound_udp_host_port(&self, guest: u16) -> Option<u16> {
        self.udp
            .iter()
            .find(|port| port.guest == guest)
            .and_then(|port| port.socket.source().local_addr().ok())
            .map(|addr| addr.port())
    }

    pub(crate) fn drive_queued(
        &mut self,
        blocked_tcp_reads: &[HostConnectionId],
        events: &mut Vec<HostPortEvent>,
        drive: &mut DriveTurn<'_>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        loop {
            if let Some(event) = self.pending_events.pop_front() {
                if !self.emit_or_queue(events, drive, event) {
                    break;
                }
                continue;
            }
            if !drive.can_start_operation() {
                break;
            }
            if let Some(event) = self.pop_pending_error() {
                if !self.emit_or_queue(events, drive, event) {
                    break;
                }
                continue;
            }

            let before = drive.progress();
            if let Some(event) = self.drive_tcp_writes(runtime.reactor_mut(), drive) {
                if !self.emit_or_queue(events, drive, event) {
                    break;
                }
                continue;
            }
            if drive.progress() != before {
                continue;
            }

            let before = drive.progress();
            if let Some(event) = self.drive_udp_writes(runtime.reactor_mut(), drive) {
                if !self.emit_or_queue(events, drive, event) {
                    break;
                }
                continue;
            }
            if drive.progress() != before {
                continue;
            }

            let before = drive.progress();
            self.drive_tcp_reads(blocked_tcp_reads, events, drive, runtime.reactor_mut());
            if drive.progress() != before {
                continue;
            }

            let before = drive.progress();
            self.drive_udp_reads(events, drive);
            if drive.progress() != before {
                continue;
            }

            self.drive_accepts(events, drive, runtime.reactor_mut());
            break;
        }
    }

    pub(crate) fn drive_ready(
        &mut self,
        readiness: &[ReactorReady],
        blocked_tcp_reads: &[HostConnectionId],
        events: &mut Vec<HostPortEvent>,
        drive: &mut DriveTurn<'_>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        self.latch_ready(readiness);
        self.drive_queued(blocked_tcp_reads, events, drive, runtime);
    }

    fn latch_ready(&mut self, readiness: &[ReactorReady]) {
        for ready in readiness {
            let ReactorReady::Io {
                item,
                readable,
                writable,
            } = *ready
            else {
                continue;
            };
            match item {
                ReactorItemId::IngressTcpListener { port } => {
                    if let Some(listener) = self.tcp.iter_mut().find(|listener| listener.host == port) {
                        listener.listener.mark_reactor_ready(readable, writable);
                    }
                }
                ReactorItemId::IngressTcpConnection { connection } => {
                    if let Some(connection) = self.connections.get_mut(&connection) {
                        connection.stream.mark_reactor_ready(readable, writable);
                    }
                }
                ReactorItemId::IngressUdpSocket { port } => {
                    if let Some(socket) = self.udp.iter_mut().find(|socket| socket.host == port) {
                        socket.socket.mark_reactor_ready(readable, writable);
                    }
                }
                ReactorItemId::Guest | ReactorItemId::TcpProxy { .. } | ReactorItemId::UdpProxy { .. } => {}
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "host-port accept readiness and capacity handling is kept in one loop"
    )]
    fn drive_accepts(&mut self, events: &mut Vec<HostPortEvent>, drive: &mut DriveTurn<'_>, reactor: &mut R) {
        self.port_scratch.clear();
        self.port_scratch.extend(
            self.tcp
                .iter()
                .filter_map(|tcp| tcp.listener.io().can_read().then_some(tcp.host)),
        );
        while let Some(port) = self.port_scratch.pop() {
            if !drive.can_start_operation() {
                break;
            }
            let Some(tcp_index) = self.tcp.iter().position(|tcp| tcp.host == port) else {
                continue;
            };
            loop {
                let guest_port = self.tcp[tcp_index].guest;
                if self.max_tcp_connections == 0 {
                    let mut port = self.tcp.swap_remove(tcp_index);
                    port.listener.deregister(reactor);
                    if !self.emit_error_or_queue(
                        events,
                        drive,
                        format!(
                            "host TCP connection limit {} exceeded for guest port {}",
                            self.max_tcp_connections, guest_port
                        ),
                    ) {
                        return;
                    }
                    break;
                }
                if self.connections.len() >= self.max_tcp_connections {
                    drive.wait_for_connection_slot();
                    return;
                }
                let accept = self.tcp[tcp_index].listener.source().accept();
                let (stream, _peer) = match accept {
                    Ok(accepted) => accepted,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        drive.wait_for_reactor_read();
                        self.tcp[tcp_index].listener.clear_read_after_would_block();
                        break;
                    }
                    Err(error) => {
                        let mut port = self.tcp.swap_remove(tcp_index);
                        port.listener.deregister(reactor);
                        if !self.emit_error_or_queue(events, drive, error.to_string()) {
                            return;
                        }
                        break;
                    }
                };
                if let Err(error) = stream.set_nodelay(true) {
                    if !self.emit_error_or_queue(events, drive, error.to_string()) {
                        return;
                    }
                    if !drive.can_start_operation() {
                        return;
                    }
                    continue;
                }
                if let Err(error) = stream.prevent_child_inheritance() {
                    if !self.emit_error_or_queue(events, drive, error.to_string()) {
                        return;
                    }
                    if !drive.can_start_operation() {
                        return;
                    }
                    continue;
                }
                let connection = {
                    let port = &mut self.tcp[tcp_index];
                    let connection = HostConnectionId(port.next_connection);
                    port.next_connection = port.next_connection.saturating_add(1);
                    connection
                };
                let reservation = match self.connections.reserve_vacant(connection) {
                    Ok(reservation) => reservation,
                    Err(FixedTableReserveError::KeyExists | FixedTableReserveError::Full) => {
                        if !self.emit_error_or_queue(
                            events,
                            drive,
                            format!(
                                "host TCP connection limit {} exceeded for guest port {}",
                                self.max_tcp_connections, guest_port
                            ),
                        ) {
                            return;
                        }
                        if !drive.can_start_operation() {
                            return;
                        }
                        continue;
                    }
                };
                let stream = match RegisteringTcpStream::new(
                    reactor,
                    stream,
                    ReactorItemId::IngressTcpConnection { connection },
                    ReactorInterest::Readable,
                ) {
                    Ok(registered) => registered.commit(),
                    Err(error) => {
                        drop(reservation);
                        if !self.emit_error_or_queue(events, drive, error.to_string()) {
                            return;
                        }
                        if !drive.can_start_operation() {
                            return;
                        }
                        continue;
                    }
                };
                let accepted_connection = IngressTcpConnection {
                    stream,
                    pending: WriteQueue::new(),
                    write_state: HostWriteState::Open,
                    close_requested: false,
                    error: None,
                };
                reservation.insert(accepted_connection);
                if !self.emit_or_queue(
                    events,
                    drive,
                    HostPortEvent::TcpAccepted {
                        port: guest_port,
                        connection,
                    },
                ) {
                    return;
                }
                if !drive.can_start_operation() {
                    return;
                }
            }
        }
    }

    fn pop_pending_error(&mut self) -> Option<HostPortEvent> {
        for connection in self.connections.values_mut() {
            if let Some(message) = connection.error.take() {
                return Some(HostPortEvent::error(message));
            }
        }
        for socket in &mut self.udp {
            if let Some(message) = socket.error.take() {
                return Some(HostPortEvent::error(message));
            }
        }
        None
    }

    fn drive_tcp_reads(
        &mut self,
        blocked_tcp_reads: &[HostConnectionId],
        events: &mut Vec<HostPortEvent>,
        drive: &mut DriveTurn<'_>,
        reactor: &mut R,
    ) {
        self.connection_scratch.clear();
        self.connection_scratch.extend(
            self.connections
                .iter()
                .filter_map(|(connection, state)| state.stream.io().can_read().then_some(connection)),
        );
        while let Some(connection) = self.connection_scratch.pop() {
            if !drive.can_start_operation() {
                break;
            }
            if blocked_tcp_reads.contains(&connection) {
                drive.wait_for_guest_send_capacity();
                continue;
            }
            let read = {
                let Some(connection_state) = self.connections.get_mut(&connection) else {
                    continue;
                };
                let (stream, io) = connection_state.stream.source_and_io_mut();
                drive.read_stream_ready(io, &self.buffers, stream, DriveRunnable::READ_UPSTREAM)
            };
            match read {
                Ok(DriveStreamRead::Bytes(bytes)) => {
                    let event = HostPortEvent::TcpBytes { connection, bytes };
                    if !self.emit_or_queue(events, drive, event) {
                        return;
                    }
                }
                Ok(DriveStreamRead::Closed) => {
                    if self.remove_connection(connection, reactor).is_some() {
                        let event = HostPortEvent::TcpClosed { connection };
                        if !self.emit_or_queue(events, drive, event) {
                            return;
                        }
                    }
                }
                Ok(DriveStreamRead::NotReady | DriveStreamRead::WouldBlock) => {}
                Ok(DriveStreamRead::Blocked) => return,
                Err(error) => {
                    if self.remove_connection(connection, reactor).is_some()
                        && !self.emit_error_or_queue(events, drive, error.to_string())
                    {
                        return;
                    }
                }
            }
        }
    }

    fn drive_tcp_writes(&mut self, reactor: &mut R, drive: &mut DriveTurn<'_>) -> Option<HostPortEvent> {
        self.connection_scratch.clear();
        self.connection_scratch
            .extend(self.connections.iter().filter_map(|(connection, state)| {
                (!state.pending.is_empty() && state.stream.io().can_write()).then_some(connection)
            }));
        while let Some(connection) = self.connection_scratch.pop() {
            if !drive.can_start_operation() {
                break;
            }
            let remove_error = {
                let Some(connection_state) = self.connections.get_mut(&connection) else {
                    continue;
                };
                if connection_state.pending.is_empty() {
                    continue;
                }
                let mut remove_error = None;
                let (stream, io) = connection_state.stream.source_and_io_mut();
                let write = match drive.write_stream_queue_ready(io, &mut connection_state.pending, stream) {
                    Ok(write) => Some(write),
                    Err(error) => {
                        remove_error = Some(error.to_string());
                        None
                    }
                };
                if write.is_some() {
                    if let Some(message) = finish_write_if_ready(connection_state) {
                        return Some(HostPortEvent::error(message));
                    }
                    if connection_state.pending.is_empty() && connection_state.stream.io().watches_write() {
                        match drive.try_apply_state_change(|| {
                            connection_state.stream.reregister(reactor, ReactorInterest::Readable)
                        }) {
                            DriveApply::Applied(()) => {}
                            DriveApply::Failed(error) => {
                                remove_error = Some(error.to_string());
                            }
                            DriveApply::Deferred => continue,
                        }
                    }
                    if remove_error.is_none() && connection_state.close_requested && connection_state.pending.is_empty()
                    {
                        let _removed = drive.apply_state_change(|| self.remove_connection(connection, reactor));
                        continue;
                    }
                }
                remove_error
            };
            if let Some(message) = remove_error {
                self.remove_connection(connection, reactor);
                return Some(HostPortEvent::error(message));
            }
        }
        None
    }

    fn drive_udp_reads(&mut self, events: &mut Vec<HostPortEvent>, drive: &mut DriveTurn<'_>) {
        self.port_scratch.clear();
        self.port_scratch.extend(
            self.udp
                .iter()
                .filter_map(|udp| udp.socket.io().can_read().then_some(udp.host)),
        );
        while let Some(port) = self.port_scratch.pop() {
            if !drive.can_start_operation() {
                break;
            }
            let Some(udp_index) = self.udp.iter().position(|udp| udp.host == port) else {
                continue;
            };
            loop {
                let udp = &mut self.udp[udp_index];
                let guest = udp.guest;
                let (socket, io) = udp.socket.source_and_io_mut();
                match drive.recv_datagram_from_ready(
                    io,
                    &self.buffers,
                    socket,
                    self.buffers.limits().ingress_udp_datagram_buffer_capacity,
                    DriveRunnable::READ_UPSTREAM,
                ) {
                    Ok(DriveDatagramRecvFrom::Bytes { bytes, peer }) => {
                        let event = HostPortEvent::UdpDatagram {
                            port: guest,
                            peer,
                            bytes,
                        };
                        if !self.emit_or_queue(events, drive, event) {
                            return;
                        }
                        if !drive.can_start_operation() {
                            return;
                        }
                    }
                    Ok(DriveDatagramRecvFrom::WouldBlock) => break,
                    Ok(
                        DriveDatagramRecvFrom::NotReady
                        | DriveDatagramRecvFrom::Blocked
                        | DriveDatagramRecvFrom::Budget,
                    ) => {
                        return;
                    }
                    Err(error) => {
                        if !self.emit_error_or_queue(events, drive, error.to_string()) {
                            return;
                        }
                        if !drive.can_start_operation() {
                            return;
                        }
                    }
                }
            }
        }
    }

    fn drive_udp_writes(&mut self, reactor: &mut R, drive: &mut DriveTurn<'_>) -> Option<HostPortEvent> {
        self.port_scratch.clear();
        self.port_scratch.extend(
            self.udp
                .iter()
                .filter_map(|udp| (!udp.pending.is_empty() && udp.socket.io().can_write()).then_some(udp.host)),
        );
        while let Some(host) = self.port_scratch.pop() {
            if !drive.can_start_operation() {
                break;
            }
            let Some(udp_index) = self.udp.iter().position(|udp| udp.host == host) else {
                continue;
            };
            let remove_error = {
                let port = &mut self.udp[udp_index];
                while let Some((peer, bytes)) = port.pending.front() {
                    let (socket, io) = port.socket.source_and_io_mut();
                    match drive.send_datagram_to_ready(io, socket, bytes.as_slice(), *peer) {
                        Ok(DriveDatagramSend::Sent) => {
                            port.pending.pop_front();
                        }
                        Ok(DriveDatagramSend::NotReady | DriveDatagramSend::WouldBlock | DriveDatagramSend::Budget) => {
                            break;
                        }
                        Err(error) => {
                            port.pending.pop_front();
                            return Some(HostPortEvent::error(error.to_string()));
                        }
                    }
                }
                let mut remove_error = None;
                if port.pending.is_empty() && port.socket.io().watches_write() {
                    match drive.try_apply_state_change(|| port.socket.reregister(reactor, ReactorInterest::Readable)) {
                        DriveApply::Applied(()) | DriveApply::Deferred => {}
                        DriveApply::Failed(error) => {
                            remove_error = Some(error.to_string());
                        }
                    }
                }
                remove_error
            };
            if let Some(message) = remove_error {
                let mut port = self.udp.swap_remove(udp_index);
                port.socket.deregister(reactor);
                return Some(HostPortEvent::error(message));
            }
        }
        None
    }

    fn remove_connection(&mut self, connection: HostConnectionId, reactor: &mut R) -> Option<()> {
        let mut connection = self.connections.remove(&connection)?;
        connection.stream.deregister(reactor);
        Some(())
    }

    fn emit_or_queue(
        &mut self,
        events: &mut Vec<HostPortEvent>,
        drive: &mut DriveTurn<'_>,
        event: HostPortEvent,
    ) -> bool {
        match drive.push_event(events, event) {
            Ok(()) => true,
            Err(event) => {
                self.pending_events.push_front(event);
                false
            }
        }
    }

    fn emit_error_or_queue(
        &mut self,
        events: &mut Vec<HostPortEvent>,
        drive: &mut DriveTurn<'_>,
        message: impl Into<String>,
    ) -> bool {
        self.emit_or_queue(events, drive, HostPortEvent::error(message))
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
    listener: RegisteredTcpListener<R>,
    next_connection: u64,
}

#[derive(Debug)]
struct HostUdpPort<R: ReactorBackend> {
    name: String,
    host: u16,
    guest: u16,
    socket: RegisteredUdpSocket<R>,
    pending: VecDeque<(SocketAddr, ByteBuf)>,
    error: Option<String>,
}

#[derive(Debug)]
struct IngressTcpConnection<R: ReactorBackend> {
    stream: RegisteredTcpStream<R>,
    pending: WriteQueue,
    write_state: HostWriteState,
    close_requested: bool,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostWriteState {
    Open,
    FinishRequested,
    Finished,
}

fn finish_write_if_ready<R: ReactorBackend>(connection: &mut IngressTcpConnection<R>) -> Option<String> {
    if connection.write_state != HostWriteState::FinishRequested || !connection.pending.is_empty() {
        return None;
    }
    match connection.stream.source().shutdown_write() {
        Ok(()) => {
            connection.write_state = HostWriteState::Finished;
            None
        }
        Err(error) => Some(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::io::{Read as _, Write as _};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use crate::buffers::{BufferPool, WriteQueue};
    use crate::connectors::tcp::ProductionTcpConnector;
    use crate::connectors::udp::ProductionUdpSocketFactory;
    use crate::drive::{DriveBudget, DriveReport, DriveTurn, DriveWait};
    use crate::network::{HostConnectionId, HostPortProtocol, HostPortSpec, NetworkLimits};
    use crate::reactor::{
        ReactorBackend, ReactorInterest, ReactorItemId, ReactorReady, RegisteredTcpListener, RegisteredTcpStream,
        RegisteredUdpSocket, RegisteringTcpListener, RegisteringTcpStream, RegisteringUdpSocket, default_backend,
    };
    use crate::runtime::{NetworkRuntime, RuntimeContext};
    use crate::test_support::unit::{UnusedTransport, runtime_context};

    use super::{HostPortEvent, HostPorts, HostTcpPort, HostWriteState, IngressTcpConnection};

    fn test_buffers() -> BufferPool {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        buffers
    }

    fn with_drive<T>(budget: &mut DriveBudget, f: impl FnOnce(&mut DriveTurn<'_>) -> T) -> (T, DriveReport) {
        let mut report = DriveReport::new();
        let result = {
            let mut drive = DriveTurn::new(budget, &mut report);
            f(&mut drive)
        };
        (result, report)
    }

    fn registered_test_stream(
        connection: HostConnectionId,
        stream: TestTcpStream,
        interest: ReactorInterest,
    ) -> RegisteredTcpStream<TestReactor> {
        let mut reactor = TestReactor::default();
        RegisteringTcpStream::new(
            &mut reactor,
            stream,
            ReactorItemId::IngressTcpConnection { connection },
            interest,
        )
        .expect("test TCP stream should register")
        .commit()
    }

    fn registered_test_stream_with_write_probe(
        connection: HostConnectionId,
        stream: TestTcpStream,
    ) -> RegisteredTcpStream<TestReactor> {
        let reactor = TestReactor::default();
        let mut stream = registered_test_stream(connection, stream, ReactorInterest::Readable);
        stream
            .reregister(&reactor, ReactorInterest::ReadWrite)
            .expect("test TCP stream should reregister writable");
        stream
    }

    fn registered_test_udp_socket(
        host: u16,
        socket: TestUdpSocket,
        interest: ReactorInterest,
    ) -> RegisteredUdpSocket<TestReactor> {
        let mut reactor = TestReactor::default();
        RegisteringUdpSocket::new(
            &mut reactor,
            socket,
            ReactorItemId::IngressUdpSocket { port: host },
            interest,
        )
        .expect("test UDP socket should register")
        .commit()
    }

    fn registered_test_udp_socket_with_write_probe(
        host: u16,
        socket: TestUdpSocket,
    ) -> RegisteredUdpSocket<TestReactor> {
        let reactor = TestReactor::default();
        let mut socket = registered_test_udp_socket(host, socket, ReactorInterest::Readable);
        socket
            .reregister(&reactor, ReactorInterest::ReadWrite)
            .expect("test UDP socket should reregister writable");
        socket
    }

    fn registered_test_listener(host: u16, listener: TestTcpListener) -> RegisteredTcpListener<TestReactor> {
        let mut reactor = TestReactor::default();
        let mut listener = RegisteringTcpListener::new(
            &mut reactor,
            listener,
            ReactorItemId::IngressTcpListener { port: host },
            ReactorInterest::Readable,
        )
        .expect("test TCP listener should register")
        .commit();
        listener.mark_reactor_ready(true, false);
        listener
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
        tcp_peer.set_read_timeout(Some(Duration::from_secs(1)))?;
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

        host_ports.finish_tcp_write(connection);
        let mut eof = [0_u8; 1];
        assert_eq!(tcp_peer.read(&mut eof)?, 0);
        tcp_peer.write_all(b"after-finish")?;
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
                assert_eq!(bytes.as_slice(), b"after-finish");
            }
            _ => return Err("expected tcp bytes after write side finished".into()),
        }

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
    async fn mio_host_ports_hold_tcp_reads_while_guest_send_is_blocked() -> Result<(), Box<dyn std::error::Error>> {
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let buffers = test_buffers();
        let mut host_ports = HostPorts::bind([host_port("tcp", HostPortProtocol::Tcp, 3000)], &buffers, &mut runtime)?;
        let tcp_host = host_ports
            .bound_tcp_host_port(3000)
            .ok_or("TCP host port was not bound")?;
        let mut tcp_peer = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;
        tcp_peer.set_nodelay(true)?;

        let events = wait_until_host_event(&mut runtime, &mut host_ports, |event| {
            matches!(event, HostPortEvent::TcpAccepted { .. })
        })?;
        let connection = match events.as_slice() {
            [HostPortEvent::TcpAccepted { connection, .. }] => *connection,
            _ => return Err("expected accepted connection".into()),
        };

        tcp_peer.write_all(b"backpressured")?;
        let mut readiness = Vec::new();
        runtime
            .reactor_mut()
            .ready_into(&mut readiness, Some(Duration::from_secs(1)))?;
        let mut events = Vec::new();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            host_ports.drive_ready(&readiness, &[connection], &mut events, drive, &mut runtime);
        });
        assert!(events.is_empty());
        assert!(report.wait().contains(DriveWait::GUEST_SEND_CAPACITY));

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            host_ports.drive_queued(&[], &mut events, drive, &mut runtime);
        });
        assert!(report.made_progress());
        match events.as_slice() {
            [
                HostPortEvent::TcpBytes {
                    connection: event_connection,
                    bytes,
                },
            ] => {
                assert_eq!(*event_connection, connection);
                assert_eq!(bytes.as_slice(), b"backpressured");
            }
            _ => return Err("expected latched tcp bytes after guest send unblocked".into()),
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mio_host_ports_tcp_read_is_bounded_by_drive_byte_budget() -> Result<(), Box<dyn std::error::Error>> {
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let buffers = test_buffers();
        let mut host_ports = HostPorts::bind([host_port("tcp", HostPortProtocol::Tcp, 3000)], &buffers, &mut runtime)?;
        let tcp_host = host_ports
            .bound_tcp_host_port(3000)
            .ok_or("TCP host port was not bound")?;
        let mut tcp_peer = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;
        tcp_peer.set_nodelay(true)?;

        let accepted = wait_until_host_event(&mut runtime, &mut host_ports, |event| {
            matches!(event, HostPortEvent::TcpAccepted { .. })
        })?;
        let connection = match accepted.as_slice() {
            [HostPortEvent::TcpAccepted { connection, .. }] => *connection,
            _ => return Err("expected accepted connection".into()),
        };
        tcp_peer.write_all(b"0123456789abcdef")?;

        let mut readiness = Vec::new();
        runtime
            .reactor_mut()
            .ready_into(&mut readiness, Some(Duration::from_secs(1)))?;
        let mut events = Vec::new();
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 4,
            ..NetworkLimits::default()
        });
        let (_result, _report) = with_drive(&mut budget, |drive| {
            host_ports.drive_ready(&readiness, &[], &mut events, drive, &mut runtime);
        });

        match events.as_slice() {
            [
                HostPortEvent::TcpBytes {
                    connection: event_connection,
                    bytes,
                },
            ] => {
                assert_eq!(*event_connection, connection);
                assert_eq!(bytes.len(), 4);
            }
            _ => return Err("expected one budget-bounded host-port TCP bytes event".into()),
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mio_host_ports_close_waits_for_pending_writes() -> Result<(), Box<dyn std::error::Error>> {
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let buffers = test_buffers();
        let mut host_ports = HostPorts::bind([host_port("tcp", HostPortProtocol::Tcp, 3000)], &buffers, &mut runtime)?;
        let tcp_host = host_ports
            .bound_tcp_host_port(3000)
            .ok_or("TCP host port was not bound")?;
        let mut tcp_peer = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;
        tcp_peer.set_nodelay(true)?;
        tcp_peer.set_read_timeout(Some(Duration::from_secs(1)))?;
        let events = wait_until_host_event(&mut runtime, &mut host_ports, |event| {
            matches!(event, HostPortEvent::TcpAccepted { .. })
        })?;
        let connection = match events.as_slice() {
            [HostPortEvent::TcpAccepted { connection, .. }] => *connection,
            _ => return Err("expected accepted connection".into()),
        };

        host_ports.write_tcp(connection, io_buffer(&buffers, b"before-close"), &runtime);
        host_ports.close_tcp(connection, &mut runtime);
        assert!(drive_queued(&mut runtime, &mut host_ports)?);
        let mut observed = [0_u8; 12];
        tcp_peer.read_exact(&mut observed)?;
        assert_eq!(&observed, b"before-close");
        let mut eof = [0_u8; 1];
        assert_eq!(tcp_peer.read(&mut eof)?, 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn host_port_tcp_write_scheduler_skips_idle_connections() -> Result<(), Box<dyn std::error::Error>> {
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let buffers = test_buffers();
        let mut host_ports = HostPorts::bind([host_port("tcp", HostPortProtocol::Tcp, 3000)], &buffers, &mut runtime)?;
        let tcp_host = host_ports
            .bound_tcp_host_port(3000)
            .ok_or("TCP host port was not bound")?;
        let _first_peer = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;
        let mut second_peer = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;
        second_peer.set_read_timeout(Some(Duration::from_secs(1)))?;

        let events = wait_until_host_event_count(&mut runtime, &mut host_ports, 2, |event| {
            matches!(event, HostPortEvent::TcpAccepted { .. })
        })?;
        let second_connection = match events.as_slice() {
            [
                HostPortEvent::TcpAccepted { .. },
                HostPortEvent::TcpAccepted { connection, .. },
            ] => *connection,
            _ => return Err("expected two accepted TCP connections".into()),
        };

        host_ports.write_tcp(second_connection, io_buffer(&buffers, b"second"), &runtime);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            let _event = host_ports.drive_tcp_writes(runtime.reactor_mut(), drive);
        });

        assert!(report.made_progress());
        let mut observed = [0_u8; 6];
        second_peer.read_exact(&mut observed)?;
        assert_eq!(&observed, b"second");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn host_port_udp_send_scheduler_skips_idle_ports() -> Result<(), Box<dyn std::error::Error>> {
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let buffers = test_buffers();
        let mut host_ports = HostPorts::bind(
            [
                host_port("udp-a", HostPortProtocol::Udp, 5353),
                host_port("udp-b", HostPortProtocol::Udp, 5354),
            ],
            &buffers,
            &mut runtime,
        )?;
        let peer = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        peer.set_read_timeout(Some(Duration::from_secs(1)))?;
        let peer_addr = peer.local_addr()?;

        host_ports.send_udp(5354, peer_addr, io_buffer(&buffers, b"second"), &runtime);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            let _event = host_ports.drive_udp_writes(runtime.reactor_mut(), drive);
        });

        assert!(report.made_progress());
        let mut observed = [0_u8; 6];
        let (len, _from) = peer.recv_from(&mut observed)?;
        assert_eq!(&observed[..len], b"second");
        Ok(())
    }

    #[test]
    fn host_port_tcp_write_waits_for_writable_after_would_block() {
        let buffers = test_buffers();
        let connection = HostConnectionId(1);
        let mut host_ports = HostPorts::<TestReactor> {
            tcp: Vec::new(),
            udp: Vec::new(),
            connections: agentdp_ds::fixed_table::FixedTable::with_capacity(1),
            connection_scratch: Vec::new(),
            port_scratch: Vec::new(),
            pending_events: VecDeque::new(),
            buffers: buffers.clone(),
            max_tcp_connections: 1,
        };
        assert!(
            host_ports
                .connections
                .insert(
                    connection,
                    IngressTcpConnection {
                        stream: registered_test_stream_with_write_probe(
                            connection,
                            TestTcpStream {
                                write_would_block: true,
                                written: Vec::new(),
                            },
                        ),
                        pending: {
                            let mut queue = WriteQueue::new();
                            queue.push(io_buffer(&buffers, b"queued"));
                            queue
                        },
                        write_state: HostWriteState::Open,
                        close_requested: false,
                        error: None,
                    },
                )
                .is_ok()
        );
        let mut reactor = TestReactor::default();

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            let _event = host_ports.drive_tcp_writes(&mut reactor, drive);
        });
        assert!(!report.made_progress());
        assert!(report.wait().contains(DriveWait::REACTOR_WRITE));
        assert!(!host_ports.connections.get(&connection).unwrap().stream.io().can_write());
        assert_eq!(
            host_ports.connections.get(&connection).unwrap().pending.pending_bytes(),
            6
        );

        host_ports
            .connections
            .get_mut(&connection)
            .unwrap()
            .stream
            .source_mut()
            .write_would_block = false;
        let runtime = RuntimeContext::new(
            UnusedTransport,
            TestReactor::default(),
            crate::clock::SystemClock,
            ProductionTcpConnector,
            ProductionUdpSocketFactory,
        );
        host_ports.write_tcp(connection, io_buffer(&buffers, b"-more"), &runtime);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            let mut runtime = RuntimeContext::new(
                UnusedTransport,
                TestReactor::default(),
                crate::clock::SystemClock,
                ProductionTcpConnector,
                ProductionUdpSocketFactory,
            );
            host_ports.drive_queued(&[], &mut Vec::new(), drive, &mut runtime);
        });
        assert!(!report.made_progress());
        assert_eq!(
            host_ports.connections.get(&connection).unwrap().pending.pending_bytes(),
            11
        );
        host_ports.latch_ready(&[ReactorReady::Io {
            item: ReactorItemId::IngressTcpConnection { connection },
            readable: false,
            writable: true,
        }]);

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            let _event = host_ports.drive_tcp_writes(&mut reactor, drive);
        });
        assert!(report.made_progress());
        let connection_state = host_ports.connections.get(&connection).unwrap();
        assert_eq!(connection_state.pending.pending_bytes(), 0);
        assert_eq!(connection_state.stream.source().written, b"queued-more");
    }

    #[test]
    fn host_port_udp_write_waits_for_writable_after_would_block() {
        let buffers = test_buffers();
        let host = 5353;
        let peer = SocketAddr::from(([127, 0, 0, 1], 40_000));
        let mut host_ports = HostPorts::<TestReactor> {
            tcp: Vec::new(),
            udp: vec![super::HostUdpPort {
                name: "udp".to_owned(),
                host,
                guest: host,
                socket: registered_test_udp_socket_with_write_probe(
                    host,
                    TestUdpSocket {
                        send_would_block: Cell::new(true),
                        sent: RefCell::new(Vec::new()),
                    },
                ),
                pending: {
                    let mut pending = VecDeque::new();
                    pending.push_back((peer, io_buffer(&buffers, b"queued")));
                    pending
                },
                error: None,
            }],
            connections: agentdp_ds::fixed_table::FixedTable::with_capacity(0),
            connection_scratch: Vec::new(),
            port_scratch: Vec::new(),
            pending_events: VecDeque::new(),
            buffers,
            max_tcp_connections: 0,
        };
        let mut reactor = TestReactor::default();

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            let _event = host_ports.drive_udp_writes(&mut reactor, drive);
        });
        assert!(!report.made_progress());
        assert!(report.wait().contains(DriveWait::REACTOR_WRITE));
        assert!(!host_ports.udp[0].socket.io().can_write());
        assert_eq!(host_ports.udp[0].pending.len(), 1);

        host_ports.udp[0].socket.source().send_would_block.set(false);
        let runtime = RuntimeContext::new(
            UnusedTransport,
            TestReactor::default(),
            crate::clock::SystemClock,
            ProductionTcpConnector,
            ProductionUdpSocketFactory,
        );
        host_ports.send_udp(host, peer, io_buffer(&host_ports.buffers, b"-more"), &runtime);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            let mut runtime = RuntimeContext::new(
                UnusedTransport,
                TestReactor::default(),
                crate::clock::SystemClock,
                ProductionTcpConnector,
                ProductionUdpSocketFactory,
            );
            host_ports.drive_queued(&[], &mut Vec::new(), drive, &mut runtime);
        });
        assert!(!report.made_progress());
        assert_eq!(host_ports.udp[0].pending.len(), 2);

        host_ports.latch_ready(&[ReactorReady::Io {
            item: ReactorItemId::IngressUdpSocket { port: host },
            readable: false,
            writable: true,
        }]);

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            let _event = host_ports.drive_udp_writes(&mut reactor, drive);
        });
        assert!(report.made_progress());
        assert!(host_ports.udp[0].pending.is_empty());
        assert_eq!(
            host_ports.udp[0].socket.source().sent.borrow().as_slice(),
            &[(peer, b"queued".to_vec()), (peer, b"-more".to_vec())]
        );
    }

    #[test]
    fn host_port_tcp_drops_connection_when_write_interest_demotion_fails() {
        let buffers = test_buffers();
        let connection = HostConnectionId(1);
        let mut host_ports = HostPorts::<TestReactor> {
            tcp: Vec::new(),
            udp: Vec::new(),
            connections: agentdp_ds::fixed_table::FixedTable::with_capacity(1),
            connection_scratch: Vec::new(),
            port_scratch: Vec::new(),
            pending_events: VecDeque::new(),
            buffers: buffers.clone(),
            max_tcp_connections: 1,
        };
        assert!(
            host_ports
                .connections
                .insert(
                    connection,
                    IngressTcpConnection {
                        stream: registered_test_stream_with_write_probe(
                            connection,
                            TestTcpStream {
                                write_would_block: false,
                                written: Vec::new(),
                            },
                        ),
                        pending: {
                            let mut queue = WriteQueue::new();
                            queue.push(io_buffer(&buffers, b"queued"));
                            queue
                        },
                        write_state: HostWriteState::Open,
                        close_requested: false,
                        error: None,
                    },
                )
                .is_ok()
        );
        let mut reactor = TestReactor::default();
        reactor.fail_readable_reregister.set(true);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());

        let (event, report) = with_drive(&mut budget, |drive| host_ports.drive_tcp_writes(&mut reactor, drive));

        assert!(matches!(event, Some(HostPortEvent::Error { .. })));
        assert!(report.made_progress());
        assert!(host_ports.connections.get(&connection).is_none());
        assert_eq!(reactor.deregistered_tcp_streams.borrow().as_slice(), &[connection]);
    }

    #[test]
    fn host_port_udp_drops_socket_when_write_interest_demotion_fails() {
        let buffers = test_buffers();
        let host = 5353;
        let peer = SocketAddr::from(([127, 0, 0, 1], 40_000));
        let mut host_ports = HostPorts::<TestReactor> {
            tcp: Vec::new(),
            udp: vec![super::HostUdpPort {
                name: "udp".to_owned(),
                host,
                guest: host,
                socket: registered_test_udp_socket_with_write_probe(
                    host,
                    TestUdpSocket {
                        send_would_block: Cell::new(false),
                        sent: RefCell::new(Vec::new()),
                    },
                ),
                pending: {
                    let mut pending = VecDeque::new();
                    pending.push_back((peer, io_buffer(&buffers, b"queued")));
                    pending
                },
                error: None,
            }],
            connections: agentdp_ds::fixed_table::FixedTable::with_capacity(0),
            connection_scratch: Vec::new(),
            port_scratch: Vec::new(),
            pending_events: VecDeque::new(),
            buffers,
            max_tcp_connections: 0,
        };
        let mut reactor = TestReactor::default();
        reactor.fail_readable_reregister.set(true);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());

        let (event, report) = with_drive(&mut budget, |drive| host_ports.drive_udp_writes(&mut reactor, drive));

        assert!(matches!(event, Some(HostPortEvent::Error { .. })));
        assert!(report.made_progress());
        assert!(host_ports.udp.is_empty());
        assert_eq!(reactor.deregistered_udp_ports.borrow().as_slice(), &[host]);
    }

    #[test]
    fn host_port_accept_capacity_reports_connection_slot_wait() {
        let buffers = test_buffers();
        let connection = HostConnectionId(1);
        let mut host_ports = HostPorts::<TestReactor> {
            tcp: vec![HostTcpPort {
                name: "tcp".to_owned(),
                host: 3000,
                guest: 3000,
                listener: registered_test_listener(
                    3000,
                    TestTcpListener {
                        accept_once: Cell::new(false),
                        accept_error: std::io::ErrorKind::WouldBlock,
                    },
                ),
                next_connection: 2,
            }],
            udp: Vec::new(),
            connections: agentdp_ds::fixed_table::FixedTable::with_capacity(1),
            connection_scratch: Vec::new(),
            port_scratch: Vec::new(),
            pending_events: VecDeque::new(),
            buffers,
            max_tcp_connections: 1,
        };
        assert!(
            host_ports
                .connections
                .insert(
                    connection,
                    IngressTcpConnection {
                        stream: registered_test_stream(
                            connection,
                            TestTcpStream {
                                write_would_block: false,
                                written: Vec::new(),
                            },
                            ReactorInterest::Readable,
                        ),
                        pending: WriteQueue::new(),
                        write_state: HostWriteState::Open,
                        close_requested: false,
                        error: None,
                    },
                )
                .is_ok()
        );
        let mut events = Vec::new();
        let mut reactor = TestReactor::default();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());

        let (_result, report) = with_drive(&mut budget, |drive| {
            host_ports.drive_accepts(&mut events, drive, &mut reactor);
        });

        assert!(events.is_empty());
        assert!(report.wait().contains(DriveWait::CONNECTION_SLOT));
        assert!(host_ports.tcp[0].listener.io().can_read());
    }

    #[test]
    fn host_port_accept_would_block_reports_reactor_read_wait() {
        let buffers = test_buffers();
        let mut host_ports = HostPorts::<TestReactor> {
            tcp: vec![HostTcpPort {
                name: "tcp".to_owned(),
                host: 3000,
                guest: 3000,
                listener: registered_test_listener(
                    3000,
                    TestTcpListener {
                        accept_once: Cell::new(false),
                        accept_error: std::io::ErrorKind::WouldBlock,
                    },
                ),
                next_connection: 1,
            }],
            udp: Vec::new(),
            connections: agentdp_ds::fixed_table::FixedTable::with_capacity(1),
            connection_scratch: Vec::new(),
            port_scratch: Vec::new(),
            pending_events: VecDeque::new(),
            buffers,
            max_tcp_connections: 1,
        };
        let mut events = Vec::new();
        let mut reactor = TestReactor::default();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());

        let (_result, report) = with_drive(&mut budget, |drive| {
            host_ports.drive_accepts(&mut events, drive, &mut reactor);
        });

        assert!(events.is_empty());
        assert!(report.wait().contains(DriveWait::REACTOR_READ));
        assert!(!host_ports.tcp[0].listener.io().can_read());
    }

    #[test]
    fn host_port_accept_error_drops_listener_without_spinning() {
        let buffers = test_buffers();
        let mut host_ports = HostPorts::<TestReactor> {
            tcp: vec![HostTcpPort {
                name: "tcp".to_owned(),
                host: 3000,
                guest: 3000,
                listener: registered_test_listener(
                    3000,
                    TestTcpListener {
                        accept_once: Cell::new(false),
                        accept_error: std::io::ErrorKind::ConnectionAborted,
                    },
                ),
                next_connection: 1,
            }],
            udp: Vec::new(),
            connections: agentdp_ds::fixed_table::FixedTable::with_capacity(1),
            connection_scratch: Vec::new(),
            port_scratch: Vec::new(),
            pending_events: VecDeque::new(),
            buffers,
            max_tcp_connections: 1,
        };
        let mut events = Vec::new();
        let mut reactor = TestReactor::default();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());

        let (_result, _report) = with_drive(&mut budget, |drive| {
            host_ports.drive_accepts(&mut events, drive, &mut reactor);
        });

        assert!(matches!(events.as_slice(), [HostPortEvent::Error { .. }]));
        assert!(host_ports.tcp.is_empty());

        events.clear();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            host_ports.drive_accepts(&mut events, drive, &mut reactor);
        });
        assert!(events.is_empty());
        assert!(!report.made_progress());
    }

    #[test]
    fn host_port_accept_full_table_does_not_register_stream() {
        let buffers = test_buffers();
        let mut host_ports = HostPorts::<TestReactor> {
            tcp: vec![HostTcpPort {
                name: "tcp".to_owned(),
                host: 3000,
                guest: 3000,
                listener: registered_test_listener(
                    3000,
                    TestTcpListener {
                        accept_once: Cell::new(true),
                        accept_error: std::io::ErrorKind::WouldBlock,
                    },
                ),
                next_connection: 1,
            }],
            udp: Vec::new(),
            connections: agentdp_ds::fixed_table::FixedTable::with_capacity(0),
            connection_scratch: Vec::new(),
            port_scratch: Vec::new(),
            pending_events: VecDeque::new(),
            buffers,
            max_tcp_connections: 1,
        };
        let mut events = Vec::new();
        let mut reactor = TestReactor::default();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());

        let (_result, _report) = with_drive(&mut budget, |drive| {
            host_ports.drive_accepts(&mut events, drive, &mut reactor);
        });

        assert!(matches!(events.as_slice(), [HostPortEvent::Error { .. }]));
        assert_eq!(
            reactor.deregistered_tcp_streams.borrow().as_slice(),
            &[] as &[HostConnectionId]
        );
        assert!(host_ports.connections.get(&HostConnectionId(1)).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tcp_accept_capacity_does_not_block_existing_connection_reads() -> Result<(), Box<dyn std::error::Error>> {
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let buffers = BufferPool::new(NetworkLimits {
            ingress_tcp_connection_limit: 1,
            ..NetworkLimits::default()
        });
        buffers.prewarm_instance_network();
        let mut host_ports = HostPorts::bind([host_port("tcp", HostPortProtocol::Tcp, 3000)], &buffers, &mut runtime)?;
        let tcp_host = host_ports
            .bound_tcp_host_port(3000)
            .ok_or("TCP host port was not bound")?;
        let mut first = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;
        first.set_nodelay(true)?;
        let accepted = wait_until_host_event(&mut runtime, &mut host_ports, |event| {
            matches!(event, HostPortEvent::TcpAccepted { .. })
        })?;
        let connection = match accepted.as_slice() {
            [HostPortEvent::TcpAccepted { connection, .. }] => *connection,
            _ => return Err("expected first accepted connection".into()),
        };

        let _second = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, tcp_host))?;
        first.write_all(b"still-readable")?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut readiness = Vec::new();
        let mut events = Vec::new();
        while tokio::time::Instant::now() < deadline {
            runtime
                .reactor_mut()
                .ready_into(&mut readiness, Some(Duration::from_millis(20)))?;
            let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
            let (_result, _report) = with_drive(&mut budget, |drive| {
                host_ports.drive_ready(&readiness, &[], &mut events, drive, &mut runtime);
            });
            if events.iter().any(|event| {
                matches!(
                    event,
                    HostPortEvent::TcpBytes {
                        connection: event_connection,
                        bytes,
                    } if *event_connection == connection && bytes.as_slice() == b"still-readable"
                )
            }) {
                return Ok(());
            }
            events.clear();
        }

        Err("timed out waiting for existing connection bytes while accept capacity was full".into())
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
        assert!(host_ports.bound_tcp_host_port(3000).is_none());

        let mut events = Vec::new();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            host_ports.drive_queued(&[], &mut events, drive, &mut runtime);
        });
        assert!(events.is_empty());
        assert!(!report.made_progress());
        Ok(())
    }

    #[test]
    fn host_port_push_event_returns_event_when_budget_is_exhausted() {
        let buffers = test_buffers();
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_event_budget: 0,
            ..NetworkLimits::default()
        });
        let mut report = crate::drive::DriveReport::new();
        let mut events = Vec::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        let event = drive.push_event(
            &mut events,
            HostPortEvent::TcpBytes {
                connection: HostConnectionId(7),
                bytes: io_buffer(&buffers, b"owned"),
            },
        );

        assert!(matches!(event, Err(HostPortEvent::TcpBytes { connection, .. }) if connection == HostConnectionId(7)));
        assert!(!report.made_progress());
        assert!(report.budget_exhausted());
        assert!(events.is_empty());
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
            let (_result, _report) = with_drive(&mut budget, |drive| {
                host_ports.drive_ready(&readiness, &[], &mut output, drive, runtime);
            });
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
        let (_result, report) = with_drive(&mut budget, |drive| {
            host_ports.drive_queued(&[], &mut events, drive, runtime);
        });
        if events.is_empty() {
            Ok(report.made_progress())
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

    #[derive(Default)]
    struct TestReactor {
        fail_readable_reregister: Cell<bool>,
        deregistered_tcp_streams: RefCell<Vec<HostConnectionId>>,
        deregistered_udp_ports: RefCell<Vec<u16>>,
    }

    #[derive(Clone)]
    struct TestWake;

    impl crate::reactor::ReactorWake for TestWake {
        fn wake(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct TestTcpStream {
        write_would_block: bool,
        written: Vec<u8>,
    }

    impl std::io::Read for TestTcpStream {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }
    }

    impl std::io::Write for TestTcpStream {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.write_would_block {
                return Err(std::io::ErrorKind::WouldBlock.into());
            }
            self.written.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl crate::reactor::ReactorTcpStream for TestTcpStream {
        fn connect(_addr: std::net::SocketAddr) -> std::io::Result<Self> {
            Ok(Self {
                write_would_block: false,
                written: Vec::new(),
            })
        }

        fn set_nodelay(&self, _nodelay: bool) -> std::io::Result<()> {
            Ok(())
        }

        fn take_error(&self) -> std::io::Result<Option<std::io::Error>> {
            Ok(None)
        }

        fn shutdown_write(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct TestTcpListener {
        accept_once: Cell<bool>,
        accept_error: std::io::ErrorKind,
    }

    impl crate::reactor::ReactorTcpListener for TestTcpListener {
        type Stream = TestTcpStream;

        fn bind(_addr: std::net::SocketAddr) -> std::io::Result<Self> {
            Ok(Self {
                accept_once: Cell::new(false),
                accept_error: std::io::ErrorKind::WouldBlock,
            })
        }

        fn accept(&self) -> std::io::Result<(Self::Stream, std::net::SocketAddr)> {
            if self.accept_once.replace(false) {
                return Ok((
                    TestTcpStream {
                        write_would_block: false,
                        written: Vec::new(),
                    },
                    SocketAddr::from(([127, 0, 0, 1], 40000)),
                ));
            }
            Err(self.accept_error.into())
        }

        fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
            Ok(([127, 0, 0, 1], 0).into())
        }
    }

    struct TestUdpSocket {
        send_would_block: Cell<bool>,
        sent: RefCell<Vec<(SocketAddr, Vec<u8>)>>,
    }

    impl crate::reactor::ReactorUdpSocket for TestUdpSocket {
        fn bind(_addr: std::net::SocketAddr) -> std::io::Result<Self> {
            Ok(Self {
                send_would_block: Cell::new(false),
                sent: RefCell::new(Vec::new()),
            })
        }

        fn from_std(_socket: std::net::UdpSocket) -> Self {
            Self {
                send_would_block: Cell::new(false),
                sent: RefCell::new(Vec::new()),
            }
        }

        fn send(&self, _bytes: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }

        fn recv(&self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }

        fn send_to(&self, bytes: &[u8], target: std::net::SocketAddr) -> std::io::Result<usize> {
            if self.send_would_block.get() {
                return Err(std::io::ErrorKind::WouldBlock.into());
            }
            self.sent.borrow_mut().push((target, bytes.to_vec()));
            Ok(bytes.len())
        }

        fn recv_from(&self, _buffer: &mut [u8]) -> std::io::Result<(usize, std::net::SocketAddr)> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }

        fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
            Ok(([127, 0, 0, 1], 0).into())
        }
    }

    impl ReactorBackend for TestReactor {
        type Wake = TestWake;
        type TcpListener = TestTcpListener;
        type TcpStream = TestTcpStream;
        type UdpSocket = TestUdpSocket;

        fn wake_handle(&self) -> Self::Wake {
            TestWake
        }

        fn register_tcp_listener(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpListener,
            _item: ReactorItemId,
            _interest: crate::reactor::ReactorInterest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn register_tcp_stream(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpStream,
            _item: ReactorItemId,
            _interest: crate::reactor::ReactorInterest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn register_udp_socket(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
            _interest: crate::reactor::ReactorInterest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn reregister_tcp_stream(
            &self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpStream,
            _item: ReactorItemId,
            interest: crate::reactor::ReactorInterest,
        ) -> std::io::Result<()> {
            if interest == ReactorInterest::Readable && self.fail_readable_reregister.get() {
                return Err(std::io::Error::other("failed to demote TCP write interest"));
            }
            Ok(())
        }

        fn reregister_udp_socket(
            &self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
            interest: crate::reactor::ReactorInterest,
        ) -> std::io::Result<()> {
            if interest == ReactorInterest::Readable && self.fail_readable_reregister.get() {
                return Err(std::io::Error::other("failed to demote UDP write interest"));
            }
            Ok(())
        }

        fn deregister_tcp_listener(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpListener,
            _item: ReactorItemId,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn deregister_tcp_stream(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpStream,
            item: ReactorItemId,
        ) -> std::io::Result<()> {
            if let ReactorItemId::IngressTcpConnection { connection } = item {
                self.deregistered_tcp_streams.borrow_mut().push(connection);
            }
            Ok(())
        }

        fn deregister_udp_socket(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            item: ReactorItemId,
        ) -> std::io::Result<()> {
            if let ReactorItemId::IngressUdpSocket { port } = item {
                self.deregistered_udp_ports.borrow_mut().push(port);
            }
            Ok(())
        }

        fn register_guest_source(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: crate::guest::GuestIoSource<'_>,
            _item: ReactorItemId,
        ) -> Result<(), crate::guest::TransportError> {
            Ok(())
        }

        fn reregister_guest_source(
            &self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: crate::guest::GuestIoSource<'_>,
            _item: ReactorItemId,
            _writable: bool,
        ) -> Result<(), crate::guest::TransportError> {
            Ok(())
        }

        fn deregister_guest_source(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: crate::guest::GuestIoSource<'_>,
            _item: ReactorItemId,
        ) -> Result<(), crate::guest::TransportError> {
            Ok(())
        }

        fn ready_into(&mut self, _output: &mut Vec<ReactorReady>, _timeout: Option<Duration>) -> std::io::Result<()> {
            Ok(())
        }
    }
}
