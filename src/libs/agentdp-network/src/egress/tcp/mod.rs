use std::collections::VecDeque;
#[cfg(any(test, feature = "simulation"))]
use std::fmt::Write as _;
use std::io;
use std::net::SocketAddr;

mod plain;
mod tls;
mod tls_upstream;

use plain::{PlainRoute, PlainTcpProxy};
use tls::{TlsProxyPoll, TlsTcpProxy};

use crate::buffers::WriteQueue;
use crate::buffers::{BufferPool, ByteBuf};
use crate::clock::NetworkClock;
use crate::drive::{DriveRunnable, DriveSmoltcpTcpRecv, DriveTurn};
use crate::gateway::Gateway;
use crate::network::NetworkLimits;
use crate::network::{TcpEgressRoute, TcpProxyId};
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorReady};
use crate::readiness::IoSlotState;
use crate::runtime::NetworkRuntime;
use smoltcp::iface::{SocketHandle, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::wire::{IpAddress, IpListenEndpoint};

#[derive(Debug)]
pub(crate) enum TcpProxyEvent {
    Closed {
        proxy: TcpProxyId,
    },
    Error {
        proxy: TcpProxyId,
        context: Option<Box<TcpProxyErrorContext>>,
        message: String,
    },
    DnsResolved {
        host: String,
        addresses: Vec<std::net::IpAddr>,
        ttl: std::time::Duration,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct TcpProxyErrorContext {
    pub(crate) destination: String,
    pub(crate) upstream: String,
    pub(crate) authority: Option<String>,
    pub(crate) route: &'static str,
    pub(crate) phase: &'static str,
}

pub(crate) struct TcpProxies<R: ReactorBackend> {
    proxies: TcpProxySlots<R>,
    buffers: BufferPool,
    activation_scratch: Vec<(usize, SocketAddr)>,
    poll_scratch: Vec<TcpProxyId>,
}

impl<R> TcpProxies<R>
where
    R: ReactorBackend,
{
    pub(crate) fn new(limits: &NetworkLimits, buffers: &BufferPool) -> Self {
        Self {
            proxies: TcpProxySlots::new(limits.tcp_proxy_limit, limits.tcp_socket_buffer_capacity),
            buffers: buffers.clone(),
            activation_scratch: Vec::with_capacity(limits.tcp_proxy_limit),
            poll_scratch: Vec::with_capacity(limits.tcp_proxy_limit),
        }
    }

    pub(crate) fn has_connection(&self, src: SocketAddr, dst: SocketAddr) -> bool {
        self.proxies.has_connection(src, dst)
    }

    pub(crate) fn listen(&mut self, src: SocketAddr, dst: SocketAddr, sockets: &mut SocketSet<'static>) -> bool {
        self.proxies.listen(src, dst, sockets)
    }

    pub(crate) fn drive_gateway<C: NetworkClock>(
        &mut self,
        gateway: &mut Gateway<C>,
        readiness: &[ReactorReady],
        events: &mut Vec<TcpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        self.activate_guest_connections(gateway, drive, runtime);
        self.latch_reactor_readiness(readiness);
        let sockets = gateway.tcp_sockets_mut();
        self.collect_runnable_proxies(sockets);
        while let Some(event) = self.proxies.pending_events.pop_front() {
            let terminal_proxy = event.terminal_proxy();
            if !self.proxies.emit_or_queue(events, drive, event) {
                self.poll_scratch.clear();
                return;
            }
            if let Some(proxy_id) = terminal_proxy {
                self.proxies.remove_proxy(proxy_id, runtime.reactor_mut());
                self.proxies.close_guest(proxy_id, sockets, drive);
            }
            if !drive.can_start_operation() {
                self.poll_scratch.clear();
                return;
            }
        }
        let mut index = 0;
        while index < self.poll_scratch.len() {
            if !drive.can_start_operation() {
                break;
            }
            let proxy_id = self.poll_scratch[index];
            index += 1;
            self.proxies
                .drive_proxy_entry(proxy_id, sockets, events, drive, &self.buffers, runtime);
        }
        self.poll_scratch.clear();
    }

    pub(crate) fn drive_queued<C: NetworkClock>(
        &mut self,
        gateway: &mut Gateway<C>,
        events: &mut Vec<TcpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        self.drive_gateway(gateway, &[], events, drive, runtime);
    }

    pub(crate) fn drive_ready<C: NetworkClock>(
        &mut self,
        gateway: &mut Gateway<C>,
        readiness: &[ReactorReady],
        events: &mut Vec<TcpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        self.drive_gateway(gateway, readiness, events, drive, runtime);
    }

    fn activate_guest_connections<C: NetworkClock>(
        &mut self,
        gateway: &mut Gateway<C>,
        drive: &mut DriveTurn<'_>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        let mut established = std::mem::take(&mut self.activation_scratch);
        established.clear();
        self.proxies
            .append_established_destinations(gateway.tcp_sockets(), &mut established);
        for &(slot_index, requested_dst) in &established {
            if drive
                .apply_state_change(|| {
                    let proxy_id = TcpProxyId(self.proxies.next_proxy);
                    self.proxies.next_proxy = self.proxies.next_proxy.saturating_add(1);
                    let (upstream_dst, route) = gateway.tcp_egress_route(requested_dst);
                    let route_name = route_name(&route);
                    let proxy =
                        TcpProxy::connecting(proxy_id, requested_dst, upstream_dst, route, &self.buffers, runtime)
                            .unwrap_or_else(|error| {
                                TcpProxy::failed(
                                    proxy_id,
                                    TcpProxyErrorContext::new(requested_dst, upstream_dst, None, route_name, "connect"),
                                    error.to_string(),
                                )
                            });
                    self.proxies.activate(slot_index, proxy_id, proxy);
                })
                .is_none()
            {
                break;
            }
        }
        established.clear();
        self.activation_scratch = established;
    }

    fn latch_reactor_readiness(&mut self, readiness: &[ReactorReady]) {
        for ready in readiness {
            let ReactorReady::Io {
                item,
                readable,
                writable,
            } = *ready
            else {
                continue;
            };
            let ReactorItemId::TcpProxy { proxy: proxy_id } = item else {
                continue;
            };
            self.proxies.mark_reactor_ready(proxy_id, readable, writable);
        }
    }

    fn collect_runnable_proxies(&mut self, sockets: &SocketSet<'static>) {
        self.poll_scratch.clear();
        self.proxies.append_runnable_proxies(sockets, &mut self.poll_scratch);
    }

    #[cfg(any(test, feature = "simulation"))]
    pub(crate) fn debug_snapshot(&self, sockets: &SocketSet<'static>) -> String {
        self.proxies.debug_snapshot(sockets)
    }

    #[cfg(any(test, feature = "simulation"))]
    pub(crate) fn active_proxy_slots(&self) -> usize {
        self.proxies.active_proxy_slots()
    }

    pub(crate) fn shutdown(&mut self, runtime: &mut impl NetworkRuntime<Reactor = R>) {
        self.proxies.shutdown(runtime.reactor_mut());
    }
}

struct TcpProxySlots<R: ReactorBackend> {
    slots: Vec<Option<TcpProxySlot<R>>>,
    idle_sockets: Vec<tcp::Socket<'static>>,
    pending_events: VecDeque<TcpProxyEvent>,
    next_proxy: u64,
}

struct TcpProxySlot<R: ReactorBackend> {
    handle: SocketHandle,
    entry: TcpProxyEntry<R>,
}

struct TcpProxyEntry<R: ReactorBackend> {
    requested: Option<(SocketAddr, SocketAddr)>,
    active: Option<TcpProxyId>,
    upstream_read_masked: bool,
    pending_writes: WriteQueue,
    guest_write_closed: bool,
    proxy_closed: bool,
    proxy: Option<TcpProxy<R>>,
    #[cfg(any(test, feature = "simulation"))]
    last_proxy_snapshot: Option<String>,
}

impl<R> TcpProxySlots<R>
where
    R: ReactorBackend,
{
    fn new(max_connections: usize, socket_buffer_bytes: usize) -> Self {
        let mut slots = Vec::with_capacity(max_connections);
        slots.resize_with(max_connections, || None);
        let mut idle_sockets = Vec::with_capacity(max_connections);
        for _ in 0..max_connections {
            idle_sockets.push(tcp_socket_buffered(socket_buffer_bytes));
        }
        Self {
            slots,
            idle_sockets,
            pending_events: VecDeque::new(),
            next_proxy: 0,
        }
    }

    fn has_connection(&self, src: SocketAddr, dst: SocketAddr) -> bool {
        self.slots
            .iter()
            .filter_map(Option::as_ref)
            .any(|slot| slot.entry.requested == Some((src, dst)))
    }

    fn listen(&mut self, src: SocketAddr, dst: SocketAddr, sockets: &mut SocketSet<'static>) -> bool {
        let Some(slot) = self.slots.iter_mut().find(|slot| slot.is_none()) else {
            return false;
        };
        let Some(endpoint) = tcp_listen_endpoint(dst) else {
            return false;
        };
        let Some(mut socket) = self.idle_sockets.pop() else {
            return false;
        };
        socket.abort();
        if socket.listen(endpoint).is_err() {
            self.idle_sockets.push(socket);
            return false;
        }
        let handle = sockets.add(socket);
        *slot = Some(TcpProxySlot {
            handle,
            entry: TcpProxyEntry::listening(src, dst),
        });
        true
    }

    fn append_established_destinations(
        &self,
        sockets: &SocketSet<'static>,
        destinations: &mut Vec<(usize, SocketAddr)>,
    ) {
        for (index, slot) in self.slots.iter().enumerate() {
            let Some(slot) = slot else {
                continue;
            };
            if slot.entry.active.is_some() {
                continue;
            }
            let socket = sockets.get::<tcp::Socket>(slot.handle);
            if !matches!(socket.state(), tcp::State::Established | tcp::State::CloseWait) {
                continue;
            }
            if let Some(endpoint) = socket.local_endpoint() {
                destinations.push((index, SocketAddr::new(endpoint.addr.into(), endpoint.port)));
            }
        }
    }

    fn activate(&mut self, slot_index: usize, proxy_id: TcpProxyId, proxy: TcpProxy<R>) {
        let Some(slot) = self.slots.get_mut(slot_index).and_then(Option::as_mut) else {
            return;
        };
        slot.entry.active = Some(proxy_id);
        slot.entry.proxy = Some(proxy);
    }

    fn drive_proxy_entry(
        &mut self,
        proxy_id: TcpProxyId,
        sockets: &mut SocketSet<'static>,
        events: &mut Vec<TcpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        let mut close_slot = None;
        let mut permit = TcpProxyPermit::ALL;
        if let Some(slot) = self.slot_by_proxy_mut(proxy_id) {
            let socket = sockets.get_mut::<tcp::Socket>(slot.handle);
            if let Some(proxy) = slot.entry.proxy.as_mut() {
                while let DriveSmoltcpTcpRecv::Bytes(bytes) =
                    drive.recv_smoltcp_tcp(buffers, socket, DriveRunnable::READ_GUEST)
                {
                    proxy.write(bytes);
                }
            }
            drive.send_smoltcp_tcp_queue(&mut slot.entry.pending_writes, socket);
            permit = if slot.entry.pending_writes.is_empty() && socket.can_send() {
                TcpProxyPermit::ALL
            } else {
                TcpProxyPermit::WRITE_UPSTREAM
            };
            slot.entry.upstream_read_masked =
                !permit.contains(TcpProxyPermit::READ_UPSTREAM) && slot.entry.proxy.is_some();
            if socket.state() == tcp::State::CloseWait
                && !socket.can_recv()
                && !slot.entry.guest_write_closed
                && drive
                    .apply_state_change(|| {
                        slot.entry.guest_write_closed = true;
                        if let Some(proxy) = slot.entry.proxy.as_mut() {
                            proxy.finish_guest_write();
                        }
                    })
                    .is_none()
            {
                return;
            }
            if slot.entry.proxy_closed
                && slot.entry.pending_writes.is_empty()
                && socket.may_send()
                && drive.apply_state_change(|| socket.close()).is_none()
            {
                return;
            }
            if !socket.is_active() {
                if drive
                    .apply_state_change(|| {
                        if let Some(proxy) = slot.entry.proxy.as_mut() {
                            proxy.close();
                        }
                    })
                    .is_none()
                {
                    return;
                }
                close_slot = Some((slot.handle, proxy_id));
            }
        }
        self.drive_proxy(proxy_id, sockets, events, drive, buffers, runtime, permit);
        if let Some((handle, proxy_id)) = close_slot {
            self.remove_proxy(proxy_id, runtime.reactor_mut());
            self.remove_slot_by_handle(handle);
            self.recycle_socket(handle, sockets);
        }
    }

    fn slot_by_proxy_mut(&mut self, proxy_id: TcpProxyId) -> Option<&mut TcpProxySlot<R>> {
        self.slots
            .iter_mut()
            .filter_map(Option::as_mut)
            .find(|slot| slot.entry.active == Some(proxy_id))
    }

    fn append_runnable_proxies(&self, sockets: &SocketSet<'static>, proxies: &mut Vec<TcpProxyId>) {
        for slot in self.slots.iter().filter_map(Option::as_ref) {
            let Some(proxy_id) = slot.entry.active else {
                continue;
            };
            let socket = sockets.get::<tcp::Socket>(slot.handle);
            if slot.entry.is_runnable(socket) && !proxies.contains(&proxy_id) {
                proxies.push(proxy_id);
            }
        }
    }

    fn mark_reactor_ready(&mut self, proxy_id: TcpProxyId, readable: bool, writable: bool) {
        let Some(slot) = self.slot_by_proxy_mut(proxy_id) else {
            return;
        };
        if let Some(proxy) = slot.entry.proxy.as_mut() {
            proxy.mark_reactor_ready(readable, writable);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn drive_proxy(
        &mut self,
        proxy_id: TcpProxyId,
        sockets: &mut SocketSet<'static>,
        events: &mut Vec<TcpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
        permit: TcpProxyPermit,
    ) {
        loop {
            if !drive.can_start_operation() {
                break;
            }
            let before_proxy_drive = drive.progress();
            let poll = {
                let Some(slot) = self.slot_by_proxy_mut(proxy_id) else {
                    break;
                };
                let Some(proxy) = slot.entry.proxy.as_mut() else {
                    break;
                };
                proxy.drive(buffers, runtime, drive, permit)
            };
            let direct_progress = drive.progress() != before_proxy_drive;
            match poll {
                TcpProxyPoll::Bytes(bytes) => {
                    if let Some(slot) = self.slot_by_proxy_mut(proxy_id) {
                        slot.entry.pending_writes.push(bytes);
                        let socket = sockets.get_mut::<tcp::Socket>(slot.handle);
                        drive.send_smoltcp_tcp_queue(&mut slot.entry.pending_writes, socket);
                        if !slot.entry.pending_writes.is_empty() || !socket.can_send() {
                            drive.wait_for_guest_send_capacity();
                            break;
                        }
                    }
                }
                TcpProxyPoll::Event(event) if event.is_terminal() => {
                    if !self.emit_or_queue(events, drive, event) {
                        break;
                    }
                    self.remove_proxy(proxy_id, runtime.reactor_mut());
                    self.close_guest(proxy_id, sockets, drive);
                    break;
                }
                TcpProxyPoll::Event(event) => {
                    if !self.emit_or_queue(events, drive, event) {
                        break;
                    }
                }
                TcpProxyPoll::Pending => {
                    if !direct_progress {
                        break;
                    }
                }
            }
        }
    }

    fn close_guest(&mut self, proxy_id: TcpProxyId, sockets: &mut SocketSet<'static>, drive: &mut DriveTurn<'_>) {
        let Some(slot) = self.slot_by_proxy_mut(proxy_id) else {
            return;
        };
        slot.entry.proxy_closed = true;
        let socket = sockets.get_mut::<tcp::Socket>(slot.handle);
        drive.send_smoltcp_tcp_queue(&mut slot.entry.pending_writes, socket);
        if slot.entry.pending_writes.is_empty() && socket.may_send() {
            socket.close();
        }
    }

    fn remove_proxy(&mut self, proxy_id: TcpProxyId, reactor: &mut R) {
        let Some(slot) = self.slot_by_proxy_mut(proxy_id) else {
            return;
        };
        if let Some(mut proxy) = slot.entry.proxy.take() {
            #[cfg(any(test, feature = "simulation"))]
            {
                slot.entry.last_proxy_snapshot = Some(proxy.debug_snapshot());
            }
            proxy.deregister(reactor);
        }
    }

    fn shutdown(&mut self, reactor: &mut R) {
        self.pending_events.clear();
        for slot in self.slots.iter_mut().filter_map(Option::as_mut) {
            if let Some(mut proxy) = slot.entry.proxy.take() {
                proxy.deregister(reactor);
            }
        }
    }

    fn emit_or_queue(
        &mut self,
        events: &mut Vec<TcpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        event: TcpProxyEvent,
    ) -> bool {
        match drive.push_event(events, event) {
            Ok(()) => true,
            Err(event) => {
                self.pending_events.push_front(event);
                false
            }
        }
    }

    fn remove_slot_by_handle(&mut self, handle: SocketHandle) {
        if let Some(slot) = self
            .slots
            .iter_mut()
            .find(|slot| slot.as_ref().is_some_and(|slot| slot.handle == handle))
        {
            *slot = None;
        }
    }

    fn recycle_socket(&mut self, handle: SocketHandle, sockets: &mut SocketSet<'static>) {
        let socket = sockets.remove(handle);
        if let smoltcp::socket::Socket::Tcp(socket) = socket {
            let mut socket = socket;
            socket.abort();
            self.idle_sockets.push(socket);
        }
    }

    #[cfg(any(test, feature = "simulation"))]
    fn active_proxy_slots(&self) -> usize {
        self.slots.iter().filter(|slot| slot.is_some()).count()
    }

    #[cfg(any(test, feature = "simulation"))]
    fn debug_snapshot(&self, sockets: &SocketSet<'static>) -> String {
        let mut output = String::new();
        let active = self.active_proxy_slots();
        let _ = write!(
            output,
            "TcpProxies {{ slots_active: {active}, idle_sockets: {}",
            self.idle_sockets.len()
        );
        for (index, slot) in self.slots.iter().enumerate() {
            let Some(slot) = slot else {
                continue;
            };
            let socket = sockets.get::<tcp::Socket>(slot.handle);
            let active_proxy_snapshot = slot.entry.proxy.as_ref().map(TcpProxy::debug_snapshot);
            let io = slot.entry.proxy.as_ref().map_or_else(
                || IoSlotState::new(crate::reactor::ReactorInterest::Disabled),
                TcpProxy::io,
            );
            let _ = write!(
                output,
                ", slot[{index}]: {{ active: {:?}, has_proxy: {}, guest_write_closed: {}, proxy_closed: {}, pending_write_bytes: {}, io: {:?}, upstream_read_masked: {}, socket_state: {:?}, socket_can_send: {}, socket_can_recv: {}, socket_may_send: {}, socket_may_recv: {}, active_proxy_snapshot: {:?}, last_proxy_snapshot: {:?} }}",
                slot.entry.active,
                slot.entry.proxy.is_some(),
                slot.entry.guest_write_closed,
                slot.entry.proxy_closed,
                slot.entry.pending_writes.pending_bytes(),
                io,
                slot.entry.upstream_read_masked,
                socket.state(),
                socket.can_send(),
                socket.can_recv(),
                socket.may_send(),
                socket.may_recv(),
                active_proxy_snapshot.as_deref(),
                slot.entry.last_proxy_snapshot.as_deref(),
            );
        }
        output.push_str(" }");
        output
    }
}

impl<R> TcpProxyEntry<R>
where
    R: ReactorBackend,
{
    const fn listening(src: SocketAddr, dst: SocketAddr) -> Self {
        Self {
            requested: Some((src, dst)),
            active: None,
            upstream_read_masked: false,
            pending_writes: WriteQueue::new(),
            guest_write_closed: false,
            proxy_closed: false,
            proxy: None,
            #[cfg(any(test, feature = "simulation"))]
            last_proxy_snapshot: None,
        }
    }

    fn is_runnable(&self, socket: &tcp::Socket<'_>) -> bool {
        if self.active.is_none() {
            return false;
        }
        if self.proxy.is_none() {
            return (self.proxy_closed && socket.may_send())
                || !socket.is_active()
                || (!self.pending_writes.is_empty() && socket.can_send());
        }
        let Some(proxy) = self.proxy.as_ref() else {
            return false;
        };
        socket.can_recv()
            || (matches!(socket.state(), tcp::State::CloseWait) && !socket.can_recv() && !self.guest_write_closed)
            || !socket.is_active()
            || (!self.pending_writes.is_empty() && socket.can_send())
            || (self.upstream_read_masked && self.pending_writes.is_empty() && socket.can_send())
            || (!self.upstream_read_masked && proxy.io().can_read())
            || (proxy.io().can_write() && proxy.has_reactor_write_work())
            || proxy.has_local_work(socket.can_send())
    }
}

fn tcp_socket_buffered(capacity: usize) -> tcp::Socket<'static> {
    let rx = tcp::SocketBuffer::new(vec![0; capacity]);
    let tx = tcp::SocketBuffer::new(vec![0; capacity]);
    let mut socket = tcp::Socket::new(rx, tx);
    socket.set_ack_delay(None);
    socket.set_nagle_enabled(false);
    socket
}

pub(crate) const fn tcp_listen_endpoint(dst: SocketAddr) -> Option<IpListenEndpoint> {
    Some(IpListenEndpoint {
        addr: Some(match dst.ip() {
            std::net::IpAddr::V4(address) => IpAddress::Ipv4(address),
            std::net::IpAddr::V6(_) => return None,
        }),
        port: dst.port(),
    })
}

impl TcpProxyEvent {
    const fn terminal_proxy(&self) -> Option<TcpProxyId> {
        match self {
            Self::Closed { proxy } | Self::Error { proxy, .. } => Some(*proxy),
            Self::DnsResolved { .. } => None,
        }
    }

    const fn is_terminal(&self) -> bool {
        matches!(self, Self::Closed { .. } | Self::Error { .. })
    }

    fn error(proxy: TcpProxyId, message: impl Into<String>) -> Self {
        Self::Error {
            proxy,
            context: None,
            message: message.into(),
        }
    }

    fn with_context(mut self, context: TcpProxyErrorContext) -> Self {
        if let Self::Error {
            context: event_context, ..
        } = &mut self
            && event_context.is_none()
        {
            *event_context = Some(Box::new(context));
        }
        self
    }

    const fn closed(proxy: TcpProxyId) -> Self {
        Self::Closed { proxy }
    }
}

impl TcpProxyErrorContext {
    fn new(
        destination: SocketAddr,
        upstream: SocketAddr,
        authority: Option<String>,
        route: &'static str,
        phase: &'static str,
    ) -> Self {
        Self {
            destination: destination.to_string(),
            upstream: upstream.to_string(),
            authority,
            route,
            phase,
        }
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "HTTPS/TLS is the expected agent hot path, so boxing TLS would add per-proxy allocation on the path we optimize"
)]
enum TcpProxy<R: ReactorBackend> {
    Plain(PlainTcpProxy<R>),
    Tls(TlsTcpProxy<R>),
}

enum TcpProxyPoll {
    Bytes(ByteBuf),
    Event(TcpProxyEvent),
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpProxyPermit(u8);

impl TcpProxyPermit {
    const READ_UPSTREAM_BITS: u8 = 1 << 0;
    const WRITE_UPSTREAM_BITS: u8 = 1 << 1;

    const READ_UPSTREAM: Self = Self(Self::READ_UPSTREAM_BITS);
    const WRITE_UPSTREAM: Self = Self(Self::WRITE_UPSTREAM_BITS);
    const ALL: Self = Self(Self::READ_UPSTREAM_BITS | Self::WRITE_UPSTREAM_BITS);

    const fn contains(self, permit: Self) -> bool {
        self.0 & permit.0 != 0
    }
}

impl<R> TcpProxy<R>
where
    R: ReactorBackend,
{
    fn connecting(
        proxy: TcpProxyId,
        requested_dst: SocketAddr,
        dst: SocketAddr,
        route: TcpEgressRoute,
        _buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> io::Result<Self> {
        match route {
            TcpEgressRoute::Tls(policy) => Ok(Self::Tls(TlsTcpProxy::new(proxy, requested_dst, policy))),
            TcpEgressRoute::Plain(policy) => {
                PlainTcpProxy::connecting(proxy, requested_dst, dst, None, PlainRoute::Policy(policy), runtime)
                    .map(Self::Plain)
            }
            TcpEgressRoute::Dns { upstream } => {
                PlainTcpProxy::connecting(proxy, requested_dst, upstream, None, PlainRoute::Dns, runtime)
                    .map(Self::Plain)
            }
        }
    }

    fn failed(proxy: TcpProxyId, context: TcpProxyErrorContext, message: String) -> Self {
        Self::Plain(PlainTcpProxy::failed(proxy, context, message))
    }

    fn write(&mut self, bytes: ByteBuf) {
        match self {
            Self::Plain(plain) => plain.write(bytes),
            Self::Tls(tls) => tls.write(bytes),
        }
    }

    const fn finish_guest_write(&mut self) {
        match self {
            Self::Plain(plain) => plain.finish_guest_write(),
            Self::Tls(tls) => tls.finish_guest_write(),
        }
    }

    const fn close(&mut self) {
        match self {
            Self::Plain(plain) => plain.close(),
            Self::Tls(tls) => tls.close(),
        }
    }

    const fn mark_reactor_ready(&mut self, readable: bool, writable: bool) {
        match self {
            Self::Plain(plain) => plain.mark_reactor_ready(readable, writable),
            Self::Tls(tls) => tls.mark_reactor_ready(readable, writable),
        }
    }

    fn deregister(&mut self, reactor: &mut R) {
        match self {
            Self::Plain(plain) => plain.deregister(reactor),
            Self::Tls(tls) => tls.deregister(reactor),
        }
    }

    fn has_local_work(&self, guest_can_send: bool) -> bool {
        match self {
            Self::Plain(plain) => plain.has_local_work(guest_can_send),
            Self::Tls(tls) => tls.has_local_work(guest_can_send),
        }
    }

    const fn io(&self) -> IoSlotState {
        match self {
            Self::Plain(plain) => plain.io(),
            Self::Tls(tls) => tls.io(),
        }
    }

    fn has_reactor_write_work(&self) -> bool {
        match self {
            Self::Plain(plain) => plain.has_reactor_write_work(),
            Self::Tls(tls) => tls.has_reactor_write_work(),
        }
    }

    #[cfg(any(test, feature = "simulation"))]
    fn debug_snapshot(&self) -> String {
        match self {
            Self::Plain(plain) => plain.debug_snapshot(),
            Self::Tls(tls) => tls.debug_snapshot(),
        }
    }

    fn drive(
        &mut self,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
        drive: &mut DriveTurn<'_>,
        permit: TcpProxyPermit,
    ) -> TcpProxyPoll {
        match self {
            Self::Plain(plain) => {
                attach_error_context(plain.drive(buffers, runtime.reactor_mut(), drive, permit), || {
                    plain.error_context()
                })
            }
            Self::Tls(tls) => match tls.drive(buffers, runtime, drive, permit) {
                TlsProxyPoll::Bypass {
                    dst,
                    bytes,
                    mut pending,
                } => {
                    let context = tls.error_context();
                    let guest_write_finished = tls.guest_write_finished;
                    let close_requested = tls.close_requested;
                    let proxy_id = tls.proxy;
                    match PlainTcpProxy::connecting(
                        proxy_id,
                        tls.requested_dst,
                        dst,
                        tls.authority.clone(),
                        PlainRoute::Bypass,
                        runtime,
                    ) {
                        Ok(mut plain) => {
                            plain.write(bytes);
                            while let Some(write) = pending.pop_front() {
                                match write.into_remaining(buffers) {
                                    Ok(bytes) => plain.write(bytes),
                                    Err(error) => {
                                        return TcpProxyPoll::Event(
                                            TcpProxyEvent::error(proxy_id, error.to_string()).with_context(context),
                                        );
                                    }
                                }
                            }
                            if guest_write_finished {
                                plain.finish_guest_write();
                            }
                            if close_requested {
                                plain.close();
                            }
                            *self = Self::Plain(plain);
                            self.drive(buffers, runtime, drive, permit)
                        }
                        Err(error) => {
                            TcpProxyPoll::Event(TcpProxyEvent::error(proxy_id, error.to_string()).with_context(context))
                        }
                    }
                }
                TlsProxyPoll::Bytes(bytes) => TcpProxyPoll::Bytes(bytes),
                TlsProxyPoll::Event(event) => TcpProxyPoll::Event(event.with_context(tls.error_context())),
                TlsProxyPoll::Pending => TcpProxyPoll::Pending,
            },
        }
    }
}

fn attach_error_context(poll: TcpProxyPoll, context: impl FnOnce() -> TcpProxyErrorContext) -> TcpProxyPoll {
    match poll {
        TcpProxyPoll::Event(event) => TcpProxyPoll::Event(event.with_context(context())),
        other => other,
    }
}

const fn route_name(route: &TcpEgressRoute) -> &'static str {
    match route {
        TcpEgressRoute::Plain(_) => "plain",
        TcpEgressRoute::Tls(_) => "tls",
        TcpEgressRoute::Dns { .. } => "dns",
    }
}

#[cfg(test)]
mod tests;
