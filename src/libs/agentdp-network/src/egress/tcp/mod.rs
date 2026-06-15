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
use crate::drive::DriveBudget;
use crate::gateway::Gateway;
use crate::network::NetworkLimits;
use crate::network::{TcpEgressRoute, TcpProxyId};
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorReady};
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
        budget: &mut DriveBudget,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        let start_len = events.len();
        let mut made_progress = false;
        made_progress |= self.activate_guest_connections(gateway, runtime);
        self.collect_runnable_proxies(readiness);
        let sockets = gateway.tcp_sockets_mut();
        let mut index = 0;
        while index < self.poll_scratch.len() && budget.can_continue() {
            let proxy_id = self.poll_scratch[index];
            index += 1;
            made_progress |= self
                .proxies
                .drive_proxy_entry(proxy_id, sockets, events, budget, &self.buffers, runtime);
        }
        self.poll_scratch.clear();
        made_progress || events.len() > start_len
    }

    pub(crate) fn drive_queued<C: NetworkClock>(
        &mut self,
        gateway: &mut Gateway<C>,
        events: &mut Vec<TcpProxyEvent>,
        budget: &mut DriveBudget,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        self.drive_gateway(gateway, &[], events, budget, runtime)
    }

    pub(crate) fn drive_ready<C: NetworkClock>(
        &mut self,
        gateway: &mut Gateway<C>,
        readiness: &[ReactorReady],
        events: &mut Vec<TcpProxyEvent>,
        budget: &mut DriveBudget,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        self.drive_gateway(gateway, readiness, events, budget, runtime)
    }

    fn activate_guest_connections<C: NetworkClock>(
        &mut self,
        gateway: &mut Gateway<C>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        let mut established = std::mem::take(&mut self.activation_scratch);
        established.clear();
        self.proxies
            .append_established_destinations(gateway.tcp_sockets(), &mut established);
        let mut made_progress = false;
        for &(slot_index, requested_dst) in &established {
            let proxy_id = TcpProxyId(self.proxies.next_proxy);
            self.proxies.next_proxy = self.proxies.next_proxy.saturating_add(1);
            let (upstream_dst, route) = gateway.tcp_egress_route(requested_dst);
            let route_name = route_name(&route);
            let proxy = TcpProxy::connecting(proxy_id, requested_dst, upstream_dst, route, &self.buffers, runtime)
                .unwrap_or_else(|error| {
                    TcpProxy::failed(
                        proxy_id,
                        TcpProxyErrorContext::new(requested_dst, upstream_dst, None, route_name, "connect"),
                        error.to_string(),
                    )
                });
            self.proxies.activate(slot_index, proxy_id, proxy);
            made_progress = true;
        }
        established.clear();
        self.activation_scratch = established;
        made_progress
    }

    fn collect_runnable_proxies(&mut self, readiness: &[ReactorReady]) {
        self.poll_scratch.clear();
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
            if readable || writable {
                self.proxies.mark_connect_ready(proxy_id);
            }
            if !self.poll_scratch.contains(&proxy_id) {
                self.poll_scratch.push(proxy_id);
            }
        }
        self.proxies.append_active_proxies(&mut self.poll_scratch);
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
    next_proxy: u64,
}

struct TcpProxySlot<R: ReactorBackend> {
    handle: SocketHandle,
    entry: TcpProxyEntry<R>,
}

struct TcpProxyEntry<R: ReactorBackend> {
    requested: Option<(SocketAddr, SocketAddr)>,
    active: Option<TcpProxyId>,
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
        budget: &mut DriveBudget,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        let mut made_progress = false;
        let mut close_slot = None;
        if let Some(slot) = self.slot_by_proxy_mut(proxy_id) {
            let socket = sockets.get_mut::<tcp::Socket>(slot.handle);
            if let Some(proxy) = slot.entry.proxy.as_mut() {
                while socket.can_recv() {
                    let Ok(mut bytes) = buffers.try_tcp_byte() else {
                        break;
                    };
                    bytes.resize_zeroed(buffers.tcp_byte_capacity());
                    match socket.recv_slice(bytes.as_mut_slice()) {
                        Ok(0) => break,
                        Ok(n) => {
                            bytes.truncate(n);
                            proxy.write(bytes);
                            made_progress = true;
                        }
                        Err(_error) => break,
                    }
                }
            }
            made_progress |= slot.entry.pending_writes.flush_to_guest_socket(socket).made_progress();
            if socket.state() == tcp::State::CloseWait && !slot.entry.guest_write_closed {
                slot.entry.guest_write_closed = true;
                if let Some(proxy) = slot.entry.proxy.as_mut() {
                    proxy.finish_guest_write();
                }
                made_progress = true;
            }
            if slot.entry.proxy_closed && slot.entry.pending_writes.is_empty() && socket.may_send() {
                socket.close();
                made_progress = true;
            }
            if !socket.is_active() {
                if let Some(proxy) = slot.entry.proxy.as_mut() {
                    proxy.close();
                    made_progress = true;
                }
                close_slot = Some((slot.handle, proxy_id));
            }
        }
        made_progress |= self.drive_proxy(proxy_id, sockets, events, budget, buffers, runtime);
        if let Some((handle, proxy_id)) = close_slot {
            self.remove_proxy(proxy_id, runtime.reactor_mut());
            self.remove_slot_by_handle(handle);
            self.recycle_socket(handle, sockets);
        }
        made_progress
    }

    fn slot_by_proxy_mut(&mut self, proxy_id: TcpProxyId) -> Option<&mut TcpProxySlot<R>> {
        self.slots
            .iter_mut()
            .filter_map(Option::as_mut)
            .find(|slot| slot.entry.active == Some(proxy_id))
    }

    fn append_active_proxies(&self, proxies: &mut Vec<TcpProxyId>) {
        for slot in self.slots.iter().filter_map(Option::as_ref) {
            if let Some(proxy_id) = slot.entry.active
                && !proxies.contains(&proxy_id)
            {
                proxies.push(proxy_id);
            }
        }
    }

    fn mark_connect_ready(&mut self, proxy_id: TcpProxyId) {
        let Some(slot) = self.slot_by_proxy_mut(proxy_id) else {
            return;
        };
        if let Some(proxy) = slot.entry.proxy.as_mut() {
            proxy.mark_connect_ready();
        }
    }

    fn drive_proxy(
        &mut self,
        proxy_id: TcpProxyId,
        sockets: &mut SocketSet<'static>,
        events: &mut Vec<TcpProxyEvent>,
        budget: &mut DriveBudget,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        let mut made_progress = false;
        loop {
            if !budget.step() || !budget.can_continue() {
                break;
            }
            let poll = {
                let Some(slot) = self.slot_by_proxy_mut(proxy_id) else {
                    break;
                };
                let Some(proxy) = slot.entry.proxy.as_mut() else {
                    break;
                };
                proxy.drive(buffers, runtime)
            };
            match poll {
                TcpProxyPoll::Bytes(bytes) => {
                    if let Some(slot) = self.slot_by_proxy_mut(proxy_id) {
                        let socket = sockets.get_mut::<tcp::Socket>(slot.handle);
                        slot.entry.pending_writes.push(bytes);
                        let _flushed = slot.entry.pending_writes.flush_to_guest_socket(socket);
                    }
                    made_progress = true;
                }
                TcpProxyPoll::Event(event) if event.is_terminal() => {
                    self.remove_proxy(proxy_id, runtime.reactor_mut());
                    self.close_guest(proxy_id, sockets);
                    push_event(events, budget, event);
                    made_progress = true;
                    break;
                }
                TcpProxyPoll::Event(event) => {
                    push_event(events, budget, event);
                    made_progress = true;
                }
                TcpProxyPoll::Progress => made_progress = true,
                TcpProxyPoll::Blocked => break,
            }
        }
        made_progress
    }

    fn close_guest(&mut self, proxy_id: TcpProxyId, sockets: &mut SocketSet<'static>) {
        let Some(slot) = self.slot_by_proxy_mut(proxy_id) else {
            return;
        };
        slot.entry.proxy_closed = true;
        let socket = sockets.get_mut::<tcp::Socket>(slot.handle);
        let _flushed = slot.entry.pending_writes.flush_to_guest_socket(socket);
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
        for slot in self.slots.iter_mut().filter_map(Option::as_mut) {
            if let Some(mut proxy) = slot.entry.proxy.take() {
                proxy.deregister(reactor);
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
            let _ = write!(
                output,
                ", slot[{index}]: {{ active: {:?}, has_proxy: {}, guest_write_closed: {}, proxy_closed: {}, pending_write_bytes: {}, socket_state: {:?}, socket_can_send: {}, socket_can_recv: {}, socket_may_send: {}, socket_may_recv: {}, active_proxy_snapshot: {:?}, last_proxy_snapshot: {:?} }}",
                slot.entry.active,
                slot.entry.proxy.is_some(),
                slot.entry.guest_write_closed,
                slot.entry.proxy_closed,
                slot.entry.pending_writes.pending_bytes(),
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
            pending_writes: WriteQueue::new(),
            guest_write_closed: false,
            proxy_closed: false,
            proxy: None,
            #[cfg(any(test, feature = "simulation"))]
            last_proxy_snapshot: None,
        }
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

fn push_event(events: &mut Vec<TcpProxyEvent>, budget: &mut DriveBudget, event: TcpProxyEvent) {
    if budget.event(1) {
        events.push(event);
    }
}

impl TcpProxyEvent {
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
    Progress,
    Blocked,
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

    const fn mark_connect_ready(&mut self) {
        match self {
            Self::Plain(plain) => plain.mark_connect_ready(),
            Self::Tls(tls) => tls.mark_connect_ready(),
        }
    }

    fn deregister(&mut self, reactor: &mut R) {
        match self {
            Self::Plain(plain) => plain.deregister(reactor),
            Self::Tls(tls) => tls.deregister(reactor),
        }
    }

    #[cfg(any(test, feature = "simulation"))]
    fn debug_snapshot(&self) -> String {
        match self {
            Self::Plain(_plain) => "PlainTcpProxy".to_owned(),
            Self::Tls(tls) => tls.debug_snapshot(),
        }
    }

    fn drive(&mut self, buffers: &BufferPool, runtime: &mut impl NetworkRuntime<Reactor = R>) -> TcpProxyPoll {
        match self {
            Self::Plain(plain) => {
                attach_error_context(plain.drive(buffers, runtime.reactor_mut()), || plain.error_context())
            }
            Self::Tls(tls) => match tls.drive(buffers, runtime) {
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
                            self.drive(buffers, runtime)
                        }
                        Err(error) => {
                            TcpProxyPoll::Event(TcpProxyEvent::error(proxy_id, error.to_string()).with_context(context))
                        }
                    }
                }
                TlsProxyPoll::Bytes(bytes) => TcpProxyPoll::Bytes(bytes),
                TlsProxyPoll::Event(event) => TcpProxyPoll::Event(event.with_context(tls.error_context())),
                TlsProxyPoll::Progress => TcpProxyPoll::Progress,
                TlsProxyPoll::Blocked => TcpProxyPoll::Blocked,
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
