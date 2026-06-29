use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::rc::{Rc, Weak};
use std::time::Duration;

use crate::connectors::tcp::TcpConnector;
use crate::connectors::udp::UdpSocketFactory;
use crate::guest::{GuestIoSource, TransportError};
use crate::reactor::ReactorItemId;
use crate::reactor::{
    ReactorBackend, ReactorInterest, ReactorReady, ReactorRegistrationToken, ReactorTcpListener, ReactorTcpStream,
    ReactorUdpSocket, ReactorWake,
};

const LOOPBACK_EPHEMERAL: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));

#[derive(Debug, Clone, Default)]
pub(crate) struct SimReactor {
    inner: Rc<RefCell<SimReactorState>>,
}

#[derive(Debug, Default)]
struct SimReactorState {
    registered: BTreeMap<ReactorItemId, ReactorInterest>,
    ready: VecDeque<ReactorReady>,
    wake_requested: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SimEndpointRegistry {
    inner: Rc<RefCell<SimEndpointRegistryState>>,
}

enum SimEndpoint {
    TcpEcho,
    TcpHandler {
        handler: SimTcpHandler,
        write_limit: Option<usize>,
    },
    UdpEcho,
    UdpHandler {
        handler: SimUdpHandler,
    },
    DnsA {
        address: Ipv4Addr,
    },
}

#[derive(Default)]
struct SimEndpointRegistryState {
    next_port: u16,
    endpoints: BTreeMap<SocketAddr, SimEndpoint>,
}

pub type SimTcpHandler = Rc<RefCell<dyn SimTcpHandlerFn>>;
pub type SimUdpHandler = Rc<RefCell<dyn SimUdpHandlerFn>>;

pub trait SimTcpHandlerFn {
    /// # Errors
    ///
    /// Returns an error when the simulated endpoint rejects the guest bytes.
    fn handle(&mut self, bytes: &[u8]) -> io::Result<SimTcpResponse>;
}

pub trait SimUdpHandlerFn {
    /// # Errors
    ///
    /// Returns an error when the simulated endpoint rejects the datagram.
    fn handle(&mut self, bytes: &[u8]) -> io::Result<SimUdpResponse>;
}

#[derive(Debug, Default)]
pub struct SimTcpResponse {
    pub bytes: Vec<u8>,
    pub followup_bytes: Vec<Vec<u8>>,
    pub close: bool,
    pub reset: bool,
}

struct SimTcpWrite {
    accepted: usize,
    response: SimTcpResponse,
}

impl SimTcpResponse {
    #[must_use]
    pub const fn bytes(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            followup_bytes: Vec::new(),
            close: false,
            reset: false,
        }
    }

    #[must_use]
    pub const fn segmented(bytes: Vec<u8>, followup_bytes: Vec<Vec<u8>>) -> Self {
        Self {
            bytes,
            followup_bytes,
            close: false,
            reset: false,
        }
    }

    #[must_use]
    pub fn from_ordered_chunks(mut chunks: Vec<Vec<u8>>) -> Self {
        if chunks.is_empty() {
            return Self::default();
        }
        let bytes = chunks.remove(0);
        Self::segmented(bytes, chunks)
    }

    #[must_use]
    pub fn into_ordered_chunks(self) -> Vec<Vec<u8>> {
        let mut chunks = Vec::with_capacity(usize::from(!self.bytes.is_empty()) + self.followup_bytes.len());
        if !self.bytes.is_empty() {
            chunks.push(self.bytes);
        }
        chunks.extend(self.followup_bytes);
        chunks
    }

    #[must_use]
    pub const fn close() -> Self {
        Self {
            bytes: Vec::new(),
            followup_bytes: Vec::new(),
            close: true,
            reset: false,
        }
    }

    #[must_use]
    pub const fn reset() -> Self {
        Self {
            bytes: Vec::new(),
            followup_bytes: Vec::new(),
            close: false,
            reset: true,
        }
    }
}

#[derive(Debug, Default)]
pub struct SimUdpResponse {
    pub bytes: Option<Vec<u8>>,
    pub ready_after_polls: usize,
}

impl SimUdpResponse {
    #[must_use]
    pub const fn bytes(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Some(bytes),
            ready_after_polls: 0,
        }
    }

    #[must_use]
    pub const fn delayed(bytes: Vec<u8>, ready_after_polls: usize) -> Self {
        Self {
            bytes: Some(bytes),
            ready_after_polls,
        }
    }

    #[must_use]
    pub const fn none() -> Self {
        Self {
            bytes: None,
            ready_after_polls: 0,
        }
    }
}

impl std::fmt::Debug for SimEndpointRegistryState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SimEndpointRegistryState")
            .field("next_port", &self.next_port)
            .field("endpoints", &self.endpoints.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl std::fmt::Debug for SimEndpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TcpEcho => formatter.write_str("TcpEcho"),
            Self::TcpHandler { .. } => formatter.write_str("TcpHandler"),
            Self::UdpHandler { .. } => formatter.write_str("UdpHandler"),
            Self::UdpEcho => formatter.write_str("UdpEcho"),
            Self::DnsA { address } => formatter.debug_struct("DnsA").field("address", address).finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SimReactorWake {
    #[allow(
        dead_code,
        reason = "used by the simulated wake hook when external simulation drivers wake the reactor"
    )]
    inner: Weak<RefCell<SimReactorState>>,
}

#[derive(Debug, Default)]
pub(crate) struct SimTcpStream {
    readable: Option<ReadableChunk>,
    followup_readable: VecDeque<Vec<u8>>,
    endpoint: Option<SocketAddr>,
    endpoints: Option<SimEndpointRegistry>,
    reactor: Option<Rc<RefCell<SimReactorState>>>,
    item: Option<ReactorItemId>,
    closed: bool,
    reset: bool,
}

#[derive(Debug)]
struct ReadableChunk {
    bytes: Vec<u8>,
    offset: usize,
}

#[derive(Debug)]
pub(crate) struct SimTcpListener {
    local_addr: SocketAddr,
    accepted: RefCell<VecDeque<(SimTcpStream, SocketAddr)>>,
}

#[derive(Debug)]
pub(crate) struct SimUdpSocket {
    local_addr: SocketAddr,
    recv: RefCell<VecDeque<(Vec<u8>, SocketAddr)>>,
    delayed_recv: RefCell<VecDeque<DelayedUdpResponse>>,
    connected: RefCell<Option<SocketAddr>>,
    endpoints: RefCell<Option<SimEndpointRegistry>>,
    reactor: RefCell<Option<Rc<RefCell<SimReactorState>>>>,
    item: RefCell<Option<ReactorItemId>>,
}

#[derive(Debug)]
struct DelayedUdpResponse {
    bytes: Vec<u8>,
    peer: SocketAddr,
    remaining_polls: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct SimTcpConnector {
    endpoints: SimEndpointRegistry,
}

#[derive(Debug, Clone)]
pub(crate) struct SimUdpSocketFactory {
    endpoints: SimEndpointRegistry,
}

impl SimReactor {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push_ready(&self, item: ReactorItemId, readable: bool, writable: bool) {
        self.inner.borrow_mut().push_ready(item, readable, writable);
    }

    pub(crate) fn pending_ready_len(&self) -> usize {
        self.inner.borrow().ready.len()
    }
}

impl SimReactorState {
    fn push_ready(&mut self, item: ReactorItemId, readable: bool, writable: bool) {
        let Some(interest) = self.registered.get(&item).copied() else {
            return;
        };
        let readable = readable && interest.readable();
        let writable = writable && interest.writable();
        if !readable && !writable {
            return;
        }
        for ready in &mut self.ready {
            let ReactorReady::Io {
                item: ready_item,
                readable: ready_readable,
                writable: ready_writable,
            } = ready
            else {
                continue;
            };
            if *ready_item == item {
                *ready_readable |= readable;
                *ready_writable |= writable;
                return;
            }
        }
        self.ready.push_back(ReactorReady::Io {
            item,
            readable,
            writable,
        });
    }
}

impl SimEndpointRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn tcp_echo(&self) -> SocketAddr {
        self.insert(SimEndpoint::TcpEcho)
    }

    pub(crate) fn tcp_handler(
        &self,
        addr: SocketAddr,
        handler: SimTcpHandler,
        write_limit: Option<usize>,
    ) -> SocketAddr {
        self.insert_at(addr, SimEndpoint::TcpHandler { handler, write_limit })
    }

    pub(crate) fn udp_echo(&self) -> SocketAddr {
        self.insert(SimEndpoint::UdpEcho)
    }

    pub(crate) fn udp_handler(&self, addr: SocketAddr, handler: SimUdpHandler) -> SocketAddr {
        self.insert_at(addr, SimEndpoint::UdpHandler { handler })
    }

    pub(crate) fn dns_a(&self, address: Ipv4Addr) -> SocketAddr {
        self.insert(SimEndpoint::DnsA { address })
    }

    pub(crate) fn dns_a_at(&self, addr: SocketAddr, address: Ipv4Addr) -> SocketAddr {
        self.insert_at(addr, SimEndpoint::DnsA { address })
    }

    pub(crate) fn tcp_connector(&self) -> SimTcpConnector {
        SimTcpConnector {
            endpoints: self.clone(),
        }
    }

    pub(crate) fn udp_socket_factory(&self) -> SimUdpSocketFactory {
        SimUdpSocketFactory {
            endpoints: self.clone(),
        }
    }

    fn insert(&self, endpoint: SimEndpoint) -> SocketAddr {
        let mut inner = self.inner.borrow_mut();
        if inner.next_port == 0 {
            inner.next_port = 30_000;
        }
        let port = inner.next_port;
        inner.next_port = inner.next_port.saturating_add(1);
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        inner.endpoints.insert(addr, endpoint);
        addr
    }

    fn insert_at(&self, addr: SocketAddr, endpoint: SimEndpoint) -> SocketAddr {
        self.inner.borrow_mut().endpoints.insert(addr, endpoint);
        addr
    }

    fn handle_tcp_write(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<SimTcpWrite> {
        match self.inner.borrow().endpoints.get(&dst) {
            Some(SimEndpoint::TcpEcho) => Ok(SimTcpWrite {
                accepted: bytes.len(),
                response: SimTcpResponse::bytes(bytes.to_vec()),
            }),
            Some(SimEndpoint::TcpHandler { handler, write_limit }) => {
                let accepted = bytes.len().min(write_limit.unwrap_or(usize::MAX));
                let response = handler.borrow_mut().handle(&bytes[..accepted])?;
                Ok(SimTcpWrite { accepted, response })
            }
            Some(SimEndpoint::UdpEcho | SimEndpoint::UdpHandler { .. } | SimEndpoint::DnsA { .. }) => {
                Err(io::Error::new(io::ErrorKind::ConnectionRefused, "endpoint is not TCP"))
            }
            None => Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "unknown simulated TCP endpoint",
            )),
        }
    }

    fn handle_udp_send(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<SimUdpResponse> {
        match self.inner.borrow().endpoints.get(&dst) {
            Some(SimEndpoint::UdpEcho) => Ok(SimUdpResponse::bytes(bytes.to_vec())),
            Some(SimEndpoint::UdpHandler { handler }) => handler.borrow_mut().handle(bytes),
            Some(SimEndpoint::DnsA { address }) => dns_a_response(bytes, *address).map(SimUdpResponse::bytes),
            Some(SimEndpoint::TcpEcho | SimEndpoint::TcpHandler { .. }) => {
                Err(io::Error::new(io::ErrorKind::ConnectionRefused, "endpoint is not UDP"))
            }
            None => Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "unknown simulated UDP endpoint",
            )),
        }
    }
}

fn dns_a_response(query: &[u8], address: Ipv4Addr) -> io::Result<Vec<u8>> {
    if query.len() < 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS query is shorter than header",
        ));
    }
    let Some(question_end) = dns_question_end(query) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS query has invalid question",
        ));
    };
    if query.len() < question_end + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS query is missing question type",
        ));
    }

    let mut response = Vec::with_capacity(question_end + 20);
    response.extend_from_slice(&query[..2]);
    response.extend_from_slice(&0x8180_u16.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    response.extend_from_slice(&query[12..question_end + 4]);
    response.extend_from_slice(&[0xc0, 0x0c]);
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&60_u32.to_be_bytes());
    response.extend_from_slice(&4_u16.to_be_bytes());
    response.extend_from_slice(&address.octets());
    Ok(response)
}

fn dns_question_end(packet: &[u8]) -> Option<usize> {
    let mut index = 12;
    loop {
        let len = *packet.get(index)? as usize;
        index += 1;
        if len == 0 {
            return Some(index);
        }
        index = index.checked_add(len)?;
        if index > packet.len() {
            return None;
        }
    }
}

impl ReactorWake for SimReactorWake {
    fn wake(&self) -> io::Result<()> {
        if let Some(inner) = self.inner.upgrade() {
            inner.borrow_mut().wake_requested = true;
        }
        Ok(())
    }
}

impl ReactorTcpStream for SimTcpStream {
    fn connect(_addr: SocketAddr) -> io::Result<Self> {
        Ok(Self::default())
    }

    fn set_nodelay(&self, _nodelay: bool) -> io::Result<()> {
        Ok(())
    }

    fn take_error(&self) -> io::Result<Option<io::Error>> {
        if self.reset {
            Ok(Some(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "simulated TCP reset",
            )))
        } else {
            Ok(None)
        }
    }

    fn shutdown_write(&self) -> io::Result<()> {
        Ok(())
    }
}

impl SimTcpStream {
    pub(crate) fn connect_endpoint(dst: SocketAddr, endpoints: SimEndpointRegistry) -> Self {
        Self {
            endpoint: Some(dst),
            endpoints: Some(endpoints),
            ..Self::default()
        }
    }

    fn register(&mut self, reactor: Rc<RefCell<SimReactorState>>, item: ReactorItemId) {
        self.reactor = Some(reactor);
        self.item = Some(item);
    }

    fn push_readiness(&self, readable: bool, writable: bool) {
        let (Some(reactor), Some(item)) = (&self.reactor, self.item) else {
            return;
        };
        reactor.borrow_mut().push_ready(item, readable, writable);
    }

    fn has_readable_bytes(&self) -> bool {
        self.readable.is_some() || !self.followup_readable.is_empty() || self.closed || self.reset
    }

    const fn is_writable(&self) -> bool {
        !self.closed && !self.reset
    }
}

impl Read for SimTcpStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.reset {
            return Err(io::Error::new(io::ErrorKind::ConnectionReset, "simulated TCP reset"));
        }
        let len = self.read_from_current_chunk(buffer);
        if len > 0 {
            if self.has_readable_bytes() {
                self.push_readiness(true, false);
            }
            Ok(len)
        } else if let Some(bytes) = self.followup_readable.pop_front() {
            self.readable = Some(ReadableChunk { bytes, offset: 0 });
            self.push_readiness(true, false);
            Err(io::ErrorKind::WouldBlock.into())
        } else if self.closed {
            Ok(0)
        } else {
            Err(io::ErrorKind::WouldBlock.into())
        }
    }
}

impl Write for SimTcpStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.reset {
            return Err(io::Error::new(io::ErrorKind::ConnectionReset, "simulated TCP reset"));
        }
        if self.closed {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if let (Some(endpoint), Some(endpoints)) = (self.endpoint, &self.endpoints) {
            let write = endpoints.handle_tcp_write(endpoint, bytes)?;
            let response = write.response;
            self.push_readable(response.bytes);
            self.followup_readable.extend(response.followup_bytes);
            self.closed |= response.close;
            self.reset |= response.reset;
            if self.has_readable_bytes() {
                self.push_readiness(true, false);
            }
            if self.is_writable() {
                self.push_readiness(false, true);
            }
            return Ok(write.accepted);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SimTcpStream {
    fn push_readable(&mut self, bytes: Vec<u8>) {
        if bytes.is_empty() {
            return;
        }
        if self.readable.is_none() {
            self.readable = Some(ReadableChunk { bytes, offset: 0 });
        } else {
            self.followup_readable.push_back(bytes);
        }
    }

    fn read_from_current_chunk(&mut self, buffer: &mut [u8]) -> usize {
        let Some(chunk) = &mut self.readable else {
            return 0;
        };
        let available = &chunk.bytes[chunk.offset..];
        let len = buffer.len().min(available.len());
        buffer[..len].copy_from_slice(&available[..len]);
        chunk.offset += len;
        if chunk.offset == chunk.bytes.len() {
            self.readable = None;
        }
        len
    }
}

impl ReactorTcpListener for SimTcpListener {
    type Stream = SimTcpStream;

    fn bind(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            local_addr: addr,
            accepted: RefCell::new(VecDeque::new()),
        })
    }

    fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)> {
        self.accepted
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| io::ErrorKind::WouldBlock.into())
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }
}

impl ReactorUdpSocket for SimUdpSocket {
    fn bind(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            local_addr: addr,
            recv: RefCell::new(VecDeque::new()),
            delayed_recv: RefCell::new(VecDeque::new()),
            connected: RefCell::new(None),
            endpoints: RefCell::new(None),
            reactor: RefCell::new(None),
            item: RefCell::new(None),
        })
    }

    fn from_std(_socket: std::net::UdpSocket) -> Self {
        Self {
            local_addr: LOOPBACK_EPHEMERAL,
            recv: RefCell::new(VecDeque::new()),
            delayed_recv: RefCell::new(VecDeque::new()),
            connected: RefCell::new(None),
            endpoints: RefCell::new(None),
            reactor: RefCell::new(None),
            item: RefCell::new(None),
        }
    }

    fn send(&self, bytes: &[u8]) -> io::Result<usize> {
        let Some(target) = *self.connected.borrow() else {
            return Err(io::ErrorKind::NotConnected.into());
        };
        self.enqueue_response(target, bytes)?;
        Ok(bytes.len())
    }

    fn recv(&self, buffer: &mut [u8]) -> io::Result<usize> {
        self.drain_ready_delayed();
        let Some((bytes, _peer)) = self.recv.borrow_mut().pop_front() else {
            self.push_pending_readiness();
            return Err(io::ErrorKind::WouldBlock.into());
        };
        let len = buffer.len().min(bytes.len());
        buffer[..len].copy_from_slice(&bytes[..len]);
        Ok(len)
    }

    fn send_to(&self, bytes: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.enqueue_response(target, bytes)?;
        Ok(bytes.len())
    }

    fn recv_from(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.drain_ready_delayed();
        let Some((bytes, peer)) = self.recv.borrow_mut().pop_front() else {
            self.push_pending_readiness();
            return Err(io::ErrorKind::WouldBlock.into());
        };
        let len = buffer.len().min(bytes.len());
        buffer[..len].copy_from_slice(&bytes[..len]);
        Ok((len, peer))
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }
}

impl SimUdpSocket {
    pub(crate) const fn connected(target: SocketAddr, endpoints: SimEndpointRegistry) -> Self {
        Self {
            local_addr: LOOPBACK_EPHEMERAL,
            recv: RefCell::new(VecDeque::new()),
            delayed_recv: RefCell::new(VecDeque::new()),
            connected: RefCell::new(Some(target)),
            endpoints: RefCell::new(Some(endpoints)),
            reactor: RefCell::new(None),
            item: RefCell::new(None),
        }
    }

    fn register(&self, reactor: Rc<RefCell<SimReactorState>>, item: ReactorItemId) {
        *self.reactor.borrow_mut() = Some(reactor);
        *self.item.borrow_mut() = Some(item);
    }

    fn enqueue_response(&self, target: SocketAddr, bytes: &[u8]) -> io::Result<()> {
        let Some(endpoints) = self.endpoints.borrow().clone() else {
            return Ok(());
        };
        let response = endpoints.handle_udp_send(target, bytes)?;
        if let Some(bytes) = response.bytes {
            if response.ready_after_polls == 0 {
                self.recv.borrow_mut().push_back((bytes, target));
            } else {
                self.delayed_recv.borrow_mut().push_back(DelayedUdpResponse {
                    bytes,
                    peer: target,
                    remaining_polls: response.ready_after_polls,
                });
            }
            self.push_readiness(true, false);
        }
        Ok(())
    }

    fn drain_ready_delayed(&self) {
        let mut delayed = self.delayed_recv.borrow_mut();
        let mut ready = Vec::new();
        let mut pending = VecDeque::new();
        while let Some(mut response) = delayed.pop_front() {
            if response.remaining_polls == 0 {
                ready.push((response.bytes, response.peer));
            } else {
                response.remaining_polls -= 1;
                pending.push_back(response);
            }
        }
        *delayed = pending;
        drop(delayed);
        self.recv.borrow_mut().extend(ready);
    }

    fn push_pending_readiness(&self) {
        if !self.delayed_recv.borrow().is_empty() {
            self.push_readiness(true, false);
        }
    }

    fn push_readiness(&self, readable: bool, writable: bool) {
        let item = *self.item.borrow();
        let Some(reactor) = self.reactor.borrow().clone() else {
            return;
        };
        let Some(item) = item else {
            return;
        };
        reactor.borrow_mut().push_ready(item, readable, writable);
    }
}

impl TcpConnector<SimReactor> for SimTcpConnector {
    fn connect_tcp_stream(&self, dst: SocketAddr) -> io::Result<SimTcpStream> {
        if !matches!(
            self.endpoints.inner.borrow().endpoints.get(&dst),
            Some(SimEndpoint::TcpEcho | SimEndpoint::TcpHandler { .. })
        ) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "unknown simulated TCP endpoint",
            ));
        }
        Ok(SimTcpStream::connect_endpoint(dst, self.endpoints.clone()))
    }
}

impl UdpSocketFactory<SimReactor> for SimUdpSocketFactory {
    fn connect_udp_socket(&self, dst: SocketAddr) -> io::Result<SimUdpSocket> {
        if !matches!(
            self.endpoints.inner.borrow().endpoints.get(&dst),
            Some(SimEndpoint::UdpEcho | SimEndpoint::UdpHandler { .. } | SimEndpoint::DnsA { .. })
        ) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "unknown simulated UDP endpoint",
            ));
        }
        Ok(SimUdpSocket::connected(dst, self.endpoints.clone()))
    }
}

impl ReactorBackend for SimReactor {
    type Wake = SimReactorWake;
    type TcpListener = SimTcpListener;
    type TcpStream = SimTcpStream;
    type UdpSocket = SimUdpSocket;

    fn wake_handle(&self) -> Self::Wake {
        SimReactorWake {
            inner: Rc::downgrade(&self.inner),
        }
    }

    fn register_tcp_listener(
        &mut self,
        _registration: ReactorRegistrationToken,
        _source: &mut Self::TcpListener,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<()> {
        self.register(item, interest);
        Ok(())
    }

    fn register_tcp_stream(
        &mut self,
        _registration: ReactorRegistrationToken,
        source: &mut Self::TcpStream,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<()> {
        self.register(item, interest);
        source.register(self.inner.clone(), item);
        if interest.writable() && source.is_writable() {
            self.push_ready(item, false, true);
        }
        if interest.readable() && source.has_readable_bytes() {
            self.push_ready(item, true, false);
        }
        Ok(())
    }

    fn register_udp_socket(
        &mut self,
        _registration: ReactorRegistrationToken,
        source: &mut Self::UdpSocket,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<()> {
        self.register(item, interest);
        source.register(self.inner.clone(), item);
        Ok(())
    }

    fn reregister_tcp_stream(
        &self,
        _registration: ReactorRegistrationToken,
        source: &mut Self::TcpStream,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<()> {
        self.reregister(item, interest)?;
        if interest.writable() && source.is_writable() {
            self.push_ready(item, false, true);
        }
        if interest.readable() && source.has_readable_bytes() {
            self.push_ready(item, true, false);
        }
        Ok(())
    }

    fn reregister_udp_socket(
        &self,
        _registration: ReactorRegistrationToken,
        _source: &mut Self::UdpSocket,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<()> {
        self.reregister(item, interest)
    }

    fn deregister_tcp_listener(
        &mut self,
        _registration: ReactorRegistrationToken,
        _source: &mut Self::TcpListener,
        item: ReactorItemId,
    ) -> io::Result<()> {
        self.inner.borrow_mut().registered.remove(&item);
        Ok(())
    }

    fn deregister_tcp_stream(
        &mut self,
        _registration: ReactorRegistrationToken,
        _source: &mut Self::TcpStream,
        item: ReactorItemId,
    ) -> io::Result<()> {
        self.inner.borrow_mut().registered.remove(&item);
        Ok(())
    }

    fn deregister_udp_socket(
        &mut self,
        _registration: ReactorRegistrationToken,
        _source: &mut Self::UdpSocket,
        item: ReactorItemId,
    ) -> io::Result<()> {
        self.inner.borrow_mut().registered.remove(&item);
        Ok(())
    }

    fn register_guest_source(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: GuestIoSource<'_>,
        item: ReactorItemId,
    ) -> Result<(), TransportError> {
        self.register(item, ReactorInterest::Readable);
        Ok(())
    }

    fn reregister_guest_source(
        &self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: GuestIoSource<'_>,
        item: ReactorItemId,
        writable: bool,
    ) -> Result<(), TransportError> {
        let interest = if writable {
            ReactorInterest::ReadWrite
        } else {
            ReactorInterest::Readable
        };
        self.reregister(item, interest)
            .map_err(|error| TransportError::operation("reregister simulated guest source", error))
    }

    fn deregister_guest_source(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: GuestIoSource<'_>,
        item: ReactorItemId,
    ) -> Result<(), TransportError> {
        self.inner.borrow_mut().registered.remove(&item);
        Ok(())
    }

    fn ready_into(&mut self, output: &mut Vec<ReactorReady>, _timeout: Option<Duration>) -> io::Result<()> {
        let mut inner = self.inner.borrow_mut();
        output.clear();
        if inner.wake_requested {
            inner.wake_requested = false;
            output.push(ReactorReady::Wake);
        }
        let ready = std::mem::take(&mut inner.ready);
        output.extend(ready.into_iter().filter_map(|ready| match ready {
            ReactorReady::Wake => Some(ReactorReady::Wake),
            ReactorReady::Io {
                item,
                readable,
                writable,
            } => {
                let interest = inner.registered.get(&item).copied()?;
                let readable = readable && interest.readable();
                let writable = writable && interest.writable();
                (readable || writable).then_some(ReactorReady::Io {
                    item,
                    readable,
                    writable,
                })
            }
        }));
        Ok(())
    }
}

impl SimReactor {
    fn register(&self, item: ReactorItemId, interest: ReactorInterest) {
        self.inner.borrow_mut().registered.insert(item, interest);
    }

    fn reregister(&self, item: ReactorItemId, interest: ReactorInterest) -> io::Result<()> {
        let mut inner = self.inner.borrow_mut();
        let Some(current) = inner.registered.get_mut(&item) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("reactor item {item:?} is not registered"),
            ));
        };
        *current = interest;
        Ok(())
    }
}

impl ReactorInterest {
    const fn readable(self) -> bool {
        matches!(self, Self::Readable | Self::ReadWrite)
    }

    const fn writable(self) -> bool {
        matches!(self, Self::Writable | Self::ReadWrite)
    }
}

#[cfg(test)]
mod tests {
    use super::{ReactorBackend as _, *};

    #[test]
    fn queued_readiness_is_filtered_by_current_interest() {
        let mut reactor = SimReactor::new();
        let stream = SimTcpStream::default();
        let item = ReactorItemId::TcpProxy {
            proxy: crate::network::TcpProxyId(7),
        };
        let mut stream =
            crate::reactor::RegisteringTcpStream::new(&mut reactor, stream, item, ReactorInterest::ReadWrite)
                .expect("simulated stream should register")
                .commit();
        stream
            .reregister(&reactor, ReactorInterest::Disabled)
            .expect("simulated stream should disable");
        reactor.push_ready(item, true, true);

        let mut ready = Vec::new();
        reactor
            .ready_into(&mut ready, Some(Duration::ZERO))
            .expect("simulated readiness should drain");

        assert!(ready.is_empty());
    }
}
