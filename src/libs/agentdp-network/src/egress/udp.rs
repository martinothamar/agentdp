use std::collections::VecDeque;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use crate::application;
use crate::buffers::{BufferPool, ByteBuf};
use crate::clock::NetworkClock;
use crate::connectors::udp::UdpSocketFactory;
use crate::drive::{DriveDatagramRecv, DriveDatagramSend, DriveTurn};
use crate::network::UdpProxyKey;
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorInterest, ReactorReady, RegisteredUdpSocket, RegisteringUdpSocket};
use crate::runtime::NetworkRuntime;
use agentdp_ds::fixed_table::{FixedTable, FixedTableReserveError};

#[derive(Debug)]
pub(crate) enum UdpProxyEvent {
    Bytes {
        proxy: UdpProxyKey,
        bytes: ByteBuf,
        is_dns: bool,
    },
    DnsResolved {
        host: String,
        addresses: Vec<IpAddr>,
        ttl: Duration,
    },
    Closed,
    Error {
        message: String,
    },
}

pub(crate) struct UdpProxies<R: ReactorBackend> {
    proxies: UdpProxyTable<R>,
    buffers: BufferPool,
    proxy_timeout: Duration,
    sends: VecDeque<UdpSend>,
    pending: VecDeque<UdpProxyEvent>,
    scan_scratch: Vec<UdpProxyKey>,
}

impl<R> UdpProxies<R>
where
    R: ReactorBackend,
{
    pub(crate) fn new(buffers: &BufferPool) -> Self {
        Self {
            proxies: UdpProxyTable::new(buffers.limits().udp_proxy_limit),
            buffers: buffers.clone(),
            proxy_timeout: buffers.limits().udp_proxy_timeout,
            sends: VecDeque::new(),
            pending: VecDeque::new(),
            scan_scratch: Vec::with_capacity(buffers.limits().udp_proxy_limit),
        }
    }

    pub(crate) fn send(&mut self, proxy: UdpProxyKey, bytes: ByteBuf, is_dns: bool) {
        self.sends.push_back(UdpSend { proxy, bytes, is_dns });
    }

    pub(crate) fn shutdown(&mut self, runtime: &mut impl NetworkRuntime<Reactor = R>) {
        self.sends.clear();
        self.pending.clear();
        self.scan_scratch.clear();
        self.proxies.shutdown(runtime.reactor_mut());
    }

    pub(crate) fn next_expiry(&self) -> Option<Instant> {
        self.proxies.next_expiry()
    }

    pub(crate) fn expire_due(
        &mut self,
        events: &mut Vec<UdpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        let clock = runtime.clock().clone();
        while drive.can_start_operation() {
            let Some(event) = self.expire_next(runtime.reactor_mut(), &clock) else {
                break;
            };
            if !self.emit_or_queue(events, drive, event) {
                break;
            }
        }
    }

    pub(crate) fn drive_queued(
        &mut self,
        events: &mut Vec<UdpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        self.drive(events, drive, runtime);
    }

    pub(crate) fn drive_ready(
        &mut self,
        readiness: &[ReactorReady],
        events: &mut Vec<UdpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        self.latch_ready(readiness);
        self.drive(events, drive, runtime);
    }

    fn drive(
        &mut self,
        events: &mut Vec<UdpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) {
        let clock = runtime.clock().clone();
        loop {
            if self.pending.is_empty() && self.sends.is_empty() && !self.proxies.has_readable_proxy() {
                break;
            }
            if !drive.can_start_operation() {
                break;
            }
            if let Some(event) = self.pending.pop_front() {
                if !self.emit_or_queue(events, drive, event) {
                    break;
                }
                continue;
            }
            let udp_socket_factory = runtime.udp_socket_factory().clone();
            let blocked = self.drain_sends(events, drive, runtime.reactor_mut(), &udp_socket_factory, &clock);
            let before_reads = drive.progress();
            self.drain_ready_reads(events, drive, runtime.reactor_mut(), &clock);
            let ready_progress = drive.progress() != before_reads;
            if ready_progress {
                continue;
            }
            if blocked {
                break;
            }
            break;
        }
    }

    fn latch_ready(&mut self, readiness: &[ReactorReady]) {
        for ready in readiness {
            let ReactorReady::Io {
                item,
                readable,
                writable,
                ..
            } = *ready
            else {
                continue;
            };
            let ReactorItemId::UdpProxy { proxy } = item else {
                continue;
            };
            self.proxies.mark_ready(proxy, readable, writable);
        }
    }

    fn drain_sends(
        &mut self,
        events: &mut Vec<UdpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        reactor: &mut R,
        udp_socket_factory: &impl UdpSocketFactory<R>,
        clock: &impl NetworkClock,
    ) -> bool {
        let mut context = UdpSendContext {
            proxy_timeout: self.proxy_timeout,
            reactor,
            udp_socket_factory,
            clock,
        };
        let attempts = self.sends.len();
        let mut blocked = false;
        for _attempt in 0..attempts {
            if !drive.can_start_operation() {
                break;
            }
            let Some(send) = self.sends.pop_front() else {
                break;
            };
            match self
                .proxies
                .send(send.proxy, send.bytes.as_slice(), send.is_dns, &mut context, drive)
            {
                Ok(DriveDatagramSend::Sent) => {}
                Ok(DriveDatagramSend::NotReady | DriveDatagramSend::WouldBlock) => {
                    self.sends.push_back(send);
                    blocked = true;
                }
                Ok(DriveDatagramSend::Budget) => {
                    self.sends.push_front(send);
                    break;
                }
                Err(error) => {
                    self.remove_proxy(send.proxy, context.reactor);
                    let event = UdpProxyEvent::Error {
                        message: error.to_string(),
                    };
                    if !self.emit_or_queue(events, drive, event) {
                        break;
                    }
                }
            }
        }
        blocked
    }

    fn drain_ready_reads(
        &mut self,
        events: &mut Vec<UdpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        reactor: &mut R,
        clock: &impl NetworkClock,
    ) {
        self.proxies.readable_keys_into(&mut self.scan_scratch);
        while let Some(proxy) = self.scan_scratch.pop() {
            if !drive.can_start_operation() {
                break;
            }
            while let Some(udp) = self.proxies.by_key.get_mut(&proxy) {
                let (socket, io) = udp.socket.source_and_io_mut();
                match drive.recv_datagram_ready(
                    io,
                    &self.buffers,
                    socket,
                    self.buffers.limits().udp_datagram_buffer_capacity,
                ) {
                    Ok(DriveDatagramRecv::Bytes(bytes)) => {
                        udp.expires = clock.now() + self.proxy_timeout;
                        let is_dns = udp.is_dns;
                        let dns_event = if is_dns {
                            application::dns_question(bytes.as_slice()).map(|question| {
                                let records = application::dns_address_records(bytes.as_slice());
                                UdpProxyEvent::DnsResolved {
                                    host: question.host,
                                    addresses: records.addresses,
                                    ttl: records.ttl,
                                }
                            })
                        } else {
                            None
                        };
                        let bytes_event = UdpProxyEvent::Bytes { proxy, bytes, is_dns };
                        if let Err(bytes_event) = drive.push_event(events, bytes_event) {
                            if let Some(dns_event) = dns_event {
                                self.pending.push_front(dns_event);
                            }
                            self.pending.push_front(bytes_event);
                            return;
                        }
                        if let Some(dns_event) = dns_event
                            && let Err(dns_event) = drive.push_event(events, dns_event)
                        {
                            self.pending.push_front(dns_event);
                            return;
                        }
                        if !drive.can_start_operation() {
                            return;
                        }
                    }
                    Ok(DriveDatagramRecv::NotReady | DriveDatagramRecv::Blocked | DriveDatagramRecv::Budget) => return,
                    Err(error) => {
                        self.remove_proxy(proxy, reactor);
                        let event = UdpProxyEvent::Error {
                            message: error.to_string(),
                        };
                        if !self.emit_or_queue(events, drive, event) {
                            return;
                        }
                        break;
                    }
                    Ok(DriveDatagramRecv::WouldBlock) => {
                        break;
                    }
                }
            }
        }
    }

    fn expire_next(&mut self, reactor: &mut R, clock: &impl NetworkClock) -> Option<UdpProxyEvent> {
        self.proxies.expire_next(reactor, clock).map(|proxy| {
            self.drop_proxy_queues(proxy);
            UdpProxyEvent::Closed
        })
    }

    fn remove_proxy(&mut self, proxy: UdpProxyKey, reactor: &mut R) {
        self.proxies.remove(proxy, reactor);
        self.drop_proxy_queues(proxy);
    }

    fn drop_proxy_queues(&mut self, proxy: UdpProxyKey) {
        self.sends.retain(|send| send.proxy != proxy);
        self.scan_scratch.retain(|ready| *ready != proxy);
    }

    fn emit_or_queue(
        &mut self,
        events: &mut Vec<UdpProxyEvent>,
        drive: &mut DriveTurn<'_>,
        event: UdpProxyEvent,
    ) -> bool {
        match drive.push_event(events, event) {
            Ok(()) => true,
            Err(event) => {
                self.pending.push_front(event);
                false
            }
        }
    }
}

struct UdpSend {
    proxy: UdpProxyKey,
    bytes: ByteBuf,
    is_dns: bool,
}

struct UdpProxyTable<R: ReactorBackend> {
    by_key: FixedTable<UdpProxyKey, UdpProxySocket<R>>,
}

impl<R> UdpProxyTable<R>
where
    R: ReactorBackend,
{
    fn new(limit: usize) -> Self {
        Self {
            by_key: FixedTable::with_capacity(limit),
        }
    }

    fn send(
        &mut self,
        key: UdpProxyKey,
        bytes: &[u8],
        is_dns: bool,
        context: &mut UdpSendContext<'_, R, impl UdpSocketFactory<R>, impl NetworkClock>,
        drive: &mut DriveTurn<'_>,
    ) -> std::io::Result<DriveDatagramSend> {
        if self.by_key.get(&key).is_some() {
            return self.send_existing(key, bytes, is_dns, &*context, drive);
        }
        if !drive.can_prepare_whole_item_operation(bytes.len()) {
            return Ok(DriveDatagramSend::Budget);
        }
        let capacity = self.by_key.capacity();
        let reservation = self.by_key.reserve_vacant(key).map_err(|error| match error {
            FixedTableReserveError::KeyExists => std::io::Error::other("UDP proxy already exists"),
            FixedTableReserveError::Full => std::io::Error::other(format!("UDP proxy limit {capacity} exceeded")),
        })?;
        let socket = context.udp_socket_factory.connect_udp_socket(key.host_dst)?;
        let item = ReactorItemId::UdpProxy { proxy: key };
        let mut registered = RegisteringUdpSocket::new(context.reactor, socket, item, ReactorInterest::ReadWrite)?;
        let (socket, io) = registered.source_and_io_mut();
        let send_result = drive.send_datagram_ready(io, socket, bytes)?;
        if matches!(send_result, DriveDatagramSend::Sent) {
            registered.reregister(ReactorInterest::Readable)?;
        }
        let proxy = UdpProxySocket {
            socket: registered.commit(),
            is_dns,
            expires: context.clock.now() + context.proxy_timeout,
        };
        reservation.insert(proxy);
        Ok(send_result)
    }

    fn send_existing(
        &mut self,
        key: UdpProxyKey,
        bytes: &[u8],
        is_dns: bool,
        context: &UdpSendContext<'_, R, impl UdpSocketFactory<R>, impl NetworkClock>,
        drive: &mut DriveTurn<'_>,
    ) -> std::io::Result<DriveDatagramSend> {
        let Some(proxy) = self.by_key.get_mut(&key) else {
            return Err(std::io::Error::other("UDP proxy disappeared before send"));
        };
        proxy.is_dns |= is_dns;
        proxy.expires = context.clock.now() + context.proxy_timeout;
        if !proxy.socket.io().watches_write() {
            proxy.socket.reregister(context.reactor, ReactorInterest::ReadWrite)?;
        }
        let (socket, io) = proxy.socket.source_and_io_mut();
        match drive.send_datagram_ready(io, socket, bytes)? {
            DriveDatagramSend::Sent => {
                if proxy.socket.io().watches_write() {
                    proxy.socket.reregister(context.reactor, ReactorInterest::Readable)?;
                }
                Ok(DriveDatagramSend::Sent)
            }
            DriveDatagramSend::NotReady | DriveDatagramSend::WouldBlock => Ok(DriveDatagramSend::WouldBlock),
            DriveDatagramSend::Budget => Ok(DriveDatagramSend::Budget),
        }
    }

    fn expire_next(&mut self, reactor: &mut R, clock: &impl NetworkClock) -> Option<UdpProxyKey> {
        let now = clock.now();
        let key = self
            .by_key
            .iter()
            .find_map(|(key, udp)| (udp.expires <= now).then_some(key))?;
        self.remove(key, reactor);
        Some(key)
    }

    fn next_expiry(&self) -> Option<Instant> {
        self.by_key.values().map(|proxy| proxy.expires).min()
    }

    fn remove(&mut self, key: UdpProxyKey, reactor: &mut R) {
        let Some(mut proxy) = self.by_key.remove(&key) else {
            return;
        };
        proxy.socket.deregister(reactor);
    }

    fn shutdown(&mut self, reactor: &mut R) {
        let mut keys = Vec::with_capacity(self.by_key.len());
        self.by_key.keys_into(&mut keys);
        for key in keys {
            self.remove(key, reactor);
        }
    }

    fn mark_ready(&mut self, key: UdpProxyKey, readable: bool, writable: bool) {
        if let Some(proxy) = self.by_key.get_mut(&key) {
            proxy.socket.mark_reactor_ready(readable, writable);
        }
    }

    fn has_readable_proxy(&self) -> bool {
        self.by_key.values().any(|proxy| proxy.socket.io().can_read())
    }

    fn readable_keys_into(&self, output: &mut Vec<UdpProxyKey>) {
        output.clear();
        output.extend(
            self.by_key
                .iter()
                .filter_map(|(key, proxy)| proxy.socket.io().can_read().then_some(key)),
        );
    }
}

struct UdpSendContext<'a, R, U, C>
where
    R: ReactorBackend,
    U: UdpSocketFactory<R>,
    C: NetworkClock,
{
    proxy_timeout: Duration,
    reactor: &'a mut R,
    udp_socket_factory: &'a U,
    clock: &'a C,
}

#[derive(Debug)]
struct UdpProxySocket<R: ReactorBackend> {
    socket: RegisteredUdpSocket<R>,
    is_dns: bool,
    expires: Instant,
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use crate::buffers::BufferPool;
    use crate::clock::SystemClock;
    use crate::connectors::udp::UdpSocketFactory;
    use crate::drive::{DriveBudget, DriveDatagramSend, DriveReport, DriveTurn};
    use crate::network::NetworkLimits;
    use crate::reactor::{
        ReactorBackend, ReactorInterest, ReactorItemId, ReactorReady, ReactorTcpListener, ReactorTcpStream,
        ReactorUdpSocket, ReactorWake, RegisteredUdpSocket, RegisteringUdpSocket, default_backend,
    };
    use crate::runtime::NetworkRuntime;
    use crate::test_support::unit::{dns_a_response, dns_query, runtime_context};

    use super::{UdpProxies, UdpProxyEvent, UdpProxyKey, UdpProxySocket, UdpProxyTable, UdpSendContext};

    fn test_buffers() -> BufferPool {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        buffers
    }

    fn io_buffer(buffers: &BufferPool, bytes: &[u8]) -> crate::buffers::ByteBuf {
        let mut output = buffers
            .try_byte_with_capacity(bytes.len())
            .expect("prewarmed byte buffer");
        output.extend_from_slice(bytes);
        output
    }

    fn proxy(host_dst: SocketAddr) -> UdpProxyKey {
        UdpProxyKey {
            guest_src: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 2)), 40_001),
            guest_dst: host_dst,
            host_dst,
        }
    }

    fn with_drive<T>(budget: &mut DriveBudget, f: impl FnOnce(&mut DriveTurn<'_>) -> T) -> (T, DriveReport) {
        let mut report = DriveReport::new();
        let result = {
            let mut drive = DriveTurn::new(budget, &mut report);
            f(&mut drive)
        };
        (result, report)
    }

    fn mark_udp_write_waiting<R: ReactorBackend>(
        socket: &mut RegisteredUdpSocket<R>,
        reactor: &R,
    ) -> std::io::Result<()> {
        socket.reregister(reactor, ReactorInterest::ReadWrite)?;
        socket.clear_write_after_would_block();
        Ok(())
    }

    fn error_recv_udp_socket(proxy: UdpProxyKey) -> RegisteredUdpSocket<ErrorRecvReactor> {
        let mut reactor = ErrorRecvReactor;
        let mut socket = RegisteringUdpSocket::new(
            &mut reactor,
            ErrorRecvUdpSocket,
            ReactorItemId::UdpProxy { proxy },
            ReactorInterest::Readable,
        )
        .expect("error recv UDP socket should register")
        .commit();
        socket.mark_reactor_ready(true, false);
        socket
    }

    #[tokio::test(flavor = "current_thread")]
    async fn udp_proxy_sends_and_receives_datagram() -> Result<(), Box<dyn std::error::Error>> {
        let server = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let server_addr = server.local_addr()?;
        let server_task = tokio::spawn(async move {
            let mut buf = [0_u8; 16];
            let (len, peer) = server.recv_from(&mut buf).await?;
            assert_eq!(&buf[..len], b"ping");
            server.send_to(b"pong", peer).await?;
            Ok::<_, std::io::Error>(())
        });
        let buffers = test_buffers();
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let mut proxies = UdpProxies::new(&buffers);
        let bytes = io_buffer(&buffers, b"ping");

        let proxy = proxy(server_addr);
        proxies.send(proxy, bytes, false);

        let mut events = drive_udp(&mut runtime, &mut proxies).await?;
        match events.remove(0) {
            UdpProxyEvent::Bytes {
                proxy: response_proxy,
                bytes,
                is_dns,
            } => {
                assert_eq!(response_proxy, proxy);
                assert_eq!(bytes.as_slice(), b"pong");
                assert!(!is_dns);
            }
            _ => return Err("expected UDP bytes event".into()),
        }
        server_task.await??;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn udp_dns_response_emits_attribution_and_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let server = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let server_addr = server.local_addr()?;
        let server_task = tokio::spawn(async move {
            let mut buf = [0_u8; 128];
            let (_len, peer) = server.recv_from(&mut buf).await?;
            let response = dns_a_response(0x5101, "allowed.test", Ipv4Addr::new(10, 73, 0, 42), 60);
            server.send_to(&response, peer).await?;
            Ok::<_, std::io::Error>(())
        });
        let buffers = test_buffers();
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let mut proxies = UdpProxies::new(&buffers);
        let proxy = proxy(server_addr);
        let bytes = io_buffer(&buffers, &dns_query(0x5101, "allowed.test", 1));

        proxies.send(proxy, bytes, true);

        let mut events = drive_udp(&mut runtime, &mut proxies).await?;
        match events.remove(0) {
            UdpProxyEvent::Bytes {
                proxy: response_proxy,
                bytes,
                is_dns,
            } => {
                assert_eq!(response_proxy, proxy);
                assert!(is_dns);
                assert_eq!(
                    bytes.as_slice(),
                    dns_a_response(0x5101, "allowed.test", Ipv4Addr::new(10, 73, 0, 42), 60)
                );
            }
            _ => return Err("expected UDP DNS response bytes event".into()),
        }
        match events.remove(0) {
            UdpProxyEvent::DnsResolved { host, addresses, .. } => {
                assert_eq!(host, "allowed.test");
                assert_eq!(addresses, vec![IpAddr::V4(Ipv4Addr::new(10, 73, 0, 42))]);
            }
            _ => return Err("expected DNS attribution event".into()),
        }
        server_task.await??;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn udp_dns_response_respects_event_budget_without_losing_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let server = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let server_addr = server.local_addr()?;
        let server_task = tokio::spawn(async move {
            let mut buf = [0_u8; 128];
            let (_len, peer) = server.recv_from(&mut buf).await?;
            let response = dns_a_response(0x5101, "allowed.test", Ipv4Addr::new(10, 73, 0, 42), 60);
            server.send_to(&response, peer).await?;
            Ok::<_, std::io::Error>(())
        });
        let buffers = test_buffers();
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let mut proxies = UdpProxies::new(&buffers);
        let proxy = proxy(server_addr);
        proxies.send(proxy, io_buffer(&buffers, &dns_query(0x5101, "allowed.test", 1)), true);

        let mut events = Vec::new();
        let mut readiness = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let first_report = loop {
            let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
            let (_result, _report) = with_drive(&mut budget, |drive| {
                proxies.drive_queued(&mut events, drive, &mut runtime);
            });
            assert!(events.is_empty());
            runtime.reactor_mut().ready_into(&mut readiness, Some(Duration::ZERO))?;
            if !readiness.is_empty() {
                let mut budget = DriveBudget::event_loop(&NetworkLimits {
                    drive_event_budget: 1,
                    ..NetworkLimits::default()
                });
                let (_result, report) = with_drive(&mut budget, |drive| {
                    proxies.drive_ready(&readiness, &mut events, drive, &mut runtime);
                });
                break report;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("timed out waiting for UDP DNS response".into());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };

        assert!(first_report.budget_exhausted());
        assert!(matches!(events.as_slice(), [UdpProxyEvent::Bytes { .. }]));

        events.clear();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, second_report) = with_drive(&mut budget, |drive| {
            proxies.drive_queued(&mut events, drive, &mut runtime);
        });
        assert!(second_report.made_progress());
        assert!(matches!(events.as_slice(), [UdpProxyEvent::DnsResolved { .. }]));
        server_task.await??;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_udp_sends_do_not_wait_for_first_response() -> Result<(), Box<dyn std::error::Error>> {
        let server = Arc::new(tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?);
        let server_addr = server.local_addr()?;
        let server_task = tokio::spawn(async move {
            let mut buf = [0_u8; 128];
            let mut responses = Vec::new();
            let mut first_peer = None;
            for _ in 0..2 {
                let (len, peer) = server.recv_from(&mut buf).await?;
                if let Some(first_peer) = first_peer {
                    assert_eq!(peer, first_peer);
                } else {
                    first_peer = Some(peer);
                }
                let txid = u16::from_be_bytes([buf[0], buf[1]]);
                let delay = match txid {
                    0x5101 => Duration::from_millis(200),
                    0x5102 => Duration::ZERO,
                    _ => {
                        return Err(std::io::Error::other(format!(
                            "unexpected DNS transaction id {txid:#x}"
                        )));
                    }
                };
                let response = match txid {
                    0x5101 => dns_a_response(txid, "slow.test", Ipv4Addr::new(10, 73, 0, 11), 60),
                    0x5102 => dns_a_response(txid, "fast.test", Ipv4Addr::new(10, 73, 0, 12), 60),
                    _ => unreachable!(),
                };
                let server = server.clone();
                responses.push(tokio::spawn(async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    server.send_to(&response, peer).await.map(|_sent| ())
                }));
                assert!(len > 0);
            }
            for response in responses {
                response
                    .await
                    .map_err(|error| std::io::Error::other(format!("response task failed: {error}")))??;
            }
            Ok::<_, std::io::Error>(())
        });
        let buffers = test_buffers();
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let mut proxies = UdpProxies::new(&buffers);
        let slow = io_buffer(&buffers, &dns_query(0x5101, "slow.test", 1));
        let fast = io_buffer(&buffers, &dns_query(0x5102, "fast.test", 1));

        let proxy = proxy(server_addr);
        proxies.send(proxy, slow, true);
        proxies.send(proxy, fast, true);

        let mut events = tokio::time::timeout(Duration::from_secs(1), drive_udp(&mut runtime, &mut proxies)).await??;

        match events.remove(0) {
            UdpProxyEvent::Bytes {
                proxy: response_proxy,
                bytes,
                is_dns,
            } => {
                assert_eq!(response_proxy, proxy);
                assert!(is_dns);
                assert_eq!(
                    bytes.as_slice(),
                    dns_a_response(0x5102, "fast.test", Ipv4Addr::new(10, 73, 0, 12), 60)
                );
            }
            _ => return Err("expected fast DNS response bytes before delayed slow response".into()),
        }
        match events.remove(0) {
            UdpProxyEvent::DnsResolved { host, addresses, .. } => {
                assert_eq!(host, "fast.test");
                assert_eq!(addresses, vec![IpAddr::V4(Ipv4Addr::new(10, 73, 0, 12))]);
            }
            _ => return Err("expected fast DNS attribution after response bytes".into()),
        }
        server_task.await??;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn udp_proxy_reports_send_errors() -> Result<(), Box<dyn std::error::Error>> {
        let buffers = test_buffers();
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let mut proxies = UdpProxies::new(&buffers);
        let bytes = io_buffer(&buffers, b"ping");

        proxies.send(proxy(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9)), bytes, false);

        let events = drive_udp(&mut runtime, &mut proxies).await?;
        assert!(matches!(events.as_slice(), [UdpProxyEvent::Error { .. }]));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn udp_proxies_report_limit_exhaustion() -> Result<(), Box<dyn std::error::Error>> {
        let buffers = BufferPool::new(NetworkLimits {
            udp_proxy_limit: 0,
            ..NetworkLimits::default()
        });
        buffers.prewarm_instance_network();
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let mut proxies = UdpProxies::new(&buffers);
        let bytes = io_buffer(&buffers, b"ping");

        proxies.send(proxy(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9)), bytes, false);

        let events = drive_udp(&mut runtime, &mut proxies).await?;
        match events.as_slice() {
            [UdpProxyEvent::Error { message }] => {
                assert!(message.contains("UDP proxy limit 0 exceeded"));
            }
            _ => return Err("expected UDP proxy limit error".into()),
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn udp_send_queue_skips_write_waiting_proxy_without_reordering_that_proxy()
    -> Result<(), Box<dyn std::error::Error>> {
        let server = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let blocked = proxy(server.local_addr()?);
        let buffers = test_buffers();
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let mut proxies = UdpProxies::new(&buffers);
        let ready = proxy(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10));
        proxies.send(blocked, io_buffer(&buffers, b"initial"), false);
        let mut events = Vec::new();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, _report) = with_drive(&mut budget, |drive| {
            proxies.drive_queued(&mut events, drive, &mut runtime);
        });
        assert!(events.is_empty());
        mark_udp_write_waiting(
            &mut proxies.proxies.by_key.get_mut(&blocked).unwrap().socket,
            runtime.reactor(),
        )?;
        proxies.send(blocked, io_buffer(&buffers, b"blocked-1"), false);
        proxies.send(ready, io_buffer(&buffers, b"ready"), false);
        proxies.send(blocked, io_buffer(&buffers, b"blocked-2"), false);

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            proxies.drive_queued(&mut events, drive, &mut runtime);
        });

        assert!(report.wait().contains(crate::drive::DriveWait::REACTOR_WRITE));
        assert!(events.is_empty());
        assert_eq!(proxies.sends.len(), 2);
        assert_eq!(proxies.sends[0].proxy, blocked);
        assert_eq!(proxies.sends[0].bytes.as_slice(), b"blocked-1");
        assert_eq!(proxies.sends[1].proxy, blocked);
        assert_eq!(proxies.sends[1].bytes.as_slice(), b"blocked-2");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expired_udp_proxy_drops_blocked_state_and_pending_sends() -> Result<(), Box<dyn std::error::Error>> {
        let server = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let server_addr = server.local_addr()?;
        let buffers = BufferPool::new(NetworkLimits {
            udp_proxy_timeout: Duration::ZERO,
            ..NetworkLimits::default()
        });
        buffers.prewarm_instance_network();
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let mut proxies = UdpProxies::new(&buffers);
        let proxy = proxy(server_addr);
        proxies.send(proxy, io_buffer(&buffers, b"initial"), false);

        let mut events = Vec::new();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, _report) = with_drive(&mut budget, |drive| {
            proxies.drive_queued(&mut events, drive, &mut runtime);
        });
        assert!(events.is_empty());

        mark_udp_write_waiting(
            &mut proxies.proxies.by_key.get_mut(&proxy).unwrap().socket,
            runtime.reactor(),
        )?;
        proxies.send(proxy, io_buffer(&buffers, b"stale"), false);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, _report) = with_drive(&mut budget, |drive| {
            proxies.expire_due(&mut events, drive, &mut runtime);
        });

        assert!(matches!(events.as_slice(), [UdpProxyEvent::Closed]));
        assert!(proxies.proxies.by_key.get(&proxy).is_none());
        assert!(proxies.sends.iter().all(|send| send.proxy != proxy));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn udp_read_budget_preserves_readiness() -> Result<(), Box<dyn std::error::Error>> {
        let server = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let buffers = test_buffers();
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let mut proxies = UdpProxies::new(&buffers);
        let proxy = proxy(server.local_addr()?);
        proxies.send(proxy, io_buffer(&buffers, b"initial"), false);
        let mut events = Vec::new();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, _report) = with_drive(&mut budget, |drive| {
            proxies.drive_queued(&mut events, drive, &mut runtime);
        });
        proxies.proxies.mark_ready(proxy, true, false);

        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 1,
            ..NetworkLimits::default()
        });
        let (_result, _report) = with_drive(&mut budget, |drive| {
            proxies.drive_queued(&mut events, drive, &mut runtime);
        });

        assert!(events.is_empty());
        assert!(proxies.proxies.by_key.get(&proxy).unwrap().socket.io().can_read());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn udp_read_would_block_clears_readiness() -> Result<(), Box<dyn std::error::Error>> {
        let server = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let buffers = test_buffers();
        let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
        let mut proxies = UdpProxies::new(&buffers);
        let proxy = proxy(server.local_addr()?);
        proxies.send(proxy, io_buffer(&buffers, b"initial"), false);
        let mut events = Vec::new();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, _report) = with_drive(&mut budget, |drive| {
            proxies.drive_queued(&mut events, drive, &mut runtime);
        });
        proxies.proxies.mark_ready(proxy, true, false);

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, report) = with_drive(&mut budget, |drive| {
            proxies.drive_queued(&mut events, drive, &mut runtime);
        });

        assert!(events.is_empty());
        assert!(report.wait().contains(crate::drive::DriveWait::REACTOR_READ));
        assert!(!proxies.proxies.by_key.get(&proxy).unwrap().socket.io().can_read());
        Ok(())
    }

    #[test]
    fn udp_recv_error_removes_ready_proxy_without_reentering_it() {
        let buffers = test_buffers();
        let mut proxies = UdpProxies::<ErrorRecvReactor>::new(&buffers);
        let proxy = proxy(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 53));
        assert!(
            proxies
                .proxies
                .by_key
                .insert(
                    proxy,
                    UdpProxySocket {
                        socket: error_recv_udp_socket(proxy),
                        is_dns: false,
                        expires: Instant::now() + Duration::from_mins(1),
                    },
                )
                .is_ok()
        );
        let mut reactor = ErrorRecvReactor;
        let mut events = Vec::new();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());

        let (_result, _report) = with_drive(&mut budget, |drive| {
            proxies.drain_ready_reads(&mut events, drive, &mut reactor, &SystemClock);
        });

        assert!(matches!(events.as_slice(), [UdpProxyEvent::Error { .. }]));
        assert!(proxies.proxies.by_key.get(&proxy).is_none());
    }

    #[test]
    fn new_udp_proxy_send_budget_block_does_not_create_or_register_socket() {
        let stats = CountingUdpStats::default();
        let mut reactor = CountingReactor { stats: stats.clone() };
        let factory = CountingUdpFactory { stats: stats.clone() };
        let mut proxies = UdpProxyTable::<CountingReactor>::new(8);
        let proxy = proxy(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 53));
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 4,
            ..NetworkLimits::default()
        });
        let mut context = UdpSendContext {
            proxy_timeout: Duration::from_secs(30),
            reactor: &mut reactor,
            udp_socket_factory: &factory,
            clock: &SystemClock,
        };

        let (result, report) = with_drive(&mut budget, |drive| {
            proxies
                .send(proxy, b"too-large", false, &mut context, drive)
                .expect("budget block should not fail")
        });

        assert!(matches!(result, DriveDatagramSend::Budget));
        assert_eq!(stats.connects.get(), 0);
        assert_eq!(stats.registers.get(), 0);
        assert_eq!(stats.sends.get(), 0);
        assert!(proxies.by_key.get(&proxy).is_none());
        assert!(report.budget_exhausted());
        assert!(!report.made_progress());
    }

    #[test]
    fn new_udp_proxy_reregister_failure_deregisters_registered_socket() {
        let stats = CountingUdpStats::default();
        stats.fail_reregister.set(true);
        let mut reactor = CountingReactor { stats: stats.clone() };
        let factory = CountingUdpFactory { stats: stats.clone() };
        let mut proxies = UdpProxyTable::<CountingReactor>::new(8);
        let proxy = proxy(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 53));
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut context = UdpSendContext {
            proxy_timeout: Duration::from_secs(30),
            reactor: &mut reactor,
            udp_socket_factory: &factory,
            clock: &SystemClock,
        };

        let result = with_drive(&mut budget, |drive| {
            proxies.send(proxy, b"small", false, &mut context, drive)
        })
        .0;
        let Err(error) = result else {
            panic!("reregister failure should be reported");
        };

        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(stats.connects.get(), 1);
        assert_eq!(stats.registers.get(), 1);
        assert_eq!(stats.sends.get(), 1);
        assert_eq!(stats.reregisters.get(), 1);
        assert_eq!(stats.deregisters.get(), 1);
        assert!(proxies.by_key.get(&proxy).is_none());
    }

    #[test]
    fn new_udp_proxy_send_failure_deregisters_registered_socket() {
        let stats = CountingUdpStats::default();
        stats.fail_send.set(true);
        let mut reactor = CountingReactor { stats: stats.clone() };
        let factory = CountingUdpFactory { stats: stats.clone() };
        let mut proxies = UdpProxyTable::<CountingReactor>::new(8);
        let proxy = proxy(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 53));
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut context = UdpSendContext {
            proxy_timeout: Duration::from_secs(30),
            reactor: &mut reactor,
            udp_socket_factory: &factory,
            clock: &SystemClock,
        };

        let result = with_drive(&mut budget, |drive| {
            proxies.send(proxy, b"small", false, &mut context, drive)
        })
        .0;
        let Err(error) = result else {
            panic!("send failure should be reported");
        };

        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(stats.connects.get(), 1);
        assert_eq!(stats.registers.get(), 1);
        assert_eq!(stats.sends.get(), 1);
        assert_eq!(stats.reregisters.get(), 0);
        assert_eq!(stats.deregisters.get(), 1);
        assert!(proxies.by_key.get(&proxy).is_none());
    }

    async fn drive_udp<N>(
        runtime: &mut N,
        proxies: &mut UdpProxies<N::Reactor>,
    ) -> Result<Vec<UdpProxyEvent>, Box<dyn std::error::Error>>
    where
        N: NetworkRuntime,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut events = Vec::new();
        let mut readiness = Vec::new();
        loop {
            let mut budget = DriveBudget::event_loop(&crate::network::NetworkLimits::default());
            let (_result, _report) = with_drive(&mut budget, |drive| {
                proxies.drive_queued(&mut events, drive, runtime);
            });
            if !events.is_empty() {
                return Ok(events);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("timed out waiting for UDP proxy events".into());
            }
            runtime.reactor_mut().ready_into(&mut readiness, Some(Duration::ZERO))?;
            if readiness.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            let mut budget = DriveBudget::event_loop(&crate::network::NetworkLimits::default());
            let (_result, _report) = with_drive(&mut budget, |drive| {
                proxies.drive_ready(&readiness, &mut events, drive, runtime);
            });
            if !events.is_empty() {
                return Ok(events);
            }
        }
    }

    #[derive(Clone)]
    struct ErrorRecvWake;

    impl ReactorWake for ErrorRecvWake {
        fn wake(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct ErrorRecvTcpStream;

    impl std::io::Read for ErrorRecvTcpStream {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            unimplemented!("unused test TCP stream")
        }
    }

    impl std::io::Write for ErrorRecvTcpStream {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            unimplemented!("unused test TCP stream")
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl ReactorTcpStream for ErrorRecvTcpStream {
        fn connect(_addr: SocketAddr) -> std::io::Result<Self> {
            Ok(Self)
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

    struct ErrorRecvTcpListener;

    impl ReactorTcpListener for ErrorRecvTcpListener {
        type Stream = ErrorRecvTcpStream;

        fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
            Ok(Self)
        }

        fn accept(&self) -> std::io::Result<(Self::Stream, SocketAddr)> {
            unimplemented!("unused test TCP listener")
        }

        fn local_addr(&self) -> std::io::Result<SocketAddr> {
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        }
    }

    struct ErrorRecvUdpSocket;

    impl ReactorUdpSocket for ErrorRecvUdpSocket {
        fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
            Ok(Self)
        }

        fn from_std(_socket: std::net::UdpSocket) -> Self {
            Self
        }

        fn send(&self, bytes: &[u8]) -> std::io::Result<usize> {
            Ok(bytes.len())
        }

        fn recv(&self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("forced recv error"))
        }

        fn send_to(&self, bytes: &[u8], _target: SocketAddr) -> std::io::Result<usize> {
            Ok(bytes.len())
        }

        fn recv_from(&self, _buffer: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            Err(std::io::Error::other("forced recv error"))
        }

        fn local_addr(&self) -> std::io::Result<SocketAddr> {
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        }
    }

    struct ErrorRecvReactor;

    impl ReactorBackend for ErrorRecvReactor {
        type Wake = ErrorRecvWake;
        type TcpListener = ErrorRecvTcpListener;
        type TcpStream = ErrorRecvTcpStream;
        type UdpSocket = ErrorRecvUdpSocket;

        fn wake_handle(&self) -> Self::Wake {
            ErrorRecvWake
        }

        fn register_tcp_listener(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpListener,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn register_tcp_stream(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpStream,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn register_udp_socket(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn reregister_tcp_stream(
            &self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpStream,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn reregister_udp_socket(
            &self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> std::io::Result<()> {
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
            _item: ReactorItemId,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn deregister_udp_socket(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
        ) -> std::io::Result<()> {
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

    #[derive(Clone, Default)]
    struct CountingUdpStats {
        connects: Rc<Cell<usize>>,
        registers: Rc<Cell<usize>>,
        reregisters: Rc<Cell<usize>>,
        deregisters: Rc<Cell<usize>>,
        sends: Rc<Cell<usize>>,
        fail_reregister: Rc<Cell<bool>>,
        fail_send: Rc<Cell<bool>>,
    }

    #[derive(Clone)]
    struct CountingUdpFactory {
        stats: CountingUdpStats,
    }

    impl UdpSocketFactory<CountingReactor> for CountingUdpFactory {
        fn connect_udp_socket(&self, _dst: SocketAddr) -> std::io::Result<CountingUdpSocket> {
            self.stats.connects.set(self.stats.connects.get() + 1);
            Ok(CountingUdpSocket {
                stats: self.stats.clone(),
            })
        }
    }

    struct CountingUdpSocket {
        stats: CountingUdpStats,
    }

    impl ReactorUdpSocket for CountingUdpSocket {
        fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
            Ok(Self {
                stats: CountingUdpStats::default(),
            })
        }

        fn from_std(_socket: std::net::UdpSocket) -> Self {
            Self {
                stats: CountingUdpStats::default(),
            }
        }

        fn send(&self, bytes: &[u8]) -> std::io::Result<usize> {
            self.stats.sends.set(self.stats.sends.get() + 1);
            if self.stats.fail_send.get() {
                return Err(std::io::Error::other("forced send failure"));
            }
            Ok(bytes.len())
        }

        fn recv(&self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }

        fn send_to(&self, bytes: &[u8], _target: SocketAddr) -> std::io::Result<usize> {
            self.stats.sends.set(self.stats.sends.get() + 1);
            if self.stats.fail_send.get() {
                return Err(std::io::Error::other("forced send failure"));
            }
            Ok(bytes.len())
        }

        fn recv_from(&self, _buffer: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }

        fn local_addr(&self) -> std::io::Result<SocketAddr> {
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        }
    }

    struct CountingReactor {
        stats: CountingUdpStats,
    }

    impl ReactorBackend for CountingReactor {
        type Wake = ErrorRecvWake;
        type TcpListener = ErrorRecvTcpListener;
        type TcpStream = ErrorRecvTcpStream;
        type UdpSocket = CountingUdpSocket;

        fn wake_handle(&self) -> Self::Wake {
            ErrorRecvWake
        }

        fn register_tcp_listener(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpListener,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn register_tcp_stream(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpStream,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn register_udp_socket(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> std::io::Result<()> {
            self.stats.registers.set(self.stats.registers.get() + 1);
            Ok(())
        }

        fn reregister_tcp_stream(
            &self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpStream,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn reregister_udp_socket(
            &self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> std::io::Result<()> {
            self.stats.reregisters.set(self.stats.reregisters.get() + 1);
            if self.stats.fail_reregister.get() {
                return Err(std::io::Error::other("forced reregister failure"));
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
            _item: ReactorItemId,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn deregister_udp_socket(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
        ) -> std::io::Result<()> {
            self.stats.deregisters.set(self.stats.deregisters.get() + 1);
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
