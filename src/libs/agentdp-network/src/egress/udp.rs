use std::collections::VecDeque;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use crate::application;
use crate::buffers::{BufferPool, ByteBuf};
use crate::clock::NetworkClock;
use crate::connectors::udp::UdpSocketFactory;
use crate::drive::DriveBudget;
use crate::network::UdpProxyKey;
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorInterest, ReactorReady, ReactorUdpSocket};
use crate::runtime::NetworkRuntime;
use agentdp_ds::fixed_table::FixedTable;

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
    buffer: Vec<u8>,
    buffers: BufferPool,
    proxy_timeout: Duration,
    sends: VecDeque<UdpSend>,
    pending: VecDeque<UdpProxyEvent>,
}

impl<R> UdpProxies<R>
where
    R: ReactorBackend,
{
    pub(crate) fn new(buffers: &BufferPool) -> Self {
        Self {
            proxies: UdpProxyTable::new(buffers.limits().udp_proxy_limit),
            buffer: vec![0; buffers.limits().udp_datagram_buffer_capacity],
            buffers: buffers.clone(),
            proxy_timeout: buffers.limits().udp_proxy_timeout,
            sends: VecDeque::new(),
            pending: VecDeque::new(),
        }
    }

    pub(crate) fn send(&mut self, proxy: UdpProxyKey, bytes: ByteBuf, is_dns: bool) {
        self.sends.push_back(UdpSend { proxy, bytes, is_dns });
    }

    pub(crate) fn shutdown(&mut self, runtime: &mut impl NetworkRuntime<Reactor = R>) {
        self.sends.clear();
        self.pending.clear();
        self.proxies.shutdown(runtime.reactor_mut());
    }

    pub(crate) fn next_expiry(&self) -> Option<Instant> {
        self.proxies.next_expiry()
    }

    pub(crate) fn expire_due(
        &mut self,
        events: &mut Vec<UdpProxyEvent>,
        budget: &mut DriveBudget,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        let clock = runtime.clock().clone();
        let start_len = events.len();
        while budget.step() && budget.can_continue() {
            let Some(event) = self.expire_next(runtime.reactor_mut(), &clock) else {
                break;
            };
            push_event(events, budget, event);
        }
        events.len() > start_len
    }

    pub(crate) fn drive_queued(
        &mut self,
        events: &mut Vec<UdpProxyEvent>,
        budget: &mut DriveBudget,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        self.drive(events, budget, runtime, &[])
    }

    pub(crate) fn drive_ready(
        &mut self,
        readiness: &[ReactorReady],
        events: &mut Vec<UdpProxyEvent>,
        budget: &mut DriveBudget,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> bool {
        self.drive(events, budget, runtime, readiness)
    }

    fn drive(
        &mut self,
        events: &mut Vec<UdpProxyEvent>,
        budget: &mut DriveBudget,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
        readiness: &[ReactorReady],
    ) -> bool {
        let clock = runtime.clock().clone();
        let start_len = events.len();
        let mut made_progress = false;
        loop {
            if !budget.step() || !budget.can_continue() {
                break;
            }
            if let Some(event) = self.pending.pop_front() {
                push_event(events, budget, event);
                continue;
            }
            let udp_socket_factory = runtime.udp_socket_factory().clone();
            match self.drain_sends(events, budget, runtime.reactor_mut(), &udp_socket_factory, &clock) {
                SendDrain::Drained {
                    made_progress: progress,
                } => made_progress |= progress,
                SendDrain::Blocked {
                    made_progress: progress,
                } => {
                    made_progress |= progress;
                    break;
                }
                SendDrain::Error => return made_progress || events.len() > start_len,
            }
            if self.drain_ready(
                readiness,
                events,
                budget,
                runtime.reactor_mut(),
                &udp_socket_factory,
                &clock,
            ) {
                made_progress = true;
                continue;
            }
            break;
        }
        made_progress || events.len() > start_len
    }

    fn drain_sends(
        &mut self,
        events: &mut Vec<UdpProxyEvent>,
        budget: &mut DriveBudget,
        reactor: &mut R,
        udp_socket_factory: &impl UdpSocketFactory<R>,
        clock: &impl NetworkClock,
    ) -> SendDrain {
        let mut made_progress = false;
        let mut context = UdpSendContext {
            proxy_timeout: self.proxy_timeout,
            reactor,
            udp_socket_factory,
            clock,
        };
        while let Some(send) = self.sends.front() {
            match self
                .proxies
                .send(send.proxy, send.bytes.as_slice(), send.is_dns, &mut context)
            {
                Ok(SendResult::Sent) => {
                    made_progress = true;
                    self.sends.pop_front();
                }
                Ok(SendResult::Blocked) => return SendDrain::Blocked { made_progress },
                Err(error) => {
                    self.sends.pop_front();
                    push_event(
                        events,
                        budget,
                        UdpProxyEvent::Error {
                            message: error.to_string(),
                        },
                    );
                    return SendDrain::Error;
                }
            }
        }
        SendDrain::Drained { made_progress }
    }

    fn drain_ready(
        &mut self,
        readiness: &[ReactorReady],
        events: &mut Vec<UdpProxyEvent>,
        budget: &mut DriveBudget,
        reactor: &mut R,
        udp_socket_factory: &impl UdpSocketFactory<R>,
        clock: &impl NetworkClock,
    ) -> bool {
        let mut made_progress = false;
        for ready in readiness {
            if !budget.can_continue() {
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
            let ReactorItemId::UdpProxy { proxy } = item else {
                continue;
            };
            if writable {
                let start_len = events.len();
                match self.drain_sends(events, budget, reactor, udp_socket_factory, clock) {
                    SendDrain::Drained {
                        made_progress: progress,
                    }
                    | SendDrain::Blocked {
                        made_progress: progress,
                    } => made_progress |= progress || events.len() > start_len,
                    SendDrain::Error => made_progress = true,
                }
            }
            if readable {
                while budget.can_continue() {
                    let Some(event) = self.proxies.try_recv(
                        proxy,
                        &mut self.buffer,
                        &self.buffers,
                        self.proxy_timeout,
                        reactor,
                        clock,
                    ) else {
                        break;
                    };
                    self.push_proxy_event(event, events, budget);
                    made_progress = true;
                }
            }
        }
        made_progress
    }

    fn expire_next(&mut self, reactor: &mut R, clock: &impl NetworkClock) -> Option<UdpProxyEvent> {
        self.proxies
            .expire_next(reactor, clock)
            .map(|_proxy| UdpProxyEvent::Closed)
    }

    fn push_proxy_event(
        &mut self,
        event: UdpProxyIoEvent,
        events: &mut Vec<UdpProxyEvent>,
        budget: &mut DriveBudget,
    ) -> bool {
        match event {
            UdpProxyIoEvent::Bytes { proxy, bytes, is_dns } => {
                self.push_bytes_event(proxy, bytes, is_dns, events, budget)
            }
            UdpProxyIoEvent::Error { message } => {
                push_event(events, budget, UdpProxyEvent::Error { message });
                false
            }
        }
    }

    fn push_bytes_event(
        &mut self,
        proxy: UdpProxyKey,
        bytes: ByteBuf,
        is_dns: bool,
        events: &mut Vec<UdpProxyEvent>,
        budget: &mut DriveBudget,
    ) -> bool {
        if is_dns && let Some(question) = application::dns_question(bytes.as_slice()) {
            let records = application::dns_address_records(bytes.as_slice());
            push_event(
                events,
                budget,
                UdpProxyEvent::DnsResolved {
                    host: question.host,
                    addresses: records.addresses,
                    ttl: records.ttl,
                },
            );
            let bytes_event = UdpProxyEvent::Bytes { proxy, bytes, is_dns };
            if budget.can_continue() {
                push_event(events, budget, bytes_event);
            } else {
                self.pending.push_front(bytes_event);
            }
            return true;
        }
        push_event(events, budget, UdpProxyEvent::Bytes { proxy, bytes, is_dns });
        false
    }
}

enum SendDrain {
    Drained { made_progress: bool },
    Blocked { made_progress: bool },
    Error,
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
    ) -> std::io::Result<SendResult> {
        if self.by_key.get(&key).is_some() {
            return self.send_existing(
                key,
                bytes,
                is_dns,
                context.proxy_timeout,
                &mut *context.reactor,
                context.clock,
            );
        }
        if self.by_key.len() >= self.by_key.capacity() {
            return Err(std::io::Error::other(format!(
                "UDP proxy limit {} exceeded",
                self.by_key.capacity()
            )));
        }
        let mut socket = context.udp_socket_factory.connect_udp_socket(key.host_dst)?;
        let item = ReactorItemId::UdpProxy { proxy: key };
        context
            .reactor
            .register_udp_socket(&mut socket, item, ReactorInterest::Readable)?;
        let send_result = match socket.send(bytes) {
            Ok(_sent) => SendResult::Sent,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                context
                    .reactor
                    .reregister_udp_socket(&mut socket, item, ReactorInterest::ReadWrite)?;
                SendResult::Blocked
            }
            Err(error) => return Err(error),
        };
        let _replaced = self.by_key.insert(
            key,
            UdpProxySocket {
                socket,
                wants_write: matches!(send_result, SendResult::Blocked),
                is_dns,
                expires: context.clock.now() + context.proxy_timeout,
            },
        );
        Ok(send_result)
    }

    fn send_existing(
        &mut self,
        key: UdpProxyKey,
        bytes: &[u8],
        is_dns: bool,
        proxy_timeout: Duration,
        reactor: &mut R,
        clock: &impl NetworkClock,
    ) -> std::io::Result<SendResult> {
        let Some(proxy) = self.by_key.get_mut(&key) else {
            return Ok(SendResult::Blocked);
        };
        proxy.is_dns |= is_dns;
        proxy.expires = clock.now() + proxy_timeout;
        match proxy.socket.send(bytes) {
            Ok(_sent) => {
                if proxy.wants_write {
                    proxy.wants_write = false;
                    reactor.reregister_udp_socket(
                        &mut proxy.socket,
                        ReactorItemId::UdpProxy { proxy: key },
                        ReactorInterest::Readable,
                    )?;
                }
                Ok(SendResult::Sent)
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if !proxy.wants_write {
                    proxy.wants_write = true;
                    reactor.reregister_udp_socket(
                        &mut proxy.socket,
                        ReactorItemId::UdpProxy { proxy: key },
                        ReactorInterest::ReadWrite,
                    )?;
                }
                Ok(SendResult::Blocked)
            }
            Err(error) => {
                self.remove(key, reactor);
                Err(error)
            }
        }
    }

    fn try_recv(
        &mut self,
        proxy: UdpProxyKey,
        buffer: &mut [u8],
        buffers: &BufferPool,
        proxy_timeout: Duration,
        reactor: &mut R,
        clock: &impl NetworkClock,
    ) -> Option<UdpProxyIoEvent> {
        let udp = self.by_key.get_mut(&proxy)?;
        match udp.socket.recv(buffer) {
            Ok(len) => {
                udp.expires = clock.now() + proxy_timeout;
                let is_dns = udp.is_dns;
                let Ok(mut bytes) = buffers.try_byte_with_capacity(len) else {
                    return None;
                };
                bytes.extend_from_slice(&buffer[..len]);
                Some(UdpProxyIoEvent::Bytes { proxy, bytes, is_dns })
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(error) => {
                let message = error.to_string();
                self.remove(proxy, reactor);
                Some(UdpProxyIoEvent::Error { message })
            }
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
        let _deregistered = reactor.deregister_udp_socket(&mut proxy.socket, ReactorItemId::UdpProxy { proxy: key });
    }

    fn shutdown(&mut self, reactor: &mut R) {
        let mut keys = Vec::with_capacity(self.by_key.len());
        self.by_key.keys_into(&mut keys);
        for key in keys {
            self.remove(key, reactor);
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendResult {
    Sent,
    Blocked,
}

fn push_event(events: &mut Vec<UdpProxyEvent>, budget: &mut DriveBudget, event: UdpProxyEvent) {
    let bytes = match &event {
        UdpProxyEvent::Bytes { bytes, .. } => bytes.len(),
        UdpProxyEvent::DnsResolved { .. } | UdpProxyEvent::Closed | UdpProxyEvent::Error { .. } => 1,
    };
    if budget.event(bytes) {
        events.push(event);
    }
}

#[derive(Debug)]
struct UdpProxySocket<R: ReactorBackend> {
    socket: R::UdpSocket,
    wants_write: bool,
    is_dns: bool,
    expires: Instant,
}

enum UdpProxyIoEvent {
    Bytes {
        proxy: UdpProxyKey,
        bytes: ByteBuf,
        is_dns: bool,
    },
    Error {
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::buffers::BufferPool;
    use crate::drive::DriveBudget;
    use crate::network::NetworkLimits;
    use crate::reactor::{ReactorBackend, default_backend};
    use crate::runtime::NetworkRuntime;
    use crate::test_support::unit::{dns_a_response, dns_query, runtime_context};

    use super::{UdpProxies, UdpProxyEvent, UdpProxyKey};

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
            UdpProxyEvent::DnsResolved { host, addresses, .. } => {
                assert_eq!(host, "allowed.test");
                assert_eq!(addresses, vec![IpAddr::V4(Ipv4Addr::new(10, 73, 0, 42))]);
            }
            _ => return Err("expected DNS attribution event".into()),
        }
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
            UdpProxyEvent::DnsResolved { host, addresses, .. } => {
                assert_eq!(host, "fast.test");
                assert_eq!(addresses, vec![IpAddr::V4(Ipv4Addr::new(10, 73, 0, 12))]);
            }
            _ => return Err("expected fast DNS attribution before delayed slow response".into()),
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
            proxies.drive_queued(&mut events, &mut budget, runtime);
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
            proxies.drive_ready(&readiness, &mut events, &mut budget, runtime);
            if !events.is_empty() {
                return Ok(events);
            }
        }
    }
}
