use std::cell::Cell;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;

use agentdp_crypto::test_support::{connected_tls_pair, feed_server_ciphertext};
use agentdp_crypto::{
    CertificateAuthority, CertificateAuthorityPem, CertificateValidity, TlsClientConfig, TlsPlaintextWrite,
    TlsServerConfig, TlsServerSession,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::RuntimeSecrets;
use crate::application::Http1Filter;
use crate::buffers::BufferPool;
use crate::buffers::WriteQueue;
use crate::clock::SystemClock;
use crate::connectors::tcp::TcpConnector;
use crate::connectors::udp::UdpSocketFactory;
use crate::drive::{DriveBudget, DriveReport, DriveRunnable, DriveTurn};
use crate::guest::{
    ConnectStatus, FrameRead, FrameWrite, GuestFrameSession, GuestFrameTransport, GuestIoSource, TransportError,
};
use crate::network::{
    ApplicationPolicy, BlockReason, EgressDecision, NetworkLimits, TcpEgressPolicy, TcpEgressRoute, TcpProxyId,
    TlsEgressPolicy,
};
use crate::policy::Authority;
use crate::reactor::{ReactorBackend, ReactorInterest, ReactorReady, default_backend};
use crate::reactor::{ReactorItemId, ReactorTcpListener, ReactorTcpStream, ReactorUdpSocket, ReactorWake};
use crate::runtime::{NetworkRuntime, RuntimeContext};
use crate::test_support::unit::{dns_a_response, dns_query, runtime_context, tcp_dns_frame};

use super::plain::{PlainRoute, PlainTcpProxy, PlainTcpProxyState};
use super::tls::{
    QueueStep, RelayStep, TlsHttp1Proxy, TlsProxyPoll, TlsRoute, TlsTcpProxy, TlsTcpProxyState, should_bypass_tls,
    tls_route,
};
use super::tls_upstream::{TlsDrive, TlsUpstream};
use super::{TcpProxy, TcpProxyEvent, TcpProxyPermit, TcpProxyPoll};

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

fn counting_runtime(
    stats: CountingStreamStats,
) -> RuntimeContext<CountingTransport, CountingReactor, SystemClock, CountingTcpConnector, CountingUdpSocketFactory> {
    RuntimeContext::new(
        CountingTransport,
        CountingReactor,
        SystemClock,
        CountingTcpConnector { stats },
        CountingUdpSocketFactory,
    )
}

#[derive(Debug, Clone, Default)]
struct CountingStreamStats {
    reads: Rc<Cell<usize>>,
    writes: Rc<Cell<usize>>,
}

impl CountingStreamStats {
    fn reads(&self) -> usize {
        self.reads.get()
    }

    fn writes(&self) -> usize {
        self.writes.get()
    }
}

#[derive(Debug, Clone, Copy)]
struct CountingTransport;

struct CountingSession;

impl GuestFrameTransport for CountingTransport {
    type Session = CountingSession;

    fn try_connect(&mut self) -> Result<ConnectStatus<Self::Session>, TransportError> {
        Err(TransportError::operation(
            "unused counting transport",
            "counting TCP tests do not connect guest transport",
        ))
    }

    fn cleanup(self) -> Result<(), TransportError> {
        Ok(())
    }

    fn describe(&self) -> String {
        "unused counting transport".to_owned()
    }
}

impl GuestFrameSession for CountingSession {
    fn io_source(&mut self) -> GuestIoSource<'_> {
        unreachable!("counting transport never creates sessions")
    }

    fn read_frame_into(&mut self, _frame: &mut crate::buffers::FrameBuf) -> Result<FrameRead, TransportError> {
        Ok(FrameRead::Blocked)
    }

    fn write_frame(&mut self, _frame: &[u8]) -> Result<FrameWrite, TransportError> {
        Ok(FrameWrite::Blocked)
    }

    fn shutdown_write(&mut self) -> Result<(), TransportError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct CountingTcpConnector {
    stats: CountingStreamStats,
}

impl TcpConnector<CountingReactor> for CountingTcpConnector {
    fn connect_tcp_stream(&self, _dst: SocketAddr) -> io::Result<CountingTcpStream> {
        Ok(CountingTcpStream {
            stats: self.stats.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CountingUdpSocketFactory;

impl UdpSocketFactory<CountingReactor> for CountingUdpSocketFactory {
    fn connect_udp_socket(&self, _dst: SocketAddr) -> io::Result<CountingUdpSocket> {
        Ok(CountingUdpSocket)
    }
}

#[derive(Debug, Clone, Copy)]
struct CountingWake;

impl ReactorWake for CountingWake {
    fn wake(&self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct CountingReactor;

impl ReactorBackend for CountingReactor {
    type Wake = CountingWake;
    type TcpListener = CountingTcpListener;
    type TcpStream = CountingTcpStream;
    type UdpSocket = CountingUdpSocket;

    fn wake_handle(&self) -> Self::Wake {
        CountingWake
    }

    fn register_tcp_listener(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: &mut Self::TcpListener,
        _item: ReactorItemId,
        _interest: ReactorInterest,
    ) -> io::Result<()> {
        Ok(())
    }

    fn register_tcp_stream(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: &mut Self::TcpStream,
        _item: ReactorItemId,
        _interest: ReactorInterest,
    ) -> io::Result<()> {
        Ok(())
    }

    fn register_udp_socket(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: &mut Self::UdpSocket,
        _item: ReactorItemId,
        _interest: ReactorInterest,
    ) -> io::Result<()> {
        Ok(())
    }

    fn reregister_tcp_stream(
        &self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: &mut Self::TcpStream,
        _item: ReactorItemId,
        _interest: ReactorInterest,
    ) -> io::Result<()> {
        Ok(())
    }

    fn reregister_udp_socket(
        &self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: &mut Self::UdpSocket,
        _item: ReactorItemId,
        _interest: ReactorInterest,
    ) -> io::Result<()> {
        Ok(())
    }

    fn deregister_tcp_listener(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: &mut Self::TcpListener,
        _item: ReactorItemId,
    ) -> io::Result<()> {
        Ok(())
    }

    fn deregister_tcp_stream(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: &mut Self::TcpStream,
        _item: ReactorItemId,
    ) -> io::Result<()> {
        Ok(())
    }

    fn deregister_udp_socket(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: &mut Self::UdpSocket,
        _item: ReactorItemId,
    ) -> io::Result<()> {
        Ok(())
    }

    fn register_guest_source(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: GuestIoSource<'_>,
        _item: ReactorItemId,
    ) -> Result<(), TransportError> {
        Ok(())
    }

    fn reregister_guest_source(
        &self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: GuestIoSource<'_>,
        _item: ReactorItemId,
        _writable: bool,
    ) -> Result<(), TransportError> {
        Ok(())
    }

    fn deregister_guest_source(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        _source: GuestIoSource<'_>,
        _item: ReactorItemId,
    ) -> Result<(), TransportError> {
        Ok(())
    }

    fn ready_into(&mut self, _output: &mut Vec<ReactorReady>, _timeout: Option<Duration>) -> io::Result<()> {
        Ok(())
    }
}

struct CountingTcpStream {
    stats: CountingStreamStats,
}

impl io::Read for CountingTcpStream {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        self.stats.reads.set(self.stats.reads.get().saturating_add(1));
        Err(io::ErrorKind::WouldBlock.into())
    }
}

impl io::Write for CountingTcpStream {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        self.stats.writes.set(self.stats.writes.get().saturating_add(1));
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ReactorTcpStream for CountingTcpStream {
    fn connect(_addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            stats: CountingStreamStats::default(),
        })
    }

    fn set_nodelay(&self, _nodelay: bool) -> io::Result<()> {
        Ok(())
    }

    fn take_error(&self) -> io::Result<Option<io::Error>> {
        Ok(None)
    }

    fn shutdown_write(&self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct CountingTcpListener;

impl ReactorTcpListener for CountingTcpListener {
    type Stream = CountingTcpStream;

    fn bind(_addr: SocketAddr) -> io::Result<Self> {
        Ok(Self)
    }

    fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(test_dst())
    }
}

#[derive(Debug)]
struct CountingUdpSocket;

impl ReactorUdpSocket for CountingUdpSocket {
    fn bind(_addr: SocketAddr) -> io::Result<Self> {
        Ok(Self)
    }

    fn from_std(_socket: std::net::UdpSocket) -> Self {
        Self
    }

    fn send(&self, _bytes: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn recv(&self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn send_to(&self, _bytes: &[u8], _target: SocketAddr) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn recv_from(&self, _buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(test_dst())
    }
}

#[test]
fn plain_tcp_write_would_block_does_not_retry_on_read_readiness_only() {
    let buffers = test_buffers();
    let stats = CountingStreamStats::default();
    let mut runtime = counting_runtime(stats.clone());
    let proxy_id = TcpProxyId(7101);
    let dst = test_dst();
    let mut proxy = TcpProxy::connecting(
        proxy_id,
        dst,
        dst,
        TcpEgressRoute::Plain(plain_policy(ApplicationPolicy::Raw, false)),
        &buffers,
        &mut runtime,
    )
    .expect("proxy should connect");
    proxy.mark_reactor_ready(false, true);
    proxy.write(io_buf(&buffers, b"request"));

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, _report) = with_drive(&mut budget, |drive| {
        proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::WRITE_UPSTREAM)
    });
    assert!(matches!(poll, TcpProxyPoll::Pending));
    assert_eq!(stats.writes(), 0);

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, report) = with_drive(&mut budget, |drive| {
        proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::WRITE_UPSTREAM)
    });
    assert!(matches!(poll, TcpProxyPoll::Pending));
    assert!(report.wait().contains(crate::drive::DriveWait::REACTOR_WRITE));
    assert_eq!(stats.writes(), 1);
    assert!(!proxy.io().can_write(), "write WouldBlock must clear write readiness");

    proxy.mark_reactor_ready(true, false);
    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (_poll, _report) = with_drive(&mut budget, |drive| {
        proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::ALL)
    });
    assert_eq!(stats.writes(), 1, "read readiness must not admit another write syscall");
}

#[test]
fn plain_tcp_read_would_block_does_not_retry_on_write_readiness_only() {
    let buffers = test_buffers();
    let stats = CountingStreamStats::default();
    let mut runtime = counting_runtime(stats.clone());
    let proxy_id = TcpProxyId(7102);
    let dst = test_dst();
    let mut proxy = TcpProxy::connecting(
        proxy_id,
        dst,
        dst,
        TcpEgressRoute::Plain(plain_policy(ApplicationPolicy::Raw, false)),
        &buffers,
        &mut runtime,
    )
    .expect("proxy should connect");
    proxy.mark_reactor_ready(true, false);

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, report) = with_drive(&mut budget, |drive| {
        proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::READ_UPSTREAM)
    });
    assert!(matches!(poll, TcpProxyPoll::Pending));
    assert!(report.wait().contains(crate::drive::DriveWait::REACTOR_READ));
    assert_eq!(stats.reads(), 1);
    assert!(!proxy.io().can_read(), "read WouldBlock must clear read readiness");

    proxy.mark_reactor_ready(false, true);
    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (_poll, _report) = with_drive(&mut budget, |drive| {
        proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::ALL)
    });
    assert_eq!(stats.reads(), 1, "write readiness must not admit another read syscall");
}

#[test]
fn plain_tcp_read_blocked_on_local_buffer_stays_read_runnable() {
    let buffers = BufferPool::new(NetworkLimits {
        tcp_byte_pool_capacity: 0,
        ..NetworkLimits::default()
    });
    buffers.prewarm_instance_network();
    let stats = CountingStreamStats::default();
    let mut runtime = counting_runtime(stats);
    let proxy_id = TcpProxyId(7103);
    let dst = test_dst();
    let mut proxy = TcpProxy::connecting(
        proxy_id,
        dst,
        dst,
        TcpEgressRoute::Plain(plain_policy(ApplicationPolicy::Raw, false)),
        &buffers,
        &mut runtime,
    )
    .expect("proxy should connect");
    proxy.mark_reactor_ready(true, false);

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, report) = with_drive(&mut budget, |drive| {
        proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::READ_UPSTREAM)
    });

    assert!(matches!(poll, TcpProxyPoll::Pending));
    assert!(report.wait().contains(crate::drive::DriveWait::LOCAL_BUFFER_CAPACITY));
    assert_eq!(report.runnable(), DriveRunnable::READ_UPSTREAM);
}

#[tokio::test(flavor = "current_thread")]
async fn tcp_dns_response_emits_attribution_and_response_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let buffers = test_buffers();
    let server = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let (stream, _peer) = server.accept().await?;
        let mut query = [0_u8; 256];
        stream.readable().await?;
        let _read = stream.try_read(&mut query)?;
        let response = tcp_dns_frame(&dns_a_response(
            0x5101,
            "allowed.test",
            Ipv4Addr::new(10, 73, 0, 42),
            60,
        ));
        stream.writable().await?;
        let _written = stream.try_write(&response)?;
        Ok::<_, std::io::Error>(())
    });
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let proxy_id = TcpProxyId(51);
    let mut proxy = TcpProxy::connecting(
        proxy_id,
        upstream,
        upstream,
        TcpEgressRoute::Dns { upstream },
        &buffers,
        &mut runtime,
    )?;
    let query = io_buf(&buffers, &tcp_dns_frame(&dns_query(0x5101, "allowed.test", 1)));
    proxy.write(query);

    let poll = drive_tcp(&mut runtime, &buffers, &mut proxy).await?.remove(0);
    match poll {
        TcpProxyPoll::Event(TcpProxyEvent::DnsResolved { host, addresses, .. }) => {
            assert_eq!(host, "allowed.test");
            assert_eq!(addresses, vec![IpAddr::V4(Ipv4Addr::new(10, 73, 0, 42))]);
        }
        _ => return Err("expected DNS attribution event".into()),
    }

    let poll = drive_tcp(&mut runtime, &buffers, &mut proxy).await?.remove(0);
    match poll {
        TcpProxyPoll::Bytes(bytes) => {
            assert!(bytes.as_slice().starts_with(&0x002e_u16.to_be_bytes()));
        }
        _ => return Err("expected DNS response bytes event".into()),
    }
    server_task.await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn plain_tcp_pending_read_bytes_do_not_block_allowed_writes() -> Result<(), Box<dyn std::error::Error>> {
    let buffers = test_buffers();
    let server = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = server.local_addr()?;
    let first_query = tcp_dns_frame(&dns_query(0x5102, "allowed.test", 1));
    let second_query = tcp_dns_frame(&dns_query(0x5103, "allowed.test", 1));
    let first_query_server = first_query.clone();
    let second_len = second_query.len();
    let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let (mut stream, _peer) = server.accept().await?;
        let mut first = vec![0_u8; first_query_server.len()];
        stream.read_exact(&mut first).await?;
        let response = tcp_dns_frame(&dns_a_response(
            0x5102,
            "allowed.test",
            Ipv4Addr::new(10, 73, 0, 42),
            60,
        ));
        stream.write_all(&response).await?;
        let mut second = vec![0_u8; second_len];
        stream.read_exact(&mut second).await?;
        let _sent = observed_tx.send(second);
        Ok::<_, std::io::Error>(())
    });
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let proxy_id = TcpProxyId(5102);
    let mut proxy = TcpProxy::connecting(
        proxy_id,
        upstream,
        upstream,
        TcpEgressRoute::Dns { upstream },
        &buffers,
        &mut runtime,
    )?;
    proxy.write(io_buf(&buffers, &first_query));

    let poll = drive_tcp(&mut runtime, &buffers, &mut proxy).await?.remove(0);
    assert!(matches!(poll, TcpProxyPoll::Event(TcpProxyEvent::DnsResolved { .. })));
    proxy.write(io_buf(&buffers, &second_query));
    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    for _ in 0..16 {
        let (poll, _report) = with_drive(&mut budget, |drive| {
            proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::WRITE_UPSTREAM)
        });
        match poll {
            TcpProxyPoll::Pending => {}
            TcpProxyPoll::Event(event) => panic!("unexpected TCP event while read bytes are blocked: {event:?}"),
            TcpProxyPoll::Bytes(_) => panic!("pending read bytes escaped while READ_UPSTREAM was disallowed"),
        }
        if !budget.can_continue() {
            budget = DriveBudget::event_loop(&NetworkLimits::default());
        }
    }

    let observed = tokio::time::timeout(Duration::from_secs(1), observed_rx).await??;
    assert_eq!(observed, second_query);
    server_task.await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn plain_tcp_egress_drains_queued_writes_before_waiting_for_reads() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let server_task = tokio::spawn(async move {
        let (mut stream, _peer) = listener.accept().await?;
        let mut observed = [0_u8; 11];
        stream.read_exact(&mut observed).await?;
        assert_eq!(&observed, b"firstsecond");
        stream.write_all(b"inbound").await?;
        Ok::<_, std::io::Error>(())
    });
    let buffers = test_buffers();
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let proxy_id = TcpProxyId(52);
    let mut proxy = TcpProxy::connecting(
        proxy_id,
        upstream,
        upstream,
        TcpEgressRoute::Plain(plain_policy(ApplicationPolicy::Raw, false)),
        &buffers,
        &mut runtime,
    )?;
    proxy.write(io_buf(&buffers, b"first"));
    proxy.write(io_buf(&buffers, b"second"));

    let poll = tokio::time::timeout(Duration::from_secs(1), drive_tcp(&mut runtime, &buffers, &mut proxy))
        .await??
        .remove(0);

    match poll {
        TcpProxyPoll::Bytes(bytes) => {
            assert_eq!(bytes.as_slice(), b"inbound");
        }
        _ => return Err("expected TCP egress bytes after queued writes drained".into()),
    }
    server_task.await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn plain_tcp_allowed_work_blocks_upstream_read_but_allows_upstream_write()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let server_task = tokio::spawn(async move {
        let (mut stream, _peer) = listener.accept().await?;
        let mut observed = [0_u8; 8];
        stream.read_exact(&mut observed).await?;
        assert_eq!(&observed, b"outbound");
        stream.write_all(b"inbound").await?;
        Ok::<_, std::io::Error>(())
    });
    let buffers = test_buffers();
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let proxy_id = TcpProxyId(59);
    let mut proxy = TcpProxy::connecting(
        proxy_id,
        upstream,
        upstream,
        TcpEgressRoute::Plain(plain_policy(ApplicationPolicy::Raw, false)),
        &buffers,
        &mut runtime,
    )?;
    proxy.write(io_buf(&buffers, b"outbound"));
    wait_for_tcp_ready(&mut runtime, &mut proxy).await?;
    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    for _ in 0..8 {
        let (poll, _report) = with_drive(&mut budget, |drive| {
            proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::WRITE_UPSTREAM)
        });
        match poll {
            TcpProxyPoll::Pending => {}
            TcpProxyPoll::Event(event) => panic!("unexpected TCP event while upstream read is blocked: {event:?}"),
            TcpProxyPoll::Bytes(_) => panic!("upstream read produced guest bytes while READ_UPSTREAM was disallowed"),
        }
        if !budget.can_continue() {
            budget = DriveBudget::event_loop(&NetworkLimits::default());
        }
    }
    server_task.await??;
    let mut readiness = Vec::new();
    runtime
        .reactor_mut()
        .ready_into(&mut readiness, Some(Duration::from_millis(20)))?;
    assert!(
        !readiness.iter().any(|ready| matches!(
            ready,
            ReactorReady::Io {
                item: ReactorItemId::TcpProxy { proxy },
                readable: true,
                ..
            } | ReactorReady::Io {
                item: ReactorItemId::TcpProxy { proxy },
                writable: true,
                ..
            } if *proxy == proxy_id
        )),
        "proxy should not remain reactor-ready while READ_UPSTREAM is disallowed and no writes are pending"
    );

    let poll = tokio::time::timeout(
        Duration::from_secs(1),
        drive_tcp_with_io(&mut runtime, &buffers, &mut proxy),
    )
    .await??
    .remove(0);
    match poll {
        TcpProxyPoll::Bytes(bytes) => assert_eq!(bytes.as_slice(), b"inbound"),
        _ => return Err("expected inbound bytes once READ_UPSTREAM is allowed".into()),
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn plain_tcp_upstream_read_is_bounded_by_drive_byte_budget() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let server_task = tokio::spawn(async move {
        let (mut stream, _peer) = listener.accept().await?;
        stream.write_all(b"0123456789abcdef").await?;
        Ok::<_, std::io::Error>(())
    });
    let buffers = test_buffers();
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let mut proxy = TcpProxy::connecting(
        TcpProxyId(60),
        upstream,
        upstream,
        TcpEgressRoute::Plain(plain_policy(ApplicationPolicy::Raw, false)),
        &buffers,
        &mut runtime,
    )?;
    wait_for_tcp_ready(&mut runtime, &mut proxy).await?;
    let mut open_budget = DriveBudget::event_loop(&NetworkLimits::default());
    let mut ignored = Vec::new();
    drive_test_proxy(&mut proxy, &buffers, &mut ignored, &mut open_budget, &mut runtime);
    let mut readiness = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        runtime.reactor_mut().ready_into(&mut readiness, Some(Duration::ZERO))?;
        for ready in &readiness {
            if let ReactorReady::Io {
                item: ReactorItemId::TcpProxy { proxy: ready_proxy },
                readable,
                writable,
            } = *ready
                && ready_proxy == TcpProxyId(60)
                && readable
            {
                proxy.mark_reactor_ready(readable, writable);
            }
        }
        if proxy.io().can_read() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for plain TCP readable readiness".into());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let mut budget = DriveBudget::event_loop(&NetworkLimits {
        drive_byte_budget: 4,
        ..NetworkLimits::default()
    });
    let mut polls = Vec::new();
    drive_test_proxy(&mut proxy, &buffers, &mut polls, &mut budget, &mut runtime);

    match polls.as_slice() {
        [TcpProxyPoll::Bytes(bytes)] => assert_eq!(bytes.len(), 4),
        _ => return Err("expected one budget-bounded TCP bytes poll".into()),
    }
    server_task.await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn plain_tcp_egress_waits_for_connect_readiness_before_opening() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let buffers = test_buffers();
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let mut proxy_id = PlainTcpProxy::connecting(
        TcpProxyId(53),
        upstream,
        upstream,
        None,
        PlainRoute::Policy(plain_policy(ApplicationPolicy::Raw, false)),
        &mut runtime,
    )?;

    proxy_id.write(io_buf(&buffers, b"queued before connect"));

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, turn_report) = with_drive(&mut budget, |drive| {
        proxy_id.drive(&buffers, runtime.reactor_mut(), drive, TcpProxyPermit::ALL)
    });
    assert!(matches!(poll, TcpProxyPoll::Pending));
    assert!(turn_report.wait().contains(crate::drive::DriveWait::REACTOR_READ));
    assert!(turn_report.wait().contains(crate::drive::DriveWait::REACTOR_WRITE));
    assert!(matches!(
        proxy_id.state,
        PlainTcpProxyState::Connecting {
            connect_ready: false,
            ..
        }
    ));
    Ok(())
}

async fn wait_for_tcp_ready<N>(
    runtime: &mut N,
    proxy: &mut TcpProxy<N::Reactor>,
) -> Result<(), Box<dyn std::error::Error>>
where
    N: NetworkRuntime,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut readiness = Vec::new();
    loop {
        runtime.reactor_mut().ready_into(&mut readiness, Some(Duration::ZERO))?;
        for ready in &readiness {
            if let ReactorReady::Io { readable, writable, .. } = *ready
                && (readable || writable)
            {
                proxy.mark_reactor_ready(readable, writable);
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for TCP readiness".into());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn tls_intercept_not_queued_after_upstream_write_finished() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let buffers = test_buffers();
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let policy = tls_policy(raw_decision());
    let mut server_tls = TlsUpstream::connect(
        TcpProxyId(54),
        upstream,
        "allowed.test",
        &policy.client_config,
        &mut runtime,
    )?;
    server_tls.mark_write_finished_for_test();

    let proxy_id = TlsTcpProxy {
        proxy: TcpProxyId(54),
        requested_dst: upstream,
        upstream_dst: upstream,
        authority: Some("allowed.test".to_owned()),
        pending: WriteQueue::new(),
        guest_write_finished: true,
        close_requested: false,
        state: TlsTcpProxyState::OpenIntercept(TlsHttp1Proxy {
            guest_tls: Box::new(TlsServerSession::accept(&server_config()?)?),
            server_tls,
            filter: Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers),
            tls_out: io_buf(&buffers, b""),
            server_buf: Some(io_buf(&buffers, b"")),
            server_buf_pending_offset: 0,
            server_buf_pending_len: 0,
            plaintext_buf: io_buf(&buffers, b""),
            substitute_buf: io_buf(&buffers, b""),
            server_output_offset: 0,
            server_pending: WriteQueue::new(),
            server_read_pending: false,
            guest_tls_closed: false,
            guest_close_notify_queued: false,
        }),
    };

    assert!(!proxy_id.has_local_work(true));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tls_guest_close_notify_keeps_upstream_finish_runnable() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let buffers = test_buffers();
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let policy = tls_policy(raw_decision());
    let server_tls = TlsUpstream::connect(
        TcpProxyId(56),
        upstream,
        "allowed.test",
        &policy.client_config,
        &mut runtime,
    )?;

    let mut proxy = TlsTcpProxy {
        proxy: TcpProxyId(56),
        requested_dst: upstream,
        upstream_dst: upstream,
        authority: Some("allowed.test".to_owned()),
        pending: WriteQueue::new(),
        guest_write_finished: false,
        close_requested: false,
        state: TlsTcpProxyState::OpenIntercept(TlsHttp1Proxy {
            guest_tls: Box::new(TlsServerSession::accept(&server_config()?)?),
            server_tls,
            filter: Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers),
            tls_out: io_buf(&buffers, b""),
            server_buf: None,
            server_buf_pending_offset: 0,
            server_buf_pending_len: 0,
            plaintext_buf: io_buf(&buffers, b""),
            substitute_buf: io_buf(&buffers, b""),
            server_output_offset: 0,
            server_pending: WriteQueue::new(),
            server_read_pending: false,
            guest_tls_closed: true,
            guest_close_notify_queued: false,
        }),
    };
    proxy.mark_reactor_ready(false, true);

    assert!(proxy.has_local_work(false));
    assert!(proxy.has_reactor_write_work());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tls_guest_output_local_work_requires_guest_send_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let buffers = test_buffers();
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let policy = tls_policy(raw_decision());
    let server_tls = TlsUpstream::connect(
        TcpProxyId(57),
        upstream,
        "allowed.test",
        &policy.client_config,
        &mut runtime,
    )?;
    let proxy = TlsTcpProxy {
        proxy: TcpProxyId(57),
        requested_dst: upstream,
        upstream_dst: upstream,
        authority: Some("allowed.test".to_owned()),
        pending: WriteQueue::new(),
        guest_write_finished: false,
        close_requested: false,
        state: TlsTcpProxyState::OpenIntercept(TlsHttp1Proxy {
            guest_tls: Box::new(TlsServerSession::accept(&server_config()?)?),
            server_tls,
            filter: Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers),
            tls_out: io_buf(&buffers, b"guest tls output"),
            server_buf: None,
            server_buf_pending_offset: 0,
            server_buf_pending_len: 0,
            plaintext_buf: io_buf(&buffers, b""),
            substitute_buf: io_buf(&buffers, b""),
            server_output_offset: 0,
            server_pending: WriteQueue::new(),
            server_read_pending: false,
            guest_tls_closed: false,
            guest_close_notify_queued: false,
        }),
    };
    assert!(!proxy.has_local_work(false));
    assert!(proxy.has_local_work(true));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tls_connecting_server_has_no_write_work_after_handshake_flush_blocks_on_read()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let server_task = tokio::spawn(async move {
        let (_stream, _peer) = listener.accept().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok::<_, io::Error>(())
    });
    let buffers = test_buffers();
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let policy = tls_policy(raw_decision());
    let proxy_id = TcpProxyId(55);
    let server_tls = TlsUpstream::connect(proxy_id, upstream, "allowed.test", &policy.client_config, &mut runtime)?;
    let mut server_pending = WriteQueue::new();
    server_pending.push(io_buf(&buffers, b"pending request bytes"));
    let mut proxy = TlsTcpProxy {
        proxy: proxy_id,
        requested_dst: upstream,
        upstream_dst: upstream,
        authority: Some("allowed.test".to_owned()),
        pending: WriteQueue::new(),
        guest_write_finished: false,
        close_requested: false,
        state: TlsTcpProxyState::ConnectingServer {
            guest_tls: Box::new(TlsServerSession::accept(&server_config()?)?),
            filter: Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers),
            tls_out: io_buf(&buffers, b""),
            plaintext_buf: io_buf(&buffers, b""),
            substitute_buf: io_buf(&buffers, b""),
            server_output_offset: 0,
            server_pending,
            server_tls,
        },
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut readiness = Vec::new();
    loop {
        runtime.reactor_mut().ready_into(&mut readiness, Some(Duration::ZERO))?;
        if readiness.iter().any(|ready| {
            matches!(
                ready,
                ReactorReady::Io {
                    item: ReactorItemId::TcpProxy { proxy },
                    writable: true,
                    ..
                } if *proxy == proxy_id
            )
        }) {
            for ready in &readiness {
                if let ReactorReady::Io {
                    item: ReactorItemId::TcpProxy { proxy: ready_proxy },
                    readable,
                    writable,
                } = *ready
                    && ready_proxy == proxy_id
                {
                    proxy.mark_reactor_ready(readable, writable);
                }
            }
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for upstream connect readiness".into());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    for _ in 0..4 {
        let (poll, report) = with_drive(&mut budget, |drive| {
            proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::ALL)
        });
        match poll {
            TlsProxyPoll::Pending if report.made_progress() => {}
            TlsProxyPoll::Pending => {
                assert!(report.wait().contains(crate::drive::DriveWait::REACTOR_READ));
                assert!(!report.wait().contains(crate::drive::DriveWait::REACTOR_WRITE));
                break;
            }
            TlsProxyPoll::Bytes(_) => return Err("unexpected guest TLS bytes".into()),
            TlsProxyPoll::Event(event) => return Err(format!("unexpected TLS event: {event:?}").into()),
            TlsProxyPoll::Bypass { .. } => return Err("unexpected TLS bypass".into()),
        }
    }

    assert!(!proxy.has_reactor_write_work());
    proxy.mark_reactor_ready(false, true);
    assert!(!proxy.has_local_work(false));
    server_task.abort();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tls_connecting_server_pending_guest_bytes_wait_for_upstream_handshake()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let server_task = tokio::spawn(async move {
        let (_stream, _peer) = listener.accept().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok::<_, io::Error>(())
    });
    let buffers = test_buffers();
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let policy = tls_policy(raw_decision());
    let server_tls = TlsUpstream::connect(
        TcpProxyId(58),
        upstream,
        "allowed.test",
        &policy.client_config,
        &mut runtime,
    )?;
    let mut proxy = TlsTcpProxy {
        proxy: TcpProxyId(58),
        requested_dst: upstream,
        upstream_dst: upstream,
        authority: Some("allowed.test".to_owned()),
        pending: WriteQueue::new(),
        guest_write_finished: false,
        close_requested: false,
        state: TlsTcpProxyState::ConnectingServer {
            guest_tls: Box::new(TlsServerSession::accept(&server_config()?)?),
            filter: Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers),
            tls_out: io_buf(&buffers, b""),
            plaintext_buf: io_buf(&buffers, b""),
            substitute_buf: io_buf(&buffers, b""),
            server_output_offset: 0,
            server_pending: WriteQueue::new(),
            server_tls,
        },
    };
    proxy.write(io_buf(&buffers, b"GET / HTTP/1.1\r\nHost: allowed.test\r\n\r\n"));

    assert!(!proxy.has_local_work(true));
    server_task.abort();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tls_guest_output_stays_in_order_when_guest_output_is_blocked() -> Result<(), Box<dyn std::error::Error>> {
    let buffers = BufferPool::new(NetworkLimits {
        tcp_byte_capacity: 4,
        tls_relay_buffer_capacity: 64,
        ..NetworkLimits::default()
    });
    buffers.prewarm_instance_network();

    let (mut client, guest_tls) = connected_tls_pair()?;
    let request = b"GET / HTTP/1.1\r\nHost: allowed.test\r\n\r\n";
    assert_eq!(
        client.write_plaintext_some(request)?,
        TlsPlaintextWrite::Accepted(request.len())
    );
    let mut guest_ciphertext = Vec::new();
    let _drained = client.drain_ciphertext_to(&mut guest_ciphertext, usize::MAX)?;

    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let server_task = tokio::spawn(async move {
        let (mut stream, _peer) = listener.accept().await?;
        let mut buf = [0_u8; 4096];
        let _read = stream.read(&mut buf).await?;
        Ok::<_, io::Error>(())
    });
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let client_config = TlsClientConfig::with_platform_roots(&[])?;
    let server_tls = TlsUpstream::connect(TcpProxyId(1), upstream, "allowed.test", &client_config, &mut runtime)?;

    let tls_out = io_buf(&buffers, b"aaaabbbb");
    let mut plaintext_buf = io_buf(&buffers, b"");
    plaintext_buf.resize_zeroed(buffers.limits().tls_relay_buffer_capacity);
    let mut pending = WriteQueue::new();
    pending.push(io_buf(&buffers, &guest_ciphertext));
    let mut proxy = TlsTcpProxy {
        proxy: TcpProxyId(1),
        requested_dst: upstream,
        upstream_dst: upstream,
        authority: Some("allowed.test".to_owned()),
        pending,
        guest_write_finished: false,
        close_requested: false,
        state: TlsTcpProxyState::OpenIntercept(TlsHttp1Proxy {
            guest_tls: Box::new(guest_tls),
            server_tls,
            filter: Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers),
            tls_out,
            server_buf: None,
            server_buf_pending_offset: 0,
            server_buf_pending_len: 0,
            plaintext_buf,
            substitute_buf: io_buf(&buffers, b""),
            server_output_offset: 0,
            server_pending: WriteQueue::new(),
            server_read_pending: false,
            guest_tls_closed: false,
            guest_close_notify_queued: false,
        }),
    };
    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, report) = with_drive(&mut budget, |drive| {
        proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::WRITE_UPSTREAM)
    });
    let TlsProxyPoll::Pending = poll else {
        panic!("guest-output-blocked drive should report progress instead of emitting bytes");
    };

    assert!(report.made_progress());
    let TlsTcpProxyState::OpenIntercept(proxy) = proxy.state else {
        panic!("TLS proxy should stay open");
    };
    assert_eq!(proxy.tls_out.as_slice(), b"aaaabbbb");
    server_task.abort();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tls_proxy_upstream_read_consumes_only_drive_byte_budget() -> Result<(), Box<dyn std::error::Error>> {
    let buffers = test_buffers();
    let (client, mut server) = connected_tls_pair()?;
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\n\r\n0123456789abcdef";
    assert_eq!(
        server.write_plaintext_some(response)?,
        TlsPlaintextWrite::Accepted(response.len())
    );
    let mut inbound_tls = Vec::new();
    let _drained = server.drain_ciphertext_to(&mut inbound_tls, usize::MAX)?;

    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let server_task = tokio::spawn(async move {
        let (mut stream, _peer) = listener.accept().await?;
        stream.write_all(&inbound_tls).await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok::<_, io::Error>(())
    });

    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let client_config = TlsClientConfig::with_platform_roots(&[])?;
    let mut server_tls = TlsUpstream::connect(TcpProxyId(60), upstream, "allowed.test", &client_config, &mut runtime)?;
    server_tls.connection = client;
    server_tls.mark_connect_ready();
    let (_guest_client, guest_tls) = connected_tls_pair()?;
    let mut proxy = TlsTcpProxy {
        proxy: TcpProxyId(60),
        requested_dst: upstream,
        upstream_dst: upstream,
        authority: Some("allowed.test".to_owned()),
        pending: WriteQueue::new(),
        guest_write_finished: false,
        close_requested: false,
        state: TlsTcpProxyState::OpenIntercept(TlsHttp1Proxy {
            guest_tls: Box::new(guest_tls),
            server_tls,
            filter: Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers),
            tls_out: io_buf(&buffers, b""),
            server_buf: None,
            server_buf_pending_offset: 0,
            server_buf_pending_len: 0,
            plaintext_buf: io_buf(&buffers, b""),
            substitute_buf: io_buf(&buffers, b""),
            server_output_offset: 0,
            server_pending: WriteQueue::new(),
            server_read_pending: false,
            guest_tls_closed: false,
            guest_close_notify_queued: false,
        }),
    };
    let mut budget = DriveBudget::event_loop(&NetworkLimits {
        drive_byte_budget: 4,
        ..NetworkLimits::default()
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut readiness = Vec::new();
    loop {
        runtime.reactor_mut().ready_into(&mut readiness, Some(Duration::ZERO))?;
        for ready in &readiness {
            if let ReactorReady::Io {
                item: ReactorItemId::TcpProxy { proxy: ready_proxy },
                readable,
                writable,
            } = *ready
                && ready_proxy == TcpProxyId(60)
            {
                proxy.mark_reactor_ready(readable, writable);
            }
        }
        let (poll, report) = with_drive(&mut budget, |drive| {
            proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::READ_UPSTREAM)
        });
        match poll {
            TlsProxyPoll::Event(event) => return Err(format!("unexpected TLS event: {event:?}").into()),
            TlsProxyPoll::Bypass { .. } => return Err("unexpected TLS bypass".into()),
            TlsProxyPoll::Bytes(_) => {}
            TlsProxyPoll::Pending => {
                assert!(
                    report.progress().bytes_read <= 4,
                    "TLS upstream read should not exceed the drive byte budget"
                );
            }
        }
        if budget.remaining_bytes() == 0 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for budgeted TLS upstream read".into());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    server_task.abort();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tls_proxy_upstream_ciphertext_progress_does_not_clear_read_readiness() -> Result<(), Box<dyn std::error::Error>>
{
    let buffers = test_buffers();
    let (client, mut server) = connected_tls_pair()?;
    assert_eq!(
        server.write_plaintext_some(b"response")?,
        TlsPlaintextWrite::Accepted(b"response".len())
    );
    let mut inbound_tls = Vec::new();
    let _drained = server.drain_ciphertext_to(&mut inbound_tls, usize::MAX)?;
    inbound_tls.truncate(1);

    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = listener.local_addr()?;
    let server_task = tokio::spawn(async move {
        let (mut stream, _peer) = listener.accept().await?;
        stream.write_all(&inbound_tls).await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok::<_, io::Error>(())
    });

    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let client_config = TlsClientConfig::with_platform_roots(&[])?;
    let mut server_tls = TlsUpstream::connect(TcpProxyId(61), upstream, "allowed.test", &client_config, &mut runtime)?;
    server_tls.connection = client;
    server_tls.mark_connect_ready();
    let (_guest_client, guest_tls) = connected_tls_pair()?;
    let mut proxy = TlsTcpProxy {
        proxy: TcpProxyId(61),
        requested_dst: upstream,
        upstream_dst: upstream,
        authority: Some("allowed.test".to_owned()),
        pending: WriteQueue::new(),
        guest_write_finished: false,
        close_requested: false,
        state: TlsTcpProxyState::OpenIntercept(TlsHttp1Proxy {
            guest_tls: Box::new(guest_tls),
            server_tls,
            filter: Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers),
            tls_out: io_buf(&buffers, b""),
            server_buf: None,
            server_buf_pending_offset: 0,
            server_buf_pending_len: 0,
            plaintext_buf: io_buf(&buffers, b""),
            substitute_buf: io_buf(&buffers, b""),
            server_output_offset: 0,
            server_pending: WriteQueue::new(),
            server_read_pending: false,
            guest_tls_closed: false,
            guest_close_notify_queued: false,
        }),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut readiness = Vec::new();
    loop {
        runtime.reactor_mut().ready_into(&mut readiness, Some(Duration::ZERO))?;
        for ready in &readiness {
            if let ReactorReady::Io {
                item: ReactorItemId::TcpProxy { proxy: ready_proxy },
                readable,
                writable,
            } = *ready
                && ready_proxy == TcpProxyId(61)
            {
                proxy.mark_reactor_ready(readable, writable);
            }
        }
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (poll, report) = with_drive(&mut budget, |drive| {
            proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::READ_UPSTREAM)
        });
        match poll {
            TlsProxyPoll::Pending if report.progress().bytes_read > 0 => {
                assert!(
                    !report.wait().contains(crate::drive::DriveWait::REACTOR_READ),
                    "TLS ciphertext progress must not be treated as read WouldBlock"
                );
                break;
            }
            TlsProxyPoll::Pending => {}
            TlsProxyPoll::Bytes(_) => return Err("unexpected guest TLS bytes".into()),
            TlsProxyPoll::Event(event) => return Err(format!("unexpected TLS event: {event:?}").into()),
            TlsProxyPoll::Bypass { .. } => return Err("unexpected TLS bypass".into()),
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for partial upstream TLS read".into());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    server_task.abort();
    Ok(())
}

#[test]
fn tls_guest_close_notify_finishes_guest_write() {
    let (mut client, mut guest_tls) = connected_tls_pair().expect("TLS pair should connect");
    let mut ciphertext = Vec::new();
    client.queue_close_notify();
    let _drain = client
        .drain_ciphertext_to(&mut ciphertext, usize::MAX)
        .expect("client should serialize close_notify");
    feed_server_ciphertext(&mut guest_tls, &ciphertext).expect("guest TLS should accept close_notify");

    let buffers = test_buffers();
    let mut filter = Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers);
    let mut buffer = io_buf(&buffers, b"");
    buffer
        .as_mut_vec()
        .resize(buffers.limits().tls_relay_buffer_capacity, 0);
    let mut output = io_buf(&buffers, b"");
    let mut output_offset = 0;
    let mut server_pending = WriteQueue::new();

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (step, _report) = with_drive(&mut budget, |drive| {
        TlsHttp1Proxy::<crate::reactor::MioReactor>::forward_plaintext_to_server(
            &mut guest_tls,
            &mut filter,
            &mut buffer,
            &mut output,
            &mut output_offset,
            &mut server_pending,
            &buffers,
            drive,
        )
    });
    let step = step.expect("guest close_notify should be readable");

    assert_eq!(step, RelayStep::Closed);
    assert!(server_pending.is_empty());
}

#[test]
fn tls_guest_plaintext_and_close_notify_finishes_guest_write() {
    let (mut client, mut guest_tls) = connected_tls_pair().expect("TLS pair should connect");
    let mut ciphertext = Vec::new();
    let request = b"GET / HTTP/1.1\r\nHost: allowed.test\r\n\r\n";
    assert_eq!(
        client
            .write_plaintext_some(request)
            .expect("client should accept request plaintext"),
        TlsPlaintextWrite::Accepted(request.len())
    );
    client.queue_close_notify();
    let _drain = client
        .drain_ciphertext_to(&mut ciphertext, usize::MAX)
        .expect("client should serialize request and close_notify");
    feed_server_ciphertext(&mut guest_tls, &ciphertext).expect("guest TLS should accept request and close_notify");

    let buffers = test_buffers();
    let mut filter = Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers);
    let mut buffer = io_buf(&buffers, b"");
    buffer
        .as_mut_vec()
        .resize(buffers.limits().tls_relay_buffer_capacity, 0);
    let mut output = io_buf(&buffers, b"");
    let mut output_offset = 0;
    let mut server_pending = WriteQueue::new();

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (step, _report) = with_drive(&mut budget, |drive| {
        TlsHttp1Proxy::<crate::reactor::MioReactor>::forward_plaintext_to_server(
            &mut guest_tls,
            &mut filter,
            &mut buffer,
            &mut output,
            &mut output_offset,
            &mut server_pending,
            &buffers,
            drive,
        )
    });
    let step = step.expect("guest plaintext and close_notify should be readable");

    assert_eq!(step, RelayStep::ProgressClosed);
    assert_eq!(server_pending.front_slice(), Some(&request[..]));
}

#[test]
fn tls_route_respects_bypass_drop_and_intercept_decisions() -> Result<(), Box<dyn std::error::Error>> {
    let authority = Authority::new("allowed.test");
    let mut policy = tls_policy(EgressDecision {
        application: ApplicationPolicy::Raw,
    });

    assert!(matches!(tls_route(&policy, "unknown.test"), Ok(TlsRoute::Bypass)));

    policy.fallback = EgressDecision {
        application: ApplicationPolicy::Block {
            reason: BlockReason::AuthorityNotAllowed,
        },
    };
    assert!(matches!(
        tls_route(&policy, "unknown.test"),
        Ok(TlsRoute::Drop(BlockReason::AuthorityNotAllowed))
    ));

    policy.bypass_hosts = vec!["*.internal.test".to_owned()];
    policy.decisions.push((
        Authority::new("api.internal.test"),
        EgressDecision {
            application: ApplicationPolicy::Http1 {
                authority: Authority::new("api.internal.test"),
                secrets: RuntimeSecrets::new(),
            },
        },
    ));
    assert!(matches!(tls_route(&policy, "api.internal.test"), Ok(TlsRoute::Bypass)));

    policy.bypass_hosts.clear();
    policy.server_configs.push((authority.clone(), server_config()?));
    policy.decisions.push((
        authority.clone(),
        EgressDecision {
            application: ApplicationPolicy::Http1 {
                authority: authority.clone(),
                secrets: RuntimeSecrets::new(),
            },
        },
    ));
    assert!(tls_route(&policy, authority.as_str()).is_err());
    Ok(())
}

#[test]
fn tls_wildcard_bypass_matches_subdomains_and_base_domain() {
    let patterns = vec!["*.example.test".to_owned(), "exact.test".to_owned()];

    assert!(should_bypass_tls(&patterns, "api.example.test"));
    assert!(should_bypass_tls(&patterns, "example.test"));
    assert!(should_bypass_tls(&patterns, "Exact.TEST."));
    assert!(!should_bypass_tls(&patterns, "other.test"));
}

#[test]
fn plain_policy_processing_handles_raw_block_and_plain_http1() {
    let buffers = test_buffers();
    let raw = plain_policy(ApplicationPolicy::Raw, false);
    let placeholder = io_buf(&buffers, b"Bearer AGENTDP_SECRET_TOKEN");

    let bytes =
        PlainTcpProxy::<crate::reactor::MioReactor>::process_guest_bytes(&PlainRoute::Policy(raw), placeholder, None)
            .expect("raw policy without configured secrets should not scan placeholders");
    assert_eq!(bytes.as_slice(), b"Bearer AGENTDP_SECRET_TOKEN");

    let raw_with_secrets = plain_policy(ApplicationPolicy::Raw, true);
    let error = PlainTcpProxy::<crate::reactor::MioReactor>::process_guest_bytes(
        &PlainRoute::Policy(raw_with_secrets),
        io_buf(&buffers, b"Bearer AGENTDP_SECRET_TOKEN"),
        None,
    )
    .expect_err("raw policy with configured secrets should reject unresolved placeholders");
    assert!(error.contains("unresolved mediated secret placeholder"));

    let blocked = plain_policy(
        ApplicationPolicy::Block {
            reason: BlockReason::AuthorityNotAllowed,
        },
        false,
    );
    let error = PlainTcpProxy::<crate::reactor::MioReactor>::process_guest_bytes(
        &PlainRoute::Policy(blocked),
        io_buf(&buffers, b"GET / HTTP/1.1\r\n\r\n"),
        None,
    )
    .expect_err("block policy should fail closed");
    assert!(error.contains("egress blocked by application policy"));
    assert!(error.contains("Http1"));

    let http1 = plain_policy(
        ApplicationPolicy::Http1 {
            authority: Authority::new("allowed.test"),
            secrets: RuntimeSecrets::new(),
        },
        false,
    );
    let error = PlainTcpProxy::<crate::reactor::MioReactor>::process_guest_bytes(
        &PlainRoute::Policy(http1),
        io_buf(&buffers, b"GET / HTTP/1.1\r\n\r\n"),
        None,
    )
    .expect_err("plain HTTP/1.x substitution should stay disabled");
    assert!(error.contains("plain HTTP/1.x substitution is not enabled"));
}

#[test]
fn write_queue_tracks_partial_front_write() {
    let buffers = test_buffers();
    let mut queue = WriteQueue::new();
    queue.push(io_buf(&buffers, b"abcdef"));
    queue.push(io_buf(&buffers, b"gh"));

    assert_eq!(queue.front_slice(), Some(&b"abcdef"[..]));
    assert!(!queue.advance_front(2));
    assert_eq!(queue.front_slice(), Some(&b"cdef"[..]));
    assert!(queue.advance_front(4));
    assert_eq!(queue.front_slice(), Some(&b"gh"[..]));
    assert!(queue.advance_front(2));
    assert!(queue.is_empty());
}

#[test]
fn server_plaintext_queue_retains_remainder_when_pool_is_exhausted() {
    let buffers = BufferPool::new(NetworkLimits {
        small_byte_capacity: 4,
        medium_byte_capacity: 8,
        tcp_byte_capacity: 4,
        small_byte_pool_capacity: 1,
        medium_byte_pool_capacity: 1,
        tcp_byte_pool_capacity: 0,
        tls_relay_buffer_capacity: 4,
        ..NetworkLimits::default()
    });
    buffers.prewarm_instance_network();
    let mut output = buffers.try_byte_with_capacity(8).expect("prewarmed output buffer");
    output.extend_from_slice(b"abcdefgh");
    let mut offset = 0;
    let mut queue = WriteQueue::new();

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (queue_step, _report) = with_drive(&mut budget, |drive| {
        TlsHttp1Proxy::<crate::reactor::MioReactor>::queue_server_plaintext(
            &mut queue,
            &mut output,
            &mut offset,
            &buffers,
            drive,
        )
    });
    assert_eq!(queue_step, QueueStep::ProgressBlocked);
    assert_eq!(offset, 4);
    assert_eq!(output.as_slice(), b"abcdefgh");
    assert_eq!(queue.front_slice(), Some(&b"abcd"[..]));

    drop(queue);
    let mut queue = WriteQueue::new();
    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (queue_step, _report) = with_drive(&mut budget, |drive| {
        TlsHttp1Proxy::<crate::reactor::MioReactor>::queue_server_plaintext(
            &mut queue,
            &mut output,
            &mut offset,
            &buffers,
            drive,
        )
    });
    assert_eq!(queue_step, QueueStep::Progress);
    assert_eq!(offset, 0);
    assert!(output.is_empty());
    assert_eq!(queue.front_slice(), Some(&b"efgh"[..]));
}

#[test]
fn relay_preserves_progress_blocked_when_existing_server_output_exhausts_pool() {
    let buffers = BufferPool::new(NetworkLimits {
        small_byte_capacity: 4,
        medium_byte_capacity: 8,
        tcp_byte_capacity: 4,
        small_byte_pool_capacity: 1,
        medium_byte_pool_capacity: 2,
        tcp_byte_pool_capacity: 0,
        tls_relay_buffer_capacity: 4,
        ..NetworkLimits::default()
    });
    buffers.prewarm_instance_network();
    let (_client, mut guest_tls) = connected_tls_pair().expect("connected TLS pair");
    let mut filter = Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers);
    let mut plaintext = buffers.try_byte_with_capacity(8).expect("prewarmed plaintext buffer");
    let mut output = buffers.try_byte_with_capacity(8).expect("prewarmed output buffer");
    output.extend_from_slice(b"abcdefgh");
    let mut output_offset = 0;
    let mut server_pending = WriteQueue::new();

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (step, _report) = with_drive(&mut budget, |drive| {
        TlsHttp1Proxy::<crate::reactor::MioReactor>::forward_plaintext_to_server(
            &mut guest_tls,
            &mut filter,
            &mut plaintext,
            &mut output,
            &mut output_offset,
            &mut server_pending,
            &buffers,
            drive,
        )
    });
    let step = step.expect("relay should preserve progress-blocked");

    assert_eq!(step, RelayStep::ProgressBlocked);
    assert_eq!(server_pending.front_slice(), Some(&b"abcd"[..]));
    assert_eq!(output.as_slice(), b"abcdefgh");
    assert_eq!(output_offset, 4);
}

#[tokio::test(flavor = "current_thread")]
async fn tls_client_hello_buffer_pressure_blocks_without_error() {
    let cold_buffers = BufferPool::default();
    let source_buffers = test_buffers();
    let mut proxy_id = TlsTcpProxy::new(TcpProxyId(47), test_dst(), tls_policy(raw_decision()));
    let hello = client_hello_bytes("allowed.test");
    proxy_id.write(io_buf(&source_buffers, &hello[..7]));
    let mut runtime = runtime_context(
        default_backend(NetworkLimits::default().reactor_event_capacity).expect("reactor should initialize"),
    );

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, report) = with_drive(&mut budget, |drive| {
        proxy_id.drive(&cold_buffers, &mut runtime, drive, TcpProxyPermit::ALL)
    });
    let TlsProxyPoll::Pending = poll else {
        panic!("expected local buffer wait report");
    };
    assert!(report.wait().contains(crate::drive::DriveWait::LOCAL_BUFFER_CAPACITY));
    assert!(matches!(
        proxy_id.state,
        TlsTcpProxyState::WaitingClientHelloBuffer { .. }
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn tls_flow_close_before_client_hello_reports_closed() {
    let buffers = test_buffers();
    let mut proxy_id = TlsTcpProxy::new(TcpProxyId(45), test_dst(), tls_policy(raw_decision()));
    proxy_id.close();
    let mut runtime = runtime_context(
        default_backend(NetworkLimits::default().reactor_event_capacity).expect("reactor should initialize"),
    );

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, _report) = with_drive(&mut budget, |drive| {
        proxy_id.drive(&buffers, &mut runtime, drive, TcpProxyPermit::ALL)
    });
    match poll {
        TlsProxyPoll::Event(TcpProxyEvent::Closed { proxy }) => {
            assert_eq!(proxy, TcpProxyId(45));
        }
        _ => panic!("expected closed event"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn tls_client_hello_waits_for_complete_sni() {
    let buffers = test_buffers();
    let mut proxy_id = TlsTcpProxy::new(TcpProxyId(46), test_dst(), tls_policy(raw_decision()));
    let hello = client_hello_bytes("allowed.test");
    proxy_id.write(io_buf(&buffers, &hello[..7]));
    let mut runtime = runtime_context(
        default_backend(NetworkLimits::default().reactor_event_capacity).expect("reactor should initialize"),
    );

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, report) = with_drive(&mut budget, |drive| {
        proxy_id.drive(&buffers, &mut runtime, drive, TcpProxyPermit::ALL)
    });
    let TlsProxyPoll::Pending = poll else {
        panic!("expected guest receive wait report");
    };
    assert!(report.wait().contains(crate::drive::DriveWait::GUEST_RECV));
    let TlsTcpProxyState::ReadingClientHello { initial, .. } = &proxy_id.state else {
        panic!("partial ClientHello should stay in ClientHello state");
    };
    assert_eq!(initial.as_slice(), &hello[..7]);
    assert!(proxy_id.pending.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn tls_client_hello_state_extracts_fragmented_sni() {
    let buffers = test_buffers();
    let dst = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443);
    let mut runtime = runtime_context(
        default_backend(NetworkLimits::default().reactor_event_capacity).expect("reactor should initialize"),
    );
    let mut proxy_id = TcpProxy::connecting(
        TcpProxyId(44),
        dst,
        dst,
        TcpEgressRoute::Tls(tls_policy(raw_decision())),
        &buffers,
        &mut runtime,
    )
    .expect("TLS proxy should initialize");
    let hello = client_hello_bytes("allowed.test");
    let split = 7;
    proxy_id.write(io_buf(&buffers, &hello[..split]));
    proxy_id.write(io_buf(&buffers, &hello[split..]));
    proxy_id.write(io_buf(&buffers, b"extra tls bypass bytes"));
    proxy_id.finish_guest_write();

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (_poll, _report) = with_drive(&mut budget, |drive| {
        proxy_id.drive(&buffers, &mut runtime, drive, TcpProxyPermit::ALL)
    });
    let TcpProxy::Plain(plain) = &mut proxy_id else {
        panic!("TLS bypass should replace the TLS proxy with a plain proxy");
    };
    assert!(plain.guest_write_finished);
    assert!(matches!(
        plain.state,
        PlainTcpProxyState::Connecting {
            route: Some(PlainRoute::Bypass),
            ..
        }
    ));
    let pending = plain
        .pending
        .pop_front()
        .expect("initial ClientHello should be queued for bypass");
    assert_eq!(pending.bytes.as_slice(), hello.as_slice());
    let pending = plain
        .pending
        .pop_front()
        .expect("bytes queued after the ClientHello should be preserved");
    assert_eq!(pending.bytes.as_slice(), b"extra tls bypass bytes");
}

#[tokio::test(flavor = "current_thread")]
async fn tls_client_hello_state_rejects_non_tls_input() {
    let buffers = test_buffers();
    let dst = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443);
    let mut runtime = runtime_context(
        default_backend(NetworkLimits::default().reactor_event_capacity).expect("reactor should initialize"),
    );
    let mut proxy_id = TcpProxy::connecting(
        TcpProxyId(44),
        dst,
        dst,
        TcpEgressRoute::Tls(tls_policy(raw_decision())),
        &buffers,
        &mut runtime,
    )
    .expect("TLS proxy should initialize");
    proxy_id.write(io_buf(&buffers, b"GET / HTTP/1.1\r\n\r\n"));

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, _report) = with_drive(&mut budget, |drive| {
        proxy_id.drive(&buffers, &mut runtime, drive, TcpProxyPermit::ALL)
    });
    match poll {
        TcpProxyPoll::Event(TcpProxyEvent::Error { message, .. }) => {
            assert_eq!(message, "not a TLS ClientHello");
        }
        _ => panic!("expected ClientHello error event"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn tls_guest_handshake_setup_buffer_pressure_waits_without_progress() -> Result<(), Box<dyn std::error::Error>> {
    let buffers = BufferPool::new(NetworkLimits {
        medium_byte_pool_capacity: 2,
        tls_relay_buffer_capacity: 4096,
        ..NetworkLimits::default()
    });
    buffers.prewarm_instance_network();

    let mut policy = tls_policy(raw_decision());
    let authority = Authority::new("allowed.test");
    policy.decisions.push((
        authority.clone(),
        EgressDecision {
            application: ApplicationPolicy::Http1 {
                authority: authority.clone(),
                secrets: RuntimeSecrets::new(),
            },
        },
    ));
    policy.server_configs.push((authority, server_config()?));
    let Err(intercept) = tls_route(&policy, "allowed.test") else {
        panic!("configured host should be intercepted");
    };
    let (_client, guest_tls) = connected_tls_pair()?;
    let tls_out = buffers.try_byte_with_capacity(4096)?;
    let mut proxy = TlsTcpProxy {
        proxy: TcpProxyId(55),
        requested_dst: test_dst(),
        upstream_dst: test_dst(),
        authority: Some("allowed.test".to_owned()),
        pending: WriteQueue::new(),
        guest_write_finished: false,
        close_requested: false,
        state: TlsTcpProxyState::GuestTlsHandshake {
            policy,
            intercept,
            guest_tls: Box::new(guest_tls),
            tls_out,
        },
    };
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);

    let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
    let (poll, report) = with_drive(&mut budget, |drive| {
        proxy.drive(&buffers, &mut runtime, drive, TcpProxyPermit::ALL)
    });
    let TlsProxyPoll::Pending = poll else {
        panic!("expected local buffer wait report");
    };
    assert!(!report.made_progress());
    assert!(report.wait().contains(crate::drive::DriveWait::LOCAL_BUFFER_CAPACITY));
    assert!(matches!(proxy.state, TlsTcpProxyState::GuestTlsHandshake { .. }));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tls_server_connect_failure_reports_error() -> Result<(), Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let dst = listener.local_addr()?;
    drop(listener);

    let policy = tls_policy(raw_decision());
    let mut runtime = runtime_context(default_backend(NetworkLimits::default().reactor_event_capacity)?);
    let mut upstream = TlsUpstream::connect(TcpProxyId(47), dst, "allowed.test", &policy.client_config, &mut runtime)?;
    let mut readiness = Vec::new();

    for _attempt in 0..32 {
        readiness.clear();
        runtime.reactor_mut().ready_into(&mut readiness, Some(Duration::ZERO))?;
        if readiness.iter().any(|ready| {
            matches!(
                ready,
                ReactorReady::Io {
                    item: ReactorItemId::TcpProxy { proxy: TcpProxyId(47) },
                    readable: true,
                    ..
                } | ReactorReady::Io {
                    item: ReactorItemId::TcpProxy { proxy: TcpProxyId(47) },
                    writable: true,
                    ..
                }
            )
        }) {
            upstream.mark_connect_ready();
        }
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut turn_report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut turn_report);
        match upstream.drive_handshake(runtime.reactor(), &mut drive) {
            Err(error) => {
                let message = error.to_string();
                assert!(
                    message.contains("Connection refused") || message.contains("connection refused"),
                    "unexpected error message: {message}"
                );
                return Ok(());
            }
            Ok(TlsDrive::Ready | TlsDrive::Pending) => {
                tokio::task::yield_now().await;
            }
        }
    }

    Err("TLS connect failure was not reported".into())
}

fn io_buf(buffers: &BufferPool, bytes: &[u8]) -> crate::buffers::ByteBuf {
    let mut output = buffers
        .try_byte_with_capacity(bytes.len())
        .expect("prewarmed byte buffer");
    output.extend_from_slice(bytes);
    output
}

async fn drive_tcp<N>(
    runtime: &mut N,
    buffers: &BufferPool,
    proxy: &mut TcpProxy<N::Reactor>,
) -> Result<Vec<TcpProxyPoll>, Box<dyn std::error::Error>>
where
    N: NetworkRuntime,
{
    drive_tcp_with_io(runtime, buffers, proxy).await
}

async fn drive_tcp_with_io<N>(
    runtime: &mut N,
    buffers: &BufferPool,
    proxy: &mut TcpProxy<N::Reactor>,
) -> Result<Vec<TcpProxyPoll>, Box<dyn std::error::Error>>
where
    N: NetworkRuntime,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut polls = Vec::new();
    let mut readiness = Vec::new();
    loop {
        let mut budget = DriveBudget::event_loop(&crate::network::NetworkLimits::default());
        drive_test_proxy(proxy, buffers, &mut polls, &mut budget, runtime);
        if !polls.is_empty() {
            return Ok(polls);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for TCP egress events".into());
        }
        runtime.reactor_mut().ready_into(&mut readiness, Some(Duration::ZERO))?;
        if readiness.is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        }
        let mut budget = DriveBudget::event_loop(&crate::network::NetworkLimits::default());
        for ready in &readiness {
            if let ReactorReady::Io { readable, writable, .. } = *ready
                && (readable || writable)
            {
                proxy.mark_reactor_ready(readable, writable);
            }
        }
        drive_test_proxy(proxy, buffers, &mut polls, &mut budget, runtime);
        if !polls.is_empty() {
            return Ok(polls);
        }
    }
}

fn drive_test_proxy<N>(
    proxy: &mut TcpProxy<N::Reactor>,
    buffers: &BufferPool,
    polls: &mut Vec<TcpProxyPoll>,
    budget: &mut DriveBudget,
    runtime: &mut N,
) where
    N: NetworkRuntime,
{
    while budget.can_continue() {
        let mut report = DriveReport::new();
        let poll = {
            let mut drive = DriveTurn::new(budget, &mut report);
            proxy.drive(buffers, runtime, &mut drive, TcpProxyPermit::ALL)
        };
        match poll {
            TcpProxyPoll::Bytes(bytes) => {
                polls.push(TcpProxyPoll::Bytes(bytes));
                break;
            }
            TcpProxyPoll::Event(event) => {
                polls.push(TcpProxyPoll::Event(event));
                break;
            }
            TcpProxyPoll::Pending if report.made_progress() => {}
            TcpProxyPoll::Pending => break,
        }
    }
}

#[test]
fn tcp_push_event_preserves_materialized_event_and_reports_exhaustion() {
    let mut budget = DriveBudget::event_loop(&NetworkLimits {
        drive_event_budget: 0,
        ..NetworkLimits::default()
    });
    let mut report = DriveReport::new();
    let mut events = Vec::new();
    let mut drive = DriveTurn::new(&mut budget, &mut report);

    let event = drive.push_event(&mut events, TcpProxyEvent::closed(TcpProxyId(99)));
    assert!(matches!(event, Err(TcpProxyEvent::Closed { proxy }) if proxy == TcpProxyId(99)));
    assert!(!report.made_progress());
    assert!(report.budget_exhausted());
    assert!(events.is_empty());
}

fn tls_policy(fallback: EgressDecision) -> TlsEgressPolicy {
    TlsEgressPolicy {
        dst: test_dst(),
        client_config: TlsClientConfig::with_platform_roots(&[]).expect("empty root set should build"),
        bypass_hosts: Vec::new(),
        server_configs: Vec::new(),
        decisions: Vec::new(),
        fallback,
    }
}

fn test_dst() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443)
}

fn raw_decision() -> EgressDecision {
    EgressDecision {
        application: ApplicationPolicy::Raw,
    }
}

fn plain_policy(application: ApplicationPolicy, reject_secret_placeholders: bool) -> TcpEgressPolicy {
    TcpEgressPolicy {
        decision: EgressDecision { application },
        reject_secret_placeholders,
    }
}

fn server_config() -> Result<TlsServerConfig, Box<dyn std::error::Error>> {
    let ca = CertificateAuthorityPem::generate()?;
    let ca = CertificateAuthority::load(&ca.cert_pem, &ca.key_pem)?;
    Ok(ca.server_config_for_host(
        "allowed.test",
        CertificateValidity::valid_for(Duration::from_hours(1), Duration::from_mins(1)),
    )?)
}

fn client_hello_bytes(host: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]);
    body.extend_from_slice(&[0_u8; 32]);
    body.push(0);
    body.extend_from_slice(&2_u16.to_be_bytes());
    body.extend_from_slice(&0x1301_u16.to_be_bytes());
    body.push(1);
    body.push(0);

    let host = host.as_bytes();
    let mut sni = Vec::new();
    sni.extend_from_slice(&usize_to_u16(host.len() + 3).to_be_bytes());
    sni.push(0);
    sni.extend_from_slice(&usize_to_u16(host.len()).to_be_bytes());
    sni.extend_from_slice(host);

    let mut extensions = Vec::new();
    extensions.extend_from_slice(&0_u16.to_be_bytes());
    extensions.extend_from_slice(&usize_to_u16(sni.len()).to_be_bytes());
    extensions.extend_from_slice(&sni);
    body.extend_from_slice(&usize_to_u16(extensions.len()).to_be_bytes());
    body.extend_from_slice(&extensions);

    let mut handshake = Vec::new();
    handshake.push(0x01);
    handshake.extend_from_slice(&u24_bytes(body.len()));
    handshake.extend_from_slice(&body);

    let mut record = Vec::new();
    record.extend_from_slice(&[0x16, 0x03, 0x03]);
    record.extend_from_slice(&usize_to_u16(handshake.len()).to_be_bytes());
    record.extend_from_slice(&handshake);
    record
}

fn usize_to_u16(value: usize) -> u16 {
    u16::try_from(value).unwrap_or(u16::MAX)
}

fn u24_bytes(value: usize) -> [u8; 3] {
    let bytes = u32::try_from(value).unwrap_or(u32::MAX).to_be_bytes();
    [bytes[1], bytes[2], bytes[3]]
}
