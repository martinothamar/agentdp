use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use agentdp_crypto::test_support::{connected_tls_pair, feed_server_ciphertext};
use agentdp_crypto::{
    CertificateAuthority, CertificateAuthorityPem, CertificateValidity, TlsClientConfig, TlsPlaintextRead,
    TlsPlaintextWrite, TlsServerConfig, TlsServerSession,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::RuntimeSecrets;
use crate::application::Http1Filter;
use crate::buffers::BufferPool;
use crate::buffers::WriteQueue;
use crate::drive::DriveBudget;
use crate::network::{
    ApplicationPolicy, BlockReason, EgressDecision, NetworkLimits, TcpEgressPolicy, TcpEgressRoute, TcpProxyId,
    TlsEgressPolicy,
};
use crate::policy::Authority;
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorReady, default_backend};
use crate::runtime::NetworkRuntime;
use crate::test_support::unit::{dns_a_response, dns_query, runtime_context, tcp_dns_frame};

use super::plain::{PlainRoute, PlainTcpProxy, PlainTcpProxyState};
use super::tls::{
    QueueStep, RelayStep, TlsHttp1Proxy, TlsProxyPoll, TlsRoute, TlsTcpProxy, TlsTcpProxyState, should_bypass_tls,
    tls_route,
};
use super::tls_upstream::{
    TlsDrive, TlsReadState, TlsUpstream, is_benign_shutdown_write_error, read_bounded_tls_plaintext,
    write_bounded_tls_plaintext,
};
use super::{TcpProxy, TcpProxyEvent, TcpProxyPoll};

fn test_buffers() -> BufferPool {
    let buffers = BufferPool::default();
    buffers.prewarm_instance_network();
    buffers
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

    assert!(matches!(
        proxy_id.drive(&buffers, runtime.reactor_mut()),
        TcpProxyPoll::Blocked
    ));
    assert!(matches!(
        proxy_id.state,
        PlainTcpProxyState::Connecting {
            connect_ready: false,
            ..
        }
    ));
    Ok(())
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
    server_tls.write_finished = true;

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
            server_buf: io_buf(&buffers, b""),
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

    assert!(!proxy_id.has_queued_work());
    Ok(())
}

#[test]
fn tls_upstream_read_plaintext_does_not_ingest_without_output_capacity() {
    let (client, mut server) = connected_tls_pair().expect("TLS pair should connect");
    let mut inbound_tls = Vec::new();
    assert_eq!(
        server
            .write_plaintext_some(b"response")
            .expect("server should accept response plaintext"),
        TlsPlaintextWrite::Accepted(b"response".len())
    );
    let _drain = server
        .drain_ciphertext_to(&mut inbound_tls)
        .expect("server should serialize response TLS");

    let mut stream = TlsReadProbeStream::new(inbound_tls);
    let mut connection = client;
    let mut output = [];
    let read = read_bounded_tls_plaintext(&mut connection, &mut stream, &mut output)
        .expect("zero-capacity output should block without consuming TLS input");

    assert_eq!(read.state, TlsReadState::Blocked);
    assert_eq!(stream.read_offset, 0);
}

#[test]
fn tls_guest_close_notify_finishes_guest_write() {
    let (mut client, mut guest_tls) = connected_tls_pair().expect("TLS pair should connect");
    let mut ciphertext = Vec::new();
    client.queue_close_notify();
    let _drain = client
        .drain_ciphertext_to(&mut ciphertext)
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

    let step = TlsHttp1Proxy::<crate::reactor::MioReactor>::forward_plaintext_to_server(
        &mut guest_tls,
        &mut filter,
        &mut buffer,
        &mut output,
        &mut output_offset,
        &mut server_pending,
        &buffers,
    )
    .expect("guest close_notify should be readable");

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
        .drain_ciphertext_to(&mut ciphertext)
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

    let step = TlsHttp1Proxy::<crate::reactor::MioReactor>::forward_plaintext_to_server(
        &mut guest_tls,
        &mut filter,
        &mut buffer,
        &mut output,
        &mut output_offset,
        &mut server_pending,
        &buffers,
    )
    .expect("guest plaintext and close_notify should be readable");

    assert_eq!(step, RelayStep::ProgressClosed);
    assert_eq!(server_pending.front_slice(), Some(&request[..]));
}

#[test]
fn tls_upstream_write_plaintext_preserves_backpressure_in_caller_queue() {
    let (mut client, mut server) = connected_tls_pair().expect("TLS pair should connect");
    let mut stream = BlockingTlsWrite::blocked();

    let first = write_bounded_tls_plaintext(&mut client, &mut stream, b"first")
        .expect("first plaintext chunk should be accepted before transport blocks");
    assert_eq!(first, TlsPlaintextWrite::Accepted(b"first".len()));

    let blocked = write_bounded_tls_plaintext(&mut client, &mut stream, b"second")
        .expect("pending TLS records should block accepting more plaintext");
    assert_eq!(blocked, TlsPlaintextWrite::BlockedByPendingCiphertext);

    stream.blocked = false;
    let flushed = write_bounded_tls_plaintext(&mut client, &mut stream, b"")
        .expect("pending TLS records should flush once transport is writable");
    assert_eq!(flushed, TlsPlaintextWrite::Accepted(0));
    assert!(
        !stream.written.is_empty(),
        "accepted plaintext should serialize after transport becomes writable"
    );

    feed_server_ciphertext(&mut server, &stream.written).expect("server should accept serialized TLS records");
    let mut plaintext = [0_u8; 16];
    assert_eq!(
        server
            .read_plaintext_some(&mut plaintext)
            .expect("server should read exactly the accepted plaintext"),
        TlsPlaintextRead::Plaintext(b"first".len())
    );
    assert_eq!(&plaintext[..b"first".len()], b"first");
    assert_eq!(
        server
            .read_plaintext_some(&mut plaintext)
            .expect("second plaintext chunk should still be owned by the caller queue"),
        TlsPlaintextRead::Blocked
    );
}

#[test]
fn tls_upstream_shutdown_write_error_is_not_benign_with_pending_application_ciphertext() {
    let error = io::Error::from(io::ErrorKind::BrokenPipe);

    assert!(is_benign_shutdown_write_error(&error, false));
    assert!(!is_benign_shutdown_write_error(&error, true));
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

    assert_eq!(
        TlsHttp1Proxy::<crate::reactor::MioReactor>::queue_server_plaintext(
            &mut queue,
            &mut output,
            &mut offset,
            &buffers,
        ),
        QueueStep::Blocked
    );
    assert_eq!(offset, 4);
    assert_eq!(output.as_slice(), b"abcdefgh");
    assert_eq!(queue.front_slice(), Some(&b"abcd"[..]));

    drop(queue);
    let mut queue = WriteQueue::new();
    assert_eq!(
        TlsHttp1Proxy::<crate::reactor::MioReactor>::queue_server_plaintext(
            &mut queue,
            &mut output,
            &mut offset,
            &buffers,
        ),
        QueueStep::Progress
    );
    assert_eq!(offset, 0);
    assert!(output.is_empty());
    assert_eq!(queue.front_slice(), Some(&b"efgh"[..]));
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

    assert!(matches!(
        proxy_id.drive(&cold_buffers, &mut runtime),
        TlsProxyPoll::Blocked
    ));
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

    match proxy_id.drive(&buffers, &mut runtime) {
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

    assert!(matches!(proxy_id.drive(&buffers, &mut runtime), TlsProxyPoll::Blocked));
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

    let _poll = proxy_id.drive(&buffers, &mut runtime);
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

    match proxy_id.drive(&buffers, &mut runtime) {
        TcpProxyPoll::Event(TcpProxyEvent::Error { message, .. }) => {
            assert_eq!(message, "not a TLS ClientHello");
        }
        _ => panic!("expected ClientHello error event"),
    }
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
        match upstream.drive_handshake(runtime.reactor()) {
            Err(error) => {
                let message = error.to_string();
                assert!(
                    message.contains("Connection refused") || message.contains("connection refused"),
                    "unexpected error message: {message}"
                );
                return Ok(());
            }
            Ok(TlsDrive::Ready | TlsDrive::Progress | TlsDrive::Blocked) => {
                tokio::task::yield_now().await;
            }
        }
    }

    Err("TLS connect failure was not reported".into())
}

struct BlockingTlsWrite {
    blocked: bool,
    written: Vec<u8>,
}

impl BlockingTlsWrite {
    const fn blocked() -> Self {
        Self {
            blocked: true,
            written: Vec::new(),
        }
    }
}

impl Write for BlockingTlsWrite {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.blocked {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.written.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct TlsReadProbeStream {
    readable: Vec<u8>,
    read_offset: usize,
}

impl TlsReadProbeStream {
    fn new(readable: Vec<u8>) -> Self {
        Self {
            readable,
            read_offset: 0,
        }
    }
}

impl Read for TlsReadProbeStream {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let readable = &self.readable[self.read_offset..];
        if readable.is_empty() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let len = output.len().min(readable.len());
        output[..len].copy_from_slice(&readable[..len]);
        self.read_offset += len;
        Ok(len)
    }
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
                proxy.mark_connect_ready();
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
    while budget.step() && budget.can_continue() {
        match proxy.drive(buffers, runtime) {
            TcpProxyPoll::Bytes(bytes) => {
                polls.push(TcpProxyPoll::Bytes(bytes));
                break;
            }
            TcpProxyPoll::Event(event) => {
                polls.push(TcpProxyPoll::Event(event));
                break;
            }
            TcpProxyPoll::Progress => {}
            TcpProxyPoll::Blocked => break,
        }
    }
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
