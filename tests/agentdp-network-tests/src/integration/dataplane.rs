#![forbid(unsafe_code)]
#![allow(
    clippy::future_not_send,
    reason = "network test support drives the same current-thread guest transport model as agentdp-network"
)]

use std::collections::VecDeque;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::io::{Cursor, Read as _, Write as _};
use std::mem::size_of;
use std::net::Ipv4Addr;
use std::os::fd::AsFd as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentdp_crypto::{TlsCiphertextRead, TlsClientConfig, TlsClientSession, TlsPlaintextRead, TlsPlaintextWrite};
use agentdp_network::{
    ConnectStatus, EgressPolicy, EventLoop, FrameBuf, FrameRead, FrameWrite, GuestFrameSession, GuestFrameTransport,
    GuestIoSource, InstanceAddresses, InstanceMacAddresses, InstanceNetworkConfig, InstanceNetworkError,
    InstanceNetworkSpec, InstanceNetworkStatus, Ipv4AddressText, MacAddress, NetworkCommand, NetworkCommandSource,
    NetworkEventEnvelope, NetworkEventSink, NetworkExit, NetworkPolicy, ProductionWake, RuntimeSecret, RuntimeSecrets,
    TlsInterceptConfig, TransportError,
};
use smoltcp::iface::{Config as SmoltcpConfig, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::{tcp, udp};
use smoltcp::time::Instant as SmoltcpInstant;
use smoltcp::wire::{
    ETHERNET_HEADER_LEN, EthernetAddress, HardwareAddress, IpAddress, IpCidr, IpEndpoint, Ipv4Address,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{oneshot, watch};

#[path = "upstream.rs"]
mod upstream;

pub use upstream::{AgentWorkflowHarness, agent_https_request, agent_https_response};

const GUEST_IP: Ipv4Addr = agentdp_core::mediated_network::DEFAULT_PROFILE.guest_ipv4;
const GATEWAY_IP: Ipv4Addr = agentdp_core::mediated_network::DEFAULT_PROFILE.gateway_ipv4;
const TIMEOUT: Duration = Duration::from_secs(5);
const TCP_BUFFER_BYTES: usize = 1024 * 1024;
const UDP_BUFFER_BYTES: usize = 64 * 1024;
const UDP_PACKET_SLOTS: usize = 16;
const HARNESS_FRAME_CAPACITY: usize = 1514;
const HARNESS_FRAME_POOL_FRAMES: usize = 512;
const HARNESS_GUEST_TO_NETWORK_BACKLOG_LIMIT: usize = HARNESS_FRAME_POOL_FRAMES / 2;
const DEFAULT_AGENT_WORKFLOW_HOST: &str = "allowed.test";
const DEFAULT_SECRET_PLACEHOLDER: &str = "AGENTDP_SECRET_TOKEN";
const DEFAULT_SECRET_VALUE: &str = "substituted-token";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn from_display(context: &str, error: impl Display) -> Self {
        Self::new(format!("{context}: {error}"))
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl StdError for Error {}

struct TestNetworkHandle {
    label: String,
    commands: mpsc::Sender<NetworkCommand>,
    wake: ProductionWake,
    status: watch::Receiver<InstanceNetworkStatus>,
    thread: JoinHandle<NetworkExit>,
}

struct TestCommandSource {
    receiver: mpsc::Receiver<NetworkCommand>,
}

struct TestOutputSink {
    status: watch::Sender<InstanceNetworkStatus>,
}

impl TestNetworkHandle {
    fn start<T>(spec: InstanceNetworkSpec, transport: T) -> std::result::Result<Self, InstanceNetworkError>
    where
        T: GuestFrameTransport + Send + 'static,
    {
        let label = spec.label.clone();
        let (status_tx, status_rx) = watch::channel(InstanceNetworkStatus::starting(&spec.config.limits));
        let (commands_tx, commands_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let thread_label = label.clone();
        let thread = std::thread::Builder::new()
            .name(format!("agentdp-network-test-{thread_label}"))
            .spawn(move || {
                let event_loop = match EventLoop::new(
                    spec,
                    transport,
                    TestOutputSink { status: status_tx },
                    TestCommandSource { receiver: commands_rx },
                ) {
                    Ok(event_loop) => event_loop,
                    Err(error) => {
                        let _sent = started_tx.send(Err(error.clone()));
                        return NetworkExit::Failed(error);
                    }
                };
                let wake = event_loop.wake_handle();
                let _sent = started_tx.send(Ok(wake));
                event_loop.run()
            })
            .map_err(|error| InstanceNetworkError::TaskFailed {
                label: label.clone(),
                message: format!("failed to spawn test network thread: {error}"),
            })?;
        let wake = match started_rx.recv() {
            Ok(Ok(wake)) => wake,
            Ok(Err(error)) => return Err(error),
            Err(_disconnected) => {
                return Err(InstanceNetworkError::TaskFailed {
                    label,
                    message: "test network thread stopped during startup".to_owned(),
                });
            }
        };
        Ok(Self {
            label,
            commands: commands_tx,
            wake,
            status: status_rx,
            thread,
        })
    }

    fn status(&self) -> InstanceNetworkStatus {
        self.status.borrow().clone()
    }

    async fn stop(self) -> std::result::Result<(), InstanceNetworkError> {
        let _sent = self.commands.send(NetworkCommand::Stop);
        let _woken = self.wake.wake();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self.thread.is_finished() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_elapsed| InstanceNetworkError::StopTimeout {
            label: self.label.clone(),
            timeout: Duration::from_secs(2),
        })?;
        match self.thread.join() {
            Ok(NetworkExit::Stopped) => Ok(()),
            Ok(NetworkExit::Failed(error)) => Err(error),
            Err(_panic) => Err(InstanceNetworkError::TaskFailed {
                label: self.label,
                message: "test network thread panicked".to_owned(),
            }),
        }
    }
}

impl NetworkCommandSource for TestCommandSource {
    fn try_recv(&mut self) -> Option<NetworkCommand> {
        match self.receiver.try_recv() {
            Ok(command) => Some(command),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => Some(NetworkCommand::Stop),
        }
    }
}

impl NetworkEventSink for TestOutputSink {
    fn emit(&mut self, fill: impl FnOnce(&mut NetworkEventEnvelope)) {
        let mut event = NetworkEventEnvelope::default();
        fill(&mut event);
        let mut status = self.status.borrow().clone();
        status.observe_event(&event);
        let _sent = self.status.send(status);
    }

    fn flush(&mut self) {}
}

pub struct AsyncDataplane {
    handle: TestNetworkHandle,
    guest: GuestPeer,
    http_server: Option<PersistentHttpServer>,
    udp_sink: Option<PersistentUdpSink>,
    udp_connection: Option<GuestUdpConnection>,
    tcp_server: Option<PersistentTcpEchoServer>,
    tcp_sink: Option<PersistentTcpSink>,
    tcp_connection: Option<GuestTcpConnection>,
}

impl AsyncDataplane {
    /// # Errors
    ///
    /// Returns an error when the in-memory guest transport, instance network, or HTTP server cannot start.
    pub async fn start_with_http_server(response_body_size: usize) -> Result<Self> {
        Self::start_inner(Some(response_body_size)).await
    }

    /// # Errors
    ///
    /// Returns an error when the in-memory guest transport, instance network, UDP sink, or guest UDP socket cannot start.
    pub async fn start_with_udp_sink() -> Result<Self> {
        let mut dataplane = Self::start_inner(None).await?;
        let sink = PersistentUdpSink::start().await?;
        let connection = dataplane.guest.open_udp(sink.port)?;
        dataplane.udp_sink = Some(sink);
        dataplane.udp_connection = Some(connection);
        Ok(dataplane)
    }

    /// # Errors
    ///
    /// Returns an error when the in-memory guest transport, instance network, TCP server, or guest TCP stream cannot start.
    pub async fn start_with_established_tcp_server() -> Result<Self> {
        let mut dataplane = Self::start_inner(None).await?;
        let server = PersistentTcpEchoServer::start().await?;
        let connection = dataplane.guest.connect_tcp(server.port).await?;
        dataplane.tcp_server = Some(server);
        dataplane.tcp_connection = Some(connection);
        Ok(dataplane)
    }

    /// # Errors
    ///
    /// Returns an error when the in-memory guest transport, instance network, TCP sink, or guest TCP stream cannot start.
    pub async fn start_with_tcp_sink() -> Result<Self> {
        let mut dataplane = Self::start_inner(None).await?;
        let sink = PersistentTcpSink::start().await?;
        let connection = dataplane.guest.connect_tcp(sink.port).await?;
        dataplane.tcp_sink = Some(sink);
        dataplane.tcp_connection = Some(connection);
        Ok(dataplane)
    }

    async fn start_inner(response_body_size: Option<usize>) -> Result<Self> {
        let mut config = InstanceNetworkConfig::new(
            mediated_network_addresses(),
            mediated_network_mac(),
            EgressPolicy::allow_all(),
        );
        config.limits.tcp_proxy_limit = 128;
        let mut dataplane = Self::start_with_config(config)?;

        dataplane.http_server = match response_body_size {
            Some(size) => Some(PersistentHttpServer::start(http_response(size)).await?),
            None => None,
        };

        Ok(dataplane)
    }

    fn start_with_config(config: InstanceNetworkConfig) -> Result<Self> {
        let (transport, endpoint) = MemoryTransport::new()?;
        let handle = TestNetworkHandle::start(
            InstanceNetworkSpec {
                label: "agentdp-network-tests".to_owned(),
                config,
                reconnect_delay: Duration::from_millis(10),
                write_timeout: TIMEOUT,
            },
            transport,
        )
        .map_err(|error| Error::from_display("start instance network", error))?;

        Ok(Self {
            handle,
            guest: GuestPeer::new(endpoint),
            http_server: None,
            udp_sink: None,
            udp_connection: None,
            tcp_server: None,
            tcp_sink: None,
            tcp_connection: None,
        })
    }

    /// # Errors
    ///
    /// Returns an error when the persistent HTTP server or instance network cannot stop cleanly.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(server) = self.http_server.take() {
            server.stop().await?;
        }
        if let Some(server) = self.udp_sink.take() {
            server.stop().await?;
        }
        if let Some(server) = self.tcp_server.take() {
            server.stop().await?;
        }
        if let Some(server) = self.tcp_sink.take() {
            server.stop().await?;
        }
        self.handle
            .stop()
            .await
            .map_err(|error| Error::from_display("stop instance network", error))
    }

    /// # Errors
    ///
    /// Returns an error when the guest DNS query does not complete through the instance network.
    async fn resolve_host(&mut self, dns_port: u16, host: &str) -> Result<()> {
        let query = dns_query(0x5130, host);
        let response = self.guest.udp_roundtrip(dns_port, &query).await?;
        if !dns_response_has_answer(&response) {
            return Err(Error::new(format!("DNS response for {host} did not contain an answer")));
        }
        Ok(())
    }

    /// # Errors
    ///
    /// Returns an error when HTTPS/HTTP1 traffic is not proxied through the instance network.
    async fn https_http1_roundtrip(
        &mut self,
        host: &str,
        port: u16,
        ca_cert_pem: &str,
        request: &[u8],
    ) -> Result<Vec<u8>> {
        self.guest.tls_http_roundtrip(port, host, ca_cert_pem, request).await
    }

    #[must_use]
    fn status(&self) -> agentdp_network::InstanceNetworkStatus {
        self.handle.status()
    }

    /// # Errors
    ///
    /// Returns an error when the UDP sink was not configured or the guest cannot send all datagrams.
    pub async fn established_udp_upload(&mut self, payload: &[u8], iterations: usize) -> Result<usize> {
        let Some(connection) = &self.udp_connection else {
            return Err(Error::new("established UDP socket was not configured"));
        };
        let Some(sink) = &self.udp_sink else {
            return Err(Error::new("persistent UDP sink was not configured"));
        };
        let received = Arc::clone(&sink.received);
        let before = received.load(Ordering::Relaxed);
        self.guest
            .established_udp_send_many(connection, payload, iterations)
            .await?;
        let after = self
            .guest
            .drive_until_future(wait_for_sink_quiescent(received, before))
            .await?;
        Ok(after.saturating_sub(before))
    }

    /// # Errors
    ///
    /// Returns an error when the established TCP stream was not configured or TCP traffic is not echoed.
    pub async fn established_tcp_roundtrip_into(&mut self, payload: &[u8], response: &mut Vec<u8>) -> Result<()> {
        let Some(connection) = &mut self.tcp_connection else {
            return Err(Error::new("established TCP stream was not configured"));
        };
        self.guest
            .established_tcp_roundtrip_into(connection, payload, response)
            .await
    }

    /// # Errors
    ///
    /// Returns an error when the TCP sink was not configured or does not receive all bytes.
    pub async fn established_tcp_upload(&mut self, payload: &[u8], iterations: usize) -> Result<()> {
        let Some(connection) = &self.tcp_connection else {
            return Err(Error::new("established TCP stream was not configured"));
        };
        let Some(sink) = &self.tcp_sink else {
            return Err(Error::new("persistent TCP sink was not configured"));
        };
        let received = Arc::clone(&sink.received);
        let expected_bytes = payload.len().saturating_mul(iterations);
        self.guest
            .established_tcp_send_many(connection, payload, iterations)
            .await?;
        self.guest
            .drive_until_future(wait_for_sink_bytes(received, expected_bytes))
            .await
    }

    /// # Errors
    ///
    /// Returns an error when the persistent HTTP server was not configured or the request fails.
    pub async fn persistent_http1_roundtrip(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        let Some(server) = &self.http_server else {
            return Err(Error::new("persistent HTTP server was not configured"));
        };
        self.guest
            .tcp_http_roundtrip(server.port, request)
            .await
            .map_err(|error| Error::new(format!("{error}; network status={:?}", self.status())))
    }
}

struct GuestPeer {
    endpoint: GuestEndpoint,
    device: PeerDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    next_port: u16,
}

impl GuestPeer {
    fn new(endpoint: GuestEndpoint) -> Self {
        let mut device = PeerDevice::new();
        let mut config = SmoltcpConfig::new(HardwareAddress::Ethernet(guest_mac()));
        config.random_seed = 0x6745_2301;
        let mut iface = Interface::new(config, &mut device, smoltcp_now());
        iface.update_ip_addrs(|addrs| {
            let _pushed = addrs.push(IpCidr::new(IpAddress::Ipv4(ipv4(GUEST_IP)), 24));
        });
        let _route = iface.routes_mut().add_default_ipv4_route(ipv4(GATEWAY_IP));
        Self {
            endpoint,
            device,
            iface,
            sockets: SocketSet::new(vec![]),
            next_port: 40_000,
        }
    }

    async fn udp_roundtrip(&mut self, port: u16, payload: &[u8]) -> Result<Vec<u8>> {
        let rx_meta = vec![udp::PacketMetadata::EMPTY; UDP_PACKET_SLOTS];
        let tx_meta = vec![udp::PacketMetadata::EMPTY; UDP_PACKET_SLOTS];
        let rx_buffer = udp::PacketBuffer::new(rx_meta, vec![0; UDP_BUFFER_BYTES]);
        let tx_buffer = udp::PacketBuffer::new(tx_meta, vec![0; UDP_BUFFER_BYTES]);
        let mut socket = udp::Socket::new(rx_buffer, tx_buffer);
        let local_port = self.next_ephemeral_port();
        socket
            .bind(local_port)
            .map_err(|error| Error::from_display("bind guest UDP socket", error))?;
        let handle = self.sockets.add(socket);
        let mut sent = false;
        let result = self
            .drive_until(|peer| {
                let socket = peer.sockets.get_mut::<udp::Socket>(handle);
                if !sent && socket.can_send() {
                    socket
                        .send_slice(payload, IpEndpoint::new(IpAddress::Ipv4(ipv4(GATEWAY_IP)), port))
                        .map_err(|error| Error::from_display("send UDP datagram", error))?;
                    sent = true;
                }
                if socket.can_recv() {
                    let (bytes, _meta) = socket
                        .recv()
                        .map_err(|error| Error::from_display("receive UDP datagram", error))?;
                    return Ok(Some(bytes.to_vec()));
                }
                Ok(None)
            })
            .await;
        self.sockets.remove(handle);
        result
    }

    fn open_udp(&mut self, port: u16) -> Result<GuestUdpConnection> {
        let rx_meta = vec![udp::PacketMetadata::EMPTY; UDP_PACKET_SLOTS];
        let tx_meta = vec![udp::PacketMetadata::EMPTY; UDP_PACKET_SLOTS];
        let rx_buffer = udp::PacketBuffer::new(rx_meta, vec![0; UDP_BUFFER_BYTES]);
        let tx_buffer = udp::PacketBuffer::new(tx_meta, vec![0; UDP_BUFFER_BYTES]);
        let mut socket = udp::Socket::new(rx_buffer, tx_buffer);
        socket
            .bind(self.next_ephemeral_port())
            .map_err(|error| Error::from_display("bind guest UDP socket", error))?;
        Ok(GuestUdpConnection {
            handle: self.sockets.add(socket),
            port,
        })
    }

    async fn established_udp_send_many(
        &mut self,
        connection: &GuestUdpConnection,
        payload: &[u8],
        iterations: usize,
    ) -> Result<()> {
        let mut sent = 0;
        let mut drained = false;
        self.drive_until(|peer| {
            if peer.endpoint.queued_to_network_frames() >= HARNESS_GUEST_TO_NETWORK_BACKLOG_LIMIT {
                return Ok(None);
            }
            let socket = peer.sockets.get_mut::<udp::Socket>(connection.handle);
            while sent < iterations && socket.can_send() {
                socket
                    .send_slice(
                        payload,
                        IpEndpoint::new(IpAddress::Ipv4(ipv4(GATEWAY_IP)), connection.port),
                    )
                    .map_err(|error| Error::from_display("send UDP datagram", error))?;
                sent += 1;
            }
            if sent == iterations && socket.send_queue() == 0 {
                if drained {
                    return Ok(Some(()));
                }
                drained = true;
            }
            Ok(None)
        })
        .await
    }

    async fn tcp_http_roundtrip(&mut self, port: u16, request: &[u8]) -> Result<Vec<u8>> {
        self.tcp_roundtrip_until(port, request, TcpCompletion::Http).await
    }

    async fn tls_http_roundtrip(
        &mut self,
        port: u16,
        host: &str,
        ca_cert_pem: &str,
        request: &[u8],
    ) -> Result<Vec<u8>> {
        let mut client = self.add_tls_client(port, host, ca_cert_pem, request)?;
        client.flush_tls_output()?;

        let handle = client.handle;
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        let result = loop {
            self.poll();
            self.poll_tls_http_client(&mut client)?;
            if http_message_complete(&client.response)? && client.tls_closed {
                break Ok(std::mem::take(&mut client.response));
            }
            if tokio::time::Instant::now() >= deadline {
                let socket = self.sockets.get::<tcp::Socket>(handle);
                break Err(Error::new(format!(
                    "timed out waiting for guest TLS HTTP response; tls_handshaking={} tls_closed={} request_written={} tls_pending={} response_bytes={} socket_state={:?} socket_send_queue={} socket_recv_queue={}",
                    client.tls.is_handshaking(),
                    client.tls_closed,
                    client.request_written,
                    client.pending_tls_output().len(),
                    client.response.len(),
                    socket.state(),
                    socket.send_queue(),
                    socket.recv_queue(),
                )));
            }
            self.wait_for_input().await?;
        };
        if result.is_ok() {
            self.sockets.get_mut::<tcp::Socket>(handle).close();
            self.poll();
        }
        self.sockets.remove(handle);
        result
    }

    async fn connect_tcp(&mut self, port: u16) -> Result<GuestTcpConnection> {
        let connection = self.add_tcp_connection(port);
        let handle = connection.handle;
        let result = self
            .drive_until(|peer| {
                let cx = peer.iface.context();
                let socket = peer.sockets.get_mut::<tcp::Socket>(handle);
                if !socket.is_open() {
                    socket
                        .connect(cx, (IpAddress::Ipv4(ipv4(GATEWAY_IP)), port), connection.local_port)
                        .map_err(|error| Error::from_display("connect guest TCP socket", error))?;
                }
                Ok(socket.can_send().then_some(()))
            })
            .await;
        match result {
            Ok(()) => Ok(connection),
            Err(error) => {
                self.sockets.remove(handle);
                Err(error)
            }
        }
    }

    async fn established_tcp_roundtrip_into(
        &mut self,
        connection: &mut GuestTcpConnection,
        payload: &[u8],
        response: &mut Vec<u8>,
    ) -> Result<()> {
        connection.request.clear();
        connection.request.reserve(size_of::<u32>() + payload.len());
        connection
            .request
            .extend_from_slice(&u32::try_from(payload.len()).unwrap_or(u32::MAX).to_be_bytes());
        connection.request.extend_from_slice(payload);
        connection.request_offset = 0;
        connection.response.clear();
        connection.response.reserve(payload.len());
        connection.expected_bytes = payload.len();
        response.clear();
        response.reserve(payload.len());
        self.drive_until(|peer| {
            peer.poll_established_tcp_connection(connection)?;
            if connection.response.len() >= connection.expected_bytes {
                response.extend_from_slice(&connection.response[..connection.expected_bytes]);
                return Ok(Some(()));
            }
            Ok(None)
        })
        .await
    }

    async fn established_tcp_send_many(
        &mut self,
        connection: &GuestTcpConnection,
        payload: &[u8],
        iterations: usize,
    ) -> Result<()> {
        let mut sent = 0;
        let mut offset = 0;
        self.drive_until(|peer| {
            peer.poll_established_tcp_upload(connection, payload, iterations, &mut sent, &mut offset)?;
            let socket = peer.sockets.get::<tcp::Socket>(connection.handle);
            if sent == iterations && socket.send_queue() == 0 {
                return Ok(Some(()));
            }
            Ok(None)
        })
        .await
    }

    async fn tcp_roundtrip_until(&mut self, port: u16, request: &[u8], complete: TcpCompletion) -> Result<Vec<u8>> {
        let mut client = self.add_tcp_client();
        let handle = client.handle;
        let result = self
            .drive_until(|peer| {
                peer.poll_tcp_client(port, request, &mut client, complete)?;
                if client.complete {
                    return Ok(Some(std::mem::take(&mut client.response)));
                }
                Ok(None)
            })
            .await;
        let socket = self.sockets.get::<tcp::Socket>(handle);
        let socket_debug = format!(
            "state={:?} send_queue={} recv_queue={} may_recv={} may_send={}",
            socket.state(),
            socket.send_queue(),
            socket.recv_queue(),
            socket.may_recv(),
            socket.may_send()
        );
        if result.is_ok() {
            self.sockets.get_mut::<tcp::Socket>(handle).close();
            self.poll();
        }
        self.sockets.remove(handle);
        result.map_err(|error| {
            Error::new(format!(
                "{error}; received {} response bytes; client {socket_debug}",
                client.response.len()
            ))
        })
    }

    fn add_tcp_client(&mut self) -> GuestHttpClient {
        let rx_buffer = tcp::SocketBuffer::new(vec![0; TCP_BUFFER_BYTES]);
        let tx_buffer = tcp::SocketBuffer::new(vec![0; TCP_BUFFER_BYTES]);
        let socket = tcp_socket(rx_buffer, tx_buffer);
        let handle = self.sockets.add(socket);
        GuestHttpClient {
            handle,
            local_port: self.next_ephemeral_port(),
            request_offset: 0,
            response: Vec::new(),
            complete: false,
        }
    }

    fn add_tls_client(
        &mut self,
        port: u16,
        host: &str,
        ca_cert_pem: &str,
        request: &[u8],
    ) -> Result<GuestTlsHttpClient> {
        let config = TlsClientConfig::with_platform_roots(&[ca_cert_pem.to_owned()])
            .map_err(|error| Error::from_display("trust guest TLS root", error))?;
        let tls = TlsClientSession::connect(&config, host)
            .map_err(|error| Error::from_display("create guest TLS client", error))?;

        let rx_buffer = tcp::SocketBuffer::new(vec![0; TCP_BUFFER_BYTES]);
        let tx_buffer = tcp::SocketBuffer::new(vec![0; TCP_BUFFER_BYTES]);
        let socket = tcp_socket(rx_buffer, tx_buffer);
        let handle = self.sockets.add(socket);
        Ok(GuestTlsHttpClient {
            handle,
            port,
            local_port: self.next_ephemeral_port(),
            tls,
            request: request.to_vec(),
            request_offset: 0,
            request_written: false,
            tls_output: Vec::with_capacity(16 * 1024),
            tls_output_offset: 0,
            response: Vec::new(),
            tls_closed: false,
        })
    }

    fn add_tcp_connection(&mut self, port: u16) -> GuestTcpConnection {
        let rx_buffer = tcp::SocketBuffer::new(vec![0; TCP_BUFFER_BYTES]);
        let tx_buffer = tcp::SocketBuffer::new(vec![0; TCP_BUFFER_BYTES]);
        let socket = tcp_socket(rx_buffer, tx_buffer);
        let handle = self.sockets.add(socket);
        GuestTcpConnection {
            handle,
            port,
            local_port: self.next_ephemeral_port(),
            request: Vec::new(),
            request_offset: 0,
            response: Vec::new(),
            expected_bytes: 0,
        }
    }

    fn poll_tcp_client(
        &mut self,
        port: u16,
        request: &[u8],
        client: &mut GuestHttpClient,
        complete: TcpCompletion,
    ) -> Result<()> {
        let cx = self.iface.context();
        let socket = self.sockets.get_mut::<tcp::Socket>(client.handle);
        if !socket.is_open() {
            socket
                .connect(cx, (IpAddress::Ipv4(ipv4(GATEWAY_IP)), port), client.local_port)
                .map_err(|error| Error::from_display("connect guest TCP socket", error))?;
        }
        if socket.may_send() && client.request_offset < request.len() {
            let written = socket
                .send_slice(&request[client.request_offset..])
                .map_err(|error| Error::from_display("send HTTP request", error))?;
            client.request_offset += written;
        }
        while socket.can_recv() {
            socket
                .recv(|bytes| {
                    client.response.extend_from_slice(bytes);
                    (bytes.len(), ())
                })
                .map_err(|error| Error::from_display("receive HTTP response", error))?;
        }
        client.complete = complete.is_complete(&client.response)?;
        Ok(())
    }

    fn poll_established_tcp_connection(&mut self, connection: &mut GuestTcpConnection) -> Result<()> {
        let cx = self.iface.context();
        let socket = self.sockets.get_mut::<tcp::Socket>(connection.handle);
        if !socket.is_open() {
            socket
                .connect(
                    cx,
                    (IpAddress::Ipv4(ipv4(GATEWAY_IP)), connection.port),
                    connection.local_port,
                )
                .map_err(|error| Error::from_display("connect guest TCP socket", error))?;
        }
        if socket.may_send() && connection.request_offset < connection.request.len() {
            let written = socket
                .send_slice(&connection.request[connection.request_offset..])
                .map_err(|error| Error::from_display("send TCP request", error))?;
            connection.request_offset += written;
        }
        while socket.can_recv() {
            socket
                .recv(|bytes| {
                    connection.response.extend_from_slice(bytes);
                    (bytes.len(), ())
                })
                .map_err(|error| Error::from_display("receive TCP response", error))?;
        }
        Ok(())
    }

    fn poll_established_tcp_upload(
        &mut self,
        connection: &GuestTcpConnection,
        payload: &[u8],
        iterations: usize,
        sent: &mut usize,
        offset: &mut usize,
    ) -> Result<()> {
        let cx = self.iface.context();
        let socket = self.sockets.get_mut::<tcp::Socket>(connection.handle);
        if !socket.is_open() {
            socket
                .connect(
                    cx,
                    (IpAddress::Ipv4(ipv4(GATEWAY_IP)), connection.port),
                    connection.local_port,
                )
                .map_err(|error| Error::from_display("connect guest TCP socket", error))?;
        }
        while socket.may_send() && *sent < iterations {
            let written = socket
                .send_slice(&payload[*offset..])
                .map_err(|error| Error::from_display("send TCP upload bytes", error))?;
            if written == 0 {
                break;
            }
            *offset += written;
            if *offset == payload.len() {
                *sent += 1;
                *offset = 0;
            }
        }
        Ok(())
    }

    fn poll_tls_http_client(&mut self, client: &mut GuestTlsHttpClient) -> Result<()> {
        let cx = self.iface.context();
        let socket = self.sockets.get_mut::<tcp::Socket>(client.handle);
        if !socket.is_open() {
            socket
                .connect(cx, (IpAddress::Ipv4(ipv4(GATEWAY_IP)), client.port), client.local_port)
                .map_err(|error| Error::from_display("connect guest TLS TCP socket", error))?;
        }
        client.flush_tls_output()?;
        while socket.may_send() && client.has_tls_output() {
            let written = socket
                .send_slice(client.pending_tls_output())
                .map_err(|error| Error::from_display("send guest TLS bytes", error))?;
            client.advance_tls_output(written);
        }
        while socket.can_recv() {
            socket
                .recv(|bytes| {
                    let mut input = Cursor::new(&*bytes);
                    let read = client.read_tls_ciphertext(&mut input);
                    let consumed = usize::try_from(input.position()).unwrap_or(bytes.len());
                    (consumed, read)
                })
                .map_err(|error| Error::from_display("receive guest TLS bytes", error))??;
            client.drain_plaintext()?;
        }
        client.write_request_after_handshake()?;
        Ok(())
    }

    async fn drive_until<T, F>(&mut self, mut check: F) -> Result<T>
    where
        F: FnMut(&mut Self) -> Result<Option<T>>,
    {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            self.poll();
            if let Some(result) = check(self)? {
                return Ok(result);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::new("timed out waiting for guest peer traffic"));
            }
            self.wait_for_input().await?;
        }
    }

    async fn drive_until_future<T>(&mut self, future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        tokio::pin!(future);
        loop {
            self.poll();
            tokio::select! {
                result = &mut future => return result,
                () = tokio::task::yield_now() => {}
            }
        }
    }

    fn poll(&mut self) {
        while let Some(frame) = self.endpoint.try_recv() {
            self.device.receive(frame);
        }
        let now = smoltcp_now();
        while !matches!(
            self.iface.poll(now, &mut self.device, &mut self.sockets),
            smoltcp::iface::PollResult::None
        ) {}
        while let Some(frame) = self.device.transmit() {
            self.endpoint.send(frame);
        }
    }

    async fn wait_for_input(&mut self) -> Result<()> {
        if let Some(frame) = self.endpoint.try_recv() {
            self.device.receive(frame);
        } else {
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    const fn next_ephemeral_port(&mut self) -> u16 {
        let port = self.next_port;
        self.next_port = if self.next_port == 60_000 {
            40_000
        } else {
            self.next_port.saturating_add(1)
        };
        port
    }
}

struct GuestHttpClient {
    handle: SocketHandle,
    local_port: u16,
    request_offset: usize,
    response: Vec<u8>,
    complete: bool,
}

struct GuestTlsHttpClient {
    handle: SocketHandle,
    port: u16,
    local_port: u16,
    tls: TlsClientSession,
    request: Vec<u8>,
    request_offset: usize,
    request_written: bool,
    tls_output: Vec<u8>,
    tls_output_offset: usize,
    response: Vec<u8>,
    tls_closed: bool,
}

impl GuestTlsHttpClient {
    fn flush_tls_output(&mut self) -> Result<()> {
        if self.tls_output_offset == self.tls_output.len() {
            self.tls_output.clear();
            self.tls_output_offset = 0;
        }
        loop {
            let before = self.tls_output.len();
            self.tls
                .drain_ciphertext_to(&mut self.tls_output, usize::MAX)
                .map_err(|error| Error::from_display("write guest TLS records", error))?;
            if self.tls_output.len() == before {
                return Ok(());
            }
        }
    }

    fn write_request_after_handshake(&mut self) -> Result<()> {
        if self.request_written || self.tls.is_handshaking() {
            return Ok(());
        }
        while self.request_offset < self.request.len() {
            let written = self
                .tls
                .write_plaintext_some(&self.request[self.request_offset..])
                .map_err(|error| Error::from_display("write TLS HTTP request", error))?;
            match written {
                TlsPlaintextWrite::Accepted(written) => {
                    self.request_offset += written;
                }
                TlsPlaintextWrite::BlockedByPendingCiphertext => {
                    break;
                }
            }
        }
        self.request_written = self.request_offset == self.request.len();
        self.flush_tls_output()
    }

    fn read_tls_ciphertext(&mut self, input: &mut Cursor<&[u8]>) -> Result<()> {
        let remaining = input
            .get_ref()
            .len()
            .saturating_sub(usize::try_from(input.position()).unwrap_or(usize::MAX));
        match self
            .tls
            .read_ciphertext_bounded(input, remaining)
            .map_err(|error| Error::from_display("read guest TLS records", error))?
        {
            TlsCiphertextRead::Read(_read) => Ok(()),
            TlsCiphertextRead::Blocked => Err(Error::new("guest TLS blocked while consuming in-memory ciphertext")),
            TlsCiphertextRead::Closed => Err(Error::new("guest TLS closed while reading records")),
        }
    }

    fn drain_plaintext(&mut self) -> Result<()> {
        let mut plaintext = [0_u8; 16 * 1024];
        loop {
            match self
                .tls
                .read_plaintext_some(&mut plaintext)
                .map_err(|error| Error::from_display("read guest TLS plaintext", error))?
            {
                TlsPlaintextRead::Plaintext(read) => self.response.extend_from_slice(&plaintext[..read]),
                TlsPlaintextRead::Blocked => return Ok(()),
                TlsPlaintextRead::Closed => {
                    self.tls_closed = true;
                    return Ok(());
                }
            }
        }
    }

    const fn has_tls_output(&self) -> bool {
        self.tls_output_offset < self.tls_output.len()
    }

    fn pending_tls_output(&self) -> &[u8] {
        &self.tls_output[self.tls_output_offset..]
    }

    fn advance_tls_output(&mut self, written: usize) {
        self.tls_output_offset = self
            .tls_output_offset
            .saturating_add(written)
            .min(self.tls_output.len());
    }
}

struct GuestUdpConnection {
    handle: SocketHandle,
    port: u16,
}

struct GuestTcpConnection {
    handle: SocketHandle,
    port: u16,
    local_port: u16,
    request: Vec<u8>,
    request_offset: usize,
    response: Vec<u8>,
    expected_bytes: usize,
}

fn tcp_socket(rx: tcp::SocketBuffer<'static>, tx: tcp::SocketBuffer<'static>) -> tcp::Socket<'static> {
    let mut socket = tcp::Socket::new(rx, tx);
    socket.set_ack_delay(None);
    socket.set_nagle_enabled(false);
    socket
}

#[derive(Clone, Copy)]
enum TcpCompletion {
    Http,
}

impl TcpCompletion {
    fn is_complete(self, response: &[u8]) -> Result<bool> {
        match self {
            Self::Http => http_message_complete(response),
        }
    }
}

struct PeerDevice {
    rx: VecDeque<PooledFrame>,
    tx: VecDeque<PooledFrame>,
    frames: SharedFramePool,
}

impl PeerDevice {
    fn new() -> Self {
        Self {
            rx: VecDeque::with_capacity(HARNESS_FRAME_POOL_FRAMES),
            tx: VecDeque::with_capacity(HARNESS_FRAME_POOL_FRAMES),
            frames: SharedFramePool::prewarmed(HARNESS_FRAME_POOL_FRAMES, HARNESS_FRAME_CAPACITY),
        }
    }

    fn receive(&mut self, frame: PooledFrame) {
        self.rx.push_back(frame);
    }

    fn transmit(&mut self) -> Option<PooledFrame> {
        self.tx.pop_front()
    }
}

struct PeerRxToken {
    frame: PooledFrame,
}

struct PeerTxToken<'a> {
    device: &'a mut PeerDevice,
}

impl Device for PeerDevice {
    type RxToken<'a> = PeerRxToken;
    type TxToken<'a> = PeerTxToken<'a>;

    fn receive(&mut self, _timestamp: SmoltcpInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let frame = self.rx.pop_front()?;
        Some((PeerRxToken { frame }, PeerTxToken { device: self }))
    }

    fn transmit(&mut self, _timestamp: SmoltcpInstant) -> Option<Self::TxToken<'_>> {
        Some(PeerTxToken { device: self })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ethernet;
        caps.max_transmission_unit = 1500 + ETHERNET_HEADER_LEN;
        caps
    }
}

impl RxToken for PeerRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(self.frame.as_slice())
    }
}

impl TxToken for PeerTxToken<'_> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut frame = self.device.frames.frame(len);
        frame.as_mut_vec().resize(len, 0);
        let result = f(frame.as_mut_vec());
        self.device.tx.push_back(frame);
        result
    }
}

struct PersistentHttpServer {
    port: u16,
    stop_tx: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl PersistentHttpServer {
    async fn start(response: Vec<u8>) -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(|error| Error::from_display("bind persistent HTTP server", error))?;
        let port = listener
            .local_addr()
            .map_err(|error| Error::from_display("read persistent HTTP server address", error))?
            .port();
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return Ok(()),
                    accepted = listener.accept() => {
                        let (mut stream, _peer) = accepted?;
                        let response = response.clone();
                        tokio::spawn(async move {
                            read_http_message(&mut stream).await?;
                            stream.write_all(&response).await?;
                            stream.shutdown().await
                        });
                    }
                }
            }
        });
        Ok(Self { port, stop_tx, task })
    }

    async fn stop(self) -> Result<()> {
        let _sent = self.stop_tx.send(());
        self.task
            .await
            .map_err(|error| Error::from_display("join persistent HTTP server", error))?
            .map_err(|error| Error::from_display("run persistent HTTP server", error))
    }
}

struct PersistentUdpSink {
    port: u16,
    received: Arc<AtomicUsize>,
    stop_tx: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl PersistentUdpSink {
    async fn start() -> Result<Self> {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(|error| Error::from_display("bind persistent UDP sink", error))?;
        let port = socket
            .local_addr()
            .map_err(|error| Error::from_display("read persistent UDP sink address", error))?
            .port();
        let received = Arc::new(AtomicUsize::new(0));
        let task_received = Arc::clone(&received);
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut bytes = vec![0_u8; UDP_BUFFER_BYTES];
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return Ok(()),
                    datagram = socket.recv_from(&mut bytes) => {
                        let (read, _peer) = datagram?;
                        task_received.fetch_add(read, Ordering::Relaxed);
                    }
                }
            }
        });
        Ok(Self {
            port,
            received,
            stop_tx,
            task,
        })
    }

    async fn stop(self) -> Result<()> {
        let _sent = self.stop_tx.send(());
        self.task
            .await
            .map_err(|error| Error::from_display("join persistent UDP sink", error))?
            .map_err(|error| Error::from_display("run persistent UDP sink", error))
    }
}

struct PersistentTcpEchoServer {
    port: u16,
    stop_tx: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl PersistentTcpEchoServer {
    async fn start() -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(|error| Error::from_display("bind persistent TCP server", error))?;
        let port = listener
            .local_addr()
            .map_err(|error| Error::from_display("read persistent TCP server address", error))?
            .port();
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return Ok(()),
                    accepted = listener.accept() => {
                        let (mut stream, _peer) = accepted?;
                        tokio::spawn(async move {
                            let mut bytes = Vec::new();
                            loop {
                                let mut len = [0_u8; size_of::<u32>()];
                                if let Err(error) = stream.read_exact(&mut len).await {
                                    return match error.kind() {
                                        std::io::ErrorKind::UnexpectedEof
                                        | std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::BrokenPipe => Ok(()),
                                        _ => Err(error),
                                    };
                                }
                                bytes.resize(u32::from_be_bytes(len) as usize, 0);
                                stream.read_exact(&mut bytes).await?;
                                stream.write_all(&bytes).await?;
                            }
                        });
                    }
                }
            }
        });
        Ok(Self { port, stop_tx, task })
    }

    async fn stop(self) -> Result<()> {
        let _sent = self.stop_tx.send(());
        self.task
            .await
            .map_err(|error| Error::from_display("join persistent TCP server", error))?
            .map_err(|error| Error::from_display("run persistent TCP server", error))
    }
}

struct PersistentTcpSink {
    port: u16,
    received: Arc<AtomicUsize>,
    stop_tx: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl PersistentTcpSink {
    async fn start() -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(|error| Error::from_display("bind persistent TCP sink", error))?;
        let port = listener
            .local_addr()
            .map_err(|error| Error::from_display("read persistent TCP sink address", error))?
            .port();
        let received = Arc::new(AtomicUsize::new(0));
        let task_received = Arc::clone(&received);
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut stream, _peer) = tokio::select! {
                _ = &mut stop_rx => return Ok(()),
                accepted = listener.accept() => accepted?,
            };
            let mut bytes = vec![0_u8; TCP_BUFFER_BYTES];
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return Ok(()),
                    read = stream.read(&mut bytes) => {
                        match read {
                            Ok(0) => return Ok(()),
                            Ok(read) => {
                                task_received.fetch_add(read, Ordering::Relaxed);
                            }
                            Err(error)
                                if matches!(
                                    error.kind(),
                                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                                ) =>
                            {
                                return Ok(());
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
        });
        Ok(Self {
            port,
            received,
            stop_tx,
            task,
        })
    }

    async fn stop(self) -> Result<()> {
        let _sent = self.stop_tx.send(());
        self.task
            .await
            .map_err(|error| Error::from_display("join persistent TCP sink", error))?
            .map_err(|error| Error::from_display("run persistent TCP sink", error))
    }
}

async fn wait_for_sink_bytes(received: Arc<AtomicUsize>, expected: usize) -> Result<()> {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let current = received.load(Ordering::Relaxed);
        if current >= expected {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::new(format!(
                "timed out waiting for sink bytes; received={current} expected={expected}"
            )));
        }
        tokio::task::yield_now().await;
    }
}

async fn wait_for_sink_quiescent(received: Arc<AtomicUsize>, minimum: usize) -> Result<usize> {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let mut previous = minimum;
    let mut unchanged_since = tokio::time::Instant::now();
    loop {
        let current = received.load(Ordering::Relaxed);
        if current != previous {
            previous = current;
            unchanged_since = tokio::time::Instant::now();
        } else if current > minimum && unchanged_since.elapsed() >= Duration::from_millis(2) {
            return Ok(current);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(current);
        }
        tokio::task::yield_now().await;
    }
}

#[derive(Clone)]
struct SharedFramePool {
    frames: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl SharedFramePool {
    fn prewarmed(frames: usize, capacity: usize) -> Self {
        let mut pool = Vec::with_capacity(frames);
        for _ in 0..frames {
            pool.push(Vec::with_capacity(capacity));
        }
        Self {
            frames: Arc::new(Mutex::new(pool)),
        }
    }

    fn frame(&self, capacity: usize) -> PooledFrame {
        let mut frame = self
            .frames
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(capacity));
        if frame.capacity() < capacity {
            frame.reserve(capacity - frame.capacity());
        }
        frame.clear();
        PooledFrame {
            frame,
            pool: self.clone(),
        }
    }

    fn recycle(&self, mut frame: Vec<u8>) {
        frame.clear();
        self.frames
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(frame);
    }
}

struct PooledFrame {
    frame: Vec<u8>,
    pool: SharedFramePool,
}

impl PooledFrame {
    fn as_slice(&self) -> &[u8] {
        &self.frame
    }

    const fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.frame
    }
}

impl Drop for PooledFrame {
    fn drop(&mut self) {
        self.pool.recycle(std::mem::take(&mut self.frame));
    }
}

#[derive(Clone)]
struct FrameQueue {
    inner: Arc<Mutex<FrameQueueInner>>,
    reactor_stream: Arc<Mutex<Option<std::os::unix::net::UnixStream>>>,
    wake_stream: Arc<Mutex<std::os::unix::net::UnixStream>>,
}

#[derive(Default)]
struct FrameQueueInner {
    frames: VecDeque<PooledFrame>,
    closed: bool,
}

impl FrameQueue {
    fn new() -> Result<Self> {
        let (reactor_stream, wake_stream) = std::os::unix::net::UnixStream::pair()
            .map_err(|error| Error::from_display("create memory transport wake socket", error))?;
        reactor_stream
            .set_nonblocking(true)
            .map_err(|error| Error::from_display("configure memory transport reactor socket", error))?;
        wake_stream
            .set_nonblocking(true)
            .map_err(|error| Error::from_display("configure memory transport wake socket", error))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(FrameQueueInner {
                frames: VecDeque::with_capacity(4096),
                closed: false,
            })),
            reactor_stream: Arc::new(Mutex::new(Some(reactor_stream))),
            wake_stream: Arc::new(Mutex::new(wake_stream)),
        })
    }

    fn push(&self, frame: PooledFrame) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .frames
            .push_back(frame);
        self.wake();
    }

    fn try_pop(&self) -> Option<PooledFrame> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .frames
            .pop_front()
    }

    fn has_frames(&self) -> bool {
        !self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .frames
            .is_empty()
    }

    fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .frames
            .len()
    }

    fn is_closed(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed
    }

    fn close(&self) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
        self.wake();
    }

    fn take_reactor_stream(&self) -> std::result::Result<std::os::unix::net::UnixStream, TransportError> {
        self.reactor_stream
            .lock()
            .map_err(|error| TransportError::operation("lock memory transport wake stream", error))?
            .take()
            .ok_or_else(|| TransportError::operation("connect memory transport", "session already connected"))
    }

    fn drain_wake(stream: &mut std::os::unix::net::UnixStream) -> std::result::Result<(), TransportError> {
        let mut buffer = [0_u8; 64];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => return Ok(()),
                Ok(_len) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => return Err(TransportError::operation("drain memory transport wake stream", error)),
            }
        }
    }

    fn wake(&self) {
        if let Ok(mut stream) = self.wake_stream.lock() {
            let _sent = stream.write(&[1]);
        }
    }
}

struct GuestEndpoint {
    to_network: FrameQueue,
    from_network: FrameQueue,
}

impl GuestEndpoint {
    fn send(&self, frame: PooledFrame) {
        self.to_network.push(frame);
    }

    fn try_recv(&self) -> Option<PooledFrame> {
        self.from_network.try_pop()
    }

    fn queued_to_network_frames(&self) -> usize {
        self.to_network.len()
    }
}

#[derive(Clone)]
struct MemoryTransport {
    frames: SharedFramePool,
    frames_to_network: FrameQueue,
    frames_from_network: FrameQueue,
    connected: Arc<Mutex<bool>>,
}

impl MemoryTransport {
    fn new() -> Result<(Self, GuestEndpoint)> {
        let frames = SharedFramePool::prewarmed(HARNESS_FRAME_POOL_FRAMES, HARNESS_FRAME_CAPACITY);
        let frames_to_network = FrameQueue::new()?;
        let frames_from_network = FrameQueue::new()?;
        Ok((
            Self {
                frames,
                frames_to_network: frames_to_network.clone(),
                frames_from_network: frames_from_network.clone(),
                connected: Arc::new(Mutex::new(false)),
            },
            GuestEndpoint {
                to_network: frames_to_network,
                from_network: frames_from_network,
            },
        ))
    }
}

impl GuestFrameTransport for MemoryTransport {
    type Session = MemorySession;

    fn try_connect(&mut self) -> std::result::Result<ConnectStatus<Self::Session>, TransportError> {
        let mut connected = self
            .connected
            .lock()
            .map_err(|error| TransportError::operation("lock memory transport", error))?;
        if *connected {
            return Err(TransportError::operation(
                "connect memory transport",
                "session already connected",
            ));
        }
        *connected = true;
        drop(connected);
        Ok(ConnectStatus::Connected(MemorySession {
            frames: self.frames.clone(),
            frames_to_network: self.frames_to_network.clone(),
            frames_from_network: self.frames_from_network.clone(),
            wake_stream: self.frames_to_network.take_reactor_stream()?,
        }))
    }

    fn cleanup(self) -> std::result::Result<(), TransportError> {
        self.frames_to_network.close();
        self.frames_from_network.close();
        Ok(())
    }

    fn describe(&self) -> String {
        "memory".to_owned()
    }
}

struct MemorySession {
    frames: SharedFramePool,
    frames_to_network: FrameQueue,
    frames_from_network: FrameQueue,
    wake_stream: std::os::unix::net::UnixStream,
}

impl GuestFrameSession for MemorySession {
    fn io_source(&mut self) -> GuestIoSource<'_> {
        GuestIoSource::Fd(self.wake_stream.as_fd())
    }

    fn read_frame_into(&mut self, frame: &mut FrameBuf) -> std::result::Result<FrameRead, TransportError> {
        FrameQueue::drain_wake(&mut self.wake_stream)?;
        let Some(bytes) = self.frames_to_network.try_pop() else {
            return if self.frames_to_network.is_closed() {
                Ok(FrameRead::Closed)
            } else {
                Ok(FrameRead::Blocked)
            };
        };
        if self.frames_to_network.has_frames() {
            self.frames_to_network.wake();
        }
        frame.as_mut_vec().clear();
        frame.as_mut_vec().extend_from_slice(bytes.as_slice());
        Ok(FrameRead::Frame)
    }

    fn write_frame(&mut self, frame: &[u8]) -> std::result::Result<FrameWrite, TransportError> {
        let mut output = self.frames.frame(frame.len());
        output.as_mut_vec().extend_from_slice(frame);
        self.frames_from_network.push(output);
        Ok(FrameWrite::Flushed)
    }

    fn shutdown_write(&mut self) -> std::result::Result<(), TransportError> {
        Ok(())
    }
}

async fn read_http_message(stream: &mut (impl AsyncReadExt + Unpin)) -> std::io::Result<Vec<u8>> {
    let mut message = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "HTTP peer closed before complete message",
            ));
        }
        message.extend_from_slice(&buffer[..read]);
        if http_message_complete(&message).map_err(std::io::Error::other)? {
            return Ok(message);
        }
    }
}

fn http_message_complete(message: &[u8]) -> Result<bool> {
    let Some(header_end) = header_end(message) else {
        return Ok(false);
    };
    let headers = std::str::from_utf8(&message[..header_end])
        .map_err(|error| Error::from_display("parse HTTP headers", error))?;
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    Ok(message.len() >= header_end + 4 + content_length)
}

fn header_end(message: &[u8]) -> Option<usize> {
    message.windows(4).position(|window| window == b"\r\n\r\n")
}

fn dns_query(id: u16, host: &str) -> Vec<u8> {
    let mut query = Vec::with_capacity(32 + host.len());
    query.extend_from_slice(&id.to_be_bytes());
    query.extend_from_slice(&[0x01, 0x00]);
    query.extend_from_slice(&[0x00, 0x01]);
    query.extend_from_slice(&[0x00, 0x00]);
    query.extend_from_slice(&[0x00, 0x00]);
    query.extend_from_slice(&[0x00, 0x00]);
    push_dns_name(&mut query, host);
    query.extend_from_slice(&[0x00, 0x01]);
    query.extend_from_slice(&[0x00, 0x01]);
    query
}

fn dns_response_has_answer(response: &[u8]) -> bool {
    response.len() >= 12 && u16::from_be_bytes([response[6], response[7]]) > 0
}

pub(crate) fn dns_response(query: &[u8], address: Ipv4Addr) -> std::io::Result<Vec<u8>> {
    if query.len() < 12 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "DNS query is too short",
        ));
    }
    let question_end = dns_question_end(query)?;
    let mut response = Vec::with_capacity(question_end + 16);
    response.extend_from_slice(&query[..2]);
    response.extend_from_slice(&[0x81, 0x80]);
    response.extend_from_slice(&[0x00, 0x01]);
    response.extend_from_slice(&[0x00, 0x01]);
    response.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
    response.extend_from_slice(&query[12..question_end]);
    response.extend_from_slice(&[0xc0, 0x0c]);
    response.extend_from_slice(&[0x00, 0x01]);
    response.extend_from_slice(&[0x00, 0x01]);
    response.extend_from_slice(&[0x00, 0x00, 0x00, 0x3c]);
    response.extend_from_slice(&[0x00, 0x04]);
    response.extend_from_slice(&address.octets());
    Ok(response)
}

fn dns_question_end(query: &[u8]) -> std::io::Result<usize> {
    let mut index = 12;
    while index < query.len() {
        let label_len = usize::from(query[index]);
        index += 1;
        if label_len == 0 {
            let end = index + 4;
            return (end <= query.len()).then_some(end).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "DNS question is missing type/class")
            });
        }
        index = index.saturating_add(label_len);
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "DNS question name is unterminated",
    ))
}

fn push_dns_name(output: &mut Vec<u8>, host: &str) {
    for label in host.trim_end_matches('.').split('.') {
        output.push(u8::try_from(label.len()).unwrap_or(0));
        output.extend_from_slice(label.as_bytes());
    }
    output.push(0);
}

fn smoltcp_now() -> SmoltcpInstant {
    SmoltcpInstant::from_micros(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_micros().try_into().unwrap_or(i64::MAX)),
    )
}

pub(super) const fn mediated_network_mac() -> InstanceMacAddresses {
    let profile = agentdp_core::mediated_network::DEFAULT_PROFILE;
    InstanceMacAddresses {
        gateway: MacAddress::new(profile.gateway_mac.octets()),
        guest: MacAddress::new(profile.guest_mac.octets()),
    }
}

pub(super) const fn mediated_network_addresses() -> InstanceAddresses {
    let profile = agentdp_core::mediated_network::DEFAULT_PROFILE;
    InstanceAddresses {
        gateway: Ipv4AddressText(ipv4(profile.gateway_ipv4)),
        address: Ipv4AddressText(ipv4(profile.guest_ipv4)),
        cidr_prefix: profile.ipv4_cidr_prefix,
    }
}

const fn guest_mac() -> EthernetAddress {
    EthernetAddress(agentdp_core::mediated_network::DEFAULT_PROFILE.guest_mac.octets())
}

const fn ipv4(address: Ipv4Addr) -> Ipv4Address {
    let [a, b, c, d] = address.octets();
    Ipv4Address::new(a, b, c, d)
}

#[must_use]
pub fn payload(size: usize) -> Vec<u8> {
    (0..size).map(|index| u8::try_from(index % 251).unwrap_or(0)).collect()
}

#[must_use]
pub fn http_get_request(path: &str, host: &str, body_size: usize) -> Vec<u8> {
    let body = payload(body_size);
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nhost: {host}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    request
}

#[must_use]
pub(super) fn http_response(body_size: usize) -> Vec<u8> {
    let body = payload(body_size);
    let mut response = format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(&body);
    response
}
