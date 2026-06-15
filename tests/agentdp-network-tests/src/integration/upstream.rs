use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::oneshot;

use agentdp_crypto::{
    CertificateAuthority, CertificateAuthorityPem, CertificateValidity, TlsCiphertextRead, TlsPlaintextRead,
    TlsPlaintextWrite, TlsServerConfig, TlsServerSession,
};
use agentdp_network::{EgressPolicy, InstanceNetworkConfig};

use super::{
    AsyncDataplane, DEFAULT_AGENT_WORKFLOW_HOST, DEFAULT_SECRET_PLACEHOLDER, DEFAULT_SECRET_VALUE, Error, GATEWAY_IP,
    NetworkPolicy, Result, RuntimeSecret, RuntimeSecrets, TlsInterceptConfig, dns_response, http_get_request,
    http_response, mediated_network_addresses, mediated_network_mac,
};

#[derive(Debug, Clone)]
struct UpstreamFleetConfig {
    host: String,
    https_response_body_size: usize,
}

impl UpstreamFleetConfig {
    #[must_use]
    fn https_http1(response_body_size: usize) -> Self {
        Self {
            host: DEFAULT_AGENT_WORKFLOW_HOST.to_owned(),
            https_response_body_size: response_body_size,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct UpstreamFleetMetrics {
    dns_queries: u64,
    https_connections: u64,
    https_requests: u64,
    https_errors: u64,
    https_bytes_in: u64,
    https_bytes_out: u64,
}

#[derive(Default)]
struct SharedMetrics {
    dns_queries: AtomicU64,
    https_connections: AtomicU64,
    https_requests: AtomicU64,
    https_errors: AtomicU64,
    https_bytes_in: AtomicU64,
    https_bytes_out: AtomicU64,
}

struct UpstreamFleet {
    config: UpstreamFleetConfig,
    identity: TlsIdentity,
    dns: DnsUpstream,
    https: HttpsUpstream,
    metrics: Arc<SharedMetrics>,
}

impl UpstreamFleet {
    /// # Errors
    ///
    /// Returns an error when the DNS or HTTPS upstream cannot bind or initialize TLS material.
    async fn start(config: UpstreamFleetConfig) -> Result<Self> {
        let metrics = Arc::new(SharedMetrics::default());
        let identity = TlsIdentity::generate(&config.host)?;
        let dns = DnsUpstream::start(metrics.clone()).await?;
        let https = HttpsUpstream::start(
            config.host.clone(),
            http_response(config.https_response_body_size),
            identity.server_config(),
            metrics.clone(),
        )
        .await?;
        Ok(Self {
            config,
            identity,
            dns,
            https,
            metrics,
        })
    }

    #[must_use]
    fn host(&self) -> &str {
        &self.config.host
    }

    #[must_use]
    const fn dns_port(&self) -> u16 {
        self.dns.port
    }

    #[must_use]
    const fn https_port(&self) -> u16 {
        self.https.port
    }

    #[must_use]
    fn root_ca_pem(&self) -> &str {
        &self.identity.root_ca_pem
    }

    #[must_use]
    fn metrics(&self) -> UpstreamFleetMetrics {
        UpstreamFleetMetrics {
            dns_queries: self.metrics.dns_queries.load(Ordering::Relaxed),
            https_connections: self.metrics.https_connections.load(Ordering::Relaxed),
            https_requests: self.metrics.https_requests.load(Ordering::Relaxed),
            https_errors: self.metrics.https_errors.load(Ordering::Relaxed),
            https_bytes_in: self.metrics.https_bytes_in.load(Ordering::Relaxed),
            https_bytes_out: self.metrics.https_bytes_out.load(Ordering::Relaxed),
        }
    }

    /// # Errors
    ///
    /// Returns an error when either upstream task fails while shutting down.
    async fn stop(self) -> Result<()> {
        self.dns.stop().await?;
        self.https.stop().await
    }
}

pub struct AgentWorkflowHarness {
    dataplane: AsyncDataplane,
    fleet: UpstreamFleet,
    guest_ca_pem: String,
}

impl AgentWorkflowHarness {
    /// # Errors
    ///
    /// Returns an error when the upstream fleet or instance network cannot start.
    pub async fn start_https_http1(response_body_size: usize) -> Result<Self> {
        let fleet = UpstreamFleet::start(UpstreamFleetConfig::https_http1(response_body_size)).await?;
        let mediated_ca =
            CertificateAuthorityPem::generate().map_err(|error| Error::from_display("generate mediated CA", error))?;
        let mut dataplane = AsyncDataplane::start_with_config(network_config(&fleet, &mediated_ca))?;
        dataplane.resolve_host(fleet.dns_port(), fleet.host()).await?;
        Ok(Self {
            dataplane,
            fleet,
            guest_ca_pem: mediated_ca.cert_pem,
        })
    }

    /// # Errors
    ///
    /// Returns an error when the upstream fleet or direct instance network cannot start.
    pub async fn start_direct_https_http1(response_body_size: usize) -> Result<Self> {
        let fleet = UpstreamFleet::start(UpstreamFleetConfig::https_http1(response_body_size)).await?;
        let mut config = InstanceNetworkConfig::new(
            mediated_network_addresses(),
            mediated_network_mac(),
            EgressPolicy::allow_all(),
        );
        config.limits.tcp_proxy_limit = 128;
        let dataplane = AsyncDataplane::start_with_config(config)?;
        let guest_ca_pem = fleet.root_ca_pem().to_owned();
        Ok(Self {
            dataplane,
            fleet,
            guest_ca_pem,
        })
    }

    #[must_use]
    pub fn host(&self) -> &str {
        self.fleet.host()
    }

    #[must_use]
    fn metrics(&self) -> UpstreamFleetMetrics {
        self.fleet.metrics()
    }

    /// # Errors
    ///
    /// Returns an error when the HTTPS request does not complete through the instance network.
    pub async fn https_http1_roundtrip(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        self.dataplane
            .https_http1_roundtrip(
                self.fleet.host(),
                self.fleet.https_port(),
                self.guest_ca_pem.as_str(),
                request,
            )
            .await
            .map_err(|error| {
                Error::new(format!(
                    "HTTPS workflow roundtrip failed: {error}; metrics={:?}; status={:?}",
                    self.metrics(),
                    self.dataplane.status()
                ))
            })
    }

    /// # Errors
    ///
    /// Returns an error when the instance network or upstream fleet cannot stop cleanly.
    pub async fn shutdown(self) -> Result<()> {
        let Self {
            dataplane,
            fleet,
            guest_ca_pem: _,
        } = self;
        dataplane.shutdown().await?;
        fleet.stop().await
    }
}

#[must_use]
pub fn agent_https_request(host: &str, body_size: usize) -> Vec<u8> {
    let mut request = http_get_request("/agent-workflow", host, body_size);
    let header = format!("authorization: Bearer {DEFAULT_SECRET_PLACEHOLDER}\r\n");
    let insert = request
        .windows(2)
        .position(|window| window == b"\r\n")
        .map_or(request.len(), |index| index + 2);
    request.splice(insert..insert, header.bytes());
    request
}

#[must_use]
pub fn agent_https_response(body_size: usize) -> Vec<u8> {
    http_response(body_size)
}

fn network_config(fleet: &UpstreamFleet, mediated_ca: &CertificateAuthorityPem) -> InstanceNetworkConfig {
    let egress = EgressPolicy::allow_all().with_allowed_authority(fleet.host());
    let mut secrets = RuntimeSecrets::new();
    secrets.insert(RuntimeSecret::new(
        DEFAULT_SECRET_PLACEHOLDER,
        DEFAULT_SECRET_VALUE,
        [fleet.host().to_owned()],
    ));
    let mut config = InstanceNetworkConfig::new(mediated_network_addresses(), mediated_network_mac(), egress.clone());
    config.limits.tcp_proxy_limit = 128;
    config.policy = NetworkPolicy::new(egress).with_secrets(secrets);
    config.dns_upstream = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), fleet.dns_port());
    config.tls = Some(TlsInterceptConfig {
        ca_cert_pem: mediated_ca.cert_pem.clone(),
        ca_key_pem: mediated_ca.key_pem.clone(),
        upstream_root_ca_pems: vec![fleet.root_ca_pem().to_owned()],
        intercepted_ports: vec![fleet.https_port()],
        bypass_hosts: Vec::new(),
    });
    config
}

struct DnsUpstream {
    port: u16,
    stop_tx: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl DnsUpstream {
    async fn start(metrics: Arc<SharedMetrics>) -> Result<Self> {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(|error| Error::from_display("bind upstream DNS server", error))?;
        let port = socket
            .local_addr()
            .map_err(|error| Error::from_display("read upstream DNS address", error))?
            .port();
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut bytes = vec![0_u8; 512];
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return Ok(()),
                    received = socket.recv_from(&mut bytes) => {
                        let (read, peer) = received?;
                        let response = dns_response(&bytes[..read], GATEWAY_IP)?;
                        metrics.dns_queries.fetch_add(1, Ordering::Relaxed);
                        socket.send_to(&response, peer).await?;
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
            .map_err(|error| Error::from_display("join upstream DNS server", error))?
            .map_err(|error| Error::from_display("run upstream DNS server", error))
    }
}

struct HttpsUpstream {
    port: u16,
    stop_tx: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl HttpsUpstream {
    async fn start(
        host: String,
        response: Vec<u8>,
        server_config: TlsServerConfig,
        metrics: Arc<SharedMetrics>,
    ) -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(|error| Error::from_display("bind upstream HTTPS server", error))?;
        let port = listener
            .local_addr()
            .map_err(|error| Error::from_display("read upstream HTTPS address", error))?
            .port();
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return Ok(()),
                    accepted = listener.accept() => {
                        let (stream, _peer) = accepted?;
                        let server_config = server_config.clone();
                        let response = response.clone();
                        let metrics = metrics.clone();
                        let host = host.clone();
                        metrics.https_connections.fetch_add(1, Ordering::Relaxed);
                        tokio::spawn(async move {
                            if let Err(error) =
                                Box::pin(handle_https_connection(stream, server_config, response, metrics.clone(), &host)).await
                            {
                                metrics.https_errors.fetch_add(1, Ordering::Relaxed);
                                eprintln!("agentdp-network upstream HTTPS error: {error}");
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
            .map_err(|error| Error::from_display("join upstream HTTPS server", error))?
            .map_err(|error| Error::from_display("run upstream HTTPS server", error))
    }
}

async fn handle_https_connection(
    mut stream: tokio::net::TcpStream,
    server_config: TlsServerConfig,
    response: Vec<u8>,
    metrics: Arc<SharedMetrics>,
    host: &str,
) -> std::io::Result<()> {
    let mut tls = TlsServerSession::accept(&server_config).map_err(std::io::Error::other)?;
    let request = Box::pin(read_tls_http_message(&mut stream, &mut tls)).await?;
    if !request
        .windows(host.len())
        .any(|window| window.eq_ignore_ascii_case(host.as_bytes()))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "upstream HTTPS request did not contain expected host",
        ));
    }
    metrics.https_requests.fetch_add(1, Ordering::Relaxed);
    metrics
        .https_bytes_in
        .fetch_add(u64::try_from(request.len()).unwrap_or(u64::MAX), Ordering::Relaxed);
    metrics
        .https_bytes_out
        .fetch_add(u64::try_from(response.len()).unwrap_or(u64::MAX), Ordering::Relaxed);
    write_tls_plaintext(&mut stream, &mut tls, &response).await?;
    tls.queue_close_notify();
    drain_tls_ciphertext(&mut stream, &mut tls).await?;
    stream.shutdown().await
}

struct TlsIdentity {
    root_ca_pem: String,
    server_config: TlsServerConfig,
}

impl TlsIdentity {
    fn generate(host: &str) -> Result<Self> {
        let ca_pem = CertificateAuthorityPem::generate()
            .map_err(|error| Error::from_display("generate upstream TLS CA", error))?;
        let ca = CertificateAuthority::load(&ca_pem.cert_pem, &ca_pem.key_pem)
            .map_err(|error| Error::from_display("load upstream TLS CA", error))?;
        let server_config = ca
            .server_config_for_host(
                host,
                CertificateValidity::valid_for(Duration::from_hours(1), Duration::from_mins(1)),
            )
            .map_err(|error| Error::from_display("build upstream TLS server config", error))?;
        Ok(Self {
            root_ca_pem: ca_pem.cert_pem,
            server_config,
        })
    }

    fn server_config(&self) -> TlsServerConfig {
        self.server_config.clone()
    }
}

async fn read_tls_http_message(
    stream: &mut tokio::net::TcpStream,
    tls: &mut TlsServerSession,
) -> std::io::Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut ciphertext = [0_u8; 16 * 1024];
    loop {
        drain_tls_ciphertext(stream, tls).await?;
        drain_tls_plaintext(tls, &mut request)?;
        if super::http_message_complete(&request).map_err(std::io::Error::other)? {
            return Ok(request);
        }
        let read = stream.read(&mut ciphertext).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "TLS peer closed before complete HTTP request",
            ));
        }
        feed_tls_ciphertext(tls, &ciphertext[..read])?;
    }
}

fn feed_tls_ciphertext(tls: &mut TlsServerSession, mut bytes: &[u8]) -> std::io::Result<()> {
    while !bytes.is_empty() {
        match tls.accept_ciphertext_bounded(bytes)? {
            TlsCiphertextRead::Read(read) => bytes = &bytes[read..],
            TlsCiphertextRead::Blocked => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "server TLS blocked while consuming in-memory ciphertext",
                ));
            }
            TlsCiphertextRead::Closed => return Err(std::io::ErrorKind::UnexpectedEof.into()),
        }
    }
    Ok(())
}

fn drain_tls_plaintext(tls: &mut TlsServerSession, output: &mut Vec<u8>) -> std::io::Result<()> {
    let mut plaintext = [0_u8; 16 * 1024];
    loop {
        match tls.read_plaintext_some(&mut plaintext)? {
            TlsPlaintextRead::Plaintext(read) => output.extend_from_slice(&plaintext[..read]),
            TlsPlaintextRead::Blocked | TlsPlaintextRead::Closed => return Ok(()),
        }
    }
}

async fn write_tls_plaintext(
    stream: &mut tokio::net::TcpStream,
    tls: &mut TlsServerSession,
    plaintext: &[u8],
) -> std::io::Result<()> {
    let mut offset = 0_usize;
    while offset < plaintext.len() {
        match tls.write_plaintext_some(&plaintext[offset..])? {
            TlsPlaintextWrite::Accepted(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            TlsPlaintextWrite::Accepted(written) => offset = offset.saturating_add(written),
            TlsPlaintextWrite::BlockedByPendingCiphertext => {}
        }
        drain_tls_ciphertext(stream, tls).await?;
    }
    Ok(())
}

async fn drain_tls_ciphertext(stream: &mut tokio::net::TcpStream, tls: &mut TlsServerSession) -> std::io::Result<()> {
    let mut output = Vec::new();
    tls.drain_ciphertext_to(&mut output)?;
    if !output.is_empty() {
        stream.write_all(&output).await?;
    }
    Ok(())
}
