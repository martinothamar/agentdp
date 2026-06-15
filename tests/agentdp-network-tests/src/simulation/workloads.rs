use std::cell::RefCell;
use std::fmt::Write as _;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;

use agentdp_crypto::TlsClientSession;
use agentdp_network::test_support::simulation::SimulationUpstreams;
use agentdp_network::{RuntimeSecret, RuntimeSecrets};
use agentdp_rand::Seed;

use super::checkers::{NoUnexpectedEgressErrors, ProgressComplete, Quiescent, check_all};
use super::fixtures::{
    DNS_UPSTREAM, HOST, HTTPS_PORT, PLACEHOLDER, SECRET_VALUE, UPSTREAM_IP, attribute_named_host_to_upstream,
    tls_network_config_for,
};
use super::protocol::http1::{Http1Response, TLS_PLAINTEXT_WRITE_BYTES_PER_STEP, TlsHttpUpstream, TlsTranscript};
use super::protocol::tls::{
    client_tls, drive_client_tls_io, drive_tls_until, fixed_mediated_ca, write_client_plaintext_limited,
};
use super::protocol::websocket::{TlsWssUpstream, client_text_frames, parse_server_text_frame, wss_upgrade_request};
use super::tls_case::{LinkAction, apply_link_actions, https_http1_case, https_http1_sequence_case, wss_http1_case};
use super::{
    AgentdpNetworkSim, DriveBudget, DriveGuestProgress, Error, LinkDirection, LinkTraceEventKind, NetworkUnderTest,
    Result, ScenarioNetworkConfig, ScenarioReport, Simulator, SmolTcpGuest, SteppedNetwork, TcpHandle,
};

const UPLOAD_BODY_LEN: usize = 384 * 1024;
const PACKAGE_BLOB_BODY_LEN: usize = 64 * 1024;
const DOCKER_LAYER_BODY_LEN: usize = 128 * 1024;
const NPM_TARBALL_BODY_LEN: usize = 96 * 1024;
const NUGET_PACKAGE_BODY_LEN: usize = 112 * 1024;
const GO_ZIP_BODY_LEN: usize = 96 * 1024;

static PACKAGE_BLOB_BODY: &[u8; PACKAGE_BLOB_BODY_LEN] = &[b'P'; PACKAGE_BLOB_BODY_LEN];
static DOCKER_LAYER_BODY: &[u8; DOCKER_LAYER_BODY_LEN] = &[b'D'; DOCKER_LAYER_BODY_LEN];
static NPM_TARBALL_BODY: &[u8; NPM_TARBALL_BODY_LEN] = &[b'N'; NPM_TARBALL_BODY_LEN];
static NUGET_PACKAGE_BODY: &[u8; NUGET_PACKAGE_BODY_LEN] = &[b'G'; NUGET_PACKAGE_BODY_LEN];
static GO_ZIP_BODY: &[u8; GO_ZIP_BODY_LEN] = &[b'Z'; GO_ZIP_BODY_LEN];
static DOCKER_MANIFEST_BODY: &[u8] =
    br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[{"digest":"sha256:layer"}]}"#;
static GO_LIST_BODY: &[u8] = b"v1.0.0\nv1.1.0\n";
static GO_INFO_BODY: &[u8] = br#"{"Version":"v1.1.0","Time":"2026-06-04T00:00:00Z"}"#;
static GO_MOD_BODY: &[u8] = b"module example.com/app\n\ngo 1.23\n";
static UPLOAD_RESPONSE_BODY: &[u8] = b"{\"ok\":true,\"stored\":\"artifact\"}\n";
static BROWSER_ASSET_BODY: &[u8] = b"browser asset marker bytes\n";
static SSE_STREAM_BODY: &[u8] =
    b"event: thread.started\ndata: {\"id\":\"run-1\"}\n\nevent: delta\ndata: {\"text\":\"hello\"}\n\nevent: done\ndata: {}\n\n";
static CONCURRENT_WSS_RESPONSE: &[u8] = br#"{"type":"delta","text":"concurrent"}"#;
const CONCURRENT_WORKLOAD_NAME: &str = "agent_and_package_https_wss_flows_progress_concurrently";

/// Verifies package-manager-like anonymous HTTPS GETs preserve response bytes.
///
/// # Errors
///
/// Returns an error when HTTPS mediation rewrites or rejects package-manager responses.
#[test]
fn simulated_package_manager_https_anonymous_gets_preserve_bodies() -> Result<()> {
    for case in PACKAGE_GET_CASES {
        package_https_get_case(case).run::<AgentdpNetworkSim>()?;
    }
    Ok(())
}

/// Verifies anonymous HTTPS package blob downloads still complete under thin link delay.
///
/// # Errors
///
/// Returns an error when delayed guest/network delivery stalls or corrupts the blob response.
#[test]
fn simulated_bootstrap_https_anonymous_package_blob_download_survives_link_delay() -> Result<()> {
    package_https_get_case(&PackageGetCase {
        name: "bootstrap_https_anonymous_package_blob_download_survives_link_delay",
        seed: 0x505,
        path: "/package/blob",
        accept: "application/octet-stream",
        body: PACKAGE_BLOB_BODY,
    })
    .delay_path(LinkDirection::GuestToNetwork, Duration::from_millis(2))
    .delay_path(LinkDirection::NetworkToGuest, Duration::from_millis(3))
    .expect_packet_event(
        LinkDirection::GuestToNetwork,
        LinkTraceEventKind::Delivered,
        Duration::from_millis(2),
        1,
    )
    .run::<AgentdpNetworkSim>()
}

/// Verifies artifact/package upload bodies remain opaque over mediated HTTPS.
///
/// # Errors
///
/// Returns an error when the upload body is truncated, rewritten, or rejected.
#[test]
fn simulated_bootstrap_https_artifact_upload_preserves_body() -> Result<()> {
    let body = vec![b'U'; UPLOAD_BODY_LEN];
    let mut request = format!(
        "PUT /packages/artifact.tgz HTTP/1.1\r\nHost: {HOST}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);

    https_http1_case("bootstrap_https_artifact_upload_preserves_body", 0x501)
        .authority(HOST)
        .upstream_response(UPLOAD_RESPONSE_BODY)
        .request(request)
        .run::<AgentdpNetworkSim>()
}

/// Verifies agent-session HTTPS SSE style responses preserve event-stream bytes.
///
/// # Errors
///
/// Returns an error when chunked event-stream delivery is corrupted or does not quiesce.
#[test]
fn simulated_agent_https_sse_stream_preserves_events() -> Result<()> {
    https_http1_case("agent_https_sse_stream_preserves_events", 0x502)
        .authority(HOST)
        .upstream_segmented_chunked_response(SSE_STREAM_BODY, 13, 19)
        .request(format!(
            "GET /backend-api/conversation/stream HTTP/1.1\r\nHost: {HOST}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n"
        ))
        .run::<AgentdpNetworkSim>()
}

/// Verifies package-manager HTTPS request reuse preserves response ordering.
///
/// # Errors
///
/// Returns an error when metadata and blob responses are reordered, truncated, or cross-associated.
#[test]
fn simulated_package_manager_https_docker_manifest_then_blob_reuses_connection() -> Result<()> {
    https_http1_sequence_case("package_manager_https_docker_manifest_then_blob_reuses_connection", 0x50c)
        .authority(HOST)
        .keep_alive_exchange(
            format!(
                "GET /v2/library/alpine/manifests/latest HTTP/1.1\r\nHost: {HOST}\r\nAccept: application/vnd.oci.image.manifest.v1+json\r\nConnection: keep-alive\r\n\r\n"
            ),
            DOCKER_MANIFEST_BODY,
        )
        .exchange(
            format!(
                "GET /v2/library/alpine/blobs/sha256:layer HTTP/1.1\r\nHost: {HOST}\r\nAccept: application/octet-stream\r\nConnection: close\r\n\r\n"
            ),
            DOCKER_LAYER_BODY,
        )
        .run::<AgentdpNetworkSim>()
}

/// Verifies agent-session WSS JSON messages survive mediation after the upgrade.
///
/// # Errors
///
/// Returns an error when WSS upgrade mediation or message relay corrupts the JSON payload.
#[test]
fn simulated_agent_wss_json_session_relays_message() -> Result<()> {
    wss_http1_case("agent_wss_json_session_relays_message", 0x503)
        .authority(HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
        .message(br#"{"type":"input","session":"codex","text":"run tests"}"#)
        .upstream_response(br#"{"type":"delta","text":"tests passed"}"#)
        .run::<AgentdpNetworkSim>()
}

/// Verifies browser-style HTTPS asset fetches preserve authenticated request headers and body bytes.
///
/// # Errors
///
/// Returns an error when browser request headers or asset bytes are not preserved.
#[test]
fn simulated_browser_https_asset_fetch_preserves_headers_and_body() -> Result<()> {
    https_http1_case("browser_https_asset_fetch_preserves_headers_and_body", 0x504)
        .authority(HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
        .upstream_response(BROWSER_ASSET_BODY)
        .request(format!(
            "GET /assets/app.wasm HTTP/1.1\r\nHost: {HOST}\r\nCookie: session={PLACEHOLDER}\r\nConnection: close\r\n\r\n"
        ))
        .run::<AgentdpNetworkSim>()
}

#[derive(Debug, Clone, Copy)]
struct PackageGetCase {
    name: &'static str,
    seed: u64,
    path: &'static str,
    accept: &'static str,
    body: &'static [u8],
}

const PACKAGE_GET_CASES: &[PackageGetCase] = &[
    PackageGetCase {
        name: "bootstrap_https_anonymous_package_blob_download_preserves_body",
        seed: 0x500,
        path: "/package/blob",
        accept: "application/octet-stream",
        body: PACKAGE_BLOB_BODY,
    },
    PackageGetCase {
        name: "package_manager_https_docker_blob_get_preserves_body",
        seed: 0x507,
        path: "/v2/library/alpine/blobs/sha256:layer",
        accept: "application/octet-stream",
        body: DOCKER_LAYER_BODY,
    },
    PackageGetCase {
        name: "package_manager_https_npm_tarball_get_preserves_body",
        seed: 0x508,
        path: "/@scope/pkg/-/pkg-1.2.3.tgz",
        accept: "application/octet-stream",
        body: NPM_TARBALL_BODY,
    },
    PackageGetCase {
        name: "package_manager_https_nuget_flat_container_package_get_preserves_body",
        seed: 0x509,
        path: "/v3-flatcontainer/example.package/1.2.3/example.package.1.2.3.nupkg",
        accept: "application/octet-stream",
        body: NUGET_PACKAGE_BODY,
    },
    PackageGetCase {
        name: "package_manager_https_go_proxy_info_get_preserves_body",
        seed: 0x50b,
        path: "/example.com/app/@v/v1.1.0.info",
        accept: "application/json",
        body: GO_INFO_BODY,
    },
];

fn package_https_get_case(case: &PackageGetCase) -> super::tls_case::HttpsHttp1Case {
    https_http1_case(case.name, case.seed)
        .authority(HOST)
        .upstream_response(case.body)
        .request(format!(
            "GET {} HTTP/1.1\r\nHost: {HOST}\r\nAccept: {}\r\nConnection: close\r\n\r\n",
            case.path, case.accept
        ))
}

/// Verifies `Go` module proxy list/info/mod/zip GETs preserve response order and bytes on one connection.
///
/// # Errors
///
/// Returns an error when Go proxy metadata and zip responses are reordered, truncated, or blocked.
#[test]
fn simulated_package_manager_https_go_proxy_list_info_mod_zip_sequence_preserves_body() -> Result<()> {
    https_http1_sequence_case(
        "package_manager_https_go_proxy_list_info_mod_zip_sequence_preserves_body",
        0x50d,
    )
    .authority(HOST)
    .keep_alive_exchange(
        format!("GET /example.com/app/@v/list HTTP/1.1\r\nHost: {HOST}\r\nConnection: keep-alive\r\n\r\n"),
        GO_LIST_BODY,
    )
    .keep_alive_exchange(
        format!(
            "GET /example.com/app/@v/v1.1.0.info HTTP/1.1\r\nHost: {HOST}\r\nAccept: application/json\r\nConnection: keep-alive\r\n\r\n"
        ),
        GO_INFO_BODY,
    )
    .keep_alive_exchange(
        format!("GET /example.com/app/@v/v1.1.0.mod HTTP/1.1\r\nHost: {HOST}\r\nConnection: keep-alive\r\n\r\n"),
        GO_MOD_BODY,
    )
    .exchange(
        format!(
            "GET /example.com/app/@v/v1.1.0.zip HTTP/1.1\r\nHost: {HOST}\r\nAccept: application/zip\r\nConnection: close\r\n\r\n"
        ),
        GO_ZIP_BODY,
    )
    .run::<AgentdpNetworkSim>()
}

/// Verifies mixed HTTPS/WSS workloads make progress concurrently through one mediated network.
///
/// # Errors
///
/// Returns an error when concurrent flows are stalled, cross-associated, truncated, or corrupted.
#[test]
fn simulated_agent_and_package_https_wss_flows_progress_concurrently() -> Result<()> {
    run_concurrent_https_wss_workload::<AgentdpNetworkSim>(ConcurrentWorkloadSpec::default())
}

pub(super) fn run_concurrent_https_wss_workload<N>(spec: ConcurrentWorkloadSpec) -> Result<()>
where
    N: NetworkUnderTest,
    N::Running: SteppedNetwork,
{
    let mut workload = start_concurrent_workload::<N>(spec)?;
    workload.drive()?;
    workload.verify()?;
    let mut report = workload.stop_report()?;
    check_all(
        &mut report,
        vec![
            Box::new(NoUnexpectedEgressErrors),
            Box::new(Quiescent),
            Box::new(ProgressComplete),
        ],
    )
}

#[derive(Debug, Clone)]
pub(super) struct ConcurrentWorkloadSpec {
    pub(super) name: &'static str,
    pub(super) seed: Seed,
    pub(super) guest_to_network_delay: Duration,
    pub(super) network_to_guest_delay: Duration,
    pub(super) post_connect_actions: Vec<LinkAction>,
    pub(super) post_data_actions: Vec<LinkAction>,
    pub(super) package_response: ConcurrentHttpResponseSpec,
    pub(super) sse_response: ConcurrentHttpResponseSpec,
    pub(super) upload_body_len: usize,
    pub(super) upload_response: ConcurrentHttpResponseSpec,
    pub(super) wss_fragmented: bool,
    pub(super) wss_close_after_response: bool,
}

impl Default for ConcurrentWorkloadSpec {
    fn default() -> Self {
        Self {
            name: CONCURRENT_WORKLOAD_NAME,
            seed: Seed::new(0x50e),
            guest_to_network_delay: Duration::from_millis(1),
            network_to_guest_delay: Duration::from_millis(2),
            post_connect_actions: Vec::new(),
            post_data_actions: Vec::new(),
            package_response: ConcurrentHttpResponseSpec::content_length(DOCKER_LAYER_BODY).segmented(4096),
            sse_response: ConcurrentHttpResponseSpec::chunked(SSE_STREAM_BODY, 13).segmented(19),
            upload_body_len: UPLOAD_BODY_LEN,
            upload_response: ConcurrentHttpResponseSpec::content_length(UPLOAD_RESPONSE_BODY),
            wss_fragmented: true,
            wss_close_after_response: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ConcurrentHttpResponseSpec {
    body: &'static [u8],
    chunk_size: Option<usize>,
    segment_size: Option<usize>,
}

impl ConcurrentHttpResponseSpec {
    pub(super) const fn content_length(body: &'static [u8]) -> Self {
        Self {
            body,
            chunk_size: None,
            segment_size: None,
        }
    }

    pub(super) const fn chunked(body: &'static [u8], chunk_size: usize) -> Self {
        Self {
            body,
            chunk_size: Some(chunk_size),
            segment_size: None,
        }
    }

    pub(super) const fn segmented(mut self, segment_size: usize) -> Self {
        self.segment_size = Some(segment_size);
        self
    }

    const fn response(self) -> Http1Response {
        let response = if let Some(chunk_size) = self.chunk_size {
            Http1Response::chunked(self.body, chunk_size)
        } else {
            Http1Response::ok(self.body)
        };
        if let Some(segment_size) = self.segment_size {
            response.segmented(segment_size)
        } else {
            response
        }
    }
}

fn start_concurrent_workload<N>(spec: ConcurrentWorkloadSpec) -> Result<ConcurrentWorkload<N::Running>>
where
    N: NetworkUnderTest,
    N::Running: SteppedNetwork,
{
    let ConcurrentWorkloadSpec {
        name,
        seed,
        guest_to_network_delay,
        network_to_guest_delay,
        post_connect_actions,
        post_data_actions,
        package_response,
        sse_response,
        upload_body_len,
        upload_response,
        wss_fragmented,
        wss_close_after_response,
    } = spec;

    let mut sim = Simulator::new(seed);
    let guest_link = sim.guest_link()?;
    guest_link.set_path_delay(LinkDirection::GuestToNetwork, guest_to_network_delay);
    guest_link.set_path_delay(LinkDirection::NetworkToGuest, network_to_guest_delay);

    let ports = ConcurrentPorts::default();
    let mediated_ca = fixed_mediated_ca();
    let upstreams = build_concurrent_upstreams(
        package_response,
        sse_response,
        upload_response,
        wss_close_after_response,
    )?;

    let mut running = start_concurrent_network::<N>(&sim, &guest_link, &mediated_ca, &upstreams, ports)?;
    attribute_named_host_to_upstream(&mut sim, &mut running, &guest_link, HOST)?;

    let mut guest = SmolTcpGuest::new(guest_link.clone())?;
    let mut https_flows = Vec::new();
    for flow_spec in concurrent_https_flow_specs(ConcurrentHttpsFlowInputs {
        package_addr: ports.package,
        sse_addr: ports.sse,
        upload_addr: ports.upload,
        package_response: upstreams.package_response,
        sse_response: upstreams.sse_response,
        upload_response: upstreams.upload_response,
        upload_body_len,
        ca_pem: &mediated_ca.cert_pem,
        package_transcript: upstreams.package.transcript(),
        sse_transcript: upstreams.sse.transcript(),
        upload_transcript: upstreams.upload.transcript(),
    })? {
        https_flows.push(ConcurrentHttpsFlow::connect(
            &mut sim,
            &mut guest,
            &mut running,
            flow_spec,
        )?);
    }
    let wss_flow = ConcurrentWssFlow::connect(
        &mut sim,
        &mut guest,
        &mut running,
        ConcurrentWssSpec {
            addr: ports.wss,
            request_message: br#"{"type":"input","session":"codex","text":"continue"}"#,
            expected_message: CONCURRENT_WSS_RESPONSE,
            ca_pem: &mediated_ca.cert_pem,
            fragmented: wss_fragmented,
            transcript: upstreams.wss.transcript(),
        },
    )?;
    apply_link_actions(&guest_link, &post_connect_actions);
    apply_link_actions(&guest_link, &post_data_actions);

    Ok(ConcurrentWorkload {
        name,
        sim,
        guest_link,
        guest,
        running,
        https_flows,
        wss_flow,
    })
}

#[derive(Clone, Copy)]
struct ConcurrentPorts {
    package: SocketAddr,
    sse: SocketAddr,
    upload: SocketAddr,
    wss: SocketAddr,
}

impl Default for ConcurrentPorts {
    fn default() -> Self {
        Self {
            package: upstream_port(HTTPS_PORT),
            sse: upstream_port(HTTPS_PORT + 1),
            upload: upstream_port(HTTPS_PORT + 2),
            wss: upstream_port(HTTPS_PORT + 3),
        }
    }
}

fn start_concurrent_network<N>(
    sim: &Simulator,
    guest_link: &super::GuestLink,
    mediated_ca: &agentdp_crypto::CertificateAuthorityPem,
    upstreams: &ConcurrentUpstreams,
    ports: ConcurrentPorts,
) -> Result<N::Running>
where
    N: NetworkUnderTest,
{
    let mut secrets = RuntimeSecrets::new();
    secrets.insert(RuntimeSecret::new(PLACEHOLDER, SECRET_VALUE, [HOST.to_owned()]));
    let mut network = tls_network_config_for(
        mediated_ca,
        std::slice::from_ref(&upstreams.root_ca_pem),
        &[HOST],
        secrets,
        &[],
    );
    if let Some(tls) = network.tls.as_mut() {
        tls.intercepted_ports = vec![
            ports.package.port(),
            ports.sse.port(),
            ports.upload.port(),
            ports.wss.port(),
        ];
    }
    N::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network,
            upstreams: SimulationUpstreams::default()
                .with_dns_a_endpoint(DNS_UPSTREAM, UPSTREAM_IP)
                .with_tcp_handler(ports.package, upstreams.package.handler())
                .with_tcp_handler(ports.sse, upstreams.sse.handler())
                .with_tcp_handler(ports.upload, upstreams.upload.handler())
                .with_tcp_handler(ports.wss, upstreams.wss.handler()),
        },
        guest_link.clone(),
    )
}

struct ConcurrentUpstreams {
    package: TlsHttpUpstream,
    sse: TlsHttpUpstream,
    upload: TlsHttpUpstream,
    wss: TlsWssUpstream,
    package_response: Http1Response,
    sse_response: Http1Response,
    upload_response: Http1Response,
    root_ca_pem: String,
}

fn build_concurrent_upstreams(
    package_response: ConcurrentHttpResponseSpec,
    sse_response: ConcurrentHttpResponseSpec,
    upload_response: ConcurrentHttpResponseSpec,
    wss_close_after_response: bool,
) -> Result<ConcurrentUpstreams> {
    let package_response = package_response.response();
    let sse_response = sse_response.response();
    let upload_response = upload_response.response();
    let package = TlsHttpUpstream::with_response(package_response.clone())?;
    let sse = TlsHttpUpstream::with_response(sse_response.clone())?;
    let upload = TlsHttpUpstream::with_response(upload_response.clone())?;
    let wss = TlsWssUpstream::new(CONCURRENT_WSS_RESPONSE, wss_close_after_response)?;
    let root_ca_pem = package.root_ca_pem.clone();
    Ok(ConcurrentUpstreams {
        package,
        sse,
        upload,
        wss,
        package_response,
        sse_response,
        upload_response,
        root_ca_pem,
    })
}

struct ConcurrentWorkload<N> {
    name: &'static str,
    sim: Simulator,
    guest_link: super::GuestLink,
    guest: SmolTcpGuest,
    running: N,
    https_flows: Vec<ConcurrentHttpsFlow>,
    wss_flow: ConcurrentWssFlow,
}

impl<N> ConcurrentWorkload<N>
where
    N: SteppedNetwork,
{
    fn drive(&mut self) -> Result<()> {
        let name = self.name;
        let progress = std::cell::Cell::new(0_usize);
        let diagnostics = RefCell::new(String::new());
        let Self {
            sim,
            guest,
            running,
            https_flows,
            wss_flow,
            ..
        } = self;
        sim.drive_guest_until_with_progress(
            guest,
            running,
            DriveGuestProgress {
                label: name,
                budget: DriveBudget {
                    max_steps: 32_768,
                    step_time: Duration::from_millis(1),
                },
            },
            |guest, running| {
                for flow in &mut *https_flows {
                    flow.drive_step(guest, running)?;
                }
                wss_flow.drive_step(guest, running)?;
                progress.set(
                    https_flows
                        .iter()
                        .map(ConcurrentHttpsFlow::progress)
                        .sum::<usize>()
                        .saturating_add(wss_flow.progress()),
                );
                *diagnostics.borrow_mut() = concurrent_diagnostics(https_flows, wss_flow);
                Ok(https_flows.iter().all(ConcurrentHttpsFlow::is_complete) && wss_flow.is_complete())
            },
            || progress.get(),
            |output| {
                output.push_str(&diagnostics.borrow());
            },
        )
    }

    fn verify(&self) -> Result<()> {
        for flow in &self.https_flows {
            flow.verify()?;
        }
        self.wss_flow.verify()
    }

    fn stop_report(mut self) -> Result<ScenarioReport> {
        for flow in &self.https_flows {
            self.guest.close(&mut self.running, flow.tcp)?;
        }
        self.guest.close(&mut self.running, self.wss_flow.tcp)?;
        let quiescence = self.sim.drive_guest_network_until_quiescent(
            &mut self.guest,
            &mut self.running,
            &self.guest_link,
            self.name,
            DriveBudget {
                max_steps: 4096,
                ..DriveBudget::default()
            },
        )?;
        let seed = self.sim.seed();
        let simulator_trace = self.sim.trace().to_vec();
        let stop = self.running.stop()?;
        let mut report = ScenarioReport::new(
            self.name,
            seed,
            stop.final_status,
            quiescence,
            simulator_trace,
            self.guest_link.trace(),
            stop.network_events,
        );
        for flow in &self.https_flows {
            report = report.with_progress(
                flow.name,
                flow.response.len(),
                flow.expected_response.len(),
                flow.response == flow.expected_response,
            );
        }
        Ok(report.with_progress(
            "wss-message",
            self.wss_flow.response_message.len(),
            self.wss_flow.expected_message.len(),
            self.wss_flow.response_message == self.wss_flow.expected_message,
        ))
    }
}

fn concurrent_diagnostics(https_flows: &[ConcurrentHttpsFlow], wss_flow: &ConcurrentWssFlow) -> String {
    let mut snapshot = String::new();
    let _ = writeln!(snapshot, "  phase: concurrent HTTPS/WSS workload");
    for flow in https_flows {
        let _ = writeln!(
            snapshot,
            "  https_flow={} request_written={}/{} response_read={}/{} complete={}",
            flow.name,
            flow.written,
            flow.request.len(),
            flow.response.len(),
            flow.expected_response.len(),
            flow.complete
        );
    }
    let _ = writeln!(
        snapshot,
        "  wss_flow request_frames_sent={}/{} response_read={} complete={}",
        wss_flow.frame_index,
        wss_flow.frames.len(),
        wss_flow.response.len(),
        wss_flow.complete
    );
    snapshot
}

struct ConcurrentHttpsSpec<'a> {
    addr: SocketAddr,
    name: &'static str,
    request: Vec<u8>,
    expected_request: Vec<u8>,
    expected_response: Vec<u8>,
    ca_pem: &'a str,
    transcript: Rc<RefCell<TlsTranscript>>,
}

struct ConcurrentWssSpec<'a> {
    addr: SocketAddr,
    request_message: &'a [u8],
    expected_message: &'a [u8],
    ca_pem: &'a str,
    fragmented: bool,
    transcript: Rc<RefCell<TlsTranscript>>,
}

struct ConcurrentHttpsFlow {
    name: &'static str,
    tcp: TcpHandle,
    tls: TlsClientSession,
    request: Vec<u8>,
    expected_request: Vec<u8>,
    expected_response: Vec<u8>,
    written: usize,
    response: Vec<u8>,
    transcript: Rc<RefCell<TlsTranscript>>,
    tls_bytes_flushed: usize,
    complete: bool,
}

impl ConcurrentHttpsFlow {
    fn connect<N>(
        sim: &mut Simulator,
        guest: &mut SmolTcpGuest,
        running: &mut N,
        spec: ConcurrentHttpsSpec<'_>,
    ) -> Result<Self>
    where
        N: SteppedNetwork,
    {
        let tcp = guest.connect(running, spec.addr)?;
        let mut tls = client_tls(HOST, spec.ca_pem)?;
        drive_tls_until(
            sim,
            guest,
            running,
            tcp,
            &mut tls,
            "concurrent TLS handshake",
            |tls, _plaintext| !tls.is_handshaking(),
        )?;
        Ok(Self {
            name: spec.name,
            tcp,
            tls,
            request: spec.request,
            expected_request: spec.expected_request,
            expected_response: spec.expected_response,
            written: 0,
            response: Vec::new(),
            transcript: spec.transcript,
            tls_bytes_flushed: 0,
            complete: false,
        })
    }

    fn drive_step<N>(&mut self, guest: &mut SmolTcpGuest, running: &mut N) -> Result<()>
    where
        N: SteppedNetwork,
    {
        if self.complete {
            return Ok(());
        }
        write_client_plaintext_limited(
            &mut self.tls,
            &self.request,
            &mut self.written,
            TLS_PLAINTEXT_WRITE_BYTES_PER_STEP,
        )?;
        let io = drive_client_tls_io(guest, running, self.tcp, &mut self.tls, &mut self.response)?;
        self.tls_bytes_flushed = self.tls_bytes_flushed.saturating_add(io.flushed);
        if !self.expected_response.starts_with(&self.response) {
            return Err(Error::new(format!(
                "concurrent HTTPS flow {:?} response diverged at {} bytes",
                self.name,
                self.response.len()
            )));
        }
        self.complete = self.response == self.expected_response;
        Ok(())
    }

    const fn is_complete(&self) -> bool {
        self.complete
    }

    const fn progress(&self) -> usize {
        self.written
            .saturating_add(self.tls_bytes_flushed)
            .saturating_add(self.response.len())
    }

    fn verify(&self) -> Result<()> {
        let upstream_request = &self.transcript.borrow().request;
        if upstream_request != &self.expected_request {
            return Err(Error::new(format!(
                "concurrent HTTPS flow {:?} upstream request mismatch; observed={} expected={}",
                self.name,
                upstream_request.len(),
                self.expected_request.len()
            )));
        }
        if self.response != self.expected_response {
            return Err(Error::new(format!(
                "concurrent HTTPS flow {:?} response mismatch; observed={} expected={}",
                self.name,
                self.response.len(),
                self.expected_response.len()
            )));
        }
        Ok(())
    }
}

struct ConcurrentWssFlow {
    tcp: TcpHandle,
    tls: TlsClientSession,
    frames: Vec<Vec<u8>>,
    frame_index: usize,
    frame_offset: usize,
    response: Vec<u8>,
    response_message: Vec<u8>,
    expected_message: Vec<u8>,
    expected_upstream_message: Vec<u8>,
    transcript: Rc<RefCell<TlsTranscript>>,
    tls_bytes_flushed: usize,
    complete: bool,
}

impl ConcurrentWssFlow {
    fn connect<N>(
        sim: &mut Simulator,
        guest: &mut SmolTcpGuest,
        running: &mut N,
        spec: ConcurrentWssSpec<'_>,
    ) -> Result<Self>
    where
        N: SteppedNetwork,
    {
        let tcp = guest.connect(running, spec.addr)?;
        let mut tls = client_tls(HOST, spec.ca_pem)?;
        drive_tls_until(
            sim,
            guest,
            running,
            tcp,
            &mut tls,
            "concurrent WSS TLS handshake",
            |tls, _plaintext| !tls.is_handshaking(),
        )?;
        let upgrade_request = wss_upgrade_request(HOST);
        let mut upgrade_written = 0_usize;
        let mut upgrade = Vec::new();
        let upgrade_progress = std::cell::Cell::new(0_usize);
        let upgrade_written_len = std::cell::Cell::new(0_usize);
        let upgrade_response_len = std::cell::Cell::new(0_usize);
        sim.drive_guest_until_with_progress(
            guest,
            running,
            DriveGuestProgress {
                label: "concurrent WSS upgrade",
                budget: DriveBudget {
                    max_steps: 4096,
                    step_time: Duration::from_millis(1),
                },
            },
            |guest, running| {
                write_client_plaintext_limited(
                    &mut tls,
                    upgrade_request.as_bytes(),
                    &mut upgrade_written,
                    TLS_PLAINTEXT_WRITE_BYTES_PER_STEP,
                )?;
                let io = drive_client_tls_io(guest, running, tcp, &mut tls, &mut upgrade)?;
                upgrade_written_len.set(upgrade_written);
                upgrade_response_len.set(upgrade.len());
                upgrade_progress.set(upgrade_written.saturating_add(upgrade.len()).saturating_add(io.flushed));
                Ok(upgrade_written == upgrade_request.len() && upgrade.windows(4).any(|window| window == b"\r\n\r\n"))
            },
            || upgrade_progress.get(),
            |output| {
                let _ = writeln!(output, "  phase: concurrent WSS upgrade");
                let _ = writeln!(
                    output,
                    "  upgrade_request_plaintext_accepted: {}/{}",
                    upgrade_written_len.get(),
                    upgrade_request.len()
                );
                let _ = writeln!(
                    output,
                    "  upgrade_response_plaintext_read: {}",
                    upgrade_response_len.get()
                );
            },
        )?;
        if !upgrade
            .windows(b"101 Switching Protocols".len())
            .any(|window| window == b"101 Switching Protocols")
        {
            return Err(Error::new("concurrent WSS upgrade was not accepted"));
        }
        Ok(Self {
            tcp,
            tls,
            frames: client_text_frames(spec.request_message, spec.fragmented)
                .map_err(|error| Error::new(format!("build concurrent WSS frames: {error}")))?,
            frame_index: 0,
            frame_offset: 0,
            response: Vec::new(),
            response_message: Vec::new(),
            expected_message: spec.expected_message.to_vec(),
            expected_upstream_message: spec.request_message.to_vec(),
            transcript: spec.transcript,
            tls_bytes_flushed: 0,
            complete: false,
        })
    }

    fn drive_step<N>(&mut self, guest: &mut SmolTcpGuest, running: &mut N) -> Result<()>
    where
        N: SteppedNetwork,
    {
        if self.complete {
            return Ok(());
        }
        while let Some(frame) = self.frames.get(self.frame_index) {
            write_client_plaintext_limited(
                &mut self.tls,
                frame,
                &mut self.frame_offset,
                TLS_PLAINTEXT_WRITE_BYTES_PER_STEP,
            )?;
            if self.frame_offset < frame.len() {
                break;
            }
            self.frame_index += 1;
            self.frame_offset = 0;
        }
        let io = drive_client_tls_io(guest, running, self.tcp, &mut self.tls, &mut self.response)?;
        self.tls_bytes_flushed = self.tls_bytes_flushed.saturating_add(io.flushed);
        match parse_server_text_frame(&self.response) {
            Ok(message) => {
                self.response_message = message;
                self.complete = true;
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {}
            Err(error) => return Err(Error::new(format!("parse concurrent WSS response: {error}"))),
        }
        Ok(())
    }

    const fn is_complete(&self) -> bool {
        self.complete
    }

    const fn progress(&self) -> usize {
        self.frame_index
            .saturating_mul(1024 * 1024)
            .saturating_add(self.frame_offset)
            .saturating_add(self.tls_bytes_flushed)
            .saturating_add(self.response.len())
    }

    fn verify(&self) -> Result<()> {
        let upstream_message = self.transcript.borrow().websocket_message.clone().unwrap_or_default();
        if upstream_message != self.expected_upstream_message {
            return Err(Error::new(format!(
                "concurrent WSS upstream message mismatch; observed={:02x?} expected={:02x?}",
                upstream_message, self.expected_upstream_message
            )));
        }
        if self.response_message != self.expected_message {
            return Err(Error::new(format!(
                "concurrent WSS response mismatch; observed={:02x?} expected={:02x?}",
                self.response_message, self.expected_message
            )));
        }
        Ok(())
    }
}

const fn upstream_port(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(UPSTREAM_IP), port)
}

struct ConcurrentHttpsFlowInputs<'a> {
    package_addr: SocketAddr,
    sse_addr: SocketAddr,
    upload_addr: SocketAddr,
    package_response: Http1Response,
    sse_response: Http1Response,
    upload_response: Http1Response,
    upload_body_len: usize,
    ca_pem: &'a str,
    package_transcript: Rc<RefCell<TlsTranscript>>,
    sse_transcript: Rc<RefCell<TlsTranscript>>,
    upload_transcript: Rc<RefCell<TlsTranscript>>,
}

fn concurrent_https_flow_specs(inputs: ConcurrentHttpsFlowInputs<'_>) -> Result<Vec<ConcurrentHttpsSpec<'_>>> {
    let package_request = format!(
        "GET /v2/library/alpine/blobs/sha256:layer HTTP/1.1\r\nHost: {HOST}\r\nAccept: application/octet-stream\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    let sse_request = format!(
        "GET /backend-api/conversation/stream HTTP/1.1\r\nHost: {HOST}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    let upload_request = upload_request(inputs.upload_body_len);
    Ok(vec![
        ConcurrentHttpsSpec {
            addr: inputs.package_addr,
            name: "docker-layer",
            request: package_request.clone(),
            expected_request: package_request,
            expected_response: http_response_bytes(&inputs.package_response)?,
            ca_pem: inputs.ca_pem,
            transcript: inputs.package_transcript,
        },
        ConcurrentHttpsSpec {
            addr: inputs.sse_addr,
            name: "sse-stream",
            request: sse_request.clone(),
            expected_request: sse_request,
            expected_response: http_response_bytes(&inputs.sse_response)?,
            ca_pem: inputs.ca_pem,
            transcript: inputs.sse_transcript,
        },
        ConcurrentHttpsSpec {
            addr: inputs.upload_addr,
            name: "artifact-upload",
            request: upload_request.clone(),
            expected_request: upload_request,
            expected_response: http_response_bytes(&inputs.upload_response)?,
            ca_pem: inputs.ca_pem,
            transcript: inputs.upload_transcript,
        },
    ])
}

fn upload_request(body_len: usize) -> Vec<u8> {
    let body = vec![b'U'; body_len];
    let mut request = format!(
        "PUT /packages/concurrent-artifact.tgz HTTP/1.1\r\nHost: {HOST}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    request
}

fn http_response_bytes(response: &Http1Response) -> Result<Vec<u8>> {
    response
        .to_bytes()
        .map_err(|error| Error::new(format!("build expected HTTP response: {error}")))
}
