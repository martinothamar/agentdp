use agentdp_network::RuntimeSecrets;
use agentdp_network::test_support::simulation::SimulationUpstreams;
use agentdp_rand::Seed;

use super::case_support::stop_tcp_report;
use super::fixtures::{
    BLOCKED_HOST, BYPASS_HOST, HOST, PLACEHOLDER, SECRET_VALUE, UNKNOWN_PLACEHOLDER, tls_network_config_for,
    upstream_addr,
};
use super::protocol::http1::{Http1Response, TlsHttpUpstream};
use super::protocol::tls::{client_tls, drive_tls_until, fixed_mediated_ca};
use super::tls_case::{LinkAction, https_http1_case, wss_http1_case};
use super::{
    AgentdpNetworkSim, LinkDirection, NetworkUnderTest, Result, ScenarioNetworkConfig, Simulator, SmolTcpGuest,
};

const LARGE_BODY_LEN: usize = 1024 * 1024 + 8192;
const HUGE_RESPONSE_LEN: usize = 3 * 1024 * 1024;
const BYPASS_LARGE_BODY_LEN: usize = 32 * 1024;
static LARGE_RESPONSE_BODY: [u8; LARGE_BODY_LEN] = [b'R'; LARGE_BODY_LEN];
static HUGE_RESPONSE_BODY: [u8; HUGE_RESPONSE_LEN] = [b'E'; HUGE_RESPONSE_LEN];
static BYPASS_LARGE_RESPONSE_BODY: [u8; BYPASS_LARGE_BODY_LEN] = [b'B'; BYPASS_LARGE_BODY_LEN];
static CHUNKED_RESPONSE_BODY: &[u8] = b"chunked response body over mediated HTTPS\n";

/// Verifies HTTPS/1 interception, authority-bound secret substitution, and upstream transcript recording.
///
/// # Errors
///
/// Returns an error when the HTTPS scenario fails or the modeled behavior invariants are violated.
#[test]
fn simulated_guest_https_http1_substitutes_authorized_secret_header() -> Result<()> {
    https_http1_case("guest_https_http1_substitutes_authorized_secret_header", 0x201)
        .authority(HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
        .upstream_response(b"agentdp-simulated-https\n")
        .request(format!(
            "GET /real-traffic HTTP/1.1\r\nHost: {HOST}\r\nAuthorization: Bearer {PLACEHOLDER}\r\nConnection: close\r\n\r\n"
        ))
        .run::<AgentdpNetworkSim>()
}

/// Verifies that a parsed `ClientHello` is retried after another TLS flow releases local buffer capacity.
///
/// # Errors
///
/// Returns an error when the target handshake waits for more guest bytes after the pressure clears.
#[test]
fn simulated_tls_handshake_retries_parsed_client_hello_after_transient_buffer_pressure() -> Result<()> {
    let mut sim = Simulator::new(Seed::new(0x22a));
    let guest_link = sim.guest_link()?;
    let mediated_ca = fixed_mediated_ca();
    let upstream = TlsHttpUpstream::with_response(Http1Response::ok(b"unused\n"))?;
    let mut config = tls_network_config_for(
        &mediated_ca,
        std::slice::from_ref(&upstream.root_ca_pem),
        &[HOST],
        RuntimeSecrets::new(),
        &[],
    );
    config.limits.medium_byte_pool_capacity = 2;
    config.limits.tls_relay_buffer_capacity = 4096;

    let mut running = AgentdpNetworkSim::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network: config,
            upstreams: SimulationUpstreams::default()
                .with_dns_a_endpoint(super::fixtures::DNS_UPSTREAM, super::fixtures::UPSTREAM_IP)
                .with_tcp_handler(upstream_addr(), upstream.handler()),
        },
        guest_link.clone(),
    )?;
    super::fixtures::attribute_named_host_to_upstream(&mut sim, &mut running, &guest_link, HOST)?;

    let mut guest = SmolTcpGuest::new(guest_link.clone())?;
    let holder_tcp = guest.connect(&mut running, upstream_addr())?;
    let mut holder_tls = client_tls(HOST, &mediated_ca.cert_pem)?;
    let mut holder_client_hello = Vec::new();
    holder_tls
        .drain_ciphertext_to(&mut holder_client_hello, usize::MAX)
        .map_err(|error| super::Error::from_display("drain holder ClientHello", error))?;
    guest.write_all(&mut running, holder_tcp, &holder_client_hello[..16])?;
    guest.drain(&mut running, 8)?;

    let target_tcp = guest.connect(&mut running, upstream_addr())?;
    let mut target_tls = client_tls(HOST, &mediated_ca.cert_pem)?;
    let mut target_client_hello = Vec::new();
    target_tls
        .drain_ciphertext_to(&mut target_client_hello, usize::MAX)
        .map_err(|error| super::Error::from_display("drain target ClientHello", error))?;
    guest.write_all(&mut running, target_tcp, &target_client_hello)?;
    guest.drain(&mut running, 8)?;

    guest.abort_tcp(&mut running, holder_tcp)?;
    guest.drain(&mut running, 8)?;

    drive_tls_until(
        &mut sim,
        &mut guest,
        &mut running,
        target_tcp,
        &mut target_tls,
        "TLS handshake after transient local buffer pressure",
        |tls, _plaintext| !tls.is_handshaking(),
    )?;

    let _closed = guest.abort_tcp(&mut running, target_tcp);
    let _report = stop_tcp_report(
        "guest_tls_handshake_retries_parsed_client_hello_after_transient_buffer_pressure",
        sim,
        guest,
        running,
        &guest_link,
        target_tcp,
        false,
    )?;
    Ok(())
}

/// Verifies HTTP/1 substitution in headers, URL query text, and fixed request bodies.
///
/// # Errors
///
/// Returns an error when the HTTPS scenario fails or the modeled behavior invariants are violated.
#[test]
fn simulated_guest_https_http1_substitutes_header_query_and_body() -> Result<()> {
    let body = format!("body-token={PLACEHOLDER}");
    https_http1_case("guest_https_http1_substitutes_header_query_and_body", 0x203)
        .authority(HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
        .upstream_response(b"agentdp-simulated-post\n")
        .request(format!(
            "POST /real-traffic?token={PLACEHOLDER} HTTP/1.1\r\nHost: {HOST}\r\nX-Api-Key: {PLACEHOLDER}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ))
        .run::<AgentdpNetworkSim>()
}

/// Verifies that HTTPS/1 carries request and response bodies larger than the buffered mediation limit.
///
/// # Errors
///
/// Returns an error when large HTTPS request or response bytes are truncated, rewritten, or rejected.
#[test]
fn simulated_guest_https_http1_relays_large_request_and_response() -> Result<()> {
    let body = vec![b'Q'; LARGE_BODY_LEN];
    let mut request = format!(
        "POST /large HTTP/1.1\r\nHost: {HOST}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);

    https_http1_case("guest_https_http1_relays_large_request_and_response", 0x209)
        .authority(HOST)
        .upstream_response(&LARGE_RESPONSE_BODY)
        .request(request)
        .run::<AgentdpNetworkSim>()
}

/// Verifies that a large fixed-length keep-alive request is fully relayed before upstream EOF.
///
/// # Errors
///
/// Returns an error when the request is truncated around the buffered mediation boundary.
#[test]
fn simulated_guest_https_http1_relays_large_keep_alive_request_before_upstream_eof() -> Result<()> {
    let body = vec![b'K'; LARGE_BODY_LEN];
    let mut request = format!(
        "PUT /large-keep-alive HTTP/1.1\r\nHost: {HOST}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);

    https_http1_case(
        "guest_https_http1_relays_large_keep_alive_request_before_upstream_eof",
        0x225,
    )
    .authority(HOST)
    .delay_path(LinkDirection::GuestToNetwork, std::time::Duration::from_millis(150))
    .delay_path(LinkDirection::NetworkToGuest, std::time::Duration::from_millis(100))
    .post_connect_link_action(LinkAction::BlockNextWrite)
    .post_connect_link_action(LinkAction::BlockNextRead)
    .post_tls_link_action(LinkAction::BlockNextWrite)
    .upstream_chunked_response(b"", 4096)
    .upstream_close_after_response()
    .request(request)
    .run::<AgentdpNetworkSim>()
}

/// Verifies that HTTPS/1 drains a large response before observing upstream EOF.
///
/// # Errors
///
/// Returns an error when EOF truncates the response or leaves the dataplane non-quiescent.
#[test]
fn simulated_guest_https_http1_drains_large_response_before_upstream_eof() -> Result<()> {
    https_http1_case("guest_https_http1_drains_large_response_before_upstream_eof", 0x211)
        .authority(HOST)
        .upstream_segmented_response(&LARGE_RESPONSE_BODY, 1024)
        .upstream_close_after_response()
        .request(format!(
            "GET /large-before-eof HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
        ))
        .run::<AgentdpNetworkSim>()
}

/// Verifies that HTTPS/1 treats upstream EOF before a declared response body completes as failure.
///
/// # Errors
///
/// Returns an error when the incomplete response is delivered as if it were complete.
#[test]
fn simulated_guest_https_http1_rejects_incomplete_response_before_upstream_eof() -> Result<()> {
    https_http1_case(
        "guest_https_http1_rejects_incomplete_response_before_upstream_eof",
        0x227,
    )
    .authority(HOST)
    .upstream_http_response(Http1Response::with_declared_content_length(b"short", 32))
    .upstream_close_after_response()
    .request(format!(
        "GET /incomplete-before-eof HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
    ))
    .expect_failure()
    .run::<AgentdpNetworkSim>()
}

/// Verifies that HTTP/1 response completion uses the request method when interpreting response bodies.
///
/// # Errors
///
/// Returns an error when a valid `HEAD` response with `Content-Length` is treated as truncated at EOF.
#[test]
fn simulated_guest_https_http1_head_response_with_content_length_completes_on_eof() -> Result<()> {
    https_http1_case(
        "guest_https_http1_head_response_with_content_length_completes_on_eof",
        0x228,
    )
    .authority(HOST)
    .upstream_http_response(Http1Response::with_declared_content_length(b"", 1024))
    .upstream_close_after_response()
    .request(format!(
        "HEAD /head-content-length HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
    ))
    .run::<AgentdpNetworkSim>()
}

/// Verifies that a large HTTPS/1 upload continues correctly when the upstream responds after headers.
///
/// # Errors
///
/// Returns an error when concurrent upload and early response cause the mediated TLS path to reject, truncate, or stall.
#[test]
fn simulated_guest_https_http1_large_upload_survives_early_large_response() -> Result<()> {
    let mut body = vec![b'C'; LARGE_BODY_LEN];
    for offset in (8192..body.len().saturating_sub(4)).step_by(32 * 1024) {
        body[offset..offset + 4].copy_from_slice(b"\r\n\r\n");
    }
    let mut request = format!(
        "POST /backend-api/codex/responses/compact HTTP/1.1\r\nHost: {HOST}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);

    https_http1_case("guest_https_http1_large_upload_survives_early_large_response", 0x226)
        .authority(HOST)
        .large_upload_with_early_response(request, &LARGE_RESPONSE_BODY)
        .run::<AgentdpNetworkSim>()
}

/// Verifies that budget-limited dataplane turns requeue intercepted TLS upload continuations.
///
/// # Errors
///
/// Returns an error when guest ciphertext buffered by the TLS proxy stalls instead of being drained.
#[test]
fn simulated_guest_https_http1_large_upload_survives_constrained_drive_budget() -> Result<()> {
    let body = vec![b'U'; LARGE_BODY_LEN];
    let mut request = format!(
        "POST /budgeted-upload HTTP/1.1\r\nHost: {HOST}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);

    https_http1_case(
        "guest_https_http1_large_upload_survives_constrained_drive_budget",
        0x229,
    )
    .authority(HOST)
    .large_upload_with_early_response(request, &LARGE_RESPONSE_BODY)
    .drive_step_budget(8)
    .run::<AgentdpNetworkSim>()
}

/// Verifies budget-limited dataplane turns requeue intercepted TLS response continuations.
///
/// # Errors
///
/// Returns an error when a large HTTPS response is truncated or stalls under a constrained byte budget.
#[test]
fn simulated_guest_https_http1_large_response_survives_constrained_drive_byte_budget() -> Result<()> {
    https_http1_case(
        "guest_https_http1_large_response_survives_constrained_drive_byte_budget",
        0x22b,
    )
    .authority(HOST)
    .upstream_response(&LARGE_RESPONSE_BODY)
    .upstream_close_after_response()
    .request(format!(
        "GET /budgeted-response HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
    ))
    .drive_byte_budget(4096)
    .run::<AgentdpNetworkSim>()
}

/// Verifies upstream EOF does not keep the dataplane runnable while guest response reads are backpressured.
///
/// # Errors
///
/// Returns an error when upstream close plus downstream pressure truncates the response or prevents quiescence.
#[test]
fn simulated_guest_https_http1_huge_response_close_quiesces_under_backpressure() -> Result<()> {
    https_http1_case(
        "guest_https_http1_huge_response_close_quiesces_under_backpressure",
        0x227,
    )
    .authority(HOST)
    .upstream_response(&HUGE_RESPONSE_BODY)
    .upstream_close_after_response()
    .read_response_after_backpressure()
    .request(format!(
        "GET /huge-before-eof HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
    ))
    .run::<AgentdpNetworkSim>()
}

/// Verifies quiescent HTTPS/1 shutdown when upstream EOF follows a short response.
///
/// # Errors
///
/// Returns an error when close notification loses response bytes or shutdown fails to quiesce.
#[test]
fn simulated_guest_https_http1_handles_upstream_eof_after_short_response() -> Result<()> {
    https_http1_case("guest_https_http1_handles_upstream_eof_after_short_response", 0x212)
        .authority(HOST)
        .upstream_response(b"short-before-eof\n")
        .upstream_close_after_response()
        .request(format!(
            "GET /short-before-eof HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
        ))
        .run::<AgentdpNetworkSim>()
}

/// Verifies HTTPS/1 chunked request relay and chunked response completion.
///
/// # Errors
///
/// Returns an error when chunked request framing, header mediation, or response completion is wrong.
#[test]
fn simulated_guest_https_http1_preserves_chunked_request_and_reads_chunked_response() -> Result<()> {
    https_http1_case(
        "guest_https_http1_preserves_chunked_request_and_reads_chunked_response",
        0x20a,
    )
    .authority(HOST)
    .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
    .upstream_chunked_response(CHUNKED_RESPONSE_BODY, CHUNKED_RESPONSE_BODY.len())
    .request(format!(
        "POST /chunked HTTP/1.1\r\nHost: {HOST}\r\nX-Api-Key: {PLACEHOLDER}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\ntest\r\n1e\r\nbody {PLACEHOLDER} text\r\n0\r\n\r\n"
    ))
    .run::<AgentdpNetworkSim>()
}

/// Verifies that `Expect: 100-continue` streams the body without body substitution.
///
/// # Errors
///
/// Returns an error when the body is mediated, rejected, or lost after the header phase.
#[test]
fn simulated_guest_https_http1_streams_expect_continue_body_without_substitution() -> Result<()> {
    let body = format!("body keeps literal {PLACEHOLDER}");
    https_http1_case(
        "guest_https_http1_streams_expect_continue_body_without_substitution",
        0x20b,
    )
    .authority(HOST)
    .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
    .upstream_response(b"expect-ok\n")
    .request(format!(
        "POST /expect HTTP/1.1\r\nHost: {HOST}\r\nExpect: 100-continue\r\nX-Api-Key: {PLACEHOLDER}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    ))
    .run::<AgentdpNetworkSim>()
}

/// Verifies that compact request bodies may contain unrelated secret-looking history text.
///
/// # Errors
///
/// Returns an error when HTTPS interception rejects the compact body, leaks unauthorized secret values, or rewrites
/// unrelated placeholder text.
#[test]
fn simulated_guest_https_http1_preserves_unrelated_secret_like_compact_body_text() -> Result<()> {
    const AUTHORIZED_PLACEHOLDER: &str = "AGENTDP_SECRET_COMPACT";
    const AUTHORIZED_SECRET: &str = "compact-value";
    const ALTINN_PLACEHOLDER: &str = "AGENTDP_SECRET_ALTINN";
    const ALTINN_SECRET: &str = "altinn-value";

    let mut body = Vec::with_capacity(512 * 1024);
    body.extend_from_slice(b"{\"model\":\"codex\",\"input\":\"");
    while body.len() < 512 * 1024 - 256 {
        body.extend_from_slice(b"context line with ordinary compact history and tool output\\n");
    }
    body.extend_from_slice(b" authorized=");
    body.extend_from_slice(AUTHORIZED_PLACEHOLDER.as_bytes());
    body.extend_from_slice(b" unrelated-history=");
    body.extend_from_slice(ALTINN_PLACEHOLDER.as_bytes());
    body.extend_from_slice(b"\"}");

    let mut request = format!(
        "POST /backend-api/codex/responses/compact HTTP/1.1\r\nHost: {HOST}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);

    https_http1_case(
        "guest_https_http1_preserves_unrelated_secret_like_compact_body_text",
        0x208,
    )
    .authority(HOST)
    .secret(AUTHORIZED_PLACEHOLDER, AUTHORIZED_SECRET, [HOST])
    .secret(ALTINN_PLACEHOLDER, ALTINN_SECRET, ["api.altinn.no"])
    .upstream_response(b"compact-ok\n")
    .request(request)
    .run::<AgentdpNetworkSim>()
}

/// Verifies that unresolved secret-looking text in HTTP headers fails closed.
///
/// # Errors
///
/// Returns an error when the request succeeds or the unresolved header placeholder reaches upstream.
#[test]
fn simulated_guest_https_http1_rejects_unresolved_secret_placeholder_in_header() -> Result<()> {
    https_http1_case(
        "guest_https_http1_rejects_unresolved_secret_placeholder_in_header",
        0x20c,
    )
    .authority(HOST)
    .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
    .upstream_response(b"should-not-complete\n")
    .request(format!(
        "GET /bad-header HTTP/1.1\r\nHost: {HOST}\r\nX-Trace: {UNKNOWN_PLACEHOLDER}\r\nConnection: close\r\n\r\n"
    ))
    .forbid_upstream(UNKNOWN_PLACEHOLDER.as_bytes())
    .expect_failure()
    .run::<AgentdpNetworkSim>()
}

/// Verifies that a placeholder scoped to another authority fails closed and does not reach upstream as a secret.
///
/// # Errors
///
/// Returns an error when the request succeeds or modeled failure invariants are violated.
#[test]
fn simulated_guest_https_http1_rejects_secret_bound_to_different_authority() -> Result<()> {
    https_http1_case(
        "guest_https_http1_rejects_secret_bound_to_different_authority",
        0x204,
    )
    .authority(BLOCKED_HOST)
    .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
    .upstream_response(b"should-not-complete\n")
    .request(format!(
        "GET /blocked HTTP/1.1\r\nHost: {BLOCKED_HOST}\r\nAuthorization: Bearer {PLACEHOLDER}\r\nConnection: close\r\n\r\n"
    ))
    .expect_failure()
    .run::<AgentdpNetworkSim>()
}

/// Verifies that unresolved secret-looking placeholders fail closed.
///
/// # Errors
///
/// Returns an error when the request succeeds or modeled failure invariants are violated.
#[test]
fn simulated_guest_https_http1_fails_closed_on_unresolved_secret_placeholder() -> Result<()> {
    https_http1_case(
        "guest_https_http1_fails_closed_on_unresolved_secret_placeholder",
        0x205,
    )
    .authority(HOST)
    .upstream_response(b"should-not-complete\n")
    .request(format!(
        "GET /unknown HTTP/1.1\r\nHost: {HOST}\r\nAuthorization: Bearer {UNKNOWN_PLACEHOLDER}\r\nConnection: close\r\n\r\n"
    ))
    .forbid_upstream(UNKNOWN_PLACEHOLDER.as_bytes())
    .expect_failure()
    .run::<AgentdpNetworkSim>()
}

/// Verifies that an untrusted upstream certificate fails through egress telemetry.
///
/// # Errors
///
/// Returns an error when the request succeeds or modeled failure invariants are violated.
#[test]
fn simulated_guest_https_http1_reports_egress_error_for_untrusted_upstream_ca() -> Result<()> {
    https_http1_case(
        "guest_https_http1_reports_egress_error_for_untrusted_upstream_ca",
        0x206,
    )
    .authority(HOST)
    .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
    .upstream_response(b"should-not-complete\n")
    .request(format!(
        "GET /untrusted HTTP/1.1\r\nHost: {HOST}\r\nAuthorization: Bearer {PLACEHOLDER}\r\nConnection: close\r\n\r\n"
    ))
    .untrusted_upstream()
    .expect_failure()
    .run::<AgentdpNetworkSim>()
}

/// Verifies that configured TLS bypass leaves the upstream certificate visible to the guest.
///
/// # Errors
///
/// Returns an error when bypass does not complete over the upstream CA or modeled invariants are violated.
#[test]
fn simulated_guest_https_http1_bypasses_configured_authority() -> Result<()> {
    https_http1_case("guest_https_http1_bypasses_configured_authority", 0x207)
        .authority(BYPASS_HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [BYPASS_HOST])
        .upstream_response(b"agentdp-bypass\n")
        .request(format!(
            "GET /bypass HTTP/1.1\r\nHost: {BYPASS_HOST}\r\nAuthorization: Bearer bypass-token\r\nConnection: close\r\n\r\n"
        ))
        .bypass_tls()
        .run::<AgentdpNetworkSim>()
}

/// Verifies that configured TLS bypass relays large HTTPS/1 responses without interception.
///
/// # Errors
///
/// Returns an error when the bypass TLS path truncates, rejects, or rewrites the large response.
#[test]
fn simulated_guest_https_http1_bypass_relays_large_response() -> Result<()> {
    https_http1_case("guest_https_http1_bypass_relays_large_response", 0x211)
        .authority(BYPASS_HOST)
        .upstream_response(&BYPASS_LARGE_RESPONSE_BODY)
        .request(format!(
            "GET /bypass-large HTTP/1.1\r\nHost: {BYPASS_HOST}\r\nConnection: close\r\n\r\n"
        ))
        .bypass_tls()
        .run::<AgentdpNetworkSim>()
}

/// Verifies WSS upgrade header mediation and raw WebSocket message relay over the HTTPS path.
///
/// # Errors
///
/// Returns an error when the WSS scenario fails or the modeled behavior invariants are violated.
#[test]
fn simulated_guest_wss_http1_substitutes_upgrade_header_then_relays_message() -> Result<()> {
    wss_http1_case("guest_wss_http1_substitutes_upgrade_header_then_relays_message", 0x202)
        .authority(HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
        .message(b"ping-after-upgrade\n")
        .upstream_response(b"pong-after-upgrade\n")
        .run::<AgentdpNetworkSim>()
}

/// Verifies that bytes buffered behind a WSS upgrade are flushed as HTTP when the upstream rejects the upgrade.
///
/// # Errors
///
/// Returns an error when the post-rejection HTTP request is dropped, rewritten incorrectly, or treated as WebSocket.
#[test]
fn simulated_guest_wss_http1_flushes_buffered_http_after_upgrade_rejection() -> Result<()> {
    wss_http1_case("guest_wss_http1_flushes_buffered_http_after_upgrade_rejection", 0x224)
        .authority(HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
        .upstream_response(b"after-rejected-upgrade\n")
        .reject_upgrade_then_http(format!(
            "GET /after HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
        ))
        .run::<AgentdpNetworkSim>()
}

/// Verifies WSS relay for messages that require extended WebSocket payload lengths.
///
/// # Errors
///
/// Returns an error when the large tunneled message is truncated, rewritten, or not echoed.
#[test]
fn simulated_guest_wss_http1_relays_large_message() -> Result<()> {
    wss_http1_case("guest_wss_http1_relays_large_message", 0x20d)
        .authority(HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
        .message(vec![b'M'; 70 * 1024])
        .upstream_response(vec![b'P'; 512])
        .run::<AgentdpNetworkSim>()
}

/// Verifies WSS relay for fragmented client text messages.
///
/// # Errors
///
/// Returns an error when continuation frames are not reassembled as the upstream message.
#[test]
fn simulated_guest_wss_http1_relays_fragmented_message() -> Result<()> {
    wss_http1_case("guest_wss_http1_relays_fragmented_message", 0x20e)
        .authority(HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
        .message(b"fragmented-message-after-upgrade".to_vec())
        .upstream_response(b"fragmented-ok".to_vec())
        .fragmented()
        .run::<AgentdpNetworkSim>()
}

/// Verifies that secret-looking bytes after WSS upgrade are tunnel payload, not HTTP headers.
///
/// # Errors
///
/// Returns an error when post-upgrade payload bytes are substituted, rejected, or leaked as secret values.
#[test]
fn simulated_guest_wss_http1_preserves_secret_placeholder_after_upgrade() -> Result<()> {
    wss_http1_case("guest_wss_http1_preserves_secret_placeholder_after_upgrade", 0x20f)
        .authority(HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
        .message(format!("post-upgrade payload keeps {PLACEHOLDER}").into_bytes())
        .upstream_response(b"tunnel-ok".to_vec())
        .run::<AgentdpNetworkSim>()
}

/// Verifies quiescent WSS shutdown when the upstream closes after its response.
///
/// # Errors
///
/// Returns an error when close notification loses the response or leaves the scenario non-quiescent.
#[test]
fn simulated_guest_wss_http1_handles_upstream_close_after_message() -> Result<()> {
    wss_http1_case("guest_wss_http1_handles_upstream_close_after_message", 0x210)
        .authority(HOST)
        .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
        .message(b"close-after-this-message".to_vec())
        .upstream_response(b"closing-now".to_vec())
        .close_after_response()
        .run::<AgentdpNetworkSim>()
}
