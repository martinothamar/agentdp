use super::fixtures::{HOST, PLACEHOLDER, SECRET_VALUE};
use super::http_case::plain_http1_case;
use super::{AgentdpNetworkSim, Result};

const LARGE_RESPONSE_LEN: usize = 16 * 1024;
static LARGE_RESPONSE_BODY: [u8; LARGE_RESPONSE_LEN] = [b'H'; LARGE_RESPONSE_LEN];

/// Verifies plaintext HTTP/1 relay behavior without configured secret mediation.
///
/// # Errors
///
/// Returns an error when the plaintext HTTP request does not reach the upstream or the network does not stop cleanly.
#[test]
fn simulated_guest_plain_http1_reaches_scripted_upstream_without_secret_mediation() -> Result<()> {
    plain_http1_case(
        "guest_plain_http1_reaches_scripted_upstream_without_secret_mediation",
        0x220,
    )
    .request(format!(
        "GET /plain HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
    ))
    .upstream_response(b"agentdp-plain-http\n")
    .run::<AgentdpNetworkSim>()
}

/// Verifies repeated plaintext HTTP clients receive independent responses.
///
/// # Errors
///
/// Returns an error when repeated request/response transcripts do not match.
#[test]
fn simulated_guest_plain_http1_repeats_short_lived_clients() -> Result<()> {
    plain_http1_case("guest_plain_http1_repeats_short_lived_clients", 0x224)
        .request(format!(
            "GET /many HTTP/1.1\r\nHost: {HOST}\r\nUser-Agent: agentdp-sim\r\nConnection: close\r\n\r\n"
        ))
        .upstream_response(b"agentdp-many-http\n")
        .iterations(32)
        .run::<AgentdpNetworkSim>()
}

/// Verifies a large plaintext HTTP response is drained before upstream EOF closes the stream.
///
/// # Errors
///
/// Returns an error when the response is truncated or close handling prevents quiescence.
#[test]
fn simulated_guest_plain_http1_drains_large_response_after_upstream_eof() -> Result<()> {
    plain_http1_case("guest_plain_http1_drains_large_response_after_upstream_eof", 0x225)
        .request(format!(
            "GET /large-after-eof HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
        ))
        .upstream_response(&LARGE_RESPONSE_BODY)
        .upstream_close_after_response()
        .run::<AgentdpNetworkSim>()
}

/// Verifies that DNS attribution allows restricted plaintext HTTP egress to the resolved upstream IP.
///
/// # Errors
///
/// Returns an error when DNS attribution is not recorded or the attributed HTTP request is denied.
#[test]
fn simulated_guest_plain_http1_uses_dns_attribution_for_restricted_egress() -> Result<()> {
    plain_http1_case("guest_plain_http1_uses_dns_attribution_for_restricted_egress", 0x222)
        .attribute_host(HOST)
        .restrict_to_authority(HOST)
        .request(format!(
            "GET /dns-attributed HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
        ))
        .upstream_response(b"agentdp-attributed-http\n")
        .run::<AgentdpNetworkSim>()
}

/// Verifies that restricted plaintext HTTP egress fails closed without DNS attribution.
///
/// # Errors
///
/// Returns an error when an unattributed restricted request reaches the upstream.
#[test]
fn simulated_guest_plain_http1_rejects_restricted_egress_without_dns_attribution() -> Result<()> {
    plain_http1_case(
        "guest_plain_http1_rejects_restricted_egress_without_dns_attribution",
        0x223,
    )
    .restrict_to_authority(HOST)
    .request(format!(
        "GET /unattributed HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"
    ))
    .upstream_response(b"should-not-complete\n")
    .expect_denied()
    .run::<AgentdpNetworkSim>()
}

/// Verifies that plaintext HTTP with configured secrets fails closed instead of substituting.
///
/// # Errors
///
/// Returns an error when the request succeeds, leaks the secret value upstream, or no egress error is reported.
#[test]
fn simulated_guest_plain_http1_rejects_secret_placeholder_when_secrets_are_configured() -> Result<()> {
    plain_http1_case(
        "guest_plain_http1_rejects_secret_placeholder_when_secrets_are_configured",
        0x221,
    )
    .request(format!(
        "GET /plain-secret HTTP/1.1\r\nHost: {HOST}\r\nAuthorization: Bearer {PLACEHOLDER}\r\nConnection: close\r\n\r\n"
    ))
    .upstream_response(b"should-not-complete\n")
    .secret(PLACEHOLDER, SECRET_VALUE, [HOST])
    .expect_failure()
    .run::<AgentdpNetworkSim>()
}
