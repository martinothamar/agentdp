use super::tcp_case::tcp_stream_case;
use super::{AgentdpNetworkSim, Result};

/// Verifies that a guest TCP stream reaches a scripted simulated upstream and receives the scripted response.
///
/// # Errors
///
/// Returns an error when the network cannot start/stop or the TCP transcript does not match.
#[test]
fn simulated_guest_tcp_stream_reaches_scripted_upstream() -> Result<()> {
    tcp_stream_case("guest_tcp_stream_reaches_scripted_upstream", 0x200)
        .request(b"tcp-ping")
        .response(b"tcp-pong")
        .run::<AgentdpNetworkSim>()
}

/// Verifies that scripted simulated upstream EOF is visible through the guest TCP stream.
///
/// # Errors
///
/// Returns an error when the network cannot start/stop or EOF is not observed.
#[test]
fn simulated_guest_tcp_stream_observes_scripted_upstream_eof() -> Result<()> {
    tcp_stream_case("guest_tcp_stream_observes_scripted_upstream_eof", 0x210)
        .request(b"tcp-close")
        .response(b"tcp-bye")
        .upstream_eof()
        .run::<AgentdpNetworkSim>()
}

/// Verifies repeated short-lived TCP connections preserve byte transcripts independently.
///
/// # Errors
///
/// Returns an error when any repeated connection loses or changes bytes.
#[test]
fn simulated_guest_tcp_stream_repeats_short_lived_connections() -> Result<()> {
    tcp_stream_case("guest_tcp_stream_repeats_short_lived_connections", 0x212)
        .request(b"tcp-repeated-ping")
        .response(b"tcp-repeated-pong")
        .iterations(32)
        .run::<AgentdpNetworkSim>()
}

/// Verifies repeated request/response traffic on one established TCP stream.
///
/// # Errors
///
/// Returns an error when the established stream loses ordering or changes bytes.
#[test]
fn simulated_guest_tcp_stream_repeats_on_established_connection() -> Result<()> {
    tcp_stream_case("guest_tcp_stream_repeats_on_established_connection", 0x213)
        .request(b"tcp-established-ping")
        .response(b"tcp-established-pong")
        .iterations(32)
        .reuse_connection()
        .run::<AgentdpNetworkSim>()
}
