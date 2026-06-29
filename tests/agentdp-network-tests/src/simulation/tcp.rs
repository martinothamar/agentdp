use std::time::Duration;

use agentdp_network::test_support::simulation::{SimTcpResponse, SimulationUpstreams};
use agentdp_rand::Seed;

use super::case_support::allow_all_network_config;
use super::fixtures::upstream_addr;
use super::protocol::tcp::tcp_response_handler;
use super::tcp_case::tcp_stream_case;
use super::{AgentdpNetworkSim, Error, NetworkUnderTest, Result, ScenarioNetworkConfig, Simulator, SmolTcpGuest};

static LARGE_TCP_RESPONSE: [u8; 256 * 1024] = [b'P'; 256 * 1024];

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

/// Verifies that a budget-limited dataplane turn requeues plain TCP upstream-read continuations.
///
/// # Errors
///
/// Returns an error when only the first upstream read buffer is delivered and the remaining response stalls.
#[test]
fn simulated_guest_tcp_stream_drains_large_response_with_one_step_budget() -> Result<()> {
    tcp_stream_case("guest_tcp_stream_drains_large_response_with_one_step_budget", 0x214)
        .request(b"tcp-large-response")
        .response(&LARGE_TCP_RESPONSE)
        .drive_step_budget(1)
        .run::<AgentdpNetworkSim>()
}

/// Verifies that byte-budget exhaustion after an upstream read preserves the owned response buffer.
///
/// # Errors
///
/// Returns an error when the proxy reads the upstream response but loses it before it can be flushed to the guest.
#[test]
fn simulated_guest_tcp_stream_drains_response_with_tight_byte_budget() -> Result<()> {
    tcp_stream_case("guest_tcp_stream_drains_response_with_tight_byte_budget", 0x215)
        .request(b"tcp-byte-budget")
        .response(b"tcp-byte-budget-response")
        .drive_byte_budget(4096)
        .run::<AgentdpNetworkSim>()
}

/// Verifies upstream reads stop when the guest TCP receive window is closed.
///
/// # Errors
///
/// Returns an error when the proxy keeps pulling upstream bytes into local pending writes while smoltcp cannot send to
/// the guest.
#[test]
fn simulated_guest_tcp_upstream_read_waits_for_guest_send_window() -> Result<()> {
    let mut sim = Simulator::new(Seed::new(0x218));
    let guest_link = sim.guest_link()?;
    let handler = tcp_response_handler(|bytes| {
        if bytes == b"tcp-window-pressure" {
            Ok(SimTcpResponse::segmented(vec![b'A'; 2048], vec![vec![b'B'; 2048]]))
        } else {
            Ok(SimTcpResponse::reset())
        }
    });
    let mut network = allow_all_network_config();
    network.limits.tcp_socket_buffer_capacity = 2048;
    let mut running = AgentdpNetworkSim::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network,
            upstreams: SimulationUpstreams::default().with_tcp_handler(upstream_addr(), handler),
        },
        guest_link.clone(),
    )?;
    let mut guest = SmolTcpGuest::with_tcp_buffer_bytes(guest_link, 2048)?;
    let tcp = guest.connect(&mut running, upstream_addr())?;
    guest.write_all(&mut running, tcp, b"tcp-window-pressure")?;

    for _step in 0..256 {
        guest.pump(&mut running)?;
        let snapshot = running.tcp_snapshot();
        if snapshot.contains("socket_can_send: false") {
            let _stop = running
                .stop()
                .map_err(|error| Error::new(format!("stop simulated network: {error}")))?;
            if !snapshot.contains("pending_write_bytes: 0") {
                return Err(Error::new(format!(
                    "upstream bytes accumulated while guest send window was closed: {snapshot}"
                )));
            }
            return Ok(());
        }
    }

    let snapshot = running.tcp_snapshot();
    let _stop = running
        .stop()
        .map_err(|error| Error::new(format!("stop simulated network: {error}")))?;
    Err(Error::new(format!(
        "guest send window did not close during test; tcp={snapshot}"
    )))
}

/// Verifies upstream readability observed under guest backpressure is resumed after the guest receive window opens.
///
/// # Errors
///
/// Returns an error when the second upstream segment stalls after the guest drains the first segment.
#[test]
fn simulated_guest_tcp_backpressured_upstream_read_resumes_after_guest_window_opens() -> Result<()> {
    let mut sim = Simulator::new(Seed::new(0x21a));
    let guest_link = sim.guest_link()?;
    let handler = tcp_response_handler(|bytes| {
        if bytes == b"tcp-window-resume" {
            Ok(SimTcpResponse::segmented(vec![b'A'; 2048], vec![vec![b'B'; 2048]]))
        } else {
            Ok(SimTcpResponse::reset())
        }
    });
    let mut network = allow_all_network_config();
    network.limits.tcp_socket_buffer_capacity = 2048;
    let mut running = AgentdpNetworkSim::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network,
            upstreams: SimulationUpstreams::default().with_tcp_handler(upstream_addr(), handler),
        },
        guest_link.clone(),
    )?;
    let mut guest = SmolTcpGuest::with_tcp_buffer_bytes(guest_link, 2048)?;
    let tcp = guest.connect(&mut running, upstream_addr())?;
    guest.write_all(&mut running, tcp, b"tcp-window-resume")?;

    for _step in 0..256 {
        guest.pump(&mut running)?;
        if running.tcp_snapshot().contains("socket_can_send: false") {
            let first = guest.read_until(
                &mut running,
                tcp,
                "first TCP segment under guest send window pressure",
                |bytes| !bytes.is_empty(),
            )?;
            if first != vec![b'A'; 2048] {
                let _stop = running
                    .stop()
                    .map_err(|error| Error::new(format!("stop simulated network: {error}")))?;
                return Err(Error::new(format!(
                    "unexpected first guest segment after window pressure: {first:02x?}"
                )));
            }
            let second = guest.read_until(
                &mut running,
                tcp,
                "TCP upstream read after guest send window reopens",
                |bytes| bytes.len() == 2048 && bytes.iter().all(|byte| *byte == b'B'),
            )?;
            let _stop = running
                .stop()
                .map_err(|error| Error::new(format!("stop simulated network: {error}")))?;
            if second != vec![b'B'; 2048] {
                return Err(Error::new(format!("unexpected second guest segment: {second:02x?}")));
            }
            return Ok(());
        }
    }

    let snapshot = running.tcp_snapshot();
    let _stop = running
        .stop()
        .map_err(|error| Error::new(format!("stop simulated network: {error}")))?;
    Err(Error::new(format!(
        "guest send window did not close during resume test; tcp={snapshot}"
    )))
}

/// Verifies simulated TCP readiness remains advisory when the next read reports `WouldBlock`.
///
/// # Errors
///
/// Returns an error when a follow-up readiness/`WouldBlock` cycle stalls the public TCP behavior.
#[test]
fn simulated_guest_tcp_stream_handles_would_block_after_readiness() -> Result<()> {
    let mut sim = Simulator::new(Seed::new(0x216));
    let guest_link = sim.guest_link()?;
    let handler = tcp_response_handler(|bytes| {
        if bytes == b"tcp-segmented-response" {
            Ok(SimTcpResponse::segmented(
                b"tcp-".to_vec(),
                vec![b"segmented-response".to_vec()],
            ))
        } else {
            Ok(SimTcpResponse::reset())
        }
    });
    let mut running = AgentdpNetworkSim::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network: allow_all_network_config(),
            upstreams: SimulationUpstreams::default().with_tcp_handler(upstream_addr(), handler),
        },
        guest_link.clone(),
    )?;
    let mut guest = SmolTcpGuest::new(guest_link)?;
    let tcp = guest.connect(&mut running, upstream_addr())?;
    guest.write_all(&mut running, tcp, b"tcp-segmented-response")?;
    let response = guest.read_until(&mut running, tcp, "tcp readiness followed by WouldBlock", |bytes| {
        bytes == b"tcp-segmented-response"
    })?;
    let _stop = running
        .stop()
        .map_err(|error| Error::new(format!("stop simulated network: {error}")))?;
    if response != b"tcp-segmented-response" {
        return Err(Error::new(format!("unexpected response: {response:02x?}")));
    }
    Ok(())
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

/// Verifies production-mode turns still drain reactor readiness after already-known local output.
///
/// # Errors
///
/// Returns an error when a local-progress turn leaves a ready reactor item pending.
#[test]
fn simulated_production_turn_drains_reactor_readiness_after_local_progress() -> Result<()> {
    let mut sim = Simulator::new(Seed::new(0x215));
    let guest_link = sim.guest_link()?;
    let mut running = AgentdpNetworkSim::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network: allow_all_network_config(),
            upstreams: SimulationUpstreams::default(),
        },
        guest_link,
    )?;

    running
        .queue_network_to_guest_frame(&[0; 64])
        .map_err(|error| Error::new(format!("queue test guest frame: {error}")))?;
    running.queue_guest_readiness();
    if running.pending_reactor_ready() != 1 {
        return Err(Error::new("expected one queued simulated readiness item before drive"));
    }

    running.drive_once_production_mode();
    let pending = running.pending_reactor_ready();
    let _stop = running
        .stop()
        .map_err(|error| Error::new(format!("stop simulated network: {error}")))?;
    if pending != 0 {
        return Err(Error::new(format!(
            "production-mode drive left {pending} reactor readiness item(s) pending after local progress"
        )));
    }
    Ok(())
}

/// Verifies timer-driven gateway polling does not drop guest-bound frames already queued by earlier work.
///
/// # Errors
///
/// Returns an error when a queued guest-bound frame is lost while a gateway timer is expired.
#[test]
fn simulated_gateway_timer_preserves_queued_guest_frames() -> Result<()> {
    let mut sim = Simulator::new(Seed::new(0x219));
    let guest_link = sim.guest_link()?;
    let mut running = AgentdpNetworkSim::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network: allow_all_network_config(),
            upstreams: SimulationUpstreams::default(),
        },
        guest_link.clone(),
    )?;

    running
        .queue_network_to_guest_frame(b"queued-before-timer")
        .map_err(|error| Error::new(format!("queue test guest frame: {error}")))?;
    running.advance_clock(Duration::from_secs(1));
    let frame = sim.drive_until_network_frame(
        &mut running,
        &guest_link,
        "guest-bound frame queued before expired gateway timer",
        super::DriveBudget {
            max_steps: 2,
            step_time: Duration::ZERO,
        },
    )?;
    let _stop = running
        .stop()
        .map_err(|error| Error::new(format!("stop simulated network: {error}")))?;
    if frame != b"queued-before-timer" {
        return Err(Error::new(format!("unexpected guest-bound frame: {frame:02x?}")));
    }
    Ok(())
}

/// Verifies a spurious guest readiness hint is consumed without fabricating dataplane progress or spinning.
///
/// # Errors
///
/// Returns an error when a spurious readiness item remains queued or shutdown fails.
#[test]
fn simulated_spurious_guest_readiness_is_bounded() -> Result<()> {
    let mut sim = Simulator::new(Seed::new(0x217));
    let guest_link = sim.guest_link()?;
    let mut running = AgentdpNetworkSim::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network: allow_all_network_config(),
            upstreams: SimulationUpstreams::default(),
        },
        guest_link,
    )?;

    running.queue_guest_readiness();
    running.drive_once_production_mode();
    let pending = running.pending_reactor_ready();
    let _stop = running
        .stop()
        .map_err(|error| Error::new(format!("stop simulated network: {error}")))?;
    if pending != 0 {
        return Err(Error::new(format!(
            "spurious guest readiness left {pending} reactor readiness item(s) pending"
        )));
    }
    Ok(())
}
