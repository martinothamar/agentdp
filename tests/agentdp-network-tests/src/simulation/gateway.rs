use std::time::Duration;

use agentdp_network::test_support::simulation::SimulationUpstreams;
use agentdp_rand::Seed;

use super::case_support::{allow_all_network_config, stop_network_report};
use super::checkers::{LinkTraceContains, LinkTracePrecedes, Quiescent, TelemetryEquals, check_all};
use super::fixtures::{arp_request, verify_arp_reply};
use super::gateway_case::gateway_frame_case;
use super::{AgentdpNetworkSim, DriveBudget, Result, Simulator, SteppedNetwork};
use super::{
    GuestLinkConfig, LinkDirection, LinkFault, LinkTraceEventKind, NetworkUnderTest, RawFrameGuest,
    ScenarioNetworkConfig,
};

/// Verifies that a guest ARP request for the gateway address produces a gateway ARP reply.
///
/// # Errors
///
/// Returns an error when the network cannot start/stop, no reply is produced, or the reply violates the expected ARP
/// invariant.
#[test]
fn simulated_network_answers_guest_arp_request() -> Result<()> {
    gateway_frame_case("guest_arp_request_is_answered", 0x100)
        .arp_request()
        .run::<AgentdpNetworkSim>()
}

/// Verifies that a guest ICMP echo request to the gateway produces an echo reply.
///
/// # Errors
///
/// Returns an error when the network cannot start/stop, no reply is produced, or the reply violates the expected ICMP
/// invariant.
#[test]
fn simulated_network_answers_guest_icmp_echo_request() -> Result<()> {
    gateway_frame_case("guest_icmp_echo_request_is_answered", 0x101)
        .arp_request()
        .icmp_echo_request(0x1234, 0x0007, b"agentdp-simulation")
        .run::<AgentdpNetworkSim>()
}

/// Verifies scheduler-delayed guest and network frames still preserve gateway ARP behavior.
///
/// # Errors
///
/// Returns an error when delayed delivery loses the ARP request or reply.
#[test]
fn simulated_delayed_guest_arp_request_is_answered() -> Result<()> {
    gateway_frame_case("delayed_guest_arp_request_is_answered", 0x102)
        .delay_path(LinkDirection::GuestToNetwork, Duration::from_millis(3))
        .delay_path(LinkDirection::NetworkToGuest, Duration::from_millis(2))
        .expect_packet_event(
            LinkDirection::GuestToNetwork,
            LinkTraceEventKind::Scheduled,
            Duration::from_millis(3),
            1,
        )
        .expect_packet_event(
            LinkDirection::GuestToNetwork,
            LinkTraceEventKind::Delivered,
            Duration::from_millis(3),
            1,
        )
        .expect_packet_event(
            LinkDirection::NetworkToGuest,
            LinkTraceEventKind::Scheduled,
            Duration::from_millis(5),
            2,
        )
        .expect_packet_event(
            LinkDirection::NetworkToGuest,
            LinkTraceEventKind::Delivered,
            Duration::from_millis(5),
            2,
        )
        .arp_request()
        .run::<AgentdpNetworkSim>()
}

/// Verifies a scheduler-dropped guest frame never reaches the network and the scenario quiesces.
///
/// # Errors
///
/// Returns an error when the dropped frame is observed by telemetry or shutdown does not quiesce.
#[test]
fn simulated_dropped_guest_arp_request_quiesces_without_reply() -> Result<()> {
    let (sim, running, guest) = start_gateway::<AgentdpNetworkSim>(0x103, GuestLinkConfig::default(), |guest_link| {
        guest_link.push_fault(LinkFault::DropNextGuestFrame);
    })?;

    guest.send_frame(arp_request())?;

    let mut report = stop_network_report(
        "dropped_guest_arp_request_quiesces_without_reply",
        sim,
        running,
        guest.link(),
    )?;
    check_all(
        &mut report,
        vec![
            Box::new(TelemetryEquals::new().guest_frames_received(0).host_frames_sent(0)),
            Box::new(Quiescent),
        ],
    )
}

/// Verifies a scheduler-duplicated guest frame is delivered as two independent gateway requests.
///
/// # Errors
///
/// Returns an error when either duplicated ARP request fails to produce a valid reply.
#[test]
fn simulated_duplicated_guest_arp_request_produces_two_replies() -> Result<()> {
    let (mut sim, mut running, guest) =
        start_gateway::<AgentdpNetworkSim>(0x104, GuestLinkConfig::default(), |guest_link| {
            guest_link.duplicate_next(LinkDirection::GuestToNetwork);
        })?;

    guest.send_frame(arp_request())?;
    for label in ["first duplicated ARP reply", "second duplicated ARP reply"] {
        let reply = guest.recv_frame(&mut sim, &mut running, label, DriveBudget::default())?;
        verify_arp_reply(&reply)?;
    }

    let mut report = stop_network_report(
        "duplicated_guest_arp_request_produces_two_replies",
        sim,
        running,
        guest.link(),
    )?;
    check_all(
        &mut report,
        vec![
            Box::new(TelemetryEquals::new().guest_frames_received(2).host_frames_sent(2)),
            Box::new(Quiescent),
        ],
    )
}

/// Verifies a scheduler-duplicated frame is still bounded by path capacity.
///
/// # Errors
///
/// Returns an error when the duplicate bypasses capacity and produces a second gateway reply.
#[test]
fn simulated_duplicated_guest_frame_respects_capacity() -> Result<()> {
    let (mut sim, mut running, guest) = start_gateway::<AgentdpNetworkSim>(
        0x106,
        GuestLinkConfig {
            queue_capacity: 1,
            ..GuestLinkConfig::default()
        },
        |guest_link| {
            guest_link.set_path_delay(LinkDirection::GuestToNetwork, Duration::from_millis(4));
            guest_link.duplicate_next(LinkDirection::GuestToNetwork);
        },
    )?;

    guest.send_frame(arp_request())?;
    let reply = guest.recv_frame(
        &mut sim,
        &mut running,
        "capacity bounded duplicated ARP reply",
        DriveBudget::default(),
    )?;
    verify_arp_reply(&reply)?;

    let mut report = stop_network_report("duplicated_guest_frame_respects_capacity", sim, running, guest.link())?;
    check_all(
        &mut report,
        vec![
            Box::new(TelemetryEquals::new().guest_frames_received(1).host_frames_sent(1)),
            Box::new(
                LinkTraceContains::new(LinkDirection::GuestToNetwork, LinkTraceEventKind::CapacityDropped).sequence(2),
            ),
            Box::new(Quiescent),
        ],
    )
}

/// Verifies a disabled guest-to-network path drops submitted guest frames and quiesces.
///
/// # Errors
///
/// Returns an error when a disabled path allows the frame through or shutdown does not quiesce.
#[test]
fn simulated_disabled_guest_to_network_path_drops_frame() -> Result<()> {
    let (sim, running, guest) = start_gateway::<AgentdpNetworkSim>(0x107, GuestLinkConfig::default(), |guest_link| {
        guest_link.set_path_enabled(LinkDirection::GuestToNetwork, false);
    })?;

    guest.send_frame(arp_request())?;

    let mut report = stop_network_report("disabled_guest_to_network_path_drops_frame", sim, running, guest.link())?;
    check_all(
        &mut report,
        vec![
            Box::new(TelemetryEquals::new().guest_frames_received(0).host_frames_sent(0)),
            Box::new(LinkTraceContains::new(
                LinkDirection::GuestToNetwork,
                LinkTraceEventKind::DisabledPathDropped,
            )),
            Box::new(Quiescent),
        ],
    )
}

/// Verifies one-shot reorder changes delivery order without losing duplicated frames.
///
/// # Errors
///
/// Returns an error when reordering drops frames, preserves the original order, or shutdown does not quiesce.
#[test]
fn simulated_reordered_duplicated_guest_frames_are_delivered_out_of_order() -> Result<()> {
    let (mut sim, mut running, guest) =
        start_gateway::<AgentdpNetworkSim>(0x108, GuestLinkConfig::default(), |guest_link| {
            guest_link.set_path_delay(LinkDirection::GuestToNetwork, Duration::from_millis(2));
            guest_link.duplicate_next(LinkDirection::GuestToNetwork);
            guest_link.reorder_next(LinkDirection::GuestToNetwork);
        })?;

    guest.send_frame(arp_request())?;
    for label in ["first reordered ARP reply", "second reordered ARP reply"] {
        let reply = guest.recv_frame(&mut sim, &mut running, label, DriveBudget::default())?;
        verify_arp_reply(&reply)?;
    }

    let mut report = stop_network_report(
        "reordered_duplicated_guest_frames_are_delivered_out_of_order",
        sim,
        running,
        guest.link(),
    )?;
    check_all(
        &mut report,
        vec![
            Box::new(TelemetryEquals::new().guest_frames_received(2).host_frames_sent(2)),
            Box::new(LinkTraceContains::new(
                LinkDirection::GuestToNetwork,
                LinkTraceEventKind::Reordered,
            )),
            Box::new(LinkTracePrecedes::new(
                LinkTraceContains::new(LinkDirection::GuestToNetwork, LinkTraceEventKind::Delivered).sequence(2),
                LinkTraceContains::new(LinkDirection::GuestToNetwork, LinkTraceEventKind::Delivered).sequence(1),
            )),
            Box::new(Quiescent),
        ],
    )
}

/// Verifies sustained guest input does not starve already-read gateway work.
///
/// # Errors
///
/// Returns an error when the event loop keeps draining guest frames without producing gateway replies.
#[test]
fn simulated_saturated_guest_reads_do_not_starve_gateway_replies() -> Result<()> {
    let mut sim = Simulator::new(Seed::new(0x109));
    let guest_link = sim.guest_link_with(GuestLinkConfig {
        queue_capacity: 64,
        ..GuestLinkConfig::default()
    })?;
    let mut network = allow_all_network_config();
    network.limits.drive_event_budget = 4;
    let mut running = AgentdpNetworkSim::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network,
            upstreams: SimulationUpstreams::default(),
        },
        guest_link.clone(),
    )?;
    let guest = RawFrameGuest::new(guest_link);

    for _ in 0..32 {
        guest.send_frame(arp_request())?;
    }

    let reply = guest.recv_frame(
        &mut sim,
        &mut running,
        "first saturated guest-read ARP reply",
        DriveBudget {
            max_steps: 4,
            step_time: Duration::ZERO,
        },
    )?;
    verify_arp_reply(&reply)?;

    let _stop = running
        .stop()
        .map_err(|error| super::Error::new(format!("stop simulated network: {error}")))?;
    Ok(())
}

/// Verifies scheduler capacity rejects excess queued guest frames without corrupting the accepted frame.
///
/// # Errors
///
/// Returns an error when the first frame is lost, the second frame is accepted, or shutdown does not quiesce.
#[test]
fn simulated_guest_to_network_capacity_rejects_excess_frame() -> Result<()> {
    let (mut sim, mut running, guest) = start_gateway::<AgentdpNetworkSim>(
        0x105,
        GuestLinkConfig {
            queue_capacity: 1,
            ..GuestLinkConfig::default()
        },
        |guest_link| guest_link.set_path_delay(LinkDirection::GuestToNetwork, Duration::from_millis(4)),
    )?;

    guest.send_frame(arp_request())?;
    let Err(error) = guest.send_frame(arp_request()) else {
        return Err(super::Error::new(
            "expected scheduler capacity to reject the second guest ARP frame",
        ));
    };
    if !error.to_string().contains("guest-to-network queue is full") {
        return Err(error);
    }
    let reply = guest.recv_frame(
        &mut sim,
        &mut running,
        "capacity accepted ARP reply",
        DriveBudget::default(),
    )?;
    verify_arp_reply(&reply)?;

    let mut report = stop_network_report(
        "guest_to_network_capacity_rejects_excess_frame",
        sim,
        running,
        guest.link(),
    )?;
    check_all(
        &mut report,
        vec![
            Box::new(TelemetryEquals::new().guest_frames_received(1).host_frames_sent(1)),
            Box::new(Quiescent),
        ],
    )
}

fn start_gateway<N>(
    seed: u64,
    config: GuestLinkConfig,
    configure_link: impl FnOnce(&super::GuestLink),
) -> Result<(Simulator, N::Running, RawFrameGuest)>
where
    N: NetworkUnderTest,
    N::Running: SteppedNetwork,
{
    let mut sim = Simulator::new(Seed::new(seed));
    let guest_link = sim.guest_link_with(config)?;
    configure_link(&guest_link);
    let running = N::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network: allow_all_network_config(),
            upstreams: SimulationUpstreams::default(),
        },
        guest_link.clone(),
    )?;
    let guest = RawFrameGuest::new(guest_link);
    Ok((sim, running, guest))
}
