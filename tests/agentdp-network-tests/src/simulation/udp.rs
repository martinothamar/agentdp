use std::cell::RefCell;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::rc::Rc;

use agentdp_network::test_support::simulation::{SimUdpResponse, SimulationUpstreams};
use agentdp_rand::Seed;

use super::case_support::{allow_all_network_config, stop_network_report};
use super::checkers::{NoUnexpectedEgressErrors, Quiescent, TranscriptEquals, check_all};
use super::fixtures::{DNS_UPSTREAM, UPSTREAM_IP};
use super::packets::{GATEWAY_IP, dns_a_query, dns_a_response, read_u16};
use super::protocol::udp::udp_response_handler;
use super::{AgentdpNetworkSim, Result, Simulator, SteppedNetwork};
use super::{NetworkUnderTest, ScenarioNetworkConfig, SmolTcpGuest};

const GUEST_DATAGRAMS: &str = "guest.datagrams";
const UPSTREAM_DATAGRAMS: &str = "upstream.datagrams";
const UDP_PORT: u16 = 7_777;
const FAST_TXID: u16 = 0x5102;
const SLOW_TXID: u16 = 0x5101;
const FAST_HOST: &str = "fast.test";
const SLOW_HOST: &str = "slow.test";
const FAST_IP: [u8; 4] = [10, 73, 0, 12];
const SLOW_IP: [u8; 4] = [10, 73, 0, 11];

/// Verifies a guest UDP datagram reaches a scripted upstream and the reply preserves datagram bytes.
///
/// # Errors
///
/// Returns an error when the UDP transcript does not match or the network does not quiesce.
#[test]
fn simulated_guest_udp_datagram_reaches_scripted_upstream() -> Result<()> {
    guest_udp_echoes_datagrams::<AgentdpNetworkSim>("guest_udp_datagram_reaches_scripted_upstream", 0x301, 1, 64)
}

/// Verifies repeated guest UDP datagrams keep independent datagram boundaries.
///
/// # Errors
///
/// Returns an error when any repeated UDP datagram is lost, duplicated, or changed.
#[test]
fn simulated_guest_udp_repeats_datagrams_without_cross_talk() -> Result<()> {
    guest_udp_echoes_datagrams::<AgentdpNetworkSim>("guest_udp_repeats_datagrams_without_cross_talk", 0x302, 32, 1200)
}

/// Verifies concurrent DNS UDP proxies are independent when one response is delayed.
///
/// # Errors
///
/// Returns an error when the fast response is blocked by the delayed slow response, or either answer is wrong.
#[test]
fn simulated_guest_dns_udp_handles_delayed_concurrent_queries() -> Result<()> {
    let fast_query = dns_a_query(FAST_HOST, FAST_TXID)?;
    let slow_query = dns_a_query(SLOW_HOST, SLOW_TXID)?;
    let fast_response = dns_a_response(FAST_HOST, FAST_TXID, FAST_IP, 60)?;
    let slow_response = dns_a_response(SLOW_HOST, SLOW_TXID, SLOW_IP, 60)?;
    let upstream_datagrams = Rc::new(RefCell::new(Vec::new()));
    let handler = udp_response_handler({
        let fast_response = fast_response.clone();
        let slow_response = slow_response.clone();
        let upstream_datagrams = Rc::clone(&upstream_datagrams);
        move |query| {
            upstream_datagrams.borrow_mut().push(query.to_vec());
            match read_u16(query, 0) {
                FAST_TXID => Ok(SimUdpResponse::bytes(fast_response.clone())),
                SLOW_TXID => Ok(SimUdpResponse::delayed(slow_response.clone(), 8)),
                txid => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected DNS transaction id {txid:#x}"),
                )),
            }
        }
    });

    let mut sim = Simulator::new(Seed::new(0x303));
    let guest_link = sim.guest_link()?;
    let mut network = allow_all_network_config();
    network.dns_upstream = DNS_UPSTREAM;
    let mut running = AgentdpNetworkSim::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network,
            upstreams: SimulationUpstreams::default().with_udp_handler(DNS_UPSTREAM, handler),
        },
        guest_link.clone(),
    )?;

    let mut guest = SmolTcpGuest::new(guest_link.clone())?;
    let slow = guest.open_udp(gateway_dns_addr())?;
    let fast = guest.open_udp(gateway_dns_addr())?;
    guest.send_udp(&mut running, slow, &slow_query)?;
    guest.send_udp(&mut running, fast, &fast_query)?;
    let fast_actual = guest.recv_udp(&mut running, fast, "fast DNS response")?;
    let slow_actual = guest.recv_udp(&mut running, slow, "slow DNS response")?;
    guest.close_udp(fast);
    guest.close_udp(slow);

    let mut report = stop_network_report(
        "guest_dns_udp_handles_delayed_concurrent_queries",
        sim,
        running,
        &guest_link,
    )?
    .with_guest_transcript(GUEST_DATAGRAMS, encode_datagrams([&fast_actual, &slow_actual]))
    .with_upstream_transcript(UPSTREAM_DATAGRAMS, encode_datagrams(upstream_datagrams.borrow().iter()));

    check_all(
        &mut report,
        vec![
            Box::new(TranscriptEquals::guest(
                GUEST_DATAGRAMS,
                encode_datagrams([&fast_response, &slow_response]),
            )),
            Box::new(TranscriptEquals::upstream(
                UPSTREAM_DATAGRAMS,
                encode_datagrams([&slow_query, &fast_query]),
            )),
            Box::new(NoUnexpectedEgressErrors),
            Box::new(Quiescent),
        ],
    )
}

fn guest_udp_echoes_datagrams<N>(name: &'static str, seed: u64, iterations: usize, payload_len: usize) -> Result<()>
where
    N: NetworkUnderTest,
    N::Running: SteppedNetwork,
{
    let upstream_datagrams = Rc::new(RefCell::new(Vec::new()));
    let handler = udp_response_handler({
        let upstream_datagrams = Rc::clone(&upstream_datagrams);
        move |payload| {
            upstream_datagrams.borrow_mut().push(payload.to_vec());
            Ok(SimUdpResponse::bytes(payload.to_vec()))
        }
    });
    let mut sim = Simulator::new(Seed::new(seed));
    let guest_link = sim.guest_link()?;
    let mut running = N::start(
        ScenarioNetworkConfig {
            seed: sim.seed(),
            network: allow_all_network_config(),
            upstreams: SimulationUpstreams::default().with_udp_handler(udp_upstream_addr(), handler),
        },
        guest_link.clone(),
    )?;

    let mut guest = SmolTcpGuest::new(guest_link.clone())?;
    let udp = guest.open_udp(udp_upstream_addr())?;
    let mut expected = Vec::new();
    let mut actual = Vec::new();
    for index in 0..iterations {
        let payload = udp_payload(index, payload_len);
        guest.send_udp(&mut running, udp, &payload)?;
        let response = guest.recv_udp(&mut running, udp, name)?;
        expected.push(payload);
        actual.push(response);
    }
    guest.close_udp(udp);

    let mut report = stop_network_report(name, sim, running, &guest_link)?
        .with_guest_transcript(GUEST_DATAGRAMS, encode_datagrams(actual.iter()))
        .with_upstream_transcript(UPSTREAM_DATAGRAMS, encode_datagrams(upstream_datagrams.borrow().iter()));

    let expected_transcript = encode_datagrams(expected.iter());
    check_all(
        &mut report,
        vec![
            Box::new(TranscriptEquals::guest(GUEST_DATAGRAMS, expected_transcript.clone())),
            Box::new(TranscriptEquals::upstream(UPSTREAM_DATAGRAMS, expected_transcript)),
            Box::new(NoUnexpectedEgressErrors),
            Box::new(Quiescent),
        ],
    )
}

fn udp_payload(index: usize, len: usize) -> Vec<u8> {
    let mut payload = vec![0; len];
    let marker = index.to_be_bytes();
    let marker_len = marker.len().min(payload.len());
    payload[..marker_len].copy_from_slice(&marker[..marker_len]);
    for (offset, byte) in payload.iter_mut().enumerate().skip(marker_len) {
        *byte = u8::try_from(offset % 251).unwrap_or(0);
    }
    payload
}

fn encode_datagrams<'a>(datagrams: impl IntoIterator<Item = &'a Vec<u8>>) -> Vec<u8> {
    let mut encoded = Vec::new();
    for datagram in datagrams {
        let len = u32::try_from(datagram.len()).unwrap_or(u32::MAX);
        encoded.extend_from_slice(&len.to_be_bytes());
        encoded.extend_from_slice(datagram);
    }
    encoded
}

const fn udp_upstream_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(UPSTREAM_IP), UDP_PORT)
}

const fn gateway_dns_addr() -> SocketAddr {
    SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(
            GATEWAY_IP[0],
            GATEWAY_IP[1],
            GATEWAY_IP[2],
            GATEWAY_IP[3],
        )),
        53,
    )
}
