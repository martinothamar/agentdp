use std::time::Duration;

use agentdp_network::test_support::simulation::SimulationUpstreams;
use agentdp_rand::Seed;

use super::case_support::{allow_all_network_config, stop_network_report};
use super::checkers::{Checker, LinkTraceContains, Quiescent, TelemetryEquals, check_all};
use super::fixtures::{arp_request, icmp_echo_request, verify_arp_reply, verify_icmp_echo_reply};
use super::{DriveBudget, Result, Simulator, SteppedNetwork};
use super::{LinkDirection, LinkTraceEventKind, NetworkUnderTest, RawFrameGuest, ScenarioNetworkConfig};

pub(super) const fn gateway_frame_case(name: &'static str, seed: u64) -> GatewayFrameCase {
    GatewayFrameCase::new(name, seed)
}

#[derive(Clone, Debug)]
pub(super) struct GatewayFrameCase {
    name: &'static str,
    seed: u64,
    steps: Vec<GatewayStep>,
    link_delays: Vec<(LinkDirection, Duration)>,
    link_trace: Vec<LinkTraceContains>,
}

impl GatewayFrameCase {
    const fn new(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            seed,
            steps: Vec::new(),
            link_delays: Vec::new(),
            link_trace: Vec::new(),
        }
    }

    pub(super) fn delay_path(mut self, direction: LinkDirection, delay: Duration) -> Self {
        self.link_delays.push((direction, delay));
        self
    }

    pub(super) fn expect_packet_event(
        mut self,
        direction: LinkDirection,
        event: LinkTraceEventKind,
        at: Duration,
        sequence: u64,
    ) -> Self {
        self.link_trace
            .push(LinkTraceContains::new(direction, event).at(at).sequence(sequence));
        self
    }

    pub(super) fn arp_request(mut self) -> Self {
        self.steps.push(GatewayStep::ArpRequest);
        self
    }

    pub(super) fn icmp_echo_request(mut self, identifier: u16, sequence: u16, payload: &'static [u8]) -> Self {
        self.steps.push(GatewayStep::IcmpEchoRequest {
            identifier,
            sequence,
            payload,
        });
        self
    }

    pub(super) fn run<N>(self) -> Result<()>
    where
        N: NetworkUnderTest,
        N::Running: SteppedNetwork,
    {
        let mut sim = Simulator::new(Seed::new(self.seed));
        let guest_link = sim.guest_link()?;
        for (direction, delay) in &self.link_delays {
            guest_link.set_path_delay(*direction, *delay);
        }
        let mut running = N::start(
            ScenarioNetworkConfig {
                seed: sim.seed(),
                network: allow_all_network_config(),
                upstreams: SimulationUpstreams::default(),
            },
            guest_link.clone(),
        )?;
        let guest = RawFrameGuest::new(guest_link);

        for step in &self.steps {
            match *step {
                GatewayStep::ArpRequest => {
                    guest.send_frame(arp_request())?;
                    let reply = guest.recv_frame(&mut sim, &mut running, self.name, DriveBudget::default())?;
                    verify_arp_reply(&reply)?;
                }
                GatewayStep::IcmpEchoRequest {
                    identifier,
                    sequence,
                    payload,
                } => {
                    guest.send_frame(icmp_echo_request(identifier, sequence, payload)?)?;
                    let reply = guest.recv_frame(&mut sim, &mut running, self.name, DriveBudget::default())?;
                    verify_icmp_echo_reply(&reply, identifier, sequence, payload)?;
                }
            }
        }

        let expected_frames = self.steps.len() as u64;
        let mut report = stop_network_report(self.name, sim, running, guest.link())?;
        let mut checkers: Vec<Box<dyn Checker>> = vec![
            Box::new(
                TelemetryEquals::new()
                    .guest_frames_received(expected_frames)
                    .host_frames_sent(expected_frames),
            ),
            Box::new(Quiescent),
        ];
        checkers.extend(
            self.link_trace
                .into_iter()
                .map(|expectation| Box::new(expectation) as Box<dyn Checker>),
        );
        check_all(&mut report, checkers)
    }
}

#[derive(Clone, Copy, Debug)]
enum GatewayStep {
    ArpRequest,
    IcmpEchoRequest {
        identifier: u16,
        sequence: u16,
        payload: &'static [u8],
    },
}
