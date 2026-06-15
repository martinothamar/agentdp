use std::cell::RefCell;
use std::rc::Rc;

use agentdp_network::test_support::simulation::{SimTcpResponse, SimulationUpstreams};
use agentdp_rand::Seed;

use super::case_support::{allow_all_network_config, repeated_bytes, stop_network_report, stop_tcp_report};
use super::checkers::{NoUnexpectedEgressErrors, Quiescent, TranscriptEquals, check_all};
use super::fixtures::upstream_addr;
use super::protocol::tcp::{tcp_handler, tcp_response_handler};
use super::{NetworkUnderTest, ScenarioNetworkConfig, SmolTcpGuest};
use super::{Result, Simulator, SteppedNetwork};

const GUEST_RESPONSE: &str = "guest.response";
const UPSTREAM_REQUEST: &str = "upstream.request";

pub(super) const fn tcp_stream_case(name: &'static str, seed: u64) -> TcpStreamCase {
    TcpStreamCase::new(name, seed)
}

#[derive(Clone, Debug)]
pub(super) struct TcpStreamCase {
    name: &'static str,
    seed: u64,
    request: &'static [u8],
    response: &'static [u8],
    upstream_eof: bool,
    iterations: usize,
    reuse_connection: bool,
}

impl TcpStreamCase {
    const fn new(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            seed,
            request: b"",
            response: b"",
            upstream_eof: false,
            iterations: 1,
            reuse_connection: false,
        }
    }

    pub(super) const fn request(mut self, request: &'static [u8]) -> Self {
        self.request = request;
        self
    }

    pub(super) const fn response(mut self, response: &'static [u8]) -> Self {
        self.response = response;
        self
    }

    pub(super) const fn upstream_eof(mut self) -> Self {
        self.upstream_eof = true;
        self
    }

    pub(super) const fn iterations(mut self, iterations: usize) -> Self {
        self.iterations = iterations;
        self
    }

    pub(super) const fn reuse_connection(mut self) -> Self {
        self.reuse_connection = true;
        self
    }

    pub(super) fn run<N>(self) -> Result<()>
    where
        N: NetworkUnderTest,
        N::Running: SteppedNetwork,
    {
        let name = self.name;
        let seed = self.seed;
        let request = self.request;
        let response = self.response;
        let upstream_eof = self.upstream_eof;
        let iterations = self.iterations;
        let reuse_connection = self.reuse_connection;
        let mut sim = Simulator::new(Seed::new(seed));
        let guest_link = sim.guest_link()?;
        let transcript = Rc::new(RefCell::new(Vec::new()));
        let handler = if upstream_eof {
            tcp_response_handler({
                let transcript = Rc::clone(&transcript);
                move |bytes| {
                    transcript.borrow_mut().extend_from_slice(bytes);
                    Ok(SimTcpResponse {
                        bytes: response.to_vec(),
                        followup_bytes: Vec::new(),
                        close: true,
                        reset: false,
                    })
                }
            })
        } else {
            tcp_handler({
                let transcript = Rc::clone(&transcript);
                move |bytes, output| {
                    transcript.borrow_mut().extend_from_slice(bytes);
                    output.extend_from_slice(response);
                    Ok(())
                }
            })
        };
        let mut running = N::start(
            ScenarioNetworkConfig {
                seed: sim.seed(),
                network: allow_all_network_config(),
                upstreams: SimulationUpstreams::default().with_tcp_handler(upstream_addr(), handler),
            },
            guest_link.clone(),
        )?;

        let mut guest = SmolTcpGuest::new(guest_link.clone())?;
        let guest_response = if reuse_connection {
            let tcp = guest.connect(&mut running, upstream_addr())?;
            let response =
                repeated_roundtrip_on_connection(&mut guest, &mut running, tcp, name, request, response, iterations)?;
            let mut report = stop_tcp_report(name, sim, guest, running, &guest_link, tcp, true)?
                .with_guest_transcript(GUEST_RESPONSE, response)
                .with_upstream_transcript(UPSTREAM_REQUEST, transcript.borrow().clone());
            return check_tcp_report(&mut report, request, self.response, iterations);
        } else {
            let mut output = Vec::new();
            for _index in 0..iterations {
                let tcp = guest.connect(&mut running, upstream_addr())?;
                guest.write_all(&mut running, tcp, request)?;
                output.extend(guest.read_until(&mut running, tcp, name, |bytes| bytes == response)?);
                if upstream_eof {
                    guest.wait_closed(&mut running, tcp, name)?;
                } else {
                    guest.close(&mut running, tcp)?;
                }
            }
            output
        };
        guest.drain(&mut running, 16)?;
        let mut report = stop_network_report(name, sim, running, &guest_link)?
            .with_guest_transcript(GUEST_RESPONSE, guest_response)
            .with_upstream_transcript(UPSTREAM_REQUEST, transcript.borrow().clone());

        check_tcp_report(&mut report, request, response, iterations)
    }
}

fn repeated_roundtrip_on_connection<N>(
    guest: &mut SmolTcpGuest,
    running: &mut N,
    tcp: super::TcpHandle,
    name: &str,
    request: &[u8],
    response: &[u8],
    iterations: usize,
) -> Result<Vec<u8>>
where
    N: SteppedNetwork,
{
    let mut output = Vec::new();
    for _index in 0..iterations {
        guest.write_all(running, tcp, request)?;
        output.extend(guest.read_until(running, tcp, name, |bytes| bytes == response)?);
    }
    Ok(output)
}

fn check_tcp_report(
    report: &mut super::ScenarioReport,
    request: &[u8],
    response: &[u8],
    iterations: usize,
) -> Result<()> {
    check_all(
        report,
        vec![
            Box::new(TranscriptEquals::guest(
                GUEST_RESPONSE,
                repeated_bytes(response, iterations),
            )),
            Box::new(TranscriptEquals::upstream(
                UPSTREAM_REQUEST,
                repeated_bytes(request, iterations),
            )),
            Box::new(NoUnexpectedEgressErrors),
            Box::new(Quiescent),
        ],
    )
}
