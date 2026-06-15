use agentdp_network::{
    EgressPolicy, InstanceNetworkConfig, NetworkPolicy, RuntimeSecret, RuntimeSecrets,
    test_support::simulation::SimulationUpstreams,
};
use agentdp_rand::Seed;

use super::case_support::{mediated_network_addresses, mediated_network_mac, repeated_bytes, stop_network_report};
use super::checkers::{
    Checker, ExpectedEgressError, NoSecretLeak, NoUnexpectedEgressErrors, Quiescent, TranscriptContains,
    TranscriptEquals, check_all,
};
use super::fixtures::{DNS_UPSTREAM, UPSTREAM_IP, attribute_named_host_to_upstream, http_upstream_addr};
use super::protocol::http1::{Http1Response, PlainHttpUpstream, http_request};
use super::{NetworkUnderTest, ScenarioNetworkConfig, SmolTcpGuest};
use super::{Result, Simulator, SteppedNetwork};

const GUEST_RESPONSE: &str = "guest.response";
const UPSTREAM_REQUEST: &str = "upstream.request";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpectedOutcome {
    Success,
    EgressFailure,
    Denied,
}

#[derive(Clone, Debug)]
struct SecretBinding {
    placeholder: String,
    value: String,
    allowed_hosts: Vec<String>,
}

pub(super) const fn plain_http1_case(name: &'static str, seed: u64) -> PlainHttp1Case {
    PlainHttp1Case::new(name, seed)
}

#[derive(Clone, Debug)]
pub(super) struct PlainHttp1Case {
    name: &'static str,
    seed: u64,
    request: Option<Vec<u8>>,
    upstream_response_body: &'static [u8],
    secrets: Vec<SecretBinding>,
    expected_outcome: ExpectedOutcome,
    attributed_host: Option<&'static str>,
    restrict_to_authority: Option<&'static str>,
    iterations: usize,
    upstream_close_after_response: bool,
}

enum PlainHttpRun {
    Completed {
        responses: Vec<u8>,
        last_response: Result<Vec<u8>>,
    },
    ConnectFailed(super::Error),
}

impl PlainHttp1Case {
    const fn new(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            seed,
            request: None,
            upstream_response_body: b"",
            secrets: Vec::new(),
            expected_outcome: ExpectedOutcome::Success,
            attributed_host: None,
            restrict_to_authority: None,
            iterations: 1,
            upstream_close_after_response: false,
        }
    }

    pub(super) fn request(mut self, request: impl Into<Vec<u8>>) -> Self {
        self.request = Some(request.into());
        self
    }

    pub(super) const fn upstream_response(mut self, body: &'static [u8]) -> Self {
        self.upstream_response_body = body;
        self
    }

    pub(super) fn secret(
        mut self,
        placeholder: impl Into<String>,
        value: impl Into<String>,
        allowed_hosts: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        self.secrets.push(SecretBinding {
            placeholder: placeholder.into(),
            value: value.into(),
            allowed_hosts: allowed_hosts.into_iter().map(ToOwned::to_owned).collect(),
        });
        self
    }

    pub(super) const fn expect_failure(mut self) -> Self {
        self.expected_outcome = ExpectedOutcome::EgressFailure;
        self
    }

    pub(super) const fn expect_denied(mut self) -> Self {
        self.expected_outcome = ExpectedOutcome::Denied;
        self
    }

    pub(super) const fn attribute_host(mut self, host: &'static str) -> Self {
        self.attributed_host = Some(host);
        self
    }

    pub(super) const fn restrict_to_authority(mut self, authority: &'static str) -> Self {
        self.restrict_to_authority = Some(authority);
        self
    }

    pub(super) const fn iterations(mut self, iterations: usize) -> Self {
        self.iterations = iterations;
        self
    }

    pub(super) const fn upstream_close_after_response(mut self) -> Self {
        self.upstream_close_after_response = true;
        self
    }

    pub(super) fn run<N>(self) -> Result<()>
    where
        N: NetworkUnderTest,
        N::Running: SteppedNetwork,
    {
        let request = self
            .request
            .clone()
            .ok_or_else(|| super::Error::new(format!("{}: missing plaintext HTTP request", self.name)))?;
        let mut sim = Simulator::new(Seed::new(self.seed));
        let guest_link = sim.guest_link()?;
        let upstream = if self.upstream_close_after_response {
            PlainHttpUpstream::with_close_after_response(self.upstream_response_body, true)
        } else {
            PlainHttpUpstream::new(self.upstream_response_body)
        };
        let transcript = upstream.transcript();
        let mut running = N::start(
            ScenarioNetworkConfig {
                seed: sim.seed(),
                network: self.network_config(),
                upstreams: SimulationUpstreams::default()
                    .with_dns_a_endpoint(DNS_UPSTREAM, UPSTREAM_IP)
                    .with_tcp_handler(http_upstream_addr(), upstream.handler()),
            },
            guest_link.clone(),
        )?;

        if let Some(host) = self.attributed_host {
            attribute_named_host_to_upstream(&mut sim, &mut running, &guest_link, host)?;
        }

        let mut guest = SmolTcpGuest::new(guest_link.clone())?;
        let run = self.execute_requests(&mut sim, &mut guest, &mut running, &request)?;
        let (responses, last_response) = match run {
            PlainHttpRun::Completed {
                responses,
                last_response,
            } => (responses, last_response),
            PlainHttpRun::ConnectFailed(error) => {
                let mut report = stop_network_report(self.name, sim, running, &guest_link)?
                    .with_upstream_transcript(UPSTREAM_REQUEST, transcript.borrow().request.clone());
                return match self.expected_outcome {
                    ExpectedOutcome::Success => Err(report.error(format!(
                        "{}: expected plaintext HTTP connect to succeed, got {error}",
                        self.name
                    ))),
                    ExpectedOutcome::EgressFailure | ExpectedOutcome::Denied => self.check_failure_report(&mut report),
                };
            }
        };
        let mut report = stop_network_report(self.name, sim, running, &guest_link)?
            .with_upstream_transcript(UPSTREAM_REQUEST, transcript.borrow().request.clone());

        match (self.expected_outcome, last_response) {
            (ExpectedOutcome::Success, Ok(_response)) => {
                report = report.with_guest_transcript(GUEST_RESPONSE, responses);
            }
            (ExpectedOutcome::Success, Err(error)) => {
                return Err(report.error(format!(
                    "{}: expected plaintext HTTP request to succeed, got {error}",
                    self.name
                )));
            }
            (ExpectedOutcome::EgressFailure | ExpectedOutcome::Denied, Ok(response)) => {
                return Err(report.error(format!(
                    "{}: expected plaintext HTTP request to fail, got response {:02x?}",
                    self.name, response
                )));
            }
            (ExpectedOutcome::EgressFailure | ExpectedOutcome::Denied, Err(_)) => {}
        }

        if self.expected_outcome != ExpectedOutcome::Success {
            return self.check_failure_report(&mut report);
        }

        let mut checkers: Vec<Box<dyn Checker>> = Vec::new();
        let forbidden = self.modeled_forbidden_upstream_bytes();
        if !forbidden.is_empty() {
            checkers.push(Box::new(NoSecretLeak::new(forbidden)));
        }
        checkers.push(Box::new(TranscriptContains::guest(
            GUEST_RESPONSE,
            self.upstream_response_body,
        )));
        checkers.push(Box::new(TranscriptEquals::guest(
            GUEST_RESPONSE,
            repeated_bytes(&http_response_bytes(self.upstream_response_body)?, self.iterations),
        )));
        checkers.push(Box::new(TranscriptEquals::upstream(
            UPSTREAM_REQUEST,
            repeated_bytes(&request, self.iterations),
        )));
        checkers.push(Box::new(NoUnexpectedEgressErrors));
        checkers.push(Box::new(Quiescent));
        check_all(&mut report, checkers)
    }

    fn execute_requests<N>(
        &self,
        sim: &mut Simulator,
        guest: &mut SmolTcpGuest,
        running: &mut N,
        request: &[u8],
    ) -> Result<PlainHttpRun>
    where
        N: SteppedNetwork,
    {
        let mut responses = Vec::new();
        let mut last_response = Ok(Vec::new());
        for _index in 0..self.iterations {
            let tcp = match guest.connect(running, http_upstream_addr()) {
                Ok(tcp) => tcp,
                Err(error) => return Ok(PlainHttpRun::ConnectFailed(error)),
            };
            let response = http_request(sim, guest, running, tcp, request);
            if let Ok(response) = &response {
                responses.extend_from_slice(response);
            }
            if self.upstream_close_after_response && response.is_ok() {
                guest.wait_closed(running, tcp, self.name)?;
            } else if response.is_ok() {
                guest.close(running, tcp)?;
            }
            last_response = response;
            if self.expected_outcome != ExpectedOutcome::Success || last_response.is_err() {
                break;
            }
        }
        guest.drain(running, 16)?;
        Ok(PlainHttpRun::Completed {
            responses,
            last_response,
        })
    }

    fn check_failure_report(&self, report: &mut super::ScenarioReport) -> Result<()> {
        let mut checkers: Vec<Box<dyn Checker>> = Vec::new();
        let forbidden = self.modeled_forbidden_upstream_bytes();
        if !forbidden.is_empty() {
            checkers.push(Box::new(NoSecretLeak::new(forbidden)));
        }
        checkers.push(Box::new(TranscriptEquals::upstream(UPSTREAM_REQUEST, Vec::new())));
        match self.expected_outcome {
            ExpectedOutcome::Success => unreachable!("success reports are checked by the success path"),
            ExpectedOutcome::EgressFailure => checkers.push(Box::new(ExpectedEgressError)),
            ExpectedOutcome::Denied => checkers.push(Box::new(NoUnexpectedEgressErrors)),
        }
        checkers.push(Box::new(Quiescent));
        check_all(report, checkers)
    }

    fn network_config(&self) -> InstanceNetworkConfig {
        let mut runtime = RuntimeSecrets::new();
        for secret in &self.secrets {
            runtime.insert(RuntimeSecret::new(
                secret.placeholder.clone(),
                secret.value.clone(),
                secret.allowed_hosts.clone(),
            ));
        }
        let egress = self.restrict_to_authority.map_or_else(EgressPolicy::allow_all, |host| {
            EgressPolicy::allow_all().with_allowed_authority(host)
        });
        InstanceNetworkConfig {
            policy: NetworkPolicy::new(egress.clone()).with_secrets(runtime),
            dns_upstream: DNS_UPSTREAM,
            ..InstanceNetworkConfig::new(mediated_network_addresses(), mediated_network_mac(), egress)
        }
    }

    fn modeled_forbidden_upstream_bytes(&self) -> Vec<Vec<u8>> {
        let mut forbidden = Vec::new();
        for secret in &self.secrets {
            forbidden.push(secret.placeholder.as_bytes().to_vec());
            forbidden.push(secret.value.as_bytes().to_vec());
        }
        forbidden.sort();
        forbidden.dedup();
        forbidden
    }
}

fn http_response_bytes(body: &'static [u8]) -> Result<Vec<u8>> {
    Http1Response::ok(body)
        .to_bytes()
        .map_err(|error| super::Error::from_display("build expected HTTP response", error))
}
