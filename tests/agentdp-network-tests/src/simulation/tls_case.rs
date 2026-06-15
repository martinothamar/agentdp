use agentdp_network::test_support::simulation::SimulationUpstreams;
use agentdp_network::{RuntimeSecret, RuntimeSecrets};
use agentdp_rand::Seed;

use super::case_support::stop_tcp_report;
use super::checkers::{
    Checker, ExpectedEgressError, HttpResponseBodyEquals, LinkTraceContains, NoSecretLeak, NoUnexpectedEgressErrors,
    ProgressComplete, Quiescent, TranscriptContains, TranscriptEquals, check_all,
};
use super::fixtures::{
    DNS_UPSTREAM, UPSTREAM_IP, attribute_named_host_to_upstream, tls_network_config_for, upstream_addr,
};
use super::protocol::http1::{
    Http1Response, HttpsRequestRoundtrip, HttpsRequestsRoundtrip, TlsHttpUpstream,
    https_request_read_after_upload_with_hook, https_request_with_hook, https_requests_with_hook,
};
use super::protocol::http1_model::{HttpSecretSubstitution, model_intercepted_http_request};
use super::protocol::tls::fixed_mediated_ca;
use super::protocol::websocket::{
    TlsWssUpstream, WssRejectedUpgradeRoundtrip, WssRoundtrip, wss_rejected_upgrade_roundtrip, wss_roundtrip_with_hook,
    wss_upgrade_request,
};
use super::{
    LinkDirection, LinkFault, LinkTraceEventKind, NetworkUnderTest, ScenarioNetworkConfig, ScenarioReport, SmolTcpGuest,
};
use super::{Result, Simulator, SteppedNetwork};

const GUEST_RESPONSE: &str = "guest.response";
const UPSTREAM_REQUEST: &str = "upstream.request";
const UPSTREAM_WEBSOCKET_MESSAGE: &str = "upstream.websocket_message";
const CONSTRAINED_UPSTREAM_WRITE_LIMIT: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TlsMode {
    Intercept,
    Bypass,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpectedOutcome {
    Success,
    Failure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpstreamResponseTiming {
    CompleteRequest,
    Headers,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpstreamConnection {
    KeepOpen,
    CloseAfterResponse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HttpsResponseRead {
    Concurrent,
    AfterUpload,
}

#[derive(Clone, Debug)]
struct SecretBinding {
    placeholder: String,
    value: String,
    allowed_hosts: Vec<String>,
}

pub(super) const fn https_http1_case(name: &'static str, seed: u64) -> HttpsHttp1Case {
    HttpsHttp1Case::new(name, seed)
}

pub(super) const fn wss_http1_case(name: &'static str, seed: u64) -> WssHttp1Case {
    WssHttp1Case::new(name, seed)
}

pub(super) const fn https_http1_sequence_case(name: &'static str, seed: u64) -> HttpsHttp1SequenceCase {
    HttpsHttp1SequenceCase::new(name, seed)
}

#[derive(Clone, Debug)]
pub(super) struct HttpsHttp1Case {
    name: &'static str,
    seed: u64,
    authority: String,
    tls_mode: TlsMode,
    trust_upstream: bool,
    secrets: Vec<SecretBinding>,
    request: Option<Vec<u8>>,
    upstream_response: Http1Response,
    upstream_response_timing: UpstreamResponseTiming,
    upstream_connection: UpstreamConnection,
    upstream_write_limit: Option<usize>,
    request_plaintext_write_limit: usize,
    response_read: HttpsResponseRead,
    expected_outcome: ExpectedOutcome,
    forbidden_upstream: Vec<Vec<u8>>,
    link_delays: Vec<(LinkDirection, std::time::Duration)>,
    post_connect_link_actions: Vec<LinkAction>,
    post_tls_link_actions: Vec<LinkAction>,
    link_trace: Vec<LinkTraceContains>,
}

impl HttpsHttp1Case {
    const fn new(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            seed,
            authority: String::new(),
            tls_mode: TlsMode::Intercept,
            trust_upstream: true,
            secrets: Vec::new(),
            request: None,
            upstream_response: Http1Response::ok(b""),
            upstream_response_timing: UpstreamResponseTiming::CompleteRequest,
            upstream_connection: UpstreamConnection::KeepOpen,
            upstream_write_limit: None,
            request_plaintext_write_limit: super::protocol::http1::TLS_PLAINTEXT_WRITE_BYTES_PER_STEP,
            response_read: HttpsResponseRead::Concurrent,
            expected_outcome: ExpectedOutcome::Success,
            forbidden_upstream: Vec::new(),
            link_delays: Vec::new(),
            post_connect_link_actions: Vec::new(),
            post_tls_link_actions: Vec::new(),
            link_trace: Vec::new(),
        }
    }

    pub(super) fn authority(mut self, authority: impl Into<String>) -> Self {
        self.authority = authority.into();
        self
    }

    pub(super) const fn bypass_tls(mut self) -> Self {
        self.tls_mode = TlsMode::Bypass;
        self
    }

    pub(super) const fn untrusted_upstream(mut self) -> Self {
        self.trust_upstream = false;
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

    pub(super) fn request(mut self, request: impl Into<Vec<u8>>) -> Self {
        self.request = Some(request.into());
        self
    }

    pub(super) fn upstream_response(mut self, body: &'static [u8]) -> Self {
        self.upstream_response = Http1Response::ok(body);
        self
    }

    pub(super) fn upstream_chunked_response(mut self, body: &'static [u8], chunk_size: usize) -> Self {
        self.upstream_response = Http1Response::chunked(body, chunk_size);
        self
    }

    pub(super) fn upstream_http_response(mut self, response: Http1Response) -> Self {
        self.upstream_response = response;
        self
    }

    pub(super) fn upstream_segmented_response(mut self, body: &'static [u8], segment_size: usize) -> Self {
        self.upstream_response = Http1Response::ok(body).segmented(segment_size);
        self
    }

    pub(super) fn large_upload_with_early_response(
        mut self,
        request: impl Into<Vec<u8>>,
        response_body: &'static [u8],
    ) -> Self {
        let request = request.into();
        self.request_plaintext_write_limit = request.len();
        self.request = Some(request);
        self.upstream_response = Http1Response::ok(response_body);
        self.upstream_response_timing = UpstreamResponseTiming::Headers;
        self.upstream_write_limit = Some(CONSTRAINED_UPSTREAM_WRITE_LIMIT);
        self.response_read = HttpsResponseRead::AfterUpload;
        self
    }

    pub(super) const fn upstream_close_after_response(mut self) -> Self {
        self.upstream_connection = UpstreamConnection::CloseAfterResponse;
        self
    }

    pub(super) fn upstream_segmented_chunked_response(
        mut self,
        body: &'static [u8],
        chunk_size: usize,
        segment_size: usize,
    ) -> Self {
        self.upstream_response = Http1Response::chunked(body, chunk_size).segmented(segment_size);
        self
    }

    pub(super) const fn expect_failure(mut self) -> Self {
        self.expected_outcome = ExpectedOutcome::Failure;
        self
    }

    pub(super) fn forbid_upstream(mut self, bytes: impl Into<Vec<u8>>) -> Self {
        self.forbidden_upstream.push(bytes.into());
        self
    }

    pub(super) fn delay_path(mut self, direction: LinkDirection, delay: std::time::Duration) -> Self {
        self.link_delays.push((direction, delay));
        self
    }

    pub(super) fn post_connect_link_action(mut self, action: LinkAction) -> Self {
        self.post_connect_link_actions.push(action);
        self
    }

    pub(super) fn post_tls_link_action(mut self, action: LinkAction) -> Self {
        self.post_tls_link_actions.push(action);
        self
    }

    pub(super) fn expect_packet_event(
        mut self,
        direction: LinkDirection,
        event: LinkTraceEventKind,
        at: std::time::Duration,
        sequence: u64,
    ) -> Self {
        self.link_trace
            .push(LinkTraceContains::new(direction, event).at(at).sequence(sequence));
        self
    }

    pub(super) fn run<N>(self) -> Result<()>
    where
        N: NetworkUnderTest,
        N::Running: SteppedNetwork,
    {
        let request = self.required_request()?;
        let mut sim = Simulator::new(Seed::new(self.seed));
        let guest_link = sim.guest_link()?;
        for (direction, delay) in &self.link_delays {
            guest_link.set_path_delay(*direction, *delay);
        }
        let mediated_ca = fixed_mediated_ca();
        let upstream = self.upstream()?;
        let transcript = upstream.transcript();
        let mut running = N::start(
            ScenarioNetworkConfig {
                seed: sim.seed(),
                network: self.network_config(&mediated_ca, &upstream.root_ca_pem),
                upstreams: self.upstreams(&upstream),
            },
            guest_link.clone(),
        )?;
        attribute_named_host_to_upstream(&mut sim, &mut running, &guest_link, self.authority.as_str())?;

        let mut guest = SmolTcpGuest::new(guest_link.clone())?;
        let tcp = guest.connect(&mut running, upstream_addr())?;
        apply_link_actions(&guest_link, &self.post_connect_link_actions);
        let roundtrip = HttpsRequestRoundtrip {
            host: self.authority.as_str(),
            ca_pem: self.guest_trust_root(&mediated_ca, &upstream.root_ca_pem),
            request: &request,
            plaintext_write_limit: self.request_plaintext_write_limit,
        };
        let apply_tls_actions = || apply_link_actions(&guest_link, &self.post_tls_link_actions);
        let response = match self.response_read {
            HttpsResponseRead::AfterUpload => https_request_read_after_upload_with_hook(
                &mut sim,
                &mut guest,
                &mut running,
                &guest_link,
                tcp,
                roundtrip,
                apply_tls_actions,
            ),
            HttpsResponseRead::Concurrent => https_request_with_hook(
                &mut sim,
                &mut guest,
                &mut running,
                &guest_link,
                tcp,
                roundtrip,
                apply_tls_actions,
            ),
        };
        let expected_request = self.modeled_upstream_request(&request);
        if self.expected_outcome == ExpectedOutcome::Success && response.is_ok() {
            let expected_request_len = expected_request.len();
            sim.drive_guest_until(
                &mut guest,
                &mut running,
                "HTTPS upstream request flush",
                super::protocol::http1::HTTP_DRIVE_BUDGET,
                |_guest, _running| Ok(transcript.borrow().request.len() >= expected_request_len),
            )?;
        }
        let response_succeeded = response.is_ok();
        let expected_response_len = response_wire_len(&self.upstream_response)?;
        let upstream_request = transcript.borrow().request.clone();
        let mut report = stop_tcp_report(
            self.name,
            sim,
            guest,
            running,
            &guest_link,
            tcp,
            self.expected_outcome == ExpectedOutcome::Success && response_succeeded,
        )?
        .with_upstream_transcript(UPSTREAM_REQUEST, upstream_request.clone());

        report = self.record_response_outcome(
            report,
            response,
            &upstream_request,
            &expected_request,
            expected_response_len,
        )?;

        self.check_http_report(&mut report, &request)
    }

    fn record_response_outcome(
        &self,
        report: ScenarioReport,
        response: Result<Vec<u8>>,
        upstream_request: &[u8],
        expected_request: &[u8],
        expected_response_len: usize,
    ) -> Result<ScenarioReport> {
        match (self.expected_outcome, response) {
            (ExpectedOutcome::Success, Ok(response)) => {
                let response_len = response.len();
                let close_complete = report.quiescence.is_quiescent();
                Ok(report
                    .with_progress(
                        "https_request_upstream",
                        upstream_request.len(),
                        expected_request.len(),
                        upstream_request == expected_request,
                    )
                    .with_progress(
                        "https_response_guest",
                        response_len,
                        expected_response_len,
                        response_len == expected_response_len,
                    )
                    .with_progress("tcp_close", usize::from(close_complete), 1, close_complete)
                    .with_guest_transcript(GUEST_RESPONSE, response))
            }
            (ExpectedOutcome::Success, Err(error)) => Err(report.error(format!(
                "{}: expected HTTPS request to succeed, got {:?}",
                self.name,
                error.to_string()
            ))),
            (ExpectedOutcome::Failure, Ok(response)) => Err(report.error(format!(
                "{}: expected HTTPS request to fail, got response {:02x?}",
                self.name, response
            ))),
            (ExpectedOutcome::Failure, Err(_)) => Ok(report),
        }
    }

    fn upstream(&self) -> Result<TlsHttpUpstream> {
        match (self.upstream_response_timing, self.upstream_connection) {
            (UpstreamResponseTiming::Headers, UpstreamConnection::KeepOpen) => {
                TlsHttpUpstream::with_response_after_headers(self.upstream_response.clone())
            }
            (UpstreamResponseTiming::Headers, UpstreamConnection::CloseAfterResponse) => {
                TlsHttpUpstream::with_response_after_headers_and_close(self.upstream_response.clone())
            }
            (UpstreamResponseTiming::CompleteRequest, UpstreamConnection::CloseAfterResponse) => {
                TlsHttpUpstream::with_response_and_close(self.upstream_response.clone())
            }
            (UpstreamResponseTiming::CompleteRequest, UpstreamConnection::KeepOpen) => {
                TlsHttpUpstream::with_response(self.upstream_response.clone())
            }
        }
    }

    fn upstreams(&self, upstream: &TlsHttpUpstream) -> SimulationUpstreams {
        let upstreams = SimulationUpstreams::default().with_dns_a_endpoint(DNS_UPSTREAM, UPSTREAM_IP);
        if let Some(write_limit) = self.upstream_write_limit {
            upstreams.with_limited_tcp_handler(upstream_addr(), upstream.handler(), write_limit)
        } else {
            upstreams.with_tcp_handler(upstream_addr(), upstream.handler())
        }
    }

    fn required_request(&self) -> Result<Vec<u8>> {
        self.request
            .clone()
            .ok_or_else(|| super::Error::new(format!("{}: missing HTTPS request", self.name)))
    }

    fn check_http_report(&self, report: &mut ScenarioReport, request: &[u8]) -> Result<()> {
        let mut checkers = self.default_checkers();
        if self.expected_outcome == ExpectedOutcome::Success {
            checkers.push(Box::new(ProgressComplete));
            checkers.push(Box::new(HttpResponseBodyEquals::guest_for_request(
                GUEST_RESPONSE,
                request,
                self.upstream_response.body(),
            )));
            checkers.push(Box::new(TranscriptEquals::upstream(
                UPSTREAM_REQUEST,
                self.modeled_upstream_request(request),
            )));
        }
        checkers.extend(
            self.link_trace
                .iter()
                .cloned()
                .map(|expectation| Box::new(expectation) as Box<dyn Checker>),
        );
        check_all(report, checkers)
    }
}

#[derive(Clone, Debug)]
pub(super) struct HttpsHttp1SequenceCase {
    name: &'static str,
    seed: u64,
    authority: String,
    requests: Vec<Vec<u8>>,
    upstream_responses: Vec<Http1Response>,
    link_delays: Vec<(LinkDirection, std::time::Duration)>,
    post_connect_link_actions: Vec<LinkAction>,
    post_tls_link_actions: Vec<LinkAction>,
}

impl HttpsHttp1SequenceCase {
    const fn new(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            seed,
            authority: String::new(),
            requests: Vec::new(),
            upstream_responses: Vec::new(),
            link_delays: Vec::new(),
            post_connect_link_actions: Vec::new(),
            post_tls_link_actions: Vec::new(),
        }
    }

    pub(super) fn authority(mut self, authority: impl Into<String>) -> Self {
        self.authority = authority.into();
        self
    }

    pub(super) fn exchange(mut self, request: impl Into<Vec<u8>>, response_body: &'static [u8]) -> Self {
        self.requests.push(request.into());
        self.upstream_responses.push(Http1Response::ok(response_body));
        self
    }

    pub(super) fn keep_alive_exchange(mut self, request: impl Into<Vec<u8>>, response_body: &'static [u8]) -> Self {
        self.requests.push(request.into());
        self.upstream_responses
            .push(Http1Response::ok(response_body).keep_alive());
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
        let mediated_ca = fixed_mediated_ca();
        let upstream = TlsHttpUpstream::with_responses(self.upstream_responses.clone())?;
        let transcript = upstream.transcript();
        let mut running = N::start(
            ScenarioNetworkConfig {
                seed: sim.seed(),
                network: self.network_config(&mediated_ca, &upstream.root_ca_pem),
                upstreams: SimulationUpstreams::default()
                    .with_dns_a_endpoint(DNS_UPSTREAM, UPSTREAM_IP)
                    .with_tcp_handler(upstream_addr(), upstream.handler()),
            },
            guest_link.clone(),
        )?;
        attribute_named_host_to_upstream(&mut sim, &mut running, &guest_link, self.authority.as_str())?;

        let mut guest = SmolTcpGuest::new(guest_link.clone())?;
        let tcp = guest.connect(&mut running, upstream_addr())?;
        apply_link_actions(&guest_link, &self.post_connect_link_actions);
        let responses = https_requests_with_hook(
            &mut sim,
            &mut guest,
            &mut running,
            tcp,
            HttpsRequestsRoundtrip {
                host: self.authority.as_str(),
                ca_pem: &mediated_ca.cert_pem,
                requests: &self.requests,
            },
            || apply_link_actions(&guest_link, &self.post_tls_link_actions),
        )?;
        let upstream_request = transcript.borrow().request.clone();
        let guest_response = responses.concat();
        let mut report = stop_tcp_report(self.name, sim, guest, running, &guest_link, tcp, true)?
            .with_upstream_transcript(UPSTREAM_REQUEST, upstream_request.clone())
            .with_guest_transcript(GUEST_RESPONSE, guest_response.clone());

        let mut expected_upstream = Vec::new();
        for request in &self.requests {
            expected_upstream.extend_from_slice(request);
        }
        let mut expected_guest = Vec::new();
        for response in &self.upstream_responses {
            expected_guest.extend(
                response
                    .to_bytes()
                    .map_err(|error| super::Error::from_display("build expected HTTP response", error))?,
            );
        }
        let close_complete = report.quiescence.is_quiescent();
        report = report
            .with_progress(
                "https_sequence_requests_upstream",
                upstream_request.len(),
                expected_upstream.len(),
                upstream_request == expected_upstream,
            )
            .with_progress(
                "https_sequence_responses_guest",
                guest_response.len(),
                expected_guest.len(),
                guest_response == expected_guest,
            )
            .with_progress("tcp_close", usize::from(close_complete), 1, close_complete);

        let mut checkers = self.default_checkers();
        checkers.push(Box::new(ProgressComplete));
        checkers.push(Box::new(TranscriptEquals::guest(GUEST_RESPONSE, expected_guest)));
        checkers.push(Box::new(TranscriptEquals::upstream(
            UPSTREAM_REQUEST,
            expected_upstream,
        )));
        check_all(&mut report, checkers)
    }
}

#[derive(Clone, Debug)]
pub(super) struct WssHttp1Case {
    name: &'static str,
    seed: u64,
    authority: String,
    secrets: Vec<SecretBinding>,
    message: Vec<u8>,
    upstream_response_message: Vec<u8>,
    fragmented: bool,
    close_after_response: bool,
    reject_upgrade_followup: Option<Vec<u8>>,
    link_delays: Vec<(LinkDirection, std::time::Duration)>,
    post_connect_link_actions: Vec<LinkAction>,
    post_upgrade_link_actions: Vec<LinkAction>,
}

impl WssHttp1Case {
    const fn new(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            seed,
            authority: String::new(),
            secrets: Vec::new(),
            message: Vec::new(),
            upstream_response_message: Vec::new(),
            fragmented: false,
            close_after_response: false,
            reject_upgrade_followup: None,
            link_delays: Vec::new(),
            post_connect_link_actions: Vec::new(),
            post_upgrade_link_actions: Vec::new(),
        }
    }

    pub(super) fn authority(mut self, authority: impl Into<String>) -> Self {
        self.authority = authority.into();
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

    pub(super) fn message(mut self, message: impl Into<Vec<u8>>) -> Self {
        self.message = message.into();
        self
    }

    pub(super) fn upstream_response(mut self, message: impl Into<Vec<u8>>) -> Self {
        self.upstream_response_message = message.into();
        self
    }

    pub(super) const fn fragmented(mut self) -> Self {
        self.fragmented = true;
        self
    }

    pub(super) const fn close_after_response(mut self) -> Self {
        self.close_after_response = true;
        self
    }

    pub(super) fn reject_upgrade_then_http(mut self, followup_request: impl Into<Vec<u8>>) -> Self {
        self.reject_upgrade_followup = Some(followup_request.into());
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
        let mediated_ca = fixed_mediated_ca();
        let upstream = if self.reject_upgrade_followup.is_some() {
            TlsWssUpstream::reject_upgrade(self.upstream_response_message.clone())?
        } else {
            TlsWssUpstream::new(self.upstream_response_message.clone(), self.close_after_response)?
        };
        let transcript = upstream.transcript();
        let mut running = N::start(
            ScenarioNetworkConfig {
                seed: sim.seed(),
                network: self.network_config(&mediated_ca, &upstream.root_ca_pem),
                upstreams: SimulationUpstreams::default()
                    .with_dns_a_endpoint(DNS_UPSTREAM, UPSTREAM_IP)
                    .with_tcp_handler(upstream_addr(), upstream.handler()),
            },
            guest_link.clone(),
        )?;
        attribute_named_host_to_upstream(&mut sim, &mut running, &guest_link, self.authority.as_str())?;

        let mut guest = SmolTcpGuest::new(guest_link.clone())?;
        let tcp = guest.connect(&mut running, upstream_addr())?;
        apply_link_actions(&guest_link, &self.post_connect_link_actions);
        let response = if let Some(followup_request) = &self.reject_upgrade_followup {
            wss_rejected_upgrade_roundtrip(
                &mut sim,
                &mut guest,
                &mut running,
                tcp,
                WssRejectedUpgradeRoundtrip {
                    host: self.authority.as_str(),
                    ca_pem: &mediated_ca.cert_pem,
                    followup_request,
                    followup_response_body: &self.upstream_response_message,
                },
            )?
        } else {
            wss_roundtrip_with_hook(
                &mut sim,
                &mut guest,
                &mut running,
                tcp,
                WssRoundtrip {
                    host: self.authority.as_str(),
                    ca_pem: &mediated_ca.cert_pem,
                    message: &self.message,
                    fragmented: self.fragmented,
                },
                || apply_link_actions(&guest_link, &self.post_upgrade_link_actions),
            )?
        };
        let guest_response = response;
        let transcript = transcript.borrow();
        let progress_response = guest_response.clone();
        let mut report = stop_tcp_report(self.name, sim, guest, running, &guest_link, tcp, true)?
            .with_guest_transcript(GUEST_RESPONSE, guest_response)
            .with_upstream_transcript(UPSTREAM_REQUEST, transcript.request.clone())
            .with_upstream_transcript(
                UPSTREAM_WEBSOCKET_MESSAGE,
                transcript.websocket_message.clone().unwrap_or_default(),
            );
        let close_complete = report.quiescence.is_quiescent();
        report = self.with_progress(report, &transcript, &progress_response, close_complete);
        drop(transcript);

        self.check_report(&mut report)
    }

    fn with_progress(
        &self,
        report: ScenarioReport,
        transcript: &super::protocol::http1::TlsTranscript,
        guest_response: &[u8],
        close_complete: bool,
    ) -> ScenarioReport {
        let expected_upgrade_request =
            self.modeled_upstream_request(wss_upgrade_request(self.authority.as_str()).as_bytes());
        let expected_websocket_message = if self.reject_upgrade_followup.is_some() {
            Vec::new()
        } else {
            self.message.clone()
        };
        let expected_response_body = self.upstream_response_message.as_slice();
        let response_body_complete = if self.reject_upgrade_followup.is_some() {
            guest_response
                .windows(expected_response_body.len())
                .any(|window| window == expected_response_body)
        } else {
            guest_response == expected_response_body
        };
        let response_progress_observed = if response_body_complete {
            expected_response_body.len()
        } else {
            guest_response.len()
        };

        report
            .with_progress(
                "wss_upgrade_upstream",
                transcript.request.len().min(expected_upgrade_request.len()),
                expected_upgrade_request.len(),
                transcript.request.starts_with(&expected_upgrade_request),
            )
            .with_progress(
                "wss_message_upstream",
                transcript.websocket_message.as_ref().map_or(0, Vec::len),
                expected_websocket_message.len(),
                transcript.websocket_message.as_deref().unwrap_or_default() == expected_websocket_message.as_slice(),
            )
            .with_progress(
                "wss_response_body_guest",
                response_progress_observed,
                expected_response_body.len(),
                response_body_complete,
            )
            .with_progress("tcp_close", usize::from(close_complete), 1, close_complete)
    }

    fn check_report(&self, report: &mut ScenarioReport) -> Result<()> {
        let mut checkers = self.default_checkers();
        checkers.push(Box::new(ProgressComplete));
        if let Some(followup_request) = &self.reject_upgrade_followup {
            let mut expected_request =
                self.modeled_upstream_request(wss_upgrade_request(self.authority.as_str()).as_bytes());
            expected_request.extend_from_slice(followup_request);
            checkers.push(Box::new(TranscriptContains::guest(
                GUEST_RESPONSE,
                self.upstream_response_message.clone(),
            )));
            checkers.push(Box::new(TranscriptEquals::upstream(UPSTREAM_REQUEST, expected_request)));
            checkers.push(Box::new(TranscriptEquals::upstream(
                UPSTREAM_WEBSOCKET_MESSAGE,
                Vec::new(),
            )));
        } else {
            checkers.push(Box::new(TranscriptEquals::guest(
                GUEST_RESPONSE,
                self.upstream_response_message.clone(),
            )));
            checkers.push(Box::new(TranscriptEquals::upstream(
                UPSTREAM_REQUEST,
                self.modeled_upstream_request(wss_upgrade_request(self.authority.as_str()).as_bytes()),
            )));
            checkers.push(Box::new(TranscriptEquals::upstream(
                UPSTREAM_WEBSOCKET_MESSAGE,
                self.message.clone(),
            )));
        }
        check_all(report, checkers)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LinkAction {
    DropNextFrame(LinkDirection),
    DuplicateNextFrame(LinkDirection),
    ReorderNextFrames(LinkDirection),
    BlockNextRead,
    BlockNextWrite,
}

pub(super) fn apply_link_actions(guest_link: &super::GuestLink, actions: &[LinkAction]) {
    for action in actions {
        match action {
            LinkAction::DropNextFrame(LinkDirection::GuestToNetwork) => {
                guest_link.push_fault(LinkFault::DropNextGuestFrame);
            }
            LinkAction::DropNextFrame(LinkDirection::NetworkToGuest) => {
                guest_link.push_fault(LinkFault::DropNextNetworkFrame);
            }
            LinkAction::DuplicateNextFrame(direction) => {
                guest_link.duplicate_next(*direction);
            }
            LinkAction::ReorderNextFrames(direction) => {
                guest_link.reorder_next(*direction);
            }
            LinkAction::BlockNextRead => {
                guest_link.push_fault(LinkFault::BlockNextRead);
            }
            LinkAction::BlockNextWrite => {
                guest_link.push_fault(LinkFault::BlockNextWrite);
            }
        }
    }
}

trait TlsCaseModel {
    fn authority(&self) -> &str;
    fn tls_mode(&self) -> TlsMode;
    fn trust_upstream(&self) -> bool;
    fn secrets(&self) -> &[SecretBinding];
    fn expected_outcome(&self) -> ExpectedOutcome;
    fn forbidden_upstream(&self) -> &[Vec<u8>];

    fn network_config(
        &self,
        mediated_ca: &agentdp_crypto::CertificateAuthorityPem,
        upstream_root_ca_pem: &str,
    ) -> agentdp_network::InstanceNetworkConfig {
        let roots = if self.trust_upstream() {
            vec![upstream_root_ca_pem.to_owned()]
        } else {
            Vec::new()
        };
        let bypass_hosts = if self.tls_mode() == TlsMode::Bypass {
            vec![self.authority()]
        } else {
            Vec::new()
        };
        tls_network_config_for(
            mediated_ca,
            &roots,
            &[self.authority()],
            self.runtime_secrets(),
            &bypass_hosts,
        )
    }

    fn runtime_secrets(&self) -> RuntimeSecrets {
        let mut runtime = RuntimeSecrets::new();
        for secret in self.secrets() {
            runtime.insert(RuntimeSecret::new(
                secret.placeholder.clone(),
                secret.value.clone(),
                secret.allowed_hosts.clone(),
            ));
        }
        runtime
    }

    fn guest_trust_root<'a>(
        &'a self,
        mediated_ca: &'a agentdp_crypto::CertificateAuthorityPem,
        upstream_root_ca_pem: &'a str,
    ) -> &'a str {
        if self.tls_mode() == TlsMode::Bypass {
            upstream_root_ca_pem
        } else {
            &mediated_ca.cert_pem
        }
    }

    fn default_checkers(&self) -> Vec<Box<dyn Checker>> {
        let mut checkers: Vec<Box<dyn Checker>> = Vec::new();
        let forbidden = self.modeled_forbidden_upstream_bytes();
        if !forbidden.is_empty() {
            checkers.push(Box::new(NoSecretLeak::new(forbidden)));
        }
        match self.expected_outcome() {
            ExpectedOutcome::Success => checkers.push(Box::new(NoUnexpectedEgressErrors)),
            ExpectedOutcome::Failure => checkers.push(Box::new(ExpectedEgressError)),
        }
        checkers.push(Box::new(Quiescent));
        checkers
    }

    fn modeled_forbidden_upstream_bytes(&self) -> Vec<Vec<u8>> {
        let mut forbidden = self.forbidden_upstream().to_vec();
        let allow_authorized_values =
            self.expected_outcome() == ExpectedOutcome::Success && self.tls_mode() == TlsMode::Intercept;
        for secret in self.secrets() {
            if !allow_authorized_values || !secret.allows_authority(self.authority()) {
                forbidden.push(secret.value.as_bytes().to_vec());
            }
        }
        forbidden.sort();
        forbidden.dedup();
        forbidden
    }

    fn modeled_upstream_request(&self, request: &[u8]) -> Vec<u8> {
        if self.tls_mode() == TlsMode::Bypass {
            return request.to_vec();
        }
        model_intercepted_http_request(
            request,
            self.secrets()
                .iter()
                .filter(|secret| secret.allows_authority(self.authority()))
                .map(|secret| HttpSecretSubstitution {
                    placeholder: secret.placeholder.as_bytes(),
                    value: secret.value.as_bytes(),
                }),
        )
    }
}

impl TlsCaseModel for HttpsHttp1Case {
    fn authority(&self) -> &str {
        self.authority.as_str()
    }

    fn tls_mode(&self) -> TlsMode {
        self.tls_mode
    }

    fn trust_upstream(&self) -> bool {
        self.trust_upstream
    }

    fn secrets(&self) -> &[SecretBinding] {
        &self.secrets
    }

    fn expected_outcome(&self) -> ExpectedOutcome {
        self.expected_outcome
    }

    fn forbidden_upstream(&self) -> &[Vec<u8>] {
        &self.forbidden_upstream
    }
}

impl TlsCaseModel for WssHttp1Case {
    fn authority(&self) -> &str {
        self.authority.as_str()
    }

    fn tls_mode(&self) -> TlsMode {
        TlsMode::Intercept
    }

    fn trust_upstream(&self) -> bool {
        true
    }

    fn secrets(&self) -> &[SecretBinding] {
        &self.secrets
    }

    fn expected_outcome(&self) -> ExpectedOutcome {
        ExpectedOutcome::Success
    }

    fn forbidden_upstream(&self) -> &[Vec<u8>] {
        &[]
    }
}

impl TlsCaseModel for HttpsHttp1SequenceCase {
    fn authority(&self) -> &str {
        self.authority.as_str()
    }

    fn tls_mode(&self) -> TlsMode {
        TlsMode::Intercept
    }

    fn trust_upstream(&self) -> bool {
        true
    }

    fn secrets(&self) -> &[SecretBinding] {
        &[]
    }

    fn expected_outcome(&self) -> ExpectedOutcome {
        ExpectedOutcome::Success
    }

    fn forbidden_upstream(&self) -> &[Vec<u8>] {
        &[]
    }
}

impl SecretBinding {
    fn allows_authority(&self, authority: &str) -> bool {
        self.allowed_hosts.iter().any(|host| host == authority)
    }
}

fn response_wire_len(response: &Http1Response) -> Result<usize> {
    response
        .to_bytes()
        .map(|bytes| bytes.len())
        .map_err(|error| super::Error::from_display("build expected HTTP response", error))
}
