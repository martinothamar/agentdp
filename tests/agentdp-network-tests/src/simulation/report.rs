use std::fmt::Write as _;

use agentdp_network::InstanceNetworkStatus;
use agentdp_network::{NetworkEvent, NetworkEventEnvelope};
use agentdp_rand::Seed;

use super::simulator::SimulatorTraceEvent;
use super::{Error, LinkTraceEvent, QuiescenceReport};

const TRANSCRIPT_EDGE_BYTES: usize = 96;
const TRACE_LIMIT: usize = 80;

#[derive(Debug, Clone)]
pub struct ScenarioReport {
    pub name: &'static str,
    pub seed: Seed,
    pub final_status: InstanceNetworkStatus,
    pub quiescence: QuiescenceReport,
    pub simulator_trace: Vec<SimulatorTraceEvent>,
    pub link_trace: Vec<LinkTraceEvent>,
    pub network_events: Vec<NetworkEventEnvelope>,
    pub transcripts: Vec<Transcript>,
    pub progress: Vec<ProgressObservation>,
    pub checker_results: Vec<CheckerResult>,
}

impl ScenarioReport {
    #[must_use]
    pub const fn new(
        name: &'static str,
        seed: Seed,
        final_status: InstanceNetworkStatus,
        quiescence: QuiescenceReport,
        simulator_trace: Vec<SimulatorTraceEvent>,
        link_trace: Vec<LinkTraceEvent>,
        network_events: Vec<NetworkEventEnvelope>,
    ) -> Self {
        Self {
            name,
            seed,
            final_status,
            quiescence,
            simulator_trace,
            link_trace,
            network_events,
            transcripts: Vec::new(),
            progress: Vec::new(),
            checker_results: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_guest_transcript(mut self, name: &'static str, bytes: impl Into<Vec<u8>>) -> Self {
        self.transcripts
            .push(Transcript::new(TranscriptRole::Guest, name, bytes));
        self
    }

    #[must_use]
    pub fn with_upstream_transcript(mut self, name: &'static str, bytes: impl Into<Vec<u8>>) -> Self {
        self.transcripts
            .push(Transcript::new(TranscriptRole::Upstream, name, bytes));
        self
    }

    #[must_use]
    pub fn transcript(&self, role: TranscriptRole, name: &str) -> Option<&[u8]> {
        self.transcripts
            .iter()
            .find(|transcript| transcript.role == role && transcript.name == name)
            .map(|transcript| transcript.bytes.as_slice())
    }

    pub fn record_checker(&mut self, result: CheckerResult) {
        self.checker_results.push(result);
    }

    #[must_use]
    pub fn with_progress(
        mut self,
        phase: &'static str,
        observed_bytes: usize,
        expected_bytes: usize,
        complete: bool,
    ) -> Self {
        self.progress.push(ProgressObservation {
            phase,
            observed_bytes,
            expected_bytes,
            complete,
        });
        self
    }

    #[must_use]
    pub fn error(&self, message: impl Into<String>) -> Error {
        Error::new(self.failure_snapshot(message.into(), None))
    }

    #[must_use]
    pub fn failure_snapshot(&self, failure: impl AsRef<str>, generated_input: Option<&str>) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "scenario: {}", self.name);
        let _ = writeln!(output, "seed: {}", self.seed);
        let _ = writeln!(output, "failure: {}", failure.as_ref());
        if let Some(generated_input) = generated_input {
            let _ = writeln!(output, "generated_input:");
            for line in generated_input.lines() {
                let _ = writeln!(output, "  {line}");
            }
        }
        let _ = writeln!(output, "final_status: {:?}", self.final_status);
        let _ = writeln!(output, "quiescence: {:?}", self.quiescence);
        render_checkers(&mut output, &self.checker_results);
        render_progress(&mut output, &self.progress);
        render_transcripts(&mut output, &self.transcripts);
        render_limited_debug(&mut output, "simulator_trace", &self.simulator_trace);
        render_limited_debug(&mut output, "link_trace", &self.link_trace);
        render_network_events(&mut output, &self.network_events);
        output
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressObservation {
    pub phase: &'static str,
    pub observed_bytes: usize,
    pub expected_bytes: usize,
    pub complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptRole {
    Guest,
    Upstream,
}

#[derive(Debug, Clone)]
pub struct Transcript {
    pub role: TranscriptRole,
    pub name: &'static str,
    pub bytes: Vec<u8>,
}

impl Transcript {
    #[must_use]
    pub fn new(role: TranscriptRole, name: &'static str, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            role,
            name,
            bytes: bytes.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckerResult {
    pub name: &'static str,
    pub passed: bool,
    pub failure: Option<String>,
}

fn render_checkers(output: &mut String, checkers: &[CheckerResult]) {
    let _ = writeln!(output, "checkers:");
    if checkers.is_empty() {
        let _ = writeln!(output, "  - none recorded");
        return;
    }
    for checker in checkers {
        if checker.passed {
            let _ = writeln!(output, "  - {}: passed", checker.name);
        } else {
            let _ = writeln!(
                output,
                "  - {}: failed: {}",
                checker.name,
                checker.failure.as_deref().unwrap_or("unknown failure")
            );
        }
    }
}

fn render_progress(output: &mut String, progress: &[ProgressObservation]) {
    let _ = writeln!(output, "progress:");
    if progress.is_empty() {
        let _ = writeln!(output, "  - none recorded");
        return;
    }
    for observation in progress {
        let _ = writeln!(
            output,
            "  - phase={} complete={} observed_bytes={} expected_bytes={}",
            observation.phase, observation.complete, observation.observed_bytes, observation.expected_bytes
        );
    }
}

fn render_transcripts(output: &mut String, transcripts: &[Transcript]) {
    let _ = writeln!(output, "transcripts:");
    if transcripts.is_empty() {
        let _ = writeln!(output, "  - none recorded");
        return;
    }
    for transcript in transcripts {
        let _ = writeln!(
            output,
            "  - role={:?} name={} len={} preview={}",
            transcript.role,
            transcript.name,
            transcript.bytes.len(),
            bytes_preview(&transcript.bytes)
        );
    }
}

fn render_limited_debug<T: std::fmt::Debug>(output: &mut String, name: &str, values: &[T]) {
    let _ = writeln!(output, "{name}:");
    if values.is_empty() {
        let _ = writeln!(output, "  - none");
        return;
    }
    let omitted = values.len().saturating_sub(TRACE_LIMIT);
    for value in values.iter().skip(omitted) {
        let _ = writeln!(output, "  - {value:?}");
    }
    if omitted > 0 {
        let _ = writeln!(output, "  omitted_before: {omitted}");
    }
}

fn render_network_events(output: &mut String, events: &[NetworkEventEnvelope]) {
    let _ = writeln!(output, "network_events:");
    if events.is_empty() {
        let _ = writeln!(output, "  - none");
        return;
    }
    let omitted = events.len().saturating_sub(TRACE_LIMIT);
    for event in events.iter().skip(omitted) {
        let _ = writeln!(
            output,
            "  - #{} t={} dropped={} kind={} event={:?}",
            event.sequence,
            event.unix_millis,
            event.dropped_events_before,
            network_event_kind(&event.event),
            event.event
        );
    }
    if omitted > 0 {
        let _ = writeln!(output, "  omitted_before: {omitted}");
    }
}

const fn network_event_kind(event: &NetworkEvent) -> &'static str {
    match event {
        NetworkEvent::Lifecycle(_) => "lifecycle",
        NetworkEvent::Telemetry(_) => "telemetry",
        NetworkEvent::Transport(_) => "transport",
        NetworkEvent::Egress(_) => "egress",
        NetworkEvent::Dns(_) => "dns",
        NetworkEvent::HostPort(_) => "host_port",
        NetworkEvent::Reactor(_) => "reactor",
    }
}

fn bytes_preview(bytes: &[u8]) -> String {
    if bytes.len() <= TRANSCRIPT_EDGE_BYTES * 2 {
        return hex_bytes(bytes);
    }
    format!(
        "{} ... {}",
        hex_bytes(&bytes[..TRANSCRIPT_EDGE_BYTES]),
        hex_bytes(&bytes[bytes.len() - TRANSCRIPT_EDGE_BYTES..])
    )
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len().saturating_mul(3));
    for (index, byte) in bytes.iter().enumerate() {
        if index > 0 {
            output.push(' ');
        }
        let _ = write!(output, "{byte:02x}");
    }
    output
}

impl CheckerResult {
    #[must_use]
    pub const fn passed(name: &'static str) -> Self {
        Self {
            name,
            passed: true,
            failure: None,
        }
    }

    #[must_use]
    pub const fn failed(name: &'static str, failure: String) -> Self {
        Self {
            name,
            passed: false,
            failure: Some(failure),
        }
    }
}
