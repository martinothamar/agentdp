use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};
use std::{env, fs};

use agentdp_crypto::TlsServerConfig;
use agentdp_network::test_support::simulation::{SimTcpHandler, SimulationUpstreams};
use agentdp_network::{RuntimeSecret, RuntimeSecrets};
use agentdp_rand::{DeterministicRng, Seed};
use agentdp_test_support::allocation::{AllocationReport, AllocationSnapshot};

use super::checkers::{
    ExpectedEgressError, NoSecretLeak, NoUnexpectedEgressErrors, ProgressComplete, Quiescent, TranscriptEquals,
    check_all,
};
use super::fixtures::{
    BLOCKED_HOST, DNS_UPSTREAM, HOST, PLACEHOLDER, SECRET_VALUE, UNKNOWN_PLACEHOLDER, UPSTREAM_IP,
    attribute_named_host_to_upstream, tls_network_config_for,
};
use super::protocol::http1::{
    HttpsClientFlow, HttpsClientFlowSpec, RawHttpConnection, RawHttpResponse, RawHttpResponseFraming,
    TlsRawHttpUpstream, TlsTranscript, write_chunked_body,
};
use super::protocol::http1_model::{HttpSecretSubstitution, model_intercepted_http_request};
use super::protocol::tls::{TestTlsIdentity, fixed_mediated_ca};
use super::protocol::websocket::{WssClientFlow, WssClientFlowSpec};
use super::tls_case::{LinkAction, apply_link_actions};
use super::{
    AgentdpNetworkSim, DriveBudget, DriveGuestProgress, Error, LinkDirection, NetworkUnderTest, QuiescenceReport,
    Result, RunningNetwork, ScenarioNetworkConfig, ScenarioReport, Simulator, SmolTcpGuest, SteppedNetwork, TcpHandle,
};

const DEFAULT_ROOT_SEED: Seed = Seed::new(0x7b15_4a31_0000_0001);
const DEFAULT_RANDOMIZED_OPERATIONS: usize = 16;
const DEFAULT_HOT_RANDOMIZED_OPERATIONS: usize = 256;
const DEFAULT_HOT_CONCURRENT_RANDOMIZED_OPERATIONS: usize = 512;
const MAX_RANDOMIZED_OPERATIONS: usize = 100_000;
const RANDOMIZED_OPERATIONS_ENV: &str = "AGENTDP_NETWORK_RANDOMIZED_OPERATIONS";
const RANDOMIZED_ROOT_SEED_ENV: &str = "AGENTDP_NETWORK_RANDOMIZED_ROOT_SEED";
const RANDOMIZED_SECONDS_ENV: &str = "AGENTDP_NETWORK_RANDOMIZED_SECONDS";
const RANDOMIZED_LONG_SECONDS_ENV: &str = "AGENTDP_NETWORK_RANDOMIZED_LONG_SECONDS";
const RANDOMIZED_REPLAY_BATCH_INDEX_ENV: &str = "AGENTDP_NETWORK_RANDOMIZED_REPLAY_BATCH_INDEX";
const RANDOMIZED_REPLAY_PREFIX_ENV: &str = "AGENTDP_NETWORK_RANDOMIZED_REPLAY_PREFIX";
const RANDOMIZED_TRACE_CASES_ENV: &str = "AGENTDP_NETWORK_RANDOMIZED_TRACE_CASES";
const HOT_RANDOMIZED_ALLOCATIONS_ENV: &str = "AGENTDP_NETWORK_HOT_RANDOMIZED_ALLOCATIONS";
const RANDOMIZED_FAILURE_DIR: &str = "agentdp-network-randomized-failures";
const RUN_NAME: &str = "randomized_https_wss_workloads_preserve_public_behavior";
const HOT_RUN_NAME: &str = "randomized_hot_dataplane_https_requests_preserve_public_behavior";
const HOT_CONCURRENT_RUN_NAME: &str = "randomized_hot_concurrent_dataplane_https_requests_preserve_public_behavior";
const WORKLOAD_BATCH_SCENARIO: &str = "randomized_workload_batch";
const UPSTREAM_TRANSCRIPT: &str = "upstream.request";
const GUEST_TRANSCRIPT: &str = "guest.response";
const HISTORY_LIMIT: usize = 16;
const UPSTREAM_BASE_PORT: u16 = 10_000;

static EMPTY_RESPONSE: &[u8] = b"";
static SMALL_RESPONSE: &[u8] = b"randomized-small-response\n";
static MEDIUM_RESPONSE: &[u8; 16 * 1024] = &[b'M'; 16 * 1024];
static LARGE_RESPONSE: &[u8; 96 * 1024] = &[b'L'; 96 * 1024];
static MEDIATION_BOUNDARY_RESPONSE: &[u8; 1024 * 1024] = &[b'B'; 1024 * 1024];
static ABOVE_MEDIATION_BOUNDARY_RESPONSE: &[u8; 1024 * 1024 + 8192] = &[b'A'; 1024 * 1024 + 8192];
static SSE_RESPONSE: &[u8] =
    b"event: message\ndata: {\"delta\":\"one\"}\n\nevent: message\ndata: {\"delta\":\"two\"}\n\nevent: done\ndata: {}\n\n";
static WSS_RESPONSE: &[u8] = b"{\"type\":\"response.completed\",\"id\":\"randomized\"}";

#[test]
fn randomized_https_wss_workloads_preserve_public_behavior() -> Result<()> {
    run_randomized(RunControls::from_env(RunMode::isolated())?)
}

#[test]
fn randomized_hot_dataplane_https_requests_preserve_public_behavior() -> Result<()> {
    run_randomized(RunControls::from_env(RunMode::hot_sequential())?)
}

#[test]
fn randomized_hot_concurrent_dataplane_https_requests_preserve_public_behavior() -> Result<()> {
    run_randomized(RunControls::from_env(RunMode::hot_concurrent())?)
}

#[test]
#[ignore = "long randomized dataplane bug-hunting run; set AGENTDP_NETWORK_RANDOMIZED_LONG_SECONDS to override duration"]
fn randomized_https_wss_workloads_long_run() -> Result<()> {
    let controls = RunControls::from_env(RunMode::isolated())?
        .with_default_duration(parse_seconds_env(RANDOMIZED_LONG_SECONDS_ENV)?.unwrap_or(Duration::from_mins(5)));
    run_randomized(controls)
}

fn run_randomized(controls: RunControls) -> Result<()> {
    if controls.mode.execution == ExecutionMode::Hot {
        return run_hot_batches(controls);
    }
    run_isolated_batches(controls)
}

fn run_isolated_batches(controls: RunControls) -> Result<()> {
    if let Some(index) = controls.replay_batch_index {
        let batch = WorkloadBatch::for_index(&controls, index);
        trace_batch(&controls, &batch);
        return run_isolated_batch(&batch);
    }
    let started = Instant::now();
    let mut generator = BatchGenerator::new(&controls);
    let mut executed = 0_usize;
    while executed < controls.max_operations {
        if controls
            .duration
            .is_some_and(|duration| started.elapsed() >= duration && executed > 0)
        {
            break;
        }
        let batch = generator.next_batch();
        trace_batch(&controls, &batch);
        if let Err(error) = run_isolated_batch(&batch) {
            let snapshot = batch.failure_snapshot(&error);
            let path = write_failure_snapshot("randomized_workload", batch.seed, &snapshot)?;
            return Err(Error::new(format!(
                "randomized network scenario failed; failure_snapshot={}; replay_env: {RANDOMIZED_ROOT_SEED_ENV}={} {RANDOMIZED_REPLAY_BATCH_INDEX_ENV}={}",
                path.display(),
                batch.root_seed,
                batch.index
            )));
        }
        executed = executed.saturating_add(batch.limit_cost);
    }
    Ok(())
}

fn run_hot_batches(controls: RunControls) -> Result<()> {
    if let Some(index) = controls.replay_batch_index
        && !controls.replay_prefix
    {
        let batch = WorkloadBatch::for_index(&controls, index);
        trace_batch(&controls, &batch);
        return run_isolated_batch(&batch);
    }

    let started = Instant::now();
    let allocations = AllocationAccounting::from_env();
    let construction = allocations.begin();
    let network_operations = controls.hot_network_operation_capacity();
    let mut runner = BatchRunner::start_hot(&controls, network_operations)?;
    let construction = AllocationAccounting::finish(construction);
    let mut generator = BatchGenerator::new(&controls);
    let mut executed = 0_usize;
    let warmup_operations = if controls.replay_batch_index.is_none() && allocations.enabled {
        controls.max_operations.min(16)
    } else {
        0
    };
    let warmup = allocations.begin();
    while controls.replay_batch_index.is_some() || executed < controls.max_operations {
        if controls.replay_batch_index.is_none()
            && controls
                .duration
                .is_some_and(|duration| started.elapsed() >= duration && executed > 0)
        {
            break;
        }
        let batch = generator.next_batch();
        trace_batch(&controls, &batch);
        if let Err(error) = runner.run_hot_batch(&batch) {
            let snapshot = runner.failure_snapshot(&batch, &error);
            let path = write_failure_snapshot("randomized_hot_dataplane", batch.seed, &snapshot)?;
            return Err(Error::new(format!(
                "hot randomized network scenario failed; failure_snapshot={}; replay_env: {RANDOMIZED_ROOT_SEED_ENV}={} {RANDOMIZED_REPLAY_BATCH_INDEX_ENV}={}",
                path.display(),
                batch.root_seed,
                batch.index
            )));
        }
        executed = executed.saturating_add(batch.limit_cost);
        if executed >= warmup_operations && runner.allocation_report.is_none() && allocations.enabled {
            let warmup = AllocationAccounting::finish(warmup);
            runner.allocation_report = Some(HotAllocationReport {
                construction,
                warmup,
                steady_state: AllocationReport::default(),
                warmup_cases: warmup_operations,
                steady_state_cases: 0,
            });
            runner.steady_state_allocations = allocations.begin();
        }
        if controls.replay_batch_index.is_some_and(|index| batch.index >= index) {
            break;
        }
    }
    if allocations.enabled
        && let Some(mut report) = runner.allocation_report.take()
    {
        report.steady_state = AllocationAccounting::finish(runner.steady_state_allocations);
        report.steady_state_cases = executed.saturating_sub(report.warmup_cases);
        eprintln!("{report}");
    }
    runner.stop()?;
    Ok(())
}

fn run_isolated_batch(batch: &WorkloadBatch) -> Result<()> {
    let mut runner = BatchRunner::start_isolated(batch)?;
    runner.run_hot_batch(batch)?;
    runner.stop()
}

#[derive(Debug, Clone, Copy)]
struct RunControls {
    root_seed: Seed,
    max_operations: usize,
    duration: Option<Duration>,
    replay_batch_index: Option<usize>,
    replay_prefix: bool,
    trace_cases: TraceCases,
    mode: RunMode,
}

impl RunControls {
    fn from_env(mode: RunMode) -> Result<Self> {
        let root_seed = parse_seed_env(RANDOMIZED_ROOT_SEED_ENV)?.unwrap_or(DEFAULT_ROOT_SEED);
        let duration = parse_seconds_env(RANDOMIZED_SECONDS_ENV)?;
        let max_operations = parse_usize_env(RANDOMIZED_OPERATIONS_ENV)?.map_or_else(
            || {
                if duration.is_some() {
                    mode.long_default_operations
                } else {
                    mode.default_operations
                }
            },
            |operations| operations.clamp(1, MAX_RANDOMIZED_OPERATIONS),
        );
        let replay_batch_index = parse_usize_env(RANDOMIZED_REPLAY_BATCH_INDEX_ENV)?;
        let replay_prefix = parse_bool_env(RANDOMIZED_REPLAY_PREFIX_ENV)?;
        let trace_cases = TraceCases::from_env();
        Ok(Self {
            root_seed,
            max_operations,
            duration,
            replay_batch_index,
            replay_prefix,
            trace_cases,
            mode,
        })
    }

    const fn with_default_duration(mut self, duration: Duration) -> Self {
        if self.duration.is_none() {
            self.duration = Some(duration);
            self.max_operations = self.mode.long_default_operations;
        }
        self
    }

    const fn hot_network_operation_capacity(&self) -> usize {
        if let Some(index) = self.replay_batch_index
            && self.replay_prefix
        {
            return index.saturating_add(1).saturating_mul(self.mode.batch_width.max_size());
        }
        self.max_operations
    }
}

#[derive(Debug, Clone, Copy)]
struct RunMode {
    run_name: &'static str,
    execution: ExecutionMode,
    batch_width: BatchWidth,
    operation_space: OperationSpace,
    default_operations: usize,
    long_default_operations: usize,
}

impl RunMode {
    const fn isolated() -> Self {
        Self {
            run_name: RUN_NAME,
            execution: ExecutionMode::Isolated,
            batch_width: BatchWidth::One,
            operation_space: OperationSpace::Full,
            default_operations: DEFAULT_RANDOMIZED_OPERATIONS,
            long_default_operations: MAX_RANDOMIZED_OPERATIONS,
        }
    }

    const fn hot_sequential() -> Self {
        Self {
            run_name: HOT_RUN_NAME,
            execution: ExecutionMode::Hot,
            batch_width: BatchWidth::One,
            operation_space: OperationSpace::Hot,
            default_operations: DEFAULT_HOT_RANDOMIZED_OPERATIONS,
            long_default_operations: MAX_RANDOMIZED_OPERATIONS,
        }
    }

    const fn hot_concurrent() -> Self {
        Self {
            run_name: HOT_CONCURRENT_RUN_NAME,
            execution: ExecutionMode::Hot,
            batch_width: BatchWidth::Range { min: 2, max: 8 },
            operation_space: OperationSpace::Hot,
            default_operations: DEFAULT_HOT_CONCURRENT_RANDOMIZED_OPERATIONS,
            long_default_operations: MAX_RANDOMIZED_OPERATIONS,
        }
    }

    const fn limit_cost(self, operations: usize) -> usize {
        match self.operation_space {
            OperationSpace::Full => 1,
            OperationSpace::Hot => operations,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionMode {
    Isolated,
    Hot,
}

#[derive(Debug, Clone, Copy)]
enum OperationSpace {
    Full,
    Hot,
}

#[derive(Debug, Clone, Copy)]
enum BatchWidth {
    One,
    Range { min: usize, max: usize },
}

impl BatchWidth {
    const fn min_size(self) -> usize {
        match self {
            Self::One => 1,
            Self::Range { min, .. } => min,
        }
    }

    const fn max_size(self) -> usize {
        match self {
            Self::One => 1,
            Self::Range { max, .. } => max,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AllocationAccounting {
    enabled: bool,
}

impl AllocationAccounting {
    fn from_env() -> Self {
        Self {
            enabled: env::var_os(HOT_RANDOMIZED_ALLOCATIONS_ENV).is_some(),
        }
    }

    fn begin(self) -> Option<AllocationSnapshot> {
        self.enabled.then(|| super::ALLOCATOR.begin_report())
    }

    fn finish(snapshot: Option<AllocationSnapshot>) -> AllocationReport {
        snapshot.map_or_else(AllocationReport::default, |snapshot| {
            super::ALLOCATOR.report_since(snapshot)
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct HotAllocationReport {
    construction: AllocationReport,
    warmup: AllocationReport,
    steady_state: AllocationReport,
    warmup_cases: usize,
    steady_state_cases: usize,
}

impl std::fmt::Display for HotAllocationReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(formatter, "hot_randomized_allocation_report:")?;
        writeln!(
            formatter,
            "  construction: {}",
            format_allocation_report(self.construction)
        )?;
        writeln!(
            formatter,
            "  warmup: cases={} {}",
            self.warmup_cases,
            format_allocation_report(self.warmup)
        )?;
        writeln!(
            formatter,
            "  steady_state: cases={} {}",
            self.steady_state_cases,
            format_allocation_report(self.steady_state)
        )
    }
}

fn format_allocation_report(report: AllocationReport) -> String {
    format!(
        "allocation_calls={} allocated_bytes={} deallocation_calls={} deallocated_bytes={}",
        report.allocation_calls(),
        report.allocated_bytes(),
        report.deallocation_calls(),
        report.deallocated_bytes()
    )
}

fn trace_batch(controls: &RunControls, batch: &WorkloadBatch) {
    match controls.trace_cases {
        TraceCases::Off => {}
        TraceCases::Brief => {
            eprintln!(
                "randomized_batch run={} index={} seed={} operations={} fault={}",
                batch.run_name,
                batch.index,
                batch.seed,
                batch.operations.len(),
                batch.fault.name()
            );
        }
        TraceCases::Full => {
            eprintln!("randomized_batch:\n{}", batch.replay_record());
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TraceCases {
    Off,
    Brief,
    Full,
}

impl TraceCases {
    fn from_env() -> Self {
        match env::var(RANDOMIZED_TRACE_CASES_ENV).ok().as_deref() {
            Some("full") => Self::Full,
            Some(_) => Self::Brief,
            None => Self::Off,
        }
    }
}

struct BatchGenerator {
    root_seed: Seed,
    rng: DeterministicRng,
    operation_rng: DeterministicRng,
    mode: RunMode,
    next_batch_index: usize,
    next_operation_index: usize,
}

impl BatchGenerator {
    fn new(controls: &RunControls) -> Self {
        Self {
            root_seed: controls.root_seed,
            rng: DeterministicRng::from_seed(controls.root_seed.derive("workload-batches")),
            operation_rng: DeterministicRng::from_seed(controls.root_seed.derive("workload-operations")),
            mode: controls.mode,
            next_batch_index: 0,
            next_operation_index: 0,
        }
    }

    fn next_batch(&mut self) -> WorkloadBatch {
        let max_size = self.mode.batch_width.max_size();
        let min_size = self.mode.batch_width.min_size();
        let width = max_size.saturating_sub(min_size).saturating_add(1);
        let offset =
            usize::try_from(self.rng.below(u64::try_from(width).unwrap_or(u64::MAX)).unwrap_or(0)).unwrap_or(0);
        let size = min_size.saturating_add(offset);
        let mut operations = Vec::with_capacity(size);
        for _operation in 0..size {
            operations.extend(self.next_operations());
        }
        let batch = WorkloadBatch {
            root_seed: self.root_seed,
            seed: self
                .root_seed
                .derive(&format!("workload-batch-{}", self.next_batch_index)),
            index: self.next_batch_index,
            run_name: self.mode.run_name,
            fault: LinkFaultSchedule::select(&mut self.rng),
            secrets: WorkloadSecrets::for_operations(&operations),
            limit_cost: self.mode.limit_cost(operations.len()),
            operations,
        };
        self.next_batch_index = self.next_batch_index.saturating_add(1);
        batch
    }

    fn next_operations(&mut self) -> Vec<WorkloadOperation> {
        let index = self.next_operation_index;
        self.next_operation_index = self.next_operation_index.saturating_add(1);
        match self.mode.operation_space {
            OperationSpace::Hot => vec![WorkloadOperation::hot(self.root_seed, index, &mut self.operation_rng)],
            OperationSpace::Full => GeneratedWorkload::new(self.root_seed, index, &mut self.operation_rng).operations(),
        }
    }
}

#[derive(Debug, Clone)]
struct WorkloadBatch {
    root_seed: Seed,
    seed: Seed,
    index: usize,
    run_name: &'static str,
    fault: LinkFaultSchedule,
    secrets: WorkloadSecrets,
    limit_cost: usize,
    operations: Vec<WorkloadOperation>,
}

impl WorkloadBatch {
    fn for_index(controls: &RunControls, index: usize) -> Self {
        let mut generator = BatchGenerator::new(controls);
        let mut batch = generator.next_batch();
        for _current in 0..index {
            batch = generator.next_batch();
        }
        batch
    }

    fn replay_record(&self) -> String {
        let mut output = format!(
            "run_name: {}\nroot_seed: {}\nbatch_index: {}\nbatch_seed: {}\nreplay_env: {RANDOMIZED_ROOT_SEED_ENV}={} {RANDOMIZED_REPLAY_BATCH_INDEX_ENV}={}\nstateful_replay_env: {RANDOMIZED_ROOT_SEED_ENV}={} {RANDOMIZED_REPLAY_BATCH_INDEX_ENV}={} {RANDOMIZED_REPLAY_PREFIX_ENV}=1\nfault: {}\noperations:",
            self.run_name,
            self.root_seed,
            self.index,
            self.seed,
            self.root_seed,
            self.index,
            self.root_seed,
            self.index,
            self.fault.name()
        );
        for operation in &self.operations {
            let _ = write!(output, "\n  - {}", operation.summary());
        }
        output
    }

    fn failure_snapshot(&self, error: &Error) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "randomized_run: {}", self.run_name);
        let _ = writeln!(output, "generated_input:");
        for line in self.replay_record().lines() {
            let _ = writeln!(output, "  {line}");
        }
        let _ = writeln!(output, "harness:");
        let _ = writeln!(output, "  guest_tcp_buffer_bytes: {}", SmolTcpGuest::tcp_buffer_bytes());
        let _ = writeln!(output, "failure:");
        for line in error.to_string().lines() {
            let _ = writeln!(output, "  {line}");
        }
        output
    }

    fn expects_failure(&self) -> bool {
        self.operations.iter().any(WorkloadOperation::expects_failure)
    }

    fn forbidden_upstream(&self) -> Vec<Vec<u8>> {
        self.operations
            .iter()
            .flat_map(WorkloadOperation::forbidden_upstream)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
struct WorkloadSecrets {
    bindings: Vec<SecretBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SecretBinding {
    placeholder: String,
    value: String,
    allowed_hosts: Vec<String>,
}

impl WorkloadSecrets {
    fn for_operations(operations: &[WorkloadOperation]) -> Self {
        let mut secrets = Self::default();
        for operation in operations {
            for binding in operation.secret_bindings() {
                secrets.push(binding);
            }
        }
        secrets
    }

    fn authorized_for(host: &str) -> SecretBinding {
        SecretBinding {
            placeholder: PLACEHOLDER.to_owned(),
            value: SECRET_VALUE.to_owned(),
            allowed_hosts: vec![host.to_owned()],
        }
    }

    fn push(&mut self, binding: SecretBinding) {
        if !self.bindings.contains(&binding) {
            self.bindings.push(binding);
        }
    }

    fn bindings(&self) -> &[SecretBinding] {
        &self.bindings
    }

    fn single_authorized_for(host: &str) -> Self {
        Self {
            bindings: vec![Self::authorized_for(host)],
        }
    }

    fn runtime(&self) -> RuntimeSecrets {
        let mut secrets = RuntimeSecrets::new();
        for binding in &self.bindings {
            secrets.insert(RuntimeSecret::new(
                binding.placeholder.clone(),
                binding.value.clone(),
                binding.allowed_hosts.clone(),
            ));
        }
        secrets
    }
}

#[derive(Debug, Clone)]
enum WorkloadOperation {
    Https(HttpsOperation),
    HttpsSequence(HttpsSequenceOperation),
    Wss(WssOperation),
}

impl WorkloadOperation {
    fn hot(root_seed: Seed, index: usize, rng: &mut DeterministicRng) -> Self {
        let kind = HotOperationKind::select(rng);
        let request_body_len = select_usize(rng, &[0, 17, 512, 4096, 64 * 1024]);
        let path_id = rng.below(1_000_000).unwrap_or(0);
        let upstream_write_limit = select_upstream_write_limit(rng);
        let seed = root_seed.derive(&format!("hot-operation-{index}"));
        match kind {
            HotOperationKind::WebSocket => {
                Self::hot_wss(seed, index, request_body_len, path_id, upstream_write_limit, rng)
            }
            HotOperationKind::HttpsGet => Self::hot_https(
                seed,
                index,
                HotHttpOperationKind::HttpsGet,
                request_body_len,
                path_id,
                upstream_write_limit,
                rng,
            ),
            HotOperationKind::HttpsPost => Self::hot_https(
                seed,
                index,
                HotHttpOperationKind::HttpsPost,
                request_body_len,
                path_id,
                upstream_write_limit,
                rng,
            ),
            HotOperationKind::SseStream => Self::hot_https(
                seed,
                index,
                HotHttpOperationKind::SseStream,
                request_body_len,
                path_id,
                upstream_write_limit,
                rng,
            ),
        }
    }

    fn hot_wss(
        seed: Seed,
        index: usize,
        request_body_len: usize,
        path_id: u64,
        upstream_write_limit: Option<usize>,
        rng: &mut DeterministicRng,
    ) -> Self {
        let response_fragmented = rng.chance(1, 2);
        Self::Wss(WssOperation {
            seed,
            index,
            message: repeated_body(request_body_len.max(17), b"hot-wss-message "),
            response_message: repeated_body(select_usize(rng, &[17, 512, 4096, 64 * 1024]), b"hot-wss-response "),
            fragmented: rng.chance(1, 2),
            response_fragmented,
            close_after_response: rng.chance(1, 2),
            upstream_write_limit,
            details: vec![
                format!("request_body_len: {request_body_len}"),
                format!("path_id: {path_id}"),
                "hot_operation: websocket".to_owned(),
                format!("response_fragmented: {response_fragmented}"),
                format!(
                    "upstream_write_limit: {}",
                    upstream_write_limit_name(upstream_write_limit)
                ),
            ],
        })
    }

    fn hot_https(
        seed: Seed,
        index: usize,
        kind: HotHttpOperationKind,
        request_body_len: usize,
        path_id: u64,
        upstream_write_limit: Option<usize>,
        rng: &mut DeterministicRng,
    ) -> Self {
        let response_body = ResponseBody::select_hot(rng);
        let response_framing = if kind == HotHttpOperationKind::SseStream {
            ResponseFraming::SegmentedChunked
        } else {
            ResponseFraming::select(rng)
        };
        let request = match kind {
            HotHttpOperationKind::HttpsGet => format!(
                "GET /hot/{path_id}/blob HTTP/1.1\r\nHost: {HOST}\r\nAccept: application/octet-stream\r\nConnection: close\r\n\r\n"
            )
            .into_bytes(),
            HotHttpOperationKind::HttpsPost => {
                let body = repeated_body(request_body_len, b"hot-upload-body ");
                let mut request = format!(
                    "POST /hot/{path_id}/upload HTTP/1.1\r\nHost: {HOST}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .into_bytes();
                request.extend_from_slice(&body);
                request
            }
            HotHttpOperationKind::SseStream => format!(
                "GET /hot/{path_id}/events HTTP/1.1\r\nHost: {HOST}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n"
            )
            .into_bytes(),
        };
        let body = if kind == HotHttpOperationKind::SseStream {
            SSE_RESPONSE
        } else {
            response_body.bytes()
        };
        Self::Https(HttpsOperation {
            seed,
            index,
            name: kind.name(),
            expected_request: request.clone(),
            request,
            response: http_response_spec(body, response_framing, path_id, RawHttpConnection::Close),
            close_after_response: true,
            upstream_write_limit,
            expected: ExpectedOutcome::Success,
            details: vec![
                format!("response_body: {}", response_body.name()),
                format!("response_framing: {}", response_framing.name()),
                format!("request_body_len: {request_body_len}"),
                format!("path_id: {path_id}"),
                format!(
                    "upstream_write_limit: {}",
                    upstream_write_limit_name(upstream_write_limit)
                ),
            ],
        })
    }

    fn upstream_addr(&self) -> SocketAddr {
        SocketAddr::new(
            IpAddr::V4(UPSTREAM_IP),
            UPSTREAM_BASE_PORT.saturating_add(u16::try_from(self.index()).unwrap_or(u16::MAX)),
        )
    }

    const fn index(&self) -> usize {
        match self {
            Self::Https(operation) => operation.index,
            Self::HttpsSequence(operation) => operation.index,
            Self::Wss(operation) => operation.index,
        }
    }

    fn upstream(&self, server_config: &TlsServerConfig) -> Result<(SimTcpHandler, WorkloadTranscript)> {
        match self {
            Self::Https(operation) => {
                let upstream = TlsRawHttpUpstream::new(
                    server_config,
                    vec![operation.response.clone()],
                    operation.close_after_response,
                );
                Ok((upstream.handler(), WorkloadTranscript::Http(upstream.transcript())))
            }
            Self::HttpsSequence(operation) => {
                let upstream = TlsRawHttpUpstream::new(server_config, operation.responses.clone(), true);
                Ok((upstream.handler(), WorkloadTranscript::Http(upstream.transcript())))
            }
            Self::Wss(operation) => {
                let upstream = super::protocol::websocket::TlsWssUpstream::with_response_fragmentation(
                    operation.response_message.clone(),
                    operation.close_after_response,
                    operation.response_fragmented,
                )?;
                Ok((upstream.handler(), WorkloadTranscript::Wss(upstream.transcript())))
            }
        }
    }

    const fn upstream_write_limit(&self) -> Option<usize> {
        match self {
            Self::Https(operation) => operation.upstream_write_limit,
            Self::HttpsSequence(operation) => operation.upstream_write_limit,
            Self::Wss(operation) => operation.upstream_write_limit,
        }
    }

    fn summary(&self) -> String {
        match self {
            Self::Https(operation) => format!(
                "operation_index={} seed={} kind={} request_len={} response_len={} expected={}",
                operation.index,
                operation.seed,
                operation.name,
                operation.request.len(),
                operation.response.plaintext.len(),
                operation.expected.name()
            )
            .with_details(&operation.details),
            Self::HttpsSequence(operation) => format!(
                "operation_index={} seed={} kind=https-sequence requests={} response_len={} expected=success",
                operation.index,
                operation.seed,
                operation.requests.len(),
                operation
                    .responses
                    .iter()
                    .map(|response| response.plaintext.len())
                    .sum::<usize>()
            )
            .with_details(&operation.details),
            Self::Wss(operation) => format!(
                "operation_index={} seed={} kind=wss message_len={} response_len={} fragmented={} response_fragmented={}",
                operation.index,
                operation.seed,
                operation.message.len(),
                operation.response_message.len(),
                operation.fragmented,
                operation.response_fragmented
            )
            .with_details(&operation.details),
        }
    }

    fn secret_bindings(&self) -> Vec<SecretBinding> {
        match self {
            Self::Https(operation) => {
                if operation.expected.expects_failure() {
                    return operation.expected.secret_bindings().to_vec();
                }
                if operation
                    .request
                    .windows(PLACEHOLDER.len())
                    .any(|window| window == PLACEHOLDER.as_bytes())
                {
                    vec![WorkloadSecrets::authorized_for(HOST)]
                } else {
                    Vec::new()
                }
            }
            Self::HttpsSequence(_) | Self::Wss(_) => {
                vec![WorkloadSecrets::authorized_for(HOST)]
            }
        }
    }

    const fn expects_failure(&self) -> bool {
        match self {
            Self::Https(operation) => operation.expected.expects_failure(),
            Self::HttpsSequence(_) | Self::Wss(_) => false,
        }
    }

    fn forbidden_upstream(&self) -> &[Vec<u8>] {
        match self {
            Self::Https(operation) => operation.expected.forbidden_upstream(),
            Self::HttpsSequence(_) | Self::Wss(_) => &[],
        }
    }
}

#[derive(Debug, Clone)]
struct HttpsOperation {
    seed: Seed,
    index: usize,
    name: &'static str,
    request: Vec<u8>,
    expected_request: Vec<u8>,
    response: RawHttpResponse,
    close_after_response: bool,
    upstream_write_limit: Option<usize>,
    expected: ExpectedOutcome,
    details: Vec<String>,
}

#[derive(Debug, Clone)]
struct HttpsSequenceOperation {
    seed: Seed,
    index: usize,
    requests: Vec<Vec<u8>>,
    expected_requests: Vec<Vec<u8>>,
    responses: Vec<RawHttpResponse>,
    upstream_write_limit: Option<usize>,
    details: Vec<String>,
}

#[derive(Debug, Clone)]
struct WssOperation {
    seed: Seed,
    index: usize,
    message: Vec<u8>,
    response_message: Vec<u8>,
    fragmented: bool,
    response_fragmented: bool,
    close_after_response: bool,
    upstream_write_limit: Option<usize>,
    details: Vec<String>,
}

#[derive(Debug, Clone)]
enum ExpectedOutcome {
    Success,
    Failure {
        secrets: WorkloadSecrets,
        forbidden_upstream: Vec<Vec<u8>>,
    },
}

impl ExpectedOutcome {
    const fn name(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure { .. } => "failure",
        }
    }

    const fn expects_failure(&self) -> bool {
        matches!(self, Self::Failure { .. })
    }

    fn forbidden_upstream(&self) -> &[Vec<u8>] {
        match self {
            Self::Success => &[],
            Self::Failure { forbidden_upstream, .. } => forbidden_upstream,
        }
    }

    fn secret_bindings(&self) -> &[SecretBinding] {
        match self {
            Self::Success => &[],
            Self::Failure { secrets, .. } => secrets.bindings(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HotOperationKind {
    HttpsGet,
    HttpsPost,
    SseStream,
    WebSocket,
}

impl HotOperationKind {
    fn select(rng: &mut DeterministicRng) -> Self {
        match rng.below(4).unwrap_or(0) {
            0 => Self::HttpsGet,
            1 => Self::HttpsPost,
            2 => Self::SseStream,
            _ => Self::WebSocket,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HotHttpOperationKind {
    HttpsGet,
    HttpsPost,
    SseStream,
}

impl HotHttpOperationKind {
    const fn name(self) -> &'static str {
        match self {
            Self::HttpsGet => "https-get",
            Self::HttpsPost => "https-post",
            Self::SseStream => "sse-stream",
        }
    }
}

struct BatchRunner {
    sim: Simulator,
    guest_link: super::GuestLink,
    guest: SmolTcpGuest,
    running: <AgentdpNetworkSim as NetworkUnderTest>::Running,
    ca_pem: String,
    transcripts: Vec<(usize, WorkloadTranscript)>,
    next_dns_refresh: Duration,
    history: VecDeque<String>,
    allocation_report: Option<HotAllocationReport>,
    steady_state_allocations: Option<agentdp_test_support::allocation::AllocationSnapshot>,
}

impl BatchRunner {
    fn start_isolated(batch: &WorkloadBatch) -> Result<Self> {
        Self::start(batch.root_seed, batch.operations.clone(), &batch.secrets)
    }

    fn start_hot(controls: &RunControls, max_operations: usize) -> Result<Self> {
        let mut generator = BatchGenerator::new(controls);
        let mut operations = Vec::with_capacity(max_operations);
        while operations.len() < max_operations {
            operations.extend(generator.next_batch().operations);
        }
        let secrets = WorkloadSecrets::for_operations(&operations);
        Self::start(controls.root_seed, operations, &secrets)
    }

    fn start(root_seed: Seed, operations: Vec<WorkloadOperation>, secrets: &WorkloadSecrets) -> Result<Self> {
        let mut sim = Simulator::new(root_seed.derive("hot-dataplane-lane"));
        let guest_link = sim.guest_link()?;
        let mediated_ca = fixed_mediated_ca();
        let upstream_identity = TestTlsIdentity::fixed_upstream()?;
        let mut network = tls_network_config_for(
            &mediated_ca,
            std::slice::from_ref(&upstream_identity.root_ca_pem),
            &[HOST],
            secrets.runtime(),
            &[],
        );
        let mut upstreams = SimulationUpstreams::default().with_dns_a_endpoint(DNS_UPSTREAM, UPSTREAM_IP);
        let mut transcripts = Vec::with_capacity(operations.len());
        let mut intercepted_ports = Vec::with_capacity(operations.len());
        for operation in operations {
            let addr = operation.upstream_addr();
            intercepted_ports.push(addr.port());
            let (handler, transcript) = operation.upstream(&upstream_identity.server_config)?;
            upstreams = if let Some(write_limit) = operation.upstream_write_limit() {
                upstreams.with_limited_tcp_handler(addr, handler, write_limit)
            } else {
                upstreams.with_tcp_handler(addr, handler)
            };
            transcripts.push((operation.index(), transcript));
        }
        if let Some(tls) = network.tls.as_mut() {
            tls.intercepted_ports = intercepted_ports;
        }
        let mut running = AgentdpNetworkSim::start(
            ScenarioNetworkConfig {
                seed: sim.seed(),
                network,
                upstreams,
            },
            guest_link.clone(),
        )?;
        attribute_named_host_to_upstream(&mut sim, &mut running, &guest_link, HOST)?;
        let guest = SmolTcpGuest::new(guest_link.clone())?;
        Ok(Self {
            sim,
            guest_link,
            guest,
            running,
            ca_pem: mediated_ca.cert_pem,
            transcripts,
            next_dns_refresh: Duration::from_secs(50),
            history: VecDeque::with_capacity(HISTORY_LIMIT),
            allocation_report: None,
            steady_state_allocations: None,
        })
    }

    fn run_hot_batch(&mut self, batch: &WorkloadBatch) -> Result<()> {
        self.refresh_dns_attribution_if_needed()?;
        self.apply_fault_schedule(&batch.fault);
        let mut flows = match self.start_batch_flows(batch) {
            Ok(flows) => flows,
            Err(error) if batch.expects_failure() => return self.finish_expected_failure(batch, &[], &error),
            Err(error) => return Err(error),
        };
        apply_link_actions(&self.guest_link, &batch.fault.post_tls_actions);
        if let Err(error) = self.drive_batch_flows(batch, &mut flows) {
            if batch.expects_failure() {
                return self.finish_expected_failure(batch, &flows, &error);
            }
            return Err(error);
        }
        self.drain_completed_flows(&mut flows)?;
        let tcp_handles = flow_tcp_handles(&flows);
        for flow in &flows {
            self.guest.close(&mut self.running, flow.tcp())?;
        }
        let quiescence = self.sim.drive_guest_network_until_quiescent(
            &mut self.guest,
            &mut self.running,
            &self.guest_link,
            "hot dataplane close",
            DriveBudget {
                max_steps: 1024,
                ..DriveBudget::default()
            },
        )?;
        self.check_batch(batch, BatchObservation { flows, quiescence })?;
        self.cleanup_hot_tcp(batch, &tcp_handles)?;
        self.record_history(batch);
        Ok(())
    }

    fn cleanup_hot_tcp(&mut self, batch: &WorkloadBatch, handles: &[TcpHandle]) -> Result<()> {
        for &handle in handles {
            self.guest.abort_tcp(&mut self.running, handle)?;
        }
        self.sim.drive_guest_network_until_quiescent(
            &mut self.guest,
            &mut self.running,
            &self.guest_link,
            "hot dataplane cleanup",
            DriveBudget {
                max_steps: 1024,
                ..DriveBudget::default()
            },
        )?;
        let active_tcp_proxy_slots = self.running.active_tcp_proxy_slots();
        if active_tcp_proxy_slots != 0 {
            let tcp_snapshot = self.running.tcp_snapshot();
            return Err(Error::new(format!(
                "hot dataplane cleanup left {active_tcp_proxy_slots} TCP proxy slots active\n{tcp_snapshot}\n{}",
                self.batch_state_snapshot(batch)
            )));
        }
        for &handle in handles {
            self.guest.remove_tcp(handle);
        }
        Ok(())
    }

    fn finish_expected_failure(
        &mut self,
        batch: &WorkloadBatch,
        flows: &[WorkloadFlow],
        drive_error: &Error,
    ) -> Result<()> {
        let tcp_handles = flow_tcp_handles(flows);
        for flow in flows {
            let _closed = self.guest.close(&mut self.running, flow.tcp());
        }
        let quiescence = self.sim.drive_guest_network_until_quiescent(
            &mut self.guest,
            &mut self.running,
            &self.guest_link,
            "expected randomized dataplane failure",
            DriveBudget {
                max_steps: 1024,
                ..DriveBudget::default()
            },
        )?;
        self.check_expected_failure_batch(batch, quiescence, drive_error)?;
        self.cleanup_hot_tcp(batch, &tcp_handles)?;
        self.record_history(batch);
        Ok(())
    }

    fn start_batch_flows(&mut self, batch: &WorkloadBatch) -> Result<Vec<WorkloadFlow>> {
        let mut pending = Vec::with_capacity(batch.operations.len());
        for operation in &batch.operations {
            let transcript = self.transcript_for(operation)?;
            let tcp = self
                .guest
                .connect(&mut self.running, operation.upstream_addr())
                .map_err(|error| {
                    Error::new(format!(
                        "hot batch {} operation {} connect failed: {}; status={:?}; quiescence={:?}",
                        batch.index,
                        operation.index(),
                        error,
                        self.running.status(),
                        self.sim.quiescence(&self.running, &self.guest_link)
                    ))
                })?;
            pending.push((operation, transcript, tcp));
        }
        apply_link_actions(&self.guest_link, &batch.fault.post_connect_actions);
        let mut flows = Vec::with_capacity(pending.len());
        for (operation, transcript, tcp) in pending {
            flows.push(WorkloadFlow::new(operation, &self.ca_pem, transcript, tcp)?);
        }
        Ok(flows)
    }

    fn drive_batch_flows(&mut self, batch: &WorkloadBatch, flows: &mut [WorkloadFlow]) -> Result<()> {
        let progress = std::cell::Cell::new(0_usize);
        let diagnostics = RefCell::new(String::new());
        self.sim.drive_guest_until_with_progress(
            &mut self.guest,
            &mut self.running,
            DriveGuestProgress {
                label: batch.run_name,
                budget: DriveBudget {
                    max_steps: 32_768,
                    step_time: Duration::from_millis(1),
                },
            },
            |guest, running| {
                for flow in &mut *flows {
                    flow.drive_step(guest, running)?;
                }
                progress.set(flows.iter().map(WorkloadFlow::progress).sum());
                *diagnostics.borrow_mut() = batch_diagnostics(batch, flows);
                Ok(flows.iter().all(WorkloadFlow::is_complete))
            },
            || progress.get(),
            |output| {
                output.push_str(&diagnostics.borrow());
            },
        )
    }

    fn drain_completed_flows(&mut self, flows: &mut [WorkloadFlow]) -> Result<()> {
        for flow in flows {
            flow.drain_after_completion(&mut self.sim, &mut self.guest, &mut self.running, &self.guest_link)?;
        }
        Ok(())
    }

    fn check_batch(&self, batch: &WorkloadBatch, observed: BatchObservation) -> Result<()> {
        if batch.expects_failure() {
            return Err(Error::new(
                "randomized failure batch unexpectedly completed as a success",
            ));
        }
        let close_complete = observed.quiescence.is_quiescent();
        let upstream_request = observed.upstream_request();
        let expected_request = observed.expected_request();
        let guest_response = observed.guest_response();
        let expected_response = observed.expected_response();
        if let Some(error) = observed.lifecycle_error() {
            return Err(Error::new(format!("{error}\n{}", self.batch_state_snapshot(batch))));
        }
        let mut report = ScenarioReport::new(
            WORKLOAD_BATCH_SCENARIO,
            batch.seed,
            self.running.status(),
            observed.quiescence,
            self.sim.trace().to_vec(),
            self.guest_link.trace(),
            self.running.events(),
        )
        .with_upstream_transcript(UPSTREAM_TRANSCRIPT, upstream_request.clone())
        .with_guest_transcript(GUEST_TRANSCRIPT, guest_response.clone())
        .with_progress(
            "hot_upstream_request",
            upstream_request.len(),
            expected_request.len(),
            upstream_request == expected_request,
        )
        .with_progress(
            "hot_guest_response",
            guest_response.len(),
            expected_response.len(),
            guest_response == expected_response,
        )
        .with_progress("hot_tcp_close", usize::from(close_complete), 1, close_complete);
        let check = check_all(
            &mut report,
            vec![
                Box::new(ProgressComplete),
                Box::new(TranscriptEquals::upstream(
                    UPSTREAM_TRANSCRIPT,
                    expected_request.clone(),
                )),
                Box::new(TranscriptEquals::guest(GUEST_TRANSCRIPT, expected_response)),
                Box::new(NoUnexpectedEgressErrors),
                Box::new(Quiescent),
            ],
        );
        if let Err(error) = check {
            return Err(Error::new(format!(
                "{}\n{}\n{}",
                error,
                transcript_mismatch_snapshot("upstream_request", &upstream_request, &expected_request),
                self.batch_state_snapshot(batch)
            )));
        }
        Ok(())
    }

    fn check_expected_failure_batch(
        &self,
        batch: &WorkloadBatch,
        quiescence: QuiescenceReport,
        drive_error: &Error,
    ) -> Result<()> {
        let upstream_request = self.batch_upstream_transcript(batch);
        let mut report = ScenarioReport::new(
            WORKLOAD_BATCH_SCENARIO,
            batch.seed,
            self.running.status(),
            quiescence,
            self.sim.trace().to_vec(),
            self.guest_link.trace(),
            self.running.events(),
        )
        .with_upstream_transcript(UPSTREAM_TRANSCRIPT, upstream_request)
        .with_guest_transcript(GUEST_TRANSCRIPT, Vec::new());
        let check = check_all(
            &mut report,
            vec![
                Box::new(NoSecretLeak::new(batch.forbidden_upstream())),
                Box::new(ExpectedEgressError),
                Box::new(Quiescent),
            ],
        );
        if let Err(error) = check {
            return Err(Error::new(format!(
                "expected randomized failure did not satisfy failure invariants after drive error: {drive_error}\n{}\n{}",
                error,
                self.batch_state_snapshot(batch)
            )));
        }
        Ok(())
    }

    fn transcript_for(&self, operation: &WorkloadOperation) -> Result<WorkloadTranscript> {
        self.transcripts
            .iter()
            .find(|(index, _transcript)| *index == operation.index())
            .map(|(_index, transcript)| transcript.clone())
            .ok_or_else(|| {
                Error::new(format!(
                    "missing hot operation transcript for index {}",
                    operation.index()
                ))
            })
    }

    fn batch_upstream_transcript(&self, batch: &WorkloadBatch) -> Vec<u8> {
        let transcripts = batch
            .operations
            .iter()
            .filter_map(|operation| {
                self.transcripts
                    .iter()
                    .find(|(index, _transcript)| *index == operation.index())
                    .map(|(_index, transcript)| transcript)
            })
            .collect::<Vec<_>>();
        let len = transcripts
            .iter()
            .map(|transcript| transcript.upstream_observed_len())
            .sum();
        let mut bytes = Vec::with_capacity(len);
        for transcript in transcripts {
            transcript.extend_upstream_observed(&mut bytes);
        }
        bytes
    }

    fn apply_fault_schedule(&self, fault: &LinkFaultSchedule) {
        self.guest_link
            .set_path_delay(LinkDirection::GuestToNetwork, fault.guest_to_network_delay);
        self.guest_link
            .set_path_delay(LinkDirection::NetworkToGuest, fault.network_to_guest_delay);
    }

    fn record_history(&mut self, batch: &WorkloadBatch) {
        if self.history.len() == HISTORY_LIMIT {
            let _removed = self.history.pop_front();
        }
        self.history.push_back(batch.replay_record());
    }

    fn failure_snapshot(&self, batch: &WorkloadBatch, error: &Error) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "randomized_run: {}", batch.run_name);
        let _ = writeln!(output, "current_batch:");
        for line in batch.replay_record().lines() {
            let _ = writeln!(output, "  {line}");
        }
        let _ = writeln!(output, "previous_batches:");
        if self.history.is_empty() {
            let _ = writeln!(output, "  - none");
        } else {
            for (index, record) in self.history.iter().enumerate() {
                let _ = writeln!(output, "  - history_index: {index}");
                for line in record.lines() {
                    let _ = writeln!(output, "    {line}");
                }
            }
        }
        let _ = writeln!(output, "failure:");
        for line in error.to_string().lines() {
            let _ = writeln!(output, "  {line}");
        }
        let _ = write!(output, "{}", self.batch_state_snapshot(batch));
        output
    }

    fn batch_state_snapshot(&self, batch: &WorkloadBatch) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "network_state:");
        let _ = writeln!(output, "  batch_index: {}", batch.index);
        let _ = writeln!(output, "  operations: {}", batch.operations.len());
        let _ = writeln!(output, "  status: {:?}", self.running.status());
        let _ = writeln!(
            output,
            "  quiescence: {:?}",
            self.sim.quiescence(&self.running, &self.guest_link)
        );
        let _ = writeln!(output, "  buffer_snapshot: {}", self.running.buffer_snapshot());
        let _ = writeln!(output, "  tcp_snapshot: {}", self.running.tcp_snapshot());
        let _ = writeln!(
            output,
            "  pending_guest_to_network_frames: {}",
            self.guest_link.pending_to_network_frames()
        );
        let _ = writeln!(
            output,
            "  pending_network_to_guest_frames: {}",
            self.guest_link.pending_from_network_frames()
        );
        output
    }

    fn refresh_dns_attribution_if_needed(&mut self) -> Result<()> {
        if self.running.simulated_time() < self.next_dns_refresh {
            return Ok(());
        }
        attribute_named_host_to_upstream(&mut self.sim, &mut self.running, &self.guest_link, HOST)?;
        self.next_dns_refresh = self.next_dns_refresh.saturating_add(Duration::from_secs(50));
        Ok(())
    }

    fn stop(self) -> Result<()> {
        let _stop = RunningNetwork::stop(self.running)?;
        Ok(())
    }
}

#[derive(Clone)]
enum WorkloadTranscript {
    Http(Rc<RefCell<TlsTranscript>>),
    Wss(Rc<RefCell<TlsTranscript>>),
}

impl WorkloadTranscript {
    fn upstream_observed_len(&self) -> usize {
        match self {
            Self::Http(trace) => trace.borrow().request.len(),
            Self::Wss(trace) => trace.borrow().websocket_message.as_ref().map_or(0, Vec::len),
        }
    }

    fn extend_upstream_observed(&self, output: &mut Vec<u8>) {
        match self {
            Self::Http(trace) => output.extend_from_slice(&trace.borrow().request),
            Self::Wss(trace) => {
                output.extend_from_slice(trace.borrow().websocket_message.as_deref().unwrap_or_default());
            }
        }
    }
}

struct BatchObservation {
    flows: Vec<WorkloadFlow>,
    quiescence: QuiescenceReport,
}

impl BatchObservation {
    fn upstream_request(&self) -> Vec<u8> {
        let len = self.flows.iter().map(WorkloadFlow::upstream_observed_len).sum();
        let mut bytes = Vec::with_capacity(len);
        for flow in &self.flows {
            flow.extend_upstream_observed(&mut bytes);
        }
        bytes
    }

    fn expected_request(&self) -> Vec<u8> {
        concat_flow_bytes(&self.flows, WorkloadFlow::expected_upstream)
    }

    fn guest_response(&self) -> Vec<u8> {
        concat_flow_bytes(&self.flows, WorkloadFlow::guest_observed)
    }

    fn expected_response(&self) -> Vec<u8> {
        concat_flow_bytes(&self.flows, WorkloadFlow::expected_guest)
    }

    fn lifecycle_error(&self) -> Option<String> {
        self.flows
            .iter()
            .find_map(|flow| flow.lifecycle().error(flow.operation_index()))
    }
}

enum WorkloadFlow {
    Https {
        operation_index: usize,
        client: HttpsClientFlow,
        expected_upstream: Vec<u8>,
        transcript: Rc<RefCell<TlsTranscript>>,
    },
    Wss {
        operation_index: usize,
        client: WssClientFlow,
        expected_upstream: Vec<u8>,
        transcript: Rc<RefCell<TlsTranscript>>,
    },
}

impl WorkloadFlow {
    fn new(
        operation: &WorkloadOperation,
        ca_pem: &str,
        transcript: WorkloadTranscript,
        tcp: TcpHandle,
    ) -> Result<Self> {
        match (operation, transcript) {
            (WorkloadOperation::Https(operation), WorkloadTranscript::Http(transcript)) => Ok(Self::Https {
                operation_index: operation.index,
                client: HttpsClientFlow::new(HttpsClientFlowSpec {
                    tcp,
                    host: HOST,
                    ca_pem,
                    requests: vec![operation.request.clone()],
                    expected_response: operation.response.plaintext.clone(),
                })?,
                expected_upstream: operation.expected_request.clone(),
                transcript,
            }),
            (WorkloadOperation::HttpsSequence(operation), WorkloadTranscript::Http(transcript)) => Ok(Self::Https {
                operation_index: operation.index,
                client: HttpsClientFlow::new(HttpsClientFlowSpec {
                    tcp,
                    host: HOST,
                    ca_pem,
                    requests: operation.requests.clone(),
                    expected_response: operation
                        .responses
                        .iter()
                        .flat_map(|response| response.plaintext.clone())
                        .collect(),
                })?,
                expected_upstream: operation.expected_requests.concat(),
                transcript,
            }),
            (WorkloadOperation::Wss(operation), WorkloadTranscript::Wss(transcript)) => Ok(Self::Wss {
                operation_index: operation.index,
                client: WssClientFlow::new(WssClientFlowSpec {
                    tcp,
                    host: HOST,
                    ca_pem,
                    message: operation.message.clone(),
                    expected_response: operation.response_message.clone(),
                    fragmented: operation.fragmented,
                    close_after_response: operation.close_after_response,
                })?,
                expected_upstream: operation.message.clone(),
                transcript,
            }),
            _ => Err(Error::new("operation transcript type mismatch")),
        }
    }

    fn drive_step<N>(&mut self, guest: &mut SmolTcpGuest, running: &mut N) -> Result<()>
    where
        N: SteppedNetwork,
    {
        let operation_index = self.operation_index();
        match self {
            Self::Https { client, .. } => client.drive_step(guest, running),
            Self::Wss { client, .. } => client.drive_step(guest, running),
        }
        .map_err(|error| Error::new(format!("operation {operation_index} failed: {error}")))
    }

    fn drain_after_completion<N>(
        &mut self,
        sim: &mut Simulator,
        guest: &mut SmolTcpGuest,
        running: &mut N,
        guest_link: &super::GuestLink,
    ) -> Result<()>
    where
        N: SteppedNetwork,
    {
        match self {
            Self::Https { client, .. } => client.drain_after_completion(sim, guest, running, guest_link),
            Self::Wss { .. } => Ok(()),
        }
    }

    const fn is_complete(&self) -> bool {
        match self {
            Self::Https { client, .. } => client.is_complete(),
            Self::Wss { client, .. } => client.is_complete(),
        }
    }

    const fn progress(&self) -> usize {
        match self {
            Self::Https { client, .. } => client.progress(),
            Self::Wss { client, .. } => client.progress(),
        }
    }

    fn upstream_observed_len(&self) -> usize {
        match self {
            Self::Https { transcript, .. } => transcript.borrow().request.len(),
            Self::Wss { transcript, .. } => transcript.borrow().websocket_message.as_ref().map_or(0, Vec::len),
        }
    }

    fn extend_upstream_observed(&self, output: &mut Vec<u8>) {
        match self {
            Self::Https { transcript, .. } => output.extend_from_slice(&transcript.borrow().request),
            Self::Wss { transcript, .. } => {
                output.extend_from_slice(transcript.borrow().websocket_message.as_deref().unwrap_or_default());
            }
        }
    }

    fn expected_upstream(&self) -> &[u8] {
        match self {
            Self::Https { expected_upstream, .. } | Self::Wss { expected_upstream, .. } => expected_upstream,
        }
    }

    fn guest_observed(&self) -> &[u8] {
        match self {
            Self::Https { client, .. } => client.response(),
            Self::Wss { client, .. } => client.response_message(),
        }
    }

    fn expected_guest(&self) -> &[u8] {
        match self {
            Self::Https { client, .. } => client.expected_response(),
            Self::Wss { client, .. } => client.expected_response(),
        }
    }

    const fn operation_index(&self) -> usize {
        match self {
            Self::Https { operation_index, .. } | Self::Wss { operation_index, .. } => *operation_index,
        }
    }

    const fn tcp(&self) -> TcpHandle {
        match self {
            Self::Https { client, .. } => client.tcp(),
            Self::Wss { client, .. } => client.tcp(),
        }
    }

    const fn written(&self) -> usize {
        match self {
            Self::Https { client, .. } => client.written(),
            Self::Wss { client, .. } => client.frame_index(),
        }
    }

    fn lifecycle(&self) -> FlowLifecycle {
        match self {
            Self::Https { client, .. } => FlowLifecycle {
                tls_established: client.tls_established(),
                request_complete: client.request_complete(),
                response_complete: client.response_complete(),
            },
            Self::Wss { client, .. } => FlowLifecycle {
                tls_established: client.tls_established(),
                request_complete: client.request_complete(),
                response_complete: client.response_complete(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FlowLifecycle {
    tls_established: bool,
    request_complete: bool,
    response_complete: bool,
}

impl FlowLifecycle {
    fn error(self, operation_index: usize) -> Option<String> {
        if self.tls_established && self.request_complete && self.response_complete {
            return None;
        }
        Some(format!(
            "operation {operation_index} incomplete lifecycle: tls_established={} request_complete={} response_complete={}",
            self.tls_established, self.request_complete, self.response_complete
        ))
    }
}

fn concat_flow_bytes(flows: &[WorkloadFlow], bytes: impl Fn(&WorkloadFlow) -> &[u8]) -> Vec<u8> {
    let len = flows.iter().map(|flow| bytes(flow).len()).sum();
    let mut output = Vec::with_capacity(len);
    for flow in flows {
        output.extend_from_slice(bytes(flow));
    }
    output
}

fn flow_tcp_handles(flows: &[WorkloadFlow]) -> Vec<TcpHandle> {
    flows.iter().map(WorkloadFlow::tcp).collect()
}

fn batch_diagnostics(batch: &WorkloadBatch, flows: &[WorkloadFlow]) -> String {
    let mut snapshot = String::new();
    let _ = writeln!(snapshot, "  phase: randomized batch {}", batch.index);
    for flow in flows {
        let _ = writeln!(
            snapshot,
            "  operation={} written={} response_read={}/{} complete={}",
            flow.operation_index(),
            flow.written(),
            flow.guest_observed().len(),
            flow.expected_guest().len(),
            flow.is_complete()
        );
        let lifecycle = flow.lifecycle();
        let _ = writeln!(
            snapshot,
            "    lifecycle tls_established={} request_complete={} response_complete={}",
            lifecycle.tls_established, lifecycle.request_complete, lifecycle.response_complete
        );
    }
    snapshot
}

fn transcript_mismatch_snapshot(name: &str, actual: &[u8], expected: &[u8]) -> String {
    let Some(index) = first_mismatch(actual, expected) else {
        return format!("{name}: actual and expected bytes match\n");
    };
    let actual_byte = actual.get(index).copied();
    let expected_byte = expected.get(index).copied();
    format!(
        "{name}: first_mismatch={} actual={actual_byte:02x?} expected={expected_byte:02x?} actual_len={} expected_len={}\n",
        index,
        actual.len(),
        expected.len()
    )
}

fn first_mismatch(actual: &[u8], expected: &[u8]) -> Option<usize> {
    let common = actual.len().min(expected.len());
    for index in 0..common {
        if actual[index] != expected[index] {
            return Some(index);
        }
    }
    (actual.len() != expected.len()).then_some(common)
}

trait SummaryDetails {
    fn with_details(self, details: &[String]) -> Self;
}

impl SummaryDetails for String {
    fn with_details(mut self, details: &[String]) -> Self {
        for detail in details {
            let _ = write!(self, "\n      {detail}");
        }
        self
    }
}

#[derive(Debug, Clone)]
struct GeneratedWorkload {
    seed: Seed,
    index: usize,
    kind: WorkloadKind,
    method: RequestMethod,
    request_framing: RequestFraming,
    response_body: ResponseBody,
    response_framing: ResponseFraming,
    secret_mode: SecretMode,
    connection_mode: ConnectionMode,
    upstream_close_after_response: bool,
    upstream_write_limit: Option<usize>,
    path_id: u64,
    request_body_len: usize,
    fragmented_websocket: bool,
    fragmented_websocket_response: bool,
}

impl GeneratedWorkload {
    fn new(root_seed: Seed, index: usize, rng: &mut DeterministicRng) -> Self {
        let seed = root_seed.derive(&format!("case-{index}"));
        let kind = WorkloadKind::select(rng);
        Self {
            seed,
            index,
            kind,
            method: RequestMethod::select(rng),
            request_framing: RequestFraming::select(rng),
            response_body: ResponseBody::select(rng),
            response_framing: ResponseFraming::select(rng),
            secret_mode: SecretMode::select(rng),
            connection_mode: ConnectionMode::select(rng),
            upstream_close_after_response: rng.chance(1, 3),
            upstream_write_limit: select_upstream_write_limit(rng),
            path_id: rng.below(10_000).unwrap_or(0),
            request_body_len: select_usize(rng, &[0, 17, 512, 4096, 64 * 1024, 1024 * 1024 + 8192]),
            fragmented_websocket: rng.chance(1, 2),
            fragmented_websocket_response: rng.chance(1, 2),
        }
    }

    fn operations(&self) -> Vec<WorkloadOperation> {
        match self.kind {
            WorkloadKind::HttpsRequest => vec![self.https_request_operation()],
            WorkloadKind::HttpsSequence => vec![self.https_sequence_operation()],
            WorkloadKind::SseStream => vec![self.sse_operation()],
            WorkloadKind::WebSocket => vec![self.wss_operation()],
            WorkloadKind::ConcurrentHttpsWss => self.concurrent_operations(),
        }
    }

    fn https_request_operation(&self) -> WorkloadOperation {
        WorkloadOperation::Https(HttpsOperation {
            seed: self.seed,
            index: self.index,
            name: "https-request",
            expected_request: self.expected_http_request(HOST),
            request: self.http_request(HOST),
            response: self.response_spec_for_request_method(self.http_response_body()),
            close_after_response: self.upstream_close_after_response,
            upstream_write_limit: self.upstream_write_limit,
            expected: self.expected_outcome(),
            details: self.debug_details(),
        })
    }

    fn https_sequence_operation(&self) -> WorkloadOperation {
        let first = self.sequence_request(0, true);
        let second = self.sequence_request(1, false);
        let (requests, responses) = match self.connection_mode {
            ConnectionMode::Close => (
                vec![first.clone()],
                vec![self.response_spec(self.response_body.bytes())],
            ),
            ConnectionMode::KeepAliveSequence => (
                vec![first.clone(), second.clone()],
                vec![
                    self.response_spec_with_connection(self.response_body.bytes(), RawHttpConnection::KeepAlive),
                    self.response_spec(self.response_body.next().bytes()),
                ],
            ),
        };
        WorkloadOperation::HttpsSequence(HttpsSequenceOperation {
            seed: self.seed,
            index: self.index,
            requests,
            expected_requests: match self.connection_mode {
                ConnectionMode::Close => vec![first],
                ConnectionMode::KeepAliveSequence => vec![first, second],
            },
            responses,
            upstream_write_limit: self.upstream_write_limit,
            details: self.debug_details(),
        })
    }

    fn sse_operation(&self) -> WorkloadOperation {
        let request = format!(
            "GET /backend-api/randomized/{}/stream HTTP/1.1\r\nHost: {HOST}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n",
            self.path_id
        )
        .into_bytes();
        WorkloadOperation::Https(HttpsOperation {
            seed: self.seed,
            index: self.index,
            name: "sse-stream",
            expected_request: request.clone(),
            request,
            response: self.response_spec(SSE_RESPONSE),
            close_after_response: self.upstream_close_after_response,
            upstream_write_limit: self.upstream_write_limit,
            expected: ExpectedOutcome::Success,
            details: self.debug_details(),
        })
    }

    fn wss_operation(&self) -> WorkloadOperation {
        let message = format!(
            "{{\"type\":\"input\",\"path\":{},\"body_len\":{},\"secret_mode\":\"{}\"}}",
            self.path_id,
            self.request_body_len,
            self.secret_mode.name()
        )
        .into_bytes();
        WorkloadOperation::Wss(WssOperation {
            seed: self.seed,
            index: self.index,
            message,
            response_message: WSS_RESPONSE.to_vec(),
            fragmented: self.fragmented_websocket,
            response_fragmented: self.fragmented_websocket_response,
            close_after_response: self.upstream_close_after_response,
            upstream_write_limit: self.upstream_write_limit,
            details: self.debug_details(),
        })
    }

    fn concurrent_operations(&self) -> Vec<WorkloadOperation> {
        let package = WorkloadOperation::Https(HttpsOperation {
            seed: self.seed.derive("package"),
            index: self.index,
            name: "concurrent-package",
            request: format!(
                "GET /v2/library/alpine/blobs/sha256:layer HTTP/1.1\r\nHost: {HOST}\r\nAccept: application/octet-stream\r\nConnection: close\r\n\r\n"
            )
            .into_bytes(),
            expected_request: format!(
                "GET /v2/library/alpine/blobs/sha256:layer HTTP/1.1\r\nHost: {HOST}\r\nAccept: application/octet-stream\r\nConnection: close\r\n\r\n"
            )
            .into_bytes(),
            response: self.response_spec(self.response_body.bytes()),
            close_after_response: true,
            upstream_write_limit: self.upstream_write_limit,
            expected: ExpectedOutcome::Success,
            details: self.concurrent_debug_details("package"),
        });
        let upload = WorkloadOperation::Https(HttpsOperation {
            seed: self.seed.derive("upload"),
            index: self.index.saturating_add(1),
            name: "concurrent-upload",
            expected_request: upload_request(self.request_body_len),
            request: upload_request(self.request_body_len),
            response: self.response_spec(SMALL_RESPONSE),
            close_after_response: true,
            upstream_write_limit: self.upstream_write_limit,
            expected: ExpectedOutcome::Success,
            details: self.concurrent_debug_details("upload"),
        });
        let sse = WorkloadOperation::Https(HttpsOperation {
            seed: self.seed.derive("sse"),
            index: self.index.saturating_add(2),
            name: "concurrent-sse",
            request: format!(
                "GET /backend-api/conversation/stream HTTP/1.1\r\nHost: {HOST}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n"
            )
            .into_bytes(),
            expected_request: format!(
                "GET /backend-api/conversation/stream HTTP/1.1\r\nHost: {HOST}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n"
            )
            .into_bytes(),
            response: self.response_spec(SSE_RESPONSE),
            close_after_response: true,
            upstream_write_limit: self.upstream_write_limit,
            expected: ExpectedOutcome::Success,
            details: self.concurrent_debug_details("sse"),
        });
        let wss = WorkloadOperation::Wss(WssOperation {
            seed: self.seed.derive("wss"),
            index: self.index.saturating_add(3),
            message: br#"{"type":"input","session":"codex","text":"continue"}"#.to_vec(),
            response_message: WSS_RESPONSE.to_vec(),
            fragmented: self.fragmented_websocket,
            response_fragmented: self.fragmented_websocket_response,
            close_after_response: self.upstream_close_after_response,
            upstream_write_limit: self.upstream_write_limit,
            details: self.concurrent_debug_details("wss"),
        });
        vec![package, upload, sse, wss]
    }

    fn expected_outcome(&self) -> ExpectedOutcome {
        match self.secret_mode {
            SecretMode::None | SecretMode::Authorized => ExpectedOutcome::Success,
            SecretMode::WrongAuthority => ExpectedOutcome::Failure {
                secrets: WorkloadSecrets::single_authorized_for(BLOCKED_HOST),
                forbidden_upstream: vec![SECRET_VALUE.as_bytes().to_vec()],
            },
            SecretMode::Unresolved => ExpectedOutcome::Failure {
                secrets: WorkloadSecrets::single_authorized_for(HOST),
                forbidden_upstream: vec![UNKNOWN_PLACEHOLDER.as_bytes().to_vec()],
            },
        }
    }

    fn response_spec(&self, body: &'static [u8]) -> RawHttpResponse {
        self.response_spec_with_connection(body, RawHttpConnection::Close)
    }

    fn response_spec_for_request_method(&self, body: &'static [u8]) -> RawHttpResponse {
        if self.method == RequestMethod::Head {
            return http_head_response_spec(body, self.response_framing, self.path_id, RawHttpConnection::Close);
        }
        self.response_spec(body)
    }

    fn response_spec_with_connection(&self, body: &'static [u8], connection: RawHttpConnection) -> RawHttpResponse {
        http_response_spec(body, self.response_framing, self.path_id, connection)
    }

    fn http_request(&self, authority: &str) -> Vec<u8> {
        let path = self.path();
        let secret_header = self.secret_mode.header_value();
        let body_allowed = self.method.allows_body();
        let connection = if self.connection_mode == ConnectionMode::KeepAliveSequence {
            "keep-alive"
        } else {
            "close"
        };
        if !body_allowed {
            return format!(
                "{} {path} HTTP/1.1\r\nHost: {authority}\r\nAccept: application/octet-stream\r\n{secret_header}Connection: {connection}\r\n\r\n",
                self.method.as_str()
            )
            .into_bytes();
        }
        let body = self.request_body();
        match self.request_framing {
            RequestFraming::ContentLength => {
                let mut request = format!(
                    "{} {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n{secret_header}Connection: {connection}\r\n\r\n",
                    self.method.as_str(),
                    body.len()
                )
                .into_bytes();
                request.extend_from_slice(&body);
                request
            }
            RequestFraming::Chunked => {
                let mut request = format!(
                    "{} {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\n{secret_header}Connection: {connection}\r\n\r\n",
                    self.method.as_str()
                )
                .into_bytes();
                write_chunked_body(&mut request, &body, 1024);
                request
            }
        }
    }

    fn expected_http_request(&self, authority: &str) -> Vec<u8> {
        let request = self.http_request(authority);
        if self.secret_mode == SecretMode::Authorized {
            return model_intercepted_http_request(
                &request,
                [HttpSecretSubstitution {
                    placeholder: PLACEHOLDER.as_bytes(),
                    value: SECRET_VALUE.as_bytes(),
                }],
            );
        }
        request
    }

    fn debug_details(&self) -> Vec<String> {
        vec![
            format!("workload_kind: {}", self.kind.name()),
            format!("method: {}", self.method.as_str()),
            format!("request_framing: {}", self.request_framing.name()),
            format!("response_body: {}", self.http_response_body_name()),
            format!("response_framing: {}", self.response_framing.name()),
            format!("secret_mode: {}", self.secret_mode.name()),
            format!("connection_mode: {}", self.connection_mode.name()),
            format!("upstream_close_after_response: {}", self.upstream_close_after_response),
            format!(
                "upstream_write_limit: {}",
                upstream_write_limit_name(self.upstream_write_limit)
            ),
            format!("request_body_len: {}", self.request_body_len),
            format!("fragmented_websocket_response: {}", self.fragmented_websocket_response),
            format!("path_id: {}", self.path_id),
        ]
    }

    fn concurrent_debug_details(&self, flow: &'static str) -> Vec<String> {
        let mut details = self.debug_details();
        details.push(format!("concurrent_flow: {flow}"));
        details
    }

    fn sequence_request(&self, ordinal: usize, keep_alive: bool) -> Vec<u8> {
        format!(
            "GET /randomized/{}/sequence/{ordinal} HTTP/1.1\r\nHost: {HOST}\r\nAccept: application/octet-stream\r\nConnection: {}\r\n\r\n",
            self.path_id,
            if keep_alive { "keep-alive" } else { "close" }
        )
        .into_bytes()
    }

    fn request_body(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(self.request_body_len);
        while body.len() < self.request_body_len {
            body.extend_from_slice(b"randomized-post-body-line ");
            body.extend_from_slice(self.secret_mode.body_marker().as_bytes());
            body.push(b'\n');
        }
        body.truncate(self.request_body_len);
        body
    }

    fn http_response_body(&self) -> &'static [u8] {
        if self.method == RequestMethod::Head {
            EMPTY_RESPONSE
        } else {
            self.response_body.bytes()
        }
    }

    fn http_response_body_name(&self) -> &'static str {
        if self.method == RequestMethod::Head {
            "empty-head"
        } else {
            self.response_body.name()
        }
    }

    fn path(&self) -> String {
        match self.secret_mode {
            SecretMode::Authorized => format!("/randomized/{}/authorized?token={PLACEHOLDER}", self.path_id),
            SecretMode::Unresolved => format!("/randomized/{}/unresolved?token={UNKNOWN_PLACEHOLDER}", self.path_id),
            SecretMode::None | SecretMode::WrongAuthority => format!("/randomized/{}/blob", self.path_id),
        }
    }
}

fn upload_request(body_len: usize) -> Vec<u8> {
    let body = repeated_body(body_len, b"randomized-upload-body ");
    let mut request = format!(
        "PUT /packages/concurrent-artifact.tgz HTTP/1.1\r\nHost: {HOST}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    request
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkloadKind {
    HttpsRequest,
    HttpsSequence,
    SseStream,
    WebSocket,
    ConcurrentHttpsWss,
}

impl WorkloadKind {
    fn select(rng: &mut DeterministicRng) -> Self {
        match rng.below(5).unwrap_or(0) {
            0 => Self::HttpsRequest,
            1 => Self::HttpsSequence,
            2 => Self::SseStream,
            3 => Self::WebSocket,
            _ => Self::ConcurrentHttpsWss,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::HttpsRequest => "https-request",
            Self::HttpsSequence => "https-sequence",
            Self::SseStream => "sse-stream",
            Self::WebSocket => "websocket",
            Self::ConcurrentHttpsWss => "concurrent-https-wss",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestMethod {
    Get,
    Head,
    Post,
    Put,
}

impl RequestMethod {
    fn select(rng: &mut DeterministicRng) -> Self {
        match rng.below(4).unwrap_or(0) {
            0 => Self::Get,
            1 => Self::Head,
            2 => Self::Post,
            _ => Self::Put,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Put => "PUT",
        }
    }

    const fn allows_body(self) -> bool {
        matches!(self, Self::Post | Self::Put)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestFraming {
    ContentLength,
    Chunked,
}

impl RequestFraming {
    fn select(rng: &mut DeterministicRng) -> Self {
        if rng.chance(1, 3) {
            Self::Chunked
        } else {
            Self::ContentLength
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::ContentLength => "content-length",
            Self::Chunked => "chunked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseFraming {
    ContentLength,
    SegmentedContentLength,
    Chunked,
    SegmentedChunked,
}

impl ResponseFraming {
    fn select(rng: &mut DeterministicRng) -> Self {
        match rng.below(4).unwrap_or(0) {
            0 => Self::ContentLength,
            1 => Self::SegmentedContentLength,
            2 => Self::Chunked,
            _ => Self::SegmentedChunked,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::ContentLength => "content-length",
            Self::SegmentedContentLength => "segmented-content-length",
            Self::Chunked => "chunked",
            Self::SegmentedChunked => "segmented-chunked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseBody {
    Empty,
    Small,
    Medium,
    Large,
    Boundary,
    AboveBoundary,
    Sse,
}

impl ResponseBody {
    fn select(rng: &mut DeterministicRng) -> Self {
        match rng.below(6).unwrap_or(0) {
            0 => Self::Empty,
            1 => Self::Small,
            2 => Self::Medium,
            3 => Self::Large,
            4 => Self::Boundary,
            _ => Self::AboveBoundary,
        }
    }

    fn select_hot(rng: &mut DeterministicRng) -> Self {
        match rng.below(5).unwrap_or(0) {
            0 => Self::Empty,
            1 => Self::Small,
            2 => Self::Medium,
            3 => Self::Large,
            _ => Self::Sse,
        }
    }

    const fn bytes(self) -> &'static [u8] {
        match self {
            Self::Empty => EMPTY_RESPONSE,
            Self::Small => SMALL_RESPONSE,
            Self::Medium => MEDIUM_RESPONSE,
            Self::Large => LARGE_RESPONSE,
            Self::Boundary => MEDIATION_BOUNDARY_RESPONSE,
            Self::AboveBoundary => ABOVE_MEDIATION_BOUNDARY_RESPONSE,
            Self::Sse => SSE_RESPONSE,
        }
    }

    const fn next(self) -> Self {
        match self {
            Self::Empty | Self::Boundary | Self::AboveBoundary => Self::Small,
            Self::Small => Self::Medium,
            Self::Medium => Self::Large,
            Self::Large | Self::Sse => Self::Boundary,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
            Self::Boundary => "mediation-boundary",
            Self::AboveBoundary => "above-mediation-boundary",
            Self::Sse => "sse",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecretMode {
    None,
    Authorized,
    WrongAuthority,
    Unresolved,
}

impl SecretMode {
    fn select(rng: &mut DeterministicRng) -> Self {
        match rng.below(4).unwrap_or(0) {
            0 => Self::None,
            1 => Self::Authorized,
            2 => Self::WrongAuthority,
            _ => Self::Unresolved,
        }
    }

    const fn header_value(self) -> &'static str {
        match self {
            Self::None => "",
            Self::Authorized | Self::WrongAuthority => "Authorization: Bearer AGENTDP_SECRET_TOKEN\r\n",
            Self::Unresolved => "Authorization: Bearer AGENTDP_SECRET_UNKNOWN\r\n",
        }
    }

    const fn body_marker(self) -> &'static str {
        match self {
            Self::Authorized | Self::WrongAuthority => PLACEHOLDER,
            Self::Unresolved => UNKNOWN_PLACEHOLDER,
            Self::None => "ordinary-body",
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Authorized => "authorized",
            Self::WrongAuthority => "wrong-authority",
            Self::Unresolved => "unresolved",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionMode {
    Close,
    KeepAliveSequence,
}

impl ConnectionMode {
    fn select(rng: &mut DeterministicRng) -> Self {
        if rng.chance(1, 3) {
            Self::KeepAliveSequence
        } else {
            Self::Close
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Close => "close",
            Self::KeepAliveSequence => "keep-alive-sequence",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LinkFaultSchedule {
    guest_to_network_delay: Duration,
    network_to_guest_delay: Duration,
    post_connect_actions: Vec<LinkAction>,
    post_tls_actions: Vec<LinkAction>,
}

impl LinkFaultSchedule {
    fn select(rng: &mut DeterministicRng) -> Self {
        let mut post_connect_actions = Vec::new();
        let mut post_tls_actions = Vec::new();
        if rng.chance(2, 3) {
            post_connect_actions.push(select_link_action(rng));
        }
        if rng.chance(1, 5) {
            post_connect_actions.push(select_link_action(rng));
        }
        if rng.chance(2, 3) {
            post_tls_actions.push(select_data_link_action(rng));
        }
        if rng.chance(1, 4) {
            post_tls_actions.push(select_data_link_action(rng));
        }
        Self {
            guest_to_network_delay: select_duration(rng, &[0, 1, 2, 5, 20, 75, 150]),
            network_to_guest_delay: select_duration(rng, &[0, 1, 3, 7, 25, 100, 200]),
            post_connect_actions,
            post_tls_actions,
        }
    }

    fn name(&self) -> String {
        let connect_actions = link_action_list_name(&self.post_connect_actions);
        let data_actions = link_action_list_name(&self.post_tls_actions);
        format!(
            "delay_g2n_ms={},delay_n2g_ms={},post_connect_actions={},post_tls_or_upgrade_actions={}",
            self.guest_to_network_delay.as_millis(),
            self.network_to_guest_delay.as_millis(),
            connect_actions,
            data_actions
        )
    }
}

fn select_link_action(rng: &mut DeterministicRng) -> LinkAction {
    match rng.below(8).unwrap_or(0) {
        0 => LinkAction::DropNextFrame(LinkDirection::GuestToNetwork),
        1 => LinkAction::DropNextFrame(LinkDirection::NetworkToGuest),
        2 => LinkAction::DuplicateNextFrame(LinkDirection::GuestToNetwork),
        3 => LinkAction::DuplicateNextFrame(LinkDirection::NetworkToGuest),
        4 => LinkAction::ReorderNextFrames(LinkDirection::GuestToNetwork),
        5 => LinkAction::ReorderNextFrames(LinkDirection::NetworkToGuest),
        6 => LinkAction::BlockNextRead,
        _ => LinkAction::BlockNextWrite,
    }
}

fn select_data_link_action(rng: &mut DeterministicRng) -> LinkAction {
    match rng.below(6).unwrap_or(0) {
        0 => LinkAction::DuplicateNextFrame(LinkDirection::GuestToNetwork),
        1 => LinkAction::DuplicateNextFrame(LinkDirection::NetworkToGuest),
        2 => LinkAction::ReorderNextFrames(LinkDirection::GuestToNetwork),
        3 => LinkAction::ReorderNextFrames(LinkDirection::NetworkToGuest),
        4 => LinkAction::BlockNextRead,
        _ => LinkAction::BlockNextWrite,
    }
}

fn link_action_list_name(actions: &[LinkAction]) -> String {
    let name = actions
        .iter()
        .map(|action| link_action_name(*action))
        .collect::<Vec<_>>()
        .join("+");
    if name.is_empty() { "none".to_owned() } else { name }
}

fn link_action_name(action: LinkAction) -> String {
    match action {
        LinkAction::DropNextFrame(direction) => format!("drop-next-{direction}"),
        LinkAction::DuplicateNextFrame(direction) => format!("duplicate-next-{direction}"),
        LinkAction::ReorderNextFrames(direction) => format!("reorder-next-{direction}"),
        LinkAction::BlockNextRead => "block-next-read".to_owned(),
        LinkAction::BlockNextWrite => "block-next-write".to_owned(),
    }
}

fn http_response_spec(
    body: &'static [u8],
    framing: ResponseFraming,
    path_id: u64,
    connection: RawHttpConnection,
) -> RawHttpResponse {
    RawHttpResponse::response(
        body,
        raw_http_response_framing(framing, select_chunk_size(path_id, body.len())),
        connection,
        response_segment_size(framing, path_id, body.len()),
    )
}

fn http_head_response_spec(
    body: &'static [u8],
    framing: ResponseFraming,
    path_id: u64,
    connection: RawHttpConnection,
) -> RawHttpResponse {
    let mut response = RawHttpResponse::head_response(
        body.len(),
        raw_http_response_framing(framing, select_chunk_size(path_id, body.len())),
        connection,
        None,
    );
    response.segment_size = response_segment_size(framing, path_id, response.plaintext.len());
    response
}

const fn raw_http_response_framing(framing: ResponseFraming, chunk_size: usize) -> RawHttpResponseFraming {
    match framing {
        ResponseFraming::ContentLength | ResponseFraming::SegmentedContentLength => {
            RawHttpResponseFraming::ContentLength
        }
        ResponseFraming::Chunked | ResponseFraming::SegmentedChunked => RawHttpResponseFraming::Chunked { chunk_size },
    }
}

fn response_segment_size(framing: ResponseFraming, path_id: u64, plaintext_len: usize) -> Option<usize> {
    match framing {
        ResponseFraming::SegmentedContentLength | ResponseFraming::SegmentedChunked => {
            Some(select_segment_size(path_id, plaintext_len))
        }
        ResponseFraming::ContentLength | ResponseFraming::Chunked => None,
    }
}

fn repeated_body(len: usize, pattern: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(len);
    while body.len() < len {
        body.extend_from_slice(pattern);
    }
    body.truncate(len);
    body
}

fn select_usize(rng: &mut DeterministicRng, values: &[usize]) -> usize {
    let Some(index) = rng.below(u64::try_from(values.len()).unwrap_or(u64::MAX)) else {
        return 0;
    };
    values.get(usize::try_from(index).unwrap_or(0)).copied().unwrap_or(0)
}

fn select_upstream_write_limit(rng: &mut DeterministicRng) -> Option<usize> {
    if rng.chance(2, 3) {
        return None;
    }
    Some(select_usize(rng, &[1, 7, 31, 127, 1024, 4096]).max(1))
}

fn upstream_write_limit_name(limit: Option<usize>) -> String {
    limit.map_or_else(|| "none".to_owned(), |limit| limit.to_string())
}

fn select_duration(rng: &mut DeterministicRng, millis: &[u64]) -> Duration {
    Duration::from_millis(select_u64(rng, millis))
}

fn select_chunk_size(path_id: u64, body_len: usize) -> usize {
    if body_len > 4 * 1024 {
        return select_indexed_usize(path_id, &[1024, 4096, 16 * 1024]);
    }
    select_indexed_usize(path_id, &[1, 13, 512, 1024, 4096, 16 * 1024])
}

fn select_segment_size(path_id: u64, body_len: usize) -> usize {
    if body_len > 4 * 1024 {
        return select_indexed_usize(path_id.rotate_left(17), &[1500, 4096, 16 * 1024]);
    }
    select_indexed_usize(path_id.rotate_left(17), &[1, 19, 513, 1500, 4096, 16 * 1024])
}

fn select_indexed_usize(value: u64, values: &[usize]) -> usize {
    values
        .get(usize::try_from(value % u64::try_from(values.len()).unwrap_or(1)).unwrap_or(0))
        .copied()
        .unwrap_or(1)
}

fn select_u64(rng: &mut DeterministicRng, values: &[u64]) -> u64 {
    let Some(index) = rng.below(u64::try_from(values.len()).unwrap_or(u64::MAX)) else {
        return 0;
    };
    values.get(usize::try_from(index).unwrap_or(0)).copied().unwrap_or(0)
}

fn parse_seed_env(name: &str) -> Result<Option<Seed>> {
    let Some(value) = env::var_os(name) else {
        return Ok(None);
    };
    let value = env_string(name, value)?;
    value
        .parse::<Seed>()
        .map(Some)
        .map_err(|error| Error::from_display(&format!("parse {name}"), error))
}

fn parse_usize_env(name: &str) -> Result<Option<usize>> {
    let Some(value) = env::var_os(name) else {
        return Ok(None);
    };
    let value = env_string(name, value)?;
    value
        .parse::<usize>()
        .map(Some)
        .map_err(|error| Error::from_display(&format!("parse {name}"), error))
}

fn parse_bool_env(name: &str) -> Result<bool> {
    let Some(value) = env::var_os(name) else {
        return Ok(false);
    };
    match env_string(name, value)?.as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        value => Err(Error::new(format!("{name} must be a boolean value, got {value:?}"))),
    }
}

fn parse_seconds_env(name: &str) -> Result<Option<Duration>> {
    let Some(value) = parse_usize_env(name)? else {
        return Ok(None);
    };
    Ok(Some(Duration::from_secs(u64::try_from(value).unwrap_or(u64::MAX))))
}

fn env_string(name: &str, value: std::ffi::OsString) -> Result<String> {
    value
        .into_string()
        .map_err(|_error| Error::new(format!("{name} must be valid UTF-8")))
}

fn write_failure_snapshot(name: &str, seed: Seed, contents: &str) -> Result<PathBuf> {
    let dir = workspace_target_dir().join(RANDOMIZED_FAILURE_DIR);
    fs::create_dir_all(&dir)
        .map_err(|error| Error::from_display("create randomized failure snapshot directory", error))?;
    let path = dir.join(format!("{name}-{seed}.txt"));
    fs::write(&path, contents).map_err(|error| Error::from_display("write randomized failure snapshot", error))?;
    Ok(path)
}

fn workspace_target_dir() -> PathBuf {
    if let Some(target_dir) = env::var_os("CARGO_TARGET_DIR") {
        return PathBuf::from(target_dir);
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")))
        .join("target")
}

#[cfg(test)]
mod tests {
    use agentdp_rand::Seed;

    use super::{
        DEFAULT_ROOT_SEED, RANDOMIZED_REPLAY_BATCH_INDEX_ENV, RANDOMIZED_REPLAY_PREFIX_ENV, RUN_NAME, RunControls,
        RunMode, WorkloadKind,
    };

    #[test]
    fn generated_batch_replay_is_stable_from_root_seed_and_index() {
        let controls = controls(RunMode::isolated());
        let first = super::WorkloadBatch::for_index(&controls, 42);
        let second = super::WorkloadBatch::for_index(&controls, 42);

        assert_eq!(first.replay_record(), second.replay_record());
    }

    #[test]
    fn generated_batch_replay_record_names_replay_environment() {
        let batch = super::WorkloadBatch::for_index(&controls_with_seed(RunMode::isolated(), Seed::new(7)), 3);
        let replay = batch.replay_record();

        assert!(replay.contains("AGENTDP_NETWORK_RANDOMIZED_ROOT_SEED=0x0000000000000007"));
        assert!(replay.contains(&format!("{RANDOMIZED_REPLAY_BATCH_INDEX_ENV}=3")));
        assert!(replay.contains(&format!("{RANDOMIZED_REPLAY_PREFIX_ENV}=1")));
        assert!(replay.contains(RUN_NAME));
        assert!(replay.contains("fault: delay_g2n_ms="));
        assert!(replay.contains("operations:"));
    }

    #[test]
    fn hot_replay_capacity_is_direct_unless_prefix_replay_is_requested() {
        let mut controls = controls(RunMode::hot_concurrent());
        controls.replay_batch_index = Some(1280);

        assert_eq!(controls.hot_network_operation_capacity(), 256);

        controls.replay_prefix = true;

        assert_eq!(controls.hot_network_operation_capacity(), 1281 * 8);
    }

    #[test]
    fn generated_batch_failure_snapshot_includes_harness_and_drive_diagnostics() {
        let batch = super::WorkloadBatch::for_index(&controls_with_seed(RunMode::isolated(), Seed::new(7)), 3);
        let snapshot = batch.failure_snapshot(&super::Error::new(
            "drive \"HTTPS request response\" exhausted\ndrive_diagnostics:\n  phase: HTTPS request response",
        ));

        assert!(snapshot.contains("harness:"));
        assert!(snapshot.contains("guest_tcp_buffer_bytes:"));
        assert!(snapshot.contains("drive_diagnostics:"));
        assert!(snapshot.contains("phase: HTTPS request response"));
    }

    #[test]
    fn generated_head_response_has_headers_but_no_body_framing() {
        let batch = super::WorkloadBatch::for_index(&controls(RunMode::isolated()), 172);
        let operation = batch
            .operations
            .iter()
            .find_map(|operation| match operation {
                super::WorkloadOperation::Https(operation) if operation.name == "https-request" => Some(operation),
                super::WorkloadOperation::HttpsSequence(_)
                | super::WorkloadOperation::Https(_)
                | super::WorkloadOperation::Wss(_) => None,
            })
            .expect("batch 172 should contain the replayed HTTPS request");

        assert!(operation.request.starts_with(b"HEAD "));
        assert!(operation.response.plaintext.starts_with(b"HTTP/1.1 200 OK\r\n"));
        assert!(operation.response.plaintext.ends_with(b"\r\n\r\n"));
        assert!(!operation.response.plaintext.ends_with(b"0\r\n\r\n"));
    }

    #[test]
    fn full_generator_can_emit_websocket_operation() {
        let batch = generated_kind(WorkloadKind::WebSocket);

        assert!(
            batch
                .operations
                .iter()
                .any(|operation| matches!(operation, super::WorkloadOperation::Wss(_)))
        );
    }

    #[test]
    fn full_generator_can_emit_sse_operation() {
        let batch = generated_kind(WorkloadKind::SseStream);

        assert!(batch.operations.iter().any(|operation| {
            matches!(operation, super::WorkloadOperation::Https(operation) if operation.name == "sse-stream")
        }));
    }

    fn generated_kind(kind: WorkloadKind) -> super::WorkloadBatch {
        let controls = controls(RunMode::isolated());
        (0..256)
            .map(|index| super::WorkloadBatch::for_index(&controls, index))
            .find(|batch| {
                batch.operations.iter().any(|operation| match (kind, operation) {
                    (WorkloadKind::WebSocket, super::WorkloadOperation::Wss(_)) => true,
                    (WorkloadKind::SseStream, super::WorkloadOperation::Https(operation)) => {
                        operation.name == "sse-stream"
                    }
                    _ => false,
                })
            })
            .expect("default root seed should generate requested workload kind")
    }

    fn controls(mode: RunMode) -> RunControls {
        controls_with_seed(mode, DEFAULT_ROOT_SEED)
    }

    const fn controls_with_seed(mode: RunMode, root_seed: Seed) -> RunControls {
        RunControls {
            root_seed,
            max_operations: 256,
            duration: None,
            replay_batch_index: None,
            replay_prefix: false,
            trace_cases: super::TraceCases::Off,
            mode,
        }
    }
}
