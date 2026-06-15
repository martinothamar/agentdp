use std::alloc::System;
use std::hint::black_box;
use std::time::{Duration, Instant};

use agentdp_network_tests::{AgentWorkflowHarness, AsyncDataplane, agent_https_request, http_get_request};
use agentdp_test_support::allocation::{AllocationReport, ReportingAllocator};
use tokio::runtime::Builder;

#[global_allocator]
static ALLOCATOR: ReportingAllocator<System> = ReportingAllocator::new(System);

struct Scenario {
    name: &'static str,
    iterations: usize,
    request_body_bytes: usize,
    response_body_bytes: usize,
    mode: ScenarioMode,
}

#[derive(Clone, Copy)]
enum ScenarioMode {
    DirectHttp,
    DirectHttps,
    MediatedHttps,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "direct_http1_1KiB",
        iterations: 512,
        request_body_bytes: 256,
        response_body_bytes: 1024,
        mode: ScenarioMode::DirectHttp,
    },
    Scenario {
        name: "direct_https_http1_1KiB",
        iterations: 128,
        request_body_bytes: 256,
        response_body_bytes: 1024,
        mode: ScenarioMode::DirectHttps,
    },
    Scenario {
        name: "mediated_https_http1_1KiB",
        iterations: 128,
        request_body_bytes: 256,
        response_body_bytes: 1024,
        mode: ScenarioMode::MediatedHttps,
    },
    Scenario {
        name: "direct_https_http1_16KiB",
        iterations: 64,
        request_body_bytes: 1024,
        response_body_bytes: 16 * 1024,
        mode: ScenarioMode::DirectHttps,
    },
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{:<25} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "scenario", "ops", "req", "resp", "elapsed", "ops/s", "MiB/s", "p50", "p95", "allocs", "alloc B"
    );
    let runtime = Builder::new_current_thread().enable_all().build()?;
    for scenario in SCENARIOS {
        let result = runtime.block_on(run(scenario))?;
        result.print();
    }
    Ok(())
}

async fn run(scenario: &Scenario) -> agentdp_network_tests::Result<RunResult> {
    match scenario.mode {
        ScenarioMode::DirectHttp => run_http(scenario).await,
        ScenarioMode::DirectHttps | ScenarioMode::MediatedHttps => run_https(scenario).await,
    }
}

async fn run_http(scenario: &Scenario) -> agentdp_network_tests::Result<RunResult> {
    let mut dataplane = AsyncDataplane::start_with_http_server(scenario.response_body_bytes).await?;
    let request = http_get_request("/agent-workflow", "allowed.test", scenario.request_body_bytes);
    let mut latencies = Vec::with_capacity(scenario.iterations);

    for _ in 0..scenario.iterations.clamp(1, 8) {
        let response = dataplane.persistent_http1_roundtrip(black_box(&request)).await?;
        black_box(response);
    }

    let before = ALLOCATOR.begin_report();
    let started = Instant::now();
    for _ in 0..scenario.iterations {
        let operation_started = Instant::now();
        let response = dataplane.persistent_http1_roundtrip(black_box(&request)).await?;
        latencies.push(operation_started.elapsed());
        black_box(response);
    }
    let elapsed = started.elapsed();
    let allocations = ALLOCATOR.report_since(before);
    dataplane.shutdown().await?;
    Ok(RunResult::new(scenario, request.len(), elapsed, latencies, allocations))
}

async fn run_https(scenario: &Scenario) -> agentdp_network_tests::Result<RunResult> {
    let mut harness = match scenario.mode {
        ScenarioMode::DirectHttp => unreachable!("plain HTTP is handled by run_http"),
        ScenarioMode::DirectHttps => {
            AgentWorkflowHarness::start_direct_https_http1(scenario.response_body_bytes).await?
        }
        ScenarioMode::MediatedHttps => AgentWorkflowHarness::start_https_http1(scenario.response_body_bytes).await?,
    };
    let request = agent_https_request(harness.host(), scenario.request_body_bytes);
    let mut latencies = Vec::with_capacity(scenario.iterations);

    for _ in 0..scenario.iterations.clamp(1, 8) {
        let response = harness.https_http1_roundtrip(black_box(&request)).await?;
        black_box(response);
    }

    let before = ALLOCATOR.begin_report();
    let started = Instant::now();
    for _ in 0..scenario.iterations {
        let operation_started = Instant::now();
        let response = harness.https_http1_roundtrip(black_box(&request)).await?;
        latencies.push(operation_started.elapsed());
        black_box(response);
    }
    let elapsed = started.elapsed();
    let allocations = ALLOCATOR.report_since(before);
    harness.shutdown().await?;
    Ok(RunResult::new(scenario, request.len(), elapsed, latencies, allocations))
}

struct RunResult {
    name: &'static str,
    iterations: usize,
    request_body_bytes: usize,
    response_body_bytes: usize,
    elapsed: Duration,
    p50: Duration,
    p95: Duration,
    allocations: AllocationReport,
    bytes_per_operation: usize,
}

impl RunResult {
    fn new(
        scenario: &Scenario,
        request_bytes: usize,
        elapsed: Duration,
        mut latencies: Vec<Duration>,
        allocations: AllocationReport,
    ) -> Self {
        latencies.sort_unstable();
        Self {
            name: scenario.name,
            iterations: scenario.iterations,
            request_body_bytes: scenario.request_body_bytes,
            response_body_bytes: scenario.response_body_bytes,
            elapsed,
            p50: percentile(&latencies, 50),
            p95: percentile(&latencies, 95),
            allocations,
            bytes_per_operation: request_bytes
                + agentdp_network_tests::agent_https_response(scenario.response_body_bytes).len(),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn print(&self) {
        let iterations = self.iterations as f64;
        let total_bytes = self.iterations.saturating_mul(self.bytes_per_operation) as f64;
        let seconds = self.elapsed.as_secs_f64();
        println!(
            "{:<25} {:>8} {:>10} {:>10} {:>10} {:>10.1} {:>10.2} {:>10} {:>10} {:>10} {:>12}",
            self.name,
            self.iterations,
            format_bytes(self.request_body_bytes),
            format_bytes(self.response_body_bytes),
            format_duration(self.elapsed),
            iterations / seconds,
            total_bytes / seconds / 1024.0 / 1024.0,
            format_duration(self.p50),
            format_duration(self.p95),
            self.allocations.allocation_calls(),
            self.allocations.allocated_bytes(),
        );
    }
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let index = sorted
        .len()
        .saturating_mul(percentile)
        .saturating_sub(1)
        .checked_div(100)
        .unwrap_or(0)
        .min(sorted.len().saturating_sub(1));
    sorted.get(index).copied().unwrap_or_default()
}

fn format_duration(duration: Duration) -> String {
    if duration < Duration::from_millis(1) {
        format!("{}us", duration.as_micros())
    } else if duration < Duration::from_secs(1) {
        format!("{:.2}ms", duration.as_secs_f64() * 1000.0)
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}

fn format_bytes(bytes: usize) -> String {
    if bytes < 16 * 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{}KiB", bytes / 1024)
    } else {
        format!("{}MiB", bytes / 1024 / 1024)
    }
}
