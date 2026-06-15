use std::alloc::System;
use std::hint::black_box;
use std::time::{Duration, Instant};

use agentdp_network_tests::{AsyncDataplane, payload};
use agentdp_test_support::allocation::{AllocationReport, ReportingAllocator};
use tokio::runtime::Builder;

#[global_allocator]
static ALLOCATOR: ReportingAllocator<System> = ReportingAllocator::new(System);

const MIN_THROUGHPUT_BITS_PER_SECOND: f64 = 1_000_000_000.0;

struct Scenario {
    name: &'static str,
    iterations: usize,
    payload_bytes: usize,
    kind: ScenarioKind,
}

#[derive(Clone, Copy)]
enum ScenarioKind {
    UdpUpload,
    TcpUpload,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "udp_upload_1200B",
        iterations: 100_000,
        payload_bytes: 1200,
        kind: ScenarioKind::UdpUpload,
    },
    Scenario {
        name: "tcp_upload_1MiB",
        iterations: 512,
        payload_bytes: 1024 * 1024,
        kind: ScenarioKind::TcpUpload,
    },
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{:<20} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10} {:>8} {:>10} {:>12}",
        "scenario", "ops", "payload", "sent", "recv", "Gbps", "MiB/s", "loss", "allocs", "alloc B"
    );

    let runtime = Builder::new_current_thread().enable_all().build()?;
    for scenario in SCENARIOS {
        let result = runtime.block_on(run(scenario))?;
        result.print();
        result.assert_min_throughput();
    }
    Ok(())
}

async fn run(scenario: &Scenario) -> agentdp_network_tests::Result<RunResult> {
    let mut dataplane = start(scenario.kind).await?;
    let payload = payload(scenario.payload_bytes);
    let _warmup_bytes = upload(
        scenario.kind,
        &mut dataplane,
        black_box(&payload),
        warmup_iterations(scenario),
    )
    .await?;

    let allocations_before = ALLOCATOR.begin_report();
    let started = Instant::now();
    let received_bytes = upload(scenario.kind, &mut dataplane, black_box(&payload), scenario.iterations).await?;
    let elapsed = started.elapsed();
    let allocations = ALLOCATOR.report_since(allocations_before);

    dataplane.shutdown().await?;
    Ok(RunResult::new(scenario, elapsed, received_bytes, allocations))
}

async fn start(kind: ScenarioKind) -> agentdp_network_tests::Result<AsyncDataplane> {
    match kind {
        ScenarioKind::UdpUpload => AsyncDataplane::start_with_udp_sink().await,
        ScenarioKind::TcpUpload => AsyncDataplane::start_with_tcp_sink().await,
    }
}

async fn upload(
    kind: ScenarioKind,
    dataplane: &mut AsyncDataplane,
    payload: &[u8],
    iterations: usize,
) -> agentdp_network_tests::Result<usize> {
    match kind {
        ScenarioKind::UdpUpload => dataplane.established_udp_upload(payload, iterations).await,
        ScenarioKind::TcpUpload => {
            dataplane.established_tcp_upload(payload, iterations).await?;
            Ok(payload.len().saturating_mul(iterations))
        }
    }
}

fn warmup_iterations(scenario: &Scenario) -> usize {
    match scenario.kind {
        ScenarioKind::UdpUpload => scenario.iterations / 10,
        ScenarioKind::TcpUpload => scenario.iterations.clamp(1, 16),
    }
}

struct RunResult {
    name: &'static str,
    iterations: usize,
    payload_bytes: usize,
    elapsed: Duration,
    received_bytes: usize,
    allocations: AllocationReport,
}

impl RunResult {
    const fn new(scenario: &Scenario, elapsed: Duration, received_bytes: usize, allocations: AllocationReport) -> Self {
        Self {
            name: scenario.name,
            iterations: scenario.iterations,
            payload_bytes: scenario.payload_bytes,
            elapsed,
            received_bytes,
            allocations,
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn bytes_per_second(&self) -> f64 {
        self.received_bytes as f64 / self.elapsed.as_secs_f64()
    }

    fn bits_per_second(&self) -> f64 {
        self.bytes_per_second() * 8.0
    }

    fn assert_min_throughput(&self) {
        let bits_per_second = self.bits_per_second();
        assert!(
            bits_per_second >= MIN_THROUGHPUT_BITS_PER_SECOND,
            "{} throughput below 1 Gbps: {}",
            self.name,
            format_bits_per_second(bits_per_second),
        );
    }

    fn print(&self) {
        let bytes_per_second = self.bytes_per_second();
        let sent_bytes = self.sent_bytes();
        println!(
            "{:<20} {:>10} {:>10} {:>10} {:>10} {:>12.2} {:>10.2} {:>7.2}% {:>10} {:>12}",
            self.name,
            self.iterations,
            format_bytes(self.payload_bytes),
            format_bytes(sent_bytes),
            format_bytes(self.received_bytes),
            self.bits_per_second() / 1_000_000_000.0,
            bytes_per_second / 1024.0 / 1024.0,
            self.loss_percent(),
            self.allocations.allocation_calls(),
            self.allocations.allocated_bytes()
        );
    }

    const fn sent_bytes(&self) -> usize {
        self.iterations.saturating_mul(self.payload_bytes)
    }

    #[allow(clippy::cast_precision_loss)]
    fn loss_percent(&self) -> f64 {
        let sent = self.sent_bytes();
        if sent == 0 {
            return 0.0;
        }
        sent.saturating_sub(self.received_bytes) as f64 * 100.0 / sent as f64
    }
}

fn format_bits_per_second(bits_per_second: f64) -> String {
    if bits_per_second < 1_000_000_000.0 {
        format!("{:.1}Mbps", bits_per_second / 1_000_000.0)
    } else {
        format!("{:.2}Gbps", bits_per_second / 1_000_000_000.0)
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
