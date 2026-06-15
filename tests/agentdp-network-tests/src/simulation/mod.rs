#![forbid(unsafe_code)]

mod case_support;
mod checkers;
mod fixtures;
mod gateway;
mod gateway_case;
mod guest_link;
mod http;
mod http_case;
mod packet_scheduler;
pub mod packets;
mod protocol;
mod randomized;
mod raw_frame_guest;
mod report;
pub mod simulator;
mod smoltcp_guest;
mod tcp;
mod tcp_case;
mod tls;
mod tls_case;
mod udp;
mod workloads;

use std::alloc::System;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use agentdp_network::{
    InstanceNetworkConfig, InstanceNetworkSpec, InstanceNetworkStatus,
    test_support::simulation::{RunningSim as AgentdpRunningSim, SimulationUpstreams},
};
use agentdp_rand::Seed;
use agentdp_test_support::allocation::ReportingAllocator;

#[global_allocator]
static ALLOCATOR: ReportingAllocator<System> = ReportingAllocator::new(System);

pub use self::guest_link::{GuestLink, GuestLinkConfig, LinkDirection, LinkFault, LinkTraceEvent, LinkTraceEventKind};
pub use self::raw_frame_guest::RawFrameGuest;
pub use self::report::{ScenarioReport, Transcript, TranscriptRole};
pub use self::simulator::{DriveBudget, DriveGuestProgress, QuiescenceReport, Simulator};
pub use self::smoltcp_guest::{SmolTcpGuest, TcpHandle};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Error {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn from_display(context: &str, error: impl Display) -> Self {
        Self::new(format!("{context}: {error}"))
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

pub trait NetworkUnderTest {
    type Running: RunningNetwork;

    /// # Errors
    ///
    /// Returns an error when the network implementation cannot start the configured scenario.
    fn start(config: ScenarioNetworkConfig, guest_link: GuestLink) -> Result<Self::Running>;
}

pub trait RunningNetwork {
    fn status(&self) -> InstanceNetworkStatus;

    /// # Errors
    ///
    /// Returns an error when the running network does not stop cleanly.
    fn stop(self) -> Result<StopReport>;
}

pub trait SteppedNetwork: RunningNetwork {
    fn step(&mut self);
    fn advance_time(&mut self, duration: Duration);
    fn simulated_time(&self) -> Duration;
}

#[derive(Debug)]
pub struct ScenarioNetworkConfig {
    pub seed: Seed,
    pub network: InstanceNetworkConfig,
    pub upstreams: SimulationUpstreams,
}

#[derive(Debug, Clone)]
pub struct StopReport {
    pub final_status: InstanceNetworkStatus,
    pub network_events: Vec<agentdp_network::NetworkEventEnvelope>,
}

#[derive(Debug, Clone, Copy)]
pub struct AgentdpNetworkSim;

impl NetworkUnderTest for AgentdpNetworkSim {
    type Running = AgentdpRunningSim<GuestLink>;

    fn start(config: ScenarioNetworkConfig, guest_link: GuestLink) -> Result<Self::Running> {
        AgentdpRunningSim::start(
            InstanceNetworkSpec {
                label: format!("network-simulation-{}", config.seed),
                config: config.network,
                reconnect_delay: Duration::from_millis(10),
                write_timeout: Duration::from_secs(2),
            },
            guest_link,
            config.upstreams,
        )
        .map_err(|error| Error::from_display("start simulated agentdp network", error))
    }
}

impl RunningNetwork for AgentdpRunningSim<GuestLink> {
    fn status(&self) -> InstanceNetworkStatus {
        self.status()
    }

    fn stop(self) -> Result<StopReport> {
        let (final_status, network_events) = self
            .stop()
            .map_err(|error| Error::from_display("stop simulated agentdp network", error))?;
        Ok(StopReport {
            final_status,
            network_events,
        })
    }
}

impl SteppedNetwork for AgentdpRunningSim<GuestLink> {
    fn step(&mut self) {
        self.drive_once();
    }

    fn advance_time(&mut self, duration: Duration) {
        self.advance_clock(duration);
    }

    fn simulated_time(&self) -> Duration {
        self.elapsed_time()
    }
}
