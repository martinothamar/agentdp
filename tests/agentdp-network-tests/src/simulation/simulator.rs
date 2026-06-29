use std::collections::VecDeque;
use std::fmt::Write as _;
use std::time::Duration;

use agentdp_rand::{DeterministicRng, Seed};

use super::{Error, GuestLink, GuestLinkConfig, LinkTraceEvent, Result, SmolTcpGuest, SteppedNetwork};

#[derive(Debug, Clone)]
pub struct Simulator {
    seed: Seed,
    rng: DeterministicRng,
    now: Duration,
    scheduled: VecDeque<ScheduledAction>,
    trace: Vec<SimulatorTraceEvent>,
    budget: OperationBudget,
}

impl Simulator {
    #[must_use]
    pub fn new(seed: Seed) -> Self {
        Self {
            seed,
            rng: DeterministicRng::from_seed(seed),
            now: Duration::ZERO,
            scheduled: VecDeque::new(),
            trace: Vec::new(),
            budget: OperationBudget::default(),
        }
    }

    #[must_use]
    pub const fn seed(&self) -> Seed {
        self.seed
    }

    pub const fn next_u64(&mut self) -> u64 {
        self.budget.spend();
        self.rng.next_u64()
    }

    #[must_use]
    pub const fn now(&self) -> Duration {
        self.now
    }

    /// # Errors
    ///
    /// Returns an error when the guest link wake source cannot be created.
    pub fn guest_link(&mut self) -> Result<GuestLink> {
        self.budget.spend();
        GuestLinkConfig::default().open(self.seed.derive("guest-link"))
    }

    /// # Errors
    ///
    /// Returns an error when the guest link wake source cannot be created.
    pub fn guest_link_with(&mut self, config: GuestLinkConfig) -> Result<GuestLink> {
        self.budget.spend();
        config.open(self.seed.derive("guest-link"))
    }

    pub fn schedule_after(&mut self, delay: Duration, label: impl Into<String>) {
        self.budget.spend();
        let action = ScheduledAction {
            at: self.now.saturating_add(delay),
            label: label.into(),
        };
        let insert_at = self
            .scheduled
            .iter()
            .position(|scheduled| scheduled.at > action.at)
            .unwrap_or(self.scheduled.len());
        self.scheduled.insert(insert_at, action);
    }

    #[must_use]
    pub fn advance_to_next_action(&mut self) -> Option<String> {
        self.budget.spend();
        let action = self.scheduled.pop_front()?;
        self.now = action.at;
        self.trace.push(SimulatorTraceEvent::ScheduledAction {
            at: self.now,
            label: action.label.clone(),
        });
        Some(action.label)
    }

    #[must_use]
    pub fn quiescence<N>(&self, running: &N, guest_link: &GuestLink) -> QuiescenceReport
    where
        N: super::RunningNetwork,
    {
        QuiescenceReport {
            virtual_time: self.now,
            pending_actions: self.scheduled.len(),
            pending_reactor_ready: running.pending_reactor_ready(),
            pending_guest_frames: guest_link.pending_to_network_frames(),
            pending_network_frames: guest_link.pending_from_network_frames(),
            exhausted_budget: self.budget.is_exhausted(),
        }
    }

    #[must_use]
    pub fn trace(&self) -> &[SimulatorTraceEvent] {
        &self.trace
    }

    /// Drives the network until the guest receives one frame from the network.
    ///
    /// # Errors
    ///
    /// Returns an error when the drive budget is exhausted before a frame is produced.
    pub fn drive_until_network_frame<N>(
        &mut self,
        running: &mut N,
        guest_link: &GuestLink,
        label: &str,
        budget: DriveBudget,
    ) -> Result<Vec<u8>>
    where
        N: SteppedNetwork,
    {
        for step in 0..=budget.max_steps {
            if let Some(frame) = guest_link.try_recv_from_network() {
                self.record_drive(label, DriveOutcome::Reached, step, running.simulated_time());
                return Ok(frame);
            }
            if step == budget.max_steps {
                break;
            }
            self.drive_step(running, guest_link, budget.step_time);
        }

        self.record_drive(
            label,
            DriveOutcome::Exhausted,
            budget.max_steps,
            running.simulated_time(),
        );
        Err(self.drive_error(
            label,
            budget.max_steps,
            running.simulated_time(),
            &running.status(),
            &running.debug_snapshot(),
            &self.quiescence(running, guest_link),
            &guest_link.trace(),
        ))
    }

    /// Drives the network until queues are empty and public status is stable across one extra step.
    ///
    /// # Errors
    ///
    /// Returns an error when the drive budget is exhausted before quiescence is observed.
    pub fn drive_until_quiescent<N>(
        &mut self,
        running: &mut N,
        guest_link: &GuestLink,
        label: &str,
        budget: DriveBudget,
    ) -> Result<QuiescenceReport>
    where
        N: SteppedNetwork,
    {
        let mut stable_status = None;
        for step in 0..=budget.max_steps {
            let quiescence = self.quiescence(running, guest_link);
            let status = running.status();
            if quiescence.is_quiescent() {
                if stable_status.as_ref() == Some(&status) {
                    self.record_drive(label, DriveOutcome::Reached, step, running.simulated_time());
                    return Ok(quiescence);
                }
                stable_status = Some(status);
            } else {
                stable_status = None;
            }
            if step == budget.max_steps {
                break;
            }
            self.drive_step(running, guest_link, budget.step_time);
        }

        self.record_drive(
            label,
            DriveOutcome::Exhausted,
            budget.max_steps,
            running.simulated_time(),
        );
        Err(self.drive_error(
            label,
            budget.max_steps,
            running.simulated_time(),
            &running.status(),
            &running.debug_snapshot(),
            &self.quiescence(running, guest_link),
            &guest_link.trace(),
        ))
    }

    /// Drives a smoltcp guest and the network until queues are empty and public status is stable.
    ///
    /// # Errors
    ///
    /// Returns an error when the guest pump fails or the drive budget is exhausted before quiescence is observed.
    pub fn drive_guest_network_until_quiescent<N>(
        &mut self,
        guest: &mut SmolTcpGuest,
        running: &mut N,
        guest_link: &GuestLink,
        label: &str,
        budget: DriveBudget,
    ) -> Result<QuiescenceReport>
    where
        N: SteppedNetwork,
    {
        let mut stable_status = None;
        for step in 0..=budget.max_steps {
            let quiescence = self.quiescence(running, guest_link);
            let status = running.status();
            if quiescence.is_quiescent() {
                if stable_status.as_ref() == Some(&status) {
                    self.record_drive(label, DriveOutcome::Reached, step, running.simulated_time());
                    return Ok(quiescence);
                }
                stable_status = Some(status);
            } else {
                stable_status = None;
            }
            if step == budget.max_steps {
                break;
            }
            self.budget.spend();
            guest.pump_with_step(running, budget.step_time)?;
            self.now = running.simulated_time();
        }

        self.record_drive(
            label,
            DriveOutcome::Exhausted,
            budget.max_steps,
            running.simulated_time(),
        );
        Err(self.drive_error(
            label,
            budget.max_steps,
            running.simulated_time(),
            &running.status(),
            &running.debug_snapshot(),
            &self.quiescence(running, guest_link),
            &guest_link.trace(),
        ))
    }

    /// Drives a smoltcp guest and the network until `done` observes the desired protocol state.
    ///
    /// # Errors
    ///
    /// Returns an error when the protocol driver fails or the drive budget is exhausted.
    pub fn drive_guest_until<N>(
        &mut self,
        guest: &mut SmolTcpGuest,
        running: &mut N,
        label: &str,
        budget: DriveBudget,
        done: impl FnMut(&mut SmolTcpGuest, &mut N) -> Result<bool>,
    ) -> Result<()>
    where
        N: SteppedNetwork,
    {
        self.drive_guest_until_with_diagnostics(guest, running, label, budget, done, |_output| {})
    }

    /// Drives a smoltcp guest and the network until `done` observes the desired protocol state.
    ///
    /// # Errors
    ///
    /// Returns an error with protocol diagnostics when the drive budget is exhausted.
    pub fn drive_guest_until_with_diagnostics<N>(
        &mut self,
        guest: &mut SmolTcpGuest,
        running: &mut N,
        label: &str,
        budget: DriveBudget,
        mut done: impl FnMut(&mut SmolTcpGuest, &mut N) -> Result<bool>,
        mut diagnostics: impl FnMut(&mut String),
    ) -> Result<()>
    where
        N: SteppedNetwork,
    {
        for step in 0..=budget.max_steps {
            if done(guest, running)? {
                self.record_drive(label, DriveOutcome::Reached, step, running.simulated_time());
                return Ok(());
            }
            if step == budget.max_steps {
                break;
            }
            self.budget.spend();
            guest.pump_with_step(running, budget.step_time)?;
            self.now = running.simulated_time();
        }

        let mut message = format!(
            "drive {label:?} exhausted after {} guest steps at {:?}; seed={}; status={:?}; simulator_trace={:?}",
            budget.max_steps,
            running.simulated_time(),
            self.seed,
            running.status(),
            self.trace
        );
        let _ = writeln!(message);
        let _ = writeln!(message, "drive_diagnostics:");
        let _ = writeln!(message, "  virtual_time: {:?}", running.simulated_time());
        let _ = writeln!(message, "  step_time: {:?}", budget.step_time);
        let _ = writeln!(message, "  max_steps: {}", budget.max_steps);
        let _ = writeln!(
            message,
            "  pending_guest_to_network_frames: {}",
            guest.pending_to_network_frames()
        );
        let _ = writeln!(
            message,
            "  pending_network_to_guest_frames: {}",
            guest.pending_from_network_frames()
        );
        let _ = writeln!(
            message,
            "  guest_tcp_buffer_bytes: {}",
            SmolTcpGuest::tcp_buffer_bytes()
        );
        diagnostics(&mut message);
        self.record_drive(
            label,
            DriveOutcome::Exhausted,
            budget.max_steps,
            running.simulated_time(),
        );
        Err(Error::new(message))
    }

    /// Drives a smoltcp guest and the network until `done` observes the desired protocol state.
    ///
    /// The `progress` closure must return a monotonic black-box protocol progress marker. When it stops
    /// changing for a substantial number of steps, the drive fails with the same diagnostics used for budget
    /// exhaustion. That turns silent dataplane stalls into smaller, replayable failures.
    ///
    /// # Errors
    ///
    /// Returns an error when the protocol driver fails, the progress marker stalls, or the drive budget is exhausted.
    pub fn drive_guest_until_with_progress<N>(
        &mut self,
        guest: &mut SmolTcpGuest,
        running: &mut N,
        config: DriveGuestProgress<'_>,
        mut done: impl FnMut(&mut SmolTcpGuest, &mut N) -> Result<bool>,
        mut progress: impl FnMut() -> usize,
        mut diagnostics: impl FnMut(&mut String),
    ) -> Result<()>
    where
        N: SteppedNetwork,
    {
        let max_stalled_steps = config.budget.max_steps.clamp(1, 4096);
        let mut last_progress = drive_progress_marker(guest, progress());
        let mut stalled_steps = 0_usize;
        for step in 0..=config.budget.max_steps {
            if done(guest, running)? {
                self.record_drive(config.label, DriveOutcome::Reached, step, running.simulated_time());
                return Ok(());
            }
            let current_progress = drive_progress_marker(guest, progress());
            if current_progress == last_progress {
                stalled_steps = stalled_steps.saturating_add(1);
            } else {
                last_progress = current_progress;
                stalled_steps = 0;
            }
            if stalled_steps >= max_stalled_steps {
                self.record_drive(config.label, DriveOutcome::Stalled, step, running.simulated_time());
                return Err(self.progress_error(
                    ProgressStall {
                        label: config.label,
                        steps: step,
                        stalled_steps,
                        progress: last_progress,
                    },
                    running,
                    guest,
                    &mut diagnostics,
                ));
            }
            if step == config.budget.max_steps {
                break;
            }
            self.budget.spend();
            guest.pump_with_step(running, config.budget.step_time)?;
            self.now = running.simulated_time();
        }

        let mut message = format!(
            "drive {label:?} exhausted after {steps} guest steps at {at:?}; seed={seed}; status={status:?}; simulator_trace={trace:?}",
            label = config.label,
            steps = config.budget.max_steps,
            at = running.simulated_time(),
            seed = self.seed,
            status = running.status(),
            trace = self.trace
        );
        write_guest_drive_diagnostics(&mut message, running, guest);
        diagnostics(&mut message);
        self.record_drive(
            config.label,
            DriveOutcome::Exhausted,
            config.budget.max_steps,
            running.simulated_time(),
        );
        Err(Error::new(message))
    }

    fn progress_error<N>(
        &self,
        stall: ProgressStall<'_>,
        running: &N,
        guest: &SmolTcpGuest,
        diagnostics: &mut impl FnMut(&mut String),
    ) -> Error
    where
        N: SteppedNetwork,
    {
        let mut message = format!(
            "drive {label:?} stalled after {steps} guest steps at {at:?}; seed={seed}; status={status:?}; progress_marker={progress}; stalled_steps={stalled_steps}; simulator_trace={trace:?}",
            label = stall.label,
            steps = stall.steps,
            at = running.simulated_time(),
            seed = self.seed,
            status = running.status(),
            progress = stall.progress,
            stalled_steps = stall.stalled_steps,
            trace = self.trace
        );
        write_guest_drive_diagnostics(&mut message, running, guest);
        diagnostics(&mut message);
        Error::new(message)
    }

    fn drive_step<N>(&mut self, running: &mut N, guest_link: &GuestLink, step_time: Duration)
    where
        N: SteppedNetwork,
    {
        self.budget.spend();
        let _delivered = guest_link.deliver_due(running.simulated_time());
        running.step();
        running.advance_time(step_time);
        let _delivered = guest_link.deliver_due(running.simulated_time());
        self.now = running.simulated_time();
    }

    fn record_drive(&mut self, label: &str, outcome: DriveOutcome, steps: usize, at: Duration) {
        self.trace.push(SimulatorTraceEvent::Drive {
            at,
            label: label.to_owned(),
            outcome,
            steps,
        });
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "failure snapshots include each independently useful diagnostic stream"
    )]
    fn drive_error(
        &self,
        label: &str,
        steps: usize,
        at: Duration,
        status: &agentdp_network::InstanceNetworkStatus,
        debug: &str,
        quiescence: &QuiescenceReport,
        link_trace: &[LinkTraceEvent],
    ) -> Error {
        Error::new(format!(
            "drive {label:?} exhausted after {steps} steps at {at:?}; seed={}; status={status:?}; debug={debug}; quiescence={quiescence:?}; simulator_trace={:?}; link_trace={link_trace:?}",
            self.seed, self.trace
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuiescenceReport {
    pub virtual_time: Duration,
    pub pending_actions: usize,
    pub pending_reactor_ready: usize,
    pub pending_guest_frames: usize,
    pub pending_network_frames: usize,
    pub exhausted_budget: bool,
}

impl QuiescenceReport {
    #[must_use]
    pub const fn is_quiescent(&self) -> bool {
        // The exhausted flag is the simulator's global operation counter. It is
        // useful diagnostic pressure for long randomized runs, but local drive
        // calls are bounded by their own DriveBudget and must still be allowed
        // to observe an empty network after the global counter reaches zero.
        self.pending_actions == 0
            && self.pending_reactor_ready == 0
            && self.pending_guest_frames == 0
            && self.pending_network_frames == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriveBudget {
    pub max_steps: usize,
    pub step_time: Duration,
}

impl Default for DriveBudget {
    fn default() -> Self {
        Self {
            max_steps: 64,
            step_time: Duration::from_millis(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriveGuestProgress<'a> {
    pub label: &'a str,
    pub budget: DriveBudget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimulatorTraceEvent {
    ScheduledAction {
        at: Duration,
        label: String,
    },
    Drive {
        at: Duration,
        label: String,
        outcome: DriveOutcome,
        steps: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveOutcome {
    Reached,
    Exhausted,
    Stalled,
}

#[derive(Debug, Clone)]
struct ScheduledAction {
    at: Duration,
    label: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProgressStall<'a> {
    label: &'a str,
    steps: usize,
    stalled_steps: usize,
    progress: usize,
}

fn write_guest_drive_diagnostics<N>(message: &mut String, running: &N, guest: &SmolTcpGuest)
where
    N: SteppedNetwork,
{
    let _ = writeln!(message);
    let _ = writeln!(message, "drive_diagnostics:");
    let _ = writeln!(message, "  virtual_time: {:?}", running.simulated_time());
    let _ = writeln!(
        message,
        "  pending_guest_to_network_frames: {}",
        guest.pending_to_network_frames()
    );
    let _ = writeln!(
        message,
        "  pending_network_to_guest_frames: {}",
        guest.pending_from_network_frames()
    );
    let _ = writeln!(
        message,
        "  guest_tcp_buffer_bytes: {}",
        SmolTcpGuest::tcp_buffer_bytes()
    );
    let debug = running.debug_snapshot();
    if !debug.is_empty() {
        let _ = writeln!(message, "  network_debug: {debug}");
    }
}

fn drive_progress_marker(guest: &SmolTcpGuest, protocol_progress: usize) -> usize {
    protocol_progress.saturating_add(guest.progress_marker())
}

#[derive(Debug, Clone)]
struct OperationBudget {
    remaining: usize,
}

impl OperationBudget {
    const DEFAULT_OPERATIONS: usize = 1_000_000;

    const fn spend(&mut self) {
        self.remaining = self.remaining.saturating_sub(1);
    }

    const fn is_exhausted(&self) -> bool {
        self.remaining == 0
    }
}

impl Default for OperationBudget {
    fn default() -> Self {
        Self {
            remaining: Self::DEFAULT_OPERATIONS,
        }
    }
}
