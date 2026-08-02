mod executor;
mod scheduler;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use agentdp_protocol::server_guest::{
    BootstrapFailed, BootstrapFinished, BootstrapLifecycleStatus, BootstrapPlan, BootstrapStatusReport, BootstrapStep,
    BootstrapStepFinished, BootstrapStepPhase, BootstrapStepStarted, BootstrapStepStatus, GuestMessageKind,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::fs;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use self::executor::{StepExecutor, StepOutput};
use self::scheduler::BootstrapScheduler;
use crate::{Error, Result};

const BOOTSTRAP_TASK_EVENT_CAPACITY: usize = 64;

#[derive(Debug, Clone)]
pub(super) struct BootstrapExecutor {
    plan: BootstrapPlan,
    plan_id: String,
    plan_hash: String,
    state_path: PathBuf,
    script_root: PathBuf,
}

impl BootstrapExecutor {
    pub(super) fn new(plan: BootstrapPlan, plan_id: String, state_path: PathBuf, script_root: PathBuf) -> Self {
        let plan_hash = plan_hash(&plan);
        Self {
            plan,
            plan_id,
            plan_hash,
            state_path,
            script_root,
        }
    }

    pub(super) fn plan_hash(&self) -> &str {
        &self.plan_hash
    }

    pub(super) async fn run(&self, sink: &mut impl BootstrapEventSink) -> Result<()> {
        let scheduler = BootstrapScheduler::new(&self.plan)?;
        let mut state = BootstrapState::load(&self.state_path, &self.plan_hash).await?;
        self.run_state(sink, &mut state, scheduler).await
    }

    pub(super) async fn prepare_retry(&self, attempt_epoch: u64) -> Result<bool> {
        let mut state = BootstrapState::load(&self.state_path, &self.plan_hash).await?;
        if attempt_epoch == state.attempt_epoch {
            return Ok(false);
        }
        let expected_epoch = state.attempt_epoch.saturating_add(1);
        if attempt_epoch != expected_epoch {
            return Err(Error::Message(format!(
                "bootstrap retry epoch {attempt_epoch} does not follow current epoch {}",
                state.attempt_epoch
            )));
        }
        if !state.prepare_retry(attempt_epoch) {
            return Err(Error::Message(
                "bootstrap retry requested without a failed or interrupted step".to_owned(),
            ));
        }
        state.save(&self.state_path).await?;
        Ok(true)
    }

    async fn run_state(
        &self,
        sink: &mut impl BootstrapEventSink,
        state: &mut BootstrapState,
        scheduler: BootstrapScheduler,
    ) -> Result<()> {
        self.emit_status(sink, state, BootstrapLifecycleStatus::Pending, None)
            .await?;

        if self.report_existing_failure(sink, state).await? {
            return Ok(());
        }
        if self.mark_interrupted_step(sink, state).await? {
            return Ok(());
        }

        match self.run_steps(sink, state, scheduler).await? {
            BootstrapRunStatus::Completed => {}
            BootstrapRunStatus::Failed(failed) => {
                self.emit_status(sink, state, BootstrapLifecycleStatus::Failed, None)
                    .await?;
                sink.emit(GuestMessageKind::BootstrapFailed(failed)).await?;
                return Ok(());
            }
        }

        self.emit_status(sink, state, BootstrapLifecycleStatus::Passed, None)
            .await?;
        sink.emit(GuestMessageKind::BootstrapFinished(BootstrapFinished {
            plan_hash: self.plan_hash.clone(),
            attempt_epoch: state.attempt_epoch,
        }))
        .await?;
        Ok(())
    }

    async fn report_existing_failure(
        &self,
        sink: &mut impl BootstrapEventSink,
        state: &BootstrapState,
    ) -> Result<bool> {
        let Some(failed) = state.failed_step_record() else {
            return Ok(false);
        };
        self.emit_status(sink, state, BootstrapLifecycleStatus::Failed, None)
            .await?;
        sink.emit(GuestMessageKind::BootstrapFailed(
            failed.bootstrap_failed_event(state.attempt_epoch),
        ))
        .await?;
        Ok(true)
    }

    async fn mark_interrupted_step(
        &self,
        sink: &mut impl BootstrapEventSink,
        state: &mut BootstrapState,
    ) -> Result<bool> {
        let attempt_epoch = state.attempt_epoch;
        let Some(interrupted) = state.interrupted_step_mut() else {
            return Ok(false);
        };
        interrupted.status = StepRecordStatus::Failed;
        interrupted.exit_status = Some(-1);
        interrupted.message = Some("bootstrap step was interrupted before completion".to_owned());
        let failed = interrupted.bootstrap_failed_event(attempt_epoch);
        state.save(&self.state_path).await?;
        self.emit_status(sink, state, BootstrapLifecycleStatus::Failed, None)
            .await?;
        sink.emit(GuestMessageKind::BootstrapFailed(failed)).await?;
        Ok(true)
    }

    async fn run_steps(
        &self,
        sink: &mut impl BootstrapEventSink,
        state: &mut BootstrapState,
        mut scheduler: BootstrapScheduler,
    ) -> Result<BootstrapRunStatus> {
        let (event_tx, mut event_rx) = mpsc::channel(BOOTSTRAP_TASK_EVENT_CAPACITY);
        let mut tasks = JoinSet::new();
        let mut failure = None;
        let executor = StepExecutor::new(self.script_root.clone(), self.plan.user.clone(), self.plan.home.clone());

        loop {
            while scheduler.can_start() {
                let Some(step) = scheduler.next_ready(state) else {
                    break;
                };
                let attempt = state.start_step(&step);
                state.save(&self.state_path).await?;
                self.emit_status(sink, state, BootstrapLifecycleStatus::Running, Some(&step))
                    .await?;
                sink.emit(GuestMessageKind::BootstrapStepStarted(BootstrapStepStarted {
                    step: step.id.clone(),
                    label: step.label.clone(),
                    phase: step.phase,
                    attempt,
                }))
                .await?;

                scheduler.mark_running(&step);
                let task_events = event_tx.clone();
                let executor = executor.clone();
                tasks.spawn(async move {
                    let mut sink = ChannelEventSink {
                        events: task_events.clone(),
                    };
                    let output = Box::pin(executor.run(&step, &mut sink)).await;
                    let _ = task_events.send(BootstrapTaskEvent::Finished(step, output)).await;
                });
            }

            if state.all_steps_passed(scheduler.steps()) {
                drain_task_messages(sink, &mut event_rx).await?;
                return Ok(BootstrapRunStatus::Completed);
            }
            if scheduler.is_drained() {
                if scheduler.is_stopping() {
                    drain_task_messages(sink, &mut event_rx).await?;
                    let failure = failure.ok_or_else(|| {
                        Error::Message("bootstrap scheduler stopped without a failed step".to_owned())
                    })?;
                    return Ok(BootstrapRunStatus::Failed(failure));
                }
                return Err(Error::Message(
                    "bootstrap step scheduler found no runnable steps".to_owned(),
                ));
            }

            tokio::select! {
                Some(event) = event_rx.recv() => {
                    if let Some(failed) = self.handle_task_event(sink, state, &mut scheduler, event).await? {
                        failure.get_or_insert(failed);
                    }
                }
                Some(joined) = tasks.join_next() => {
                    joined.map_err(|source| {
                        Error::Message(format!("bootstrap step task failed: {source}"))
                    })?;
                }
            }
        }
    }

    async fn handle_task_event(
        &self,
        sink: &mut impl BootstrapEventSink,
        state: &mut BootstrapState,
        scheduler: &mut BootstrapScheduler,
        event: BootstrapTaskEvent,
    ) -> Result<Option<BootstrapFailed>> {
        match event {
            BootstrapTaskEvent::Message(event) => {
                sink.emit(event).await?;
                Ok(None)
            }
            BootstrapTaskEvent::Finished(step, output) => {
                scheduler.mark_finished(&step);
                match output? {
                    output if output.exit_status == 0 && !output.timed_out => {
                        self.finish_passed_step(sink, state, &step, &output).await?;
                        Ok(None)
                    }
                    output => {
                        let failed = self.finish_failed_step(sink, state, &step, output).await?;
                        scheduler.stop();
                        Ok(Some(failed))
                    }
                }
            }
        }
    }

    async fn finish_passed_step(
        &self,
        sink: &mut impl BootstrapEventSink,
        state: &mut BootstrapState,
        step: &BootstrapStep,
        output: &StepOutput,
    ) -> Result<()> {
        state.mark_passed(step, output.exit_status, output.duration_ms);
        state.save(&self.state_path).await?;
        sink.emit(GuestMessageKind::BootstrapStepFinished(BootstrapStepFinished {
            step: step.id.clone(),
            status: BootstrapStepStatus::Passed,
            exit_status: output.exit_status,
            duration_ms: output.duration_ms,
        }))
        .await?;
        let lifecycle = if state.failed_step_record().is_some() {
            BootstrapLifecycleStatus::Failed
        } else {
            BootstrapLifecycleStatus::Running
        };
        self.emit_status(sink, state, lifecycle, None).await?;
        Ok(())
    }

    async fn finish_failed_step(
        &self,
        sink: &mut impl BootstrapEventSink,
        state: &mut BootstrapState,
        step: &BootstrapStep,
        output: StepOutput,
    ) -> Result<BootstrapFailed> {
        let message = if output.timed_out {
            format!(
                "bootstrap step {} timed out after {} seconds",
                step.id, step.timeout_seconds
            )
        } else {
            format!("bootstrap step {} exited with status {}", step.id, output.exit_status)
        };
        let failed = state.mark_failed(
            step,
            output.exit_status,
            output.duration_ms,
            message,
            output.stdout_tail,
            output.stderr_tail,
        );
        state.save(&self.state_path).await?;
        self.emit_status(sink, state, BootstrapLifecycleStatus::Failed, None)
            .await?;
        Ok(failed)
    }

    async fn emit_status(
        &self,
        sink: &mut impl BootstrapEventSink,
        state: &BootstrapState,
        status: BootstrapLifecycleStatus,
        current_step: Option<&BootstrapStep>,
    ) -> Result<()> {
        let completed_steps = self.completed_steps(state);
        let failed_step = state.failed_step();
        let phase = current_step.map_or_else(|| self.status_phase(state), |step| step.phase);
        sink.emit(GuestMessageKind::BootstrapStatus(BootstrapStatusReport {
            plan_id: self.plan_id.clone(),
            plan_hash: self.plan_hash.clone(),
            attempt_epoch: state.attempt_epoch,
            phase,
            status,
            current_step: current_step.map(|step| step.id.clone()),
            completed_steps,
            failed_step,
            pending_steps: self.pending_steps(state),
        }))
        .await
    }

    fn pending_steps(&self, state: &BootstrapState) -> Vec<String> {
        self.plan
            .steps
            .iter()
            .filter(|step| !state.step_passed(&step.id))
            .map(|step| step.id.clone())
            .collect()
    }

    fn completed_steps(&self, state: &BootstrapState) -> Vec<String> {
        self.plan
            .steps
            .iter()
            .filter(|step| state.step_passed(&step.id))
            .map(|step| step.id.clone())
            .collect()
    }

    fn status_phase(&self, state: &BootstrapState) -> BootstrapStepPhase {
        state
            .failed_step_phase()
            .or_else(|| {
                self.plan
                    .steps
                    .iter()
                    .find(|step| !state.step_passed(&step.id))
                    .map(|step| step.phase)
            })
            .unwrap_or_else(|| {
                if self
                    .plan
                    .steps
                    .iter()
                    .any(|step| step.phase == BootstrapStepPhase::User)
                {
                    BootstrapStepPhase::User
                } else {
                    BootstrapStepPhase::System
                }
            })
    }
}

async fn drain_task_messages(
    sink: &mut impl BootstrapEventSink,
    event_rx: &mut mpsc::Receiver<BootstrapTaskEvent>,
) -> Result<()> {
    while let Ok(event) = event_rx.try_recv() {
        if let BootstrapTaskEvent::Message(event) = event {
            sink.emit(event).await?;
        }
    }
    Ok(())
}

pub(super) trait BootstrapEventSink {
    async fn emit(&mut self, event: GuestMessageKind) -> Result<()>;
}

impl BootstrapEventSink for Vec<GuestMessageKind> {
    async fn emit(&mut self, event: GuestMessageKind) -> Result<()> {
        self.push(event);
        Ok(())
    }
}

impl BootstrapEventSink for mpsc::Sender<GuestMessageKind> {
    async fn emit(&mut self, event: GuestMessageKind) -> Result<()> {
        self.send(event)
            .await
            .map_err(|_| Error::Message("bootstrap control driver stopped".to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BootstrapRunStatus {
    Completed,
    Failed(BootstrapFailed),
}

struct ChannelEventSink {
    events: mpsc::Sender<BootstrapTaskEvent>,
}

impl BootstrapEventSink for ChannelEventSink {
    async fn emit(&mut self, event: GuestMessageKind) -> Result<()> {
        match self.events.try_send(BootstrapTaskEvent::Message(event)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(BootstrapTaskEvent::Message(GuestMessageKind::BootstrapOutput(_)))) => {
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(Error::Message("bootstrap event queue is full".to_owned())),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(Error::Message("bootstrap event receiver closed".to_owned()))
            }
        }
    }
}

enum BootstrapTaskEvent {
    Message(GuestMessageKind),
    Finished(BootstrapStep, Result<StepOutput>),
}

#[derive(Debug, Deserialize, Serialize)]
struct BootstrapState {
    plan_hash: String,
    attempt_epoch: u64,
    steps: StepRecords,
}

type StepRecords = BTreeMap<String, StepRecord>;

impl BootstrapState {
    async fn load(path: &Path, plan_hash: &str) -> Result<Self> {
        let contents = match fs::read_to_string(path).await {
            Ok(contents) => contents,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self {
                    plan_hash: plan_hash.to_owned(),
                    attempt_epoch: 0,
                    steps: StepRecords::default(),
                });
            }
            Err(source) => return Err(source.into()),
        };
        let mut state: Self = serde_json::from_str(&contents).map_err(|source| {
            Error::Message(format!("failed to parse bootstrap state {}: {source}", path.display()))
        })?;
        if state.plan_hash != plan_hash {
            state = Self {
                plan_hash: plan_hash.to_owned(),
                attempt_epoch: 0,
                steps: StepRecords::default(),
            };
        }
        Ok(state)
    }

    async fn save(&self, path: &Path) -> Result<()> {
        let mut contents = serde_json::to_vec_pretty(self)?;
        contents.push(b'\n');
        agentdp_platform::fs::write_atomic(path, &contents, 0o600).await?;
        Ok(())
    }

    fn step_passed(&self, step: &str) -> bool {
        self.steps
            .get(step)
            .is_some_and(|record| record.status == StepRecordStatus::Passed)
    }

    fn all_steps_passed(&self, steps: &[BootstrapStep]) -> bool {
        steps.iter().all(|step| self.step_passed(&step.id))
    }

    fn failed_step(&self) -> Option<String> {
        self.failed_step_record().map(|record| record.id.clone())
    }

    fn failed_step_phase(&self) -> Option<BootstrapStepPhase> {
        self.failed_step_record().map(|record| record.phase)
    }

    fn failed_step_record(&self) -> Option<&StepRecord> {
        self.steps
            .values()
            .find(|record| record.status == StepRecordStatus::Failed)
    }

    fn interrupted_step_mut(&mut self) -> Option<&mut StepRecord> {
        self.steps
            .values_mut()
            .find(|record| record.status == StepRecordStatus::Running)
    }

    fn prepare_retry(&mut self, attempt_epoch: u64) -> bool {
        let mut found_failure = false;
        for record in self.steps.values_mut() {
            if matches!(record.status, StepRecordStatus::Running | StepRecordStatus::Failed) {
                found_failure = true;
                record.status = StepRecordStatus::Pending;
                record.exit_status = None;
                record.duration_ms = None;
                record.message = None;
                record.stdout_tail = None;
                record.stderr_tail = None;
            }
        }
        if found_failure {
            self.attempt_epoch = attempt_epoch;
        }
        found_failure
    }

    fn start_step(&mut self, step: &BootstrapStep) -> u32 {
        let attempt = self.steps.get(&step.id).map_or(0, |record| record.attempt) + 1;
        self.replace_record(StepRecord {
            id: step.id.clone(),
            phase: step.phase,
            status: StepRecordStatus::Running,
            attempt,
            exit_status: None,
            duration_ms: None,
            message: None,
            stdout_tail: None,
            stderr_tail: None,
        });
        attempt
    }

    fn mark_passed(&mut self, step: &BootstrapStep, exit_status: i32, duration_ms: u64) {
        let attempt = self.current_attempt(&step.id);
        self.replace_record(StepRecord {
            id: step.id.clone(),
            phase: step.phase,
            status: StepRecordStatus::Passed,
            attempt,
            exit_status: Some(exit_status),
            duration_ms: Some(duration_ms),
            message: None,
            stdout_tail: None,
            stderr_tail: None,
        });
    }

    fn mark_failed(
        &mut self,
        step: &BootstrapStep,
        exit_status: i32,
        duration_ms: u64,
        message: String,
        stdout_tail: String,
        stderr_tail: String,
    ) -> BootstrapFailed {
        let attempt = self.current_attempt(&step.id);
        self.replace_record(StepRecord {
            id: step.id.clone(),
            phase: step.phase,
            status: StepRecordStatus::Failed,
            attempt,
            exit_status: Some(exit_status),
            duration_ms: Some(duration_ms),
            message: Some(message.clone()),
            stdout_tail: Some(stdout_tail.clone()),
            stderr_tail: Some(stderr_tail.clone()),
        });
        BootstrapFailed {
            attempt_epoch: self.attempt_epoch,
            step: step.id.clone(),
            exit_status,
            duration_ms,
            message,
            stdout_tail,
            stderr_tail,
        }
    }

    fn current_attempt(&self, step: &str) -> u32 {
        self.steps.get(step).map_or(1, |record| record.attempt)
    }

    fn replace_record(&mut self, record: StepRecord) {
        self.steps.insert(record.id.clone(), record);
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct StepRecord {
    id: String,
    phase: BootstrapStepPhase,
    status: StepRecordStatus,
    attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exit_status: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stdout_tail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stderr_tail: Option<String>,
}

impl StepRecord {
    fn bootstrap_failed_event(&self, attempt_epoch: u64) -> BootstrapFailed {
        BootstrapFailed {
            attempt_epoch,
            step: self.id.clone(),
            exit_status: self.exit_status.unwrap_or(-1),
            duration_ms: self.duration_ms.unwrap_or(0),
            message: self
                .message
                .clone()
                .unwrap_or_else(|| "bootstrap step failed".to_owned()),
            stdout_tail: self.stdout_tail.clone().unwrap_or_default(),
            stderr_tail: self.stderr_tail.clone().unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StepRecordStatus {
    Pending,
    Running,
    Passed,
    Failed,
}

fn plan_hash(plan: &BootstrapPlan) -> String {
    let contents = serde_json::to_vec(plan).unwrap_or_default();
    let digest = Sha256::digest(&contents);
    format!("sha256:{}", hex(&digest))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        })
}

#[cfg(test)]
mod tests {
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use agentdp_protocol::server_guest::{BootstrapLifecycleStatus, GuestMessageKind};
    use tokio::sync::Notify;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn system_bootstrap_runs_scripts_once_and_persists_state() {
        let temp = TestTemp::new("guest-system-bootstrap-once");
        write_script(&temp.root.join("scripts").join("first.sh"), "printf run >> marker\n").await;
        let plan = plan(vec![step("system.first", "scripts/first.sh", &temp.path_text)]);

        let mut first = Vec::new();
        Box::pin(
            BootstrapExecutor::new(
                plan.clone(),
                "basic/basic-0".to_owned(),
                temp.state.clone(),
                temp.root.clone(),
            )
            .run(&mut first),
        )
        .await
        .expect("first bootstrap run");
        let mut second = Vec::new();
        Box::pin(
            BootstrapExecutor::new(plan, "basic/basic-0".to_owned(), temp.state.clone(), temp.root.clone())
                .run(&mut second),
        )
        .await
        .expect("second bootstrap run");

        assert_eq!(
            tokio::fs::read_to_string(temp.root.join("marker"))
                .await
                .expect("read marker"),
            "run"
        );
        assert!(matches!(last_status(&first), BootstrapLifecycleStatus::Passed));
        assert!(matches!(last_status(&second), BootstrapLifecycleStatus::Passed));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn system_bootstrap_stops_after_failed_step() {
        let temp = TestTemp::new("guest-system-bootstrap-failed");
        write_script(
            &temp.root.join("scripts").join("fail.sh"),
            "printf before > marker\nprintf failure >&2\nexit 12\n",
        )
        .await;
        write_script(&temp.root.join("scripts").join("after.sh"), "printf after >> marker\n").await;
        let plan = plan(vec![
            step("system.fail", "scripts/fail.sh", &temp.path_text),
            dependent_step("system.after", "scripts/after.sh", &temp.path_text, "system.fail"),
        ]);

        let mut events = Vec::new();
        Box::pin(
            BootstrapExecutor::new(plan, "basic/basic-0".to_owned(), temp.state.clone(), temp.root.clone())
                .run(&mut events),
        )
        .await
        .expect("bootstrap run");

        assert_eq!(
            tokio::fs::read_to_string(temp.root.join("marker"))
                .await
                .expect("read marker"),
            "before"
        );
        assert!(matches!(last_status(&events), BootstrapLifecycleStatus::Failed));
        let status = events
            .iter()
            .rev()
            .find_map(|event| match event {
                GuestMessageKind::BootstrapStatus(status) => Some(status),
                _ => None,
            })
            .expect("bootstrap status");
        assert_eq!(
            status.pending_steps,
            vec!["system.fail".to_owned(), "system.after".to_owned()]
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, GuestMessageKind::BootstrapFailed(failed) if failed.step == "system.fail" && failed.exit_status == 12))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_retry_preserves_passed_steps_and_is_epoch_idempotent() {
        let temp = TestTemp::new("guest-system-bootstrap-retry");
        write_script(
            &temp.root.join("scripts").join("first.sh"),
            "printf first >> first-marker\n",
        )
        .await;
        write_script(
            &temp.root.join("scripts").join("retry.sh"),
            "printf attempt >> retry-marker\n[ -e allow-retry ] || exit 12\n",
        )
        .await;
        let plan = plan(vec![
            step("system.first", "scripts/first.sh", &temp.path_text),
            dependent_step("system.retry", "scripts/retry.sh", &temp.path_text, "system.first"),
        ]);
        let bootstrap = BootstrapExecutor::new(plan, "basic/basic-0".to_owned(), temp.state.clone(), temp.root.clone());

        let mut initial = Vec::new();
        bootstrap.run(&mut initial).await.expect("initial bootstrap attempt");
        tokio::fs::write(temp.root.join("allow-retry"), b"yes")
            .await
            .expect("allow retry to pass");
        let mut retry = Vec::new();
        assert!(bootstrap.prepare_retry(1).await.expect("prepare retry"));
        bootstrap.run(&mut retry).await.expect("retry bootstrap");
        let mut duplicate = Vec::new();
        assert!(!bootstrap.prepare_retry(1).await.expect("prepare duplicate retry"));
        bootstrap
            .run(&mut duplicate)
            .await
            .expect("duplicate retry replays terminal state");

        assert_eq!(
            tokio::fs::read_to_string(temp.root.join("first-marker")).await.unwrap(),
            "first"
        );
        assert_eq!(
            tokio::fs::read_to_string(temp.root.join("retry-marker")).await.unwrap(),
            "attemptattempt"
        );
        assert!(matches!(
            retry.last(),
            Some(GuestMessageKind::BootstrapFinished(finished)) if finished.attempt_epoch == 1
        ));
        assert!(matches!(
            duplicate.last(),
            Some(GuestMessageKind::BootstrapFinished(finished)) if finished.attempt_epoch == 1
        ));
        let state = BootstrapState::load(&temp.state, bootstrap.plan_hash())
            .await
            .expect("load retried bootstrap state");
        assert_eq!(state.attempt_epoch, 1);
        assert_eq!(state.steps["system.retry"].attempt, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_terminal_follows_all_in_flight_step_completions() {
        if std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get) < 2 {
            return;
        }
        let temp = TestTemp::new("guest-system-bootstrap-terminal-order");
        let failure_started = temp.root.join("failure-started");
        write_script(
            &temp.root.join("scripts").join("fail.sh"),
            &format!("touch '{}'\nexit 12\n", failure_started.display()),
        )
        .await;
        write_script(
            &temp.root.join("scripts").join("late-success.sh"),
            &format!(
                "while [ ! -e '{}' ]; do sleep 0.01; done\nsleep 0.05\nprintf success > marker\n",
                failure_started.display()
            ),
        )
        .await;
        let plan = plan(vec![
            step("system.fail", "scripts/fail.sh", &temp.path_text),
            step("system.late_success", "scripts/late-success.sh", &temp.path_text),
        ]);

        let mut events = Vec::new();
        Box::pin(
            BootstrapExecutor::new(plan, "basic/basic-0".to_owned(), temp.state.clone(), temp.root.clone())
                .run(&mut events),
        )
        .await
        .expect("bootstrap run");

        let first_failed_status = events
            .iter()
            .position(
                |event| matches!(event, GuestMessageKind::BootstrapStatus(status) if status.status == BootstrapLifecycleStatus::Failed),
            )
            .expect("failed status");
        assert!(events[first_failed_status..].iter().all(|event| {
            !matches!(event, GuestMessageKind::BootstrapStatus(status) if status.status == BootstrapLifecycleStatus::Running)
        }));
        assert!(matches!(events.last(), Some(GuestMessageKind::BootstrapFailed(_))));
        let final_status = events[..events.len() - 1]
            .iter()
            .rev()
            .find_map(|event| match event {
                GuestMessageKind::BootstrapStatus(status) => Some(status),
                _ => None,
            })
            .expect("final status");
        assert_eq!(final_status.status, BootstrapLifecycleStatus::Failed);
        assert!(
            final_status
                .completed_steps
                .iter()
                .any(|step| step == "system.late_success")
        );
        assert_eq!(
            tokio::fs::read_to_string(temp.root.join("marker"))
                .await
                .expect("read late success marker"),
            "success"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn step_timeout_survives_full_output_event_queue() {
        let temp = TestTemp::new("guest-system-bootstrap-output-backpressure-timeout");
        write_script(
            &temp.root.join("scripts").join("noisy-timeout.sh"),
            "dd if=/dev/zero bs=8192 count=4 2>/dev/null\nwhile :; do :; done\n",
        )
        .await;
        let mut noisy = step("system.noisy", "scripts/noisy-timeout.sh", &temp.path_text);
        noisy.timeout_seconds = 1;
        let (events, _receiver) = mpsc::channel(1);
        let mut sink = ChannelEventSink { events };
        let executor = StepExecutor::new(temp.root.clone(), current_user(), temp.path_text.clone());

        let output = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            Box::pin(executor.run(&noisy, &mut sink)),
        )
        .await
        .expect("step timeout must remain active under event backpressure")
        .expect("step output");

        assert!(output.timed_out);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_orders_steps_by_dependencies_not_plan_order() {
        let temp = TestTemp::new("guest-bootstrap-topo");
        write_script(&temp.root.join("scripts").join("first.sh"), "printf first >> marker\n").await;
        write_script(
            &temp.root.join("scripts").join("second.sh"),
            "printf second >> marker\n",
        )
        .await;
        let plan = plan(vec![
            dependent_step("system.second", "scripts/second.sh", &temp.path_text, "system.first"),
            step("system.first", "scripts/first.sh", &temp.path_text),
        ]);

        let mut events = Vec::new();
        Box::pin(
            BootstrapExecutor::new(plan, "basic/basic-0".to_owned(), temp.state.clone(), temp.root.clone())
                .run(&mut events),
        )
        .await
        .expect("bootstrap run");

        assert_eq!(
            tokio::fs::read_to_string(temp.root.join("marker"))
                .await
                .expect("read marker"),
            "firstsecond"
        );
        assert!(matches!(last_status(&events), BootstrapLifecycleStatus::Passed));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_runs_independent_steps_in_parallel() {
        if std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get) < 2 {
            return;
        }
        let temp = TestTemp::new("guest-bootstrap-parallel");
        let first_ready = temp.root.join("first-ready");
        let second_ready = temp.root.join("second-ready");
        write_script(
            &temp.root.join("scripts").join("first.sh"),
            &format!(
                "touch '{}'\nwhile [ ! -e '{}' ]; do sleep 0.05; done\nprintf first >> marker\n",
                first_ready.display(),
                second_ready.display()
            ),
        )
        .await;
        write_script(
            &temp.root.join("scripts").join("second.sh"),
            &format!(
                "touch '{}'\nwhile [ ! -e '{}' ]; do sleep 0.05; done\nprintf second >> marker\n",
                second_ready.display(),
                first_ready.display()
            ),
        )
        .await;
        let plan = plan(vec![
            step("system.first", "scripts/first.sh", &temp.path_text),
            step("system.second", "scripts/second.sh", &temp.path_text),
        ]);

        let mut events = Vec::new();
        Box::pin(
            BootstrapExecutor::new(plan, "basic/basic-0".to_owned(), temp.state.clone(), temp.root.clone())
                .run(&mut events),
        )
        .await
        .expect("bootstrap run");

        assert!(matches!(last_status(&events), BootstrapLifecycleStatus::Passed));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_runs_user_phase_as_plan_user() {
        let temp = TestTemp::new("guest-bootstrap-user-phase");
        let user = current_user();
        write_script(
            &temp.root.join("scripts").join("system.sh"),
            "printf system:$(id -un) >> marker\n",
        )
        .await;
        write_script(
            &temp.root.join("scripts").join("user.sh"),
            "printf user:$USER:$LOGNAME:$HOME:$(id -un) >> marker\n",
        )
        .await;
        let mut plan = plan(vec![
            step("system.first", "scripts/system.sh", &temp.path_text),
            user_step("user.first", "scripts/user.sh", &temp.path_text, "system.first"),
        ]);
        plan.user = user.clone();
        plan.home = temp.path_text.clone();

        let mut events = Vec::new();
        Box::pin(
            BootstrapExecutor::new(plan, "basic/basic-0".to_owned(), temp.state.clone(), temp.root.clone())
                .run(&mut events),
        )
        .await
        .expect("bootstrap run");

        let marker = tokio::fs::read_to_string(temp.root.join("marker"))
            .await
            .expect("read marker");
        assert!(marker.contains(&format!("user:{user}:{user}:{}:{user}", temp.path_text)));
        assert!(matches!(last_status(&events), BootstrapLifecycleStatus::Passed));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_streams_output_before_step_finishes() {
        let temp = TestTemp::new("guest-bootstrap-streams-output");
        let gate = temp.root.join("continue");
        write_script(
            &temp.root.join("scripts").join("output.sh"),
            &format!(
                "printf first\nwhile [ ! -e '{}' ]; do sleep 0.05; done\nprintf second\n",
                gate.display()
            ),
        )
        .await;
        let plan = plan(vec![step("system.output", "scripts/output.sh", &temp.path_text)]);

        let observed_output = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());
        let mut sink = BlockingOutputSink {
            events: Vec::new(),
            observed_output: Arc::clone(&observed_output),
            notify: Arc::clone(&notify),
        };
        {
            let bootstrap =
                BootstrapExecutor::new(plan, "basic/basic-0".to_owned(), temp.state.clone(), temp.root.clone());
            let mut run = Box::pin(bootstrap.run(&mut sink));

            tokio::select! {
                () = notify.notified() => {}
                result = &mut run => result.expect("bootstrap should not finish before first output"),
            }
            assert!(observed_output.load(Ordering::SeqCst));
            tokio::fs::write(&gate, b"go").await.expect("release script");
            run.await.expect("bootstrap run");
        }

        let output_index = sink
            .events
            .iter()
            .position(
                |event| matches!(event, GuestMessageKind::BootstrapOutput(output) if output.chunk.contains("first")),
            )
            .expect("stdout output event");
        let finished_index = sink
            .events
            .iter()
            .position(|event| matches!(event, GuestMessageKind::BootstrapStepFinished(finished) if finished.step == "system.output"))
            .expect("step finished event");
        assert!(output_index < finished_index);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_reports_interrupted_running_step_as_failed() {
        let temp = TestTemp::new("guest-bootstrap-interrupted");
        write_script(&temp.root.join("scripts").join("interrupted.sh"), "printf nope\n").await;
        let plan = plan(vec![step(
            "system.interrupted",
            "scripts/interrupted.sh",
            &temp.path_text,
        )]);
        BootstrapState {
            plan_hash: plan_hash(&plan),
            attempt_epoch: 0,
            steps: BTreeMap::from([(
                "system.interrupted".to_owned(),
                StepRecord {
                    id: "system.interrupted".to_owned(),
                    phase: BootstrapStepPhase::System,
                    status: StepRecordStatus::Running,
                    attempt: 1,
                    exit_status: None,
                    duration_ms: None,
                    message: None,
                    stdout_tail: None,
                    stderr_tail: None,
                },
            )]),
        }
        .save(&temp.state)
        .await
        .expect("write interrupted state");

        let mut events = Vec::new();
        Box::pin(
            BootstrapExecutor::new(plan, "basic/basic-0".to_owned(), temp.state.clone(), temp.root.clone())
                .run(&mut events),
        )
        .await
        .expect("bootstrap run");

        assert!(matches!(last_status(&events), BootstrapLifecycleStatus::Failed));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, GuestMessageKind::BootstrapFailed(failed) if failed.step == "system.interrupted" && failed.exit_status == -1))
        );
    }

    fn last_status(events: &[GuestMessageKind]) -> BootstrapLifecycleStatus {
        events
            .iter()
            .rev()
            .find_map(|event| match event {
                GuestMessageKind::BootstrapStatus(status) => Some(status.status),
                _ => None,
            })
            .expect("status event")
    }

    async fn write_script(path: &Path, contents: &str) {
        tokio::fs::create_dir_all(path.parent().expect("script parent"))
            .await
            .expect("create script dir");
        tokio::fs::write(path, format!("#!/usr/bin/env bash\nset -euo pipefail\n{contents}"))
            .await
            .expect("write script");
        tokio::fs::set_permissions(path, Permissions::from_mode(0o700))
            .await
            .expect("chmod script");
    }

    fn plan(steps: Vec<BootstrapStep>) -> BootstrapPlan {
        BootstrapPlan {
            plan_version: 1,
            user: "agent".to_owned(),
            home: "/data/home".to_owned(),
            code_dir: "/data/home/code".to_owned(),
            steps,
        }
    }

    fn step(id: &str, script: &str, working_directory: &str) -> BootstrapStep {
        BootstrapStep {
            id: id.to_owned(),
            label: id.to_owned(),
            phase: BootstrapStepPhase::System,
            depends_on: Vec::new(),
            resources: Vec::new(),
            script: script.to_owned(),
            working_directory: working_directory.to_owned(),
            timeout_seconds: 30,
        }
    }

    fn dependent_step(id: &str, script: &str, working_directory: &str, dependency: &str) -> BootstrapStep {
        BootstrapStep {
            depends_on: vec![dependency.to_owned()],
            ..step(id, script, working_directory)
        }
    }

    fn user_step(id: &str, script: &str, working_directory: &str, dependency: &str) -> BootstrapStep {
        BootstrapStep {
            phase: BootstrapStepPhase::User,
            ..dependent_step(id, script, working_directory, dependency)
        }
    }

    fn current_user() -> String {
        let output = std::process::Command::new("id")
            .arg("-un")
            .output()
            .expect("resolve current user");
        String::from_utf8(output.stdout)
            .expect("current user utf8")
            .trim()
            .to_owned()
    }

    struct BlockingOutputSink {
        events: Vec<GuestMessageKind>,
        observed_output: Arc<AtomicBool>,
        notify: Arc<Notify>,
    }

    impl BootstrapEventSink for BlockingOutputSink {
        async fn emit(&mut self, event: GuestMessageKind) -> Result<()> {
            if matches!(&event, GuestMessageKind::BootstrapOutput(output) if output.chunk.contains("first")) {
                self.observed_output.store(true, Ordering::SeqCst);
                self.notify.notify_one();
            }
            self.events.push(event);
            Ok(())
        }
    }

    struct TestTemp {
        root: PathBuf,
        state: PathBuf,
        path_text: String,
    }

    impl TestTemp {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(&root).expect("create temp dir");
            let path_text = root.to_string_lossy().into_owned();
            let state = root.join("state").join("bootstrap-state.json");
            Self { root, state, path_text }
        }
    }

    impl Drop for TestTemp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}
