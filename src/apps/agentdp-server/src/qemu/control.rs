use std::path::PathBuf;
use std::time::{Duration, Instant};

use agentdp_core::Context;
use agentdp_core::agent::{AgentInstanceBootstrapStepStatus, AgentInstanceDocument, BackendState, BootstrapEvent};
use agentdp_platform::socket::{self, AsyncLocalSocket};
use agentdp_protocol::Error as ProtocolError;
use agentdp_protocol::jsonl::JsonLineReader;
use agentdp_protocol::server_guest::{
    BootstrapFailed, BootstrapLifecycleStatus, BootstrapStatusReport, BootstrapStepFinished, BootstrapStepStarted,
    GUEST_CONTROL_PROTOCOL_VERSION, GuestCommandResult, GuestError, GuestHello, GuestMessage, GuestMessageKind,
    GuestdRole, HostCommand, HostMessage, HostMessageKind, RETRY_BOOTSTRAP_COMMAND, RetryBootstrapCommand,
    WRITE_USER_FILE_COMMAND, WriteUserFileCommand, decode_guest_message_line, encode_host_message_line,
};

use super::error::{Error, ErrorKind};
use crate::backend::{BootstrapEventSink, BootstrapOutcome};

pub(super) const BOOTSTRAP_WAIT_TIMEOUT: Duration = Duration::from_mins(45);
pub(super) const CONTROL_RECONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_CONNECT_DELAY: Duration = Duration::from_millis(250);
const CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(1);
pub(super) const CONTROL_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct Session {
    stream: AsyncLocalSocket,
    reader: JsonLineReader,
    frame: Vec<u8>,
    next_command_id: u64,
    bootstrap_terminal: Option<BootstrapOutcome>,
    plan_hash: Option<String>,
    usable: bool,
}

impl Session {
    fn new(stream: AsyncLocalSocket) -> Self {
        Self {
            stream,
            reader: JsonLineReader::default(),
            frame: Vec::new(),
            next_command_id: 0,
            bootstrap_terminal: None,
            plan_hash: None,
            usable: true,
        }
    }

    fn next_command_id(&mut self) -> String {
        let id = format!("host_command_{}", self.next_command_id);
        self.next_command_id = self.next_command_id.saturating_add(1);
        id
    }

    pub(crate) const fn is_usable(&self) -> bool {
        self.usable
    }
}

pub(super) async fn write_user_file(
    context: &Context,
    session: &mut Session,
    path: &str,
    contents: &[u8],
    permissions: &str,
    command_timeout: Duration,
) -> Result<bool, Error> {
    context
        .logger()
        .verbose_with(|| format!("writing guest file {path} through the retained guest control session"));
    let payload = WriteUserFileCommand {
        path: path.to_owned(),
        contents: contents.to_vec(),
        permissions: permissions.to_owned(),
    };
    send_command(
        session,
        WRITE_USER_FILE_COMMAND,
        serde_json::to_value(payload).map_err(|source| ErrorKind::GuestControlMessage {
            code: "guest_control_encode".to_owned(),
            message: source.to_string(),
        })?,
        command_timeout,
    )
    .await
}

async fn send_command(
    session: &mut Session,
    command: &str,
    payload: serde_json::Value,
    command_timeout: Duration,
) -> Result<bool, Error> {
    let id = session.next_command_id();
    let message = HostMessage::new(
        &id,
        HostMessageKind::Command(HostCommand {
            command: command.to_owned(),
            payload,
        }),
    );
    let line = encode_host_message_line(&message).map_err(|source| ErrorKind::GuestControlDecode {
        message: "host command".to_owned(),
        source,
    })?;
    let exchange = async {
        if let Err(source) = session.stream.write_all(&line).await {
            session.usable = false;
            return Err(ErrorKind::GuestControlMessage {
                code: "guest_control_write".to_owned(),
                message: source.to_string(),
            }
            .into());
        }
        if let Err(source) = session.stream.flush().await {
            session.usable = false;
            return Err(ErrorKind::GuestControlMessage {
                code: "guest_control_write".to_owned(),
                message: source.to_string(),
            }
            .into());
        }
        read_command_result(session, &id, command).await
    };
    match tokio::time::timeout(command_timeout, exchange).await {
        Ok(result) => result,
        Err(_elapsed) => {
            session.usable = false;
            Err(ErrorKind::GuestControlMessage {
                code: "guest_control_timeout".to_owned(),
                message: format!(
                    "guest did not answer {command} after {}s",
                    command_timeout.as_secs_f64()
                ),
            }
            .into())
        }
    }
}

pub(super) async fn wait_bootstrap(
    context: &Context,
    state: &AgentInstanceDocument,
    control: &mut Option<Session>,
    retry_epoch: Option<u64>,
    bootstrap_events: Option<&mut dyn BootstrapEventSink>,
    timeout: Duration,
) -> Result<BootstrapOutcome, Error> {
    let deadline = Instant::now() + timeout;
    let mut events = BootstrapEventTarget::new(bootstrap_events);
    if control.as_ref().is_some_and(|session| !session.usable) {
        control.take();
    }
    if let Some(outcome) = reconcile_retained_bootstrap(state, control, retry_epoch, &mut events, deadline).await? {
        return Ok(outcome);
    }
    control.take();
    let BackendState::Qemu(backend) = &state.status.backend;
    let control_socket = PathBuf::from(&backend.guest_control_socket);
    context.logger().verbose_with(|| {
        format!(
            "waiting up to {}s for QEMU guest bootstrap on {}",
            timeout.as_secs(),
            control_socket.display()
        )
    });

    let mut last_event = "guest control channel has not connected".to_owned();

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ErrorKind::GuestBootstrapTimeout {
                timeout_seconds: timeout.as_secs(),
                last_event,
            }
            .into());
        }

        match socket::connect_local_socket(&control_socket).await {
            Ok(stream) => {
                "guest control channel connected".clone_into(&mut last_event);
                send_diagnostic_bootstrap_event(&mut events, "guest control channel connected".to_owned());
                let mut session = Session::new(stream);
                let mut observer = BootstrapObserver::new(ExpectedGuest::from_state(state));
                let mut read_state = BootstrapReadState {
                    events: &mut events,
                    observer: &mut observer,
                    last_event: &mut last_event,
                };
                match read_bootstrap_stream(&mut session, &mut read_state, deadline).await? {
                    StreamResult::Terminal(outcome) => {
                        session.plan_hash.clone_from(&observer.plan_hash);
                        session.bootstrap_terminal = Some(outcome.clone());
                        let requested_retry = match (&outcome, retry_epoch) {
                            (BootstrapOutcome::Failed { .. }, Some(epoch)) if epoch > outcome.attempt_epoch() => {
                                Some(epoch)
                            }
                            _ => None,
                        };
                        if let Some(requested_epoch) = requested_retry {
                            request_bootstrap_retry(&mut session, requested_epoch).await?;
                            session.bootstrap_terminal = None;
                            "bootstrap retry command accepted".clone_into(&mut last_event);
                            if let Some(retried) = observe_retained_bootstrap(
                                ExpectedGuest::from_state(state),
                                &mut session,
                                requested_epoch,
                                &mut events,
                                deadline,
                                &mut last_event,
                            )
                            .await?
                            {
                                *control = Some(session);
                                return Ok(retried);
                            }
                        } else {
                            *control = Some(session);
                            return Ok(outcome);
                        }
                    }
                    StreamResult::Disconnected => {}
                }
            }
            Err(source) => {
                last_event = format!("guest control channel is not ready: {source}");
            }
        }

        tokio::time::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(CONTROL_CONNECT_DELAY),
        )
        .await;
    }
}

async fn reconcile_retained_bootstrap(
    state: &AgentInstanceDocument,
    control: &mut Option<Session>,
    retry_epoch: Option<u64>,
    events: &mut BootstrapEventTarget<'_>,
    deadline: Instant,
) -> Result<Option<BootstrapOutcome>, Error> {
    let Some(session) = control.as_mut() else {
        return Ok(None);
    };
    let Some(outcome) = session.bootstrap_terminal.clone() else {
        return Ok(None);
    };
    let Some(requested_epoch) = retry_epoch else {
        return Ok(Some(outcome));
    };
    if !matches!(outcome, BootstrapOutcome::Failed { .. }) || requested_epoch <= outcome.attempt_epoch() {
        return Ok(Some(outcome));
    }
    request_bootstrap_retry(session, requested_epoch).await?;
    session.bootstrap_terminal = None;
    let mut last_event = "bootstrap retry command accepted".to_owned();
    let observed = observe_retained_bootstrap(
        ExpectedGuest::from_state(state),
        session,
        requested_epoch,
        events,
        deadline,
        &mut last_event,
    )
    .await?;
    if observed.is_none() {
        session.usable = false;
    }
    Ok(observed)
}

async fn observe_retained_bootstrap(
    expected: ExpectedGuest,
    session: &mut Session,
    expected_attempt_epoch: u64,
    events: &mut BootstrapEventTarget<'_>,
    deadline: Instant,
    last_event: &mut String,
) -> Result<Option<BootstrapOutcome>, Error> {
    let mut observer = BootstrapObserver::resume(expected, session.plan_hash.clone(), expected_attempt_epoch);
    let mut read_state = BootstrapReadState {
        events,
        observer: &mut observer,
        last_event,
    };
    match read_bootstrap_stream(session, &mut read_state, deadline).await? {
        StreamResult::Terminal(outcome) => {
            session.plan_hash.clone_from(&observer.plan_hash);
            session.bootstrap_terminal = Some(outcome.clone());
            Ok(Some(outcome))
        }
        StreamResult::Disconnected => {
            session.usable = false;
            Ok(None)
        }
    }
}

async fn request_bootstrap_retry(session: &mut Session, attempt_epoch: u64) -> Result<(), Error> {
    let plan_hash = session
        .plan_hash
        .clone()
        .ok_or_else(|| guest_control_handshake("retained session has no observed bootstrap plan hash".to_owned()))?;
    send_command(
        session,
        RETRY_BOOTSTRAP_COMMAND,
        serde_json::to_value(RetryBootstrapCommand {
            plan_hash,
            attempt_epoch,
        })
        .map_err(|source| ErrorKind::GuestControlMessage {
            code: "guest_control_encode".to_owned(),
            message: source.to_string(),
        })?,
        CONTROL_COMMAND_TIMEOUT,
    )
    .await?;
    Ok(())
}

struct BootstrapEventTarget<'a> {
    sink: Option<&'a mut dyn BootstrapEventSink>,
}

impl<'a> BootstrapEventTarget<'a> {
    const fn new(sink: Option<&'a mut dyn BootstrapEventSink>) -> Self {
        Self { sink }
    }

    fn emit(&mut self, event: BootstrapEvent) {
        let Some(sink) = self.sink.as_mut() else {
            return;
        };
        sink.emit(event);
    }
}

struct BootstrapReadState<'a, 'events> {
    events: &'a mut BootstrapEventTarget<'events>,
    observer: &'a mut BootstrapObserver,
    last_event: &'a mut String,
}

async fn read_bootstrap_stream(
    session: &mut Session,
    state: &mut BootstrapReadState<'_, '_>,
    deadline: Instant,
) -> Result<StreamResult, Error> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(StreamResult::Disconnected);
        }

        let read = tokio::time::timeout(
            remaining.min(CONTROL_READ_TIMEOUT),
            session.reader.read_line(&mut session.stream, &mut session.frame),
        )
        .await;
        match read {
            Ok(Ok(false)) => {
                "guest control channel closed".clone_into(state.last_event);
                return Ok(StreamResult::Disconnected);
            }
            Ok(Ok(true)) => {
                if !session.frame.ends_with(b"\n") {
                    "guest control channel closed with an incomplete frame".clone_into(state.last_event);
                    return Ok(StreamResult::Disconnected);
                }
                let message =
                    decode_guest_message_line(&session.frame).map_err(|source| ErrorKind::GuestControlDecode {
                        message: String::from_utf8_lossy(&session.frame).trim_end().to_owned(),
                        source,
                    })?;
                *state.last_event = BootstrapObserver::describe(&message);
                if let BootstrapStreamStatus::Terminal(outcome) = state.observer.handle(message, state.events)? {
                    return Ok(StreamResult::Terminal(outcome));
                }
            }
            Err(_elapsed) => {}
            Ok(Err(ProtocolError::Read(source))) => {
                *state.last_event = format!("guest control channel read failed: {source}");
                return Ok(StreamResult::Disconnected);
            }
            Ok(Err(source)) => {
                return Err(ErrorKind::GuestControlDecode {
                    message: "guest control stream".to_owned(),
                    source,
                }
                .into());
            }
        }
    }
}

async fn read_command_result(session: &mut Session, expected_id: &str, expected_command: &str) -> Result<bool, Error> {
    let read = session
        .reader
        .read_line(&mut session.stream, &mut session.frame)
        .await
        .map_err(|source| ErrorKind::GuestControlDecode {
            message: "guest control command response".to_owned(),
            source,
        });
    let has_frame = match read {
        Ok(has_frame) => has_frame,
        Err(error) => {
            session.usable = false;
            return Err(error.into());
        }
    };
    if !has_frame {
        session.usable = false;
        return Err(ErrorKind::GuestControlMessage {
            code: "guest_control_eof".to_owned(),
            message: "guest closed control channel before command response".to_owned(),
        }
        .into());
    }
    if !session.frame.ends_with(b"\n") {
        session.usable = false;
        return Err(ErrorKind::GuestControlMessage {
            code: "guest_control_eof".to_owned(),
            message: "guest closed control channel during command response".to_owned(),
        }
        .into());
    }
    let message = match decode_guest_message_line(&session.frame) {
        Ok(message) => message,
        Err(source) => {
            session.usable = false;
            return Err(ErrorKind::GuestControlDecode {
                message: String::from_utf8_lossy(&session.frame).trim_end().to_owned(),
                source,
            }
            .into());
        }
    };
    if message.id != expected_id {
        session.usable = false;
        return Err(ErrorKind::GuestControlMessage {
            code: "guest_control_correlation".to_owned(),
            message: format!(
                "guest returned command response id {}; expected {expected_id}",
                message.id
            ),
        }
        .into());
    }
    let (result, usable) = command_result_from_message(message, expected_command);
    session.usable = usable;
    result
}

fn command_result_from_message(message: GuestMessage, expected_command: &str) -> (Result<bool, Error>, bool) {
    match message.kind {
        GuestMessageKind::CommandResult(GuestCommandResult { command, updated }) if command == expected_command => {
            (Ok(updated), true)
        }
        GuestMessageKind::CommandResult(result) => (
            Err(ErrorKind::GuestControlMessage {
                code: "guest_control_command_mismatch".to_owned(),
                message: format!("guest returned result for unexpected command {}", result.command),
            }
            .into()),
            false,
        ),
        GuestMessageKind::Error(error) => (Err(guest_error(error)), true),
        other => (
            Err(ErrorKind::GuestControlMessage {
                code: "guest_control_unexpected_message".to_owned(),
                message: format!("guest returned unexpected message {other:?}"),
            }
            .into()),
            false,
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StreamResult {
    Terminal(BootstrapOutcome),
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BootstrapStreamStatus {
    Running,
    Terminal(BootstrapOutcome),
}

#[derive(Debug)]
struct BootstrapObserver {
    expected: ExpectedGuest,
    accepted_hello: bool,
    plan_hash: Option<String>,
    expected_attempt_epoch: Option<u64>,
    last_status: Option<BootstrapStatusKey>,
}

impl BootstrapObserver {
    const fn new(expected: ExpectedGuest) -> Self {
        Self {
            expected,
            accepted_hello: false,
            plan_hash: None,
            expected_attempt_epoch: None,
            last_status: None,
        }
    }

    const fn resume(expected: ExpectedGuest, plan_hash: Option<String>, expected_attempt_epoch: u64) -> Self {
        Self {
            expected,
            accepted_hello: true,
            plan_hash,
            expected_attempt_epoch: Some(expected_attempt_epoch),
            last_status: None,
        }
    }

    fn handle(
        &mut self,
        message: GuestMessage,
        events: &mut BootstrapEventTarget<'_>,
    ) -> Result<BootstrapStreamStatus, Error> {
        match message.kind {
            GuestMessageKind::Hello(hello) => {
                self.validate_hello(&hello)?;
                send_diagnostic_bootstrap_event(
                    events,
                    format!(
                        "guestd {} connected for {}/{}",
                        role_name(hello.guestd_role),
                        hello.manifest,
                        hello.instance
                    ),
                );
                self.accepted_hello = true;
            }
            GuestMessageKind::BootstrapStatus(status) => {
                self.require_accepted_hello()?;
                self.validate_status_identity(&status)?;
                let completed_steps = status.completed_steps.len();
                let pending_steps = status.pending_steps.len();
                let key = BootstrapStatusKey {
                    attempt_epoch: status.attempt_epoch,
                    status: status.status,
                    current_step: status.current_step.clone(),
                    completed_steps,
                    pending_steps,
                    failed_step: status.failed_step.clone(),
                };
                if self.last_status.as_ref() != Some(&key) {
                    self.last_status = Some(key);
                    let failed = status
                        .failed_step
                        .as_ref()
                        .map_or(String::new(), |step| format!(" failed:{step}"));
                    send_diagnostic_bootstrap_event(
                        events,
                        format!(
                            "bootstrap {} completed:{} pending:{}{}",
                            lifecycle_status_name(status.status),
                            completed_steps,
                            pending_steps,
                            failed,
                        ),
                    );
                    send_status_bootstrap_event(events, &status);
                }
            }
            GuestMessageKind::BootstrapStepStarted(started) => {
                self.require_accepted_hello()?;
                self.require_status_snapshot()?;
                send_started_bootstrap_event(events, &started);
            }
            GuestMessageKind::BootstrapOutput(_) | GuestMessageKind::CommandResult(_) => {
                self.require_accepted_hello()?;
                self.require_status_snapshot()?;
            }
            GuestMessageKind::BootstrapStepFinished(finished) => {
                self.require_accepted_hello()?;
                self.require_status_snapshot()?;
                send_finished_bootstrap_event(events, &finished);
            }
            GuestMessageKind::BootstrapFinished(finished) => {
                self.require_accepted_hello()?;
                if self.last_status.as_ref().map(|status| status.status) != Some(BootstrapLifecycleStatus::Passed)
                    || self.plan_hash.as_deref() != Some(finished.plan_hash.as_str())
                    || self.last_status.as_ref().map(|status| status.attempt_epoch) != Some(finished.attempt_epoch)
                {
                    return Err(guest_control_handshake(
                        "guest sent bootstrap finished without a matching passed status".to_owned(),
                    ));
                }
                send_diagnostic_bootstrap_event(events, "bootstrap finished".to_owned());
                return Ok(BootstrapStreamStatus::Terminal(BootstrapOutcome::Passed {
                    attempt_epoch: finished.attempt_epoch,
                }));
            }
            GuestMessageKind::BootstrapFailed(failed) => {
                self.require_accepted_hello()?;
                if self.last_status.as_ref().map(|status| status.status) != Some(BootstrapLifecycleStatus::Failed)
                    || self.last_status.as_ref().map(|status| status.attempt_epoch) != Some(failed.attempt_epoch)
                {
                    return Err(guest_control_handshake(
                        "guest sent bootstrap failure without a matching failed status".to_owned(),
                    ));
                }
                send_failed_bootstrap_event(events, &failed);
                return Ok(BootstrapStreamStatus::Terminal(BootstrapOutcome::Failed {
                    attempt_epoch: failed.attempt_epoch,
                    error: bootstrap_failed_message(&failed),
                }));
            }
            GuestMessageKind::Error(error) => return Err(guest_error(error)),
        }
        Ok(BootstrapStreamStatus::Running)
    }

    fn validate_hello(&self, hello: &GuestHello) -> Result<(), Error> {
        if hello.protocol_version != GUEST_CONTROL_PROTOCOL_VERSION {
            return Err(guest_control_handshake(format!(
                "guest protocol version {} does not match host protocol version {}",
                hello.protocol_version, GUEST_CONTROL_PROTOCOL_VERSION
            )));
        }
        if hello.guestd_role != GuestdRole::System {
            return Err(guest_control_handshake(format!(
                "guestd role {} is not system",
                role_name(hello.guestd_role)
            )));
        }
        if hello.manifest != self.expected.manifest || hello.instance != self.expected.instance {
            return Err(guest_control_handshake(format!(
                "guest identity {}/{} does not match expected {}/{}",
                hello.manifest, hello.instance, self.expected.manifest, self.expected.instance
            )));
        }
        Ok(())
    }

    fn require_accepted_hello(&self) -> Result<(), Error> {
        if self.accepted_hello {
            return Ok(());
        }
        Err(guest_control_handshake(
            "guest sent bootstrap message before accepted hello".to_owned(),
        ))
    }

    fn require_status_snapshot(&self) -> Result<(), Error> {
        if self.last_status.is_some() {
            return Ok(());
        }
        Err(guest_control_handshake(
            "guest sent bootstrap event before bootstrap status".to_owned(),
        ))
    }

    fn validate_status_identity(&mut self, status: &BootstrapStatusReport) -> Result<(), Error> {
        let expected_plan = format!("{}/{}", self.expected.manifest, self.expected.instance);
        if status.plan_id != expected_plan {
            return Err(guest_control_handshake(format!(
                "guest bootstrap plan {} does not match expected {expected_plan}",
                status.plan_id
            )));
        }
        if let Some(plan_hash) = &self.plan_hash
            && plan_hash != &status.plan_hash
        {
            return Err(guest_control_handshake(
                "guest bootstrap plan hash changed during the control session".to_owned(),
            ));
        }
        if let Some(expected_attempt_epoch) = self.expected_attempt_epoch
            && status.attempt_epoch != expected_attempt_epoch
        {
            return Err(guest_control_handshake(format!(
                "guest bootstrap attempt epoch {} does not match requested epoch {expected_attempt_epoch}",
                status.attempt_epoch
            )));
        }
        if let Some(previous) = &self.last_status
            && status.attempt_epoch < previous.attempt_epoch
        {
            return Err(guest_control_handshake(format!(
                "guest bootstrap attempt epoch regressed from {} to {}",
                previous.attempt_epoch, status.attempt_epoch
            )));
        }
        self.plan_hash = Some(status.plan_hash.clone());
        Ok(())
    }

    fn describe(message: &GuestMessage) -> String {
        match &message.kind {
            GuestMessageKind::Hello(hello) => format!("guestd {} hello", role_name(hello.guestd_role)),
            GuestMessageKind::BootstrapStatus(status) => {
                format!("bootstrap status {}", lifecycle_status_name(status.status))
            }
            GuestMessageKind::BootstrapStepStarted(started) => format!("bootstrap step {} started", started.step),
            GuestMessageKind::BootstrapOutput(output) => format!("bootstrap output from {}", output.step),
            GuestMessageKind::BootstrapStepFinished(finished) => {
                format!("bootstrap step {} {}", finished.step, step_status_name(finished.status))
            }
            GuestMessageKind::BootstrapFinished(_) => "bootstrap finished".to_owned(),
            GuestMessageKind::BootstrapFailed(failed) => format!("bootstrap step {} failed", failed.step),
            GuestMessageKind::CommandResult(result) => format!("guest command {} finished", result.command),
            GuestMessageKind::Error(error) => format!("guest error {}: {}", error.code, error.message),
        }
    }
}

#[derive(Debug)]
struct ExpectedGuest {
    manifest: String,
    instance: String,
}

impl ExpectedGuest {
    fn from_state(state: &AgentInstanceDocument) -> Self {
        Self {
            manifest: state.metadata.agent.to_string(),
            instance: state.metadata.name.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BootstrapStatusKey {
    attempt_epoch: u64,
    status: BootstrapLifecycleStatus,
    current_step: Option<String>,
    completed_steps: usize,
    pending_steps: usize,
    failed_step: Option<String>,
}

fn send_status_bootstrap_event(events: &mut BootstrapEventTarget<'_>, status: &BootstrapStatusReport) {
    let Some(name) = status.current_step.clone().or_else(|| status.failed_step.clone()) else {
        return;
    };
    let current = status.completed_steps.len().saturating_add(1);
    let total = status.completed_steps.len().saturating_add(status.pending_steps.len());
    events.emit(BootstrapEvent::StepStarted {
        step: AgentInstanceBootstrapStepStatus {
            step: name,
            label: None,
            phase: Some(status.phase),
            current: u32::try_from(current).ok(),
            total: u32::try_from(total).ok(),
            status: Some(status.status),
        },
    });
}

fn send_diagnostic_bootstrap_event(events: &mut BootstrapEventTarget<'_>, message: String) {
    events.emit(BootstrapEvent::Diagnostic {
        level: agentdp_core::agent::EventLevel::Info,
        message,
    });
}

fn send_started_bootstrap_event(events: &mut BootstrapEventTarget<'_>, started: &BootstrapStepStarted) {
    events.emit(BootstrapEvent::StepStarted {
        step: AgentInstanceBootstrapStepStatus {
            step: started.step.clone(),
            label: Some(started.label.clone()),
            phase: Some(started.phase),
            current: None,
            total: None,
            status: Some(BootstrapLifecycleStatus::Running),
        },
    });
}

fn send_finished_bootstrap_event(events: &mut BootstrapEventTarget<'_>, finished: &BootstrapStepFinished) {
    events.emit(BootstrapEvent::StepFinished {
        step: finished.step.clone(),
        status: finished.status,
        exit_status: finished.exit_status,
        duration_ms: finished.duration_ms,
    });
}

fn send_failed_bootstrap_event(events: &mut BootstrapEventTarget<'_>, failed: &BootstrapFailed) {
    events.emit(BootstrapEvent::StepFailed {
        step: failed.step.clone(),
        status: agentdp_protocol::server_guest::BootstrapStepStatus::Failed,
        exit_status: failed.exit_status,
        duration_ms: failed.duration_ms,
        message: failed.message.clone(),
    });
}

fn bootstrap_failed_message(failed: &BootstrapFailed) -> String {
    Error::from(ErrorKind::GuestBootstrapFailed {
        step: failed.step.clone(),
        message: failed.message.clone(),
        stdout_tail: failed.stdout_tail.clone(),
        stderr_tail: failed.stderr_tail.clone(),
    })
    .to_string()
}

fn guest_error(error: GuestError) -> Error {
    ErrorKind::GuestControlMessage {
        code: error.code,
        message: error.message,
    }
    .into()
}

fn guest_control_handshake(message: String) -> Error {
    ErrorKind::GuestControlMessage {
        code: "guest_control_handshake".to_owned(),
        message,
    }
    .into()
}

const fn role_name(role: GuestdRole) -> &'static str {
    match role {
        GuestdRole::System => "system",
        GuestdRole::User => "user",
    }
}

const fn lifecycle_status_name(status: BootstrapLifecycleStatus) -> &'static str {
    match status {
        BootstrapLifecycleStatus::Pending => "pending",
        BootstrapLifecycleStatus::Running => "running",
        BootstrapLifecycleStatus::Passed => "passed",
        BootstrapLifecycleStatus::Failed => "failed",
    }
}

const fn step_status_name(status: agentdp_protocol::server_guest::BootstrapStepStatus) -> &'static str {
    match status {
        agentdp_protocol::server_guest::BootstrapStepStatus::Passed => "passed",
        agentdp_protocol::server_guest::BootstrapStepStatus::Failed => "failed",
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use agentdp_core::Context;
    use agentdp_core::agent::AgentInstanceBootstrapStepStatus;
    use agentdp_ds::local::spsc;
    use agentdp_platform::socket;
    use agentdp_protocol::jsonl::JsonLineReader;
    use agentdp_protocol::server_guest::{
        BootstrapFailed, BootstrapFinished, BootstrapLifecycleStatus, BootstrapOutput, BootstrapOutputStream,
        BootstrapStatusReport, BootstrapStepPhase, BootstrapStepStarted, GUEST_CONTROL_PROTOCOL_VERSION,
        GuestCommandResult, GuestHello, GuestMessage, GuestMessageKind, GuestdRole, HostMessageKind,
        RETRY_BOOTSTRAP_COMMAND, RetryBootstrapCommand, WRITE_USER_FILE_COMMAND, decode_host_message_line,
        encode_guest_message_line,
    };

    use crate::backend::BootstrapOutcome;
    use agentdp_core::agent::BootstrapEvent;

    use super::{
        BootstrapObserver, BootstrapReadState, BootstrapStreamStatus, ExpectedGuest, Session, StreamResult,
        read_bootstrap_stream,
    };

    #[tokio::test]
    async fn bootstrap_accepts_replay_after_unterminated_frame_disconnects() {
        let socket_path = std::env::temp_dir().join(format!(
            "agentdp-control-replay-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let listener = socket::bind_local_socket(&socket_path)
            .await
            .expect("bind control socket");
        let (host, guest) = tokio::join!(socket::connect_local_socket(&socket_path), listener.accept());
        let mut session = Session::new(host.expect("connect control socket"));
        let mut guest = guest.expect("accept control socket");
        guest
            .write_all(br#"{"id":"msg_0","kind":"#)
            .await
            .expect("write partial guest frame");
        drop(guest);

        let mut observer = expected_observer();
        let mut events = super::BootstrapEventTarget::new(None);
        let mut last_event = String::new();
        let mut state = BootstrapReadState {
            events: &mut events,
            observer: &mut observer,
            last_event: &mut last_event,
        };
        let result = read_bootstrap_stream(&mut session, &mut state, Instant::now() + Duration::from_secs(1))
            .await
            .expect("partial frame disconnects without a protocol failure");
        assert_eq!(result, StreamResult::Disconnected);

        let (host, guest) = tokio::join!(socket::connect_local_socket(&socket_path), listener.accept());
        let mut session = Session::new(host.expect("reconnect control socket"));
        let mut guest = guest.expect("accept reconnected control socket");
        let replay = [
            encode_guest_message_line(&hello_message(
                "basic",
                "basic-0",
                GuestdRole::System,
                GUEST_CONTROL_PROTOCOL_VERSION,
            ))
            .expect("encode replayed hello"),
            encode_guest_message_line(&GuestMessage::new(
                "bootstrap_status",
                GuestMessageKind::BootstrapStatus(BootstrapStatusReport {
                    plan_id: "basic/basic-0".to_owned(),
                    plan_hash: "sha256:abc".to_owned(),
                    attempt_epoch: 0,
                    phase: BootstrapStepPhase::System,
                    status: BootstrapLifecycleStatus::Passed,
                    current_step: None,
                    completed_steps: Vec::new(),
                    failed_step: None,
                    pending_steps: Vec::new(),
                }),
            ))
            .expect("encode replayed status"),
            encode_guest_message_line(&GuestMessage::new(
                "bootstrap_0",
                GuestMessageKind::BootstrapFinished(BootstrapFinished {
                    plan_hash: "sha256:abc".to_owned(),
                    attempt_epoch: 0,
                }),
            ))
            .expect("encode replayed terminal event"),
        ]
        .concat();
        guest.write_all(&replay).await.expect("write replayed bootstrap");

        let mut observer = expected_observer();
        let mut events = super::BootstrapEventTarget::new(None);
        let mut last_event = String::new();
        let mut state = BootstrapReadState {
            events: &mut events,
            observer: &mut observer,
            last_event: &mut last_event,
        };
        let result = read_bootstrap_stream(&mut session, &mut state, Instant::now() + Duration::from_secs(1))
            .await
            .expect("read replayed bootstrap");
        assert_eq!(
            result,
            StreamResult::Terminal(BootstrapOutcome::Passed { attempt_epoch: 0 })
        );

        drop(listener);
        let _removed = std::fs::remove_file(socket_path);
    }

    #[tokio::test]
    async fn bootstrap_rejects_malformed_terminated_frame() {
        let socket_path = std::env::temp_dir().join(format!(
            "agentdp-control-malformed-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let listener = socket::bind_local_socket(&socket_path)
            .await
            .expect("bind control socket");
        let (host, guest) = tokio::join!(socket::connect_local_socket(&socket_path), listener.accept());
        let mut session = Session::new(host.expect("connect control socket"));
        let mut guest = guest.expect("accept control socket");
        guest
            .write_all(b"{malformed}\n")
            .await
            .expect("write malformed guest frame");

        let mut observer = expected_observer();
        let mut events = super::BootstrapEventTarget::new(None);
        let mut last_event = String::new();
        let mut state = BootstrapReadState {
            events: &mut events,
            observer: &mut observer,
            last_event: &mut last_event,
        };
        let error = read_bootstrap_stream(&mut session, &mut state, Instant::now() + Duration::from_secs(1))
            .await
            .expect_err("terminated malformed frame is a protocol failure");
        assert!(error.to_string().contains("guest control channel sent invalid message"));

        drop(listener);
        let _removed = std::fs::remove_file(socket_path);
    }

    #[tokio::test]
    async fn retained_session_writes_multiple_files_over_one_connection() {
        let socket_path = std::env::temp_dir().join(format!(
            "agentdp-control-session-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let listener = socket::bind_local_socket(&socket_path)
            .await
            .expect("bind control socket");
        let (host, guest) = tokio::join!(socket::connect_local_socket(&socket_path), listener.accept());
        let mut session = Session::new(host.expect("connect control socket"));
        let mut guest = guest.expect("accept control socket");

        let host = async {
            assert!(
                super::write_user_file(
                    &Context::quiet(),
                    &mut session,
                    ".codex/config.toml",
                    b"first",
                    "0600",
                    super::CONTROL_COMMAND_TIMEOUT,
                )
                .await
                .expect("write first file")
            );
            assert!(
                super::write_user_file(
                    &Context::quiet(),
                    &mut session,
                    ".codex/agents/reviewer.toml",
                    b"second",
                    "0600",
                    super::CONTROL_COMMAND_TIMEOUT,
                )
                .await
                .expect("write second file")
            );
        };
        let guest = async {
            let mut reader = JsonLineReader::default();
            let mut frame = Vec::new();
            for expected_path in [".codex/config.toml", ".codex/agents/reviewer.toml"] {
                assert!(
                    reader
                        .read_line(&mut guest, &mut frame)
                        .await
                        .expect("read host command")
                );
                let command = decode_host_message_line(&frame).expect("decode host command");
                let HostMessageKind::Command(payload) = command.kind;
                let file: agentdp_protocol::server_guest::WriteUserFileCommand =
                    serde_json::from_value(payload.payload).expect("decode file payload");
                assert_eq!(file.path, expected_path);
                guest
                    .write_all(
                        &encode_guest_message_line(&GuestMessage::new(
                            command.id,
                            GuestMessageKind::CommandResult(GuestCommandResult {
                                command: WRITE_USER_FILE_COMMAND.to_owned(),
                                updated: true,
                            }),
                        ))
                        .expect("encode command result"),
                    )
                    .await
                    .expect("write command result");
            }
        };

        tokio::join!(host, guest);
        drop(listener);
        let _removed = std::fs::remove_file(socket_path);
    }

    #[tokio::test]
    async fn retained_failed_session_retries_on_the_same_connection() {
        let socket_path = std::env::temp_dir().join(format!(
            "agentdp-control-retry-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let listener = socket::bind_local_socket(&socket_path)
            .await
            .expect("bind control socket");
        let (host, guest) = tokio::join!(socket::connect_local_socket(&socket_path), listener.accept());
        let mut session = Session::new(host.expect("connect control socket"));
        let mut guest = guest.expect("accept control socket");
        session.plan_hash = Some("sha256:abc".to_owned());
        session.bootstrap_terminal = Some(BootstrapOutcome::Failed {
            attempt_epoch: 0,
            error: "initial failure".to_owned(),
        });
        let guest_task = tokio::spawn(async move {
            let mut reader = JsonLineReader::default();
            let mut frame = Vec::new();
            assert!(reader.read_line(&mut guest, &mut frame).await.unwrap());
            let command = decode_host_message_line(&frame).expect("decode bootstrap retry command");
            let HostMessageKind::Command(host_command) = command.kind;
            assert_eq!(host_command.command, RETRY_BOOTSTRAP_COMMAND);
            let retry = serde_json::from_value::<RetryBootstrapCommand>(host_command.payload).unwrap();
            assert_eq!(retry.attempt_epoch, 1);
            let messages = [
                encode_guest_message_line(&GuestMessage::new(
                    command.id,
                    GuestMessageKind::CommandResult(GuestCommandResult {
                        command: RETRY_BOOTSTRAP_COMMAND.to_owned(),
                        updated: true,
                    }),
                ))
                .unwrap(),
                encode_guest_message_line(&GuestMessage::new(
                    "retry_status",
                    GuestMessageKind::BootstrapStatus(BootstrapStatusReport {
                        plan_id: "basic/basic-0".to_owned(),
                        plan_hash: "sha256:abc".to_owned(),
                        attempt_epoch: 1,
                        phase: BootstrapStepPhase::System,
                        status: BootstrapLifecycleStatus::Passed,
                        current_step: None,
                        completed_steps: Vec::new(),
                        failed_step: None,
                        pending_steps: Vec::new(),
                    }),
                ))
                .unwrap(),
                encode_guest_message_line(&GuestMessage::new(
                    "retry_finished",
                    GuestMessageKind::BootstrapFinished(BootstrapFinished {
                        plan_hash: "sha256:abc".to_owned(),
                        attempt_epoch: 1,
                    }),
                ))
                .unwrap(),
            ]
            .concat();
            guest.write_all(&messages).await.unwrap();
        });

        super::request_bootstrap_retry(&mut session, 1)
            .await
            .expect("retry command accepted");
        session.bootstrap_terminal = None;
        let mut events = super::BootstrapEventTarget::new(None);
        let mut last_event = String::new();
        let outcome = super::observe_retained_bootstrap(
            ExpectedGuest {
                manifest: "basic".to_owned(),
                instance: "basic-0".to_owned(),
            },
            &mut session,
            1,
            &mut events,
            Instant::now() + Duration::from_secs(1),
            &mut last_event,
        )
        .await
        .expect("observe retried bootstrap")
        .expect("retried bootstrap terminal");
        guest_task.await.unwrap();

        assert_eq!(outcome, BootstrapOutcome::Passed { attempt_epoch: 1 });
        assert_eq!(session.bootstrap_terminal, Some(outcome));
        assert!(session.is_usable());
        drop(listener);
        let _removed = std::fs::remove_file(socket_path);
    }

    #[tokio::test]
    async fn retained_session_rejects_mismatched_command_response_id() {
        let socket_path = std::env::temp_dir().join(format!(
            "agentdp-control-correlation-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let listener = socket::bind_local_socket(&socket_path)
            .await
            .expect("bind control socket");
        let (host, guest) = tokio::join!(socket::connect_local_socket(&socket_path), listener.accept());
        let mut session = Session::new(host.expect("connect control socket"));
        let mut guest = guest.expect("accept control socket");

        let host = async {
            let error = super::write_user_file(
                &Context::quiet(),
                &mut session,
                ".codex/config.toml",
                b"contents",
                "0600",
                super::CONTROL_COMMAND_TIMEOUT,
            )
            .await
            .expect_err("mismatched response id must fail the serial session");
            assert!(error.to_string().contains("expected host_command_0"));
            assert!(!session.is_usable());
        };
        let guest = async {
            let mut reader = JsonLineReader::default();
            let mut frame = Vec::new();
            assert!(
                reader
                    .read_line(&mut guest, &mut frame)
                    .await
                    .expect("read host command")
            );
            guest
                .write_all(
                    &encode_guest_message_line(&GuestMessage::new(
                        "wrong-command-id",
                        GuestMessageKind::CommandResult(GuestCommandResult {
                            command: WRITE_USER_FILE_COMMAND.to_owned(),
                            updated: true,
                        }),
                    ))
                    .expect("encode command result"),
                )
                .await
                .expect("write mismatched result");
        };

        tokio::join!(host, guest);
        drop(listener);
        let _removed = std::fs::remove_file(socket_path);
    }

    #[tokio::test]
    async fn retained_session_times_out_when_peer_does_not_read_command() {
        let socket_path = std::env::temp_dir().join(format!(
            "agentdp-control-blocked-write-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let listener = socket::bind_local_socket(&socket_path)
            .await
            .expect("bind control socket");
        let (host, guest) = tokio::join!(socket::connect_local_socket(&socket_path), listener.accept());
        let mut session = Session::new(host.expect("connect control socket"));
        let _non_reading_guest = guest.expect("accept control socket");
        let contents = vec![b'x'; 4 * 1024 * 1024];

        let error = super::write_user_file(
            &Context::quiet(),
            &mut session,
            ".codex/config.toml",
            &contents,
            "0600",
            Duration::from_millis(25),
        )
        .await
        .expect_err("the complete command exchange must be bounded");

        assert!(error.to_string().contains("guest did not answer"));
        assert!(!session.is_usable());
        drop(listener);
        let _removed = std::fs::remove_file(socket_path);
    }

    #[test]
    fn observer_finishes_on_bootstrap_finished() {
        let mut observer = accepted_observer();
        handle(
            &mut observer,
            GuestMessage::new(
                "bootstrap_status",
                GuestMessageKind::BootstrapStatus(BootstrapStatusReport {
                    plan_id: "basic/basic-0".to_owned(),
                    plan_hash: "sha256:abc".to_owned(),
                    attempt_epoch: 0,
                    phase: BootstrapStepPhase::System,
                    status: BootstrapLifecycleStatus::Passed,
                    current_step: None,
                    completed_steps: Vec::new(),
                    failed_step: None,
                    pending_steps: Vec::new(),
                }),
            ),
        )
        .expect("passed status");
        let status = handle(
            &mut observer,
            GuestMessage::new(
                "bootstrap_0",
                GuestMessageKind::BootstrapFinished(BootstrapFinished {
                    plan_hash: "sha256:abc".to_owned(),
                    attempt_epoch: 0,
                }),
            ),
        )
        .expect("finished");

        assert_eq!(
            status,
            BootstrapStreamStatus::Terminal(BootstrapOutcome::Passed { attempt_epoch: 0 })
        );
    }

    #[test]
    fn observer_returns_bootstrap_failure_as_terminal_outcome() {
        let mut observer = accepted_observer();
        handle(
            &mut observer,
            GuestMessage::new(
                "failed_status",
                GuestMessageKind::BootstrapStatus(BootstrapStatusReport {
                    plan_id: "basic/basic-0".to_owned(),
                    plan_hash: "sha256:abc".to_owned(),
                    attempt_epoch: 3,
                    phase: BootstrapStepPhase::System,
                    status: BootstrapLifecycleStatus::Failed,
                    current_step: None,
                    completed_steps: Vec::new(),
                    failed_step: Some("system.packages".to_owned()),
                    pending_steps: vec!["system.packages".to_owned()],
                }),
            ),
        )
        .expect("failed status");

        let outcome = handle(
            &mut observer,
            GuestMessage::new(
                "failed_terminal",
                GuestMessageKind::BootstrapFailed(BootstrapFailed {
                    attempt_epoch: 3,
                    step: "system.packages".to_owned(),
                    exit_status: 12,
                    duration_ms: 5,
                    message: "package install failed".to_owned(),
                    stdout_tail: String::new(),
                    stderr_tail: "failure".to_owned(),
                }),
            ),
        )
        .expect("bootstrap failure is an observed outcome, not a transport error");

        assert!(matches!(
            outcome,
            BootstrapStreamStatus::Terminal(BootstrapOutcome::Failed { attempt_epoch: 3, .. })
        ));
    }

    #[test]
    fn observer_rejects_wrong_guest_identity() {
        let mut observer = expected_observer();
        let error = handle(
            &mut observer,
            hello_message("other", "basic-0", GuestdRole::System, GUEST_CONTROL_PROTOCOL_VERSION),
        )
        .expect_err("wrong manifest must fail");

        assert!(error.to_string().contains("does not match expected basic/basic-0"));
    }

    #[test]
    fn observer_rejects_wrong_guest_role() {
        let mut observer = expected_observer();
        let error = handle(
            &mut observer,
            hello_message("basic", "basic-0", GuestdRole::User, GUEST_CONTROL_PROTOCOL_VERSION),
        )
        .expect_err("wrong role must fail");

        assert!(error.to_string().contains("guestd role user is not system"));
    }

    #[test]
    fn observer_rejects_wrong_protocol_version() {
        let mut observer = expected_observer();
        let error = handle(
            &mut observer,
            hello_message(
                "basic",
                "basic-0",
                GuestdRole::System,
                GUEST_CONTROL_PROTOCOL_VERSION + 1,
            ),
        )
        .expect_err("wrong protocol must fail");

        assert!(error.to_string().contains("does not match host protocol version"));
    }

    #[test]
    fn observer_rejects_bootstrap_messages_before_hello() {
        let mut observer = expected_observer();
        let error = handle(
            &mut observer,
            GuestMessage::new(
                "bootstrap_0",
                GuestMessageKind::BootstrapOutput(BootstrapOutput {
                    step: "system.prep".to_owned(),
                    stream: BootstrapOutputStream::Stdout,
                    chunk: "hello".to_owned(),
                }),
            ),
        )
        .expect_err("bootstrap output before hello must fail");

        assert!(error.to_string().contains("before accepted hello"));
    }

    #[test]
    fn observer_rejects_bootstrap_events_before_status_snapshot() {
        let mut observer = expected_observer();
        handle(
            &mut observer,
            hello_message("basic", "basic-0", GuestdRole::System, GUEST_CONTROL_PROTOCOL_VERSION),
        )
        .expect("hello accepted");
        let error = handle(
            &mut observer,
            GuestMessage::new(
                "bootstrap_0",
                GuestMessageKind::BootstrapOutput(BootstrapOutput {
                    step: "system.prep".to_owned(),
                    stream: BootstrapOutputStream::Stdout,
                    chunk: "hello".to_owned(),
                }),
            ),
        )
        .expect_err("bootstrap output before status must fail");

        assert!(error.to_string().contains("before bootstrap status"));
    }

    #[test]
    fn observer_ignores_command_results_during_bootstrap() {
        let mut observer = accepted_observer();
        let status = handle(
            &mut observer,
            GuestMessage::new(
                "write_user_file",
                GuestMessageKind::CommandResult(GuestCommandResult {
                    command: WRITE_USER_FILE_COMMAND.to_owned(),
                    updated: true,
                }),
            ),
        )
        .expect("command result should not poison bootstrap readiness");

        assert_eq!(status, BootstrapStreamStatus::Running);
    }

    #[test]
    fn observer_emits_bootstrap_step_event() {
        let mut observer = accepted_observer();
        let (mut bootstrap_events, mut bootstrap_rx) = spsc::bounded(4);
        let mut events = super::BootstrapEventTarget::new(Some(&mut bootstrap_events));
        let status = observer
            .handle(
                GuestMessage::new(
                    "bootstrap_0",
                    GuestMessageKind::BootstrapStepStarted(BootstrapStepStarted {
                        step: "system.packages".to_owned(),
                        label: "Install system packages".to_owned(),
                        phase: BootstrapStepPhase::System,
                        attempt: 1,
                    }),
                ),
                &mut events,
            )
            .expect("step started");

        assert_eq!(status, BootstrapStreamStatus::Running);
        assert_eq!(
            drain_bootstrap_events(&mut bootstrap_rx),
            vec![BootstrapEvent::StepStarted {
                step: AgentInstanceBootstrapStepStatus {
                    step: "system.packages".to_owned(),
                    label: Some("Install system packages".to_owned()),
                    phase: Some(BootstrapStepPhase::System),
                    current: None,
                    total: None,
                    status: Some(BootstrapLifecycleStatus::Running),
                }
            }]
        );
    }

    #[test]
    fn observer_does_not_double_count_failed_step_in_pending_status() {
        let mut observer = accepted_observer();
        let (mut bootstrap_events, mut bootstrap_rx) = spsc::bounded(4);
        let mut events = super::BootstrapEventTarget::new(Some(&mut bootstrap_events));
        observer
            .handle(
                GuestMessage::new(
                    "bootstrap_0",
                    GuestMessageKind::BootstrapStatus(BootstrapStatusReport {
                        plan_id: "basic/basic-0".to_owned(),
                        plan_hash: "sha256:abc".to_owned(),
                        attempt_epoch: 0,
                        phase: BootstrapStepPhase::System,
                        status: BootstrapLifecycleStatus::Failed,
                        current_step: None,
                        completed_steps: vec!["system.prep".to_owned()],
                        failed_step: Some("system.packages".to_owned()),
                        pending_steps: vec![
                            "system.packages".to_owned(),
                            "user.shell".to_owned(),
                            "user.ready".to_owned(),
                        ],
                    }),
                ),
                &mut events,
            )
            .expect("status accepted");

        assert_eq!(
            drain_bootstrap_events(&mut bootstrap_rx),
            vec![
                BootstrapEvent::Diagnostic {
                    level: agentdp_core::agent::EventLevel::Info,
                    message: "bootstrap failed completed:1 pending:3 failed:system.packages".to_owned(),
                },
                BootstrapEvent::StepStarted {
                    step: AgentInstanceBootstrapStepStatus {
                        step: "system.packages".to_owned(),
                        label: None,
                        phase: Some(BootstrapStepPhase::System),
                        current: Some(2),
                        total: Some(4),
                        status: Some(BootstrapLifecycleStatus::Failed),
                    }
                }
            ]
        );
    }

    #[test]
    fn guest_messages_use_json_lines() {
        let message = GuestMessage::new(
            "msg_0",
            GuestMessageKind::Hello(GuestHello {
                protocol_version: GUEST_CONTROL_PROTOCOL_VERSION,
                guestd_role: GuestdRole::System,
                guestd_version: "0.1.0".to_owned(),
                manifest: "basic".to_owned(),
                instance: "basic-0".to_owned(),
                os: "linux".to_owned(),
                hostname: "basic-0".to_owned(),
                user: "agent".to_owned(),
            }),
        );

        let line = encode_guest_message_line(&message).expect("encode line");

        assert!(line.ends_with(b"\n"));
        assert!(!line[..line.len() - 1].contains(&b'\n'));
    }

    fn expected_observer() -> BootstrapObserver {
        BootstrapObserver::new(ExpectedGuest {
            manifest: "basic".to_owned(),
            instance: "basic-0".to_owned(),
        })
    }

    fn accepted_observer() -> BootstrapObserver {
        let mut observer = expected_observer();
        handle(
            &mut observer,
            hello_message("basic", "basic-0", GuestdRole::System, GUEST_CONTROL_PROTOCOL_VERSION),
        )
        .expect("hello accepted");
        handle(
            &mut observer,
            GuestMessage::new(
                "bootstrap_status",
                GuestMessageKind::BootstrapStatus(BootstrapStatusReport {
                    plan_id: "basic/basic-0".to_owned(),
                    plan_hash: "sha256:abc".to_owned(),
                    attempt_epoch: 0,
                    phase: BootstrapStepPhase::System,
                    status: BootstrapLifecycleStatus::Pending,
                    current_step: None,
                    completed_steps: Vec::new(),
                    failed_step: None,
                    pending_steps: Vec::new(),
                }),
            ),
        )
        .expect("status accepted");
        observer
    }

    fn handle(observer: &mut BootstrapObserver, message: GuestMessage) -> Result<BootstrapStreamStatus, super::Error> {
        let mut events = super::BootstrapEventTarget::new(None);
        observer.handle(message, &mut events)
    }

    fn drain_bootstrap_events(receiver: &mut spsc::Receiver<BootstrapEvent>) -> Vec<BootstrapEvent> {
        let mut events = Vec::new();
        receiver.drain(|event| events.push(event));
        events
    }

    fn hello_message(manifest: &str, instance: &str, role: GuestdRole, protocol_version: u16) -> GuestMessage {
        GuestMessage::new(
            "msg_0",
            GuestMessageKind::Hello(GuestHello {
                protocol_version,
                guestd_role: role,
                guestd_version: "0.1.0".to_owned(),
                manifest: manifest.to_owned(),
                instance: instance.to_owned(),
                os: "linux".to_owned(),
                hostname: instance.to_owned(),
                user: "agent".to_owned(),
            }),
        )
    }
}
