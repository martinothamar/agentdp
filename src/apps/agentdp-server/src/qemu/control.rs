use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use agentdp_core::Context;
use agentdp_core::agent::{AgentInstanceBootstrapStepStatus, AgentInstanceDocument, BackendState, BootstrapEvent};
use agentdp_platform::socket::{self, AsyncLocalSocket};
use agentdp_protocol::Error as ProtocolError;
use agentdp_protocol::jsonl::JsonLineReader;
use agentdp_protocol::server_guest::{
    BootstrapFailed, BootstrapLifecycleStatus, BootstrapStatusReport, BootstrapStepFinished, BootstrapStepStarted,
    GUEST_CONTROL_PROTOCOL_VERSION, GuestCommandResult, GuestError, GuestHello, GuestMessage, GuestMessageKind,
    GuestdRole, HostCommand, HostMessage, HostMessageKind, WRITE_USER_FILE_COMMAND, WriteUserFileCommand,
    decode_guest_message_line, encode_host_message_line,
};

use super::error::{Error, ErrorKind};
use crate::backend::BootstrapEventSink;

const BOOTSTRAP_WAIT_TIMEOUT: Duration = Duration::from_mins(45);
const CONTROL_CONNECT_DELAY: Duration = Duration::from_millis(250);
const CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(1);
const CONTROL_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) async fn write_user_file(
    context: &Context,
    control_socket: &Path,
    path: &str,
    contents: &[u8],
    permissions: &str,
) -> Result<bool, Error> {
    context
        .logger()
        .verbose_with(|| format!("writing guest file {path} through {}", control_socket.display()));
    let mut stream =
        socket::connect_local_socket(control_socket)
            .await
            .map_err(|source| ErrorKind::GuestControlMessage {
                code: "guest_control_connect".to_owned(),
                message: format!("failed to connect to {}: {source}", control_socket.display()),
            })?;
    let id = "write_user_file";
    let payload = WriteUserFileCommand {
        path: path.to_owned(),
        contents: contents.to_vec(),
        permissions: permissions.to_owned(),
    };
    let message = HostMessage::new(
        id,
        HostMessageKind::Command(HostCommand {
            command: WRITE_USER_FILE_COMMAND.to_owned(),
            payload: serde_json::to_value(payload).map_err(|source| ErrorKind::GuestControlMessage {
                code: "guest_control_encode".to_owned(),
                message: source.to_string(),
            })?,
        }),
    );
    let line = encode_host_message_line(&message).map_err(|source| ErrorKind::GuestControlDecode {
        message: "host command".to_owned(),
        source,
    })?;
    stream
        .write_all(&line)
        .await
        .map_err(|source| ErrorKind::GuestControlMessage {
            code: "guest_control_write".to_owned(),
            message: source.to_string(),
        })?;
    stream.flush().await.map_err(|source| ErrorKind::GuestControlMessage {
        code: "guest_control_write".to_owned(),
        message: source.to_string(),
    })?;
    let result = tokio::time::timeout(CONTROL_COMMAND_TIMEOUT, read_command_result(&mut stream, id)).await;
    result.map_err(|_elapsed| ErrorKind::GuestControlMessage {
        code: "guest_control_timeout".to_owned(),
        message: format!(
            "guest did not answer {WRITE_USER_FILE_COMMAND} after {}s",
            CONTROL_COMMAND_TIMEOUT.as_secs()
        ),
    })?
}

pub(super) async fn wait_bootstrap(
    context: &Context,
    state: &AgentInstanceDocument,
    bootstrap_events: Option<&mut dyn BootstrapEventSink>,
) -> Result<(), Error> {
    let BackendState::Qemu(backend) = &state.status.backend;
    let control_socket = PathBuf::from(&backend.guest_control_socket);
    context.logger().verbose_with(|| {
        format!(
            "waiting up to {}s for QEMU guest bootstrap on {}",
            BOOTSTRAP_WAIT_TIMEOUT.as_secs(),
            control_socket.display()
        )
    });

    let started = Instant::now();
    let deadline = started + BOOTSTRAP_WAIT_TIMEOUT;
    let mut last_event = "guest control channel has not connected".to_owned();
    let mut observer = BootstrapObserver::new(ExpectedGuest::from_state(state));
    let mut events = BootstrapEventTarget::new(bootstrap_events);

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ErrorKind::GuestBootstrapTimeout {
                timeout_seconds: BOOTSTRAP_WAIT_TIMEOUT.as_secs(),
                last_event,
            }
            .into());
        }

        match socket::connect_local_socket(&control_socket).await {
            Ok(mut stream) => {
                "guest control channel connected".clone_into(&mut last_event);
                send_diagnostic_bootstrap_event(&mut events, "guest control channel connected".to_owned());
                let mut reader = JsonLineReader::default();
                let mut frame = Vec::new();
                let mut read_state = BootstrapReadState {
                    events: &mut events,
                    observer: &mut observer,
                    reader: &mut reader,
                    frame: &mut frame,
                    last_event: &mut last_event,
                };
                match read_bootstrap_stream(&mut stream, &mut read_state, deadline).await? {
                    StreamResult::Finished => return Ok(()),
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
    reader: &'a mut JsonLineReader,
    frame: &'a mut Vec<u8>,
    last_event: &'a mut String,
}

async fn read_bootstrap_stream(
    stream: &mut AsyncLocalSocket,
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
            state.reader.read_line(stream, state.frame),
        )
        .await;
        match read {
            Ok(Ok(false)) => {
                "guest control channel closed".clone_into(state.last_event);
                return Ok(StreamResult::Disconnected);
            }
            Ok(Ok(true)) => {
                let message =
                    decode_guest_message_line(state.frame).map_err(|source| ErrorKind::GuestControlDecode {
                        message: String::from_utf8_lossy(state.frame).trim_end().to_owned(),
                        source,
                    })?;
                *state.last_event = BootstrapObserver::describe(&message);
                if state.observer.handle(message, state.events)? == BootstrapStreamStatus::Finished {
                    return Ok(StreamResult::Finished);
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

async fn read_command_result(stream: &mut AsyncLocalSocket, expected_id: &str) -> Result<bool, Error> {
    let mut reader = JsonLineReader::default();
    let mut frame = Vec::new();
    loop {
        if !reader
            .read_line(stream, &mut frame)
            .await
            .map_err(|source| ErrorKind::GuestControlDecode {
                message: "guest control command response".to_owned(),
                source,
            })?
        {
            return Err(ErrorKind::GuestControlMessage {
                code: "guest_control_eof".to_owned(),
                message: "guest closed control channel before command response".to_owned(),
            }
            .into());
        }
        let message = decode_guest_message_line(&frame).map_err(|source| ErrorKind::GuestControlDecode {
            message: String::from_utf8_lossy(&frame).trim_end().to_owned(),
            source,
        })?;
        if message.id != expected_id {
            continue;
        }
        return command_result_from_message(message);
    }
}

fn command_result_from_message(message: GuestMessage) -> Result<bool, Error> {
    match message.kind {
        GuestMessageKind::CommandResult(GuestCommandResult { command, updated })
            if command == WRITE_USER_FILE_COMMAND =>
        {
            Ok(updated)
        }
        GuestMessageKind::CommandResult(result) => Err(ErrorKind::GuestControlMessage {
            code: "guest_control_command_mismatch".to_owned(),
            message: format!("guest returned result for unexpected command {}", result.command),
        }
        .into()),
        GuestMessageKind::Error(error) => Err(guest_error(error)),
        other => Err(ErrorKind::GuestControlMessage {
            code: "guest_control_unexpected_message".to_owned(),
            message: format!("guest returned unexpected message {other:?}"),
        }
        .into()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamResult {
    Finished,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BootstrapStreamStatus {
    Running,
    Finished,
}

#[derive(Debug)]
struct BootstrapObserver {
    expected: ExpectedGuest,
    accepted_hello: bool,
    last_status: Option<BootstrapStatusKey>,
}

impl BootstrapObserver {
    const fn new(expected: ExpectedGuest) -> Self {
        Self {
            expected,
            accepted_hello: false,
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
                let completed_steps = status.completed_steps.len();
                let pending_steps = status.pending_steps.len();
                let key = BootstrapStatusKey {
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
                send_started_bootstrap_event(events, &started);
            }
            GuestMessageKind::BootstrapOutput(_) | GuestMessageKind::CommandResult(_) => {
                self.require_accepted_hello()?;
            }
            GuestMessageKind::BootstrapStepFinished(finished) => {
                self.require_accepted_hello()?;
                send_finished_bootstrap_event(events, &finished);
            }
            GuestMessageKind::BootstrapFinished(finished) => {
                self.require_accepted_hello()?;
                send_diagnostic_bootstrap_event(
                    events,
                    format!("bootstrap finished {}", step_status_name(finished.status)),
                );
                return Ok(BootstrapStreamStatus::Finished);
            }
            GuestMessageKind::BootstrapFailed(failed) => {
                self.require_accepted_hello()?;
                send_failed_bootstrap_event(events, &failed);
                return Err(bootstrap_failed(failed));
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
            GuestMessageKind::BootstrapFinished(finished) => {
                format!("bootstrap finished {}", step_status_name(finished.status))
            }
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
    let active_step = status.current_step.is_some() || status.failed_step.is_some();
    let total = status
        .completed_steps
        .len()
        .saturating_add(status.pending_steps.len())
        .saturating_add(usize::from(active_step));
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
        status: failed.status,
        exit_status: failed.exit_status,
        duration_ms: failed.duration_ms,
        message: failed.message.clone(),
    });
}

fn bootstrap_failed(failed: BootstrapFailed) -> Error {
    ErrorKind::GuestBootstrapFailed {
        step: failed.step,
        message: failed.message,
        stdout_tail: failed.stdout_tail,
        stderr_tail: failed.stderr_tail,
    }
    .into()
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
    use agentdp_core::agent::AgentInstanceBootstrapStepStatus;
    use agentdp_ds::local::spsc;
    use agentdp_protocol::server_guest::{
        BootstrapFinished, BootstrapLifecycleStatus, BootstrapOutput, BootstrapOutputStream, BootstrapStatusReport,
        BootstrapStepPhase, BootstrapStepStarted, BootstrapStepStatus, GUEST_CONTROL_PROTOCOL_VERSION,
        GuestCommandResult, GuestHello, GuestMessage, GuestMessageKind, GuestdRole, WRITE_USER_FILE_COMMAND,
        encode_guest_message_line,
    };

    use agentdp_core::agent::BootstrapEvent;

    use super::{BootstrapObserver, BootstrapStreamStatus, ExpectedGuest};

    #[test]
    fn observer_finishes_on_bootstrap_finished() {
        let mut observer = accepted_observer();
        let status = handle(
            &mut observer,
            GuestMessage::new(
                "bootstrap_0",
                GuestMessageKind::BootstrapFinished(BootstrapFinished {
                    plan_hash: "sha256:abc".to_owned(),
                    status: BootstrapStepStatus::Passed,
                }),
            ),
        )
        .expect("finished");

        assert_eq!(status, BootstrapStreamStatus::Finished);
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
    fn observer_counts_failed_status_step_as_active_event() {
        let mut observer = accepted_observer();
        let (mut bootstrap_events, mut bootstrap_rx) = spsc::bounded(4);
        let mut events = super::BootstrapEventTarget::new(Some(&mut bootstrap_events));
        observer
            .handle(
                GuestMessage::new(
                    "bootstrap_0",
                    GuestMessageKind::BootstrapStatus(BootstrapStatusReport {
                        plan_id: "plan_0".to_owned(),
                        plan_hash: "sha256:abc".to_owned(),
                        phase: BootstrapStepPhase::System,
                        status: BootstrapLifecycleStatus::Failed,
                        current_step: None,
                        completed_steps: vec!["system.prep".to_owned()],
                        failed_step: Some("system.packages".to_owned()),
                        pending_steps: vec!["user.shell".to_owned(), "user.ready".to_owned()],
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
                    message: "bootstrap failed completed:1 pending:2 failed:system.packages".to_owned(),
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
