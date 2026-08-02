mod bootstrap;
mod control;
mod os;
mod seed;

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use agentdp_protocol::server_guest::{
    GuestCommandResult, GuestError, GuestMessage, GuestMessageKind, RETRY_BOOTSTRAP_COMMAND,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::Result;

use self::bootstrap::{BootstrapEventSink, BootstrapExecutor};
use self::control::{
    ControlChannelSink, HostCommandContext, HostControlAction, HostMessageWait, open_control_channel,
    wait_for_host_messages,
};
use self::seed::SeedSpec;

const HOST_CONTROL_RECONNECT_DELAY: Duration = Duration::from_millis(250);
const HOST_CONTROL_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);
const HOST_CONTROL_WRITE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(crate) struct Config {
    pub instance_spec: PathBuf,
}

pub(crate) async fn run(config: Config) -> Result<()> {
    eprintln!("guestd system: refreshing seeded instance spec");
    let seed = SeedSpec::load(&config).await?;
    eprintln!("guestd system: opening control channel");
    let control = open_control_channel(&seed.control_path()).await?;
    let sink = ControlChannelSink::new(control);
    let hello = seed.hello_message();
    let plan_id = seed.instance.plan_id();
    let bootstrap_state_path = seed.bootstrap_state_path();
    let bootstrap_root_path = seed.bootstrap_root_path();
    let control_path = seed.control_path();
    let bootstrap = BootstrapExecutor::new(seed.plan.clone(), plan_id, bootstrap_state_path, bootstrap_root_path);
    let worker_executable = std::env::current_exe()?;
    let host_command_context = HostCommandContext::from_seed(&seed, bootstrap.plan_hash(), worker_executable);
    let mut control = SystemControl::new(sink.into_inner(), ControlPathOpener { path: control_path }, hello);
    control.initialize().await?;
    eprintln!("guestd system: running bootstrap");
    Box::pin(bootstrap.run(&mut control)).await?;
    eprintln!("guestd system: bootstrap finished");
    loop {
        match serve_host_control_sessions(&mut control, &host_command_context).await? {
            HostControlAction::RetryBootstrap { id, request } => {
                eprintln!("guestd system: retrying bootstrap at epoch {}", request.attempt_epoch);
                match bootstrap.prepare_retry(request.attempt_epoch).await {
                    Ok(updated) => {
                        control.begin_bootstrap_attempt();
                        if let Err(error) = control.send_retry_result(id, updated).await {
                            eprintln!("guestd system: failed to acknowledge bootstrap retry: {error}");
                            control.reopen().await?;
                        }
                        Box::pin(bootstrap.run(&mut control)).await?;
                    }
                    Err(error) => {
                        if let Err(reply_error) = control.send_retry_error(id, &error.to_string()).await {
                            eprintln!("guestd system: failed to reject bootstrap retry: {reply_error}");
                            control.reopen().await?;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Default)]
struct BootstrapReplay {
    status: Option<GuestMessageKind>,
    terminal: Option<GuestMessageKind>,
}

impl BootstrapReplay {
    fn observe(&mut self, event: &GuestMessageKind) {
        match event {
            GuestMessageKind::BootstrapStatus(_) => {
                self.status = Some(event.clone());
                self.terminal = None;
            }
            GuestMessageKind::BootstrapFinished(_) | GuestMessageKind::BootstrapFailed(_) => {
                self.terminal = Some(event.clone());
            }
            GuestMessageKind::Hello(_)
            | GuestMessageKind::BootstrapStepStarted(_)
            | GuestMessageKind::BootstrapOutput(_)
            | GuestMessageKind::BootstrapStepFinished(_)
            | GuestMessageKind::CommandResult(_)
            | GuestMessageKind::Error(_) => {}
        }
    }
}

struct SystemControl<W, O> {
    control: Option<W>,
    opener: O,
    hello: GuestMessage,
    replay: BootstrapReplay,
    next_bootstrap_id: usize,
    reconnect_delay: Duration,
}

impl<W, O> SystemControl<W, O>
where
    W: AsyncRead + AsyncWrite + Unpin,
    O: HostControlOpener<W>,
{
    fn new(control: W, opener: O, hello: GuestMessage) -> Self {
        Self {
            control: Some(control),
            opener,
            hello,
            replay: BootstrapReplay::default(),
            next_bootstrap_id: 0,
            reconnect_delay: HOST_CONTROL_RECONNECT_DELAY,
        }
    }

    async fn initialize(&mut self) -> Result<()> {
        if let Err(error) = self.send_replay().await {
            eprintln!("guestd system: initial host control session failed: {error}");
            self.reopen().await?;
        } else {
            self.mark_session_healthy();
        }
        Ok(())
    }

    async fn send_replay(&mut self) -> Result<()> {
        let hello = self.hello.clone();
        let status = self.replay.status.clone();
        let terminal = self.replay.terminal.clone();
        self.send_message(&hello).await?;
        if let Some(status) = status {
            self.send_message(&GuestMessage::new("bootstrap_replay_status", status))
                .await?;
        }
        if let Some(terminal) = terminal {
            self.send_message(&GuestMessage::new("bootstrap_replay_terminal", terminal))
                .await?;
        }
        Ok(())
    }

    async fn send_message(&mut self, message: &GuestMessage) -> Result<()> {
        let Some(control) = self.control.as_mut() else {
            return Err(crate::Error::Message("host control session is not open".to_owned()));
        };
        tokio::time::timeout(
            HOST_CONTROL_WRITE_TIMEOUT,
            ControlChannelSink::new(control).emit_message(message),
        )
        .await
        .map_err(|_| crate::Error::Message("host control write timed out".to_owned()))?
    }

    async fn reopen(&mut self) -> Result<()> {
        drop(self.control.take());
        loop {
            let delay = self.reconnect_delay;
            self.reconnect_delay = (self.reconnect_delay * 2).min(HOST_CONTROL_RECONNECT_MAX_DELAY);
            eprintln!(
                "guestd system: host control session closed; reopening in {}ms",
                delay.as_millis()
            );
            tokio::time::sleep(delay).await;
            match self.opener.open().await {
                Ok(control) => self.control = Some(control),
                Err(error) => {
                    eprintln!("guestd system: failed to reopen host control session: {error}");
                    continue;
                }
            }
            match self.send_replay().await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    eprintln!("guestd system: failed to initialize host control session: {error}");
                    drop(self.control.take());
                }
            }
        }
    }

    const fn mark_session_healthy(&mut self) {
        self.reconnect_delay = HOST_CONTROL_RECONNECT_DELAY;
    }

    fn begin_bootstrap_attempt(&mut self) {
        self.replay.terminal = None;
    }

    async fn send_retry_result(&mut self, id: String, updated: bool) -> Result<()> {
        self.send_message(&GuestMessage::new(
            id,
            GuestMessageKind::CommandResult(GuestCommandResult {
                command: RETRY_BOOTSTRAP_COMMAND.to_owned(),
                updated,
            }),
        ))
        .await
    }

    async fn send_retry_error(&mut self, id: String, message: &str) -> Result<()> {
        self.send_message(&GuestMessage::new(
            id,
            GuestMessageKind::Error(GuestError {
                code: "bootstrap_retry_failed".to_owned(),
                message: message.to_owned(),
            }),
        ))
        .await
    }
}

impl<W, O> BootstrapEventSink for SystemControl<W, O>
where
    W: AsyncRead + AsyncWrite + Unpin,
    O: HostControlOpener<W>,
{
    async fn emit(&mut self, event: GuestMessageKind) -> Result<()> {
        loop {
            let message = GuestMessage::new(format!("bootstrap_{}", self.next_bootstrap_id), event.clone());
            match self.send_message(&message).await {
                Ok(()) => {
                    self.next_bootstrap_id = self.next_bootstrap_id.saturating_add(1);
                    self.replay.observe(&event);
                    self.mark_session_healthy();
                    return Ok(());
                }
                Err(error) => {
                    eprintln!("guestd system: bootstrap control write failed: {error}");
                    self.reopen().await?;
                }
            }
        }
    }
}

async fn serve_host_control_sessions<W, O>(
    control: &mut SystemControl<W, O>,
    context: &HostCommandContext,
) -> Result<HostControlAction>
where
    W: AsyncRead + AsyncWrite + Unpin,
    O: HostControlOpener<W>,
{
    loop {
        let Some(session) = control.control.as_mut() else {
            control.reopen().await?;
            continue;
        };
        match wait_for_host_messages(session, context).await {
            Ok(HostMessageWait { handled, action }) => {
                if handled > 0 {
                    control.mark_session_healthy();
                }
                if let Some(action) = action {
                    return Ok(action);
                }
            }
            Err(error) => eprintln!("guestd system: host control session failed: {error}"),
        }
        control.reopen().await?;
    }
}

trait HostControlOpener<W> {
    fn open(&mut self) -> Pin<Box<dyn Future<Output = Result<W>> + Send + '_>>;
}

struct ControlPathOpener {
    path: PathBuf,
}

impl HostControlOpener<tokio::fs::File> for ControlPathOpener {
    fn open(&mut self) -> Pin<Box<dyn Future<Output = Result<tokio::fs::File>> + Send + '_>> {
        Box::pin(open_control_channel(&self.path))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agentdp_protocol::server_guest::{
        BootstrapLifecycleStatus, BootstrapPlan, BootstrapStep, BootstrapStepPhase, GUEST_CONTROL_PROTOCOL_VERSION,
        GUEST_INSTANCE_SPEC_VERSION, GuestHello, GuestInstancePaths, GuestInstanceSpec, GuestInstanceUser,
        GuestMessage, GuestMessageKind, GuestPlatform, GuestdRole, HostCommand, HostMessage, HostMessageKind,
        WRITE_USER_FILE_COMMAND, WriteUserFileCommand, decode_guest_message_line, encode_host_message_line,
    };
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};

    use super::{
        BootstrapExecutor, Config,
        control::HostCommandContext,
        seed::{SeedSpec, validate_bootstrap_plan},
    };

    #[test]
    fn bootstrap_plan_accepts_relative_seed_scripts() {
        validate_bootstrap_plan(&plan("phases/040-packages.sh"), "/run/agentdp/bootstrap")
            .expect("valid bootstrap plan");
    }

    #[test]
    fn bootstrap_plan_rejects_absolute_scripts() {
        let error = validate_bootstrap_plan(&plan("/tmp/bootstrap.sh"), "/run/agentdp/bootstrap")
            .expect_err("invalid bootstrap plan");
        assert!(error.to_string().contains("relative"));
    }

    #[test]
    fn bootstrap_plan_rejects_parent_traversal() {
        let error = validate_bootstrap_plan(&plan("../bootstrap.sh"), "/run/agentdp/bootstrap")
            .expect_err("invalid bootstrap plan");
        assert!(error.to_string().contains("path components"));
    }

    #[tokio::test]
    async fn seed_spec_loads_paths_from_instance_spec() {
        let paths = SeedFiles::write(plan("phases/040-packages.sh")).await;

        let seed = SeedSpec::load(&Config {
            instance_spec: paths.instance_spec.clone(),
        })
        .await
        .expect("load seed spec");

        assert_eq!(seed.instance.plan_id(), "basic/basic-0");
        assert_eq!(seed.control_path(), paths.control);
        assert_eq!(seed.bootstrap_state_path(), paths.bootstrap_state);
        assert_eq!(seed.bootstrap_root_path(), paths.bootstrap_root);
        assert_eq!(seed.hello_message().kind.user(), Some("agent"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bootstrap_continues_across_host_disconnect() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = TestTempDir::create("guestd-bootstrap-host-reconnect");
        let script = temp.path.join("quiet.sh");
        let release = temp.path.join("release");
        tokio::fs::write(
            &script,
            format!(
                "#!/bin/sh\nwhile [ ! -f '{}' ]; do sleep 0.01; done\n",
                release.display()
            ),
        )
        .await
        .expect("write quiet script");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("make quiet script executable");
        let bootstrap = test_bootstrap(&temp, "quiet.sh");
        let (first_host, first_guest) = tokio::io::duplex(8192);
        let (second_host, second_guest) = tokio::io::duplex(8192);
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
        let opener = TestControlOpener {
            next: Some(second_guest),
            opened: Some(opened_tx),
            gate: None,
        };
        let hello = test_hello();
        let task = tokio::spawn(run_test_bootstrap(first_guest, opener, hello, bootstrap));
        let mut first_host = BufReader::new(first_host);
        let first_messages = read_until_step_started(&mut first_host).await;
        assert!(first_messages.iter().any(|message| matches!(
            &message.kind,
            GuestMessageKind::BootstrapStepStarted(started) if started.attempt == 1
        )));

        drop(first_host);
        tokio::fs::write(&release, b"ready\n")
            .await
            .expect("release bootstrap step");
        opened_rx.await.expect("control session reopened");
        let mut second_host = BufReader::new(second_host);
        let replayed = read_guest_message(&mut second_host).await;
        assert!(matches!(replayed.kind, GuestMessageKind::Hello(_)));
        let replayed = read_guest_message(&mut second_host).await;
        assert!(matches!(
            replayed.kind,
            GuestMessageKind::BootstrapStatus(ref status)
                if status.status == BootstrapLifecycleStatus::Running
        ));

        let remaining = read_until_bootstrap_terminal(&mut second_host).await;
        let control = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("bootstrap should finish after reconnect")
            .expect("bootstrap driver should join")
            .expect("bootstrap should pass");
        drop(control);

        assert!(remaining.iter().any(|message| matches!(
            &message.kind,
            GuestMessageKind::BootstrapStatus(status)
                if status.status == BootstrapLifecycleStatus::Passed
        )));
        assert!(
            remaining
                .iter()
                .any(|message| matches!(message.kind, GuestMessageKind::BootstrapFinished(_)))
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bootstrap_replays_failure_after_host_disconnect() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = TestTempDir::create("guestd-bootstrap-failure-reconnect");
        let script = temp.path.join("fail.sh");
        let release = temp.path.join("release");
        tokio::fs::write(
            &script,
            format!(
                "#!/bin/sh\nwhile [ ! -f '{}' ]; do sleep 0.01; done\nexit 7\n",
                release.display()
            ),
        )
        .await
        .expect("write failing script");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("make failing script executable");
        let bootstrap = test_bootstrap(&temp, "fail.sh");
        let (first_host, first_guest) = tokio::io::duplex(8192);
        let (second_host, second_guest) = tokio::io::duplex(8192);
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
        let (open_gate_tx, open_gate_rx) = tokio::sync::oneshot::channel();
        let opener = TestControlOpener {
            next: Some(second_guest),
            opened: Some(opened_tx),
            gate: Some(open_gate_rx),
        };
        let hello = test_hello();
        let task = tokio::spawn(run_test_bootstrap(first_guest, opener, hello, bootstrap));
        let mut first_host = BufReader::new(first_host);
        read_until_step_started(&mut first_host).await;
        drop(first_host);
        tokio::fs::write(&release, b"fail\n")
            .await
            .expect("release failing step");
        opened_rx.await.expect("control reopen started");
        wait_for_file_text(&temp.path.join("bootstrap-state.json"), "\"status\": \"failed\"").await;
        open_gate_tx.send(()).expect("permit control reopen");

        let mut second_host = BufReader::new(second_host);
        let messages = read_until_bootstrap_terminal(&mut second_host).await;
        let control = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("failed bootstrap should finish after reconnect")
            .expect("bootstrap driver should join")
            .expect("bootstrap failure is a reported terminal state");
        drop(control);

        assert!(matches!(messages[0].kind, GuestMessageKind::Hello(_)));
        assert!(messages.iter().any(|message| matches!(
            message.kind,
            GuestMessageKind::BootstrapStatus(ref status)
                if status.status == BootstrapLifecycleStatus::Failed
        )));
        assert!(
            messages
                .iter()
                .any(|message| matches!(message.kind, GuestMessageKind::BootstrapFailed(_)))
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn noisy_bootstrap_retires_nonreading_control_session() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = TestTempDir::create("guestd-bootstrap-nonreading-host");
        let script = temp.path.join("noisy.sh");
        tokio::fs::write(
            &script,
            "#!/bin/sh\ndd if=/dev/zero bs=8192 count=256 2>/dev/null | tr '\\000' x\n",
        )
        .await
        .expect("write noisy script");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("make noisy script executable");
        let bootstrap = test_bootstrap(&temp, "noisy.sh");
        let (_nonreading_host, first_guest) = tokio::io::duplex(1024);
        let (second_host, second_guest) = tokio::io::duplex(8192);
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
        let opener = TestControlOpener {
            next: Some(second_guest),
            opened: Some(opened_tx),
            gate: None,
        };
        let hello = test_hello();
        let task = tokio::spawn(run_test_bootstrap(first_guest, opener, hello, bootstrap));

        tokio::time::timeout(Duration::from_secs(3), opened_rx)
            .await
            .expect("non-reading session should hit the write timeout")
            .expect("control session reopened");
        let mut second_host = BufReader::new(second_host);
        let messages = read_until_bootstrap_terminal(&mut second_host).await;
        let control = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("noisy bootstrap should finish after retiring blocked session")
            .expect("bootstrap driver should join")
            .expect("noisy bootstrap should pass");
        drop(control);

        assert!(matches!(messages[0].kind, GuestMessageKind::Hello(_)));
        assert!(
            messages
                .iter()
                .any(|message| matches!(message.kind, GuestMessageKind::BootstrapFinished(_)))
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn host_control_reopen_replays_bootstrap_and_serves_command() {
        let temp = TestTempDir::create("guestd-control-reopen");
        let (mut first_host, first_guest) = tokio::io::duplex(8192);
        let (mut second_host, second_guest) = tokio::io::duplex(8192);
        let context = host_command_context(&temp);
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let opener = TestControlOpener {
            next: Some(second_guest),
            opened: Some(signal_tx),
            gate: None,
        };
        let hello = test_hello();
        let replay = terminal_replay();
        let mut control = terminal_control(first_guest, opener, hello, replay);
        let task = tokio::spawn(async move { super::serve_host_control_sessions(&mut control, &context).await });

        first_host.shutdown().await.expect("close first host session");
        signal_rx.await.expect("control session reopened");

        let response = write_user_file_over_host(&mut second_host).await;
        task.abort();

        assert_user_file_written_response(&response);
        assert_eq!(
            tokio::fs::read(temp.path.join(".codex/auth.json")).await.unwrap(),
            b"{\"tokens\":\"placeholder\"}\n"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn host_control_session_error_reopens_with_bootstrap_replay() {
        let temp = TestTempDir::create("guestd-control-reopen-after-error");
        let (mut first_host, first_guest) = tokio::io::duplex(8192);
        let (mut second_host, second_guest) = tokio::io::duplex(8192);
        let context = host_command_context(&temp);
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let opener = TestControlOpener {
            next: Some(second_guest),
            opened: Some(signal_tx),
            gate: None,
        };
        let hello = test_hello();
        let replay = terminal_replay();
        let mut control = terminal_control(first_guest, opener, hello, replay);
        let task = tokio::spawn(async move { super::serve_host_control_sessions(&mut control, &context).await });

        first_host
            .write_all(b"{not valid host json}\n")
            .await
            .expect("write invalid host frame");
        first_host.shutdown().await.expect("close first host session");
        signal_rx.await.expect("control session reopened after invalid frame");

        let response = write_user_file_over_host(&mut second_host).await;
        task.abort();

        assert_user_file_written_response(&response);
        assert_eq!(
            tokio::fs::read(temp.path.join(".codex/auth.json")).await.unwrap(),
            b"{\"tokens\":\"placeholder\"}\n"
        );
    }

    async fn write_user_file_over_host(host: &mut tokio::io::DuplexStream) -> Vec<u8> {
        let mut host = BufReader::new(host);
        let mut response = Vec::new();
        loop {
            let mut line = Vec::new();
            host.read_until(b'\n', &mut line).await.expect("read bootstrap replay");
            assert!(!line.is_empty(), "guest closed before replaying bootstrap");
            let message = decode_guest_message_line(&line).expect("decode bootstrap replay");
            response.extend_from_slice(&line);
            if matches!(message.kind, GuestMessageKind::BootstrapFinished(_)) {
                break;
            }
        }

        host.get_mut()
            .write_all(
                &encode_host_message_line(&write_command(
                    "cmd_1",
                    ".codex/auth.json",
                    b"{\"tokens\":\"placeholder\"}\n",
                    "0600",
                ))
                .expect("encode command"),
            )
            .await
            .expect("write command");
        host.get_mut().shutdown().await.expect("close host write side");

        host.read_to_end(&mut response).await.expect("read command response");
        response
    }

    fn assert_user_file_written_response(response: &[u8]) {
        let messages = decode_lines(response);
        assert!(matches!(&messages[0].kind, GuestMessageKind::Hello(_)));
        assert!(
            messages
                .iter()
                .any(|message| matches!(&message.kind, GuestMessageKind::BootstrapFinished(_)))
        );
        assert!(messages.iter().any(|message| matches!(
            &message.kind,
            GuestMessageKind::CommandResult(result)
                if result.command == WRITE_USER_FILE_COMMAND && result.updated
        )));
    }

    async fn read_until_step_started<R>(reader: &mut R) -> Vec<GuestMessage>
    where
        R: tokio::io::AsyncBufRead + Unpin,
    {
        let mut messages = Vec::new();
        loop {
            let message = read_guest_message(reader).await;
            let started = matches!(&message.kind, GuestMessageKind::BootstrapStepStarted(_));
            messages.push(message);
            if started {
                return messages;
            }
        }
    }

    async fn read_until_bootstrap_terminal<R>(reader: &mut R) -> Vec<GuestMessage>
    where
        R: tokio::io::AsyncBufRead + Unpin,
    {
        let mut messages = Vec::new();
        loop {
            let message = read_guest_message(reader).await;
            let finished = matches!(
                message.kind,
                GuestMessageKind::BootstrapFinished(_) | GuestMessageKind::BootstrapFailed(_)
            );
            messages.push(message);
            if finished {
                return messages;
            }
        }
    }

    async fn read_guest_message<R>(reader: &mut R) -> GuestMessage
    where
        R: tokio::io::AsyncBufRead + Unpin,
    {
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await.expect("read guest message");
        assert!(!line.is_empty(), "guest closed before sending expected message");
        decode_guest_message_line(&line).expect("decode guest message")
    }

    async fn wait_for_file_text(path: &std::path::Path, expected: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {}",
                path.display()
            );
            if tokio::fs::read_to_string(path)
                .await
                .is_ok_and(|contents| contents.contains(expected))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn run_test_bootstrap(
        control: tokio::io::DuplexStream,
        opener: TestControlOpener,
        hello: GuestMessage,
        bootstrap: BootstrapExecutor,
    ) -> crate::Result<super::SystemControl<tokio::io::DuplexStream, TestControlOpener>> {
        let mut control = super::SystemControl::new(control, opener, hello);
        control.initialize().await?;
        bootstrap.run(&mut control).await?;
        Ok(control)
    }

    fn terminal_control(
        control: tokio::io::DuplexStream,
        opener: TestControlOpener,
        hello: GuestMessage,
        replay: super::BootstrapReplay,
    ) -> super::SystemControl<tokio::io::DuplexStream, TestControlOpener> {
        let mut control = super::SystemControl::new(control, opener, hello);
        control.replay = replay;
        control
    }

    fn test_hello() -> GuestMessage {
        GuestMessage::new(
            "msg_0",
            GuestMessageKind::Hello(GuestHello {
                protocol_version: GUEST_CONTROL_PROTOCOL_VERSION,
                guestd_role: GuestdRole::System,
                guestd_version: env!("CARGO_PKG_VERSION").to_owned(),
                manifest: "basic".to_owned(),
                instance: "basic-0".to_owned(),
                os: "linux".to_owned(),
                hostname: "basic-0".to_owned(),
                user: current_user(),
            }),
        )
    }

    fn terminal_replay() -> super::BootstrapReplay {
        let mut replay = super::BootstrapReplay::default();
        replay.observe(&GuestMessageKind::BootstrapFinished(
            agentdp_protocol::server_guest::BootstrapFinished {
                plan_hash: "plan-hash".to_owned(),
                attempt_epoch: 0,
            },
        ));
        replay
    }

    fn test_bootstrap(temp: &TestTempDir, script: &str) -> BootstrapExecutor {
        BootstrapExecutor::new(
            BootstrapPlan {
                plan_version: 1,
                user: current_user(),
                home: temp.path.display().to_string(),
                code_dir: temp.path.display().to_string(),
                steps: vec![BootstrapStep {
                    id: "system.test".to_owned(),
                    label: "Test step".to_owned(),
                    phase: BootstrapStepPhase::System,
                    depends_on: Vec::new(),
                    resources: Vec::new(),
                    script: script.to_owned(),
                    working_directory: temp.path.display().to_string(),
                    timeout_seconds: 30,
                }],
            },
            "basic/basic-0".to_owned(),
            temp.path.join("bootstrap-state.json"),
            temp.path.clone(),
        )
    }

    fn plan(script: &str) -> BootstrapPlan {
        BootstrapPlan {
            plan_version: 1,
            user: "agent".to_owned(),
            home: "/data/home".to_owned(),
            code_dir: "/data/home/code".to_owned(),
            steps: vec![BootstrapStep {
                id: "system.packages".to_owned(),
                label: "Install manifest packages".to_owned(),
                phase: BootstrapStepPhase::System,
                depends_on: vec!["system.prep".to_owned()],
                resources: Vec::new(),
                script: script.to_owned(),
                working_directory: "/".to_owned(),
                timeout_seconds: 900,
            }],
        }
    }

    fn write_command(id: &str, path: &str, contents: &[u8], permissions: &str) -> HostMessage {
        HostMessage::new(
            id,
            HostMessageKind::Command(HostCommand {
                command: WRITE_USER_FILE_COMMAND.to_owned(),
                payload: serde_json::to_value(WriteUserFileCommand {
                    path: path.to_owned(),
                    contents: contents.to_vec(),
                    permissions: permissions.to_owned(),
                })
                .unwrap(),
            }),
        )
    }

    fn current_user() -> String {
        std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "agent".to_owned())
    }

    #[cfg(unix)]
    fn host_command_context(temp: &TestTempDir) -> HostCommandContext {
        use std::os::unix::fs::PermissionsExt as _;

        let worker = temp.path.join("user-file-worker");
        std::fs::write(
            &worker,
            "#!/bin/sh\npath=${6#--path=}\ntarget=\"$5/$path\"\nmkdir -p \"${target%/*}\"\ncat > \"$target\"\nchmod \"$8\" \"$target\"\nprintf 'updated\\n'\n",
        )
        .expect("write user-file worker");
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755))
            .expect("make user-file worker executable");
        HostCommandContext {
            user: current_user(),
            home: temp.path.display().to_string(),
            bootstrap_plan_hash: "plan-hash".to_owned(),
            worker_executable: worker,
            worker_timeout: Duration::from_secs(1),
        }
    }

    struct TestControlOpener {
        next: Option<tokio::io::DuplexStream>,
        opened: Option<tokio::sync::oneshot::Sender<()>>,
        gate: Option<tokio::sync::oneshot::Receiver<()>>,
    }

    impl super::HostControlOpener<tokio::io::DuplexStream> for TestControlOpener {
        fn open(
            &mut self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<tokio::io::DuplexStream>> + Send + '_>>
        {
            Box::pin(async move {
                if let Some(opened) = self.opened.take() {
                    let _ = opened.send(());
                }
                if let Some(gate) = self.gate.take() {
                    let _ = gate.await;
                }
                self.next
                    .take()
                    .ok_or_else(|| crate::Error::Message("no test control session available".to_owned()))
            })
        }
    }

    struct TestTempDir {
        path: std::path::PathBuf,
    }

    impl TestTempDir {
        fn create(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "agentdp-{name}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    struct SeedFiles {
        root: std::path::PathBuf,
        instance_spec: std::path::PathBuf,
        control: std::path::PathBuf,
        bootstrap_root: std::path::PathBuf,
        bootstrap_state: std::path::PathBuf,
    }

    impl SeedFiles {
        async fn write(plan: BootstrapPlan) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "agentdp-guest-system-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time")
                    .as_nanos()
            ));
            let spec_dir = dir.join("spec");
            let bootstrap_root = dir.join("bootstrap");
            let manifest = spec_dir.join("agent-manifest.yaml");
            let bootstrap_plan = spec_dir.join("bootstrap-plan.json");
            let instance_spec = spec_dir.join("instance.json");
            let bootstrap_state = dir.join("state/bootstrap-state.json");
            let control = dir.join("agentdp.control");
            tokio::fs::create_dir_all(&spec_dir).await.expect("create spec dir");
            tokio::fs::create_dir_all(&bootstrap_root)
                .await
                .expect("create bootstrap dir");
            tokio::fs::write(&manifest, "name: basic\n")
                .await
                .expect("write manifest");
            tokio::fs::write(&bootstrap_plan, serde_json::to_vec(&plan).expect("serialize plan"))
                .await
                .expect("write plan");
            tokio::fs::write(
                &instance_spec,
                serde_json::to_vec(&GuestInstanceSpec {
                    schema_version: GUEST_INSTANCE_SPEC_VERSION,
                    manifest: "basic".to_owned(),
                    instance: "basic-0".to_owned(),
                    hostname: "basic-0".to_owned(),
                    platform: GuestPlatform::Linux,
                    user: GuestInstanceUser {
                        name: "agent".to_owned(),
                        home: "/data/home".to_owned(),
                        code_dir: "/data/home/code".to_owned(),
                    },
                    paths: GuestInstancePaths {
                        spec_dir: spec_dir.display().to_string(),
                        instance_spec: instance_spec.display().to_string(),
                        manifest: manifest.display().to_string(),
                        bootstrap_plan: bootstrap_plan.display().to_string(),
                        bootstrap_root: bootstrap_root.display().to_string(),
                        bootstrap_state: bootstrap_state.display().to_string(),
                        control: control.display().to_string(),
                    },
                })
                .expect("serialize instance spec"),
            )
            .await
            .expect("write instance spec");
            Self {
                root: dir,
                instance_spec,
                control,
                bootstrap_root,
                bootstrap_state,
            }
        }
    }

    impl Drop for SeedFiles {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn decode_lines(bytes: &[u8]) -> Vec<agentdp_protocol::server_guest::GuestMessage> {
        bytes
            .split_inclusive(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| decode_guest_message_line(line).expect("decode line"))
            .collect()
    }

    trait GuestMessageKindExt {
        fn user(&self) -> Option<&str>;
    }

    impl GuestMessageKindExt for GuestMessageKind {
        fn user(&self) -> Option<&str> {
            match self {
                Self::Hello(hello) => Some(&hello.user),
                _ => None,
            }
        }
    }
}
