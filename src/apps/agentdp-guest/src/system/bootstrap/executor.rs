use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;

use agentdp_platform::text::Utf8Stream;
use agentdp_protocol::server_guest::{
    BootstrapOutput, BootstrapOutputStream, BootstrapStep, BootstrapStepPhase, GuestMessageKind,
};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::time::{Duration, sleep};

use super::BootstrapEventSink;
use crate::system::os;
use crate::{Error, Result};

const OUTPUT_TAIL_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone)]
pub(super) struct StepExecutor {
    script_root: PathBuf,
    user: String,
    home: String,
}

impl StepExecutor {
    pub(super) fn new(script_root: PathBuf, user: impl Into<String>, home: impl Into<String>) -> Self {
        Self {
            script_root,
            user: user.into(),
            home: home.into(),
        }
    }

    pub(super) async fn run(&self, step: &BootstrapStep, sink: &mut impl BootstrapEventSink) -> Result<StepOutput> {
        let script = self.script_root.join(&step.script);
        let started = Instant::now();
        let mut command = step_command(step, &script, &self.user, &self.home)?;
        let mut child = command
            .spawn()
            .map_err(|source| Error::Message(format!("failed to start bootstrap step {}: {source}", step.id)))?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Message(format!("failed to capture stdout for bootstrap step {}", step.id)))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::Message(format!("failed to capture stderr for bootstrap step {}", step.id)))?;

        let output = collect_step_output(step, sink, &mut child, &mut stdout, &mut stderr).await?;
        Ok(StepOutput {
            duration_ms: millis(started),
            ..output
        })
    }
}

#[derive(Debug)]
pub(super) struct StepOutput {
    pub(super) exit_status: i32,
    pub(super) timed_out: bool,
    pub(super) duration_ms: u64,
    pub(super) stdout_tail: String,
    pub(super) stderr_tail: String,
}

fn step_command(step: &BootstrapStep, script: &Path, user: &str, home: &str) -> Result<Command> {
    let mut command = Command::new(script);
    command
        .current_dir(&step.working_directory)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if step.phase == BootstrapStepPhase::User {
        os::configure_user_command(&mut command, user, home)?;
    }
    Ok(command)
}

async fn collect_step_output(
    step: &BootstrapStep,
    sink: &mut impl BootstrapEventSink,
    child: &mut Child,
    stdout: &mut ChildStdout,
    stderr: &mut ChildStderr,
) -> Result<StepOutput> {
    let mut stdout_open = true;
    let mut stderr_open = true;
    let mut status = None;
    let mut timed_out = false;
    let mut stdout_tail = String::new();
    let mut stderr_tail = String::new();
    let mut stdout_text = Utf8Stream::default();
    let mut stderr_text = Utf8Stream::default();
    let mut stdout_buffer = [0_u8; 8192];
    let mut stderr_buffer = [0_u8; 8192];
    let timeout = sleep(Duration::from_secs(step.timeout_seconds));
    tokio::pin!(timeout);

    while status.is_none() || stdout_open || stderr_open {
        tokio::select! {
            wait_status = child.wait(), if status.is_none() => {
                status = Some(wait_status.map_err(|source| {
                    Error::Message(format!("failed to wait for bootstrap step {}: {source}", step.id))
                })?);
            }
            () = &mut timeout, if status.is_none() => {
                timed_out = true;
                child.kill().await.map_err(|source| {
                    Error::Message(format!("failed to stop timed-out bootstrap step {}: {source}", step.id))
                })?;
                status = Some(child.wait().await.map_err(|source| {
                    Error::Message(format!(
                        "failed to wait for timed-out bootstrap step {}: {source}",
                        step.id
                    ))
                })?);
            }
            read = stdout.read(&mut stdout_buffer), if stdout_open => {
                let bytes = read?;
                stdout_open = bytes != 0;
                if bytes != 0 {
                    emit_output(
                        sink,
                        step,
                        BootstrapOutputStream::Stdout,
                        &stdout_buffer[..bytes],
                        &mut stdout_text,
                        &mut stdout_tail,
                    ).await?;
                }
            }
            read = stderr.read(&mut stderr_buffer), if stderr_open => {
                let bytes = read?;
                stderr_open = bytes != 0;
                if bytes != 0 {
                    emit_output(
                        sink,
                        step,
                        BootstrapOutputStream::Stderr,
                        &stderr_buffer[..bytes],
                        &mut stderr_text,
                        &mut stderr_tail,
                    ).await?;
                }
            }
        }
    }
    let status =
        status.ok_or_else(|| Error::Message(format!("bootstrap step {} did not report an exit status", step.id)))?;
    flush_output(
        sink,
        step,
        BootstrapOutputStream::Stdout,
        &mut stdout_text,
        &mut stdout_tail,
    )
    .await?;
    flush_output(
        sink,
        step,
        BootstrapOutputStream::Stderr,
        &mut stderr_text,
        &mut stderr_tail,
    )
    .await?;

    Ok(StepOutput {
        exit_status: status.code().unwrap_or(-1),
        timed_out,
        duration_ms: 0,
        stdout_tail,
        stderr_tail,
    })
}

async fn emit_output(
    sink: &mut impl BootstrapEventSink,
    step: &BootstrapStep,
    stream: BootstrapOutputStream,
    bytes: &[u8],
    decoder: &mut Utf8Stream,
    tail: &mut String,
) -> Result<()> {
    if let Some(chunk) = decoder.push(bytes) {
        emit_text_output(sink, step, stream, chunk, tail).await?;
    }
    Ok(())
}

async fn flush_output(
    sink: &mut impl BootstrapEventSink,
    step: &BootstrapStep,
    stream: BootstrapOutputStream,
    decoder: &mut Utf8Stream,
    tail: &mut String,
) -> Result<()> {
    if let Some(chunk) = decoder.finish() {
        emit_text_output(sink, step, stream, chunk, tail).await?;
    }
    Ok(())
}

async fn emit_text_output(
    sink: &mut impl BootstrapEventSink,
    step: &BootstrapStep,
    stream: BootstrapOutputStream,
    chunk: String,
    tail: &mut String,
) -> Result<()> {
    append_tail(tail, &chunk);
    sink.emit(GuestMessageKind::BootstrapOutput(BootstrapOutput {
        step: step.id.clone(),
        stream,
        chunk,
    }))
    .await
}

fn tail_text(value: &str) -> String {
    let start = value.len().saturating_sub(OUTPUT_TAIL_BYTES);
    String::from_utf8_lossy(&value.as_bytes()[start..]).into_owned()
}

fn append_tail(tail: &mut String, chunk: &str) {
    tail.push_str(chunk);
    if tail.len() > OUTPUT_TAIL_BYTES {
        *tail = tail_text(tail);
    }
}

fn millis(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[tokio::test(flavor = "current_thread")]
    async fn runs_system_step_script_and_reports_success() -> TestResult {
        let temp = TestTemp::new("success")?;
        temp.write_script("ok.sh", "exit 0").await?;
        let mut sink = Vec::new();
        let step = step("system.ok", "ok.sh", &temp.path_text, 30);

        let output = Box::pin(executor(&temp).run(&step, &mut sink)).await?;

        assert_eq!(output.exit_status, 0);
        assert!(!output.timed_out);
        assert!(output.duration_ms < u64::MAX);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn captures_stdout_and_stderr_streams() -> TestResult {
        let temp = TestTemp::new("streams")?;
        temp.write_script("streams.sh", "printf stdout-text\nprintf stderr-text >&2")
            .await?;
        let mut sink = Vec::new();
        let step = step("system.streams", "streams.sh", &temp.path_text, 30);

        Box::pin(executor(&temp).run(&step, &mut sink)).await?;

        assert!(sink.iter().any(|event| {
            matches!(
                event,
                GuestMessageKind::BootstrapOutput(output)
                    if output.stream == BootstrapOutputStream::Stdout
                        && output.chunk.contains("stdout-text")
            )
        }));
        assert!(sink.iter().any(|event| {
            matches!(
                event,
                GuestMessageKind::BootstrapOutput(output)
                    if output.stream == BootstrapOutputStream::Stderr
                        && output.chunk.contains("stderr-text")
            )
        }));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn preserves_utf8_split_across_chunks() -> TestResult {
        let step = step("system.utf8", "utf8.sh", "/", 30);
        let mut sink = Vec::new();
        let mut decoder = Utf8Stream::default();
        let mut tail = String::new();

        emit_output(
            &mut sink,
            &step,
            BootstrapOutputStream::Stdout,
            &[0xE2, 0x82],
            &mut decoder,
            &mut tail,
        )
        .await?;
        emit_output(
            &mut sink,
            &step,
            BootstrapOutputStream::Stdout,
            &[0xAC],
            &mut decoder,
            &mut tail,
        )
        .await?;

        assert_eq!(sink.len(), 1);
        assert!(matches!(&sink[0], GuestMessageKind::BootstrapOutput(output) if output.chunk == "\u{20ac}"));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn keeps_bounded_stdout_and_stderr_tails() -> TestResult {
        let temp = TestTemp::new("tails")?;
        temp.write_script(
            "tails.sh",
            "printf '%*s' 9000 '' | tr ' ' A\nprintf TAIL\nprintf '%*s' 9000 '' | tr ' ' B >&2\nprintf ERRTAIL >&2",
        )
        .await?;
        let mut sink = Vec::new();
        let step = step("system.tails", "tails.sh", &temp.path_text, 30);

        let output = Box::pin(executor(&temp).run(&step, &mut sink)).await?;

        assert!(output.stdout_tail.len() <= OUTPUT_TAIL_BYTES);
        assert!(output.stdout_tail.ends_with("TAIL"));
        assert!(output.stderr_tail.len() <= OUTPUT_TAIL_BYTES);
        assert!(output.stderr_tail.ends_with("ERRTAIL"));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn returns_nonzero_exit_status() -> TestResult {
        let temp = TestTemp::new("nonzero")?;
        temp.write_script("nonzero.sh", "exit 23").await?;
        let mut sink = Vec::new();
        let step = step("system.nonzero", "nonzero.sh", &temp.path_text, 30);

        let output = Box::pin(executor(&temp).run(&step, &mut sink)).await?;

        assert_eq!(output.exit_status, 23);
        assert!(!output.timed_out);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn times_out_and_kills_step() -> TestResult {
        let temp = TestTemp::new("timeout")?;
        temp.write_script("timeout.sh", "while true; do :; done").await?;
        let mut sink = Vec::new();
        let step = step("system.timeout", "timeout.sh", &temp.path_text, 1);

        let output = Box::pin(executor(&temp).run(&step, &mut sink)).await?;

        assert!(output.timed_out);
        assert_ne!(output.exit_status, 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn uses_working_directory() -> TestResult {
        let temp = TestTemp::new("working-dir")?;
        let work_dir = temp.root.join("work");
        tokio::fs::create_dir_all(&work_dir).await?;
        temp.write_script("pwd.sh", "pwd > pwd.txt").await?;
        let mut sink = Vec::new();
        let work_dir_text = work_dir.to_string_lossy().into_owned();
        let step = step("system.pwd", "pwd.sh", &work_dir_text, 30);

        let output = Box::pin(executor(&temp).run(&step, &mut sink)).await?;

        assert_eq!(output.exit_status, 0);
        assert_eq!(
            tokio::fs::read_to_string(work_dir.join("pwd.txt")).await?.trim(),
            work_dir_text
        );
        Ok(())
    }

    fn executor(temp: &TestTemp) -> StepExecutor {
        StepExecutor::new(temp.root.join("scripts"), "agent", "/data/home")
    }

    fn step(id: &str, script: &str, working_directory: &str, timeout_seconds: u64) -> BootstrapStep {
        BootstrapStep {
            id: id.to_owned(),
            label: id.to_owned(),
            phase: BootstrapStepPhase::System,
            depends_on: Vec::new(),
            resources: Vec::new(),
            script: script.to_owned(),
            working_directory: working_directory.to_owned(),
            timeout_seconds,
        }
    }

    struct TestTemp {
        root: PathBuf,
        path_text: String,
    }

    impl TestTemp {
        fn new(name: &str) -> std::io::Result<Self> {
            let root = std::env::temp_dir().join(format!("agentdp-step-executor-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(root.join("scripts"))?;
            let path_text = root.to_string_lossy().into_owned();
            Ok(Self { root, path_text })
        }

        async fn write_script(&self, name: &str, contents: &str) -> std::io::Result<()> {
            let path = self.root.join("scripts").join(name);
            tokio::fs::write(&path, format!("#!/usr/bin/env bash\nset -euo pipefail\n{contents}")).await?;
            tokio::fs::set_permissions(&path, Permissions::from_mode(0o700)).await
        }
    }

    impl Drop for TestTemp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}
