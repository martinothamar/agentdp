use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use agentdp_core::{Context, layout::AgentdpLayout, manifest::LoadedAgentManifest};
use agentdp_protocol::client_server::{
    AgentInstanceExecParams, AgentInstanceExecResult, Event, EventKind, OutputStreamResult, RequestKind,
};
use clap::Args;
use tokio::io::{self as tokio_io, AsyncWriteExt};

use crate::server_client;

#[derive(Debug, Args)]
#[command(trailing_var_arg = true)]
pub(crate) struct Command {
    #[arg(value_name = "INSTANCE_ID")]
    pub instance_id: u32,

    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,

    #[arg(long, default_value = "300s", value_name = "DURATION")]
    timeout: String,

    #[arg(required = true)]
    #[arg(value_name = "COMMAND")]
    argv: Vec<String>,
}

pub(crate) async fn run(command: &Command, context: &Context) -> ExitCode {
    match try_run(command, context).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn try_run(command: &Command, context: &Context) -> Result<ExitCode, Error> {
    let manifest = LoadedAgentManifest::load_from_current_dir(context, command.file.as_deref()).await?;
    let layout = AgentdpLayout::resolve().map_err(Error::AgentdpLayout)?;
    let timeout_seconds = parse_duration_seconds(&command.timeout)?;
    let mut streamed_stdout = false;
    let mut streamed_stderr = false;
    let mut stream_error = None;
    let result: AgentInstanceExecResult = {
        let mut on_event = |event: Event| match event.event {
            EventKind::SessionOutput { stream, chunk } => match stream {
                OutputStreamResult::Stdout => {
                    streamed_stdout = true;
                    if stream_error.is_none()
                        && let Err(error) = std::io::stdout()
                            .write_all(chunk.as_bytes())
                            .and_then(|()| std::io::stdout().flush())
                    {
                        stream_error = Some(Error::WriteStdout(error));
                    }
                }
                OutputStreamResult::Stderr => {
                    streamed_stderr = true;
                    if stream_error.is_none()
                        && let Err(error) = std::io::stderr()
                            .write_all(chunk.as_bytes())
                            .and_then(|()| std::io::stderr().flush())
                    {
                        stream_error = Some(Error::WriteStderr(error));
                    }
                }
            },
            EventKind::Diagnostic { .. } | EventKind::AgentDocumentChanged { .. } | EventKind::AgentEvent { .. } => {
                server_client::log_event(context, event);
            }
        };
        server_client::request_with_response_timeout(
            context,
            &layout,
            RequestKind::AgentInstanceExec(AgentInstanceExecParams {
                agent: manifest.agent_name().to_owned(),
                instance_id: command.instance_id,
                command: command.argv.clone(),
                timeout_seconds: Some(timeout_seconds),
            }),
            Some(&mut on_event),
            response_timeout(timeout_seconds),
        )
        .await
        .map_err(Error::Server)?
    };
    if let Some(error) = stream_error {
        return Err(error);
    }

    if !streamed_stdout {
        let mut stdout = tokio_io::stdout();
        stdout
            .write_all(result.stdout.as_bytes())
            .await
            .map_err(Error::WriteStdout)?;
        stdout.flush().await.map_err(Error::WriteStdout)?;
    }

    if !streamed_stderr {
        let mut stderr = tokio_io::stderr();
        stderr
            .write_all(result.stderr.as_bytes())
            .await
            .map_err(Error::WriteStderr)?;
        stderr.flush().await.map_err(Error::WriteStderr)?;
    }
    Ok(exit_code(result.exit_status))
}

fn exit_code(status: u64) -> ExitCode {
    ExitCode::from(u8::try_from(status).unwrap_or(1))
}

fn parse_duration_seconds(value: &str) -> Result<u64, Error> {
    let digit_count = value.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return Err(Error::InvalidTimeout(value.to_owned()));
    }
    let number = value[..digit_count]
        .parse::<u64>()
        .map_err(|_| Error::InvalidTimeout(value.to_owned()))?;
    if number == 0 {
        return Err(Error::InvalidTimeout(value.to_owned()));
    }
    let multiplier = match &value[digit_count..] {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        _ => return Err(Error::InvalidTimeout(value.to_owned())),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| Error::InvalidTimeout(value.to_owned()))
}

const fn response_timeout(command_timeout_seconds: u64) -> Duration {
    Duration::from_secs(command_timeout_seconds).saturating_add(Duration::from_secs(30))
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("{0}")]
    AgentManifest(#[from] agentdp_core::manifest::Error),
    #[error("{0}")]
    AgentdpLayout(agentdp_core::layout::Error),
    #[error("{0}")]
    Server(server_client::Error),
    #[error("timeout must be a positive duration like 30s, 5m, or 1h: {0}")]
    InvalidTimeout(String),
    #[error("failed to write command stdout: {0}")]
    WriteStdout(std::io::Error),
    #[error("failed to write command stderr: {0}")]
    WriteStderr(std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{parse_duration_seconds, response_timeout};

    #[test]
    fn parses_timeout_duration() {
        assert_eq!(parse_duration_seconds("30s").unwrap(), 30);
        assert_eq!(parse_duration_seconds("5m").unwrap(), 300);
        assert_eq!(parse_duration_seconds("1h").unwrap(), 3600);
    }

    #[test]
    fn response_timeout_tracks_exec_timeout() {
        assert_eq!(response_timeout(120), Duration::from_secs(150));
    }
}
