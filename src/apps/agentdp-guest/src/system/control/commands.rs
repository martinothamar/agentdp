use agentdp_protocol::jsonl::{self, JsonLineReader};

use agentdp_protocol::server_guest::{
    GuestCommandResult, GuestMessage, GuestMessageKind, HostCommand, HostMessage, HostMessageKind,
    RETRY_BOOTSTRAP_COMMAND, RetryBootstrapCommand, WRITE_USER_FILE_COMMAND,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{Error, Result};

use super::super::seed::SeedSpec;
use super::channel::ControlChannelSink;

const USER_FILE_WORKER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

#[derive(Clone)]
pub(in crate::system) struct HostCommandContext {
    pub(in crate::system) user: String,
    pub(in crate::system) home: String,
    pub(in crate::system) bootstrap_plan_hash: String,
    pub(in crate::system) worker_executable: std::path::PathBuf,
    pub(in crate::system) worker_timeout: std::time::Duration,
}

impl HostCommandContext {
    pub(in crate::system) fn from_seed(
        seed: &SeedSpec,
        bootstrap_plan_hash: &str,
        worker_executable: std::path::PathBuf,
    ) -> Self {
        Self {
            user: seed.user_name().to_owned(),
            home: seed.user_home().to_owned(),
            bootstrap_plan_hash: bootstrap_plan_hash.to_owned(),
            worker_executable,
            worker_timeout: USER_FILE_WORKER_TIMEOUT,
        }
    }
}

pub(in crate::system) struct HostMessageWait {
    pub(in crate::system) handled: u64,
    pub(in crate::system) action: Option<HostControlAction>,
}

pub(in crate::system) enum HostControlAction {
    RetryBootstrap { id: String, request: RetryBootstrapCommand },
}

pub(super) enum HostCommandFailure {
    NotReady(String),
    Failed(Error),
}

impl HostCommandFailure {
    pub(super) fn not_ready(message: impl Into<String>) -> Self {
        Self::NotReady(message.into())
    }

    const fn code(&self) -> &'static str {
        match self {
            Self::NotReady(_) => "not_ready",
            Self::Failed(_) => "host_command_failed",
        }
    }
}

impl std::fmt::Display for HostCommandFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotReady(message) => formatter.write_str(message),
            Self::Failed(error) => error.fmt(formatter),
        }
    }
}

impl From<Error> for HostCommandFailure {
    fn from(error: Error) -> Self {
        Self::Failed(error)
    }
}

pub(in crate::system) async fn wait_for_host_messages<W>(
    control: &mut W,
    context: &HostCommandContext,
) -> Result<HostMessageWait>
where
    W: AsyncRead + AsyncWrite + Unpin,
{
    let mut sink = ControlChannelSink::new(control);
    let mut line_reader = JsonLineReader::default();
    let mut frame = Vec::new();
    let mut handled = 0;
    loop {
        if !line_reader.read_line(&mut sink.writer, &mut frame).await? || !frame.ends_with(b"\n") {
            return Ok(HostMessageWait { handled, action: None });
        }
        let message = jsonl::decode::<HostMessage>(&frame)?;
        let action = handle_host_message(message, context, &mut sink).await?;
        handled += 1;
        if action.is_some() {
            return Ok(HostMessageWait { handled, action });
        }
    }
}

async fn handle_host_message<W>(
    message: HostMessage,
    context: &HostCommandContext,
    sink: &mut ControlChannelSink<W>,
) -> Result<Option<HostControlAction>>
where
    W: AsyncWrite + Unpin,
{
    match message.kind {
        HostMessageKind::Command(command) => handle_host_command(message.id, command, context, sink).await,
    }
}

async fn handle_host_command<W>(
    id: String,
    command: HostCommand,
    context: &HostCommandContext,
    sink: &mut ControlChannelSink<W>,
) -> Result<Option<HostControlAction>>
where
    W: AsyncWrite + Unpin,
{
    if command.command == RETRY_BOOTSTRAP_COMMAND {
        return match parse_bootstrap_retry(command.payload, context) {
            Ok(request) => Ok(Some(HostControlAction::RetryBootstrap { id, request })),
            Err(error) => {
                sink.emit_error_with_id(id, error.code(), error.to_string()).await?;
                Ok(None)
            }
        };
    }
    let result = match command.command.as_str() {
        WRITE_USER_FILE_COMMAND => super::user_file::write(command.payload, context).await,
        other => Err(Error::Message(format!("unknown host command {other}")).into()),
    };
    match result {
        Ok(updated) => {
            sink.emit_message(&GuestMessage::new(
                id,
                GuestMessageKind::CommandResult(GuestCommandResult {
                    command: command.command,
                    updated,
                }),
            ))
            .await?;
            Ok(None)
        }
        Err(error) => {
            sink.emit_error_with_id(id, error.code(), error.to_string()).await?;
            Ok(None)
        }
    }
}

fn parse_bootstrap_retry(
    payload: serde_json::Value,
    context: &HostCommandContext,
) -> std::result::Result<RetryBootstrapCommand, HostCommandFailure> {
    let retry = serde_json::from_value::<RetryBootstrapCommand>(payload).map_err(Error::from)?;
    if retry.plan_hash != context.bootstrap_plan_hash {
        return Err(Error::Message("bootstrap retry plan hash does not match the seeded plan".to_owned()).into());
    }
    Ok(retry)
}
