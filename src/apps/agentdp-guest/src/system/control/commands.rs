use agentdp_protocol::jsonl::{self, JsonLineReader, ReadJsonLine};
use agentdp_protocol::server_guest::{
    GuestCommandResult, GuestMessage, GuestMessageKind, HostCommand, HostMessage, HostMessageKind,
    WRITE_USER_FILE_COMMAND,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{Error, Result};

use super::super::seed::SeedSpec;
use super::channel::ControlChannelSink;

pub(in crate::system) struct HostCommandContext {
    pub(super) user: String,
    pub(super) home: String,
}

impl HostCommandContext {
    pub(in crate::system) fn from_seed(seed: &SeedSpec) -> Self {
        Self {
            user: seed.user_name().to_owned(),
            home: seed.user_home().to_owned(),
        }
    }
}

pub(in crate::system) async fn wait_for_host_messages<W>(control: W, context: &HostCommandContext) -> Result<()>
where
    W: AsyncRead + AsyncWrite + Unpin,
{
    let mut sink = ControlChannelSink::new(control);
    let mut line_reader = JsonLineReader::default();
    let mut frame = Vec::new();
    loop {
        match jsonl::read::<HostMessage, _>(&mut line_reader, &mut sink.writer, &mut frame).await {
            Ok(ReadJsonLine::Value(message)) => handle_host_message(message, context, &mut sink).await?,
            Ok(ReadJsonLine::Eof) => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
}

async fn handle_host_message<W>(
    message: HostMessage,
    context: &HostCommandContext,
    sink: &mut ControlChannelSink<W>,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    match message.kind {
        HostMessageKind::Accept(_) => Ok(()),
        HostMessageKind::Command(command) => handle_host_command(message.id, command, context, sink).await,
        HostMessageKind::Cancel(cancel) => Err(Error::Message(format!(
            "host cancelled guestd system: {}",
            cancel.reason
        ))),
    }
}

async fn handle_host_command<W>(
    id: String,
    command: HostCommand,
    context: &HostCommandContext,
    sink: &mut ControlChannelSink<W>,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let result = match command.command.as_str() {
        WRITE_USER_FILE_COMMAND => super::user_file::write(command.payload, context).await,
        other => Err(Error::Message(format!("unknown host command {other}"))),
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
            .await
        }
        Err(error) => {
            sink.emit_error_with_id(id, "host_command_failed", error.to_string())
                .await
        }
    }
}
