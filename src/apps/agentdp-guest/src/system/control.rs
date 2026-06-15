use std::path::Path;
use std::time::{Duration, Instant};

use agentdp_protocol::jsonl::{self, JsonLineReader, ReadJsonLine};
use agentdp_protocol::server_guest::{GuestError, GuestMessage, GuestMessageKind, HostMessage, HostMessageKind};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::{Error, Result};

use super::bootstrap::BootstrapEventSink;

const CONTROL_DEVICE_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_DEVICE_WAIT_DELAY: Duration = Duration::from_millis(250);

pub(super) async fn open_control_channel(path: &Path) -> Result<tokio::fs::File> {
    let started = Instant::now();
    let mut last_error = None;
    while started.elapsed() < CONTROL_DEVICE_WAIT_TIMEOUT {
        match tokio::fs::OpenOptions::new().read(true).write(true).open(path).await {
            Ok(control) => return Ok(control),
            Err(source) => last_error = Some(source),
        }
        tokio::time::sleep(CONTROL_DEVICE_WAIT_DELAY).await;
    }
    Err(Error::Message(format!(
        "failed to open guest control channel {} after {}s: {}",
        path.display(),
        CONTROL_DEVICE_WAIT_TIMEOUT.as_secs(),
        last_error.map_or_else(|| "timed out".to_owned(), |error| error.to_string())
    )))
}

pub(super) struct ControlChannelSink<W> {
    writer: W,
    next_id: usize,
    frame: Vec<u8>,
}

impl<W> ControlChannelSink<W>
where
    W: AsyncWrite + Unpin,
{
    pub(super) const fn new(writer: W) -> Self {
        Self {
            writer,
            next_id: 0,
            frame: Vec::new(),
        }
    }

    pub(super) async fn emit_message(&mut self, message: &GuestMessage) -> Result<()> {
        jsonl::encode_into(message, &mut self.frame)?;
        self.writer.write_all(&self.frame).await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub(super) async fn emit_error(&mut self, code: impl Into<String>, message: impl Into<String>) -> Result<()> {
        self.emit_message(&GuestMessage::new(
            "guest_error",
            GuestMessageKind::Error(GuestError {
                code: code.into(),
                message: message.into(),
            }),
        ))
        .await
    }

    pub(super) fn into_inner(self) -> W {
        self.writer
    }
}

impl<W> BootstrapEventSink for ControlChannelSink<W>
where
    W: AsyncWrite + Unpin,
{
    async fn emit(&mut self, event: GuestMessageKind) -> Result<()> {
        let message = GuestMessage::new(format!("bootstrap_{}", self.next_id), event);
        self.next_id += 1;
        self.emit_message(&message).await
    }
}

pub(super) async fn wait_for_host_messages<R>(reader: R) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut reader = reader;
    let mut line_reader = JsonLineReader::default();
    let mut frame = Vec::new();
    loop {
        match jsonl::read::<HostMessage, _>(&mut line_reader, &mut reader, &mut frame).await {
            Ok(ReadJsonLine::Value(message)) => handle_host_message(message)?,
            Ok(ReadJsonLine::Eof) => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
}

fn handle_host_message(message: HostMessage) -> Result<()> {
    match message.kind {
        HostMessageKind::Accept(_) | HostMessageKind::Command(_) => Ok(()),
        HostMessageKind::Cancel(cancel) => Err(Error::Message(format!(
            "host cancelled guestd system: {}",
            cancel.reason
        ))),
    }
}
