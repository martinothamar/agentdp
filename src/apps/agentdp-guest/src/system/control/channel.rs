use std::path::Path;
use std::time::{Duration, Instant};

use agentdp_protocol::jsonl;
use agentdp_protocol::server_guest::{GuestError, GuestMessage, GuestMessageKind};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{Error, Result};

use super::super::bootstrap::BootstrapEventSink;

const CONTROL_DEVICE_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_DEVICE_WAIT_DELAY: Duration = Duration::from_millis(250);

pub(in crate::system) async fn open_control_channel(path: &Path) -> Result<tokio::fs::File> {
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

pub(in crate::system) struct ControlChannelSink<W> {
    pub(super) writer: W,
    next_id: usize,
    frame: Vec<u8>,
}

impl<W> ControlChannelSink<W>
where
    W: AsyncWrite + Unpin,
{
    pub(in crate::system) const fn new(writer: W) -> Self {
        Self {
            writer,
            next_id: 0,
            frame: Vec::new(),
        }
    }

    pub(in crate::system) async fn emit_message(&mut self, message: &GuestMessage) -> Result<()> {
        jsonl::encode_into(message, &mut self.frame)?;
        self.writer.write_all(&self.frame).await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub(super) async fn emit_error_with_id(
        &mut self,
        id: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<()> {
        self.emit_message(&GuestMessage::new(
            id,
            GuestMessageKind::Error(GuestError {
                code: code.into(),
                message: message.into(),
            }),
        ))
        .await
    }

    pub(in crate::system) fn into_inner(self) -> W {
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
