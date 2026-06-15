use agentdp_core::Context;
use agentdp_ds::local::spsc;
use agentdp_platform as platform;
use agentdp_protocol::client_server::{self as protocol, RequestKind, Response, ServerMessage};
use agentdp_protocol::jsonl::{self, JsonLineReader, ReadJsonLine};
use std::rc::Rc;

use crate::agent::AgentRegistry;

use super::Error;
use super::request;

const CONNECTION_EVENT_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionAction {
    Continue,
    Shutdown,
}

pub(super) async fn handle_connection(
    context: &Context,
    agents: Rc<AgentRegistry>,
    mut stream: platform::socket::AsyncLocalSocket,
) -> Result<ConnectionAction, Error> {
    let mut reader = JsonLineReader::default();
    let mut frame = Vec::new();
    let request = match jsonl::read::<protocol::Request, _>(&mut reader, &mut stream, &mut frame).await {
        Ok(ReadJsonLine::Value(request)) => request,
        Ok(ReadJsonLine::Eof) => return Ok(ConnectionAction::Continue),
        Err(error) => {
            let (_reader, mut writer) = stream.split();
            let mut write_frame = Vec::new();
            write_server_message(
                &mut writer,
                ServerMessage::response(protocol::invalid_request(error.to_string())),
                &mut write_frame,
            )
            .await?;
            return Ok(ConnectionAction::Continue);
        }
    };
    let request_label = format!("{:?}", request.kind);
    if reader.buffered_len() > 0 {
        context.logger().warn(format!(
            "client sent {} unexpected buffered bytes during {request_label}",
            reader.buffered_len()
        ));
        return Ok(ConnectionAction::Continue);
    }
    context
        .logger()
        .verbose_with(|| format!("handling agentdp-server request {request_label}"));

    let (mut reader, mut writer) = stream.split();
    let mut write_frame = Vec::new();
    let client_disconnect = async move {
        let mut buffer = [0_u8; 1];
        reader.read(&mut buffer).await
    };
    tokio::pin!(client_disconnect);

    let (event_tx, mut event_rx) = spsc::bounded(CONNECTION_EVENT_CAPACITY);
    let task_context = context.clone();
    let task_agents = Rc::clone(&agents);
    let task_request = request.clone();
    let mut task = tokio::task::spawn_local(async move {
        let events = ConnectionEvents {
            events: event_tx,
            request_id: task_request.id.clone(),
        };
        request::handle(&task_context, task_agents.as_ref(), &task_request, events).await
    });

    loop {
        tokio::select! {
            disconnected = &mut client_disconnect => {
                match disconnected {
                    Ok(0) => {
                        context.logger().warn(format!(
                            "client disconnected during {request_label}"
                        ));
                    }
                    Ok(read) => {
                        context.logger().warn(format!(
                            "client sent {read} unexpected bytes during {request_label}"
                        ));
                    }
                    Err(error) => {
                        context.logger().warn(format!(
                            "client socket failed during {request_label}: {error}"
                        ));
                    }
                }
                finish_disconnected_request_task(context, &request.kind, request_label, task);
                return Ok(ConnectionAction::Continue);
            }
            event = event_rx.recv() => {
                let event = match event {
                    Ok(event) => event,
                    Err(spsc::TryRecvError::Empty | spsc::TryRecvError::Disconnected) => continue,
                };
                if let Err(error) = write_server_message(&mut writer, ServerMessage::event(event), &mut write_frame).await {
                    context.logger().warn(format!(
                        "client disconnected during {request_label}: {error}"
                    ));
                    finish_disconnected_request_task(context, &request.kind, request_label, task);
                    return Err(error);
                }
            }
            result = &mut task => {
                let (response, action) = result?;
                let mut events = Vec::new();
                event_rx.drain(|event| events.push(event));
                for event in events {
                    write_server_message(&mut writer, ServerMessage::event(event), &mut write_frame).await?;
                }
                write_server_message(&mut writer, ServerMessage::response(response), &mut write_frame).await?;
                context
                    .logger()
                    .verbose_with(|| format!("completed agentdp-server request {request_label}"));
                return Ok(action);
            }
        }
    }
}

fn finish_disconnected_request_task(
    context: &Context,
    kind: &RequestKind,
    request_label: String,
    task: tokio::task::JoinHandle<(Response, ConnectionAction)>,
) {
    if !request::continues_after_disconnect(kind) {
        task.abort();
        tokio::task::spawn_local(async move {
            let _result = task.await;
        });
        return;
    }

    let context = context.clone();
    tokio::task::spawn_local(async move {
        match task.await {
            Ok((response, action)) => {
                if let Some(error) = response.error() {
                    context.logger().warn(format!(
                        "disconnected request {request_label} completed with error {}: {}",
                        error.code, error.message
                    ));
                } else {
                    context
                        .logger()
                        .verbose_with(|| format!("disconnected request {request_label} completed"));
                }
                if action == ConnectionAction::Shutdown {
                    context
                        .logger()
                        .warn("disconnected shutdown request completed, but the shutdown response path was gone");
                }
            }
            Err(error) => {
                context
                    .logger()
                    .warn(format!("disconnected request {request_label} task failed: {error}"));
            }
        }
    });
}

async fn write_server_message(
    stream: &mut platform::socket::AsyncLocalSocketWriter,
    message: ServerMessage,
    frame: &mut Vec<u8>,
) -> Result<(), Error> {
    jsonl::encode_into(&message, frame)?;
    stream.write_all(frame).await?;
    stream.flush().await?;
    Ok(())
}

pub(crate) struct ConnectionEvents {
    events: spsc::Sender<protocol::Event>,
    request_id: String,
}

impl ConnectionEvents {
    pub(super) fn emit(&mut self, event: protocol::Event) {
        let _result = self.events.try_send(event);
    }

    pub(super) fn diagnostic(&mut self, level: protocol::EventLevel, message: impl Into<String>) {
        self.emit(protocol::Event::diagnostic(self.request_id.clone(), level, message));
    }

    pub(super) fn stdout(&mut self, chunk: String) {
        self.emit(protocol::Event::session_stdout(self.request_id.clone(), chunk));
    }

    pub(super) fn stderr(&mut self, chunk: String) {
        self.emit(protocol::Event::session_stderr(self.request_id.clone(), chunk));
    }

    pub(crate) fn agent_document(&mut self, document: impl serde::Serialize) {
        match protocol::Event::agent_document_changed(self.request_id.clone(), document) {
            Ok(event) => self.emit(event),
            Err(error) => self.diagnostic(
                protocol::EventLevel::Error,
                format!("failed to serialize agent document event: {error}"),
            ),
        }
    }

    pub(crate) fn agent_event(&mut self, event: impl serde::Serialize) {
        match protocol::Event::agent_event(self.request_id.clone(), event) {
            Ok(event) => self.emit(event),
            Err(error) => self.diagnostic(
                protocol::EventLevel::Error,
                format!("failed to serialize agent event stream item: {error}"),
            ),
        }
    }
}
