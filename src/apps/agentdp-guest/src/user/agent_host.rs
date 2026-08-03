use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use super::github_pr::{PrEvent, render_prompt, stable_hash_hex};
use crate::{Error, Result};

const AHP_PROTOCOL_VERSION: &str = "0.7.0";
const AHP_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_ARCHIVED: u64 = 1 << 6;
const QUEUE_MESSAGE_CLIENT_SEQUENCE: u64 = 1;

#[derive(Debug, PartialEq, Eq)]
enum DeliveryState {
    Absent,
    Pending,
    Complete,
}

pub(super) async fn queue_pr_events(url: &str, events: &[PrEvent], repo_paths: &[String]) -> Result<bool> {
    if events.is_empty() || repo_paths.is_empty() {
        return Ok(false);
    }

    let mut client = AhpClient::connect(url).await?;
    client.initialize().await?;
    let sessions = client.list_sessions().await?;
    let mut event_ids = events.iter().map(|event| event.id.as_str()).collect::<Vec<_>>();
    event_ids.sort_unstable();
    event_ids.dedup();
    let message_id = format!("agentdp-pr-{}", stable_hash_hex(&event_ids.join("\n")));
    let marker = format!("<agentdp_delivery>{message_id}</agentdp_delivery>");
    let prompt = format!("{marker}\n{}", render_prompt(events));

    let matching_sessions = sessions
        .iter()
        .filter(|session| session_covers_repositories(session, repo_paths))
        .collect::<Vec<_>>();
    let mut available = Vec::new();
    let mut pending = false;
    for session in matching_sessions {
        let (chat, snapshot) = client.default_chat(&session.resource).await?;
        match delivery_state(&snapshot, &message_id, &marker) {
            DeliveryState::Complete => return Ok(true),
            DeliveryState::Pending => pending = true,
            DeliveryState::Absent if session.status & SESSION_ARCHIVED == 0 => available.push(chat),
            DeliveryState::Absent => {}
        }
    }

    if pending {
        return Ok(false);
    }
    let [chat] = available.as_slice() else {
        return Ok(false);
    };
    client.queue_message(chat, &message_id, &prompt).await?;
    Ok(false)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionSummary {
    resource: String,
    provider: String,
    status: u64,
    #[serde(default)]
    working_directories: Vec<String>,
}

fn session_covers_repositories(session: &SessionSummary, repo_paths: &[String]) -> bool {
    session.provider == "codex"
        && repo_paths.iter().all(|repo| {
            session.working_directories.iter().any(|directory| {
                file_uri_path(directory).is_some_and(|working_directory| Path::new(repo).starts_with(working_directory))
            })
        })
}

fn delivery_state(subscription: &Value, message_id: &str, marker: &str) -> DeliveryState {
    let Some(state) = subscription.pointer("/snapshot/state") else {
        return DeliveryState::Absent;
    };
    if state.get("turns").and_then(Value::as_array).is_some_and(|turns| {
        turns.iter().any(|turn| {
            turn.get("state").and_then(Value::as_str) == Some("complete")
                && message_contains(turn.get("message"), marker)
        })
    }) {
        return DeliveryState::Complete;
    }
    if state
        .get("queuedMessages")
        .and_then(Value::as_array)
        .is_some_and(|messages| {
            messages.iter().any(|pending| {
                pending.get("id").and_then(Value::as_str) == Some(message_id)
                    || message_contains(pending.get("message"), marker)
            })
        })
        || state
            .get("activeTurn")
            .is_some_and(|turn| message_contains(turn.get("message"), marker))
    {
        return DeliveryState::Pending;
    }
    DeliveryState::Absent
}

fn message_contains(message: Option<&Value>, marker: &str) -> bool {
    message
        .and_then(|message| message.get("text"))
        .and_then(Value::as_str)
        .is_some_and(|text| text.contains(marker))
}

fn queued_message_outcome(message: &Value, chat: &str, message_id: &str, client_id: &str) -> Option<Result<()>> {
    let own_action = message.get("method").and_then(Value::as_str) == Some("action")
        && message.pointer("/params/channel").and_then(Value::as_str) == Some(chat)
        && message.pointer("/params/action/type").and_then(Value::as_str) == Some("chat/pendingMessageSet")
        && message.pointer("/params/action/id").and_then(Value::as_str) == Some(message_id)
        && message.pointer("/params/origin/clientId").and_then(Value::as_str) == Some(client_id)
        && message.pointer("/params/origin/clientSeq").and_then(Value::as_u64) == Some(QUEUE_MESSAGE_CLIENT_SEQUENCE);
    if !own_action {
        return None;
    }
    Some(
        message
            .pointer("/params/rejectionReason")
            .and_then(Value::as_str)
            .map_or(Ok(()), |reason| {
                Err(Error::Message(format!("Agent Host rejected queued message: {reason}")))
            }),
    )
}

fn file_uri_path(uri: &str) -> Option<PathBuf> {
    let encoded = uri.strip_prefix("file://")?;
    if !encoded.starts_with('/') {
        return None;
    }
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        let high = hex_value(*bytes.get(index + 1)?)?;
        let low = hex_value(*bytes.get(index + 2)?)?;
        decoded.push((high << 4) | low);
        index += 3;
    }
    String::from_utf8(decoded).ok().map(PathBuf::from)
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

type AgentHostWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct AhpClient {
    socket: AgentHostWebSocket,
    next_request_id: u64,
    client_id: String,
}

impl AhpClient {
    async fn connect(url: &str) -> Result<Self> {
        let (socket, _) = connect_async(url)
            .await
            .map_err(|error| Error::Message(format!("failed to connect to Agent Host at {url}: {error}")))?;
        let connection_marker = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        Ok(Self {
            socket,
            next_request_id: 1,
            client_id: format!("agentdp-guestd-{}-{connection_marker}", std::process::id()),
        })
    }

    async fn initialize(&mut self) -> Result<()> {
        let result = self
            .call(
                "initialize",
                json!({
                    "channel": "ahp-root://",
                    "protocolVersions": [AHP_PROTOCOL_VERSION],
                    "clientId": self.client_id,
                    "clientInfo": {
                        "name": "agentdp-guestd",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
            )
            .await?;
        if result.get("protocolVersion").and_then(Value::as_str) != Some(AHP_PROTOCOL_VERSION) {
            return Err(Error::Message(format!(
                "Agent Host negotiated an unexpected AHP version: {}",
                result.get("protocolVersion").unwrap_or(&Value::Null)
            )));
        }
        Ok(())
    }

    async fn list_sessions(&mut self) -> Result<Vec<SessionSummary>> {
        let result = self
            .call(
                "listSessions",
                json!({
                    "channel": "ahp-root://"
                }),
            )
            .await?;
        serde_json::from_value(
            result
                .get("items")
                .cloned()
                .ok_or_else(|| Error::Message("Agent Host listSessions result omitted items".to_owned()))?,
        )
        .map_err(Error::from)
    }

    async fn default_chat(&mut self, session: &str) -> Result<(String, Value)> {
        let result = self.subscribe(session).await?;
        let chat = result
            .pointer("/snapshot/state/defaultChat")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| Error::Message(format!("Agent Host session {session} has no default chat")))?;
        let snapshot = self.subscribe(&chat).await?;
        Ok((chat, snapshot))
    }

    async fn subscribe(&mut self, channel: &str) -> Result<Value> {
        self.call("subscribe", json!({ "channel": channel })).await
    }

    async fn queue_message(&mut self, chat: &str, message_id: &str, prompt: &str) -> Result<()> {
        self.send(json!({
            "jsonrpc": "2.0",
            "method": "dispatchAction",
            "params": {
                "channel": chat,
                "clientSeq": QUEUE_MESSAGE_CLIENT_SEQUENCE,
                "action": {
                    "type": "chat/pendingMessageSet",
                    "kind": "queued",
                    "id": message_id,
                    "message": {
                        "text": prompt,
                        "origin": { "kind": "user" }
                    }
                }
            }
        }))
        .await?;

        loop {
            let message = self.receive().await?;
            if self.respond_to_server_request(&message).await? {
                continue;
            }
            if let Some(outcome) = queued_message_outcome(&message, chat, message_id, &self.client_id) {
                return outcome;
            }
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        self.send(json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params
        }))
        .await?;

        loop {
            let message = self.receive().await?;
            if self.respond_to_server_request(&message).await? {
                continue;
            }
            if message.get("id").and_then(Value::as_u64) != Some(request_id) {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(Error::Message(format!("Agent Host {method} failed: {error}")));
            }
            return message
                .get("result")
                .cloned()
                .ok_or_else(|| Error::Message(format!("Agent Host {method} response omitted result")));
        }
    }

    async fn send(&mut self, value: Value) -> Result<()> {
        let text = serde_json::to_string(&value)?;
        self.socket
            .send(Message::Text(text.into()))
            .await
            .map_err(|error| Error::Message(format!("failed to send AHP message: {error}")))
    }

    async fn receive(&mut self) -> Result<Value> {
        timeout(AHP_OPERATION_TIMEOUT, async {
            loop {
                let message = self
                    .socket
                    .next()
                    .await
                    .ok_or_else(|| Error::Message("Agent Host closed the AHP connection".to_owned()))?
                    .map_err(|error| Error::Message(format!("failed to receive AHP message: {error}")))?;
                match message {
                    Message::Text(text) => return serde_json::from_str(text.as_str()).map_err(Error::from),
                    Message::Ping(payload) => {
                        self.socket
                            .send(Message::Pong(payload))
                            .await
                            .map_err(|error| Error::Message(format!("failed to reply to Agent Host ping: {error}")))?;
                    }
                    Message::Close(_) => {
                        return Err(Error::Message("Agent Host closed the AHP connection".to_owned()));
                    }
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        })
        .await
        .map_err(|_| Error::Message("timed out waiting for Agent Host AHP response".to_owned()))?
    }

    async fn respond_to_server_request(&mut self, message: &Value) -> Result<bool> {
        let Some(request_id) = message.get("id") else {
            return Ok(false);
        };
        if message.get("method").and_then(Value::as_str).is_none() {
            return Ok(false);
        }
        self.send(json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32601,
                "message": "agentdp automation does not provide client-side AHP methods"
            }
        }))
        .await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DeliveryState, SessionSummary, delivery_state, file_uri_path, queue_pr_events, queued_message_outcome,
        session_covers_repositories,
    };
    use crate::user::github_pr::PrEvent;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use std::path::Path;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

    type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    fn session(resource: &str, status: u64, directories: &[&str]) -> SessionSummary {
        SessionSummary {
            resource: resource.to_owned(),
            provider: "codex".to_owned(),
            status,
            working_directories: directories.iter().map(|value| (*value).to_owned()).collect(),
        }
    }

    #[test]
    fn matches_codex_sessions_covering_every_repository_including_archived_sessions() {
        let repositories = [
            "/data/home/code/altinn-studio".to_owned(),
            "/data/home/code/app-lib-dotnet".to_owned(),
        ];

        assert!(session_covers_repositories(
            &session("codex:/archived", 65, &["file:///data/home/code"]),
            &repositories
        ));
        assert!(!session_covers_repositories(
            &session("codex:/other", 1, &["file:///data/home/other"]),
            &repositories
        ));
    }

    #[test]
    fn decodes_file_uri_paths_without_treating_prefixes_as_directories() {
        let path = file_uri_path("file:///data/home/code/repo%20name");

        assert_eq!(path.as_deref(), Some(Path::new("/data/home/code/repo name")));
        assert!(!Path::new("/data/home/code-other").starts_with(Path::new("/data/home/code")));
    }

    #[test]
    fn classifies_delivery_from_durable_chat_state() {
        let message_id = "agentdp-pr-batch";
        let marker = "<agentdp_delivery>agentdp-pr-batch</agentdp_delivery>";
        let message = json!({ "text": marker, "origin": { "kind": "user" } });

        let complete = json!({
            "snapshot": { "state": { "turns": [{ "state": "complete", "message": message }] } }
        });
        let failed = json!({
            "snapshot": { "state": { "turns": [{ "state": "error", "message": message }] } }
        });
        let queued = json!({
            "snapshot": { "state": { "queuedMessages": [{ "id": message_id, "message": message }] } }
        });
        let active = json!({
            "snapshot": { "state": { "activeTurn": { "message": message } } }
        });

        assert_eq!(delivery_state(&complete, message_id, marker), DeliveryState::Complete);
        assert_eq!(delivery_state(&failed, message_id, marker), DeliveryState::Absent);
        assert_eq!(delivery_state(&queued, message_id, marker), DeliveryState::Pending);
        assert_eq!(delivery_state(&active, message_id, marker), DeliveryState::Pending);
        assert_eq!(
            delivery_state(&json!({ "snapshot": { "state": {} } }), message_id, marker),
            DeliveryState::Absent
        );
    }

    #[test]
    fn accepts_only_its_own_non_rejected_action_echo() {
        let action = |client_id: &str, rejection: Option<&str>| {
            json!({
                "method": "action",
                "params": {
                    "channel": "ahp-chat://default/target",
                    "origin": { "clientId": client_id, "clientSeq": 1 },
                    "rejectionReason": rejection,
                    "action": { "type": "chat/pendingMessageSet", "id": "message-1" }
                }
            })
        };

        assert!(
            queued_message_outcome(
                &action("other-client", None),
                "ahp-chat://default/target",
                "message-1",
                "agentdp-client"
            )
            .is_none()
        );
        assert!(
            queued_message_outcome(
                &action("agentdp-client", None),
                "ahp-chat://default/target",
                "message-1",
                "agentdp-client"
            )
            .expect("own action should have an outcome")
            .is_ok()
        );
        let rejected = queued_message_outcome(
            &action("agentdp-client", Some("not allowed")),
            "ahp-chat://default/target",
            "message-1",
            "agentdp-client",
        )
        .expect("own action should have an outcome");
        assert!(rejected.is_err());
    }

    #[tokio::test]
    async fn keeps_events_until_the_marked_turn_completes_even_if_the_session_is_archived() -> TestResult<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(serve_mock_agent_host(listener));
        let events = [PrEvent {
            id: "event-1".to_owned(),
            line: "pr=#1 event=review".to_owned(),
        }];
        let repositories = ["/data/home/code/altinn-studio".to_owned()];

        let dispatched = queue_pr_events(&format!("ws://{address}"), &events, &repositories).await?;
        let completed = queue_pr_events(&format!("ws://{address}"), &events, &repositories).await?;

        assert!(!dispatched);
        assert!(completed);
        server.await??;
        Ok(())
    }

    async fn serve_mock_agent_host(listener: TcpListener) -> TestResult<()> {
        let (mut socket, client_id) = subscribe_mock_client(&listener, 1, json!({ "turns": [] })).await?;
        let dispatch = receive_json(&mut socket).await?;
        assert_eq!(dispatch.get("method").and_then(Value::as_str), Some("dispatchAction"));
        assert_eq!(
            dispatch.pointer("/params/action/type").and_then(Value::as_str),
            Some("chat/pendingMessageSet")
        );
        assert_eq!(
            dispatch.pointer("/params/action/kind").and_then(Value::as_str),
            Some("queued")
        );
        let action = dispatch
            .pointer("/params/action")
            .cloned()
            .ok_or_else(|| std::io::Error::other("dispatch omitted action"))?;
        let prompt = action
            .pointer("/message/text")
            .and_then(Value::as_str)
            .ok_or_else(|| std::io::Error::other("dispatch omitted prompt"))?
            .to_owned();
        assert!(prompt.contains("<agentdp_delivery>agentdp-pr-"));
        send_action(&mut socket, &action, "another-client", 3).await?;
        send_action(&mut socket, &action, &client_id, 4).await?;
        drop(socket);

        let completed_state = json!({
            "turns": [{
                "state": "complete",
                "message": { "text": prompt, "origin": { "kind": "user" } }
            }]
        });
        let _ = subscribe_mock_client(&listener, 65, completed_state).await?;
        Ok(())
    }

    async fn subscribe_mock_client(
        listener: &TcpListener,
        session_status: u64,
        chat_state: Value,
    ) -> TestResult<(WebSocketStream<TcpStream>, String)> {
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;

        let initialize = receive_json(&mut socket).await?;
        let client_id = initialize
            .pointer("/params/clientId")
            .and_then(Value::as_str)
            .ok_or_else(|| std::io::Error::other("initialize omitted client ID"))?
            .to_owned();
        send_result(
            &mut socket,
            &initialize,
            json!({ "protocolVersion": "0.7.0", "serverSeq": 0, "snapshots": [] }),
        )
        .await?;

        let list = receive_json(&mut socket).await?;
        assert_eq!(list.get("method").and_then(Value::as_str), Some("listSessions"));
        send_result(
            &mut socket,
            &list,
            json!({
                "items": [{
                    "resource": "codex:/target",
                    "provider": "codex",
                    "status": session_status,
                    "workingDirectories": ["file:///data/home/code"]
                }]
            }),
        )
        .await?;

        let session_subscription = receive_json(&mut socket).await?;
        assert_eq!(
            session_subscription.pointer("/params/channel").and_then(Value::as_str),
            Some("codex:/target")
        );
        send_result(
            &mut socket,
            &session_subscription,
            json!({
                "snapshot": {
                    "channel": "codex:/target",
                    "serverSeq": 1,
                    "state": { "defaultChat": "ahp-chat://default/target" }
                }
            }),
        )
        .await?;

        let chat_subscription = receive_json(&mut socket).await?;
        assert_eq!(
            chat_subscription.pointer("/params/channel").and_then(Value::as_str),
            Some("ahp-chat://default/target")
        );
        send_result(
            &mut socket,
            &chat_subscription,
            json!({
                "snapshot": {
                    "channel": "ahp-chat://default/target",
                    "serverSeq": 2,
                    "state": chat_state
                }
            }),
        )
        .await?;
        Ok((socket, client_id))
    }

    async fn send_action(
        socket: &mut WebSocketStream<TcpStream>,
        action: &Value,
        client_id: &str,
        server_sequence: u64,
    ) -> TestResult<()> {
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "method": "action",
                    "params": {
                        "channel": "ahp-chat://default/target",
                        "serverSeq": server_sequence,
                        "origin": { "clientId": client_id, "clientSeq": 1 },
                        "action": action
                    }
                })
                .to_string()
                .into(),
            ))
            .await?;
        Ok(())
    }

    async fn receive_json(socket: &mut WebSocketStream<TcpStream>) -> TestResult<Value> {
        loop {
            let message = socket
                .next()
                .await
                .ok_or_else(|| std::io::Error::other("websocket closed"))??;
            if let Message::Text(text) = message {
                return Ok(serde_json::from_str(text.as_str())?);
            }
        }
    }

    async fn send_result(socket: &mut WebSocketStream<TcpStream>, request: &Value, result: Value) -> TestResult<()> {
        let id = request
            .get("id")
            .cloned()
            .ok_or_else(|| std::io::Error::other("request omitted id"))?;
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": result
                })
                .to_string()
                .into(),
            ))
            .await?;
        Ok(())
    }
}
