use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
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
    Accepted,
}

pub(super) async fn queue_pr_events(url: &str, events: &[PrEvent], target_session: &str) -> Result<bool> {
    if events.is_empty() || target_session.is_empty() {
        return Ok(false);
    }

    let mut client = AhpClient::connect(url).await?;
    client.initialize().await?;
    let mut event_ids = events.iter().map(|event| event.id.as_str()).collect::<Vec<_>>();
    event_ids.sort_unstable();
    event_ids.dedup();
    let message_id = format!("agentdp-pr-{}", stable_hash_hex(&event_ids.join("\n")));
    let marker = format!("<agentdp_delivery>{message_id}</agentdp_delivery>");
    let prompt = format!("{marker}\n{}", render_prompt(events));

    // Subscribing is AHP's restore path for an evicted session. Catalog listing
    // is insufficient because a provider may temporarily expose the same
    // durable thread under a different URI while its session alias is evicted.
    let session = client.subscribe(target_session).await?;
    let session_status = session
        .pointer("/snapshot/state/status")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::Message(format!("Agent Host session {target_session} has no status")))?;
    if session_status & SESSION_ARCHIVED != 0 {
        return Ok(false);
    }
    let chat = session
        .pointer("/snapshot/state/defaultChat")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Message(format!("Agent Host session {target_session} has no default chat")))?
        .to_owned();
    let snapshot = client.subscribe(&chat).await?;
    if delivery_state(&snapshot, &message_id, &marker) == DeliveryState::Accepted {
        return Ok(true);
    }
    client.queue_message(&chat, &message_id, &prompt).await?;
    // Agent Host owns delivery after accepting the queued action. Waiting for
    // turn completion lets an overlapping user prompt erase the marker and
    // makes the next poll deliver the same event again.
    Ok(true)
}

fn delivery_state(subscription: &Value, message_id: &str, marker: &str) -> DeliveryState {
    let Some(state) = subscription.pointer("/snapshot/state") else {
        return DeliveryState::Absent;
    };
    if state
        .get("turns")
        .and_then(Value::as_array)
        .is_some_and(|turns| turns.iter().any(|turn| message_contains(turn.get("message"), marker)))
        || state
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
        return DeliveryState::Accepted;
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
pub(super) mod test_support {
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

    pub(crate) type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    pub(crate) async fn serve_missing_session(listener: &TcpListener, session: &str) -> TestResult<()> {
        let (mut socket, _) = accept_initialized(listener).await?;
        let subscription = receive_json(&mut socket).await?;
        assert_method(&subscription, "subscribe");
        assert_eq!(
            subscription.pointer("/params/channel").and_then(Value::as_str),
            Some(session)
        );
        let id = subscription
            .get("id")
            .cloned()
            .ok_or_else(|| std::io::Error::other("request omitted id"))?;
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32004, "message": "session not found" }
                })
                .to_string()
                .into(),
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn serve_delivery_attempt(listener: &TcpListener, session: &str, status: u64) -> TestResult<()> {
        let (mut socket, client_id) = accept_initialized(listener).await?;

        let session_subscription = receive_json(&mut socket).await?;
        assert_method(&session_subscription, "subscribe");
        assert_eq!(
            session_subscription.pointer("/params/channel").and_then(Value::as_str),
            Some(session)
        );
        let chat = format!("ahp-chat://default/{}", session.replace([':', '/'], "-"));
        let state = if status & super::SESSION_ARCHIVED == 0 {
            json!({ "status": status, "defaultChat": chat })
        } else {
            json!({ "status": status })
        };
        send_result(
            &mut socket,
            &session_subscription,
            json!({
                "snapshot": {
                    "channel": session,
                    "serverSeq": 1,
                    "state": state
                }
            }),
        )
        .await?;
        if status & super::SESSION_ARCHIVED != 0 {
            return Ok(());
        }

        let chat_subscription = receive_json(&mut socket).await?;
        assert_method(&chat_subscription, "subscribe");
        assert_eq!(
            chat_subscription.pointer("/params/channel").and_then(Value::as_str),
            Some(chat.as_str())
        );
        send_result(
            &mut socket,
            &chat_subscription,
            json!({
                "snapshot": {
                    "channel": chat,
                    "serverSeq": 2,
                    "state": { "turns": [] }
                }
            }),
        )
        .await?;

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
        assert!(
            dispatch
                .pointer("/params/action/message/text")
                .and_then(Value::as_str)
                .is_some_and(|prompt| prompt.contains("<agentdp_delivery>agentdp-pr-"))
        );
        let action = dispatch
            .pointer("/params/action")
            .cloned()
            .ok_or_else(|| std::io::Error::other("dispatch omitted action"))?;
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "method": "action",
                    "params": {
                        "channel": chat,
                        "serverSeq": 3,
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

    async fn accept_initialized(listener: &TcpListener) -> TestResult<(WebSocketStream<TcpStream>, String)> {
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
        Ok((socket, client_id))
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

    fn assert_method(message: &Value, expected: &str) {
        assert_eq!(message.get("method").and_then(Value::as_str), Some(expected));
    }

    async fn send_result(socket: &mut WebSocketStream<TcpStream>, request: &Value, result: Value) -> TestResult<()> {
        let id = request
            .get("id")
            .cloned()
            .ok_or_else(|| std::io::Error::other("request omitted id"))?;
        socket
            .send(Message::Text(
                json!({ "jsonrpc": "2.0", "id": id, "result": result })
                    .to_string()
                    .into(),
            ))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{TestResult, serve_delivery_attempt};
    use super::{DeliveryState, delivery_state, queue_pr_events, queued_message_outcome};
    use crate::user::github_pr::PrEvent;
    use serde_json::json;
    use tokio::net::TcpListener;

    #[test]
    fn classifies_accepted_delivery_from_durable_chat_state() {
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

        assert_eq!(delivery_state(&complete, message_id, marker), DeliveryState::Accepted);
        assert_eq!(delivery_state(&failed, message_id, marker), DeliveryState::Accepted);
        assert_eq!(delivery_state(&queued, message_id, marker), DeliveryState::Accepted);
        assert_eq!(delivery_state(&active, message_id, marker), DeliveryState::Accepted);
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
    async fn direct_session_subscription_restores_the_registered_session() -> TestResult<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { serve_delivery_attempt(&listener, "claude:/target", 1).await });
        let events = [PrEvent {
            id: "event-1".to_owned(),
            line: "pr=#1 event=review".to_owned(),
        }];

        let delivered = queue_pr_events(&format!("ws://{address}"), &events, "claude:/target").await?;

        assert!(delivered);
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn archived_session_is_not_delivered_to() -> TestResult<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { serve_delivery_attempt(&listener, "claude:/archived", 65).await });
        let events = [PrEvent {
            id: "event-1".to_owned(),
            line: "pr=#1 event=review".to_owned(),
        }];

        let delivered = queue_pr_events(&format!("ws://{address}"), &events, "claude:/archived").await?;

        assert!(!delivered);
        server.await??;
        Ok(())
    }
}
