use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::Response;

#[derive(Debug, Clone)]
pub enum ServerMessage {
    Response(Response),
    Event(Event),
}

impl ServerMessage {
    #[must_use]
    pub const fn response(response: Response) -> Self {
        Self::Response(response)
    }

    #[must_use]
    pub const fn event(event: Event) -> Self {
        Self::Event(event)
    }

    #[must_use]
    pub fn into_response(self) -> Option<Response> {
        match self {
            Self::Response(response) => Some(response),
            Self::Event(_) => None,
        }
    }

    #[must_use]
    pub fn into_event(self) -> Option<Event> {
        match self {
            Self::Event(event) => Some(event),
            Self::Response(_) => None,
        }
    }
}

impl Serialize for ServerMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Response(response) => ServerMessageRef {
                message_type: ServerMessageType::Response,
                response: Some(response),
                event: None,
            }
            .serialize(serializer),
            Self::Event(event) => ServerMessageRef {
                message_type: ServerMessageType::Event,
                response: None,
                event: Some(event),
            }
            .serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ServerMessageType {
    Response,
    Event,
}

#[derive(Serialize)]
struct ServerMessageRef<'a> {
    #[serde(rename = "type")]
    message_type: ServerMessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    response: Option<&'a Response>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event: Option<&'a Event>,
}

impl<'de> Deserialize<'de> for ServerMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let envelope = ServerMessageEnvelope::deserialize(deserializer)?;
        match envelope.message_type {
            ServerMessageType::Response => match (envelope.response, envelope.event) {
                (Some(response), None) => Ok(Self::response(response)),
                (None, None) => Err(de::Error::custom("server response message requires response payload")),
                (None | Some(_), Some(_)) => Err(de::Error::custom(
                    "server response message does not accept event payload",
                )),
            },
            ServerMessageType::Event => match (envelope.response, envelope.event) {
                (None, Some(event)) => Ok(Self::event(event)),
                (None, None) => Err(de::Error::custom("server event message requires event payload")),
                (Some(_), None | Some(_)) => Err(de::Error::custom(
                    "server event message does not accept response payload",
                )),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerMessageEnvelope {
    #[serde(rename = "type")]
    message_type: ServerMessageType,
    response: Option<Response>,
    event: Option<Event>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Event {
    pub id: String,
    #[serde(flatten)]
    pub event: EventKind,
}

impl Event {
    #[must_use]
    pub fn diagnostic(id: impl Into<String>, level: EventLevel, message: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            event: EventKind::Diagnostic {
                level,
                message: message.into(),
            },
        }
    }

    #[must_use]
    pub fn session_stdout(id: impl Into<String>, chunk: impl Into<String>) -> Self {
        Self::session_output(id, OutputStreamResult::Stdout, chunk)
    }

    #[must_use]
    pub fn session_stderr(id: impl Into<String>, chunk: impl Into<String>) -> Self {
        Self::session_output(id, OutputStreamResult::Stderr, chunk)
    }

    fn session_output(id: impl Into<String>, stream: OutputStreamResult, chunk: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            event: EventKind::SessionOutput {
                stream,
                chunk: chunk.into(),
            },
        }
    }

    /// Builds an agent-document change event from a serializable document.
    ///
    /// # Errors
    ///
    /// Returns an error if the document cannot be represented as JSON.
    pub fn agent_document_changed(id: impl Into<String>, document: impl Serialize) -> Result<Self, serde_json::Error> {
        let document = serde_json::to_value(document)?;
        Ok(Self {
            id: id.into(),
            event: EventKind::AgentDocumentChanged { document },
        })
    }

    #[must_use]
    pub fn agent_document_value_changed(id: impl Into<String>, document: serde_json::Value) -> Self {
        Self {
            id: id.into(),
            event: EventKind::AgentDocumentChanged { document },
        }
    }

    /// Builds an agent-event stream item from a serializable event envelope.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be represented as JSON.
    pub fn agent_event(id: impl Into<String>, event: impl Serialize) -> Result<Self, serde_json::Error> {
        let item = serde_json::to_value(event)?;
        Ok(Self {
            id: id.into(),
            event: EventKind::AgentEvent { item },
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventKind {
    Diagnostic { level: EventLevel, message: String },
    SessionOutput { stream: OutputStreamResult, chunk: String },
    AgentDocumentChanged { document: serde_json::Value },
    AgentEvent { item: serde_json::Value },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OutputStreamResult {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventLevel {
    Info,
    Warn,
    Error,
    Verbose,
}
