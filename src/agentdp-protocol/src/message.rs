use serde::{Deserialize, Serialize};

use crate::Response;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerMessage {
    #[serde(rename = "type")]
    pub message_type: ServerMessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<Response>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<Event>,
}

impl ServerMessage {
    #[must_use]
    pub const fn response(response: Response) -> Self {
        Self {
            message_type: ServerMessageType::Response,
            response: Some(response),
            event: None,
        }
    }

    #[must_use]
    pub const fn event(event: Event) -> Self {
        Self {
            message_type: ServerMessageType::Event,
            response: None,
            event: Some(event),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServerMessageType {
    Response,
    Event,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Event {
    pub id: String,
    pub event: EventKind,
    pub level: EventLevel,
    pub message: String,
}

impl Event {
    #[must_use]
    pub fn info(id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            event: EventKind::Progress,
            level: EventLevel::Info,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Progress,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventLevel {
    Info,
    Warn,
    Error,
    Verbose,
}
