use serde::de::{self, DeserializeOwned, Deserializer, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const GUEST_CONTROL_PROTOCOL_VERSION: u16 = 1;
pub const WRITE_USER_FILE_COMMAND: &str = "user_file.write";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GuestMessage {
    pub id: String,
    #[serde(flatten)]
    pub kind: GuestMessageKind,
}

impl<'de> Deserialize<'de> for GuestMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(GuestMessageVisitor)
    }
}

struct GuestMessageVisitor;

impl<'de> Visitor<'de> for GuestMessageVisitor {
    type Value = GuestMessage;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("agentdp guest-to-host control message")
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let envelope = StrictMessageEnvelope::read(map)?;
        Ok(GuestMessage {
            id: envelope.id,
            kind: decode_guest_message_kind(&envelope.message_type, envelope.payload)?,
        })
    }
}

impl GuestMessage {
    #[must_use]
    pub fn new(id: impl Into<String>, kind: GuestMessageKind) -> Self {
        Self { id: id.into(), kind }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", content = "payload")]
pub enum GuestMessageKind {
    #[serde(rename = "guest.hello")]
    Hello(GuestHello),
    #[serde(rename = "bootstrap.status")]
    BootstrapStatus(BootstrapStatusReport),
    #[serde(rename = "bootstrap.step_started")]
    BootstrapStepStarted(BootstrapStepStarted),
    #[serde(rename = "bootstrap.output")]
    BootstrapOutput(BootstrapOutput),
    #[serde(rename = "bootstrap.step_finished")]
    BootstrapStepFinished(BootstrapStepFinished),
    #[serde(rename = "bootstrap.finished")]
    BootstrapFinished(BootstrapFinished),
    #[serde(rename = "bootstrap.failed")]
    BootstrapFailed(BootstrapFailed),
    #[serde(rename = "guest.command_result")]
    CommandResult(GuestCommandResult),
    #[serde(rename = "guest.error")]
    Error(GuestError),
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HostMessage {
    pub id: String,
    #[serde(flatten)]
    pub kind: HostMessageKind,
}

impl<'de> Deserialize<'de> for HostMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(HostMessageVisitor)
    }
}

struct HostMessageVisitor;

impl<'de> Visitor<'de> for HostMessageVisitor {
    type Value = HostMessage;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("agentdp host-to-guest control message")
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let envelope = StrictMessageEnvelope::read(map)?;
        Ok(HostMessage {
            id: envelope.id,
            kind: decode_host_message_kind(&envelope.message_type, envelope.payload)?,
        })
    }
}

struct StrictMessageEnvelope {
    id: String,
    message_type: String,
    payload: Value,
}

impl StrictMessageEnvelope {
    fn read<'de, A>(mut map: A) -> Result<Self, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut id = None;
        let mut message_type = None;
        let mut payload = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "id" => {
                    if id.replace(map.next_value::<String>()?).is_some() {
                        return Err(de::Error::duplicate_field("id"));
                    }
                }
                "type" => {
                    if message_type.replace(map.next_value::<String>()?).is_some() {
                        return Err(de::Error::duplicate_field("type"));
                    }
                }
                "payload" => {
                    if payload.replace(map.next_value::<Value>()?).is_some() {
                        return Err(de::Error::duplicate_field("payload"));
                    }
                }
                _ => return Err(de::Error::unknown_field(&key, MESSAGE_FIELDS)),
            }
        }
        Ok(Self {
            id: id.ok_or_else(|| de::Error::missing_field("id"))?,
            message_type: message_type.ok_or_else(|| de::Error::missing_field("type"))?,
            payload: payload.ok_or_else(|| de::Error::missing_field("payload"))?,
        })
    }
}

const MESSAGE_FIELDS: &[&str] = &["id", "type", "payload"];
const GUEST_MESSAGE_TYPES: &[&str] = &[
    "guest.hello",
    "bootstrap.status",
    "bootstrap.step_started",
    "bootstrap.output",
    "bootstrap.step_finished",
    "bootstrap.finished",
    "bootstrap.failed",
    "guest.command_result",
    "guest.error",
];
const HOST_MESSAGE_TYPES: &[&str] = &["host.accept", "host.cancel", "host.command"];

fn decode_guest_message_kind<E>(message_type: &str, payload: Value) -> Result<GuestMessageKind, E>
where
    E: de::Error,
{
    match message_type {
        "guest.hello" => Ok(GuestMessageKind::Hello(decode_payload(payload)?)),
        "bootstrap.status" => Ok(GuestMessageKind::BootstrapStatus(decode_payload(payload)?)),
        "bootstrap.step_started" => Ok(GuestMessageKind::BootstrapStepStarted(decode_payload(payload)?)),
        "bootstrap.output" => Ok(GuestMessageKind::BootstrapOutput(decode_payload(payload)?)),
        "bootstrap.step_finished" => Ok(GuestMessageKind::BootstrapStepFinished(decode_payload(payload)?)),
        "bootstrap.finished" => Ok(GuestMessageKind::BootstrapFinished(decode_payload(payload)?)),
        "bootstrap.failed" => Ok(GuestMessageKind::BootstrapFailed(decode_payload(payload)?)),
        "guest.command_result" => Ok(GuestMessageKind::CommandResult(decode_payload(payload)?)),
        "guest.error" => Ok(GuestMessageKind::Error(decode_payload(payload)?)),
        _ => Err(de::Error::unknown_variant(message_type, GUEST_MESSAGE_TYPES)),
    }
}

fn decode_host_message_kind<E>(message_type: &str, payload: Value) -> Result<HostMessageKind, E>
where
    E: de::Error,
{
    match message_type {
        "host.accept" => Ok(HostMessageKind::Accept(decode_payload(payload)?)),
        "host.cancel" => Ok(HostMessageKind::Cancel(decode_payload(payload)?)),
        "host.command" => Ok(HostMessageKind::Command(decode_payload(payload)?)),
        _ => Err(de::Error::unknown_variant(message_type, HOST_MESSAGE_TYPES)),
    }
}

fn decode_payload<T, E>(payload: Value) -> Result<T, E>
where
    T: DeserializeOwned,
    E: de::Error,
{
    serde_json::from_value(payload).map_err(E::custom)
}

impl HostMessage {
    #[must_use]
    pub fn new(id: impl Into<String>, kind: HostMessageKind) -> Self {
        Self { id: id.into(), kind }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", content = "payload")]
pub enum HostMessageKind {
    #[serde(rename = "host.accept")]
    Accept(HostAccept),
    #[serde(rename = "host.cancel")]
    Cancel(HostCancel),
    #[serde(rename = "host.command")]
    Command(HostCommand),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GuestHello {
    pub protocol_version: u16,
    pub guestd_role: GuestdRole,
    pub guestd_version: String,
    pub manifest: String,
    pub instance: String,
    pub os: String,
    pub hostname: String,
    pub user: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GuestdRole {
    System,
    User,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BootstrapStatusReport {
    pub plan_id: String,
    pub plan_hash: String,
    pub phase: BootstrapStepPhase,
    pub status: BootstrapLifecycleStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    pub completed_steps: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_step: Option<String>,
    pub pending_steps: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapLifecycleStatus {
    Pending,
    Running,
    Passed,
    Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BootstrapStepStarted {
    pub step: String,
    pub label: String,
    pub phase: BootstrapStepPhase,
    pub attempt: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BootstrapOutput {
    pub step: String,
    pub stream: BootstrapOutputStream,
    pub chunk: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapOutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BootstrapStepFinished {
    pub step: String,
    pub status: BootstrapStepStatus,
    pub exit_status: i32,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BootstrapFinished {
    pub plan_hash: String,
    pub status: BootstrapStepStatus,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BootstrapFailed {
    pub step: String,
    pub status: BootstrapStepStatus,
    pub exit_status: i32,
    pub duration_ms: u64,
    pub message: String,
    pub stdout_tail: String,
    pub stderr_tail: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapStepStatus {
    Passed,
    Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GuestError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GuestCommandResult {
    pub command: String,
    pub updated: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WriteUserFileCommand {
    pub path: String,
    pub contents: Vec<u8>,
    pub permissions: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostAccept {
    pub instance: String,
    pub protocol_version: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostCancel {
    pub reason: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostCommand {
    pub command: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BootstrapPlan {
    pub plan_version: u16,
    pub user: String,
    pub home: String,
    pub code_dir: String,
    pub steps: Vec<BootstrapStep>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BootstrapStep {
    pub id: String,
    pub label: String,
    pub phase: BootstrapStepPhase,
    pub depends_on: Vec<String>,
    pub resources: Vec<BootstrapStepResource>,
    pub script: String,
    pub working_directory: String,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapStepResource {
    AgentHome,
    CodeDir,
    GuestTooling,
    Mise,
    NpmGlobal,
    PackageManager,
    Systemd,
    UserDb,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapStepPhase {
    System,
    User,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        BootstrapLifecycleStatus, BootstrapStatusReport, BootstrapStepPhase, GUEST_CONTROL_PROTOCOL_VERSION,
        GuestCommandResult, GuestHello, GuestMessage, GuestMessageKind, GuestdRole, HostAccept, HostMessage,
        HostMessageKind,
    };
    use crate::server_guest::{
        decode_guest_message_line, decode_host_message_line, encode_guest_message_line, encode_host_message_line,
    };

    #[test]
    fn guest_hello_uses_control_channel_json_line() {
        let message = GuestMessage::new(
            "msg_0",
            GuestMessageKind::Hello(GuestHello {
                protocol_version: GUEST_CONTROL_PROTOCOL_VERSION,
                guestd_role: GuestdRole::System,
                guestd_version: "0.1.0".to_owned(),
                manifest: "basic".to_owned(),
                instance: "basic-0".to_owned(),
                os: "linux".to_owned(),
                hostname: "basic-0".to_owned(),
                user: "agent".to_owned(),
            }),
        );

        let line = encode_guest_message_line(&message).expect("encode guest message");
        let value = line_value(&line);

        assert_eq!(
            value,
            json!({
                "id": "msg_0",
                "type": "guest.hello",
                "payload": {
                    "protocol_version": 1,
                    "guestd_role": "system",
                    "guestd_version": "0.1.0",
                    "manifest": "basic",
                    "instance": "basic-0",
                    "os": "linux",
                    "hostname": "basic-0",
                    "user": "agent"
                }
            })
        );
        assert_eq!(decode_guest_message_line(&line).expect("decode guest message"), message);
    }

    #[test]
    fn host_accept_uses_control_channel_json_line() {
        let message = HostMessage::new(
            "msg_1",
            HostMessageKind::Accept(HostAccept {
                instance: "basic/basic-0".to_owned(),
                protocol_version: GUEST_CONTROL_PROTOCOL_VERSION,
            }),
        );

        let line = encode_host_message_line(&message).expect("encode host message");
        let value = line_value(&line);

        assert_eq!(
            value,
            json!({
                "id": "msg_1",
                "type": "host.accept",
                "payload": {
                    "instance": "basic/basic-0",
                    "protocol_version": 1
                }
            })
        );
        assert_eq!(decode_host_message_line(&line).expect("decode host message"), message);
    }

    #[test]
    fn bootstrap_status_reports_guest_authoritative_state() {
        let message = GuestMessage::new(
            "msg_2",
            GuestMessageKind::BootstrapStatus(BootstrapStatusReport {
                plan_id: "basic/basic-0".to_owned(),
                plan_hash: "sha256:abc123".to_owned(),
                phase: BootstrapStepPhase::System,
                status: BootstrapLifecycleStatus::Running,
                current_step: Some("system.packages".to_owned()),
                completed_steps: vec!["system.prep".to_owned(), "system.runtime_env".to_owned()],
                failed_step: None,
                pending_steps: vec!["system.packages".to_owned(), "system.agent_user".to_owned()],
            }),
        );

        let line = encode_guest_message_line(&message).expect("encode guest message");
        let value = line_value(&line);

        assert_eq!(
            value,
            json!({
                "id": "msg_2",
                "type": "bootstrap.status",
                "payload": {
                    "plan_id": "basic/basic-0",
                    "plan_hash": "sha256:abc123",
                    "phase": "system",
                    "status": "running",
                    "current_step": "system.packages",
                    "completed_steps": ["system.prep", "system.runtime_env"],
                    "pending_steps": ["system.packages", "system.agent_user"]
                }
            })
        );
        assert_eq!(decode_guest_message_line(&line).expect("decode guest message"), message);
    }

    #[test]
    fn guest_command_result_uses_control_channel_json_line() {
        let message = GuestMessage::new(
            "cmd_1",
            GuestMessageKind::CommandResult(GuestCommandResult {
                command: "user_file.write".to_owned(),
                updated: true,
            }),
        );

        let line = encode_guest_message_line(&message).expect("encode guest message");
        let value = line_value(&line);

        assert_eq!(
            value,
            json!({
                "id": "cmd_1",
                "type": "guest.command_result",
                "payload": {
                    "command": "user_file.write",
                    "updated": true
                }
            })
        );
        assert_eq!(decode_guest_message_line(&line).expect("decode guest message"), message);
    }

    #[test]
    fn guest_message_rejects_unknown_envelope_fields() {
        let line = br#"{"id":"msg_0","type":"guest.error","payload":{"code":"failed","message":"boom"},"extra":true}
"#;

        assert!(decode_guest_message_line(line).is_err());
    }

    #[test]
    fn host_message_rejects_unknown_payload_fields() {
        let line = br#"{"id":"msg_1","type":"host.accept","payload":{"instance":"basic/basic-0","protocol_version":1,"extra":true}}
"#;

        assert!(decode_host_message_line(line).is_err());
    }

    #[test]
    fn bootstrap_status_requires_authoritative_step_sets() {
        let line = br#"{"id":"msg_2","type":"bootstrap.status","payload":{"plan_id":"basic/basic-0","plan_hash":"sha256:abc123","phase":"system","status":"running","pending_steps":[]}}
"#;

        assert!(decode_guest_message_line(line).is_err());
    }

    #[test]
    fn host_command_requires_payload() {
        let line = br#"{"id":"msg_3","type":"host.command","payload":{"command":"ping"}}
"#;

        assert!(decode_host_message_line(line).is_err());
    }

    #[test]
    fn bootstrap_step_requires_resources() {
        let value = json!({
            "id": "step-1",
            "label": "Step 1",
            "phase": "system",
            "depends_on": [],
            "script": "true",
            "working_directory": "/",
            "timeout_seconds": 60
        });

        assert!(serde_json::from_value::<super::BootstrapStep>(value).is_err());
    }

    fn line_value(line: &[u8]) -> serde_json::Value {
        assert!(line.ends_with(b"\n"));
        crate::jsonl::decode(line).expect("decode json value")
    }
}
