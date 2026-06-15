use serde::{Deserialize, Serialize};

use super::BackendKind;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PingResult {
    pub service: String,
    pub pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executable: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShutdownResult {
    pub shutdown: bool,
    pub pid: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerDoctorResult {
    pub backend: BackendKind,
    pub checks: Vec<DoctorCheckResult>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DoctorCheckResult {
    pub name: String,
    pub status: String,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceLogsResult {
    pub name: String,
    pub file: String,
    pub path: String,
    pub lines: usize,
    pub contents: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceExecResult {
    pub name: String,
    pub command: Vec<String>,
    pub exit_status: u64,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceListResult {
    pub instances: Vec<AgentInstanceListItem>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceShellResult {
    pub name: String,
    pub command: HostCommandResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostCommandResult {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceListItem {
    pub name: String,
    pub agent: String,
    pub instance: String,
    pub instance_id: u32,
    pub status: String,
    pub stale: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<bool>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::AgentInstanceListItem;

    #[test]
    fn instance_list_item_requires_stale_field() {
        let value = json!({
            "name": "altinn-studio/0",
            "agent": "altinn-studio",
            "instance": "0",
            "instance_id": 0,
            "status": "created"
        });

        assert!(serde_json::from_value::<AgentInstanceListItem>(value).is_err());
    }
}
