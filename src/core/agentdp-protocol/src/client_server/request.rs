use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::de::{self, DeserializeOwned, Deserializer, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Response;
use super::{
    AgentApplyParams, AgentInstanceExecParams, AgentInstanceListParams, AgentInstanceLogsParams, AgentInstanceSelector,
    AgentScaleParams, AgentSelector, AgentWaitParams, AgentWatchParams, ServerDoctorParams,
};

static REQUEST_FACTORY: OnceLock<RequestFactory> = OnceLock::new();

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Request {
    pub id: String,
    #[serde(flatten)]
    pub kind: RequestKind,
}

impl Request {
    #[must_use]
    pub fn new(id: impl Into<String>, kind: RequestKind) -> Self {
        Self { id: id.into(), kind }
    }

    #[must_use]
    pub fn respond_with_success(&self, result: impl Serialize) -> Response {
        Response::success(self.id.clone(), result)
    }

    #[must_use]
    pub fn respond_with_failure(&self, code: impl Into<String>, message: impl Into<String>) -> Response {
        Response::failure(self.id.clone(), code, message)
    }
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RequestVisitor)
    }
}

struct RequestVisitor;

impl<'de> Visitor<'de> for RequestVisitor {
    type Value = Request;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("agentdp client/server request object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut id = None;
        let mut method = None;
        let mut params = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "id" => {
                    if id.replace(map.next_value::<String>()?).is_some() {
                        return Err(de::Error::duplicate_field("id"));
                    }
                }
                "method" => {
                    if method.replace(map.next_value::<String>()?).is_some() {
                        return Err(de::Error::duplicate_field("method"));
                    }
                }
                "params" => {
                    if params.replace(map.next_value::<Value>()?).is_some() {
                        return Err(de::Error::duplicate_field("params"));
                    }
                }
                _ => return Err(de::Error::unknown_field(&key, REQUEST_FIELDS)),
            }
        }
        let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
        let method = method.ok_or_else(|| de::Error::missing_field("method"))?;
        Ok(Request {
            id,
            kind: decode_request_kind(&method, params)?,
        })
    }
}

const REQUEST_FIELDS: &[&str] = &["id", "method", "params"];
const REQUEST_METHODS: &[&str] = &[
    "server.ping",
    "server.shutdown",
    "server.doctor",
    "agent.apply",
    "agent.scale",
    "agent.delete",
    "agent.status",
    "agent.wait",
    "agent.watch",
    "agent.instance.status",
    "agent.instance.logs",
    "agent.instance.shell",
    "agent.instance.exec",
    "agent.instance.list",
];

fn decode_request_kind<E>(method: &str, params: Option<Value>) -> Result<RequestKind, E>
where
    E: de::Error,
{
    match method {
        "server.ping" => {
            reject_params(method, params.as_ref())?;
            Ok(RequestKind::ServerPing)
        }
        "server.shutdown" => {
            reject_params(method, params.as_ref())?;
            Ok(RequestKind::ServerShutdown)
        }
        "server.doctor" => Ok(RequestKind::ServerDoctor(decode_params(method, params)?)),
        "agent.apply" => Ok(RequestKind::AgentApply(decode_params(method, params)?)),
        "agent.scale" => Ok(RequestKind::AgentScale(decode_params(method, params)?)),
        "agent.delete" => Ok(RequestKind::AgentDelete(decode_params(method, params)?)),
        "agent.status" => Ok(RequestKind::AgentStatus(decode_params(method, params)?)),
        "agent.wait" => Ok(RequestKind::AgentWait(decode_params(method, params)?)),
        "agent.watch" => Ok(RequestKind::AgentWatch(decode_params(method, params)?)),
        "agent.instance.status" => Ok(RequestKind::AgentInstanceStatus(decode_params(method, params)?)),
        "agent.instance.logs" => Ok(RequestKind::AgentInstanceLogs(decode_params(method, params)?)),
        "agent.instance.shell" => Ok(RequestKind::AgentInstanceShell(decode_params(method, params)?)),
        "agent.instance.exec" => Ok(RequestKind::AgentInstanceExec(decode_params(method, params)?)),
        "agent.instance.list" => Ok(RequestKind::AgentInstanceList(decode_params(method, params)?)),
        _ => Err(de::Error::unknown_variant(method, REQUEST_METHODS)),
    }
}

fn decode_params<T, E>(method: &str, params: Option<Value>) -> Result<T, E>
where
    T: DeserializeOwned,
    E: de::Error,
{
    let params = params.ok_or_else(|| E::custom(format!("request method {method} requires params")))?;
    serde_json::from_value(params).map_err(E::custom)
}

fn reject_params<E>(method: &str, params: Option<&Value>) -> Result<(), E>
where
    E: de::Error,
{
    if params.is_some() {
        return Err(E::custom(format!("request method {method} does not accept params")));
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "method", content = "params")]
pub enum RequestKind {
    #[serde(rename = "server.ping")]
    ServerPing,
    #[serde(rename = "server.shutdown")]
    ServerShutdown,
    #[serde(rename = "server.doctor")]
    ServerDoctor(ServerDoctorParams),
    #[serde(rename = "agent.apply")]
    AgentApply(AgentApplyParams),
    #[serde(rename = "agent.scale")]
    AgentScale(AgentScaleParams),
    #[serde(rename = "agent.delete")]
    AgentDelete(AgentSelector),
    #[serde(rename = "agent.status")]
    AgentStatus(AgentSelector),
    #[serde(rename = "agent.wait")]
    AgentWait(AgentWaitParams),
    #[serde(rename = "agent.watch")]
    AgentWatch(AgentWatchParams),
    #[serde(rename = "agent.instance.status")]
    AgentInstanceStatus(AgentInstanceSelector),
    #[serde(rename = "agent.instance.logs")]
    AgentInstanceLogs(AgentInstanceLogsParams),
    #[serde(rename = "agent.instance.shell")]
    AgentInstanceShell(AgentInstanceSelector),
    #[serde(rename = "agent.instance.exec")]
    AgentInstanceExec(AgentInstanceExecParams),
    #[serde(rename = "agent.instance.list")]
    AgentInstanceList(AgentInstanceListParams),
}

#[derive(Debug)]
pub struct RequestFactory {
    process_id: u32,
    next_id: AtomicU64,
}

impl Default for RequestFactory {
    fn default() -> Self {
        Self::new(std::process::id())
    }
}

impl RequestFactory {
    #[must_use]
    pub const fn new(process_id: u32) -> Self {
        Self {
            process_id,
            next_id: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub fn request(&self, kind: RequestKind) -> Request {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        Request::new(format!("cmd_{}_{}", self.process_id, id), kind)
    }
}

#[must_use]
pub fn request(kind: RequestKind) -> Request {
    REQUEST_FACTORY.get_or_init(RequestFactory::default).request(kind)
}
