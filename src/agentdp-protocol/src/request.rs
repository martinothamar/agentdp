use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::Response;
use crate::{
    InstanceCloneParams, InstanceCreateParams, InstanceExecParams, InstanceLogsParams, InstancePsParams, InstanceRef,
    ProvisioningPlanParams, ServerDoctorParams,
};

static REQUEST_FACTORY: OnceLock<RequestFactory> = OnceLock::new();

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "method", content = "params")]
pub enum RequestKind {
    #[serde(rename = "server.ping")]
    ServerPing,
    #[serde(rename = "server.shutdown")]
    ServerShutdown,
    #[serde(rename = "server.doctor")]
    ServerDoctor(ServerDoctorParams),
    #[serde(rename = "provisioning.plan")]
    ProvisioningPlan(ProvisioningPlanParams),
    #[serde(rename = "instance.create")]
    InstanceCreate(InstanceCreateParams),
    #[serde(rename = "instance.clone")]
    InstanceClone(InstanceCloneParams),
    #[serde(rename = "instance.status")]
    InstanceStatus(InstanceRef),
    #[serde(rename = "instance.up")]
    InstanceUp(InstanceRef),
    #[serde(rename = "instance.down")]
    InstanceDown(InstanceRef),
    #[serde(rename = "instance.rm")]
    InstanceRm(InstanceRef),
    #[serde(rename = "instance.logs")]
    InstanceLogs(InstanceLogsParams),
    #[serde(rename = "instance.shell")]
    InstanceShell(InstanceRef),
    #[serde(rename = "instance.exec")]
    InstanceExec(InstanceExecParams),
    #[serde(rename = "instance.ps")]
    InstancePs(InstancePsParams),
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
