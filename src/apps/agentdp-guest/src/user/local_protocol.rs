use agentdp_platform::socket::AsyncLocalSocket;
use agentdp_protocol::jsonl::{self, JsonLineReader, ReadJsonLine};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub(crate) enum Request {
    Ping,
    PrRegister { target: Option<String>, cwd: String },
    PrUnregister { target: Option<String>, cwd: String },
    PrList,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Response {
    ok: bool,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    prs: Option<Vec<PrListItem>>,
}

impl Response {
    pub(crate) fn ok(message: impl Into<String>) -> Self {
        Self {
            ok: true,
            message: message.into(),
            prs: None,
        }
    }

    pub(crate) const fn with_prs(prs: Vec<PrListItem>) -> Self {
        Self {
            ok: true,
            message: String::new(),
            prs: Some(prs),
        }
    }

    pub(crate) fn error(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            message: message.into(),
            prs: None,
        }
    }

    pub(crate) const fn is_ok(&self) -> bool {
        self.ok
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn prs(&self) -> Option<&[PrListItem]> {
        self.prs.as_deref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrListItem {
    pub number: u64,
    pub url: String,
    pub branch: Option<String>,
}

pub(crate) async fn read_request(stream: &mut AsyncLocalSocket) -> Result<Request> {
    let mut reader = JsonLineReader::default();
    let mut frame = Vec::new();
    match jsonl::read::<Request, _>(&mut reader, stream, &mut frame).await? {
        ReadJsonLine::Value(request) => Ok(request),
        ReadJsonLine::Eof => Err(Error::Message("guest daemon socket closed before request".to_owned())),
    }
}

pub(crate) async fn write_request(stream: &mut AsyncLocalSocket, request: &Request) -> Result<()> {
    let mut frame = Vec::new();
    jsonl::encode_into(request, &mut frame)?;
    stream.write_all(&frame).await.map_err(Error::WriteResponse)?;
    stream.shutdown_write().await.map_err(Error::WriteResponse)
}

pub(crate) async fn read_response(stream: &mut AsyncLocalSocket) -> Result<Response> {
    let mut reader = JsonLineReader::default();
    let mut frame = Vec::new();
    match jsonl::read::<Response, _>(&mut reader, stream, &mut frame).await? {
        ReadJsonLine::Value(response) => Ok(response),
        ReadJsonLine::Eof => Err(Error::Message("guest daemon socket closed before response".to_owned())),
    }
}

pub(crate) async fn write_response(stream: &mut AsyncLocalSocket, response: &Response) -> Result<()> {
    let mut frame = Vec::new();
    jsonl::encode_into(response, &mut frame)?;
    stream.write_all(&frame).await.map_err(Error::WriteResponse)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::Request;

    #[test]
    fn pr_requests_include_the_callers_working_directory() {
        let cwd = "/data/home/code/altinn-studio".to_owned();

        assert_eq!(
            serde_json::to_value(Request::PrRegister {
                target: Some("19634".to_owned()),
                cwd: cwd.clone(),
            })
            .expect("serialize register request"),
            json!({
                "command": "pr_register",
                "target": "19634",
                "cwd": "/data/home/code/altinn-studio",
            })
        );
        assert_eq!(
            serde_json::to_value(Request::PrUnregister { target: None, cwd }).expect("serialize unregister request"),
            json!({
                "command": "pr_unregister",
                "target": null,
                "cwd": "/data/home/code/altinn-studio",
            })
        );
    }
}
