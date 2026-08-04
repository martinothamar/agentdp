use std::path::Path;
use std::sync::Arc;

use agentdp_platform::socket::AsyncLocalSocket;

use super::local_protocol::{Request, Response, read_request, write_response};
use crate::Result;
use crate::user::GithubPrService;

#[derive(Debug)]
pub(crate) struct ControlHandler {
    github_pr: Arc<GithubPrService>,
}

impl ControlHandler {
    pub(crate) const fn new(github_pr: Arc<GithubPrService>) -> Self {
        Self { github_pr }
    }

    pub(crate) async fn handle_stream(&self, mut stream: AsyncLocalSocket) {
        let response = match read_request(&mut stream).await {
            Ok(request) => match self.handle_request(request).await {
                Ok(response) => response,
                Err(error) => Response::error(error.to_string()),
            },
            Err(error) => Response::error(error.to_string()),
        };
        if let Err(error) = write_response(&mut stream, &response).await {
            eprintln!("guestd: {error}");
        }
    }

    async fn handle_request(&self, request: Request) -> Result<Response> {
        match request {
            Request::Ping => Ok(Response::ok("pong")),
            Request::PrRegister { target, cwd } => {
                let entry = self.github_pr.register(target.as_deref(), Path::new(&cwd)).await?;
                Ok(Response::ok(entry.url))
            }
            Request::PrUnregister { target, cwd } => Ok(Response::ok(
                self.github_pr.unregister(target.as_deref(), Path::new(&cwd)).await?,
            )),
            Request::PrList => Ok(Response::with_prs(self.github_pr.list().await?)),
        }
    }
}
