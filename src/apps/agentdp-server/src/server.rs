use std::path::{Path, PathBuf};
use std::rc::Rc;

use agentdp_core::Context;
use agentdp_platform as platform;
use agentdp_protocol::client_server as protocol;
use thiserror::Error;

use crate::agent::{AgentRegistry, AgentdpLayout};
use crate::host::tailscale::TailscaleService;
mod dispatch;
mod lock;
mod request;
mod web;

pub(crate) use dispatch::ConnectionAction;
pub(crate) use dispatch::ConnectionEvents;
use dispatch::handle_connection;
use lock::acquire_server_lock;
use tokio::sync::Notify;
use web::WebControlPlane;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("agentdp-server is already running with pid {pid}: {path}")]
    AlreadyRunning { path: PathBuf, pid: u32 },
    #[error("failed to read agentdp-server lock {path}: {source}")]
    ReadLock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write agentdp-server lock {path}: {source}")]
    WriteLock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to inspect agentdp-server lock owner pid {pid}: {source}")]
    ProcessStatus {
        pid: u32,
        #[source]
        source: platform::process::ProcessStatusError,
    },
    #[error("local socket error: {0}")]
    Socket(#[from] platform::socket::LocalSocketError),
    #[error("I/O error while handling local server connection: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] protocol::Error),
    #[error("server request task failed: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),
    #[error("agent registry failed to load: {0}")]
    AgentRegistry(#[source] crate::agent::AgentError),
}

/// Starts a local agentdp-server loop on the provided socket path.
///
/// # Errors
///
/// Returns an error when the socket cannot be bound or a connection cannot be
/// accepted.
pub(crate) async fn serve(context: &Context, layout: AgentdpLayout, socket_path: &Path) -> Result<(), Error> {
    context
        .logger()
        .verbose_with(|| format!("binding agentdp-server socket {}", socket_path.display()));
    let lock = acquire_server_lock(socket_path).await?;
    let listener = match platform::socket::bind_local_socket(socket_path).await {
        Ok(listener) => listener,
        Err(error) => {
            let _result = lock.release().await;
            return Err(Error::Socket(error));
        }
    };
    let shutdown = Rc::new(Notify::new());
    let tailscale = Rc::new(TailscaleService::new());
    let agents = match AgentRegistry::load(context.clone(), layout.clone(), Rc::clone(&tailscale)).await {
        Ok(agents) => Rc::new(agents),
        Err(error) => {
            let _result = lock.release().await;
            return Err(Error::AgentRegistry(error));
        }
    };
    let mut web = WebControlPlane::new(context, Rc::clone(&agents), layout, tailscale).await;

    let result = loop {
        tokio::select! {
            () = shutdown.notified() => {
                break Ok(());
            }
            accepted = listener.accept() => {
                let stream = match accepted {
                    Ok(stream) => stream,
                    Err(error) => break Err(Error::Io(error)),
                };
                let context = context.clone();
                let agents = Rc::clone(&agents);
                let shutdown = Rc::clone(&shutdown);
                tokio::task::spawn_local(async move { match handle_connection(&context, agents, stream).await {
                    Ok(ConnectionAction::Continue) => {}
                    Ok(ConnectionAction::Shutdown) => {
                        shutdown.notify_one();
                    }
                    Err(error) => {
                        context
                            .logger()
                            .warn(format!("failed to handle agentdp-server connection: {error}"));
                    }
                }});
            }
        }
    };
    web.stop().await;
    agents.stop().await;
    context.logger().verbose("agentdp-server shutdown requested");
    lock.release().await?;
    result
}
