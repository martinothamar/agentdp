use std::sync::Arc;
use std::time::Duration;

use agentdp_platform::fs::{ensure_private_dir, set_private_file};
use agentdp_platform::socket;

use crate::user::{CodexSessionService, ControlHandler, GithubPrService, RuntimePaths};
use crate::user::{local_socket_io_error, remove_stale_socket};
use crate::{Error, Result};

const DEFAULT_POLL_SECONDS: u64 = 60;
const DEFAULT_IDLE_SECONDS: u64 = 20;

pub(crate) async fn run() -> Result<()> {
    let paths = RuntimePaths::discover()?;
    ensure_private_dir(&paths.state_dir, "state").await?;
    ensure_private_dir(&paths.queue_dir, "state").await?;
    ensure_private_dir(&paths.socket_dir, "socket").await?;
    remove_stale_socket(&paths.socket).await?;
    let listener = socket::bind_local_socket(&paths.socket)
        .await
        .map_err(|error| Error::BindSocket {
            path: paths.socket.clone(),
            source: local_socket_io_error(error),
        })?;
    set_private_file(&paths.socket).await?;

    let codex_session = Arc::new(CodexSessionService::new(
        &paths,
        env_u64("AGENTDP_PR_IDLE_SECONDS").unwrap_or(DEFAULT_IDLE_SECONDS),
    ));
    let github_pr = Arc::new(GithubPrService::new(
        &paths,
        Arc::clone(&codex_session),
        env_u64("AGENTDP_PR_POLL_SECONDS").unwrap_or(DEFAULT_POLL_SECONDS),
    ));
    if env_bool("AGENTDP_CODEX_SESSION") {
        tokio::spawn(codex_session_loop(Arc::clone(&codex_session)));
    }
    let control = Arc::new(ControlHandler::new(Arc::clone(&github_pr)));
    tokio::spawn(poll_loop(Arc::clone(&github_pr)));

    loop {
        let stream = listener
            .accept()
            .await
            .map_err(|source| Error::Message(format!("failed to accept guest daemon connection: {source}")))?;
        let control = Arc::clone(&control);
        tokio::spawn(async move {
            control.handle_stream(stream).await;
        });
    }
}

async fn poll_loop(service: Arc<GithubPrService>) {
    loop {
        if let Err(error) = service.poll_once().await {
            eprintln!("guestd: PR poll failed: {error}");
        }
        tokio::time::sleep(Duration::from_secs(service.poll_seconds())).await;
    }
}

async fn codex_session_loop(service: Arc<CodexSessionService>) {
    loop {
        if let Err(error) = service.ensure_session().await {
            eprintln!("guestd: Codex session startup failed: {error}");
        }
        tokio::time::sleep(Duration::from_mins(1)).await;
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

fn env_bool(name: &str) -> bool {
    matches!(std::env::var(name).as_deref(), Ok("1" | "true" | "yes" | "on"))
}
