use std::sync::Arc;
use std::time::Duration;

use agentdp_platform::fs::{ensure_private_dir, set_private_file};
use agentdp_platform::socket;

use crate::user::{
    AgentAutomation, CLAUDE_SESSION_COMMAND, CODEX_SESSION_COMMAND, ControlHandler, GithubPrService, RuntimePaths,
    TmuxAgentSession,
};
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

    let agent_launch_command = agent_launch_command();
    let tmux_session = agent_launch_command.map(|command| {
        Arc::new(TmuxAgentSession::new(
            &paths,
            command,
            env_u64("AGENTDP_PR_IDLE_SECONDS").unwrap_or(DEFAULT_IDLE_SECONDS),
        ))
    });
    let automation = std::env::var("AGENTDP_AGENT_HOST_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .map_or_else(
            || {
                tmux_session.as_ref().map_or(AgentAutomation::Unavailable, |tmux| {
                    AgentAutomation::Tmux(Arc::clone(tmux))
                })
            },
            AgentAutomation::AgentHost,
        );
    let github_pr = Arc::new(GithubPrService::new(
        &paths,
        Arc::new(automation),
        env_u64("AGENTDP_PR_POLL_SECONDS").unwrap_or(DEFAULT_POLL_SECONDS),
    ));
    if let Some(tmux_session) = tmux_session {
        tokio::spawn(agent_session_loop(tmux_session));
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

async fn agent_session_loop(service: Arc<TmuxAgentSession>) {
    loop {
        if let Err(error) = service.ensure_session().await {
            eprintln!("guestd: agent session startup failed: {error}");
        }
        tokio::time::sleep(Duration::from_mins(1)).await;
    }
}

fn agent_launch_command() -> Option<&'static str> {
    if env_bool("AGENTDP_CLAUDE_SESSION") {
        Some(CLAUDE_SESSION_COMMAND)
    } else if env_bool("AGENTDP_CODEX_SESSION") {
        Some(CODEX_SESSION_COMMAND)
    } else {
        None
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

fn env_bool(name: &str) -> bool {
    matches!(std::env::var(name).as_deref(), Ok("1" | "true" | "yes" | "on"))
}
