use std::sync::Arc;

use super::agent_session::TmuxAgentSession;

#[derive(Debug)]
pub(crate) enum AgentAutomation {
    AgentHost(String),
    Tmux(Arc<TmuxAgentSession>),
    Unavailable,
}
