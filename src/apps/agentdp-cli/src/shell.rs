use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::{Context, layout::AgentdpLayout, manifest::LoadedAgentManifest};
use agentdp_platform as platform;
use agentdp_protocol::client_server::{AgentInstanceSelector, AgentInstanceShellResult, RequestKind};
use clap::Args;
use tokio::process::Command as ProcessCommand;

use crate::server_client;

#[derive(Debug, Args)]
pub(crate) struct Command {
    #[arg(value_name = "INSTANCE_ID")]
    pub instance_id: u32,

    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,
}

pub(crate) async fn run(command: &Command, context: &Context) -> ExitCode {
    match try_run(command, context).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn try_run(command: &Command, context: &Context) -> Result<(), Error> {
    let manifest = LoadedAgentManifest::load_from_current_dir(context, command.file.as_deref()).await?;
    let layout = AgentdpLayout::resolve().map_err(Error::AgentdpLayout)?;
    let result: AgentInstanceShellResult = server_client::request(
        context,
        &layout,
        RequestKind::AgentInstanceShell(AgentInstanceSelector {
            agent: manifest.agent_name().to_owned(),
            instance_id: command.instance_id,
        }),
        None,
    )
    .await
    .map_err(Error::Server)?;
    let program = result.command.program;
    let binary = platform::host::find_binary(&program)
        .await
        .ok_or_else(|| Error::MissingProgram(program.clone()))?;
    let status = ProcessCommand::new(binary)
        .args(result.command.args)
        .status()
        .await
        .map_err(Error::RunCommand)?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::CommandFailed(status.code()))
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("{0}")]
    AgentManifest(#[from] agentdp_core::manifest::Error),
    #[error("{0}")]
    AgentdpLayout(agentdp_core::layout::Error),
    #[error("{0}")]
    Server(server_client::Error),
    #[error("{0} was not found on PATH")]
    MissingProgram(String),
    #[error("failed to run shell command: {0}")]
    RunCommand(std::io::Error),
    #[error("shell command exited with status {0:?}")]
    CommandFailed(Option<i32>),
}
