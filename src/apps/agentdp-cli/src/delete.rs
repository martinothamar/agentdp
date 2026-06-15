use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::{Context, agent::AgentDeleteResult, layout::AgentdpLayout, manifest::LoadedAgentManifest};
use agentdp_protocol::client_server::{AgentSelector, AgentWaitCondition, RequestKind};
use clap::Args;

use crate::server_client;
use crate::wait;

#[derive(Debug, Args)]
pub(crate) struct Command {
    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,

    #[arg(long)]
    wait: bool,
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
    let result: AgentDeleteResult = server_client::request(
        context,
        &layout,
        RequestKind::AgentDelete(AgentSelector {
            agent: manifest.agent_name().to_owned(),
        }),
        None,
    )
    .await
    .map_err(Error::Server)?;

    println!("delete {}", result.agent());
    println!("desired generation: {}", result.generation());
    println!("observed generation: {}", result.observed_generation());
    println!("phase: {:?}", result.status.phase);
    println!("reconciling: {}", result.status.reconciling);

    if command.wait {
        let mut progress = wait::WaitProgress::default();
        let mut on_event = |event| progress.print_event(event);
        let wait_result = wait::wait_for(
            context,
            &layout,
            result.agent().to_string(),
            result.generation(),
            AgentWaitCondition::Deleted,
            None,
            Some(&mut on_event),
        )
        .await
        .map_err(Error::Server)?;
        wait::print_result(&wait_result);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("{0}")]
    AgentManifest(#[from] agentdp_core::manifest::Error),
    #[error("{0}")]
    AgentdpLayout(agentdp_core::layout::Error),
    #[error("{0}")]
    Server(server_client::Error),
}
