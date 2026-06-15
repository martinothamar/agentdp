use std::path::{Path, PathBuf};
use std::process::ExitCode;

use agentdp_core::{Context, layout::AgentdpLayout, manifest::LoadedAgentManifest};
use agentdp_protocol::client_server::{AgentInstanceListParams, AgentInstanceListResult, RequestKind};
use clap::Args;

use crate::server_client;

#[derive(Debug, Args)]
pub(crate) struct Command {
    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,

    #[arg(long)]
    json: bool,
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
    let agent = optional_agent(context, command.file.as_deref()).await?;
    let layout = AgentdpLayout::resolve().map_err(Error::AgentdpLayout)?;
    let result: AgentInstanceListResult = server_client::request(
        context,
        &layout,
        RequestKind::AgentInstanceList(AgentInstanceListParams { agent }),
        None,
    )
    .await
    .map_err(Error::Server)?;

    if command.json {
        println!("{}", serde_json::to_string_pretty(&result).map_err(Error::Json)?);
    } else {
        print_instances(&result);
    }
    Ok(())
}

async fn optional_agent(context: &Context, explicit: Option<&Path>) -> Result<Option<String>, Error> {
    Ok(LoadedAgentManifest::load_optional_from_current_dir(context, explicit)
        .await?
        .map(|manifest| manifest.agent_name().to_owned()))
}

fn print_instances(result: &AgentInstanceListResult) {
    let instances = &result.instances;
    if instances.is_empty() {
        println!("instances: none");
        return;
    }

    println!("instances:");
    for instance in instances {
        let name = format!("{}/{}", instance.agent, instance.instance_id);
        let status = &instance.status;
        let pid = instance.pid.map_or_else(|| "none".to_owned(), |pid| pid.to_string());
        let ready = instance
            .ready
            .map_or("unknown", |ready| if ready { "ready" } else { "not-ready" });
        let stale = if instance.stale { " stale" } else { "" };
        println!("  {name}: {status} pid:{pid} readiness:{ready}{stale}");
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
    #[error("failed to serialize ps result as JSON: {0}")]
    Json(serde_json::Error),
}
