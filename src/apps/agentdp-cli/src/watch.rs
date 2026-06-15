use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::{Context, agent::AgentDocument, layout::AgentdpLayout, manifest::LoadedAgentManifest};
use agentdp_protocol::client_server::{AgentWatchParams, Event, EventKind, EventLevel};
use clap::Args;
use serde::Serialize;

use crate::server_client;
use crate::status;
use crate::wait;

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
    let manifest = LoadedAgentManifest::load_from_current_dir(context, command.file.as_deref()).await?;
    let layout = AgentdpLayout::resolve().map_err(Error::AgentdpLayout)?;
    server_client::watch_agent(
        context,
        &layout,
        AgentWatchParams {
            agent: manifest.agent_name().to_owned(),
        },
        |event| {
            if let Err(error) = print_event(event, command.json) {
                eprintln!("{error}");
            }
        },
    )
    .await
    .map_err(Error::Server)?;
    Ok(())
}

fn print_event(event: Event, json: bool) -> Result<(), Error> {
    if json {
        match &event.event {
            EventKind::AgentDocumentChanged { document } => print_json(document),
            EventKind::AgentEvent { item } => print_json(item),
            EventKind::Diagnostic { .. } | EventKind::SessionOutput { .. } => print_json(&event),
        }?;
        return std::io::stdout().flush().map_err(Error::Stdout);
    }

    match event.event {
        EventKind::AgentDocumentChanged { document } => print_document(document),
        EventKind::Diagnostic { level, message } if level != EventLevel::Verbose => {
            println!("{}", wait::progress_message(level, &message));
            std::io::stdout().flush().map_err(Error::Stdout)
        }
        EventKind::Diagnostic { .. } | EventKind::SessionOutput { .. } | EventKind::AgentEvent { .. } => Ok(()),
    }
}

fn print_document(document: serde_json::Value) -> Result<(), Error> {
    let document = serde_json::from_value::<AgentDocument>(document).map_err(Error::Json)?;
    status::print_agent_document(&document);
    std::io::stdout().flush().map_err(Error::Stdout)?;
    Ok(())
}

fn print_json(value: &impl Serialize) -> Result<(), Error> {
    println!("{}", serde_json::to_string(value).map_err(Error::Json)?);
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
    #[error("failed to serialize watch result as JSON: {0}")]
    Json(serde_json::Error),
    #[error("failed to flush watch output: {0}")]
    Stdout(std::io::Error),
}
