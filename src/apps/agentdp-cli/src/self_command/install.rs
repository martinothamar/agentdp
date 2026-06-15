use std::process::ExitCode;

use agentdp_core::{Context, layout::AgentdpLayout};
use clap::Args;

use crate::server_client::{self, Stop};

use super::installation::install_current_agentctl;

#[derive(Debug, Args)]
pub(crate) struct Command;

pub(crate) async fn run(_command: &Command, context: &Context) -> ExitCode {
    match try_run(context).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn try_run(context: &Context) -> Result<(), Error> {
    let layout = AgentdpLayout::resolve().map_err(Error::AgentdpLayout)?;
    let stopped = server_client::stop_if_running(context, &layout).await?;
    let result = install_current_agentctl(context).await?;
    for artifact in &result.artifacts {
        println!("installed {}", artifact.name);
        println!("source: {}", artifact.source.display());
        println!("destination: {}", artifact.destination.display());
    }

    if let (Stop::Stopped(_), Some(server)) = (stopped, result.agentdp_server_destination()) {
        let ping = server_client::start_server_from(context, &layout, server).await?;
        println!("restarted agentdp-server");
        println!("pid: {}", ping.pid);
        println!("socket: {}", ping.socket.display());
        if let Some(version) = ping.version {
            println!("version: {version}");
        }
        if let Some(executable) = ping.executable {
            println!("executable: {executable}");
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("{0}")]
    Install(#[from] super::installation::Error),
    #[error("{0}")]
    AgentdpLayout(#[from] agentdp_core::layout::Error),
    #[error("{0}")]
    Server(#[from] server_client::Error),
}
