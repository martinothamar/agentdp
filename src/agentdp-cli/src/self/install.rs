use std::process::ExitCode;

use agentdp_core::Context;
use agentdp_core::installation::install_current_agentctl;
use clap::Args;

use crate::server_client::{self, Refresh};

#[derive(Debug, Args)]
pub struct Command;

pub fn run(_command: &Command, context: &Context) -> ExitCode {
    match try_run(context) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn try_run(context: &Context) -> Result<(), Error> {
    let result = install_current_agentctl(context)?;
    for artifact in &result.artifacts {
        println!("installed {}", artifact.name);
        println!("source: {}", artifact.source.display());
        println!("destination: {}", artifact.destination.display());
    }

    if let Some(server) = result.agentdp_server_destination() {
        let paths = context.paths().map_err(|error| Error::PlatformPaths(error.clone()))?;
        match server_client::refresh_if_running(context, paths, server)? {
            Refresh::NotRunning => {}
            Refresh::Restarted(ping) => {
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
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("{0}")]
    Install(#[from] agentdp_core::installation::Error),
    #[error("{0}")]
    PlatformPaths(#[from] agentdp_core::platform::Error),
    #[error("{0}")]
    Server(#[from] server_client::Error),
}
