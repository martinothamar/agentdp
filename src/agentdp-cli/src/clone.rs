use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::Context;
use agentdp_core::manifest::resolve_manifest_path;
use agentdp_protocol::{BackendCreateResult, InstanceCloneParams, InstanceCloneResult, RequestKind};
use clap::Args;

use crate::port::{PortOverride, port_overrides};
use crate::server_client;

#[derive(Debug, Args)]
pub struct Command {
    pub source: String,
    pub target: String,

    #[arg(long = "port", value_name = "NAME:HOST_PORT")]
    ports: Vec<PortOverride>,

    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,
}

pub fn run(command: &Command, context: &Context) -> ExitCode {
    match try_run(command, context) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn try_run(command: &Command, context: &Context) -> Result<(), Error> {
    let cwd = env::current_dir().map_err(Error::CurrentDirectory)?;
    let manifest = resolve_manifest_path(context, command.file.as_deref(), &cwd).map_err(Error::ManifestPath)?;
    let paths = context.paths().map_err(|error| Error::PlatformPaths(error.clone()))?;
    let result: InstanceCloneResult = server_client::request(
        context,
        paths,
        RequestKind::InstanceClone(InstanceCloneParams {
            manifest,
            source: command.source.clone(),
            target: command.target.clone(),
            ports: port_overrides(&command.ports)?,
        }),
        None,
    )
    .map_err(Error::Server)?;

    println!("cloned {} -> {}", result.source, result.name);
    println!("state: {}", result.state);
    println!("manifest: {}", result.manifest.copy);
    match result.backend {
        BackendCreateResult::Qemu(qemu) => {
            println!("disk: {}", qemu.disk);
            println!("seed: {}", qemu.seed_media);
            println!("image: {}", qemu.image.cache_path);
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("failed to read current directory: {0}")]
    CurrentDirectory(std::io::Error),
    #[error("{0}")]
    ManifestPath(agentdp_core::manifest::PathError),
    #[error("{0}")]
    PlatformPaths(agentdp_core::platform::Error),
    #[error("{0}")]
    Server(server_client::Error),
    #[error("{0}")]
    Port(#[from] crate::port::Error),
}
