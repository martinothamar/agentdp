use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use agentdp_core::Context;
use agentdp_core::manifest::{PathError, resolve_manifest_path};
use agentdp_protocol::{InstancePsParams, InstancePsResult, RequestKind};
use clap::Args;

use crate::server_client;

#[derive(Debug, Args)]
pub struct Command {
    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,

    #[arg(long)]
    json: bool,
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
    let manifest = optional_manifest_path(context, command.file.as_deref(), &cwd)?;
    let paths = context.paths().map_err(|error| Error::PlatformPaths(error.clone()))?;
    let result: InstancePsResult = server_client::request(
        context,
        paths,
        RequestKind::InstancePs(InstancePsParams { manifest }),
        None,
    )
    .map_err(Error::Server)?;

    if command.json {
        println!("{}", serde_json::to_string_pretty(&result).map_err(Error::Json)?);
    } else {
        print_instances(&result);
    }
    Ok(())
}

fn optional_manifest_path(context: &Context, explicit: Option<&Path>, cwd: &Path) -> Result<Option<PathBuf>, Error> {
    match resolve_manifest_path(context, explicit, cwd) {
        Ok(path) => Ok(Some(path)),
        Err(PathError::MissingDefault(_)) if explicit.is_none() => Ok(None),
        Err(error) => Err(Error::ManifestPath(error)),
    }
}

fn print_instances(result: &InstancePsResult) {
    let instances = &result.instances;
    if instances.is_empty() {
        println!("instances: none");
        return;
    }

    println!("instances:");
    for instance in instances {
        let name = &instance.name;
        let status = &instance.status;
        let pid = instance.pid.map_or_else(|| "none".to_owned(), |pid| pid.to_string());
        let ready = instance
            .ready
            .map_or("unknown", |ready| if ready { "ready" } else { "not-ready" });
        println!("  {name}: {status} pid:{pid} readiness:{ready}");
    }
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
    #[error("failed to serialize ps result as JSON: {0}")]
    Json(serde_json::Error),
}
