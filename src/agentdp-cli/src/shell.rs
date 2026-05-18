use std::env;
use std::path::PathBuf;
use std::process::{Command as ProcessCommand, ExitCode};

use agentdp_core::Context;
use agentdp_core::manifest::resolve_manifest_path;
use agentdp_core::platform;
use agentdp_protocol::{InstanceRef, InstanceShellResult, RequestKind};
use clap::Args;

use crate::server_client;

#[derive(Debug, Args)]
pub struct Command {
    pub instance: String,

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
    let result: InstanceShellResult = server_client::request(
        context,
        paths,
        RequestKind::InstanceShell(InstanceRef {
            manifest,
            instance: command.instance.clone(),
        }),
        None,
    )
    .map_err(Error::Server)?;
    let program = result.command.program;
    let binary = platform::find_binary(&program).ok_or_else(|| Error::MissingProgram(program.clone()))?;
    let status = ProcessCommand::new(binary)
        .args(result.command.args)
        .status()
        .map_err(Error::RunCommand)?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::CommandFailed(status.code()))
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
    #[error("{0} was not found on PATH")]
    MissingProgram(String),
    #[error("failed to run shell command: {0}")]
    RunCommand(std::io::Error),
    #[error("shell command exited with status {0:?}")]
    CommandFailed(Option<i32>),
}
