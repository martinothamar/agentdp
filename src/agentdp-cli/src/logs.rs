use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::Context;
use agentdp_core::manifest::resolve_manifest_path;
use agentdp_protocol::{InstanceLogsParams, InstanceLogsResult, LogFile, RequestKind};
use clap::Args;

use crate::server_client;

#[derive(Debug, Args)]
pub struct Command {
    pub instance: String,

    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,

    #[arg(long, conflicts_with = "qemu")]
    serial: bool,

    #[arg(long)]
    qemu: bool,

    #[arg(long, default_value_t = 200)]
    lines: usize,
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
    if command.lines == 0 {
        return Err(Error::InvalidLines);
    }

    let cwd = env::current_dir().map_err(Error::CurrentDirectory)?;
    let manifest = resolve_manifest_path(context, command.file.as_deref(), &cwd).map_err(Error::ManifestPath)?;
    let paths = context.paths().map_err(|error| Error::PlatformPaths(error.clone()))?;
    let result: InstanceLogsResult = server_client::request(
        context,
        paths,
        RequestKind::InstanceLogs(InstanceLogsParams {
            manifest,
            instance: command.instance.clone(),
            file: log_file(command),
            lines: command.lines,
        }),
        None,
    )
    .map_err(Error::Server)?;

    print!("{}", result.contents);
    Ok(())
}

const fn log_file(command: &Command) -> LogFile {
    if command.qemu { LogFile::Qemu } else { LogFile::Serial }
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
    #[error("log line count must be greater than zero")]
    InvalidLines,
}
