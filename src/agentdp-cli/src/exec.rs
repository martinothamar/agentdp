use std::env;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::Context;
use agentdp_core::manifest::resolve_manifest_path;
use agentdp_protocol::{InstanceExecParams, InstanceExecResult, RequestKind};
use clap::Args;

use crate::server_client;

#[derive(Debug, Args)]
#[command(trailing_var_arg = true)]
pub struct Command {
    pub instance: String,

    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,

    #[arg(long, default_value = "300s", value_name = "DURATION")]
    timeout: String,

    #[arg(required = true)]
    #[arg(value_name = "COMMAND")]
    argv: Vec<String>,
}

pub fn run(command: &Command, context: &Context) -> ExitCode {
    match try_run(command, context) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn try_run(command: &Command, context: &Context) -> Result<ExitCode, Error> {
    let cwd = env::current_dir().map_err(Error::CurrentDirectory)?;
    let manifest = resolve_manifest_path(context, command.file.as_deref(), &cwd).map_err(Error::ManifestPath)?;
    let paths = context.paths().map_err(|error| Error::PlatformPaths(error.clone()))?;
    let result: InstanceExecResult = server_client::request(
        context,
        paths,
        RequestKind::InstanceExec(InstanceExecParams {
            manifest,
            instance: command.instance.clone(),
            command: command.argv.clone(),
            timeout_seconds: Some(parse_duration_seconds(&command.timeout)?),
        }),
        None,
    )
    .map_err(Error::Server)?;

    io::stdout()
        .write_all(result.stdout.as_bytes())
        .map_err(Error::WriteStdout)?;
    io::stderr()
        .write_all(result.stderr.as_bytes())
        .map_err(Error::WriteStderr)?;
    Ok(exit_code(result.exit_status))
}

fn exit_code(status: u64) -> ExitCode {
    ExitCode::from(u8::try_from(status).unwrap_or(1))
}

fn parse_duration_seconds(value: &str) -> Result<u64, Error> {
    let digit_count = value.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return Err(Error::InvalidTimeout(value.to_owned()));
    }
    let number = value[..digit_count]
        .parse::<u64>()
        .map_err(|_| Error::InvalidTimeout(value.to_owned()))?;
    if number == 0 {
        return Err(Error::InvalidTimeout(value.to_owned()));
    }
    let multiplier = match &value[digit_count..] {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        _ => return Err(Error::InvalidTimeout(value.to_owned())),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| Error::InvalidTimeout(value.to_owned()))
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
    #[error("timeout must be a positive duration like 30s, 5m, or 1h: {0}")]
    InvalidTimeout(String),
    #[error("failed to write command stdout: {0}")]
    WriteStdout(std::io::Error),
    #[error("failed to write command stderr: {0}")]
    WriteStderr(std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::parse_duration_seconds;

    #[test]
    fn parses_timeout_duration() {
        assert_eq!(parse_duration_seconds("30s").unwrap(), 30);
        assert_eq!(parse_duration_seconds("5m").unwrap(), 300);
        assert_eq!(parse_duration_seconds("1h").unwrap(), 3600);
    }
}
