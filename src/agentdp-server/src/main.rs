#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::Context;

mod instance;
mod progress;
mod qemu;
mod runtime;
mod server;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("agentdp-server: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Error> {
    let context = Context::quiet();
    let socket = parse_socket_arg()?.map_or_else(|| default_socket_path(&context), Ok)?;
    server::serve(&context, &socket)?;
    Ok(())
}

fn parse_socket_arg() -> Result<Option<PathBuf>, Error> {
    let mut args = std::env::args_os().skip(1);
    match args.next() {
        None => Ok(None),
        Some(flag) if flag == "--socket" => args.next().map(PathBuf::from).map(Some).ok_or(Error::MissingSocketPath),
        Some(flag) => Err(Error::UnknownArgument(flag.to_string_lossy().into_owned())),
    }
}

fn default_socket_path(context: &Context) -> Result<PathBuf, Error> {
    Ok(context
        .paths()
        .map_err(|error| Error::PlatformPaths(error.clone()))?
        .socket_path())
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("missing path after --socket")]
    MissingSocketPath,
    #[error("unknown argument {0}")]
    UnknownArgument(String),
    #[error("{0}")]
    PlatformPaths(#[from] agentdp_core::platform::Error),
    #[error("{0}")]
    Server(#[from] server::Error),
}
