#![allow(clippy::missing_errors_doc)]
#![allow(clippy::module_name_repetitions)]

use std::io;
use std::path::PathBuf;

use thiserror::Error;

mod cli;
mod containers;
mod daemon;

mod system;
mod user;

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub use cli::run as cli_run;
pub use daemon::run as daemon_run;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Message(String),
    #[error("{0}")]
    PlatformCommand(#[from] agentdp_platform::command::RunError),
    #[error("{0}")]
    PrivatePath(#[from] agentdp_platform::fs::PrivatePathError),
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("failed to bind guest daemon socket {path}: {source}")]
    BindSocket { path: PathBuf, source: io::Error },
    #[error("failed to connect to guest daemon socket {path}: {source}")]
    ConnectSocket { path: PathBuf, source: io::Error },
    #[error("failed to serialize JSON: {0}")]
    JsonSerialize(#[from] serde_json::Error),
    #[error("{0}")]
    Protocol(#[from] agentdp_protocol::Error),
    #[error("failed to read daemon request: {0}")]
    ReadRequest(io::Error),
    #[error("failed to write daemon response: {0}")]
    WriteResponse(io::Error),
}
