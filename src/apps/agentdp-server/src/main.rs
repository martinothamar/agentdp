#![forbid(unsafe_code)]
#![allow(clippy::future_not_send)]

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use agentdp_core::Context;
use agentdp_core::logging::{LogRecord, LogSink, Logger};
use agentdp_platform::time;

mod agent;
mod backend;
mod host;
mod qemu;
mod server;
mod services;

fn main() -> ExitCode {
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build_local(tokio::runtime::LocalOptions::default())
        .map_err(Error::Runtime)
        .and_then(|runtime| runtime.block_on(run()));

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("agentdp-server: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Error> {
    let layout = agent::AgentdpLayout::resolve()?;
    let context = Context::new(Logger::new(
        Arc::new(FileLogSink {
            path: layout.server_log(),
        }),
        true,
    ));
    let socket = parse_socket_arg()?.unwrap_or_else(|| layout.socket_path());
    server::serve(&context, layout, &socket).await?;
    Ok(())
}

struct FileLogSink {
    path: PathBuf,
}

impl LogSink for FileLogSink {
    fn write(&self, record: LogRecord) {
        if let Some(parent) = self.path.parent() {
            let _result = std::fs::create_dir_all(parent);
        }
        let timestamp = time::unix_seconds();
        let line = format!("{timestamp} {}: {}\n", record.level.label(), record.message);
        let result = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .and_then(|mut file| std::io::Write::write_all(&mut file, line.as_bytes()));
        if let Err(error) = result {
            eprintln!("agentdp-server: failed to write log {}: {error}", self.path.display());
        }
    }
}

fn parse_socket_arg() -> Result<Option<PathBuf>, Error> {
    let mut args = std::env::args_os().skip(1);
    match args.next() {
        None => Ok(None),
        Some(flag) if flag == "--socket" => args.next().map(PathBuf::from).map(Some).ok_or(Error::MissingSocketPath),
        Some(flag) => Err(Error::UnknownArgument(flag.to_string_lossy().into_owned())),
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("missing path after --socket")]
    MissingSocketPath,
    #[error("unknown argument {0}")]
    UnknownArgument(String),
    #[error("{0}")]
    AgentdpLayout(#[from] agent::AgentdpLayoutError),
    #[error("failed to initialize server runtime: {0}")]
    Runtime(#[source] std::io::Error),
    #[error("{0}")]
    Server(#[from] server::Error),
}
