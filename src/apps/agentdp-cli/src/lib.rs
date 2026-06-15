#![forbid(unsafe_code)]

use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod apply;
mod delete;
mod doctor;
mod exec;
mod logging;
mod logs;
mod manifest;
mod ps;
mod scale;
mod self_command;
mod server_client;
mod shell;
mod status;
mod wait;
mod watch;

#[derive(Debug, Parser)]
#[command(name = "agentctl")]
#[command(about = "CLI frontend for the agentdp platform")]
struct Cli {
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(name = "apply")]
    Apply(apply::Command),
    Delete(delete::Command),
    Status(status::Command),
    Wait(wait::Command),
    Watch(watch::Command),
    Exec(exec::Command),
    Logs(logs::Command),
    Ps(ps::Command),
    #[command(about = "Set desired replica count")]
    Scale(scale::Command),
    Shell(shell::Command),
    Doctor(doctor::Command),
    Manifest(manifest::Command),
    #[command(name = "self")]
    Self_(self_command::Command),
}

#[must_use]
pub fn run() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: failed to build tokio runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    runtime.block_on(run_async())
}

async fn run_async() -> ExitCode {
    let cli = Cli::parse();
    let context = logging::context(cli.verbose);

    match &cli.command {
        Command::Apply(command) => apply::run(command, &context).await,
        Command::Delete(command) => delete::run(command, &context).await,
        Command::Status(command) => status::run(command, &context).await,
        Command::Wait(command) => wait::run(command, &context).await,
        Command::Watch(command) => watch::run(command, &context).await,
        Command::Exec(command) => exec::run(command, &context).await,
        Command::Logs(command) => logs::run(command, &context).await,
        Command::Ps(command) => ps::run(command, &context).await,
        Command::Scale(command) => scale::run(command, &context).await,
        Command::Shell(command) => shell::run(command, &context).await,
        Command::Doctor(command) => doctor::run(command, &context).await,
        Command::Manifest(command) => manifest::run(command, &context).await,
        Command::Self_(command) => self_command::run(command, &context).await,
    }
}
