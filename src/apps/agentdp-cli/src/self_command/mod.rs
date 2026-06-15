use std::process::ExitCode;

use agentdp_core::Context;
use clap::{Args, Subcommand};

mod install;
mod installation;

#[derive(Debug, Args)]
pub(crate) struct Command {
    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, Subcommand)]
enum Action {
    Install(install::Command),
}

pub(crate) async fn run(command: &Command, context: &Context) -> ExitCode {
    match &command.action {
        Action::Install(command) => install::run(command, context).await,
    }
}
