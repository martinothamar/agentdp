use std::process::ExitCode;

use agentdp_core::Context;
use clap::{Args, Subcommand};

mod validate;

#[derive(Debug, Args)]
pub(crate) struct Command {
    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, Subcommand)]
enum Action {
    Validate(validate::Command),
}

pub(crate) async fn run(command: &Command, context: &Context) -> ExitCode {
    match &command.action {
        Action::Validate(command) => validate::run(command, context).await,
    }
}
