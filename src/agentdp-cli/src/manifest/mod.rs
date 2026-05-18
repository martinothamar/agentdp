use std::process::ExitCode;

use agentdp_core::Context;
use clap::{Args, Subcommand};

pub mod validate;

#[derive(Debug, Args)]
pub struct Command {
    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, Subcommand)]
enum Action {
    Validate(validate::Command),
}

pub fn run(command: &Command, context: &Context) -> ExitCode {
    match &command.action {
        Action::Validate(command) => validate::run(command, context),
    }
}
