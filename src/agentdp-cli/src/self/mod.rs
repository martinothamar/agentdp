use std::process::ExitCode;

use agentdp_core::Context;
use clap::{Args, Subcommand};

pub mod install;

#[derive(Debug, Args)]
pub struct Command {
    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, Subcommand)]
enum Action {
    Install(install::Command),
}

pub fn run(command: &Command, context: &Context) -> ExitCode {
    match &command.action {
        Action::Install(command) => install::run(command, context),
    }
}
