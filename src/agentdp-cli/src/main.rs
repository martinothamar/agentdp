#![forbid(unsafe_code)]

use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod create;
mod doctor;
mod down;
mod exec;
mod logging;
mod logs;
mod manifest;
mod ps;
mod rm;
#[path = "self/mod.rs"]
mod self_command;
mod server_client;
mod shell;
mod status;
mod up;

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
    Create(create::Command),
    Status(status::Command),
    Up(up::Command),
    Down(down::Command),
    Exec(exec::Command),
    Logs(logs::Command),
    Ps(ps::Command),
    Shell(shell::Command),
    Doctor(doctor::Command),
    Manifest(manifest::Command),
    Rm(rm::Command),
    #[command(name = "self")]
    Self_(self_command::Command),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let context = logging::context(cli.verbose);

    match &cli.command {
        Command::Create(command) => create::run(command, &context),
        Command::Status(command) => status::run(command, &context),
        Command::Up(command) => up::run(command, &context),
        Command::Down(command) => down::run(command, &context),
        Command::Exec(command) => exec::run(command, &context),
        Command::Logs(command) => logs::run(command, &context),
        Command::Ps(command) => ps::run(command, &context),
        Command::Shell(command) => shell::run(command, &context),
        Command::Doctor(command) => doctor::run(command, &context),
        Command::Manifest(command) => manifest::run(command, &context),
        Command::Rm(command) => rm::run(command, &context),
        Command::Self_(command) => self_command::run(command, &context),
    }
}
