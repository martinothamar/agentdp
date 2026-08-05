use std::ffi::OsString;

use clap::{Parser, Subcommand};

use crate::containers::{docker, podman};
use crate::user::{Request, client_request};
use crate::{Error, Result};

#[derive(Debug, Parser)]
#[command(name = "guestctl")]
#[command(about = "agentdp guest helper CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(hide = true)]
    DockerCli(ContainerCliArgs),
    Ping,
    #[command(hide = true)]
    PodmanCli(ContainerCliArgs),
    Pr {
        #[command(subcommand)]
        command: PrCommand,
    },
}

#[derive(Debug, Subcommand)]
enum PrCommand {
    Register {
        target: Option<String>,
    },
    #[command(hide = true)]
    RegisterAgentHost {
        session: String,
        url: String,
    },
    Unregister {
        target: Option<String>,
    },
    #[command(hide = true)]
    UnregisterAgentHost {
        session: String,
        url: String,
    },
    List,
}

#[derive(Debug, Parser)]
struct ContainerCliArgs {
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<OsString>,
}

pub async fn run() -> Result<()> {
    if docker::cli::invoked_as_docker() {
        return docker::cli::run_from_env();
    }
    if podman::cli::invoked_as_podman() {
        return podman::cli::run_from_env();
    }

    let cli = Cli::parse();
    match cli.command {
        Command::DockerCli(args) => {
            let code = docker::cli::run(args.args)?;
            std::process::exit(code);
        }
        Command::PodmanCli(args) => {
            let code = podman::cli::run(args.args)?;
            std::process::exit(code);
        }
        command => run_control_command(command).await,
    }
}

async fn run_control_command(command: Command) -> Result<()> {
    let cwd = std::env::current_dir()
        .map_err(|error| Error::Message(format!("failed to resolve current directory: {error}")))?
        .display()
        .to_string();
    let request = match command {
        Command::Ping => Request::Ping,
        Command::Pr { command } => match command {
            PrCommand::Register { target } => Request::PrRegister { target, cwd },
            PrCommand::RegisterAgentHost { session, url } => Request::PrRegisterAgentHost { url, session },
            PrCommand::Unregister { target } => Request::PrUnregister { target, cwd },
            PrCommand::UnregisterAgentHost { session, url } => Request::PrUnregisterAgentHost { url, session },
            PrCommand::List => Request::PrList,
        },
        Command::DockerCli(_) | Command::PodmanCli(_) => {
            unreachable!("shim commands are handled before control dispatch")
        }
    };
    let response = client_request(request).await?;
    if !response.is_ok() {
        return Err(Error::Message(response.message().to_owned()));
    }
    if let Some(prs) = response.prs() {
        for pr in prs {
            let branch = pr
                .branch
                .as_ref()
                .map_or_else(String::new, |branch| format!(" {branch}"));
            println!("#{} {}{}", pr.number, pr.url, branch);
        }
        return Ok(());
    }
    if !response.message().is_empty() {
        println!("{}", response.message());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory as _, Parser as _};

    use super::{Cli, Command};

    #[test]
    fn docker_cli_accepts_trailing_docker_args() {
        let cli = Cli::try_parse_from([
            "guestctl",
            "docker-cli",
            "buildx",
            "build",
            "--load",
            "-t",
            "image:tag",
            ".",
        ])
        .expect("parse docker cli command");

        match cli.command {
            Command::DockerCli(args) => {
                assert_eq!(
                    args.args,
                    ["buildx", "build", "--load", "-t", "image:tag", "."].map(std::ffi::OsString::from)
                );
            }
            Command::Ping | Command::PodmanCli(_) | Command::Pr { .. } => panic!("expected docker cli subcommand"),
        }
    }

    #[test]
    fn podman_cli_accepts_trailing_podman_args() {
        let cli = Cli::try_parse_from([
            "guestctl",
            "podman-cli",
            "build",
            "-t",
            "image:tag",
            "-f",
            "Containerfile",
            ".",
        ])
        .expect("parse podman cli command");

        match cli.command {
            Command::PodmanCli(args) => {
                assert_eq!(
                    args.args,
                    ["build", "-t", "image:tag", "-f", "Containerfile", "."].map(std::ffi::OsString::from)
                );
            }
            Command::DockerCli(_) | Command::Ping | Command::Pr { .. } => panic!("expected podman cli subcommand"),
        }
    }

    #[test]
    fn help_hides_container_cli_shim_subcommands() {
        let help = Cli::command().render_long_help().to_string();

        assert!(!help.contains("docker-cli"));
        assert!(!help.contains("podman-cli"));
    }
}
