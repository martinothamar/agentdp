mod system;
mod user;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::Result;
use crate::containers::docker;

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        None | Some(Command::User) => user::run().await,
        Some(Command::DockerProxy(args)) => Box::pin(docker::proxy::run(args.into())).await,
        Some(Command::System(args)) => Box::pin(system::run(args.into())).await,
    }
}

#[derive(Debug, Parser)]
#[command(name = "guestd")]
#[command(about = "agentdp guest daemon")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the user-owned local guest daemon.
    User,
    /// Proxy the Docker Engine socket and inject container CA trust.
    DockerProxy(DockerProxyArgs),
    /// Run the root-owned host control-channel daemon.
    System(SystemArgs),
}

#[derive(Debug, Parser)]
struct DockerProxyArgs {
    #[arg(long)]
    listen: Option<PathBuf>,
    #[arg(long)]
    upstream: Option<PathBuf>,
    #[arg(long)]
    ca: Option<PathBuf>,
}

#[derive(Debug, Parser)]
struct SystemArgs {
    #[arg(long)]
    instance_spec: PathBuf,
}

impl From<DockerProxyArgs> for docker::proxy::Config {
    fn from(args: DockerProxyArgs) -> Self {
        Self {
            listen: args.listen.unwrap_or_else(docker::proxy::default_listen_path),
            upstream: args.upstream.unwrap_or_else(docker::proxy::default_upstream_path),
            ca: args.ca.unwrap_or_else(docker::proxy::default_ca_path),
        }
    }
}

impl From<SystemArgs> for system::Config {
    fn from(args: SystemArgs) -> Self {
        Self {
            instance_spec: args.instance_spec,
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser, error::ErrorKind};

    use super::{Cli, Command, docker};

    #[test]
    fn system_lifecycle_requires_instance_spec_path() {
        let error = Cli::try_parse_from(["guestd", "system"]).expect_err("missing spec must fail");

        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn system_lifecycle_accepts_instance_spec_path() {
        let cli = Cli::try_parse_from(["guestd", "system", "--instance-spec", "/run/agentdp/spec/instance.json"])
            .expect("parse system command");

        match cli.command.expect("system subcommand") {
            Command::System(args) => {
                assert_eq!(
                    args.instance_spec,
                    std::path::PathBuf::from("/run/agentdp/spec/instance.json")
                );
            }
            Command::User | Command::DockerProxy(_) => panic!("expected system subcommand"),
        }
    }

    #[test]
    fn docker_proxy_accepts_socket_paths() {
        let cli = Cli::try_parse_from([
            "guestd",
            "docker-proxy",
            "--listen",
            "/run/docker.sock",
            "--upstream",
            "/run/agentdp/docker/docker.sock",
            "--ca",
            "/var/lib/agentdp/ca/ca-bundle.pem",
        ])
        .expect("parse docker proxy command");

        match cli.command.expect("docker proxy subcommand") {
            Command::DockerProxy(args) => {
                let config = docker::proxy::Config::from(args);
                assert_eq!(config.listen, std::path::PathBuf::from("/run/docker.sock"));
                assert_eq!(
                    config.upstream,
                    std::path::PathBuf::from("/run/agentdp/docker/docker.sock")
                );
                assert_eq!(config.ca, std::path::PathBuf::from("/var/lib/agentdp/ca/ca-bundle.pem"));
            }
            Command::User | Command::System(_) => {
                panic!("expected docker proxy subcommand")
            }
        }
    }

    #[test]
    fn help_mentions_instance_spec_without_path_defaults() {
        let mut command = Cli::command();
        let help = command
            .find_subcommand_mut("system")
            .expect("system subcommand")
            .render_long_help()
            .to_string();

        assert!(help.contains("--instance-spec"));
        assert!(!help.contains("/run/agentdp/spec/instance.json"));
    }
}
