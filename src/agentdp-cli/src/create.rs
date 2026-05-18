use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use agentdp_core::Context;
use agentdp_core::manifest::resolve_manifest_path;
use agentdp_protocol::{BackendCreateResult, InstanceCreateParams, InstanceCreateResult, RequestKind};
use clap::Args;

use crate::server_client;

#[derive(Debug, Args)]
pub struct Command {
    pub instance: String,

    #[arg(long = "port", value_name = "NAME:HOST_PORT")]
    ports: Vec<PortOverride>,

    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PortOverride {
    name: String,
    host: u16,
}

impl FromStr for PortOverride {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (name, host) = value
            .split_once(':')
            .ok_or_else(|| "expected NAME:HOST_PORT".to_owned())?;
        validate_port_name(name)?;
        let host = host
            .parse::<u16>()
            .map_err(|_| "host port must be a number from 1 to 65535".to_owned())?;
        if host == 0 {
            return Err("host port must be greater than zero".to_owned());
        }
        Ok(Self {
            name: name.to_owned(),
            host,
        })
    }
}

pub fn run(command: &Command, context: &Context) -> ExitCode {
    match try_run(command, context) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn try_run(command: &Command, context: &Context) -> Result<(), Error> {
    let cwd = env::current_dir().map_err(Error::CurrentDirectory)?;
    let manifest = resolve_manifest_path(context, command.file.as_deref(), &cwd).map_err(Error::ManifestPath)?;
    let paths = context.paths().map_err(|error| Error::PlatformPaths(error.clone()))?;
    let result: InstanceCreateResult = server_client::request(
        context,
        paths,
        RequestKind::InstanceCreate(InstanceCreateParams {
            manifest,
            instance: command.instance.clone(),
            ports: port_overrides(&command.ports)?,
        }),
        None,
    )
    .map_err(Error::Server)?;

    println!("created {}", result.name);
    println!("state: {}", result.state);
    println!("manifest: {}", result.manifest.copy);
    match result.backend {
        BackendCreateResult::Qemu(qemu) => {
            println!("disk: {}", qemu.disk);
            println!("seed: {}", qemu.seed_media);
            println!("image: {}", qemu.image.cache_path);
        }
    }
    Ok(())
}

fn port_overrides(ports: &[PortOverride]) -> Result<std::collections::BTreeMap<String, u16>, Error> {
    let mut values = std::collections::BTreeMap::new();
    for port in ports {
        if values.insert(port.name.clone(), port.host).is_some() {
            return Err(Error::DuplicatePort(port.name.clone()));
        }
    }
    Ok(values)
}

fn validate_port_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("port name must not be empty".to_owned());
    }
    if name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        Ok(())
    } else {
        Err("port name may contain only ASCII letters, digits, '.', '_', and '-'".to_owned())
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("failed to read current directory: {0}")]
    CurrentDirectory(std::io::Error),
    #[error("{0}")]
    ManifestPath(agentdp_core::manifest::PathError),
    #[error("{0}")]
    PlatformPaths(agentdp_core::platform::Error),
    #[error("{0}")]
    Server(server_client::Error),
    #[error("port override was provided more than once: {0}")]
    DuplicatePort(String),
}
