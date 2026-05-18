use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::Context;
use agentdp_core::manifest::resolve_manifest_path;
use agentdp_protocol::{BackendRuntimeResult, InstanceRef, InstanceUpResult, RequestKind};
use clap::Args;

use crate::server_client;

#[derive(Debug, Args)]
pub struct Command {
    pub instance: String,

    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,
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
    context
        .logger()
        .info(format!("starting {} from {}", command.instance, manifest.display()));
    context
        .logger()
        .info("waiting for VM boot, cloud-init, and manifest healthchecks");
    let mut on_event = |event| server_client::log_event(context, event);
    let result: InstanceUpResult = server_client::request(
        context,
        paths,
        RequestKind::InstanceUp(InstanceRef {
            manifest,
            instance: command.instance.clone(),
        }),
        Some(&mut on_event),
    )
    .map_err(Error::Server)?;

    println!("started {}", result.name);
    match result.process.pid {
        Some(pid) => println!("pid: {pid}"),
        None => println!("pid: none"),
    }
    println!("state: {}", result.state);
    match &result.backend {
        BackendRuntimeResult::Qemu(qemu) => {
            println!("monitor: {}", qemu.monitor_socket);
            println!("serial: {}", qemu.serial_log);
        }
    }
    if result.readiness.ready {
        println!("ready: true");
    }
    if let Some(url) = result
        .readiness
        .services
        .get("code-server")
        .and_then(|service| service.url.as_deref())
    {
        println!("code-server: {url}");
    }
    print_healthchecks(&result);
    Ok(())
}

fn print_healthchecks(result: &InstanceUpResult) {
    for healthcheck in &result.readiness.healthchecks {
        let name = &healthcheck.name;
        let status = &healthcheck.status;
        match healthcheck.reason.as_deref() {
            Some(reason) => println!("healthcheck {name}: {status} ({reason})"),
            None => println!("healthcheck {name}: {status}"),
        }
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
}
