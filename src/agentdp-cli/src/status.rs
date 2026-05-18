use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::Context;
use agentdp_core::manifest::resolve_manifest_path;
use agentdp_protocol::{BackendStatusResult, InstanceRef, InstanceStatusResult, RequestKind};
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
    let result: InstanceStatusResult = server_client::request(
        context,
        paths,
        RequestKind::InstanceStatus(InstanceRef {
            manifest,
            instance: command.instance.clone(),
        }),
        None,
    )
    .map_err(Error::Server)?;

    println!("status {}", result.name);
    println!("status: {}", result.status);
    println!("state: {}", result.state);
    println!("process: {}", result.process.status);
    match result.process.pid {
        Some(pid) => println!("pid: {pid}"),
        None => println!("pid: none"),
    }
    if result.stale {
        let message = result.process.message.as_deref().unwrap_or("runtime state is stale");
        println!("stale: {message}");
    }
    print_readiness(&result);
    match &result.backend {
        BackendStatusResult::Qemu(qemu) => {
            println!("disk: {}", qemu.disk);
            println!("seed: {}", qemu.seed_media);
            println!("monitor: {}", qemu.monitor_socket);
            println!("qmp: {}", qemu.qmp_socket);
        }
    }
    print_ports(&result);
    Ok(())
}

fn print_readiness(value: &InstanceStatusResult) {
    let Some(readiness) = &value.readiness else {
        println!("readiness: unknown");
        return;
    };

    if readiness.ready {
        println!("readiness: ready");
    } else {
        println!("readiness: not-ready");
    }
    println!("last_ready_unix: {}", readiness.last_success_unix_seconds);
    let healthchecks = &readiness.result.healthchecks;
    if healthchecks.is_empty() {
        println!("healthchecks: none");
        return;
    }
    println!("healthchecks:");
    for healthcheck in healthchecks {
        let name = &healthcheck.name;
        let kind = &healthcheck.kind;
        let status = &healthcheck.status;
        let elapsed = healthcheck.elapsed_ms;
        println!("  {name}: {status} ({kind}, {elapsed}ms)");
    }
}

fn print_ports(value: &InstanceStatusResult) {
    let ports = &value.network.ports;
    if ports.is_empty() {
        println!("ports: none");
        return;
    }

    println!("ports:");
    let mut names = ports.keys().collect::<Vec<_>>();
    names.sort();
    for name in names {
        let port = &ports[name];
        let protocol = port.protocol.as_str();
        let host = port.host;
        let guest = port.guest;
        println!("  {name}: {protocol} {host}->{guest}");
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
