use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::{
    Context,
    agent::{
        AgentDocument, AgentInstanceDocument, AgentInstanceNetworkStatus, AgentInstanceWorkStatus, BackendState,
        PortMappingState, PortProtocolState,
    },
    layout::AgentdpLayout,
    manifest::LoadedAgentManifest,
};
use agentdp_protocol::client_server::{AgentInstanceSelector, AgentSelector, RequestKind};
use clap::Args;

use crate::server_client;

#[derive(Debug, Args)]
pub(crate) struct Command {
    #[arg(value_name = "INSTANCE_ID")]
    pub instance_id: Option<u32>,

    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,

    #[arg(long)]
    json: bool,
}

pub(crate) async fn run(command: &Command, context: &Context) -> ExitCode {
    match try_run(command, context).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn try_run(command: &Command, context: &Context) -> Result<(), Error> {
    let manifest = LoadedAgentManifest::load_from_current_dir(context, command.file.as_deref()).await?;
    let layout = AgentdpLayout::resolve().map_err(Error::AgentdpLayout)?;
    if let Some(instance_id) = command.instance_id {
        let document = status_instance(context, &layout, manifest.agent_name().to_owned(), instance_id).await?;
        if command.json {
            println!("{}", serde_json::to_string_pretty(&document).map_err(Error::Json)?);
        } else {
            print_instance_document(&document);
        }
    } else {
        let document = status_agent(context, &layout, manifest.agent_name().to_owned()).await?;
        if command.json {
            println!("{}", serde_json::to_string_pretty(&document).map_err(Error::Json)?);
        } else {
            print_agent_document(&document);
        }
    }
    Ok(())
}

async fn status_agent(context: &Context, layout: &AgentdpLayout, agent: String) -> Result<AgentDocument, Error> {
    server_client::request(context, layout, RequestKind::AgentStatus(AgentSelector { agent }), None)
        .await
        .map_err(Error::Server)
}

async fn status_instance(
    context: &Context,
    layout: &AgentdpLayout,
    agent: String,
    instance_id: u32,
) -> Result<AgentInstanceDocument, Error> {
    server_client::request(
        context,
        layout,
        RequestKind::AgentInstanceStatus(AgentInstanceSelector { agent, instance_id }),
        None,
    )
    .await
    .map_err(Error::Server)
}

pub(crate) fn print_agent_document(document: &AgentDocument) {
    println!("status {}", document.agent());
    println!("desired generation: {}", document.generation());
    println!("observed generation: {}", document.observed_generation());
    println!("phase: {:?}", document.status.phase);
    println!("replicas: {}", document.status.replicas.desired);
    println!("ready replicas: {}", document.status.replicas.ready);
    println!("active replicas: {}", document.status.replicas.active);
    println!("stopped replicas: {}", document.status.replicas.stopped);
    println!("deleting replicas: {}", document.status.replicas.deleting);
    println!("reconciling: {}", document.status.reconciling);
    println!("deleted: {}", document.status.deleted);
    println!(
        "agent base: {:?} desired:{} ready:{}",
        document.status.agent_base.phase,
        document
            .status
            .agent_base
            .desired_key
            .as_ref()
            .map_or("none", |key| key.as_str()),
        document
            .status
            .agent_base
            .ready_key
            .as_ref()
            .map_or("none", |key| key.as_str())
    );
    if let Some(error) = &document.status.agent_base.last_error {
        println!("agent base error: {error}");
    }
    if document.status.instances.is_empty() {
        println!("instances: none");
        return;
    }
    println!("instances:");
    for (id, status) in &document.status.instances {
        let ready = if status.readiness.as_ref().is_some_and(|readiness| readiness.ready)
            && status.host_inputs.is_ready_for(document.generation())
        {
            "ready"
        } else {
            "not-ready"
        };
        println!(
            "  {id}: {} readiness:{ready} work:{}",
            status.phase,
            work_summary(&status.work)
        );
    }
}

fn work_summary(work: &AgentInstanceWorkStatus) -> String {
    let mut parts = Vec::new();
    if let Some(transition) = &work.transition {
        parts.push(format!("transition:{}", transition.kind.as_str()));
    }
    if let Some(bootstrap) = &work.bootstrap {
        let suffix = if bootstrap.next_retry_unix_seconds.is_some() {
            ":backoff"
        } else {
            ""
        };
        parts.push(format!("bootstrap{suffix}"));
    }
    if work.sessions.active > 0 {
        parts.push(format!("sessions:{}", work.sessions.active));
    }
    if parts.is_empty() {
        "idle".to_owned()
    } else {
        parts.join(",")
    }
}

fn print_instance_document(document: &AgentInstanceDocument) {
    println!("status {}/{}", document.metadata.agent, document.metadata.id);
    println!("status: {}", document.status.phase.as_str());
    println!("instance: {}", document.name());
    println!("desired generation: {}", document.spec.desired_generation);
    println!("observed generation: {}", document.status.observed_generation);
    println!("target: {}", document.spec.target.as_str());
    if let Some(reconciliation) = &document.status.reconciliation {
        println!("process: {}", reconciliation.observed_status);
        match reconciliation.observed_pid {
            Some(pid) => println!("pid: {pid}"),
            None => println!("pid: none"),
        }
        if reconciliation.stale {
            println!(
                "stale: {}",
                reconciliation.reason.as_deref().unwrap_or("runtime state is stale")
            );
        }
    } else {
        println!("process: unknown");
        println!("pid: none");
    }
    println!("work: {}", work_summary(&document.status.work));
    println!(
        "host inputs: {} (observed generation {})",
        document.status.host_inputs.phase.as_str(),
        document.status.host_inputs.observed_generation
    );
    if let Some(error) = &document.status.host_inputs.last_error {
        println!("host inputs error: {error}");
    }
    print_readiness(document);
    print_backend(document);
    print_ports("ports", document.status.network.ports.iter());
    print_network_runtime(document.status.network.runtime.as_ref());
    print_tailscale_serve(document);
}

fn print_readiness(document: &AgentInstanceDocument) {
    let Some(readiness) = &document.status.readiness else {
        println!("readiness: unknown");
        return;
    };

    if readiness.ready {
        println!("readiness: ready");
    } else {
        println!("readiness: not-ready");
    }
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

fn print_backend(document: &AgentInstanceDocument) {
    match &document.status.backend {
        BackendState::Qemu(qemu) => {
            println!("disk: {}", qemu.disk);
            println!("seed: {}", qemu.seed_media);
            println!("monitor: {}", qemu.monitor_socket);
            println!("qmp: {}", qemu.qmp_socket);
            if let Some(network) = &qemu.instance_network {
                println!("instance_network:");
                println!("  stream_socket: {}", network.stream_socket);
                println!(
                    "  guest_ipv4: {:?}/{}",
                    network.addresses.address, network.addresses.cidr_prefix
                );
                println!("  gateway_ipv4: {:?}", network.addresses.gateway);
            }
        }
    }
}

fn print_ports<'a>(label: &str, ports: impl Iterator<Item = (&'a String, &'a PortMappingState)>) {
    let mut ports = ports.collect::<Vec<_>>();
    if ports.is_empty() {
        println!("{label}: none");
        return;
    }
    ports.sort_by(|left, right| left.0.cmp(right.0));
    println!("{label}:");
    for (name, port) in ports {
        let protocol = port_protocol(port.protocol);
        let host = port.host.map_or_else(|| "unbound".to_owned(), |host| host.to_string());
        let guest = port.guest;
        println!("  {name}: {protocol} {host}->{guest}");
    }
}

fn print_network_runtime(runtime: Option<&AgentInstanceNetworkStatus>) {
    let Some(runtime) = runtime else {
        return;
    };
    println!("network_runtime:");
    println!("  state: {}", runtime.state);
    println!("  ready: {}", runtime.ready);
    if let Some(generation) = runtime.generation {
        println!("  generation: {generation}");
    }
    println!("  host_ports: {}", runtime.host_ports.len());
    println!(
        "  guest_rx: {} frames, {} bytes",
        runtime.guest_frames_received, runtime.guest_bytes_received
    );
    println!(
        "  host_tx: {} frames, {} bytes",
        runtime.host_frames_sent, runtime.host_bytes_sent
    );
    println!("  disconnects: {}", runtime.session_disconnects);
    println!("  connect_errors: {}", runtime.connect_errors);
    println!("  egress_errors: {}", runtime.egress_errors);
    println!("  event_drops: {}", runtime.network_event_drops);
    if let Some(error) = &runtime.last_error {
        println!("  last_error: {error}");
    }
}

fn print_tailscale_serve(document: &AgentInstanceDocument) {
    let Some(tailscale_serve) = &document.status.tailscale_serve else {
        return;
    };
    if tailscale_serve.routes.is_empty() {
        return;
    }

    println!("tailscale_serve:");
    for route in &tailscale_serve.routes {
        println!(
            "  {}: {} {} -> {} ({})",
            route.service, route.mode, route.url, route.target, route.status
        );
    }
}

const fn port_protocol(protocol: PortProtocolState) -> &'static str {
    match protocol {
        PortProtocolState::Tcp => "tcp",
        PortProtocolState::Udp => "udp",
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("{0}")]
    AgentManifest(#[from] agentdp_core::manifest::Error),
    #[error("{0}")]
    AgentdpLayout(agentdp_core::layout::Error),
    #[error("{0}")]
    Server(server_client::Error),
    #[error("failed to serialize status result as JSON: {0}")]
    Json(serde_json::Error),
}
