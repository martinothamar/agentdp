use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::{
    Context,
    agent::{
        AgentInstanceEvent, AgentInstanceEventEnvelope, AgentInstanceNetworkEvent, AgentInstanceNetworkEventKind,
        PortProtocolState,
    },
    layout::AgentdpLayout,
    manifest::LoadedAgentManifest,
};
use agentdp_protocol::client_server::{AgentInstanceLogsParams, AgentInstanceLogsResult, LogFile, RequestKind};
use clap::{ArgAction, Args, ValueEnum};

use crate::server_client;

#[derive(Debug, Args)]
pub(crate) struct Command {
    #[arg(value_name = "INSTANCE_ID")]
    pub instance_id: u32,

    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,

    #[arg(long, action = ArgAction::Count)]
    serial: u8,

    #[arg(long, action = ArgAction::Count)]
    qemu: u8,

    #[arg(long, action = ArgAction::Count, help = "Read the raw instance event log")]
    events: u8,

    #[arg(long, action = ArgAction::Count, help = "Read network events from the instance event log")]
    network: u8,

    #[arg(long, help = "Show only network events classified as errors")]
    errors: bool,

    #[arg(long, value_enum, help = "Show only network events of this kind")]
    kind: Option<NetworkEventKind>,

    #[arg(long, help = "Print raw matching event JSON lines")]
    json: bool,

    #[arg(long, default_value_t = 200, visible_alias = "tail")]
    lines: usize,
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
    if command.lines == 0 {
        return Err(Error::InvalidLines);
    }
    validate_log_selection(command)?;

    let manifest = LoadedAgentManifest::load_from_current_dir(context, command.file.as_deref()).await?;
    let layout = AgentdpLayout::resolve().map_err(Error::AgentdpLayout)?;
    let result: AgentInstanceLogsResult = server_client::request(
        context,
        &layout,
        RequestKind::AgentInstanceLogs(AgentInstanceLogsParams {
            agent: manifest.agent_name().to_owned(),
            instance_id: command.instance_id,
            file: log_file(command),
            lines: requested_log_lines(command),
        }),
        None,
    )
    .await
    .map_err(Error::Server)?;

    if flag_selected(command.network) {
        print_network_events(&result.contents, command)?;
    } else {
        print!("{}", result.contents);
    }
    Ok(())
}

const fn log_file(command: &Command) -> LogFile {
    if flag_selected(command.events) || flag_selected(command.network) {
        LogFile::Events
    } else if flag_selected(command.qemu) {
        LogFile::Qemu
    } else {
        LogFile::Serial
    }
}

const fn requested_log_lines(command: &Command) -> usize {
    if flag_selected(command.network) {
        usize::MAX
    } else {
        command.lines
    }
}

const fn flag_selected(flag: u8) -> bool {
    flag > 0
}

fn validate_log_selection(command: &Command) -> Result<(), Error> {
    let selected_count = [command.serial, command.qemu, command.events, command.network]
        .into_iter()
        .filter(|flag| flag_selected(*flag))
        .count();
    if selected_count > 1 {
        return Err(Error::ConflictingLogSelection);
    }
    if (command.errors || command.kind.is_some()) && !flag_selected(command.network) {
        return Err(Error::NetworkFilterRequiresNetwork);
    }
    if command.json && !flag_selected(command.network) {
        return Err(Error::JsonRequiresNetwork);
    }
    Ok(())
}

fn print_network_events(contents: &str, command: &Command) -> Result<(), Error> {
    let mut output = Vec::new();
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let envelope = serde_json::from_str::<AgentInstanceEventEnvelope>(line).map_err(Error::ParseEvent)?;
        let AgentInstanceEvent::NetworkEvent(event) = &envelope.event else {
            continue;
        };
        if !network_event_matches(event, command) {
            continue;
        }
        if command.json {
            output.push(line.to_owned());
        } else {
            output.push(format_network_event(event));
        }
    }
    let start = output.len().saturating_sub(command.lines);
    for line in &output[start..] {
        println!("{line}");
    }
    Ok(())
}

fn network_event_matches(event: &AgentInstanceNetworkEvent, command: &Command) -> bool {
    if command.errors && !network_event_is_error(&event.event) {
        return false;
    }
    if let Some(kind) = command.kind
        && kind != network_event_kind(&event.event)
    {
        return false;
    }
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum NetworkEventKind {
    Lifecycle,
    Telemetry,
    Transport,
    Egress,
    Dns,
    HostPort,
    Reactor,
}

const fn network_event_kind(event: &AgentInstanceNetworkEventKind) -> NetworkEventKind {
    match event {
        AgentInstanceNetworkEventKind::LifecycleStateChanged { .. } => NetworkEventKind::Lifecycle,
        AgentInstanceNetworkEventKind::TelemetrySnapshot { .. } => NetworkEventKind::Telemetry,
        AgentInstanceNetworkEventKind::TransportConnectFailed { .. }
        | AgentInstanceNetworkEventKind::TransportGuestConnected { .. }
        | AgentInstanceNetworkEventKind::TransportGuestDisconnected { .. }
        | AgentInstanceNetworkEventKind::TransportRegisterFailed { .. } => NetworkEventKind::Transport,
        AgentInstanceNetworkEventKind::EgressError { .. } | AgentInstanceNetworkEventKind::EgressProxyClosed { .. } => {
            NetworkEventKind::Egress
        }
        AgentInstanceNetworkEventKind::DnsResolved { .. } => NetworkEventKind::Dns,
        AgentInstanceNetworkEventKind::HostPortBound { .. } | AgentInstanceNetworkEventKind::HostPortError { .. } => {
            NetworkEventKind::HostPort
        }
        AgentInstanceNetworkEventKind::ReactorError { .. } => NetworkEventKind::Reactor,
    }
}

fn network_event_is_error(event: &AgentInstanceNetworkEventKind) -> bool {
    match event {
        AgentInstanceNetworkEventKind::LifecycleStateChanged { state } => state == "backoff" || state == "failed",
        AgentInstanceNetworkEventKind::TransportConnectFailed { .. }
        | AgentInstanceNetworkEventKind::TransportGuestDisconnected { .. }
        | AgentInstanceNetworkEventKind::TransportRegisterFailed { .. }
        | AgentInstanceNetworkEventKind::EgressError { .. }
        | AgentInstanceNetworkEventKind::HostPortError { .. }
        | AgentInstanceNetworkEventKind::ReactorError { .. } => true,
        AgentInstanceNetworkEventKind::TelemetrySnapshot { .. }
        | AgentInstanceNetworkEventKind::TransportGuestConnected { .. }
        | AgentInstanceNetworkEventKind::EgressProxyClosed { .. }
        | AgentInstanceNetworkEventKind::DnsResolved { .. }
        | AgentInstanceNetworkEventKind::HostPortBound { .. } => false,
    }
}

fn format_network_event(event: &AgentInstanceNetworkEvent) -> String {
    let prefix = format!("#{} {}", event.sequence, event.unix_millis);
    match &event.event {
        AgentInstanceNetworkEventKind::LifecycleStateChanged { state } => {
            format!("{prefix} lifecycle.state_changed state={state}")
        }
        AgentInstanceNetworkEventKind::TelemetrySnapshot {
            guest_frames_received,
            guest_bytes_received,
            host_frames_sent,
            host_bytes_sent,
            session_disconnects,
            connect_errors,
            egress_errors,
            ..
        } => format!(
            "{prefix} telemetry.snapshot guest_rx={guest_frames_received}/{guest_bytes_received} host_tx={host_frames_sent}/{host_bytes_sent} disconnects={session_disconnects} connect_errors={connect_errors} egress_errors={egress_errors}",
        ),
        AgentInstanceNetworkEventKind::TransportConnectFailed { transport, error } => {
            format!("{prefix} transport.connect_failed transport={transport} error={error}")
        }
        AgentInstanceNetworkEventKind::TransportGuestConnected { transport, generation } => {
            format!("{prefix} transport.guest_connected transport={transport} generation={generation}")
        }
        AgentInstanceNetworkEventKind::TransportGuestDisconnected { generation, reason } => {
            format!("{prefix} transport.guest_disconnected generation={generation} reason={reason}")
        }
        AgentInstanceNetworkEventKind::TransportRegisterFailed { transport, error } => {
            format!("{prefix} transport.register_failed transport={transport} error={error}")
        }
        AgentInstanceNetworkEventKind::EgressError {
            protocol,
            proxy,
            destination,
            upstream,
            authority,
            route,
            phase,
            message,
        } => {
            let mut output = format!(
                "{prefix} egress.error protocol={protocol} proxy={}",
                optional_u64(*proxy)
            );
            push_optional_field(&mut output, "destination", destination.as_deref());
            push_optional_field(&mut output, "upstream", upstream.as_deref());
            push_optional_field(&mut output, "authority", authority.as_deref());
            push_optional_field(&mut output, "route", route.as_deref());
            push_optional_field(&mut output, "phase", phase.as_deref());
            output.push_str(" message=");
            output.push_str(message);
            output
        }
        AgentInstanceNetworkEventKind::EgressProxyClosed { protocol, proxy } => format!(
            "{prefix} egress.proxy_closed protocol={protocol} proxy={}",
            optional_u64(*proxy)
        ),
        AgentInstanceNetworkEventKind::DnsResolved {
            protocol,
            host,
            addresses,
            ttl_millis,
        } => format!(
            "{prefix} dns.resolved protocol={protocol} host={host} addresses={} ttl_millis={ttl_millis}",
            addresses
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
        AgentInstanceNetworkEventKind::HostPortBound {
            name,
            protocol,
            guest,
            host,
        } => {
            format!(
                "{prefix} host_port.bound name={name} protocol={} host={host} guest={guest}",
                port_protocol(*protocol)
            )
        }
        AgentInstanceNetworkEventKind::HostPortError { message } => {
            format!("{prefix} host_port.error message={message}")
        }
        AgentInstanceNetworkEventKind::ReactorError { message } => {
            format!("{prefix} reactor.error message={message}")
        }
    }
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_owned(), |value| value.to_string())
}

fn push_optional_field(output: &mut String, name: &str, value: Option<&str>) {
    if let Some(value) = value {
        output.push(' ');
        output.push_str(name);
        output.push('=');
        output.push_str(value);
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
    #[error("log line count must be greater than zero")]
    InvalidLines,
    #[error("select at most one log source")]
    ConflictingLogSelection,
    #[error("--errors and --kind require --network")]
    NetworkFilterRequiresNetwork,
    #[error("--json is only supported with --network")]
    JsonRequiresNetwork,
    #[error("failed to parse instance event log record: {0}")]
    ParseEvent(serde_json::Error),
}
