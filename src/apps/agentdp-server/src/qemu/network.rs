use std::net::SocketAddr;
use std::path::PathBuf;

use agentdp_core::Context;
#[cfg(test)]
use agentdp_core::agent::AgentInstanceNetworkStatus;
use agentdp_core::agent::{
    NetworkAllowState, NetworkState, PortProtocolState, QemuInstanceNetworkState, QemuMediatedCaState,
};
use agentdp_core::provisioning::secrets::SecretBindings;
use agentdp_ds::SecretString;
use agentdp_network::InstanceNetworkStatus;
use agentdp_network::{
    InstanceNetworkConfig, InstanceNetworkError, InstanceNetworkSpec, RuntimeSecret, RuntimeSecrets, TlsInterceptConfig,
};
use agentdp_platform::{self as platform, process::ProcessStatus};
use agentdp_qemu::net::{QemuStreamTransport, stream};

use crate::agent::{AgentName, InstanceName};
use crate::services::{FRAME_WRITE_TIMEOUT, InstanceNetwork, InstanceNetworkHandle, RECONNECT_DELAY, TRAFFIC_TIMEOUT};

use super::State;
use super::error::{Error, ErrorKind};
use super::lifecycle::TERMINATE_WAIT;

pub(super) async fn start_instance_network(
    context: &Context,
    runtime: &InstanceNetwork,
    agent: &str,
    instance: &str,
    instance_network: &NetworkState,
    state: &State,
    secrets: SecretBindings,
) -> Result<Option<InstanceNetworkHandle>, Error> {
    let Some(network) = state.instance_network.clone() else {
        return Ok(None);
    };
    let agent = AgentName::new(agent);
    let instance = InstanceName::new(instance);
    let socket = PathBuf::from(&network.stream_socket);
    QemuStreamTransport::prepare_server_socket(&socket).await?;
    spawn_instance_network(InstanceNetworkStart {
        context,
        runtime,
        agent,
        instance,
        config: instance_network,
        state,
        secrets,
        qemu: network,
    })
    .await
}

pub(super) async fn ensure_instance_network_attached(
    context: &Context,
    runtime: &InstanceNetwork,
    agent: &str,
    instance: &str,
    instance_network: &NetworkState,
    state: &State,
    secrets: SecretBindings,
) -> Result<bool, Error> {
    let Some(network) = state.instance_network.clone() else {
        return Ok(false);
    };
    let agent = AgentName::new(agent);
    let instance = InstanceName::new(instance);
    if runtime.is_running() {
        return Ok(false);
    }
    let task = spawn_instance_network(InstanceNetworkStart {
        context,
        runtime,
        agent,
        instance,
        config: instance_network,
        state,
        secrets,
        qemu: network,
    })
    .await?;
    Ok(task.is_some())
}

pub(super) fn update_instance_network_secrets(
    runtime: &InstanceNetwork,
    agent: &str,
    instance: &str,
    secrets: &SecretBindings,
) -> Result<bool, Error> {
    let label = format!("{agent}/{instance}");
    runtime
        .update_secrets(runtime_secrets(secrets))
        .map_err(|error| instance_network_error(label, &error))
}

pub(super) async fn instance_network_is_attached(
    runtime: &InstanceNetwork,
    agent: &AgentName,
    instance: &InstanceName,
    state: &State,
) -> bool {
    if state.instance_network.is_none() {
        return true;
    }
    let _label = agent_instance_label(agent, instance);
    runtime.is_running()
}

struct InstanceNetworkStart<'a> {
    context: &'a Context,
    runtime: &'a InstanceNetwork,
    agent: AgentName,
    instance: InstanceName,
    config: &'a NetworkState,
    state: &'a State,
    secrets: SecretBindings,
    qemu: QemuInstanceNetworkState,
}

async fn spawn_instance_network(input: InstanceNetworkStart<'_>) -> Result<Option<InstanceNetworkHandle>, Error> {
    let label = agent_instance_label(&input.agent, &input.instance);
    input
        .runtime
        .cleanup()
        .await
        .map_err(|error| instance_network_error(label.clone(), &error))?;
    let socket = PathBuf::from(&input.qemu.stream_socket);
    let config =
        instance_network_config(&label, input.config, input.state, input.qemu.addresses, input.secrets).await?;
    let transport = QemuStreamTransport::connect(socket);
    let spec = InstanceNetworkSpec {
        label: label.clone(),
        config,
        reconnect_delay: RECONNECT_DELAY,
        write_timeout: FRAME_WRITE_TIMEOUT,
    };
    let task = input
        .runtime
        .start(input.context, spec, transport)
        .map_err(|error| instance_network_error(label, &error))?;
    Ok(Some(task))
}

async fn instance_network_config(
    label: &str,
    instance_network: &NetworkState,
    state: &State,
    network: agentdp_network::InstanceAddresses,
    secrets: SecretBindings,
) -> Result<InstanceNetworkConfig, Error> {
    let ca_key_pem = mediated_ca_key_pem(&state.mediated_ca).await?;
    let tls = TlsInterceptConfig {
        ca_cert_pem: state.mediated_ca.cert_pem.clone(),
        ca_key_pem: ca_key_pem.expose_secret().to_owned(),
        upstream_root_ca_pems: Vec::new(),
        intercepted_ports: vec![443],
        bypass_hosts: Vec::new(),
    };
    if !secrets.is_empty() && !tls.is_enabled() {
        return Err(ErrorKind::InstanceNetworkConnect {
            instance: label.to_owned(),
            message: "instance network has host secrets but no TLS interception CA was configured".to_owned(),
        }
        .into());
    }

    let mac = super::lifecycle::instance_network_mac(agentdp_core::mediated_network::DEFAULT_PROFILE);
    let mut config = InstanceNetworkConfig::new(network, mac, egress_policy(instance_network));
    config.policy = config.policy.with_secrets(runtime_secrets(&secrets));
    config.tls = tls.is_enabled().then_some(tls);
    config.host_ports = host_port_specs(instance_network).collect();
    config.dns_upstream = dns_upstream().await;
    config.ipv6_enabled = instance_network.ipv6.is_enabled();
    Ok(config)
}

async fn mediated_ca_key_pem(ca: &QemuMediatedCaState) -> Result<SecretString, Error> {
    match (!ca.cert_pem.is_empty(), !ca.key_path.is_empty()) {
        (false, false) => return Ok(SecretString::empty()),
        (true, true) => {}
        (cert_configured, key_path_configured) => {
            return Err(ErrorKind::IncompleteMediatedCaState {
                cert_configured,
                key_path_configured,
            }
            .into());
        }
    }
    let key_path = PathBuf::from(&ca.key_path);
    let key = tokio::fs::read_to_string(&key_path)
        .await
        .map_err(|source| ErrorKind::ReadMediatedCaKey { path: key_path, source })?;
    Ok(SecretString::new(key))
}

fn runtime_secrets(secrets: &SecretBindings) -> RuntimeSecrets {
    let mut runtime = RuntimeSecrets::new();
    for binding in secrets.iter() {
        if let Some(value) = binding.value() {
            runtime.insert(RuntimeSecret::new(
                binding.placeholder.clone(),
                value,
                binding.allowed_hosts.iter().cloned(),
            ));
        }
    }
    runtime
}

async fn dns_upstream() -> SocketAddr {
    let address = platform::dns::system_dns_servers()
        .await
        .ok()
        .and_then(|servers| servers.into_iter().next())
        .unwrap_or_else(platform::dns::fallback_dns_server);
    SocketAddr::new(address, 53)
}

pub(super) async fn wait_instance_network_ready(
    context: &Context,
    task: InstanceNetworkHandle,
    agent: &str,
    instance: &str,
) -> Result<InstanceNetworkStatus, Error> {
    let instance_name = format!("{agent}/{instance}");
    context.logger().verbose_with(|| {
        format!("waiting up to {TRAFFIC_TIMEOUT:?} for instance network guest traffic for {instance_name}")
    });
    task.wait_ready(TRAFFIC_TIMEOUT)
        .await
        .map_err(|error| instance_network_error(instance_name.clone(), &error))?;
    context
        .logger()
        .verbose_with(|| format!("instance network guest traffic observed for {instance_name}"));
    Ok(task.status())
}

pub(super) async fn terminate_started_qemu(context: &Context, instance: &str, pid: u32) -> Result<(), Error> {
    context.logger().warn(format!(
        "terminating QEMU pid {pid} for {instance} after instance network startup failure"
    ));
    match platform::process::terminate_process(pid).await {
        Ok(()) => {
            if !platform::process::wait_for_process_exit(pid, TERMINATE_WAIT).await? {
                return Err(ErrorKind::ProcessStillRunning { pid }.into());
            }
            Ok(())
        }
        Err(error) => match platform::process::process_status(pid).await? {
            ProcessStatus::NotFound => Ok(()),
            ProcessStatus::Running => Err(error.into()),
        },
    }
}

pub(super) fn egress_policy(network: &NetworkState) -> agentdp_network::EgressPolicy {
    match &network.allow {
        NetworkAllowState::All => agentdp_network::EgressPolicy::allow_all(),
        NetworkAllowState::Public => agentdp_network::EgressPolicy::default_deny_private(),
        NetworkAllowState::Hosts(hosts) => hosts
            .iter()
            .fold(agentdp_network::EgressPolicy::default_deny_private(), |policy, host| {
                policy.with_allowed_authority(host)
            }),
    }
}

fn host_port_specs(network: &NetworkState) -> impl Iterator<Item = agentdp_network::HostPortSpec> + '_ {
    network.ports.iter().map(|(name, port)| agentdp_network::HostPortSpec {
        name: name.clone(),
        protocol: match port.protocol {
            PortProtocolState::Tcp => agentdp_network::HostPortProtocol::Tcp,
            PortProtocolState::Udp => agentdp_network::HostPortProtocol::Udp,
        },
        guest: port.guest,
        host: port.host.unwrap_or(0),
    })
}

#[cfg(test)]
pub(super) fn instance_network_status(
    runtime: &InstanceNetwork,
    agent: &AgentName,
    instance: &InstanceName,
    state: &State,
) -> Option<AgentInstanceNetworkStatus> {
    state.instance_network.as_ref()?;
    let _label = agent_instance_label(agent, instance);
    runtime.observation().map(|observation| observation.status())
}

pub(super) async fn cleanup_instance_network_for_state(
    runtime: &InstanceNetwork,
    agent: &AgentName,
    instance: &InstanceName,
    state: &State,
) -> Result<(), Error> {
    if let Some(network) = &state.instance_network {
        runtime
            .cleanup()
            .await
            .map_err(|error| instance_network_error(agent_instance_label(agent, instance), &error))?;
        stream::cleanup_socket_after_close(&network.stream_socket).await?;
    }
    Ok(())
}

fn agent_instance_label(agent: &AgentName, instance: &InstanceName) -> String {
    format!("{agent}/{instance}")
}

fn instance_network_error(instance: String, error: &InstanceNetworkError) -> Error {
    ErrorKind::InstanceNetworkConnect {
        instance,
        message: error.to_string(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::mediated_ca_key_pem;
    use crate::qemu::MediatedCaState;

    #[test]
    fn mediated_ca_state_requires_key_path_when_deserialized() {
        let value = "cert_pem: cert\n";

        assert!(serde_yaml::from_str::<MediatedCaState>(value).is_err());
    }

    #[tokio::test]
    async fn mediated_ca_key_requires_complete_cert_and_key_path_state() {
        let ca = MediatedCaState::new("cert".to_owned(), String::new());

        let error = mediated_ca_key_pem(&ca).await.unwrap_err();

        assert!(error.to_string().contains("mediated CA state is incomplete"), "{error}");
    }
}
