use std::path::{Path, PathBuf};
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::agent::{AgentInstanceDocument, PortProtocolState};
use agentdp_platform::ssh as platform_ssh;
use agentdp_protocol::client_server::HostCommandResult;
use thiserror::Error;

pub(crate) type CommandOutput = platform_ssh::CommandOutput;

const AGENT_ENV_COMMAND: &str = "/usr/local/bin/agentdp-agent-env";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GuestAccess {
    pub user: String,
    pub private_key: PathBuf,
    pub public_key: PathBuf,
    pub public_key_contents: String,
}

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Tool(#[from] platform_ssh::Error),
    #[error("instance runtime has no guest SSH access metadata")]
    MissingAccess,
    #[error("instance runtime has no tcp ssh port mapping")]
    MissingSshPort,
    #[error("guest SSH private key does not exist: {0}")]
    MissingPrivateKey(PathBuf),
}

/// Generates SSH key material used by the host to access the guest.
///
/// # Errors
///
/// Returns an error if `ssh-keygen` cannot create the key pair.
pub(crate) async fn generate_guest_access(
    context: &Context,
    work_dir: &Path,
    ssh_keygen: &platform_ssh::SshKeygen,
    user: &str,
) -> Result<GuestAccess, Error> {
    context
        .logger()
        .verbose_with(|| format!("generating instance SSH key under {}", work_dir.display()));
    let key_pair = ssh_keygen.generate_key_pair(work_dir).await?;
    Ok(GuestAccess {
        user: user.to_owned(),
        private_key: key_pair.private_key,
        public_key: key_pair.public_key,
        public_key_contents: key_pair.public_key_contents,
    })
}

/// Builds the host command used to open an interactive guest shell.
///
/// # Errors
///
/// Returns an error if guest SSH metadata is missing or invalid.
pub(crate) async fn interactive_shell_command(state: &AgentInstanceDocument) -> Result<HostCommandResult, Error> {
    let connection = connection_info(state).await?;
    Ok(HostCommandResult {
        program: platform_ssh::SSH_BINARY.to_owned(),
        args: platform_ssh::interactive_shell_args(
            &connection,
            &agent_env_shell_command(
                "cd \"${AGENTDP_CODE_DIR:-$HOME/code}\" 2>/dev/null || cd; exec ${SHELL:-/bin/sh} -l",
            ),
        ),
    })
}

/// Runs a command as the agent user in the guest over SSH after loading agent environment.
///
/// # Errors
///
/// Returns an error if guest SSH metadata is missing or the SSH command fails.
pub(crate) async fn exec(
    context: &Context,
    state: &AgentInstanceDocument,
    command: &str,
    timeout: Duration,
    output: &mut dyn platform_ssh::OutputSink,
) -> Result<CommandOutput, Error> {
    Box::pin(run_raw(
        context,
        state,
        &agent_env_shell_command(command),
        timeout,
        output,
    ))
    .await
}

fn agent_env_shell_command(command: &str) -> String {
    platform_ssh::shell_join(&[
        AGENT_ENV_COMMAND.to_owned(),
        "sh".to_owned(),
        "-lc".to_owned(),
        command.to_owned(),
    ])
}

async fn run_raw(
    context: &Context,
    state: &AgentInstanceDocument,
    command: &str,
    timeout: Duration,
    output: &mut dyn platform_ssh::OutputSink,
) -> Result<CommandOutput, Error> {
    let connection = connection_info(state).await?;
    context.logger().verbose_with(|| {
        format!(
            "running raw guest command over SSH on {}@{}:{}: {command}",
            connection.user, connection.host, connection.port
        )
    });
    let client = platform_ssh::SshClient::resolve().await?;
    Ok(Box::pin(client.run_raw_command_with_timeout_and_output(&connection, command, timeout, output)).await?)
}

async fn connection_info(state: &AgentInstanceDocument) -> Result<platform_ssh::ConnectionInfo, Error> {
    let access = state.status.guest_access.as_ref().ok_or(Error::MissingAccess)?;
    let port = state
        .status
        .network
        .ports
        .get("ssh")
        .filter(|port| port.protocol == PortProtocolState::Tcp)
        .and_then(|port| port.host)
        .ok_or(Error::MissingSshPort)?;
    let private_key = PathBuf::from(&access.private_key);
    if !tokio::fs::metadata(&private_key)
        .await
        .is_ok_and(|metadata| metadata.is_file())
    {
        return Err(Error::MissingPrivateKey(private_key));
    }

    Ok(platform_ssh::ConnectionInfo {
        user: access.user.clone(),
        host: "127.0.0.1".to_owned(),
        port,
        private_key,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use agentdp_core::agent::{
        AgentInstanceDocument, AgentInstancePhase, AgentInstanceTarget, BackendState, GuestAccessState,
        NetworkAllowState, NetworkIpv6State, NetworkModeState, NetworkState, PortMappingState, PortProtocolState,
    };
    use agentdp_core::manifest::AgentManifest;

    use super::connection_info;
    use crate::agent::{AgentBaseKey, AgentInstanceId, AgentName, InstanceName};
    use crate::qemu::{ImageState, MediatedCaState, State as QemuState};

    #[tokio::test(flavor = "local")]
    async fn reads_connection_info_from_state() {
        let state = agent_instance_document();

        let connection = connection_info(&state).await.unwrap();

        assert_eq!(connection.user, "arch");
        assert_eq!(connection.host, "127.0.0.1");
        assert_eq!(connection.port, 2222);
        assert_eq!(
            connection.private_key,
            std::env::temp_dir().join(format!("agentdp-test-existing-private-key-{}", std::process::id()))
        );
    }

    fn agent_instance_document() -> AgentInstanceDocument {
        let private_key =
            std::env::temp_dir().join(format!("agentdp-test-existing-private-key-{}", std::process::id()));
        std::fs::write(&private_key, "").unwrap();
        AgentInstanceDocument::new(
            AgentName::new("altinn-studio"),
            AgentInstanceId::new(0),
            InstanceName::new("replica-0"),
            1,
            AgentBaseKey::new("sha256-test"),
            manifest_template(),
            AgentInstanceTarget::Active,
            AgentInstancePhase::Running,
            NetworkState {
                mode: NetworkModeState::User,
                allow: NetworkAllowState::default(),
                ipv6: NetworkIpv6State::default(),
                ports: BTreeMap::from([(
                    "ssh".to_owned(),
                    PortMappingState {
                        guest: 22,
                        host: Some(2222),
                        protocol: PortProtocolState::Tcp,
                    },
                )]),
                runtime: None,
            },
            Vec::new(),
            Some(GuestAccessState {
                user: "arch".to_owned(),
                private_key: private_key.display().to_string(),
                public_key: "/instances/0/generated/qemu/ssh/agentdp_ed25519.pub".to_owned(),
            }),
            BackendState::Qemu(QemuState {
                image: ImageState {
                    os: "archlinux".to_owned(),
                    architecture: "x86_64".to_owned(),
                    variant: "cloudimg".to_owned(),
                    source_url: "https://example.test/image.qcow2".to_owned(),
                    cache_key: "archlinux-x86_64-cloudimg.qcow2".to_owned(),
                    cache_path: "/cache/image.qcow2".to_owned(),
                    download_path: "/cache/image.qcow2.part".to_owned(),
                    format: "qcow2".to_owned(),
                },
                disk: "/instances/0/disk.qcow2".to_owned(),
                work_dir: "/instances/0/generated/qemu".to_owned(),
                seed_media: "/instances/0/generated/qemu/seed.img".to_owned(),
                seed_meta_data: "/instances/0/generated/qemu/seed/meta-data".to_owned(),
                seed_network_config: "/instances/0/generated/qemu/seed/network-config".to_owned(),
                seed_user_data: "/instances/0/generated/qemu/seed/user-data".to_owned(),
                monitor_socket: "/runtime/0/qemu/monitor.sock".to_owned(),
                qmp_socket: "/runtime/0/qemu/qmp.sock".to_owned(),
                guest_control_socket: "/runtime/0/qemu/guest-control.sock".to_owned(),
                pid_file: "/runtime/0/qemu/qemu.pid".to_owned(),
                serial_log: "/instances/0/logs/serial.log".to_owned(),
                qemu_log: "/instances/0/logs/qemu.log".to_owned(),
                instance_network: None,
                mediated_secrets: agentdp_core::provisioning::secrets::SecretBindings::default(),
                mediated_ca: MediatedCaState::default(),
                pid: None,
                last_start_unix_seconds: None,
            }),
        )
    }

    fn manifest_template() -> agentdp_core::manifest::AgentSpec {
        serde_yaml::from_str::<AgentManifest>(
            "
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: altinn-studio
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 8G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap()
        .spec
        .template
    }
}
