use std::path::{Path, PathBuf};
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::platform::ssh as platform_ssh;
use agentdp_protocol::HostCommandResult;
use thiserror::Error;

use crate::instance::state::{InstanceState, PortProtocolState};

pub type CommandOutput = platform_ssh::CommandOutput;

const AGENT_ENV_COMMAND: &str = "/usr/local/bin/agentdp-agent-env";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuestAccess {
    pub user: String,
    pub private_key: PathBuf,
    pub public_key: PathBuf,
    pub public_key_contents: String,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Tool(#[from] platform_ssh::Error),
    #[error("instance runtime has no guest SSH access metadata")]
    MissingAccess,
    #[error("instance runtime has no tcp ssh port mapping")]
    MissingSshPort,
    #[error("guest SSH private key does not exist: {0}")]
    MissingPrivateKey(PathBuf),
}

impl Error {
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::Tool(error) => error.is_retryable(),
            Self::MissingAccess | Self::MissingSshPort | Self::MissingPrivateKey(_) => false,
        }
    }
}

/// Generates SSH key material used by the host to access the guest.
///
/// # Errors
///
/// Returns an error if `ssh-keygen` cannot create the key pair.
pub fn generate_guest_access(
    context: &Context,
    work_dir: &Path,
    ssh_keygen: &platform_ssh::SshKeygen,
    user: &str,
) -> Result<GuestAccess, Error> {
    let key_pair = ssh_keygen.generate_key_pair(context, work_dir)?;
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
pub fn interactive_shell_command(state: &InstanceState) -> Result<HostCommandResult, Error> {
    let connection = connection_info(state)?;
    Ok(HostCommandResult {
        program: platform_ssh::SSH_BINARY.to_owned(),
        args: platform_ssh::interactive_shell_args(
            &connection,
            &agent_env_shell_command("cd /data/home/code 2>/dev/null || cd; exec ${SHELL:-/bin/sh} -l"),
        ),
    })
}

/// Runs a root command in the guest over SSH.
///
/// # Errors
///
/// Returns an error if guest SSH metadata is missing or the SSH command fails.
pub fn run_command_with_timeout(
    context: &Context,
    state: &InstanceState,
    command: &str,
    timeout: Duration,
) -> Result<CommandOutput, Error> {
    run(context, state, command, timeout, platform_ssh::CommandPrivilege::Root)
}

/// Runs a user command in the guest over SSH after loading agent environment.
///
/// # Errors
///
/// Returns an error if guest SSH metadata is missing or the SSH command fails.
pub fn run_user_command_with_timeout(
    context: &Context,
    state: &InstanceState,
    args: &[String],
    timeout: Duration,
) -> Result<CommandOutput, Error> {
    let command = platform_ssh::shell_join(args);
    run(
        context,
        state,
        &agent_env_shell_command(&command),
        timeout,
        platform_ssh::CommandPrivilege::User,
    )
}

fn agent_env_shell_command(command: &str) -> String {
    platform_ssh::shell_join(&[
        AGENT_ENV_COMMAND.to_owned(),
        "sh".to_owned(),
        "-lc".to_owned(),
        command.to_owned(),
    ])
}

fn run(
    context: &Context,
    state: &InstanceState,
    command: &str,
    timeout: Duration,
    privilege: platform_ssh::CommandPrivilege,
) -> Result<CommandOutput, Error> {
    let connection = connection_info(state)?;
    Ok(platform_ssh::SshClient::resolve()?.run_command_with_timeout(
        context,
        &connection,
        command,
        timeout,
        privilege,
    )?)
}

fn connection_info(state: &InstanceState) -> Result<platform_ssh::ConnectionInfo, Error> {
    let access = state.guest_access.as_ref().ok_or(Error::MissingAccess)?;
    let port = state
        .network
        .ports
        .get("ssh")
        .filter(|port| port.protocol == PortProtocolState::Tcp)
        .ok_or(Error::MissingSshPort)?;
    let private_key = PathBuf::from(&access.private_key);
    if !private_key.is_file() {
        return Err(Error::MissingPrivateKey(private_key));
    }

    Ok(platform_ssh::ConnectionInfo {
        user: access.user.clone(),
        host: "127.0.0.1".to_owned(),
        port: port.host,
        private_key,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::connection_info;
    use crate::instance::state::{
        GuestAccessState, InstanceState, InstanceStatus, ManifestState, NetworkModeState, NetworkState,
        PortMappingState, PortProtocolState,
    };
    use crate::qemu::runtime::{ImageState, State as QemuState};
    use crate::runtime::BackendState;

    #[test]
    fn reads_connection_info_from_state() {
        let state = instance_state();

        let connection = connection_info(&state).unwrap();

        assert_eq!(connection.user, "arch");
        assert_eq!(connection.host, "127.0.0.1");
        assert_eq!(connection.port, 2222);
        assert_eq!(
            connection.private_key,
            std::env::temp_dir().join(format!("agentdp-test-existing-private-key-{}", std::process::id()))
        );
    }

    fn instance_state() -> InstanceState {
        let private_key =
            std::env::temp_dir().join(format!("agentdp-test-existing-private-key-{}", std::process::id()));
        std::fs::write(&private_key, "").unwrap();
        InstanceState {
            version: 1,
            manifest_name: "altinn-studio".to_owned(),
            instance: "pr-0".to_owned(),
            status: InstanceStatus::Running,
            manifest: ManifestState {
                source: "/manifest/source.yaml".to_owned(),
                copy: "/manifest/copy.yaml".to_owned(),
            },
            network: NetworkState {
                mode: NetworkModeState::User,
                ports: BTreeMap::from([(
                    "ssh".to_owned(),
                    PortMappingState {
                        guest: 22,
                        host: 2222,
                        protocol: PortProtocolState::Tcp,
                    },
                )]),
            },
            guest_access: Some(GuestAccessState {
                user: "arch".to_owned(),
                private_key: private_key.display().to_string(),
                public_key: "/instances/pr-0/generated/qemu/ssh/agentdp_ed25519.pub".to_owned(),
            }),
            backend: BackendState::Qemu(QemuState {
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
                disk: "/instances/pr-0/disk.qcow2".to_owned(),
                work_dir: "/instances/pr-0/generated/qemu".to_owned(),
                seed_media: "/instances/pr-0/generated/qemu/seed.img".to_owned(),
                seed_meta_data: "/instances/pr-0/generated/qemu/seed/meta-data".to_owned(),
                seed_user_data: "/instances/pr-0/generated/qemu/seed/user-data".to_owned(),
                bootstrap_script: "/instances/pr-0/generated/qemu/seed/bootstrap.sh".to_owned(),
                monitor_socket: "/runtime/pr-0/qemu/monitor.sock".to_owned(),
                qmp_socket: "/runtime/pr-0/qemu/qmp.sock".to_owned(),
                pid_file: "/runtime/pr-0/qemu/qemu.pid".to_owned(),
                serial_log: "/instances/pr-0/logs/serial.log".to_owned(),
                qemu_log: "/instances/pr-0/logs/qemu.log".to_owned(),
                pid: None,
                last_start_unix_seconds: None,
            }),
            readiness: Option::default(),
        }
    }
}
