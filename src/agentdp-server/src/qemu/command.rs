use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use agentdp_core::manifest::AgentManifest;

use crate::instance::state::{NetworkState, PortProtocolState};
use crate::qemu::runtime::State as QemuState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommandSpec {
    pub(super) name: String,
    pub(super) cpus: u16,
    pub(super) memory: String,
    pub(super) disk: PathBuf,
    pub(super) seed_media: PathBuf,
    pub(super) monitor_socket: PathBuf,
    pub(super) qmp_socket: PathBuf,
    pub(super) pid_file: PathBuf,
    pub(super) serial_log: PathBuf,
    pub(super) qemu_log: PathBuf,
    pub(super) ports: BTreeMap<String, PortForward>,
    pub(super) daemonize: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PortForward {
    pub(super) guest: u16,
    pub(super) host: u16,
    pub(super) protocol: PortProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PortProtocol {
    Tcp,
    Udp,
}

pub(super) fn spec_from_state(
    manifest: &AgentManifest,
    manifest_name: &str,
    instance: &str,
    network: &NetworkState,
    state: &QemuState,
) -> CommandSpec {
    CommandSpec {
        name: format!("agentdp-{manifest_name}-{instance}"),
        cpus: manifest.resources.cpus,
        memory: manifest.resources.memory.clone(),
        disk: PathBuf::from(&state.disk),
        seed_media: PathBuf::from(&state.seed_media),
        monitor_socket: PathBuf::from(&state.monitor_socket),
        qmp_socket: PathBuf::from(&state.qmp_socket),
        pid_file: PathBuf::from(&state.pid_file),
        serial_log: PathBuf::from(&state.serial_log),
        qemu_log: PathBuf::from(&state.qemu_log),
        ports: network
            .ports
            .iter()
            .map(|(name, port)| {
                (
                    name.clone(),
                    PortForward {
                        guest: port.guest,
                        host: port.host,
                        protocol: port.protocol.into(),
                    },
                )
            })
            .collect(),
        daemonize: true,
    }
}

pub(super) fn args(spec: &CommandSpec) -> Vec<String> {
    let mut args = vec![
        "-name".to_owned(),
        spec.name.clone(),
        "-machine".to_owned(),
        "type=q35,accel=kvm".to_owned(),
        "-cpu".to_owned(),
        "host".to_owned(),
        "-smp".to_owned(),
        spec.cpus.to_string(),
        "-m".to_owned(),
        spec.memory.clone(),
        "-display".to_owned(),
        "none".to_owned(),
        "-pidfile".to_owned(),
        path_text(&spec.pid_file),
        "-monitor".to_owned(),
        format!("unix:{},server=on,wait=off", path_text(&spec.monitor_socket)),
        "-qmp".to_owned(),
        format!("unix:{},server=on,wait=off", path_text(&spec.qmp_socket)),
        "-serial".to_owned(),
        format!("file:{}", path_text(&spec.serial_log)),
        "-D".to_owned(),
        path_text(&spec.qemu_log),
        "-drive".to_owned(),
        format!("if=virtio,format=qcow2,file={}", path_text(&spec.disk)),
        "-drive".to_owned(),
        format!("if=virtio,format=raw,readonly=on,file={}", path_text(&spec.seed_media)),
        "-netdev".to_owned(),
        netdev_arg(&spec.ports),
        "-device".to_owned(),
        "virtio-net-pci,netdev=net0".to_owned(),
    ];

    if spec.daemonize {
        args.push("-daemonize".to_owned());
    }

    args
}

fn netdev_arg(ports: &BTreeMap<String, PortForward>) -> String {
    let mut arg = "user,id=net0".to_owned();
    for port in ports.values() {
        arg.push_str(",hostfwd=");
        arg.push_str(port.protocol.as_qemu_name());
        arg.push_str(":127.0.0.1:");
        arg.push_str(&port.host.to_string());
        arg.push_str("-:");
        arg.push_str(&port.guest.to_string());
    }
    arg
}

impl PortProtocol {
    const fn as_qemu_name(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

impl From<PortProtocolState> for PortProtocol {
    fn from(protocol: PortProtocolState) -> Self {
        match protocol {
            PortProtocolState::Tcp => Self::Tcp,
            PortProtocolState::Udp => Self::Udp,
        }
    }
}

fn path_text(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::{CommandSpec, PortForward, PortProtocol, args};

    #[test]
    fn builds_qemu_system_args() {
        let spec = CommandSpec {
            name: "agentdp-altinn-studio-pr-0".to_owned(),
            cpus: 4,
            memory: "16G".to_owned(),
            disk: PathBuf::from("/instances/altinn-studio/pr-0/disk.qcow2"),
            seed_media: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/seed.img"),
            monitor_socket: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/monitor.sock"),
            qmp_socket: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/qmp.sock"),
            pid_file: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/qemu.pid"),
            serial_log: PathBuf::from("/instances/altinn-studio/pr-0/logs/serial.log"),
            qemu_log: PathBuf::from("/instances/altinn-studio/pr-0/logs/qemu.log"),
            ports: BTreeMap::from([
                (
                    "code-server".to_owned(),
                    PortForward {
                        guest: 4090,
                        host: 14090,
                        protocol: PortProtocol::Tcp,
                    },
                ),
                (
                    "ssh".to_owned(),
                    PortForward {
                        guest: 22,
                        host: 2222,
                        protocol: PortProtocol::Tcp,
                    },
                ),
            ]),
            daemonize: true,
        };

        assert_eq!(
            args(&spec),
            [
                "-name",
                "agentdp-altinn-studio-pr-0",
                "-machine",
                "type=q35,accel=kvm",
                "-cpu",
                "host",
                "-smp",
                "4",
                "-m",
                "16G",
                "-display",
                "none",
                "-pidfile",
                "/instances/altinn-studio/pr-0/generated/qemu/qemu.pid",
                "-monitor",
                "unix:/instances/altinn-studio/pr-0/generated/qemu/monitor.sock,server=on,wait=off",
                "-qmp",
                "unix:/instances/altinn-studio/pr-0/generated/qemu/qmp.sock,server=on,wait=off",
                "-serial",
                "file:/instances/altinn-studio/pr-0/logs/serial.log",
                "-D",
                "/instances/altinn-studio/pr-0/logs/qemu.log",
                "-drive",
                "if=virtio,format=qcow2,file=/instances/altinn-studio/pr-0/disk.qcow2",
                "-drive",
                "if=virtio,format=raw,readonly=on,file=/instances/altinn-studio/pr-0/generated/qemu/seed.img",
                "-netdev",
                "user,id=net0,hostfwd=tcp:127.0.0.1:14090-:4090,hostfwd=tcp:127.0.0.1:2222-:22",
                "-device",
                "virtio-net-pci,netdev=net0",
                "-daemonize",
            ]
        );
    }
}
