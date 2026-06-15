use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub name: String,
    pub accelerator: Accelerator,
    pub cpus: u16,
    pub memory: String,
    pub disk: PathBuf,
    pub seed_media: PathBuf,
    pub monitor_socket: PathBuf,
    pub qmp_socket: PathBuf,
    pub guest_control_socket: PathBuf,
    pub pid_file: PathBuf,
    pub serial_log: PathBuf,
    pub qemu_log: PathBuf,
    pub network: NetworkBackend,
    pub daemonize: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkBackend {
    User { ports: BTreeMap<String, PortForward> },
    Stream { socket: PathBuf, mac: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortForward {
    pub guest: u16,
    pub host: u16,
    pub protocol: PortProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accelerator {
    Kvm,
    Whpx,
}

#[must_use]
pub fn args(spec: &CommandSpec) -> Vec<String> {
    let mut args = vec![
        "-name".to_owned(),
        spec.name.clone(),
        "-machine".to_owned(),
        format!("type=q35,accel={}", spec.accelerator.as_qemu_name()),
        "-cpu".to_owned(),
        spec.accelerator.cpu_model().to_owned(),
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
        "-chardev".to_owned(),
        format!(
            "socket,id=agentdpctl,path={},server=on,wait=off",
            path_text(&spec.guest_control_socket)
        ),
        "-device".to_owned(),
        "virtio-serial-pci".to_owned(),
        "-device".to_owned(),
        "virtserialport,chardev=agentdpctl,name=agentdp.control".to_owned(),
        "-serial".to_owned(),
        format!("file:{}", path_text(&spec.serial_log)),
        "-D".to_owned(),
        path_text(&spec.qemu_log),
        "-drive".to_owned(),
        format!("if=virtio,format=qcow2,file={}", path_text(&spec.disk)),
        "-drive".to_owned(),
        format!("if=virtio,format=raw,readonly=on,file={}", path_text(&spec.seed_media)),
        "-netdev".to_owned(),
        netdev_arg(&spec.network),
        "-device".to_owned(),
        net_device_arg(&spec.network),
    ];

    if spec.daemonize {
        args.push("-daemonize".to_owned());
    }

    args
}

fn netdev_arg(network: &NetworkBackend) -> String {
    match network {
        NetworkBackend::User { ports } => user_netdev_arg(ports),
        NetworkBackend::Stream { socket, .. } => {
            format!(
                "stream,id=net0,server=on,addr.type=unix,addr.path={}",
                path_text(socket)
            )
        }
    }
}

fn net_device_arg(network: &NetworkBackend) -> String {
    match network {
        NetworkBackend::User { .. } => "virtio-net-pci,netdev=net0".to_owned(),
        NetworkBackend::Stream { mac, .. } => format!("virtio-net-pci,netdev=net0,mac={mac}"),
    }
}

fn user_netdev_arg(ports: &BTreeMap<String, PortForward>) -> String {
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

impl Accelerator {
    #[must_use]
    pub const fn local_default() -> Self {
        if cfg!(target_os = "windows") {
            Self::Whpx
        } else {
            Self::Kvm
        }
    }

    const fn as_qemu_name(self) -> &'static str {
        match self {
            Self::Kvm => "kvm",
            Self::Whpx => "whpx",
        }
    }

    const fn cpu_model(self) -> &'static str {
        match self {
            Self::Kvm => "host",
            Self::Whpx => "qemu64",
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

    use super::{Accelerator, CommandSpec, NetworkBackend, PortForward, PortProtocol, args};

    #[test]
    fn builds_qemu_system_args() {
        let spec = CommandSpec {
            name: "agentdp-altinn-studio-pr-0".to_owned(),
            accelerator: Accelerator::Kvm,
            cpus: 4,
            memory: "16G".to_owned(),
            disk: PathBuf::from("/instances/altinn-studio/pr-0/disk.qcow2"),
            seed_media: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/seed.img"),
            monitor_socket: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/monitor.sock"),
            qmp_socket: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/qmp.sock"),
            guest_control_socket: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/guest-control.sock"),
            pid_file: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/qemu.pid"),
            serial_log: PathBuf::from("/instances/altinn-studio/pr-0/logs/serial.log"),
            qemu_log: PathBuf::from("/instances/altinn-studio/pr-0/logs/qemu.log"),
            network: user_network(),
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
                "-chardev",
                "socket,id=agentdpctl,path=/instances/altinn-studio/pr-0/generated/qemu/guest-control.sock,server=on,wait=off",
                "-device",
                "virtio-serial-pci",
                "-device",
                "virtserialport,chardev=agentdpctl,name=agentdp.control",
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

    #[test]
    fn builds_windows_qemu_system_args() {
        let mut spec = qemu_spec();
        spec.accelerator = Accelerator::Whpx;
        spec.daemonize = false;

        let args = args(&spec);

        assert!(args.contains(&"type=q35,accel=whpx".to_owned()));
        assert!(args.contains(&"qemu64".to_owned()));
        assert!(!args.contains(&"-daemonize".to_owned()));
    }

    #[test]
    fn builds_stream_netdev_args() {
        let guest_mac = agentdp_core::mediated_network::DEFAULT_PROFILE.guest_mac.to_string();
        let mut spec = qemu_spec();
        spec.network = NetworkBackend::Stream {
            socket: PathBuf::from("/run/agentdp/instances/altinn-studio/pr-0/qemu/network.sock"),
            mac: guest_mac.clone(),
        };

        let args = args(&spec);
        let device_arg = format!("virtio-net-pci,netdev=net0,mac={guest_mac}");

        assert!(args.windows(2).any(|values| values == [
            "-netdev",
            "stream,id=net0,server=on,addr.type=unix,addr.path=/run/agentdp/instances/altinn-studio/pr-0/qemu/network.sock"
        ]));
        assert!(args.windows(2).any(|values| values == ["-device", device_arg.as_str()]));
    }

    fn qemu_spec() -> CommandSpec {
        CommandSpec {
            name: "agentdp-altinn-studio-pr-0".to_owned(),
            accelerator: Accelerator::Kvm,
            cpus: 4,
            memory: "16G".to_owned(),
            disk: PathBuf::from("/instances/altinn-studio/pr-0/disk.qcow2"),
            seed_media: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/seed.img"),
            monitor_socket: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/monitor.sock"),
            qmp_socket: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/qmp.sock"),
            guest_control_socket: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/guest-control.sock"),
            pid_file: PathBuf::from("/instances/altinn-studio/pr-0/generated/qemu/qemu.pid"),
            serial_log: PathBuf::from("/instances/altinn-studio/pr-0/logs/serial.log"),
            qemu_log: PathBuf::from("/instances/altinn-studio/pr-0/logs/qemu.log"),
            network: user_network(),
            daemonize: true,
        }
    }

    fn user_network() -> NetworkBackend {
        NetworkBackend::User {
            ports: BTreeMap::from([
                (
                    "code_server".to_owned(),
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
        }
    }
}
