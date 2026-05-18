use std::collections::{BTreeMap, BTreeSet};
use std::net::{TcpListener, UdpSocket};

use agentdp_core::manifest::{AgentManifest, NetworkProtocol};
use thiserror::Error;

use super::state::{PortMappingState, PortProtocolState};

#[derive(Debug, Error)]
pub enum Error {
    #[error("port override references undeclared guest port: {0}")]
    UnknownPort(String),
    #[error("host port {0} was assigned more than once")]
    DuplicateHostPort(u16),
    #[error("failed to allocate free {protocol} host port: {source}")]
    Allocate {
        protocol: &'static str,
        #[source]
        source: std::io::Error,
    },
}

pub fn assign(
    manifest: &AgentManifest,
    requested: &BTreeMap<String, u16>,
) -> Result<BTreeMap<String, PortMappingState>, Error> {
    assign_with_allocator(manifest, requested, allocate_host_port)
}

fn assign_with_allocator(
    manifest: &AgentManifest,
    requested: &BTreeMap<String, u16>,
    mut allocate: impl FnMut(NetworkProtocol, &mut BTreeSet<u16>) -> Result<u16, Error>,
) -> Result<BTreeMap<String, PortMappingState>, Error> {
    let mut used = BTreeSet::new();
    for (name, host) in requested {
        if !manifest.network.ports.contains_key(name) {
            return Err(Error::UnknownPort(name.clone()));
        }
        if !used.insert(*host) {
            return Err(Error::DuplicateHostPort(*host));
        }
    }

    let mut assigned = BTreeMap::new();
    for (name, guest) in &manifest.network.ports {
        let host = match requested.get(name) {
            Some(host) => *host,
            None => allocate(guest.protocol, &mut used)?,
        };
        assigned.insert(
            name.clone(),
            PortMappingState {
                guest: guest.guest,
                host,
                protocol: port_protocol_state(guest.protocol),
            },
        );
    }
    Ok(assigned)
}

fn allocate_host_port(protocol: NetworkProtocol, used: &mut BTreeSet<u16>) -> Result<u16, Error> {
    loop {
        let port = probe_free_port(protocol)?;
        if used.insert(port) {
            return Ok(port);
        }
    }
}

fn probe_free_port(protocol: NetworkProtocol) -> Result<u16, Error> {
    match protocol {
        NetworkProtocol::Tcp => {
            let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(|source| Error::Allocate {
                protocol: protocol_text(protocol),
                source,
            })?;
            Ok(listener
                .local_addr()
                .map_err(|source| Error::Allocate {
                    protocol: protocol_text(protocol),
                    source,
                })?
                .port())
        }
        NetworkProtocol::Udp => {
            let socket = UdpSocket::bind(("127.0.0.1", 0)).map_err(|source| Error::Allocate {
                protocol: protocol_text(protocol),
                source,
            })?;
            Ok(socket
                .local_addr()
                .map_err(|source| Error::Allocate {
                    protocol: protocol_text(protocol),
                    source,
                })?
                .port())
        }
    }
}

const fn protocol_text(protocol: NetworkProtocol) -> &'static str {
    match protocol {
        NetworkProtocol::Tcp => "tcp",
        NetworkProtocol::Udp => "udp",
    }
}

const fn port_protocol_state(protocol: NetworkProtocol) -> PortProtocolState {
    match protocol {
        NetworkProtocol::Tcp => PortProtocolState::Tcp,
        NetworkProtocol::Udp => PortProtocolState::Udp,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use agentdp_core::manifest::AgentManifest;
    use agentdp_test_support::manifest;

    use super::super::state::PortProtocolState;

    use super::{Error, assign, assign_with_allocator};

    #[test]
    fn assigns_requested_and_auto_ports() {
        let manifest = manifest();
        let requested = BTreeMap::from([("ssh".to_owned(), 2222)]);

        let ports = assign_with_allocator(&manifest, &requested, |_protocol, used| {
            let port = 14090;
            assert!(used.insert(port));
            Ok(port)
        })
        .unwrap();

        assert_eq!(ports["ssh"].host, 2222);
        assert_eq!(ports["ssh"].guest, 22);
        assert_eq!(ports["ssh"].protocol, PortProtocolState::Tcp);
        assert!(ports["code-server"].host > 0);
        assert_ne!(ports["code-server"].host, 2222);
    }

    #[test]
    fn rejects_unknown_requested_port() {
        let manifest = manifest();
        let requested = BTreeMap::from([("unknown".to_owned(), 2222)]);

        let error = assign(&manifest, &requested).unwrap_err();

        assert!(matches!(error, Error::UnknownPort(name) if name == "unknown"));
    }

    fn manifest() -> AgentManifest {
        serde_yaml::from_str(manifest::standard()).unwrap()
    }
}
