//! Guest-relative ingress plumbing.
//!
//! In this crate, ingress means traffic entering the guest VM. Host-published
//! ports are the current ingress mechanism.

mod host_ports;
mod tcp;
mod udp;

pub(crate) use host_ports::{HostPortEvent, HostPorts};
pub(crate) use tcp::TcpConnections;
pub(crate) use udp::UdpPeers;
