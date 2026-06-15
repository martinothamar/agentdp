use std::net::{IpAddr, SocketAddr};

use crate::buffers::ByteBuf;
use crate::network::IngressUdpSend;
use agentdp_ds::fixed_table::FixedTable;

#[derive(Debug)]
pub(crate) struct UdpPeers {
    peers: FixedTable<u16, SocketAddr>,
}

impl UdpPeers {
    pub(crate) fn new(max_host_peers: usize) -> Self {
        Self {
            peers: FixedTable::with_capacity(max_host_peers),
        }
    }

    pub(crate) fn open_host_port(&mut self, port: u16, peer: SocketAddr) -> bool {
        if self.peers.get(&port).is_none() && self.peers.len() >= self.peers.capacity() {
            return false;
        }
        let _replaced = self.peers.insert(port, peer);
        true
    }

    pub(crate) fn host_port_response(
        &self,
        dst: SocketAddr,
        gateway: IpAddr,
        payload: ByteBuf,
    ) -> Result<IngressUdpSend, ByteBuf> {
        if dst.ip() != gateway {
            return Err(payload);
        }
        let Some(peer) = self.peers.get(&dst.port()).copied() else {
            return Err(payload);
        };
        Ok(IngressUdpSend {
            port: dst.port(),
            peer,
            bytes: payload,
        })
    }
}
