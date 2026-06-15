use std::net::Ipv4Addr;

use crate::buffers::WriteQueue;
use crate::buffers::{BufferPool, ByteBuf};
use crate::network::{HostConnectionId, IngressTcpWrite};
use agentdp_ds::fixed_table::FixedTable;
use smoltcp::iface::{SocketHandle, SocketSet};
use smoltcp::socket::tcp;

#[derive(Debug)]
pub(crate) struct TcpConnections {
    by_connection: FixedTable<HostConnectionId, TcpConnection>,
    socket_buffer_bytes: usize,
    next_ephemeral_port: u16,
    closed_scratch: Vec<(HostConnectionId, SocketHandle)>,
}

#[derive(Debug)]
struct TcpConnection {
    handle: SocketHandle,
    pending_writes: WriteQueue,
}

impl TcpConnections {
    pub(crate) fn new(max_connections: usize, socket_buffer_bytes: usize) -> Self {
        Self {
            by_connection: FixedTable::with_capacity(max_connections),
            socket_buffer_bytes,
            next_ephemeral_port: 40_000,
            closed_scratch: Vec::with_capacity(max_connections),
        }
    }

    pub(crate) fn connect(
        &mut self,
        connection: HostConnectionId,
        guest: Ipv4Addr,
        port: u16,
        connect_socket: impl FnOnce(tcp::Socket<'static>, Ipv4Addr, u16, u16) -> Option<SocketHandle>,
    ) -> bool {
        if self.by_connection.get(&connection).is_some() || self.by_connection.len() >= self.by_connection.capacity() {
            return false;
        }
        let rx = tcp::SocketBuffer::new(vec![0; self.socket_buffer_bytes]);
        let tx = tcp::SocketBuffer::new(vec![0; self.socket_buffer_bytes]);
        let socket = tcp_socket(rx, tx);
        let local_port = take_ephemeral_port(&mut self.next_ephemeral_port);
        let Some(handle) = connect_socket(socket, guest, port, local_port) else {
            return false;
        };
        self.by_connection
            .insert(
                connection,
                TcpConnection {
                    handle,
                    pending_writes: WriteQueue::new(),
                },
            )
            .is_ok()
    }

    pub(crate) fn write_peer_bytes(
        &mut self,
        connection: HostConnectionId,
        bytes: ByteBuf,
        sockets: &mut SocketSet<'static>,
    ) {
        let Some(entry) = self.by_connection.get_mut(&connection) else {
            return;
        };
        entry.pending_writes.push(bytes);
        entry
            .pending_writes
            .flush_to_guest_socket(sockets.get_mut::<tcp::Socket>(entry.handle));
    }

    pub(crate) fn close(&mut self, connection: HostConnectionId, sockets: &mut SocketSet<'static>) {
        let Some(entry) = self.by_connection.remove(&connection) else {
            return;
        };
        sockets.get_mut::<tcp::Socket>(entry.handle).abort();
        sockets.remove(entry.handle);
    }

    pub(crate) fn relay_guest_bytes(
        &mut self,
        sockets: &mut SocketSet<'static>,
        ingress_tcp_writes: &mut Vec<IngressTcpWrite>,
        ingress_tcp_closes: &mut Vec<HostConnectionId>,
        buffers: &BufferPool,
    ) {
        self.closed_scratch.clear();
        for (connection, entry) in self.by_connection.iter_mut() {
            let socket = sockets.get_mut::<tcp::Socket>(entry.handle);
            let _flushed = entry.pending_writes.flush_to_guest_socket(socket);
            while socket.can_recv() {
                let Ok(mut bytes) = buffers.try_tcp_byte() else {
                    break;
                };
                bytes.resize_zeroed(buffers.tcp_byte_capacity());
                match socket.recv_slice(bytes.as_mut_slice()) {
                    Ok(0) => break,
                    Ok(n) => {
                        bytes.truncate(n);
                        ingress_tcp_writes.push(IngressTcpWrite { connection, bytes });
                    }
                    Err(_error) => break,
                }
            }
            if !socket.is_open() {
                self.closed_scratch.push((connection, entry.handle));
            }
        }
        for &(connection, handle) in &self.closed_scratch {
            self.by_connection.remove(&connection);
            sockets.remove(handle);
            ingress_tcp_closes.push(connection);
        }
        self.closed_scratch.clear();
    }
}

pub(crate) const fn take_ephemeral_port(next_ephemeral_port: &mut u16) -> u16 {
    let port = *next_ephemeral_port;
    *next_ephemeral_port = if *next_ephemeral_port == 60_999 {
        40_000
    } else {
        next_ephemeral_port.saturating_add(1)
    };
    port
}

fn tcp_socket(rx: tcp::SocketBuffer<'static>, tx: tcp::SocketBuffer<'static>) -> tcp::Socket<'static> {
    let mut socket = tcp::Socket::new(rx, tx);
    socket.set_ack_delay(None);
    socket.set_nagle_enabled(false);
    socket
}

#[cfg(test)]
mod tests {
    use super::take_ephemeral_port;

    #[test]
    fn ephemeral_ports_wrap_to_start_after_last_port() {
        let mut port = 60_999;

        assert_eq!(take_ephemeral_port(&mut port), 60_999);
        assert_eq!(port, 40_000);
    }
}
