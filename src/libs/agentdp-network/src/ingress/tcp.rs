use std::net::Ipv4Addr;

use crate::buffers::WriteQueue;
use crate::buffers::{BufferPool, ByteBuf};
use crate::drive::{DriveRunnable, DriveSmoltcpTcpRecv, DriveTurn};
use crate::network::{HostConnectionId, IngressTcpOutput};
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
    guest_to_host_closed: bool,
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
                    guest_to_host_closed: false,
                },
            )
            .is_ok()
    }

    pub(crate) fn write_peer_bytes(
        &mut self,
        connection: HostConnectionId,
        bytes: ByteBuf,
        sockets: &mut SocketSet<'static>,
        drive: &mut DriveTurn<'_>,
    ) {
        let Some(entry) = self.by_connection.get_mut(&connection) else {
            return;
        };
        entry.pending_writes.push(bytes);
        drive.send_smoltcp_tcp_queue(&mut entry.pending_writes, sockets.get_mut::<tcp::Socket>(entry.handle));
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
        outputs: &mut Vec<IngressTcpOutput>,
        buffers: &BufferPool,
        drive: &mut DriveTurn<'_>,
    ) {
        self.closed_scratch.clear();
        for (connection, entry) in self.by_connection.iter_mut() {
            let socket = sockets.get_mut::<tcp::Socket>(entry.handle);
            drive.send_smoltcp_tcp_queue(&mut entry.pending_writes, socket);
            loop {
                if outputs.len() >= outputs.capacity() {
                    drive.wait_for_local_buffer_capacity();
                    break;
                }
                match drive.recv_smoltcp_tcp(buffers, socket, DriveRunnable::READ_GUEST) {
                    DriveSmoltcpTcpRecv::Bytes(bytes) => {
                        let _queued = drive.push_component_output_after_progress(
                            outputs,
                            IngressTcpOutput::Write { connection, bytes },
                        );
                    }
                    DriveSmoltcpTcpRecv::Empty | DriveSmoltcpTcpRecv::Blocked => break,
                }
            }
            if guest_send_half_closed(socket) && !entry.guest_to_host_closed {
                if outputs.len() >= outputs.capacity() {
                    drive.wait_for_local_buffer_capacity();
                    break;
                }
                entry.guest_to_host_closed = true;
                let _queued =
                    drive.push_component_output_after_progress(outputs, IngressTcpOutput::FinishWrite { connection });
            }
            if !socket.is_open() {
                self.closed_scratch.push((connection, entry.handle));
            }
        }
        for &(connection, handle) in &self.closed_scratch {
            if drive
                .push_component_output(outputs, IngressTcpOutput::Close { connection })
                .is_err()
            {
                break;
            }
            self.by_connection.remove(&connection);
            sockets.remove(handle);
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

fn guest_send_half_closed(socket: &tcp::Socket<'_>) -> bool {
    !socket.can_recv()
        && matches!(
            socket.state(),
            tcp::State::CloseWait
                | tcp::State::Closing
                | tcp::State::LastAck
                | tcp::State::TimeWait
                | tcp::State::Closed
        )
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
