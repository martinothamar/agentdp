use std::io;
use std::net::{Ipv4Addr, SocketAddr};

use crate::reactor::{ReactorBackend, ReactorUdpSocket};

pub(crate) trait UdpSocketFactory<R: ReactorBackend>: Clone + 'static {
    fn connect_udp_socket(&self, dst: SocketAddr) -> io::Result<R::UdpSocket>;
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ProductionUdpSocketFactory;

impl<R> UdpSocketFactory<R> for ProductionUdpSocketFactory
where
    R: ReactorBackend,
{
    fn connect_udp_socket(&self, dst: SocketAddr) -> io::Result<R::UdpSocket> {
        let socket = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        socket.connect(dst)?;
        socket.set_nonblocking(true)?;
        let socket = R::UdpSocket::from_std(socket);
        socket.prevent_child_inheritance()?;
        Ok(socket)
    }
}
