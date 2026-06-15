use std::io;
use std::net::SocketAddr;

use crate::reactor::{ReactorBackend, ReactorTcpStream};

pub(crate) trait TcpConnector<R: ReactorBackend>: Clone + 'static {
    fn connect_tcp_stream(&self, dst: SocketAddr) -> io::Result<R::TcpStream>;
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ProductionTcpConnector;

impl<R> TcpConnector<R> for ProductionTcpConnector
where
    R: ReactorBackend,
{
    fn connect_tcp_stream(&self, dst: SocketAddr) -> io::Result<R::TcpStream> {
        let stream = R::TcpStream::connect(dst)?;
        stream.set_nodelay(true)?;
        stream.prevent_child_inheritance()?;
        Ok(stream)
    }
}
