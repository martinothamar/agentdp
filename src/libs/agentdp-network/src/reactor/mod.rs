mod mio;
mod registered;

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::time::Duration;

use crate::guest::{GuestIoSource, TransportError};
use crate::network::{HostConnectionId, TcpProxyId, UdpProxyKey};

pub use mio::MioReactorWake as ProductionWake;
pub(crate) use mio::{MioReactor, ReactorReady};
pub(crate) use registered::{
    RegisteredGuestSource, RegisteredTcpListener, RegisteredTcpStream, RegisteredUdpSocket, RegisteringTcpListener,
    RegisteringTcpStream, RegisteringUdpSocket,
};

pub(crate) fn default_backend(event_capacity: usize) -> io::Result<MioReactor> {
    mio::MioReactor::new(event_capacity)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReactorRegistrationToken {
    _private: (),
}

impl ReactorRegistrationToken {
    // Capability boundary, not a security boundary: only reactor-owned code can
    // construct this token, so adapters cannot bypass Registered* wrappers.
    pub(in crate::reactor) const fn new() -> Self {
        Self { _private: () }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ReactorItemId {
    Guest,
    IngressTcpListener { port: u16 },
    IngressTcpConnection { connection: HostConnectionId },
    IngressUdpSocket { port: u16 },
    TcpProxy { proxy: TcpProxyId },
    UdpProxy { proxy: UdpProxyKey },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReactorInterest {
    Disabled,
    Readable,
    Writable,
    ReadWrite,
}

pub(crate) trait ReactorWake: Clone + 'static {
    #[allow(
        dead_code,
        reason = "production exposes an inherent wake method; simulated backends keep a wake hook for future external drivers"
    )]
    fn wake(&self) -> io::Result<()>;
}

pub(crate) trait ReactorTcpStream: Read + Write + 'static {
    fn connect(addr: SocketAddr) -> io::Result<Self>
    where
        Self: Sized;

    fn set_nodelay(&self, nodelay: bool) -> io::Result<()>;
    fn take_error(&self) -> io::Result<Option<io::Error>>;
    fn shutdown_write(&self) -> io::Result<()>;

    fn prevent_child_inheritance(&self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) trait ReactorTcpListener: 'static {
    type Stream: ReactorTcpStream;

    fn bind(addr: SocketAddr) -> io::Result<Self>
    where
        Self: Sized;

    fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)>;
    #[allow(
        dead_code,
        reason = "test and simulated backends need to inspect ephemeral bind addresses"
    )]
    fn local_addr(&self) -> io::Result<SocketAddr>;

    fn prevent_child_inheritance(&self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) trait ReactorUdpSocket: 'static {
    fn bind(addr: SocketAddr) -> io::Result<Self>
    where
        Self: Sized;

    fn from_std(socket: std::net::UdpSocket) -> Self
    where
        Self: Sized;

    fn send(&self, bytes: &[u8]) -> io::Result<usize>;
    fn recv(&self, buffer: &mut [u8]) -> io::Result<usize>;
    fn send_to(&self, bytes: &[u8], target: SocketAddr) -> io::Result<usize>;
    fn recv_from(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)>;
    #[allow(
        dead_code,
        reason = "test and simulated backends need to inspect ephemeral bind addresses"
    )]
    fn local_addr(&self) -> io::Result<SocketAddr>;

    fn prevent_child_inheritance(&self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) trait ReactorBackend {
    type Wake: ReactorWake;
    type TcpListener: ReactorTcpListener<Stream = Self::TcpStream>;
    type TcpStream: ReactorTcpStream;
    type UdpSocket: ReactorUdpSocket;

    #[allow(
        dead_code,
        reason = "production callers obtain this through EventLoop::wake_handle outside this crate"
    )]
    fn wake_handle(&self) -> Self::Wake;

    fn register_tcp_listener(
        &mut self,
        registration: ReactorRegistrationToken,
        source: &mut Self::TcpListener,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<()>;

    fn register_tcp_stream(
        &mut self,
        registration: ReactorRegistrationToken,
        source: &mut Self::TcpStream,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<()>;

    fn register_udp_socket(
        &mut self,
        registration: ReactorRegistrationToken,
        source: &mut Self::UdpSocket,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<()>;

    fn reregister_tcp_stream(
        &self,
        registration: ReactorRegistrationToken,
        source: &mut Self::TcpStream,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<()>;

    fn reregister_udp_socket(
        &self,
        registration: ReactorRegistrationToken,
        source: &mut Self::UdpSocket,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<()>;

    fn deregister_tcp_listener(
        &mut self,
        registration: ReactorRegistrationToken,
        source: &mut Self::TcpListener,
        item: ReactorItemId,
    ) -> io::Result<()>;

    fn deregister_tcp_stream(
        &mut self,
        registration: ReactorRegistrationToken,
        source: &mut Self::TcpStream,
        item: ReactorItemId,
    ) -> io::Result<()>;

    fn deregister_udp_socket(
        &mut self,
        registration: ReactorRegistrationToken,
        source: &mut Self::UdpSocket,
        item: ReactorItemId,
    ) -> io::Result<()>;

    fn register_guest_source(
        &mut self,
        registration: ReactorRegistrationToken,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
    ) -> Result<(), TransportError>;

    fn reregister_guest_source(
        &self,
        registration: ReactorRegistrationToken,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
        writable: bool,
    ) -> Result<(), TransportError>;

    fn deregister_guest_source(
        &mut self,
        registration: ReactorRegistrationToken,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
    ) -> Result<(), TransportError>;

    fn ready_into(&mut self, output: &mut Vec<ReactorReady>, timeout: Option<Duration>) -> io::Result<()>;
}
