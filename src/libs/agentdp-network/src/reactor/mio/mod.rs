use std::io::{Read, Write};
use std::net::SocketAddr;

use ::mio::{Events, Interest, Poll, Registry, Token};
use std::cell::RefCell;
use std::sync::Arc;

use crate::guest::{GuestIoSource, TransportError};
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorInterest, ReactorRegistrationToken, ReactorWake};
use crate::reactor::{
    ReactorTcpListener as ReactorTcpListenerTrait, ReactorTcpStream as ReactorTcpStreamTrait,
    ReactorUdpSocket as ReactorUdpSocketTrait,
};
use agentdp_ds::fixed_table::FixedTable;

mod guest_source;
mod platform;

use guest_source::GuestSources;

const WAKE_TOKEN: Token = Token(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReactorReady {
    Wake,
    Io {
        item: ReactorItemId,
        readable: bool,
        writable: bool,
    },
}

pub(crate) struct MioReactor {
    poll: Poll,
    events: Events,
    waker: Arc<::mio::Waker>,
    next_backend_token: usize,
    by_backend_token: FixedTable<Token, ReactorItemId>,
    by_item: FixedTable<ReactorItemId, Token>,
    suspended_items: RefCell<Vec<ReactorItemId>>,
    guest_sources: GuestSources,
}

#[derive(Debug)]
pub(crate) struct MioTcpStream {
    inner: mio::net::TcpStream,
}

#[derive(Debug)]
pub(crate) struct MioTcpListener {
    inner: mio::net::TcpListener,
}

#[derive(Debug)]
pub(crate) struct MioUdpSocket {
    inner: mio::net::UdpSocket,
}

#[derive(Debug, Clone)]
pub struct MioReactorWake {
    waker: Arc<::mio::Waker>,
}

impl MioReactorWake {
    /// # Errors
    ///
    /// Returns an error when the underlying Mio waker cannot signal the reactor.
    pub fn wake(&self) -> std::io::Result<()> {
        self.waker.wake()
    }
}

impl ReactorWake for MioReactorWake {
    fn wake(&self) -> std::io::Result<()> {
        Self::wake(self)
    }
}

impl ReactorTcpStreamTrait for MioTcpStream {
    fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        mio::net::TcpStream::connect(addr).map(|inner| Self { inner })
    }

    fn set_nodelay(&self, nodelay: bool) -> std::io::Result<()> {
        self.inner.set_nodelay(nodelay)
    }

    fn take_error(&self) -> std::io::Result<Option<std::io::Error>> {
        self.inner.take_error()
    }

    fn shutdown_write(&self) -> std::io::Result<()> {
        self.inner.shutdown(std::net::Shutdown::Write)
    }

    fn prevent_child_inheritance(&self) -> std::io::Result<()> {
        agentdp_platform::net::prevent_child_socket_inheritance(self)
    }
}

impl ReactorTcpListenerTrait for MioTcpListener {
    type Stream = MioTcpStream;

    fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        mio::net::TcpListener::bind(addr).map(|inner| Self { inner })
    }

    fn accept(&self) -> std::io::Result<(MioTcpStream, SocketAddr)> {
        self.inner.accept().map(|(inner, addr)| (MioTcpStream { inner }, addr))
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn prevent_child_inheritance(&self) -> std::io::Result<()> {
        agentdp_platform::net::prevent_child_socket_inheritance(self)
    }
}

impl ReactorUdpSocketTrait for MioUdpSocket {
    fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        mio::net::UdpSocket::bind(addr).map(|inner| Self { inner })
    }

    fn from_std(socket: std::net::UdpSocket) -> Self {
        Self {
            inner: mio::net::UdpSocket::from_std(socket),
        }
    }

    fn send(&self, bytes: &[u8]) -> std::io::Result<usize> {
        self.inner.send(bytes)
    }

    fn recv(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.inner.recv(buffer)
    }

    fn send_to(&self, bytes: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        self.inner.send_to(bytes, target)
    }

    fn recv_from(&self, buffer: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buffer)
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn prevent_child_inheritance(&self) -> std::io::Result<()> {
        agentdp_platform::net::prevent_child_socket_inheritance(self)
    }
}

impl Read for MioTcpStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buffer)
    }
}

impl Write for MioTcpStream {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl ::mio::event::Source for MioTcpStream {
    fn register(&mut self, registry: &Registry, token: Token, interests: Interest) -> std::io::Result<()> {
        self.inner.register(registry, token, interests)
    }

    fn reregister(&mut self, registry: &Registry, token: Token, interests: Interest) -> std::io::Result<()> {
        self.inner.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> std::io::Result<()> {
        self.inner.deregister(registry)
    }
}

impl ::mio::event::Source for MioTcpListener {
    fn register(&mut self, registry: &Registry, token: Token, interests: Interest) -> std::io::Result<()> {
        self.inner.register(registry, token, interests)
    }

    fn reregister(&mut self, registry: &Registry, token: Token, interests: Interest) -> std::io::Result<()> {
        self.inner.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> std::io::Result<()> {
        self.inner.deregister(registry)
    }
}

impl ::mio::event::Source for MioUdpSocket {
    fn register(&mut self, registry: &Registry, token: Token, interests: Interest) -> std::io::Result<()> {
        self.inner.register(registry, token, interests)
    }

    fn reregister(&mut self, registry: &Registry, token: Token, interests: Interest) -> std::io::Result<()> {
        self.inner.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> std::io::Result<()> {
        self.inner.deregister(registry)
    }
}

impl MioReactor {
    pub(crate) fn new(event_capacity: usize) -> std::io::Result<Self> {
        let poll = Poll::new()?;
        let waker = Arc::new(::mio::Waker::new(poll.registry(), WAKE_TOKEN)?);
        Ok(Self {
            poll,
            events: Events::with_capacity(event_capacity),
            waker,
            next_backend_token: 1,
            by_backend_token: FixedTable::with_capacity(event_capacity),
            by_item: FixedTable::with_capacity(event_capacity),
            suspended_items: RefCell::new(Vec::with_capacity(event_capacity)),
            guest_sources: GuestSources::new(),
        })
    }

    pub(crate) fn wake_handle(&self) -> MioReactorWake {
        MioReactorWake {
            waker: self.waker.clone(),
        }
    }

    fn register_source<S: ::mio::event::Source + ?Sized>(
        &mut self,
        source: &mut S,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> std::io::Result<()> {
        self.ensure_unregistered(item)?;
        let token = self.next_token();
        let Some(interest) = interest.mio() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "initial reactor registration cannot be disabled",
            ));
        };
        self.poll.registry().register(source, token, interest)?;
        if let Err(error) = self.remember_registration(token, item) {
            let _ = self.poll.registry().deregister(source);
            return Err(error);
        }
        Ok(())
    }

    fn reregister_source<S: ::mio::event::Source + ?Sized>(
        &self,
        source: &mut S,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> std::io::Result<()> {
        let token = self.token_for_item(item)?;
        if let Some(interest) = interest.mio() {
            if self.unsuspend_item(item) {
                self.poll.registry().register(source, token, interest)
            } else {
                self.poll.registry().reregister(source, token, interest)
            }
        } else {
            if !self.is_suspended(item) {
                self.poll.registry().deregister(source)?;
                self.suspended_items.borrow_mut().push(item);
            }
            Ok(())
        }
    }

    fn deregister_source<S: ::mio::event::Source + ?Sized>(
        &mut self,
        source: &mut S,
        item: ReactorItemId,
    ) -> std::io::Result<()> {
        let token = self.by_item.remove(&item);
        if let Some(token) = token {
            self.by_backend_token.remove(&token);
        }
        if self.unsuspend_item(item) {
            Ok(())
        } else {
            self.poll.registry().deregister(source)
        }
    }

    fn register_guest_source_with_interest(
        &mut self,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
        interest: Interest,
    ) -> Result<(), TransportError> {
        self.ensure_unregistered(item)
            .map_err(|error| TransportError::operation("register guest frame session", error))?;
        let token = self.next_token();
        self.guest_sources
            .register(self.poll.registry(), source, item, token, interest)?;
        if let Err(error) = self.remember_registration(token, item) {
            let _ = self.guest_sources.deregister(self.poll.registry(), source, item);
            return Err(TransportError::operation("register guest frame session", error));
        }
        Ok(())
    }

    fn reregister_guest_source_with_interest(
        &self,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
        interest: Interest,
    ) -> Result<(), TransportError> {
        let token = self
            .token_for_item(item)
            .map_err(|error| TransportError::operation("reregister guest frame session", error))?;
        self.guest_sources
            .reregister(self.poll.registry(), source, item, token, interest)
    }
}

impl ReactorBackend for MioReactor {
    type Wake = MioReactorWake;
    type TcpListener = MioTcpListener;
    type TcpStream = MioTcpStream;
    type UdpSocket = MioUdpSocket;

    fn wake_handle(&self) -> Self::Wake {
        Self::wake_handle(self)
    }

    fn register_tcp_listener(
        &mut self,
        _registration: ReactorRegistrationToken,
        source: &mut MioTcpListener,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> std::io::Result<()> {
        self.register_source(source, item, interest)
    }

    fn register_tcp_stream(
        &mut self,
        _registration: ReactorRegistrationToken,
        source: &mut MioTcpStream,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> std::io::Result<()> {
        self.register_source(source, item, interest)
    }

    fn register_udp_socket(
        &mut self,
        _registration: ReactorRegistrationToken,
        source: &mut MioUdpSocket,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> std::io::Result<()> {
        self.register_source(source, item, interest)
    }

    fn reregister_tcp_stream(
        &self,
        _registration: ReactorRegistrationToken,
        source: &mut MioTcpStream,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> std::io::Result<()> {
        self.reregister_source(source, item, interest)
    }

    fn reregister_udp_socket(
        &self,
        _registration: ReactorRegistrationToken,
        source: &mut MioUdpSocket,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> std::io::Result<()> {
        self.reregister_source(source, item, interest)
    }

    fn deregister_tcp_listener(
        &mut self,
        _registration: ReactorRegistrationToken,
        source: &mut MioTcpListener,
        item: ReactorItemId,
    ) -> std::io::Result<()> {
        self.deregister_source(source, item)
    }

    fn deregister_tcp_stream(
        &mut self,
        _registration: ReactorRegistrationToken,
        source: &mut MioTcpStream,
        item: ReactorItemId,
    ) -> std::io::Result<()> {
        self.deregister_source(source, item)
    }

    fn deregister_udp_socket(
        &mut self,
        _registration: ReactorRegistrationToken,
        source: &mut MioUdpSocket,
        item: ReactorItemId,
    ) -> std::io::Result<()> {
        self.deregister_source(source, item)
    }

    fn register_guest_source(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
    ) -> Result<(), TransportError> {
        self.register_guest_source_with_interest(source, item, Interest::READABLE)
    }

    fn reregister_guest_source(
        &self,
        _registration: crate::reactor::ReactorRegistrationToken,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
        writable: bool,
    ) -> Result<(), TransportError> {
        let interest = if writable {
            Interest::READABLE | Interest::WRITABLE
        } else {
            Interest::READABLE
        };
        self.reregister_guest_source_with_interest(source, item, interest)
    }

    fn deregister_guest_source(
        &mut self,
        _registration: crate::reactor::ReactorRegistrationToken,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
    ) -> Result<(), TransportError> {
        let Some(token) = self.by_item.remove(&item) else {
            return Ok(());
        };
        self.by_backend_token.remove(&token);
        self.guest_sources.deregister(self.poll.registry(), source, item)
    }

    fn ready_into(
        &mut self,
        output: &mut Vec<ReactorReady>,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<()> {
        self.events.clear();
        output.clear();
        self.poll.poll(&mut self.events, timeout)?;
        output.extend(self.events.iter().filter_map(|event| {
            if event.token() == WAKE_TOKEN {
                Some(ReactorReady::Wake)
            } else {
                let item = self.by_backend_token.get(&event.token()).copied()?;
                if self.is_suspended(item) {
                    return None;
                }
                Some(ReactorReady::Io {
                    item,
                    readable: event.is_readable(),
                    writable: event.is_writable(),
                })
            }
        }));
        Ok(())
    }
}

impl MioReactor {
    const fn next_token(&mut self) -> Token {
        let token = Token(self.next_backend_token);
        self.next_backend_token = self.next_backend_token.saturating_add(1);
        token
    }

    fn token_for_item(&self, item: ReactorItemId) -> std::io::Result<Token> {
        self.by_item.get(&item).copied().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("reactor item {item:?} is not registered"),
            )
        })
    }

    fn is_suspended(&self, item: ReactorItemId) -> bool {
        self.suspended_items.borrow().contains(&item)
    }

    fn unsuspend_item(&self, item: ReactorItemId) -> bool {
        let mut suspended = self.suspended_items.borrow_mut();
        let Some(index) = suspended.iter().position(|suspended| *suspended == item) else {
            return false;
        };
        suspended.swap_remove(index);
        true
    }

    fn ensure_unregistered(&self, item: ReactorItemId) -> std::io::Result<()> {
        if self.by_item.get(&item).is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("reactor item {item:?} is already registered"),
            ));
        }
        Ok(())
    }

    fn remember_registration(&mut self, token: Token, item: ReactorItemId) -> std::io::Result<()> {
        self.by_backend_token
            .insert(token, item)
            .map_err(|_value| reactor_capacity_error())?;
        if self.by_item.insert(item, token).is_err() {
            self.by_backend_token.remove(&token);
            return Err(reactor_capacity_error());
        }
        Ok(())
    }
}

impl ReactorInterest {
    fn mio(self) -> Option<Interest> {
        match self {
            Self::Disabled => None,
            Self::Readable => Some(Interest::READABLE),
            Self::Writable => Some(Interest::WRITABLE),
            Self::ReadWrite => Some(Interest::READABLE | Interest::WRITABLE),
        }
    }
}

fn reactor_capacity_error() -> std::io::Error {
    std::io::Error::other("reactor registration capacity exhausted")
}
