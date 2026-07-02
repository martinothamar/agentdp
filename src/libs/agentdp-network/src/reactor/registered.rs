use std::fmt;
use std::io;

use crate::guest::{GuestIoSource, TransportError};
use crate::readiness::IoSlotState;

use super::{ReactorBackend, ReactorInterest, ReactorItemId, ReactorRegistrationToken};

pub(crate) struct RegisteredTcpListener<R: ReactorBackend> {
    source: R::TcpListener,
    item: ReactorItemId,
    io: IoSlotState,
}

pub(crate) struct RegisteredTcpStream<R: ReactorBackend> {
    source: R::TcpStream,
    item: ReactorItemId,
    io: IoSlotState,
}

pub(crate) struct RegisteredUdpSocket<R: ReactorBackend> {
    source: R::UdpSocket,
    item: ReactorItemId,
    io: IoSlotState,
}

#[derive(Debug)]
pub(crate) struct RegisteredGuestSource {
    item: ReactorItemId,
    io: IoSlotState,
}

impl<R> fmt::Debug for RegisteredTcpListener<R>
where
    R: ReactorBackend,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredTcpListener")
            .field("item", &self.item)
            .field("io", &self.io)
            .finish_non_exhaustive()
    }
}

impl<R> fmt::Debug for RegisteredTcpStream<R>
where
    R: ReactorBackend,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredTcpStream")
            .field("item", &self.item)
            .field("io", &self.io)
            .finish_non_exhaustive()
    }
}

impl<R> fmt::Debug for RegisteredUdpSocket<R>
where
    R: ReactorBackend,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredUdpSocket")
            .field("item", &self.item)
            .field("io", &self.io)
            .finish_non_exhaustive()
    }
}

#[must_use]
pub(crate) struct RegisteringTcpListener<'a, R: ReactorBackend> {
    reactor: &'a mut R,
    registered: Option<RegisteredTcpListener<R>>,
}

#[must_use]
pub(crate) struct RegisteringTcpStream<'a, R: ReactorBackend> {
    reactor: &'a mut R,
    registered: Option<RegisteredTcpStream<R>>,
}

#[must_use]
pub(crate) struct RegisteringUdpSocket<'a, R: ReactorBackend> {
    reactor: &'a mut R,
    registered: Option<RegisteredUdpSocket<R>>,
}

impl<R> RegisteredTcpListener<R>
where
    R: ReactorBackend,
{
    fn register(
        reactor: &mut R,
        mut source: R::TcpListener,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<Self> {
        reactor.register_tcp_listener(ReactorRegistrationToken::new(), &mut source, item, interest)?;
        Ok(Self {
            source,
            item,
            io: IoSlotState::registered(interest),
        })
    }

    pub(crate) const fn io(&self) -> IoSlotState {
        self.io
    }

    pub(crate) const fn source(&self) -> &R::TcpListener {
        &self.source
    }

    pub(crate) const fn mark_reactor_ready(&mut self, readable: bool, writable: bool) {
        self.io.mark_reactor_ready(readable, writable);
    }

    pub(crate) const fn clear_read_after_would_block(&mut self) {
        self.io.clear_read_after_would_block();
    }

    pub(crate) fn deregister(&mut self, reactor: &mut R) {
        let _deregistered =
            reactor.deregister_tcp_listener(ReactorRegistrationToken::new(), &mut self.source, self.item);
        self.io.clear_for_drop_or_reset();
    }
}

impl<R> RegisteredTcpStream<R>
where
    R: ReactorBackend,
{
    fn register(
        reactor: &mut R,
        mut source: R::TcpStream,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<Self> {
        reactor.register_tcp_stream(ReactorRegistrationToken::new(), &mut source, item, interest)?;
        Ok(Self {
            source,
            item,
            io: IoSlotState::registered_with_read_probe(interest),
        })
    }

    pub(crate) const fn io(&self) -> IoSlotState {
        self.io
    }

    pub(crate) const fn source(&self) -> &R::TcpStream {
        &self.source
    }

    #[cfg(test)]
    pub(crate) const fn source_mut(&mut self) -> &mut R::TcpStream {
        &mut self.source
    }

    pub(crate) const fn source_and_io_mut(&mut self) -> (&mut R::TcpStream, &mut IoSlotState) {
        (&mut self.source, &mut self.io)
    }

    pub(crate) const fn mark_reactor_ready(&mut self, readable: bool, writable: bool) {
        self.io.mark_reactor_ready(readable, writable);
    }

    pub(crate) fn reregister(&mut self, reactor: &R, interest: ReactorInterest) -> io::Result<()> {
        if interest == self.io.registered_interest() {
            return Ok(());
        }
        reactor.reregister_tcp_stream(ReactorRegistrationToken::new(), &mut self.source, self.item, interest)?;
        self.io.set_registered_interest_after_reregister(interest);
        Ok(())
    }

    pub(crate) fn deregister(&mut self, reactor: &mut R) {
        let _deregistered = reactor.deregister_tcp_stream(ReactorRegistrationToken::new(), &mut self.source, self.item);
        self.io.clear_for_drop_or_reset();
    }
}

impl<R> RegisteredUdpSocket<R>
where
    R: ReactorBackend,
{
    fn register(
        reactor: &mut R,
        mut source: R::UdpSocket,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<Self> {
        reactor.register_udp_socket(ReactorRegistrationToken::new(), &mut source, item, interest)?;
        Ok(Self {
            source,
            item,
            io: IoSlotState::registered(interest),
        })
    }

    pub(crate) const fn io(&self) -> IoSlotState {
        self.io
    }

    #[cfg(test)]
    pub(crate) const fn source(&self) -> &R::UdpSocket {
        &self.source
    }

    pub(crate) const fn source_and_io_mut(&mut self) -> (&R::UdpSocket, &mut IoSlotState) {
        (&self.source, &mut self.io)
    }

    pub(crate) const fn mark_reactor_ready(&mut self, readable: bool, writable: bool) {
        self.io.mark_reactor_ready(readable, writable);
    }

    #[cfg(test)]
    pub(crate) const fn clear_write_after_would_block(&mut self) {
        self.io.clear_write_after_would_block();
    }

    pub(crate) fn reregister(&mut self, reactor: &R, interest: ReactorInterest) -> io::Result<()> {
        if interest == self.io.registered_interest() {
            return Ok(());
        }
        reactor.reregister_udp_socket(ReactorRegistrationToken::new(), &mut self.source, self.item, interest)?;
        self.io.set_registered_interest_after_reregister(interest);
        Ok(())
    }

    pub(crate) fn deregister(&mut self, reactor: &mut R) {
        let _deregistered = reactor.deregister_udp_socket(ReactorRegistrationToken::new(), &mut self.source, self.item);
        self.io.clear_for_drop_or_reset();
    }
}

impl RegisteredGuestSource {
    pub(crate) fn register<R: ReactorBackend>(
        reactor: &mut R,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
    ) -> Result<Self, TransportError> {
        reactor.register_guest_source(ReactorRegistrationToken::new(), source, item)?;
        Ok(Self {
            item,
            io: IoSlotState::registered(ReactorInterest::Readable),
        })
    }

    pub(crate) const fn io(&self) -> IoSlotState {
        self.io
    }

    pub(crate) const fn mark_reactor_ready(&mut self, readable: bool, writable: bool) {
        self.io.mark_reactor_ready(readable, writable);
    }

    pub(crate) const fn clear_read_after_would_block(&mut self) {
        self.io.clear_read_after_would_block();
    }

    pub(crate) const fn clear_write_after_would_block(&mut self) {
        self.io.clear_write_after_would_block();
    }

    pub(crate) fn enable_write<R: ReactorBackend>(
        &mut self,
        reactor: &R,
        source: GuestIoSource<'_>,
    ) -> Result<(), TransportError> {
        if self.io.registered_interest() == ReactorInterest::ReadWrite {
            return Ok(());
        }
        reactor.reregister_guest_source(ReactorRegistrationToken::new(), source, self.item, true)?;
        self.io
            .set_registered_interest_after_reregister(ReactorInterest::ReadWrite);
        Ok(())
    }

    pub(crate) fn disable_write<R: ReactorBackend>(
        &mut self,
        reactor: &R,
        source: GuestIoSource<'_>,
    ) -> Result<(), TransportError> {
        if self.io.registered_interest() == ReactorInterest::Readable {
            return Ok(());
        }
        reactor.reregister_guest_source(ReactorRegistrationToken::new(), source, self.item, false)?;
        self.io
            .set_registered_interest_after_reregister(ReactorInterest::Readable);
        Ok(())
    }

    pub(crate) fn deregister<R: ReactorBackend>(&mut self, reactor: &mut R, source: GuestIoSource<'_>) {
        let _deregistered = reactor.deregister_guest_source(ReactorRegistrationToken::new(), source, self.item);
        self.io.clear_for_drop_or_reset();
    }
}

impl<'a, R> RegisteringTcpListener<'a, R>
where
    R: ReactorBackend,
{
    pub(crate) fn new(
        reactor: &'a mut R,
        source: R::TcpListener,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<Self> {
        Ok(Self {
            registered: Some(RegisteredTcpListener::register(reactor, source, item, interest)?),
            reactor,
        })
    }

    pub(crate) fn commit(mut self) -> RegisteredTcpListener<R> {
        take_registered(&mut self.registered)
    }
}

impl<'a, R> RegisteringTcpStream<'a, R>
where
    R: ReactorBackend,
{
    pub(crate) fn new(
        reactor: &'a mut R,
        source: R::TcpStream,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<Self> {
        Ok(Self {
            registered: Some(RegisteredTcpStream::register(reactor, source, item, interest)?),
            reactor,
        })
    }

    pub(crate) fn commit(mut self) -> RegisteredTcpStream<R> {
        take_registered(&mut self.registered)
    }
}

impl<'a, R> RegisteringUdpSocket<'a, R>
where
    R: ReactorBackend,
{
    pub(crate) fn new(
        reactor: &'a mut R,
        source: R::UdpSocket,
        item: ReactorItemId,
        interest: ReactorInterest,
    ) -> io::Result<Self> {
        Ok(Self {
            registered: Some(RegisteredUdpSocket::register(reactor, source, item, interest)?),
            reactor,
        })
    }

    pub(crate) fn source_and_io_mut(&mut self) -> (&R::UdpSocket, &mut IoSlotState) {
        registered_mut(&mut self.registered).source_and_io_mut()
    }

    pub(crate) fn reregister(&mut self, interest: ReactorInterest) -> io::Result<()> {
        registered_mut(&mut self.registered).reregister(self.reactor, interest)
    }

    pub(crate) fn commit(mut self) -> RegisteredUdpSocket<R> {
        take_registered(&mut self.registered)
    }
}

fn registered_mut<T>(registered: &mut Option<T>) -> &mut T {
    let Some(registered) = registered.as_mut() else {
        unreachable!("registered setup guard cannot be used after commit");
    };
    registered
}

fn take_registered<T>(registered: &mut Option<T>) -> T {
    let Some(registered) = registered.take() else {
        unreachable!("registered setup guard cannot commit twice");
    };
    registered
}

impl<R> Drop for RegisteringTcpListener<'_, R>
where
    R: ReactorBackend,
{
    fn drop(&mut self) {
        if let Some(mut registered) = self.registered.take() {
            registered.deregister(self.reactor);
        }
    }
}

impl<R> Drop for RegisteringTcpStream<'_, R>
where
    R: ReactorBackend,
{
    fn drop(&mut self) {
        if let Some(mut registered) = self.registered.take() {
            registered.deregister(self.reactor);
        }
    }
}

impl<R> Drop for RegisteringUdpSocket<'_, R>
where
    R: ReactorBackend,
{
    fn drop(&mut self) {
        if let Some(mut registered) = self.registered.take() {
            registered.deregister(self.reactor);
        }
    }
}
