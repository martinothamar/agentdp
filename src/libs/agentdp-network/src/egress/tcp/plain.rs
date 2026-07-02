use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;

use crate::application::{self, TcpDnsTracker};
use crate::buffers::WriteQueue;
use crate::buffers::{BufferPool, ByteBuf};
use crate::connectors::tcp::TcpConnector;
use crate::drive::{DriveStreamRead, DriveTurn};
use crate::network::{ApplicationPolicy, TcpProxyId};
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorInterest, ReactorTcpStream, RegisteredTcpStream, RegisteringTcpStream};
use crate::readiness::IoSlotState;
use crate::runtime::NetworkRuntime;

use super::{TcpProxyErrorContext, TcpProxyEvent, TcpProxyPermit, TcpProxyPoll};

pub(super) struct PlainTcpProxy<R: ReactorBackend> {
    proxy: TcpProxyId,
    requested_dst: SocketAddr,
    upstream_dst: SocketAddr,
    authority: Option<String>,
    route_name: &'static str,
    pub(super) pending: WriteQueue,
    pending_polls: VecDeque<TcpProxyPoll>,
    pub(super) guest_write_finished: bool,
    close_requested: bool,
    read_pending: bool,
    pub(super) state: PlainTcpProxyState<R>,
}

pub(super) enum PlainTcpProxyState<R: ReactorBackend> {
    Connecting {
        route: Option<PlainRoute>,
        stream: RegisteredTcpStream<R>,
        connect_ready: bool,
    },
    Open {
        stream: RegisteredTcpStream<R>,
        route: PlainRoute,
        dns_tracker: Option<TcpDnsTracker>,
        upstream_write_finished: bool,
    },
    Failed {
        message: String,
    },
}

pub(super) enum PlainRoute {
    Policy(crate::network::TcpEgressPolicy),
    Dns,
    Bypass,
}

impl PlainRoute {
    const fn name(&self) -> &'static str {
        match self {
            Self::Policy(_) => "plain",
            Self::Dns => "dns",
            Self::Bypass => "tls-bypass",
        }
    }
}

impl<R> PlainTcpProxy<R>
where
    R: ReactorBackend,
{
    pub(super) fn connecting(
        proxy: TcpProxyId,
        requested_dst: SocketAddr,
        dst: SocketAddr,
        authority: Option<String>,
        route: PlainRoute,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> io::Result<Self> {
        let route_name = route.name();
        let stream = runtime.tcp_connector().connect_tcp_stream(dst)?;
        let stream = RegisteringTcpStream::new(
            runtime.reactor_mut(),
            stream,
            ReactorItemId::TcpProxy { proxy },
            ReactorInterest::ReadWrite,
        )?
        .commit();
        Ok(Self {
            proxy,
            requested_dst,
            upstream_dst: dst,
            authority,
            route_name,
            pending: WriteQueue::new(),
            pending_polls: VecDeque::new(),
            guest_write_finished: false,
            close_requested: false,
            read_pending: false,
            state: PlainTcpProxyState::Connecting {
                route: Some(route),
                stream,
                connect_ready: false,
            },
        })
    }

    pub(super) fn failed(proxy: TcpProxyId, context: TcpProxyErrorContext, message: String) -> Self {
        Self {
            proxy,
            requested_dst: parse_socket_addr(&context.destination),
            upstream_dst: parse_socket_addr(&context.upstream),
            authority: context.authority.clone(),
            route_name: context.route,
            pending: WriteQueue::new(),
            pending_polls: VecDeque::from([TcpProxyPoll::Event(
                TcpProxyEvent::error(proxy, message).with_context(context),
            )]),
            guest_write_finished: false,
            close_requested: false,
            read_pending: false,
            state: PlainTcpProxyState::Failed { message: String::new() },
        }
    }

    pub(super) fn error_context(&self) -> TcpProxyErrorContext {
        TcpProxyErrorContext::new(
            self.requested_dst,
            self.upstream_dst,
            self.authority.clone(),
            self.route_name,
            self.phase(),
        )
    }

    pub(super) fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    pub(super) fn write(&mut self, bytes: ByteBuf) {
        let PlainTcpProxyState::Open { route, dns_tracker, .. } = &mut self.state else {
            self.pending.push(bytes);
            return;
        };
        match Self::process_guest_bytes(route, bytes, dns_tracker.as_mut()) {
            Ok(bytes) => self.pending.push(bytes),
            Err(message) => self
                .pending_polls
                .push_back(TcpProxyPoll::Event(TcpProxyEvent::error(self.proxy, message))),
        }
    }

    pub(super) const fn finish_guest_write(&mut self) {
        self.guest_write_finished = true;
    }

    pub(super) const fn close(&mut self) {
        self.close_requested = true;
    }

    pub(super) fn deregister(&mut self, reactor: &mut R) {
        match &mut self.state {
            PlainTcpProxyState::Connecting { stream, .. } | PlainTcpProxyState::Open { stream, .. } => {
                stream.deregister(reactor);
            }
            PlainTcpProxyState::Failed { .. } => {}
        }
    }

    pub(super) fn has_local_work(&self, guest_can_send: bool) -> bool {
        if self.close_requested {
            return true;
        }
        if let Some(poll) = self.pending_polls.front() {
            return match poll {
                TcpProxyPoll::Bytes(_) => guest_can_send,
                TcpProxyPoll::Event(_) => true,
                TcpProxyPoll::Pending => false,
            };
        }
        match &self.state {
            PlainTcpProxyState::Connecting { .. } => false,
            PlainTcpProxyState::Open {
                stream,
                upstream_write_finished,
                ..
            } => {
                let upstream_write_pending =
                    !self.pending.is_empty() || (self.guest_write_finished && !*upstream_write_finished);
                let io = stream.io();
                upstream_write_pending && (!io.watches_write() || io.can_write())
            }
            PlainTcpProxyState::Failed { .. } => true,
        }
    }

    pub(super) const fn has_reactor_write_work(&self) -> bool {
        match &self.state {
            PlainTcpProxyState::Connecting { .. } => true,
            PlainTcpProxyState::Open {
                upstream_write_finished,
                ..
            } => !self.pending.is_empty() || (self.guest_write_finished && !*upstream_write_finished),
            PlainTcpProxyState::Failed { .. } => false,
        }
    }

    pub(super) const fn io(&self) -> IoSlotState {
        match &self.state {
            PlainTcpProxyState::Connecting { stream, .. } | PlainTcpProxyState::Open { stream, .. } => stream.io(),
            PlainTcpProxyState::Failed { .. } => IoSlotState::new(ReactorInterest::Disabled),
        }
    }

    pub(super) const fn mark_reactor_ready(&mut self, readable: bool, writable: bool) {
        match &mut self.state {
            PlainTcpProxyState::Connecting {
                stream, connect_ready, ..
            } => {
                stream.mark_reactor_ready(readable, writable);
                if readable || writable {
                    *connect_ready = true;
                }
            }
            PlainTcpProxyState::Open { stream, .. } => stream.mark_reactor_ready(readable, writable),
            PlainTcpProxyState::Failed { .. } => {}
        }
    }

    #[cfg(any(test, feature = "simulation"))]
    pub(super) fn debug_snapshot(&self) -> String {
        let state = match &self.state {
            PlainTcpProxyState::Connecting { connect_ready, .. } => {
                format!("Connecting {{ connect_ready: {connect_ready} }}")
            }
            PlainTcpProxyState::Open {
                upstream_write_finished,
                ..
            } => {
                format!("Open {{ upstream_write_finished: {upstream_write_finished} }}")
            }
            PlainTcpProxyState::Failed { .. } => "Failed".to_owned(),
        };
        format!(
            "PlainTcpProxy {{ state: {state}, pending_bytes: {}, pending_polls: {}, guest_write_finished: {}, close_requested: {}, read_pending: {} }}",
            self.pending.pending_bytes(),
            self.pending_polls.len(),
            self.guest_write_finished,
            self.close_requested,
            self.read_pending,
        )
    }

    pub(super) fn drive(
        &mut self,
        buffers: &BufferPool,
        reactor: &mut R,
        drive: &mut DriveTurn<'_>,
        permit: TcpProxyPermit,
    ) -> TcpProxyPoll {
        if let Some(poll) = self.pending_polls.pop_front() {
            if matches!(poll, TcpProxyPoll::Bytes(_)) && !permit.contains(TcpProxyPermit::READ_UPSTREAM) {
                self.pending_polls.push_front(poll);
                if permit.contains(TcpProxyPermit::WRITE_UPSTREAM)
                    && matches!(self.state, PlainTcpProxyState::Open { .. })
                {
                    return match self.drive_open(buffers, reactor, drive, permit) {
                        TcpProxyPoll::Pending => {
                            drive.wait_for_guest_send_capacity();
                            TcpProxyPoll::Pending
                        }
                        poll => poll,
                    };
                }
                drive.wait_for_guest_send_capacity();
                return TcpProxyPoll::Pending;
            }
            return poll;
        }
        match &mut self.state {
            PlainTcpProxyState::Connecting {
                stream, connect_ready, ..
            } => {
                if self.close_requested {
                    return TcpProxyPoll::Event(TcpProxyEvent::closed(self.proxy));
                }
                if !*connect_ready {
                    drive.wait_for_reactor_read_write();
                    return TcpProxyPoll::Pending;
                }
                if let Err(error) = stream
                    .source()
                    .take_error()
                    .and_then(|error| error.map_or_else(|| Ok(()), Err))
                {
                    return TcpProxyPoll::Event(TcpProxyEvent::error(self.proxy, error.to_string()));
                }
                if let Err(event) = self.open_plain(buffers, reactor) {
                    return TcpProxyPoll::Event(event);
                }
                self.drive(buffers, reactor, drive, permit)
            }
            PlainTcpProxyState::Open { .. } => self.drive_open(buffers, reactor, drive, permit),
            PlainTcpProxyState::Failed { message } => {
                TcpProxyPoll::Event(TcpProxyEvent::error(self.proxy, std::mem::take(message)))
            }
        }
    }

    fn open_plain(&mut self, buffers: &BufferPool, reactor: &R) -> Result<(), TcpProxyEvent> {
        let PlainTcpProxyState::Connecting { route, stream, .. } =
            std::mem::replace(&mut self.state, PlainTcpProxyState::Failed { message: String::new() })
        else {
            return Ok(());
        };
        let Some(route) = route else {
            return Err(TcpProxyEvent::error(self.proxy, "connecting TCP proxy route missing"));
        };
        let dns_tracker = matches!(route, PlainRoute::Dns).then(TcpDnsTracker::default);
        let mut stream = stream;
        stream
            .reregister(reactor, ReactorInterest::Readable)
            .map_err(|error| TcpProxyEvent::error(self.proxy, error.to_string()))?;
        self.state = PlainTcpProxyState::Open {
            stream,
            route,
            dns_tracker,
            upstream_write_finished: false,
        };
        let mut pending = std::mem::take(&mut self.pending);
        while let Some(write) = pending.pop_front() {
            match write.into_remaining(buffers) {
                Ok(bytes) => self.write(bytes),
                Err(error) => return Err(TcpProxyEvent::error(self.proxy, error.to_string())),
            }
        }
        Ok(())
    }

    fn drive_open(
        &mut self,
        buffers: &BufferPool,
        reactor: &R,
        drive: &mut DriveTurn<'_>,
        permit: TcpProxyPermit,
    ) -> TcpProxyPoll {
        if permit.contains(TcpProxyPermit::WRITE_UPSTREAM) {
            if let PlainTcpProxyState::Open { stream, .. } = &mut self.state {
                let (stream, io) = stream.source_and_io_mut();
                match drive.write_stream_queue_ready(io, &mut self.pending, stream) {
                    Ok(_write) => {}
                    Err(error) => return TcpProxyPoll::Event(TcpProxyEvent::error(self.proxy, error.to_string())),
                }
            }
        } else if !self.pending.is_empty() {
            drive.wait_for_reactor_write();
        }
        if permit.contains(TcpProxyPermit::WRITE_UPSTREAM)
            && let Err(event) = self.update_interest(reactor, permit)
        {
            return TcpProxyPoll::Event(event);
        }
        if let Some(event) = self.finish_upstream_write_if_needed() {
            return TcpProxyPoll::Event(event);
        }
        if self.close_requested && self.pending.is_empty() {
            return TcpProxyPoll::Event(TcpProxyEvent::closed(self.proxy));
        }
        if permit.contains(TcpProxyPermit::READ_UPSTREAM) {
            if let Some(poll) = self.try_read(buffers, drive) {
                return poll;
            }
        } else if self.read_pending {
            drive.wait_for_guest_send_capacity();
        }
        TcpProxyPoll::Pending
    }

    fn finish_upstream_write_if_needed(&mut self) -> Option<TcpProxyEvent> {
        if !self.guest_write_finished || !self.pending.is_empty() {
            return None;
        }
        let PlainTcpProxyState::Open {
            stream,
            upstream_write_finished,
            ..
        } = &mut self.state
        else {
            return None;
        };
        if *upstream_write_finished {
            return None;
        }
        *upstream_write_finished = true;
        stream
            .source()
            .shutdown_write()
            .err()
            .filter(|error| error.kind() != io::ErrorKind::NotConnected)
            .map(|error| TcpProxyEvent::error(self.proxy, error.to_string()))
    }

    fn try_read(&mut self, buffers: &BufferPool, drive: &mut DriveTurn<'_>) -> Option<TcpProxyPoll> {
        let PlainTcpProxyState::Open {
            stream, dns_tracker, ..
        } = &mut self.state
        else {
            return None;
        };
        let (stream, io) = stream.source_and_io_mut();
        match drive.read_stream_ready(io, buffers, stream) {
            Ok(DriveStreamRead::Closed) => Some(TcpProxyPoll::Event(TcpProxyEvent::closed(self.proxy))),
            Ok(DriveStreamRead::Bytes(bytes)) => {
                self.read_pending = true;
                if let Some(tracker) = dns_tracker
                    && let Some(resolution) = tracker.response(bytes.as_slice())
                {
                    self.pending_polls.push_back(TcpProxyPoll::Bytes(bytes));
                    return Some(TcpProxyPoll::Event(TcpProxyEvent::DnsResolved {
                        host: resolution.host,
                        addresses: resolution.addresses,
                        ttl: resolution.ttl,
                    }));
                }
                Some(TcpProxyPoll::Bytes(bytes))
            }
            Ok(DriveStreamRead::NotReady | DriveStreamRead::WouldBlock | DriveStreamRead::Blocked) => {
                self.read_pending = false;
                None
            }
            Err(error) => Some(TcpProxyPoll::Event(TcpProxyEvent::error(self.proxy, error.to_string()))),
        }
    }

    fn update_interest(&mut self, reactor: &R, permit: TcpProxyPermit) -> Result<(), TcpProxyEvent> {
        let PlainTcpProxyState::Open { stream, .. } = &mut self.state else {
            return Ok(());
        };
        let wants_read = permit.contains(TcpProxyPermit::READ_UPSTREAM);
        let has_pending_write = !self.pending.is_empty();
        let interest = match (wants_read, has_pending_write) {
            (true, true) => ReactorInterest::ReadWrite,
            (false, true) => ReactorInterest::Writable,
            (true, false) => ReactorInterest::Readable,
            (false, false) => ReactorInterest::Disabled,
        };
        stream
            .reregister(reactor, interest)
            .map_err(|error| TcpProxyEvent::error(self.proxy, error.to_string()))?;
        Ok(())
    }

    pub(super) fn process_guest_bytes(
        route: &PlainRoute,
        bytes: ByteBuf,
        dns_tracker: Option<&mut TcpDnsTracker>,
    ) -> Result<ByteBuf, String> {
        match route {
            PlainRoute::Policy(policy) => Self::process_plain_bytes(policy, bytes),
            PlainRoute::Dns => {
                if let Some(tracker) = dns_tracker {
                    tracker.record_queries(bytes.as_slice());
                }
                Ok(bytes)
            }
            PlainRoute::Bypass => Ok(bytes),
        }
    }

    fn process_plain_bytes(policy: &crate::network::TcpEgressPolicy, bytes: ByteBuf) -> Result<ByteBuf, String> {
        match &policy.decision.application {
            ApplicationPolicy::Raw => {
                if policy.reject_secret_placeholders
                    && let Err(error) = application::reject_unresolved_secret_placeholders(bytes.as_slice())
                {
                    return Err(error.to_string());
                }
                Ok(bytes)
            }
            ApplicationPolicy::Block { reason } => {
                let protocol = application::classify_plain_tcp(bytes.as_slice());
                Err(format!(
                    "egress blocked by application policy: {reason:?}; protocol: {protocol:?}"
                ))
            }
            ApplicationPolicy::Http1 { .. } => {
                let protocol = application::classify_plain_tcp(bytes.as_slice());
                Err(format!(
                    "plain HTTP/1.x substitution is not enabled; protocol: {protocol:?}"
                ))
            }
        }
    }

    const fn phase(&self) -> &'static str {
        match &self.state {
            PlainTcpProxyState::Connecting { .. } => "connect",
            PlainTcpProxyState::Open { .. } => {
                if self.read_pending {
                    "read"
                } else if !self.pending.is_empty() {
                    "write"
                } else {
                    "open"
                }
            }
            PlainTcpProxyState::Failed { .. } => "failed",
        }
    }
}

fn parse_socket_addr(value: &str) -> SocketAddr {
    value.parse().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))
}
