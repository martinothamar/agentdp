use std::collections::VecDeque;
use std::io::{self, Read};
use std::net::SocketAddr;

use crate::application::{self, TcpDnsTracker};
use crate::buffers::{BufferPool, ByteBuf};
use crate::buffers::{PumpStep, WriteQueue};
use crate::connectors::tcp::TcpConnector;
use crate::network::{ApplicationPolicy, TcpProxyId};
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorInterest, ReactorTcpStream};
use crate::runtime::NetworkRuntime;

use super::{TcpProxyErrorContext, TcpProxyEvent, TcpProxyPoll};

pub(super) struct PlainTcpProxy<R: ReactorBackend> {
    proxy: TcpProxyId,
    requested_dst: SocketAddr,
    upstream_dst: SocketAddr,
    authority: Option<String>,
    route_name: &'static str,
    pub(super) pending: WriteQueue,
    pending_polls: VecDeque<PlainProxyPoll>,
    pub(super) guest_write_finished: bool,
    close_requested: bool,
    read_pending: bool,
    pub(super) state: PlainTcpProxyState<R>,
}

pub(super) enum PlainTcpProxyState<R: ReactorBackend> {
    Connecting {
        route: Option<PlainRoute>,
        stream: R::TcpStream,
        connect_ready: bool,
    },
    Open {
        stream: R::TcpStream,
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

enum PlainProxyPoll {
    Bytes(ByteBuf),
    Event(TcpProxyEvent),
}

impl PlainProxyPoll {
    fn into_tcp_poll(self) -> TcpProxyPoll {
        match self {
            Self::Bytes(bytes) => TcpProxyPoll::Bytes(bytes),
            Self::Event(event) => TcpProxyPoll::Event(event),
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
        let mut stream = runtime.tcp_connector().connect_tcp_stream(dst)?;
        runtime.reactor_mut().register_tcp_stream(
            &mut stream,
            ReactorItemId::TcpProxy { proxy },
            ReactorInterest::ReadWrite,
        )?;
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
            pending_polls: VecDeque::from([PlainProxyPoll::Event(
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

    pub(super) fn write(&mut self, bytes: ByteBuf) {
        let PlainTcpProxyState::Open { route, dns_tracker, .. } = &mut self.state else {
            self.pending.push(bytes);
            return;
        };
        match Self::process_guest_bytes(route, bytes, dns_tracker.as_mut()) {
            Ok(bytes) => self.pending.push(bytes),
            Err(message) => self
                .pending_polls
                .push_back(PlainProxyPoll::Event(TcpProxyEvent::error(self.proxy, message))),
        }
    }

    pub(super) const fn finish_guest_write(&mut self) {
        self.guest_write_finished = true;
    }

    pub(super) const fn close(&mut self) {
        self.close_requested = true;
    }

    pub(super) const fn mark_connect_ready(&mut self) {
        if let PlainTcpProxyState::Connecting { connect_ready, .. } = &mut self.state {
            *connect_ready = true;
        }
    }

    pub(super) fn deregister(&mut self, reactor: &mut R) {
        match &mut self.state {
            PlainTcpProxyState::Connecting { stream, .. } | PlainTcpProxyState::Open { stream, .. } => {
                let _deregistered =
                    reactor.deregister_tcp_stream(stream, ReactorItemId::TcpProxy { proxy: self.proxy });
            }
            PlainTcpProxyState::Failed { .. } => {}
        }
    }

    pub(super) fn drive(&mut self, buffers: &BufferPool, reactor: &mut R) -> TcpProxyPoll {
        if let Some(poll) = self.pending_polls.pop_front() {
            return poll.into_tcp_poll();
        }
        match &mut self.state {
            PlainTcpProxyState::Connecting {
                stream, connect_ready, ..
            } => {
                if self.close_requested {
                    return TcpProxyPoll::Event(TcpProxyEvent::closed(self.proxy));
                }
                if !*connect_ready {
                    return TcpProxyPoll::Blocked;
                }
                if let Err(error) = stream.take_error().and_then(|error| error.map_or_else(|| Ok(()), Err)) {
                    return TcpProxyPoll::Event(TcpProxyEvent::error(self.proxy, error.to_string()));
                }
                if let Err(event) = self.open_plain(buffers, reactor) {
                    return TcpProxyPoll::Event(event);
                }
                self.drive(buffers, reactor)
            }
            PlainTcpProxyState::Open { .. } => self.drive_open(buffers, reactor),
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
        reactor
            .reregister_tcp_stream(
                &mut stream,
                ReactorItemId::TcpProxy { proxy: self.proxy },
                ReactorInterest::Readable,
            )
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

    fn drive_open(&mut self, buffers: &BufferPool, reactor: &R) -> TcpProxyPoll {
        let wrote = match self.try_write_pending() {
            Ok(wrote) => {
                self.update_interest(reactor);
                wrote
            }
            Err(event) => return TcpProxyPoll::Event(event),
        };
        if let Some(event) = self.finish_upstream_write_if_needed() {
            return TcpProxyPoll::Event(event);
        }
        if self.close_requested && self.pending.is_empty() {
            return TcpProxyPoll::Event(TcpProxyEvent::closed(self.proxy));
        }
        if let Some(poll) = self.try_read(buffers) {
            return poll;
        }
        if wrote {
            return TcpProxyPoll::Progress;
        }
        TcpProxyPoll::Blocked
    }

    fn try_write_pending(&mut self) -> Result<bool, TcpProxyEvent> {
        let PlainTcpProxyState::Open { stream, .. } = &mut self.state else {
            return Ok(false);
        };
        self.pending
            .flush_to_std(stream)
            .map(PumpStep::made_progress)
            .map_err(|error| TcpProxyEvent::error(self.proxy, error.to_string()))
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
            .shutdown_write()
            .err()
            .filter(|error| error.kind() != io::ErrorKind::NotConnected)
            .map(|error| TcpProxyEvent::error(self.proxy, error.to_string()))
    }

    fn try_read(&mut self, buffers: &BufferPool) -> Option<TcpProxyPoll> {
        let PlainTcpProxyState::Open {
            stream, dns_tracker, ..
        } = &mut self.state
        else {
            return None;
        };
        let Ok(mut bytes) = buffers.try_tcp_byte() else {
            return None;
        };
        bytes.resize_zeroed(buffers.tcp_byte_capacity());
        match stream.read(bytes.as_mut_slice()) {
            Ok(0) => Some(TcpProxyPoll::Event(TcpProxyEvent::closed(self.proxy))),
            Ok(len) => {
                self.read_pending = true;
                bytes.truncate(len);
                if let Some(tracker) = dns_tracker
                    && let Some(resolution) = tracker.response(bytes.as_slice())
                {
                    self.pending_polls.push_back(PlainProxyPoll::Bytes(bytes));
                    return Some(TcpProxyPoll::Event(TcpProxyEvent::DnsResolved {
                        host: resolution.host,
                        addresses: resolution.addresses,
                        ttl: resolution.ttl,
                    }));
                }
                Some(TcpProxyPoll::Bytes(bytes))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.read_pending = false;
                None
            }
            Err(error) => Some(TcpProxyPoll::Event(TcpProxyEvent::error(self.proxy, error.to_string()))),
        }
    }

    fn update_interest(&mut self, reactor: &R) {
        let PlainTcpProxyState::Open { stream, .. } = &mut self.state else {
            return;
        };
        let interest = if self.pending.is_empty() {
            ReactorInterest::Readable
        } else {
            ReactorInterest::ReadWrite
        };
        let _reregistered =
            reactor.reregister_tcp_stream(stream, ReactorItemId::TcpProxy { proxy: self.proxy }, interest);
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
