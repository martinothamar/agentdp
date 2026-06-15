use std::io;
use std::net::SocketAddr;

use agentdp_crypto::{TlsCiphertextRead, TlsPlaintextRead, TlsPlaintextWrite, TlsServerConfig, TlsServerSession};

use super::tls_upstream::{TlsDrive, TlsUpstream};
use super::{TcpProxyErrorContext, TcpProxyEvent};
use crate::application::{Http1Filter, Http1ResponseEof};
use crate::buffers::WriteQueue;
use crate::buffers::{BufferPool, ByteBuf};
use crate::network::{ApplicationPolicy, BlockReason, TcpProxyId, TlsEgressPolicy};
use crate::policy::Authority;
use crate::reactor::ReactorBackend;
use crate::runtime::NetworkRuntime;
use crate::tls::sni;

pub(super) struct TlsTcpProxy<R: ReactorBackend> {
    pub(super) proxy: TcpProxyId,
    pub(super) requested_dst: SocketAddr,
    pub(super) upstream_dst: SocketAddr,
    pub(super) authority: Option<String>,
    pub(super) pending: WriteQueue,
    pub(super) guest_write_finished: bool,
    pub(super) close_requested: bool,
    pub(super) state: TlsTcpProxyState<R>,
}

pub(super) enum TlsTcpProxyState<R: ReactorBackend> {
    WaitingClientHelloBuffer {
        policy: TlsEgressPolicy,
    },
    ReadingClientHello {
        policy: TlsEgressPolicy,
        initial: ByteBuf,
    },
    GuestTlsHandshake {
        policy: TlsEgressPolicy,
        intercept: InterceptedTls,
        guest_tls: Box<TlsServerSession>,
        tls_out: ByteBuf,
    },
    ConnectingServer {
        guest_tls: Box<TlsServerSession>,
        filter: Http1Filter,
        tls_out: ByteBuf,
        plaintext_buf: ByteBuf,
        substitute_buf: ByteBuf,
        server_output_offset: usize,
        server_pending: WriteQueue,
        server_tls: TlsUpstream<R>,
    },
    OpenIntercept(TlsHttp1Proxy<R>),
    Closing,
}

pub(super) struct TlsHttp1Proxy<R: ReactorBackend> {
    pub(super) guest_tls: Box<TlsServerSession>,
    pub(super) server_tls: TlsUpstream<R>,
    pub(super) filter: Http1Filter,
    pub(super) tls_out: ByteBuf,
    pub(super) server_buf: ByteBuf,
    pub(super) server_buf_pending_offset: usize,
    pub(super) server_buf_pending_len: usize,
    pub(super) plaintext_buf: ByteBuf,
    pub(super) substitute_buf: ByteBuf,
    pub(super) server_output_offset: usize,
    pub(super) server_pending: WriteQueue,
    pub(super) server_read_pending: bool,
    pub(super) guest_tls_closed: bool,
    pub(super) guest_close_notify_queued: bool,
}

pub(super) enum TlsProxyPoll {
    Bytes(ByteBuf),
    Event(TcpProxyEvent),
    Progress,
    Blocked,
    Bypass {
        dst: SocketAddr,
        bytes: ByteBuf,
        pending: WriteQueue,
    },
}

enum FlushTls {
    Bytes(ByteBuf),
    Blocked,
    Empty,
}

const GUEST_TLS_FEED_CHUNK_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelayStep {
    Progress,
    Blocked,
    ProgressClosed,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QueueStep {
    Progress,
    Blocked,
}

impl TlsProxyPoll {
    const fn bytes(bytes: ByteBuf) -> Self {
        Self::Bytes(bytes)
    }

    const fn event(event: TcpProxyEvent) -> Self {
        Self::Event(event)
    }

    fn error(proxy: TcpProxyId, message: impl Into<String>) -> Self {
        Self::event(TcpProxyEvent::error(proxy, message))
    }

    const fn closed(proxy: TcpProxyId) -> Self {
        Self::event(TcpProxyEvent::closed(proxy))
    }
}

impl<R> TlsTcpProxy<R>
where
    R: ReactorBackend,
{
    pub(super) const fn new(proxy: TcpProxyId, requested_dst: SocketAddr, policy: TlsEgressPolicy) -> Self {
        let upstream_dst = policy.dst;
        Self {
            proxy,
            requested_dst,
            upstream_dst,
            authority: None,
            pending: WriteQueue::new(),
            guest_write_finished: false,
            close_requested: false,
            state: TlsTcpProxyState::WaitingClientHelloBuffer { policy },
        }
    }

    pub(super) fn error_context(&self) -> TcpProxyErrorContext {
        TcpProxyErrorContext::new(
            self.requested_dst,
            self.upstream_dst,
            self.authority.clone(),
            "tls",
            self.phase(),
        )
    }

    pub(super) fn write(&mut self, bytes: ByteBuf) {
        self.pending.push(bytes);
    }

    pub(super) const fn finish_guest_write(&mut self) {
        self.guest_write_finished = true;
    }

    pub(super) const fn close(&mut self) {
        self.close_requested = true;
    }

    #[cfg(test)]
    pub(super) fn has_queued_work(&self) -> bool {
        if !self.pending.is_empty() || self.close_requested {
            return true;
        }
        match &self.state {
            TlsTcpProxyState::WaitingClientHelloBuffer { .. } | TlsTcpProxyState::ReadingClientHello { .. } => {
                self.guest_write_finished
            }
            TlsTcpProxyState::ConnectingServer {
                guest_tls,
                server_tls,
                tls_out,
                substitute_buf,
                server_pending,
                ..
            } => {
                guest_tls.wants_write()
                    || server_tls.is_connect_ready()
                    || !tls_out.is_empty()
                    || !substitute_buf.is_empty()
                    || !server_pending.is_empty()
            }
            TlsTcpProxyState::OpenIntercept(proxy) => {
                proxy.has_queued_work(self.guest_write_finished, self.pending.is_empty())
            }
            TlsTcpProxyState::GuestTlsHandshake { .. } | TlsTcpProxyState::Closing => true,
        }
    }

    pub(super) const fn mark_connect_ready(&mut self) {
        match &mut self.state {
            TlsTcpProxyState::ConnectingServer { server_tls, .. } => server_tls.mark_connect_ready(),
            TlsTcpProxyState::OpenIntercept(proxy) => proxy.server_tls.mark_connect_ready(),
            TlsTcpProxyState::WaitingClientHelloBuffer { .. }
            | TlsTcpProxyState::ReadingClientHello { .. }
            | TlsTcpProxyState::GuestTlsHandshake { .. }
            | TlsTcpProxyState::Closing => {}
        }
    }

    pub(super) fn deregister(&mut self, reactor: &mut R) {
        match &mut self.state {
            TlsTcpProxyState::ConnectingServer { server_tls, .. } => server_tls.deregister(reactor),
            TlsTcpProxyState::OpenIntercept(proxy) => proxy.server_tls.deregister(reactor),
            TlsTcpProxyState::WaitingClientHelloBuffer { .. }
            | TlsTcpProxyState::ReadingClientHello { .. }
            | TlsTcpProxyState::GuestTlsHandshake { .. }
            | TlsTcpProxyState::Closing => {}
        }
    }

    #[cfg(any(test, feature = "simulation"))]
    pub(super) fn debug_snapshot(&self) -> String {
        match &self.state {
            TlsTcpProxyState::ConnectingServer { server_tls, .. } => {
                format!("TlsTcpProxy::ConnectingServer {{ upstream: {:?} }}", server_tls.stats)
            }
            TlsTcpProxyState::OpenIntercept(proxy) => {
                format!(
                    "TlsTcpProxy::OpenIntercept {{ upstream: {:?}, server_read_pending: {}, server_pending_empty: {}, tls_out_len: {}, guest_plaintext_pending_len: {}, substitute_buf_len: {}, server_output_offset: {} }}",
                    proxy.server_tls.stats,
                    proxy.server_read_pending,
                    proxy.server_pending.is_empty(),
                    proxy.tls_out.len(),
                    proxy
                        .server_buf_pending_len
                        .saturating_sub(proxy.server_buf_pending_offset),
                    proxy.substitute_buf.len(),
                    proxy.server_output_offset,
                )
            }
            TlsTcpProxyState::WaitingClientHelloBuffer { .. } => "TlsTcpProxy::WaitingClientHelloBuffer".to_owned(),
            TlsTcpProxyState::ReadingClientHello { .. } => "TlsTcpProxy::ReadingClientHello".to_owned(),
            TlsTcpProxyState::GuestTlsHandshake { .. } => "TlsTcpProxy::GuestTlsHandshake".to_owned(),
            TlsTcpProxyState::Closing => "TlsTcpProxy::Closing".to_owned(),
        }
    }

    pub(super) fn drive(
        &mut self,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> TlsProxyPoll {
        if self.guest_write_finished
            && self.pending.is_empty()
            && matches!(
                self.state,
                TlsTcpProxyState::WaitingClientHelloBuffer { .. }
                    | TlsTcpProxyState::ReadingClientHello { .. }
                    | TlsTcpProxyState::GuestTlsHandshake { .. }
            )
        {
            return TlsProxyPoll::closed(self.proxy);
        }
        match &mut self.state {
            TlsTcpProxyState::WaitingClientHelloBuffer { .. } => self.poll_client_hello_buffer(buffers),
            TlsTcpProxyState::ReadingClientHello { .. } => self.poll_client_hello(buffers),
            TlsTcpProxyState::GuestTlsHandshake { .. } => self.poll_guest_tls_handshake(buffers, runtime),
            TlsTcpProxyState::ConnectingServer { guest_tls, tls_out, .. }
                if guest_tls.wants_write() || !tls_out.is_empty() =>
            {
                match GuestTlsSide::new(guest_tls, tls_out).flush_ciphertext(buffers) {
                    Ok(FlushTls::Bytes(bytes)) => TlsProxyPoll::bytes(bytes),
                    Ok(FlushTls::Blocked | FlushTls::Empty) => TlsProxyPoll::Blocked,
                    Err(error) => TlsProxyPoll::error(self.proxy, error.to_string()),
                }
            }
            TlsTcpProxyState::ConnectingServer { server_tls, .. } => {
                match server_tls.drive_handshake(runtime.reactor_mut()) {
                    Ok(TlsDrive::Progress) => TlsProxyPoll::Progress,
                    Ok(TlsDrive::Ready) => {
                        if !self.open_intercept(buffers) {
                            return TlsProxyPoll::Blocked;
                        }
                        self.drive(buffers, runtime)
                    }
                    Ok(TlsDrive::Blocked) => TlsProxyPoll::Blocked,
                    Err(error) => TlsProxyPoll::error(self.proxy, error.to_string()),
                }
            }
            TlsTcpProxyState::OpenIntercept(proxy) => proxy.drive(
                self.proxy,
                self.close_requested,
                self.guest_write_finished,
                &mut self.pending,
                buffers,
                runtime.reactor_mut(),
            ),
            TlsTcpProxyState::Closing => TlsProxyPoll::closed(self.proxy),
        }
    }

    fn poll_client_hello_buffer(&mut self, buffers: &BufferPool) -> TlsProxyPoll {
        let TlsTcpProxyState::WaitingClientHelloBuffer { policy } =
            std::mem::replace(&mut self.state, TlsTcpProxyState::Closing)
        else {
            return TlsProxyPoll::Blocked;
        };
        let initial = match buffers.try_byte_with_capacity(buffers.limits().client_hello_limit.min(4096)) {
            Ok(initial) => initial,
            Err(_exhausted) => {
                self.state = TlsTcpProxyState::WaitingClientHelloBuffer { policy };
                return TlsProxyPoll::Blocked;
            }
        };
        self.state = TlsTcpProxyState::ReadingClientHello { policy, initial };
        self.poll_client_hello(buffers)
    }

    fn poll_client_hello(&mut self, buffers: &BufferPool) -> TlsProxyPoll {
        if self.close_requested {
            return TlsProxyPoll::closed(self.proxy);
        }
        let TlsTcpProxyState::ReadingClientHello { policy, initial } =
            std::mem::replace(&mut self.state, TlsTcpProxyState::Closing)
        else {
            return TlsProxyPoll::Blocked;
        };
        let mut initial = initial;
        while let Some(write) = self.pending.pop_front() {
            initial.extend_from_slice(&write.bytes.as_slice()[write.offset..]);
            if initial.as_slice().first().is_some_and(|byte| *byte != 0x16) {
                return TlsProxyPoll::error(self.proxy, "not a TLS ClientHello");
            }
            if initial.len() >= buffers.limits().client_hello_limit {
                return TlsProxyPoll::error(self.proxy, "TLS ClientHello too large or missing SNI");
            }
            let Some(host) = sni::extract_sni(initial.as_slice()) else {
                continue;
            };
            let host = normalize_host(&host);
            self.authority = Some(host.clone());
            return match tls_route(&policy, &host) {
                Ok(TlsRoute::Drop(reason)) => TlsProxyPoll::error(
                    self.proxy,
                    format!("egress blocked by TLS policy: {reason:?}; host: {host}"),
                ),
                Ok(TlsRoute::Bypass) => TlsProxyPoll::Bypass {
                    dst: policy.dst,
                    bytes: initial,
                    pending: std::mem::take(&mut self.pending),
                },
                Err(intercept) => {
                    let mut tls_out = match buffers.try_byte_with_capacity(buffers.limits().tls_relay_buffer_capacity) {
                        Ok(buffer) => buffer,
                        Err(_exhausted) => {
                            self.state = TlsTcpProxyState::ReadingClientHello { policy, initial };
                            return TlsProxyPoll::Blocked;
                        }
                    };
                    match TlsServerSession::accept(&intercept.server_config) {
                        Ok(mut guest_tls) => {
                            if let Err(error) = feed_guest_tls(&mut guest_tls, initial.as_slice()).map_err(|error| {
                                io::Error::new(error.kind(), format!("guest TLS ClientHello feed failed: {error}"))
                            }) {
                                return TlsProxyPoll::error(self.proxy, error.to_string());
                            }
                            match GuestTlsSide::new(&mut guest_tls, &mut tls_out).flush_ciphertext(buffers) {
                                Ok(FlushTls::Bytes(bytes)) => {
                                    self.state = TlsTcpProxyState::GuestTlsHandshake {
                                        policy,
                                        intercept,
                                        guest_tls: Box::new(guest_tls),
                                        tls_out,
                                    };
                                    TlsProxyPoll::bytes(bytes)
                                }
                                Ok(FlushTls::Blocked | FlushTls::Empty) => {
                                    self.state = TlsTcpProxyState::GuestTlsHandshake {
                                        policy,
                                        intercept,
                                        guest_tls: Box::new(guest_tls),
                                        tls_out,
                                    };
                                    TlsProxyPoll::Blocked
                                }
                                Err(error) => TlsProxyPoll::error(self.proxy, error.to_string()),
                            }
                        }
                        Err(error) => TlsProxyPoll::error(self.proxy, error.to_string()),
                    }
                }
            };
        }
        self.state = TlsTcpProxyState::ReadingClientHello { policy, initial };
        TlsProxyPoll::Blocked
    }

    fn poll_guest_tls_handshake(
        &mut self,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> TlsProxyPoll {
        if self.close_requested {
            return TlsProxyPoll::closed(self.proxy);
        }
        let TlsTcpProxyState::GuestTlsHandshake {
            policy,
            intercept,
            guest_tls,
            tls_out,
        } = std::mem::replace(&mut self.state, TlsTcpProxyState::Closing)
        else {
            return TlsProxyPoll::Blocked;
        };
        let mut guest_tls = guest_tls;
        let mut tls_out = tls_out;
        if guest_tls.wants_write() || !tls_out.is_empty() {
            match GuestTlsSide::new(&mut guest_tls, &mut tls_out).flush_ciphertext(buffers) {
                Ok(FlushTls::Bytes(bytes)) => {
                    self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
                    return TlsProxyPoll::bytes(bytes);
                }
                Ok(FlushTls::Blocked) => {
                    self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
                    return TlsProxyPoll::Blocked;
                }
                Ok(FlushTls::Empty) => {}
                Err(error) => return TlsProxyPoll::error(self.proxy, error.to_string()),
            }
        }
        while guest_tls.is_handshaking() {
            let Some(mut write) = self.pending.pop_front() else {
                self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
                return TlsProxyPoll::Blocked;
            };
            while write.offset < write.bytes.len() && guest_tls.is_handshaking() {
                let feed_end = (write.offset + GUEST_TLS_FEED_CHUNK_BYTES).min(write.bytes.len());
                if let Err(error) = feed_guest_tls(&mut guest_tls, &write.bytes.as_slice()[write.offset..feed_end])
                    .map_err(|error| io::Error::new(error.kind(), format!("guest TLS handshake feed failed: {error}")))
                {
                    return TlsProxyPoll::error(self.proxy, error.to_string());
                }
                write.offset = feed_end;
                match GuestTlsSide::new(&mut guest_tls, &mut tls_out).flush_ciphertext(buffers) {
                    Ok(FlushTls::Bytes(bytes)) => {
                        if write.offset < write.bytes.len() {
                            self.pending.push_front(write);
                        }
                        self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
                        return TlsProxyPoll::bytes(bytes);
                    }
                    Ok(FlushTls::Blocked) => {
                        if write.offset < write.bytes.len() {
                            self.pending.push_front(write);
                        }
                        self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
                        return TlsProxyPoll::Blocked;
                    }
                    Ok(FlushTls::Empty) => {}
                    Err(error) => {
                        return TlsProxyPoll::error(self.proxy, error.to_string());
                    }
                }
            }
            if write.offset < write.bytes.len() {
                self.pending.push_front(write);
            }
        }

        self.connect_intercept_after_guest_handshake(policy, intercept, guest_tls, tls_out, buffers, runtime)
    }

    fn connect_intercept_after_guest_handshake(
        &mut self,
        policy: TlsEgressPolicy,
        intercept: InterceptedTls,
        mut guest_tls: Box<TlsServerSession>,
        tls_out: ByteBuf,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> TlsProxyPoll {
        let authority = intercept.authority.as_str().to_owned();
        let mut plaintext_buf = match buffers.try_byte_with_capacity(buffers.limits().tls_relay_buffer_capacity) {
            Ok(buffer) => buffer,
            Err(_exhausted) => {
                self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
                return TlsProxyPoll::Blocked;
            }
        };
        plaintext_buf.resize_zeroed(buffers.limits().tls_relay_buffer_capacity);
        let mut substitute_buf = match buffers.try_byte_with_capacity(buffers.limits().tls_relay_buffer_capacity) {
            Ok(buffer) => buffer,
            Err(_exhausted) => {
                self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
                return TlsProxyPoll::Blocked;
            }
        };
        let mut server_pending = WriteQueue::new();
        let mut filter = Http1Filter::new(intercept.secrets, authority, buffers);
        let mut server_output_offset = 0;
        match TlsHttp1Proxy::<R>::forward_plaintext_to_server(
            &mut guest_tls,
            &mut filter,
            &mut plaintext_buf,
            &mut substitute_buf,
            &mut server_output_offset,
            &mut server_pending,
            buffers,
        ) {
            Ok(RelayStep::Progress | RelayStep::Blocked) => {}
            Ok(RelayStep::ProgressClosed | RelayStep::Closed) => self.guest_write_finished = true,
            Err(error) => return TlsProxyPoll::error(self.proxy, error.to_string()),
        }
        let server_tls = match TlsUpstream::connect(
            self.proxy,
            policy.dst,
            intercept.authority.as_str(),
            &policy.client_config,
            runtime,
        ) {
            Ok(server_tls) => server_tls,
            Err(error) => return TlsProxyPoll::error(self.proxy, error.to_string()),
        };
        self.state = TlsTcpProxyState::ConnectingServer {
            guest_tls,
            filter,
            tls_out,
            plaintext_buf,
            substitute_buf,
            server_output_offset,
            server_pending,
            server_tls,
        };
        self.drive(buffers, runtime)
    }

    fn restore_guest_tls_handshake(
        &mut self,
        policy: TlsEgressPolicy,
        intercept: InterceptedTls,
        guest_tls: Box<TlsServerSession>,
        tls_out: ByteBuf,
    ) {
        self.state = TlsTcpProxyState::GuestTlsHandshake {
            policy,
            intercept,
            guest_tls,
            tls_out,
        };
    }

    fn open_intercept(&mut self, buffers: &BufferPool) -> bool {
        let TlsTcpProxyState::ConnectingServer {
            guest_tls,
            filter,
            tls_out,
            plaintext_buf,
            substitute_buf,
            server_output_offset,
            server_pending,
            server_tls,
            ..
        } = std::mem::replace(&mut self.state, TlsTcpProxyState::Closing)
        else {
            return true;
        };
        let mut server_buf = match buffers.try_byte_with_capacity(buffers.limits().tls_relay_buffer_capacity) {
            Ok(buffer) => buffer,
            Err(_exhausted) => {
                self.state = TlsTcpProxyState::ConnectingServer {
                    guest_tls,
                    filter,
                    tls_out,
                    plaintext_buf,
                    substitute_buf,
                    server_output_offset,
                    server_pending,
                    server_tls,
                };
                return false;
            }
        };
        server_buf.resize_zeroed(buffers.limits().tls_relay_buffer_capacity);
        self.state = TlsTcpProxyState::OpenIntercept(TlsHttp1Proxy {
            guest_tls,
            server_tls,
            filter,
            tls_out,
            server_buf,
            server_buf_pending_offset: 0,
            server_buf_pending_len: 0,
            plaintext_buf,
            substitute_buf,
            server_output_offset,
            server_pending,
            server_read_pending: false,
            guest_tls_closed: false,
            guest_close_notify_queued: false,
        });
        true
    }

    const fn phase(&self) -> &'static str {
        match &self.state {
            TlsTcpProxyState::WaitingClientHelloBuffer { .. } | TlsTcpProxyState::ReadingClientHello { .. } => {
                "client-hello"
            }
            TlsTcpProxyState::GuestTlsHandshake { .. } => "guest-tls-handshake",
            TlsTcpProxyState::ConnectingServer { .. } => "upstream-tls-handshake",
            TlsTcpProxyState::OpenIntercept(_) => "relay",
            TlsTcpProxyState::Closing => "closing",
        }
    }
}

impl<R> TlsHttp1Proxy<R>
where
    R: ReactorBackend,
{
    #[cfg(test)]
    fn has_queued_work(&self, guest_write_finished: bool, pending_empty: bool) -> bool {
        self.guest_tls.wants_write()
            || !self.server_pending.is_empty()
            || !self.tls_out.is_empty()
            || self.server_buf_pending_offset < self.server_buf_pending_len
            || !self.substitute_buf.is_empty()
            || self.server_read_pending
            || self.guest_close_notify_queued
            || (guest_write_finished && pending_empty && !self.server_tls.write_finished())
    }

    fn drive(
        &mut self,
        proxy_id: TcpProxyId,
        close_requested: bool,
        guest_write_finished: bool,
        pending: &mut WriteQueue,
        buffers: &BufferPool,
        reactor: &R,
    ) -> TlsProxyPoll {
        if let Some(poll) = self.flush_pending_guest_tls_output(proxy_id, buffers) {
            return poll;
        }
        if let Some(poll) = self.write_pending_guest_plaintext(proxy_id, buffers) {
            return poll;
        }
        if close_requested {
            return TlsProxyPoll::closed(proxy_id);
        }
        match self.feed_pending_guest_tls(proxy_id, pending, buffers) {
            TlsProxyPoll::Bytes(bytes) => return TlsProxyPoll::bytes(bytes),
            TlsProxyPoll::Event(event) => return TlsProxyPoll::event(event),
            TlsProxyPoll::Progress => return TlsProxyPoll::Progress,
            TlsProxyPoll::Blocked => {}
            TlsProxyPoll::Bypass { .. } => unreachable!("TLS intercept cannot request bypass after it is open"),
        }
        if let Some(event) = self.write_pending_server_plaintext(proxy_id, reactor) {
            return TlsProxyPoll::event(event);
        }
        if let Some(event) = self.finish_server_write_if_needed(
            proxy_id,
            guest_write_finished || self.guest_tls_closed,
            pending.is_empty(),
            reactor,
        ) {
            return TlsProxyPoll::event(event);
        }
        match self.read_server_tls(proxy_id, buffers, reactor) {
            TlsProxyPoll::Bytes(bytes) => return TlsProxyPoll::bytes(bytes),
            TlsProxyPoll::Event(event) => return TlsProxyPoll::event(event),
            TlsProxyPoll::Progress => return TlsProxyPoll::Progress,
            TlsProxyPoll::Blocked => {}
            TlsProxyPoll::Bypass { .. } => unreachable!("TLS intercept cannot request bypass after it is open"),
        }
        TlsProxyPoll::Blocked
    }

    fn flush_pending_guest_tls_output(&mut self, proxy_id: TcpProxyId, buffers: &BufferPool) -> Option<TlsProxyPoll> {
        match GuestTlsSide::new(&mut self.guest_tls, &mut self.tls_out).flush_ciphertext(buffers) {
            Ok(FlushTls::Bytes(bytes)) => Some(TlsProxyPoll::bytes(bytes)),
            Ok(FlushTls::Blocked) => Some(TlsProxyPoll::Blocked),
            Ok(FlushTls::Empty) => None,
            Err(error) => Some(TlsProxyPoll::error(proxy_id, error.to_string())),
        }
    }

    fn feed_pending_guest_tls(
        &mut self,
        proxy_id: TcpProxyId,
        pending: &mut WriteQueue,
        buffers: &BufferPool,
    ) -> TlsProxyPoll {
        if !self.queue_existing_server_output(buffers) {
            return TlsProxyPoll::Blocked;
        }
        let Some(mut write) = pending.pop_front() else {
            return TlsProxyPoll::Blocked;
        };
        let feed_limit = (write.offset + buffers.limits().tls_relay_buffer_capacity).min(write.bytes.len());
        while write.offset < feed_limit {
            let feed_end = (write.offset + GUEST_TLS_FEED_CHUNK_BYTES).min(feed_limit);
            if let Err(error) = feed_guest_tls(&mut self.guest_tls, &write.bytes.as_slice()[write.offset..feed_end])
                .map_err(|error| io::Error::new(error.kind(), format!("guest TLS application feed failed: {error}")))
            {
                return TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string()));
            }
            write.offset = feed_end;
            match Self::forward_plaintext_to_server(
                &mut self.guest_tls,
                &mut self.filter,
                &mut self.plaintext_buf,
                &mut self.substitute_buf,
                &mut self.server_output_offset,
                &mut self.server_pending,
                buffers,
            ) {
                Ok(RelayStep::Progress) => {}
                Ok(RelayStep::Blocked) => {
                    if write.offset < write.bytes.len() {
                        pending.push_front(write);
                    }
                    return TlsProxyPoll::Progress;
                }
                Ok(RelayStep::ProgressClosed | RelayStep::Closed) => {
                    self.guest_tls_closed = true;
                    return TlsProxyPoll::Progress;
                }
                Err(error) => return TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string())),
            }
        }
        if write.offset < write.bytes.len() {
            pending.push_front(write);
        }
        match GuestTlsSide::new(&mut self.guest_tls, &mut self.tls_out).flush_ciphertext(buffers) {
            Ok(FlushTls::Bytes(bytes)) => return TlsProxyPoll::bytes(bytes),
            Ok(FlushTls::Blocked | FlushTls::Empty) => {}
            Err(error) => return TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string())),
        }
        TlsProxyPoll::Progress
    }

    fn write_pending_server_plaintext(&mut self, proxy_id: TcpProxyId, reactor: &R) -> Option<TcpProxyEvent> {
        self.server_tls
            .write_pending_plaintext(&mut self.server_pending, reactor)
            .err()
            .map(|error| TcpProxyEvent::error(proxy_id, error.to_string()))
    }

    fn finish_server_write_if_needed(
        &mut self,
        proxy_id: TcpProxyId,
        guest_write_finished: bool,
        pending_empty: bool,
        reactor: &R,
    ) -> Option<TcpProxyEvent> {
        if !guest_write_finished || !pending_empty {
            return None;
        }
        if !self.server_pending.is_empty() || self.server_tls.write_finished() {
            return None;
        }
        self.server_tls
            .finish_write(reactor)
            .err()
            .map(|error| TcpProxyEvent::error(proxy_id, error.to_string()))
    }

    fn read_server_tls(&mut self, proxy_id: TcpProxyId, buffers: &BufferPool, reactor: &R) -> TlsProxyPoll {
        if !self.queue_existing_server_output(buffers) {
            return TlsProxyPoll::Blocked;
        }
        if let Some(poll) = self.write_pending_guest_plaintext(proxy_id, buffers) {
            return poll;
        }
        self.server_buf
            .as_mut_vec()
            .resize(buffers.limits().tls_relay_buffer_capacity, 0);
        match self.server_tls.read_plaintext(self.server_buf.as_mut_slice(), reactor) {
            Ok(0) => self.handle_upstream_eof(proxy_id, buffers),
            Ok(len) => {
                self.server_read_pending = true;
                let bytes = &self.server_buf.as_slice()[..len];
                if let Err(error) = self.filter.observe_response(bytes) {
                    return TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string()));
                }
                match self
                    .filter
                    .observe_server_plaintext(bytes, self.substitute_buf.as_mut_vec())
                {
                    Ok(true) => {
                        let _queued = Self::queue_server_plaintext(
                            &mut self.server_pending,
                            &mut self.substitute_buf,
                            &mut self.server_output_offset,
                            buffers,
                        );
                    }
                    Ok(false) => {}
                    Err(error) => return TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string())),
                }
                self.server_buf_pending_offset = 0;
                self.server_buf_pending_len = len;
                self.write_pending_guest_plaintext(proxy_id, buffers)
                    .unwrap_or(TlsProxyPoll::Progress)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.server_read_pending = false;
                TlsProxyPoll::Blocked
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => self.handle_upstream_eof(proxy_id, buffers),
            Err(error) => TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string())),
        }
    }

    fn handle_upstream_eof(&mut self, proxy_id: TcpProxyId, buffers: &BufferPool) -> TlsProxyPoll {
        match self.filter.response_eof() {
            Http1ResponseEof::Complete | Http1ResponseEof::Tunnel => {
                self.close_guest_tls_after_upstream_eof(proxy_id, buffers)
            }
            Http1ResponseEof::Incomplete => TlsProxyPoll::event(TcpProxyEvent::error(
                proxy_id,
                "upstream closed before complete HTTP response",
            )),
        }
    }

    fn close_guest_tls_after_upstream_eof(&mut self, proxy_id: TcpProxyId, buffers: &BufferPool) -> TlsProxyPoll {
        match GuestTlsSide::new(&mut self.guest_tls, &mut self.tls_out).flush_ciphertext(buffers) {
            Ok(FlushTls::Bytes(bytes)) => return TlsProxyPoll::bytes(bytes),
            Ok(FlushTls::Blocked) => return TlsProxyPoll::Blocked,
            Ok(FlushTls::Empty) => {}
            Err(error) => return TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string())),
        }

        if !self.guest_close_notify_queued {
            self.guest_tls.queue_close_notify();
            self.guest_close_notify_queued = true;
        }

        match GuestTlsSide::new(&mut self.guest_tls, &mut self.tls_out).flush_ciphertext(buffers) {
            Ok(FlushTls::Bytes(bytes)) => TlsProxyPoll::bytes(bytes),
            Ok(FlushTls::Blocked) => TlsProxyPoll::Blocked,
            Ok(FlushTls::Empty) => TlsProxyPoll::closed(proxy_id),
            Err(error) => TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string())),
        }
    }

    fn write_pending_guest_plaintext(&mut self, proxy_id: TcpProxyId, buffers: &BufferPool) -> Option<TlsProxyPoll> {
        if self.server_buf_pending_offset >= self.server_buf_pending_len {
            self.server_buf_pending_offset = 0;
            self.server_buf_pending_len = 0;
            return None;
        }
        let mut made_progress = false;
        loop {
            let pending = &self.server_buf.as_slice()[self.server_buf_pending_offset..self.server_buf_pending_len];
            if pending.is_empty() {
                self.server_buf_pending_offset = 0;
                self.server_buf_pending_len = 0;
                return Some(TlsProxyPoll::Progress);
            }
            let mut guest = GuestTlsSide::new(&mut self.guest_tls, &mut self.tls_out);
            match guest.write_plaintext(pending) {
                Ok(TlsPlaintextWrite::Accepted(len)) => {
                    made_progress = true;
                    self.server_buf_pending_offset += len;
                    match guest.flush_ciphertext(buffers) {
                        Ok(FlushTls::Bytes(bytes)) => return Some(TlsProxyPoll::bytes(bytes)),
                        Ok(FlushTls::Blocked) => return Some(TlsProxyPoll::Blocked),
                        Ok(FlushTls::Empty) => {}
                        Err(error) => {
                            return Some(TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string())));
                        }
                    }
                }
                Ok(TlsPlaintextWrite::BlockedByPendingCiphertext) => match guest.flush_ciphertext(buffers) {
                    Ok(FlushTls::Bytes(bytes)) => return Some(TlsProxyPoll::bytes(bytes)),
                    Ok(FlushTls::Blocked) => return Some(TlsProxyPoll::Blocked),
                    Ok(FlushTls::Empty) => {
                        return Some(if made_progress {
                            TlsProxyPoll::Progress
                        } else {
                            TlsProxyPoll::Blocked
                        });
                    }
                    Err(error) => return Some(TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string()))),
                },
                Err(error) => return Some(TlsProxyPoll::event(TcpProxyEvent::error(proxy_id, error.to_string()))),
            }
        }
    }

    pub(super) fn forward_plaintext_to_server(
        guest_tls: &mut TlsServerSession,
        filter: &mut Http1Filter,
        buffer: &mut ByteBuf,
        output: &mut ByteBuf,
        output_offset: &mut usize,
        server_pending: &mut WriteQueue,
        buffers: &BufferPool,
    ) -> io::Result<RelayStep> {
        if matches!(
            Self::queue_server_plaintext(server_pending, output, output_offset, buffers),
            QueueStep::Blocked
        ) {
            return Ok(RelayStep::Blocked);
        }
        let mut made_progress = false;
        loop {
            let n = match guest_tls.read_plaintext_some(buffer.as_mut_vec()) {
                Ok(TlsPlaintextRead::Plaintext(n)) => n,
                Ok(TlsPlaintextRead::Blocked) => break,
                Ok(TlsPlaintextRead::Closed) => {
                    return Ok(if made_progress {
                        RelayStep::ProgressClosed
                    } else {
                        RelayStep::Closed
                    });
                }
                Err(error) => return Err(error),
            };
            made_progress = true;
            let has_output = filter.push(&buffer.as_slice()[..n], output.as_mut_vec())?;
            if has_output {
                match Self::queue_server_plaintext(server_pending, output, output_offset, buffers) {
                    QueueStep::Progress => {}
                    QueueStep::Blocked => return Ok(RelayStep::Blocked),
                }
            }
        }
        Ok(if made_progress {
            RelayStep::Progress
        } else {
            RelayStep::Blocked
        })
    }

    fn queue_existing_server_output(&mut self, buffers: &BufferPool) -> bool {
        matches!(
            Self::queue_server_plaintext(
                &mut self.server_pending,
                &mut self.substitute_buf,
                &mut self.server_output_offset,
                buffers,
            ),
            QueueStep::Progress
        )
    }

    pub(super) fn queue_server_plaintext(
        server_pending: &mut WriteQueue,
        output: &mut ByteBuf,
        output_offset: &mut usize,
        buffers: &BufferPool,
    ) -> QueueStep {
        while *output_offset < output.len() {
            let end = (*output_offset + buffers.limits().tls_relay_buffer_capacity).min(output.len());
            let chunk = &output.as_slice()[*output_offset..end];
            let Ok(mut output) = buffers.try_byte_with_capacity(chunk.len()) else {
                return QueueStep::Blocked;
            };
            output.extend_from_slice(chunk);
            server_pending.push(output);
            *output_offset = end;
        }
        output.as_mut_vec().clear();
        *output_offset = 0;
        QueueStep::Progress
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TlsRoute {
    Drop(BlockReason),
    Bypass,
}

pub(super) struct InterceptedTls {
    authority: Authority,
    secrets: crate::RuntimeSecrets,
    server_config: TlsServerConfig,
}

pub(super) fn tls_route(policy: &TlsEgressPolicy, host: &str) -> Result<TlsRoute, InterceptedTls> {
    let host = Authority::new(host);
    let decision = policy.decision_for(&host).unwrap_or(&policy.fallback);
    match &decision.application {
        ApplicationPolicy::Raw => Ok(TlsRoute::Bypass),
        ApplicationPolicy::Block { reason } => Ok(TlsRoute::Drop(*reason)),
        ApplicationPolicy::Http1 { authority, .. } if should_bypass_tls(&policy.bypass_hosts, authority.as_str()) => {
            Ok(TlsRoute::Bypass)
        }
        ApplicationPolicy::Http1 { authority, secrets } => {
            let Some(server_config) = policy.server_config_for(authority) else {
                return Ok(TlsRoute::Drop(BlockReason::TlsInterceptUnavailable));
            };
            Err(InterceptedTls {
                authority: authority.clone(),
                secrets: secrets.clone(),
                server_config: server_config.clone(),
            })
        }
    }
}

pub(super) fn should_bypass_tls(patterns: &[String], host: &str) -> bool {
    let host = normalize_host(host);
    patterns.iter().any(|pattern| {
        pattern == &host
            || pattern
                .strip_prefix("*.")
                .is_some_and(|suffix| host == suffix || host.ends_with(&format!(".{suffix}")))
    })
}

struct GuestTlsSide<'a> {
    connection: &'a mut TlsServerSession,
    output: &'a mut ByteBuf,
}

impl<'a> GuestTlsSide<'a> {
    const fn new(connection: &'a mut TlsServerSession, output: &'a mut ByteBuf) -> Self {
        Self { connection, output }
    }

    fn write_plaintext(&mut self, bytes: &[u8]) -> io::Result<TlsPlaintextWrite> {
        if self.connection.wants_write() || !self.output.is_empty() {
            return Ok(TlsPlaintextWrite::BlockedByPendingCiphertext);
        }
        self.connection.write_plaintext_some(bytes)
    }

    fn flush_ciphertext(&mut self, buffers: &BufferPool) -> io::Result<FlushTls> {
        if self.output.is_empty() && self.connection.wants_write() {
            let mut output = BoundedVecWriter::new(self.output.as_mut_vec());
            let _drained = self.connection.drain_ciphertext_to(&mut output)?;
        }
        if self.output.is_empty() {
            return Ok(FlushTls::Empty);
        }
        let len = self.output.len().min(buffers.tcp_byte_capacity());
        let Ok(mut bytes) = buffers.try_byte_with_capacity(len) else {
            return Ok(FlushTls::Blocked);
        };
        bytes.extend_from_slice(&self.output.as_slice()[..len]);
        self.output.as_mut_vec().drain(..len);
        Ok(FlushTls::Bytes(bytes))
    }
}

fn feed_guest_tls(connection: &mut TlsServerSession, bytes: &[u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        match connection.accept_ciphertext_bounded(&bytes[offset..])? {
            TlsCiphertextRead::Read(read) => offset += read,
            TlsCiphertextRead::Blocked | TlsCiphertextRead::Closed => {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
    }
    Ok(())
}

struct BoundedVecWriter<'a> {
    output: &'a mut Vec<u8>,
}

impl<'a> BoundedVecWriter<'a> {
    const fn new(output: &'a mut Vec<u8>) -> Self {
        Self { output }
    }
}

impl io::Write for BoundedVecWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let available = self.output.capacity().saturating_sub(self.output.len());
        let len = available.min(bytes.len());
        self.output.extend_from_slice(&bytes[..len]);
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn normalize_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}
