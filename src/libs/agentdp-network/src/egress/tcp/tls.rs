use std::io;
use std::net::SocketAddr;

use agentdp_crypto::{
    TlsCiphertextDrain, TlsCiphertextRead, TlsPlaintextRead, TlsPlaintextWrite, TlsServerConfig, TlsServerSession,
};

use super::tls_upstream::{TlsDrive, TlsPlaintextDrive, TlsUpstream};
use super::{TcpProxyErrorContext, TcpProxyEvent, TcpProxyPermit};
use crate::application::{Http1Filter, Http1ResponseEof};
use crate::buffers::{BufferPool, ByteBuf};
use crate::buffers::{PendingWrite, WriteQueue};
use crate::drive::{DriveProtocolOp, DriveProtocolOutput, DriveProtocolPoll, DriveTurn};
use crate::network::{ApplicationPolicy, BlockReason, TcpProxyId, TlsEgressPolicy};
use crate::policy::Authority;
use crate::reactor::ReactorBackend;
use crate::readiness::IoSlotState;
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
    pub(super) server_buf: Option<ByteBuf>,
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
    Pending,
    Bypass {
        dst: SocketAddr,
        bytes: ByteBuf,
        pending: WriteQueue,
    },
}

enum FlushTls {
    Bytes(ByteBuf),
    Blocked,
    Budget,
    Empty,
}

#[derive(Debug, Clone, Copy)]
enum GuestTlsOutputWait {
    LocalBuffer,
    ProtocolOutput,
}

impl GuestTlsOutputWait {
    const fn record(self, drive: &mut DriveTurn<'_>) {
        match self {
            Self::LocalBuffer => drive.wait_for_local_buffer_capacity(),
            Self::ProtocolOutput => drive.wait_for_local_buffer_for_protocol_output(),
        }
    }
}

const GUEST_TLS_FEED_CHUNK_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelayStep {
    Progress,
    Blocked,
    Budget,
    ProgressBlocked,
    ProgressClosed,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QueueStep {
    Progress,
    Empty,
    Blocked,
    Budget,
    ProgressBlocked,
}

impl QueueStep {
    const fn made_progress(self) -> bool {
        matches!(self, Self::Progress | Self::ProgressBlocked)
    }

    const fn blocked(self) -> bool {
        matches!(self, Self::Blocked | Self::ProgressBlocked)
    }
}

impl TlsProxyPoll {
    fn error(proxy: TcpProxyId, message: impl Into<String>) -> Self {
        Self::Event(TcpProxyEvent::error(proxy, message))
    }

    const fn closed(proxy: TcpProxyId) -> Self {
        Self::Event(TcpProxyEvent::closed(proxy))
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

    pub(super) fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
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

    pub(super) fn has_local_work(&self, guest_can_send: bool) -> bool {
        match &self.state {
            TlsTcpProxyState::WaitingClientHelloBuffer { .. } => {
                self.close_requested || self.guest_write_finished || !self.pending.is_empty()
            }
            TlsTcpProxyState::ReadingClientHello { initial, .. } => {
                self.close_requested || self.guest_write_finished || !initial.is_empty() || !self.pending.is_empty()
            }
            TlsTcpProxyState::GuestTlsHandshake { guest_tls, tls_out, .. } => {
                self.close_requested
                    || !self.pending.is_empty()
                    || (guest_can_send && (guest_tls.wants_write() || !tls_out.is_empty()))
            }
            TlsTcpProxyState::ConnectingServer { guest_tls, tls_out, .. } => {
                self.close_requested || (guest_can_send && (guest_tls.wants_write() || !tls_out.is_empty()))
            }
            TlsTcpProxyState::OpenIntercept(proxy) => {
                proxy.has_local_work(guest_can_send, self.guest_write_finished, self.pending.is_empty())
            }
            TlsTcpProxyState::Closing => true,
        }
    }

    pub(super) fn has_reactor_write_work(&self) -> bool {
        match &self.state {
            TlsTcpProxyState::ConnectingServer { server_tls, .. } => server_tls.has_reactor_write_work(),
            TlsTcpProxyState::OpenIntercept(proxy) => {
                proxy.has_reactor_write_work(self.guest_write_finished, self.pending.is_empty())
            }
            TlsTcpProxyState::WaitingClientHelloBuffer { .. }
            | TlsTcpProxyState::ReadingClientHello { .. }
            | TlsTcpProxyState::GuestTlsHandshake { .. }
            | TlsTcpProxyState::Closing => false,
        }
    }

    pub(super) const fn mark_reactor_ready(&mut self, readable: bool, writable: bool) {
        match &mut self.state {
            TlsTcpProxyState::ConnectingServer { server_tls, .. } => {
                server_tls.mark_reactor_ready(readable, writable);
                if readable || writable {
                    server_tls.mark_connect_ready();
                }
            }
            TlsTcpProxyState::OpenIntercept(proxy) => {
                proxy.server_tls.mark_reactor_ready(readable, writable);
                if readable || writable {
                    proxy.server_tls.mark_connect_ready();
                }
            }
            TlsTcpProxyState::WaitingClientHelloBuffer { .. }
            | TlsTcpProxyState::ReadingClientHello { .. }
            | TlsTcpProxyState::GuestTlsHandshake { .. }
            | TlsTcpProxyState::Closing => {}
        }
    }

    pub(super) const fn io(&self) -> IoSlotState {
        match &self.state {
            TlsTcpProxyState::ConnectingServer { server_tls, .. } => server_tls.io(),
            TlsTcpProxyState::OpenIntercept(proxy) => proxy.server_tls.io(),
            TlsTcpProxyState::WaitingClientHelloBuffer { .. }
            | TlsTcpProxyState::ReadingClientHello { .. }
            | TlsTcpProxyState::GuestTlsHandshake { .. }
            | TlsTcpProxyState::Closing => IoSlotState::new(crate::reactor::ReactorInterest::Disabled),
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
            TlsTcpProxyState::GuestTlsHandshake { guest_tls, tls_out, .. } => {
                format!(
                    "TlsTcpProxy::GuestTlsHandshake {{ pending_bytes: {}, tls_out_len: {}, guest_tls_wants_write: {}, guest_tls_handshaking: {} }}",
                    self.pending.pending_bytes(),
                    tls_out.len(),
                    guest_tls.wants_write(),
                    guest_tls.is_handshaking(),
                )
            }
            TlsTcpProxyState::Closing => "TlsTcpProxy::Closing".to_owned(),
        }
    }

    pub(super) fn drive(
        &mut self,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
        drive: &mut DriveTurn<'_>,
        permit: TcpProxyPermit,
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
            TlsTcpProxyState::WaitingClientHelloBuffer { .. } => self.poll_client_hello_buffer(buffers, drive),
            TlsTcpProxyState::ReadingClientHello { .. } => self.poll_client_hello(buffers, drive),
            TlsTcpProxyState::GuestTlsHandshake { .. } => {
                self.poll_guest_tls_handshake(buffers, runtime, drive, permit)
            }
            TlsTcpProxyState::ConnectingServer { guest_tls, tls_out, .. }
                if (guest_tls.wants_write() || !tls_out.is_empty())
                    && permit.contains(TcpProxyPermit::READ_UPSTREAM) =>
            {
                GuestTlsSide::new(guest_tls, tls_out)
                    .poll_ciphertext_output(self.proxy, buffers, drive, GuestTlsOutputWait::LocalBuffer)
                    .unwrap_or(TlsProxyPoll::Pending)
            }
            TlsTcpProxyState::ConnectingServer { .. } if self.close_requested => TlsProxyPoll::closed(self.proxy),
            TlsTcpProxyState::ConnectingServer { server_tls, .. } => {
                match server_tls.drive_handshake(runtime.reactor_mut(), drive) {
                    Ok(TlsDrive::Ready) => {
                        let _progress = drive.apply_state_change(|| self.open_intercept());
                        TlsProxyPoll::Pending
                    }
                    Ok(TlsDrive::Pending) => TlsProxyPoll::Pending,
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
                drive,
                permit,
            ),
            TlsTcpProxyState::Closing => TlsProxyPoll::closed(self.proxy),
        }
    }

    fn poll_client_hello_buffer(&mut self, buffers: &BufferPool, drive: &mut DriveTurn<'_>) -> TlsProxyPoll {
        let TlsTcpProxyState::WaitingClientHelloBuffer { policy } =
            std::mem::replace(&mut self.state, TlsTcpProxyState::Closing)
        else {
            drive.wait_for_guest_recv();
            return TlsProxyPoll::Pending;
        };
        let initial = match buffers.try_byte_with_capacity(buffers.limits().client_hello_limit.min(4096)) {
            Ok(initial) => initial,
            Err(_exhausted) => {
                self.state = TlsTcpProxyState::WaitingClientHelloBuffer { policy };
                drive.wait_for_local_buffer_capacity();
                return TlsProxyPoll::Pending;
            }
        };
        self.state = TlsTcpProxyState::ReadingClientHello { policy, initial };
        self.poll_client_hello(buffers, drive)
    }

    fn poll_client_hello(&mut self, buffers: &BufferPool, drive: &mut DriveTurn<'_>) -> TlsProxyPoll {
        if self.close_requested {
            return TlsProxyPoll::closed(self.proxy);
        }
        let TlsTcpProxyState::ReadingClientHello { policy, initial } =
            std::mem::replace(&mut self.state, TlsTcpProxyState::Closing)
        else {
            drive.wait_for_guest_recv();
            return TlsProxyPoll::Pending;
        };
        let mut initial = initial;
        loop {
            if initial.is_empty() {
                let Some(write) = self.pending.pop_front() else {
                    self.state = TlsTcpProxyState::ReadingClientHello { policy, initial };
                    drive.wait_for_guest_recv();
                    return TlsProxyPoll::Pending;
                };
                initial.extend_from_slice(&write.bytes.as_slice()[write.offset..]);
            }
            if initial.as_slice().first().is_some_and(|byte| *byte != 0x16) {
                return TlsProxyPoll::error(self.proxy, "not a TLS ClientHello");
            }
            if initial.len() >= buffers.limits().client_hello_limit {
                return TlsProxyPoll::error(self.proxy, "TLS ClientHello too large or missing SNI");
            }
            if let Some(host) = sni::extract_sni(initial.as_slice()) {
                return self.route_client_hello(policy, initial, &host, buffers, drive);
            }
            let Some(write) = self.pending.pop_front() else {
                self.state = TlsTcpProxyState::ReadingClientHello { policy, initial };
                drive.wait_for_guest_recv();
                return TlsProxyPoll::Pending;
            };
            initial.extend_from_slice(&write.bytes.as_slice()[write.offset..]);
        }
    }

    fn route_client_hello(
        &mut self,
        policy: TlsEgressPolicy,
        initial: ByteBuf,
        host: &str,
        buffers: &BufferPool,
        drive: &mut DriveTurn<'_>,
    ) -> TlsProxyPoll {
        let host = normalize_host(host);
        self.authority = Some(host.clone());
        match tls_route(&policy, &host) {
            Ok(TlsRoute::Drop(reason)) => TlsProxyPoll::error(
                self.proxy,
                format!("egress blocked by TLS policy: {reason:?}; host: {host}"),
            ),
            Ok(TlsRoute::Bypass) => TlsProxyPoll::Bypass {
                dst: policy.dst,
                bytes: initial,
                pending: std::mem::take(&mut self.pending),
            },
            Err(intercept) => self.start_guest_tls_handshake(policy, intercept, initial, buffers, drive),
        }
    }

    fn start_guest_tls_handshake(
        &mut self,
        policy: TlsEgressPolicy,
        intercept: InterceptedTls,
        initial: ByteBuf,
        buffers: &BufferPool,
        drive: &mut DriveTurn<'_>,
    ) -> TlsProxyPoll {
        let mut tls_out = match buffers.try_byte_with_capacity(buffers.limits().tls_relay_buffer_capacity) {
            Ok(buffer) => buffer,
            Err(_exhausted) => {
                self.state = TlsTcpProxyState::ReadingClientHello { policy, initial };
                drive.wait_for_local_buffer_capacity();
                return TlsProxyPoll::Pending;
            }
        };
        match TlsServerSession::accept(&intercept.server_config) {
            Ok(mut guest_tls) => {
                let mut initial = initial;
                let feed_len = match feed_guest_tls_step(&mut guest_tls, initial.as_slice(), drive) {
                    Ok(DriveProtocolPoll::Complete(feed_len)) => feed_len,
                    Ok(DriveProtocolPoll::Budget) => {
                        self.state = TlsTcpProxyState::ReadingClientHello { policy, initial };
                        return TlsProxyPoll::Pending;
                    }
                    Err(error) => {
                        let error = io::Error::new(error.kind(), format!("guest TLS ClientHello feed failed: {error}"));
                        return TlsProxyPoll::error(self.proxy, error.to_string());
                    }
                };
                initial.as_mut_vec().drain(..feed_len);
                if !initial.is_empty() {
                    self.pending.push_front(PendingWrite {
                        bytes: initial,
                        offset: 0,
                    });
                }
                let poll = GuestTlsSide::new(&mut guest_tls, &mut tls_out).poll_ciphertext_output(
                    self.proxy,
                    buffers,
                    drive,
                    GuestTlsOutputWait::LocalBuffer,
                );
                if let Some(TlsProxyPoll::Event(event)) = poll {
                    return TlsProxyPoll::Event(event);
                }
                self.restore_guest_tls_handshake(policy, intercept, Box::new(guest_tls), tls_out);
                poll.unwrap_or(TlsProxyPoll::Pending)
            }
            Err(error) => TlsProxyPoll::error(self.proxy, error.to_string()),
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "keeps the TLS handshake state transition visible without single-use wrapper methods"
    )]
    fn poll_guest_tls_handshake(
        &mut self,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
        drive: &mut DriveTurn<'_>,
        permit: TcpProxyPermit,
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
            drive.wait_for_guest_recv();
            return TlsProxyPoll::Pending;
        };
        let mut guest_tls = guest_tls;
        let mut tls_out = tls_out;
        if (guest_tls.wants_write() || !tls_out.is_empty())
            && let Some(poll) = GuestTlsSide::new(&mut guest_tls, &mut tls_out).poll_ciphertext_output(
                self.proxy,
                buffers,
                drive,
                GuestTlsOutputWait::LocalBuffer,
            )
        {
            if matches!(poll, TlsProxyPoll::Event(_)) {
                return poll;
            }
            self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
            return poll;
        }
        while guest_tls.is_handshaking() {
            let Some(mut write) = self.pending.pop_front() else {
                self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
                drive.wait_for_guest_recv();
                return TlsProxyPoll::Pending;
            };
            while write.offset < write.bytes.len() && guest_tls.is_handshaking() {
                let feed = match feed_guest_tls_step(
                    &mut guest_tls,
                    &write.bytes.as_slice()[write.offset..write.bytes.len()],
                    drive,
                ) {
                    Ok(feed) => feed,
                    Err(error) => {
                        let error = io::Error::new(error.kind(), format!("guest TLS handshake feed failed: {error}"));
                        return TlsProxyPoll::error(self.proxy, error.to_string());
                    }
                };
                let feed_len = match feed {
                    DriveProtocolPoll::Complete(feed_len) => feed_len,
                    DriveProtocolPoll::Budget => {
                        self.pending.push_front(write);
                        self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
                        return TlsProxyPoll::Pending;
                    }
                };
                write.offset += feed_len;
                if let Some(poll) = GuestTlsSide::new(&mut guest_tls, &mut tls_out).poll_ciphertext_output(
                    self.proxy,
                    buffers,
                    drive,
                    GuestTlsOutputWait::LocalBuffer,
                ) {
                    if write.offset < write.bytes.len() {
                        self.pending.push_front(write);
                    }
                    if matches!(poll, TlsProxyPoll::Event(_)) {
                        return poll;
                    }
                    self.restore_guest_tls_handshake(policy, intercept, guest_tls, tls_out);
                    return poll;
                }
            }
            if write.offset < write.bytes.len() {
                self.pending.push_front(write);
            }
        }

        self.connect_intercept_after_guest_handshake(
            CompletedGuestTlsHandshake {
                policy,
                intercept,
                guest_tls,
                tls_out,
            },
            buffers,
            runtime,
            drive,
            permit,
        )
    }

    fn connect_intercept_after_guest_handshake(
        &mut self,
        mut handshake: CompletedGuestTlsHandshake,
        buffers: &BufferPool,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
        drive: &mut DriveTurn<'_>,
        permit: TcpProxyPermit,
    ) -> TlsProxyPoll {
        let authority = handshake.intercept.authority.as_str().to_owned();
        let mut plaintext_buf = match buffers.try_byte_with_capacity(buffers.limits().tls_relay_buffer_capacity) {
            Ok(buffer) => buffer,
            Err(_exhausted) => {
                self.restore_guest_tls_handshake(
                    handshake.policy,
                    handshake.intercept,
                    handshake.guest_tls,
                    handshake.tls_out,
                );
                drive.wait_for_local_buffer_capacity();
                return TlsProxyPoll::Pending;
            }
        };
        plaintext_buf.resize_zeroed(buffers.limits().tls_relay_buffer_capacity);
        let mut substitute_buf = match buffers.try_byte_with_capacity(buffers.limits().tls_relay_buffer_capacity) {
            Ok(buffer) => buffer,
            Err(_exhausted) => {
                self.restore_guest_tls_handshake(
                    handshake.policy,
                    handshake.intercept,
                    handshake.guest_tls,
                    handshake.tls_out,
                );
                drive.wait_for_local_buffer_capacity();
                return TlsProxyPoll::Pending;
            }
        };
        let mut server_pending = WriteQueue::new();
        let mut filter = Http1Filter::new(handshake.intercept.secrets.clone(), authority, buffers);
        let mut server_output_offset = 0;
        match TlsHttp1Proxy::<R>::forward_plaintext_to_server(
            &mut handshake.guest_tls,
            &mut filter,
            &mut plaintext_buf,
            &mut substitute_buf,
            &mut server_output_offset,
            &mut server_pending,
            buffers,
            drive,
        ) {
            Ok(RelayStep::Progress | RelayStep::Blocked | RelayStep::ProgressBlocked) => {}
            Ok(RelayStep::Budget) => {
                self.restore_guest_tls_handshake(
                    handshake.policy,
                    handshake.intercept,
                    handshake.guest_tls,
                    handshake.tls_out,
                );
                return TlsProxyPoll::Pending;
            }
            Ok(RelayStep::ProgressClosed | RelayStep::Closed) => self.guest_write_finished = true,
            Err(error) => return TlsProxyPoll::error(self.proxy, error.to_string()),
        }
        let server_tls = match TlsUpstream::connect(
            self.proxy,
            handshake.policy.dst,
            handshake.intercept.authority.as_str(),
            &handshake.policy.client_config,
            runtime,
        ) {
            Ok(server_tls) => server_tls,
            Err(error) => return TlsProxyPoll::error(self.proxy, error.to_string()),
        };
        self.state = TlsTcpProxyState::ConnectingServer {
            guest_tls: handshake.guest_tls,
            filter,
            tls_out: handshake.tls_out,
            plaintext_buf,
            substitute_buf,
            server_output_offset,
            server_pending,
            server_tls,
        };
        self.drive(buffers, runtime, drive, permit)
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

    fn open_intercept(&mut self) {
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
            return;
        };
        self.state = TlsTcpProxyState::OpenIntercept(TlsHttp1Proxy {
            guest_tls,
            server_tls,
            filter,
            tls_out,
            server_buf: None,
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
    fn has_local_work(&self, guest_can_send: bool, guest_write_finished: bool, pending_empty: bool) -> bool {
        let guest_tls_output = self.guest_tls.wants_write() || !self.tls_out.is_empty();
        let guest_plaintext_output = self.server_buf_pending_offset < self.server_buf_pending_len;
        let upstream_plaintext_output = !self.substitute_buf.is_empty();
        let upstream_write_pending = !self.server_pending.is_empty();
        let upstream_should_finish = guest_write_finished || self.guest_tls_closed;
        let upstream_finish = upstream_should_finish && pending_empty && !self.server_tls.write_finished();
        let upstream_needs_write = upstream_write_pending || upstream_finish;
        let io = self.server_tls.io();
        (guest_can_send
            && (guest_tls_output
                || guest_plaintext_output
                || self.server_read_pending
                || self.guest_close_notify_queued))
            || upstream_plaintext_output
            || (upstream_needs_write && (!io.watches_write() || io.can_write()))
    }

    fn has_reactor_write_work(&self, guest_write_finished: bool, pending_empty: bool) -> bool {
        let upstream_should_finish = guest_write_finished || self.guest_tls_closed;
        !self.server_pending.is_empty()
            || self.server_tls.connection.wants_write()
            || (upstream_should_finish && pending_empty && !self.server_tls.write_finished())
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn drive(
        &mut self,
        proxy_id: TcpProxyId,
        close_requested: bool,
        guest_write_finished: bool,
        pending: &mut WriteQueue,
        buffers: &BufferPool,
        reactor: &R,
        drive: &mut DriveTurn<'_>,
        permit: TcpProxyPermit,
    ) -> TlsProxyPoll {
        if permit.contains(TcpProxyPermit::READ_UPSTREAM)
            && let Some(poll) = GuestTlsSide::new(&mut self.guest_tls, &mut self.tls_out).poll_ciphertext_output(
                proxy_id,
                buffers,
                drive,
                GuestTlsOutputWait::ProtocolOutput,
            )
        {
            return poll;
        }
        if permit.contains(TcpProxyPermit::READ_UPSTREAM)
            && let Some(poll) = self.write_pending_guest_plaintext(proxy_id, buffers, drive)
        {
            return poll;
        }
        if close_requested {
            return TlsProxyPoll::closed(proxy_id);
        }
        match self.flush_server_plaintext(proxy_id, buffers, reactor, drive) {
            Ok(true) => {
                drive.wait_for_local_buffer_for_protocol_output();
                return TlsProxyPoll::Pending;
            }
            Ok(false) => {}
            Err(event) => return TlsProxyPoll::Event(event),
        }
        if permit.contains(TcpProxyPermit::WRITE_UPSTREAM) && !pending.is_empty() {
            let can_emit_guest_tls = permit.contains(TcpProxyPermit::READ_UPSTREAM);
            let poll = self.feed_pending_guest_tls(proxy_id, pending, buffers, can_emit_guest_tls, drive);
            debug_assert!(!matches!(poll, TlsProxyPoll::Bypass { .. }));
            if matches!(poll, TlsProxyPoll::Pending)
                && !can_emit_guest_tls
                && (self.guest_tls.wants_write() || !self.tls_out.is_empty())
            {
                drive.wait_for_guest_send_capacity();
            }
            return poll;
        }
        match self.flush_server_plaintext(proxy_id, buffers, reactor, drive) {
            Ok(true) => {
                drive.wait_for_local_buffer_for_protocol_output();
                return TlsProxyPoll::Pending;
            }
            Ok(false) => {}
            Err(event) => return TlsProxyPoll::Event(event),
        }
        match self.write_pending_server_plaintext(proxy_id, reactor, drive) {
            Ok(()) => {}
            Err(event) => return TlsProxyPoll::Event(event),
        }
        if let Some(event) = self.finish_server_write_if_needed(
            proxy_id,
            guest_write_finished || self.guest_tls_closed,
            pending.is_empty(),
            reactor,
            drive,
        ) {
            return TlsProxyPoll::Event(event);
        }
        if permit.contains(TcpProxyPermit::READ_UPSTREAM) {
            let poll = self.read_server_tls(proxy_id, buffers, reactor, drive);
            debug_assert!(!matches!(poll, TlsProxyPoll::Bypass { .. }));
            if !matches!(poll, TlsProxyPoll::Pending) {
                return poll;
            }
        } else {
            if let Err(error) = self.server_tls.park_read(reactor, !self.server_pending.is_empty()) {
                return TlsProxyPoll::error(proxy_id, error.to_string());
            }
            if self.server_read_pending {
                drive.wait_for_guest_send_capacity();
            }
        }
        TlsProxyPoll::Pending
    }

    fn feed_pending_guest_tls(
        &mut self,
        proxy_id: TcpProxyId,
        pending: &mut WriteQueue,
        buffers: &BufferPool,
        can_emit_guest_tls: bool,
        drive: &mut DriveTurn<'_>,
    ) -> TlsProxyPoll {
        let Some(mut write) = pending.pop_front() else {
            drive.wait_for_guest_recv();
            return TlsProxyPoll::Pending;
        };
        let feed_limit = (write.offset + buffers.limits().tls_relay_buffer_capacity).min(write.bytes.len());
        while write.offset < feed_limit {
            let feed = feed_guest_tls_step(
                &mut self.guest_tls,
                &write.bytes.as_slice()[write.offset..feed_limit],
                drive,
            )
            .map_err(|error| io::Error::new(error.kind(), format!("guest TLS application feed failed: {error}")));
            let feed_len = match feed {
                Ok(DriveProtocolPoll::Complete(feed_len)) => feed_len,
                Ok(DriveProtocolPoll::Budget) => {
                    pending.push_front(write);
                    return TlsProxyPoll::Pending;
                }
                Err(error) => return TlsProxyPoll::error(proxy_id, error.to_string()),
            };
            write.offset += feed_len;
            match Self::forward_plaintext_to_server(
                &mut self.guest_tls,
                &mut self.filter,
                &mut self.plaintext_buf,
                &mut self.substitute_buf,
                &mut self.server_output_offset,
                &mut self.server_pending,
                buffers,
                drive,
            ) {
                Ok(RelayStep::Progress) => {}
                Ok(RelayStep::Budget | RelayStep::Blocked) => {
                    if write.offset < write.bytes.len() {
                        pending.push_front(write);
                    }
                    return TlsProxyPoll::Pending;
                }
                Ok(RelayStep::ProgressBlocked) => {
                    if write.offset < write.bytes.len() {
                        pending.push_front(write);
                    }
                    drive.wait_for_local_buffer_for_protocol_output();
                    return TlsProxyPoll::Pending;
                }
                Ok(RelayStep::ProgressClosed | RelayStep::Closed) => {
                    let _progress = drive.apply_state_change(|| {
                        self.guest_tls_closed = true;
                    });
                    return TlsProxyPoll::Pending;
                }
                Err(error) => return TlsProxyPoll::error(proxy_id, error.to_string()),
            }
        }
        if write.offset < write.bytes.len() {
            pending.push_front(write);
        }
        if can_emit_guest_tls
            && let Some(poll) = GuestTlsSide::new(&mut self.guest_tls, &mut self.tls_out).poll_ciphertext_output(
                proxy_id,
                buffers,
                drive,
                GuestTlsOutputWait::LocalBuffer,
            )
        {
            return poll;
        }
        TlsProxyPoll::Pending
    }

    fn flush_server_plaintext(
        &mut self,
        proxy_id: TcpProxyId,
        buffers: &BufferPool,
        reactor: &R,
        drive: &mut DriveTurn<'_>,
    ) -> Result<bool, TcpProxyEvent> {
        self.write_pending_server_plaintext(proxy_id, reactor, drive)?;
        let queue = Self::queue_server_plaintext(
            &mut self.server_pending,
            &mut self.substitute_buf,
            &mut self.server_output_offset,
            buffers,
            drive,
        );
        if queue.made_progress() {
            self.write_pending_server_plaintext(proxy_id, reactor, drive)?;
        }
        Ok(queue.blocked())
    }

    fn write_pending_server_plaintext(
        &mut self,
        proxy_id: TcpProxyId,
        reactor: &R,
        drive: &mut DriveTurn<'_>,
    ) -> Result<(), TcpProxyEvent> {
        self.server_tls
            .write_pending_plaintext(&mut self.server_pending, reactor, drive)
            .map_err(|error| TcpProxyEvent::error(proxy_id, error.to_string()))?;
        if !self.server_pending.is_empty() {
            drive.wait_for_reactor_write();
        }
        Ok(())
    }

    fn finish_server_write_if_needed(
        &mut self,
        proxy_id: TcpProxyId,
        guest_write_finished: bool,
        pending_empty: bool,
        reactor: &R,
        drive: &mut DriveTurn<'_>,
    ) -> Option<TcpProxyEvent> {
        if !guest_write_finished || !pending_empty {
            return None;
        }
        if self.server_output_offset < self.substitute_buf.len()
            || !self.server_pending.is_empty()
            || self.server_tls.write_finished()
        {
            return None;
        }
        self.server_tls
            .finish_write(reactor, drive)
            .err()
            .map(|error| TcpProxyEvent::error(proxy_id, error.to_string()))
    }

    fn read_server_tls(
        &mut self,
        proxy_id: TcpProxyId,
        buffers: &BufferPool,
        reactor: &R,
        drive: &mut DriveTurn<'_>,
    ) -> TlsProxyPoll {
        match Self::queue_server_plaintext(
            &mut self.server_pending,
            &mut self.substitute_buf,
            &mut self.server_output_offset,
            buffers,
            drive,
        ) {
            QueueStep::Progress | QueueStep::Empty => {}
            QueueStep::Budget => return TlsProxyPoll::Pending,
            QueueStep::Blocked | QueueStep::ProgressBlocked => {
                drive.wait_for_local_buffer_for_protocol_output();
                return TlsProxyPoll::Pending;
            }
        }
        if let Some(poll) = self.write_pending_guest_plaintext(proxy_id, buffers, drive) {
            return poll;
        }
        let read_limit = buffers.limits().tls_relay_buffer_capacity;
        if self.server_buf.is_none() {
            let mut server_buf = match buffers.try_byte_with_capacity(buffers.limits().tls_relay_buffer_capacity) {
                Ok(buffer) => buffer,
                Err(_exhausted) => {
                    drive.wait_for_local_buffer_for_protocol_output();
                    return TlsProxyPoll::Pending;
                }
            };
            server_buf.resize_zeroed(read_limit);
            self.server_buf = Some(server_buf);
        }
        let Some(server_buf) = self.server_buf.as_mut() else {
            drive.wait_for_local_buffer_for_protocol_output();
            return TlsProxyPoll::Pending;
        };
        server_buf.as_mut_vec().resize(read_limit, 0);
        let before_read = drive.progress();
        match self.server_tls.read_plaintext(
            server_buf.as_mut_slice(),
            reactor,
            !self.server_pending.is_empty(),
            drive,
        ) {
            Ok(TlsPlaintextDrive::Eof) => self.handle_upstream_eof(proxy_id, buffers, drive),
            Ok(TlsPlaintextDrive::Plaintext(len)) => {
                self.server_read_pending = true;
                let bytes = &server_buf.as_slice()[..len];
                if let Err(error) = self.filter.observe_response(bytes) {
                    return TlsProxyPoll::error(proxy_id, error.to_string());
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
                            drive,
                        );
                    }
                    Ok(false) => {}
                    Err(error) => return TlsProxyPoll::error(proxy_id, error.to_string()),
                }
                self.server_buf_pending_offset = 0;
                self.server_buf_pending_len = len;
                self.write_pending_guest_plaintext(proxy_id, buffers, drive)
                    .unwrap_or(TlsProxyPoll::Pending)
            }
            Ok(TlsPlaintextDrive::Blocked) => {
                self.server_read_pending = false;
                if drive.progress() == before_read {
                    drive.wait_for_reactor_read();
                }
                TlsProxyPoll::Pending
            }
            Ok(TlsPlaintextDrive::Budget) => TlsProxyPoll::Pending,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                self.handle_upstream_eof(proxy_id, buffers, drive)
            }
            Err(error) => TlsProxyPoll::error(proxy_id, error.to_string()),
        }
    }

    fn handle_upstream_eof(
        &mut self,
        proxy_id: TcpProxyId,
        buffers: &BufferPool,
        drive: &mut DriveTurn<'_>,
    ) -> TlsProxyPoll {
        match self.filter.response_eof() {
            Http1ResponseEof::Complete | Http1ResponseEof::Tunnel => {
                self.close_guest_tls_after_upstream_eof(proxy_id, buffers, drive)
            }
            Http1ResponseEof::Incomplete => {
                TlsProxyPoll::error(proxy_id, "upstream closed before complete HTTP response")
            }
        }
    }

    fn close_guest_tls_after_upstream_eof(
        &mut self,
        proxy_id: TcpProxyId,
        buffers: &BufferPool,
        drive: &mut DriveTurn<'_>,
    ) -> TlsProxyPoll {
        if let Some(poll) = GuestTlsSide::new(&mut self.guest_tls, &mut self.tls_out).poll_ciphertext_output(
            proxy_id,
            buffers,
            drive,
            GuestTlsOutputWait::LocalBuffer,
        ) {
            return poll;
        }

        if !self.guest_close_notify_queued {
            self.guest_tls.queue_close_notify();
            self.guest_close_notify_queued = true;
        }

        GuestTlsSide::new(&mut self.guest_tls, &mut self.tls_out)
            .poll_ciphertext_output(proxy_id, buffers, drive, GuestTlsOutputWait::LocalBuffer)
            .unwrap_or_else(|| TlsProxyPoll::closed(proxy_id))
    }

    fn write_pending_guest_plaintext(
        &mut self,
        proxy_id: TcpProxyId,
        buffers: &BufferPool,
        drive: &mut DriveTurn<'_>,
    ) -> Option<TlsProxyPoll> {
        if self.server_buf_pending_offset >= self.server_buf_pending_len {
            self.server_buf_pending_offset = 0;
            self.server_buf_pending_len = 0;
            return None;
        }
        let mut made_progress = false;
        loop {
            let Some(server_buf) = self.server_buf.as_ref() else {
                return Some(TlsProxyPoll::error(
                    proxy_id,
                    "server plaintext pending without read buffer",
                ));
            };
            let mut guest = GuestTlsSide::new(&mut self.guest_tls, &mut self.tls_out);
            match drive.drive_protocol_op(
                self.server_buf_pending_len - self.server_buf_pending_offset,
                |pending_len| {
                    let pending_end = self.server_buf_pending_offset + pending_len;
                    let pending = &server_buf.as_slice()[self.server_buf_pending_offset..pending_end];
                    if pending.is_empty() {
                        return Ok(DriveProtocolOp::NoProgress {
                            value: TlsPlaintextWrite::Accepted(0),
                        });
                    }
                    guest.write_plaintext(pending).map(|write| match write {
                        TlsPlaintextWrite::Accepted(len) => DriveProtocolOp::Progress {
                            bytes: len,
                            value: TlsPlaintextWrite::Accepted(len),
                        },
                        TlsPlaintextWrite::BlockedByPendingCiphertext => DriveProtocolOp::NoProgress {
                            value: TlsPlaintextWrite::BlockedByPendingCiphertext,
                        },
                    })
                },
            ) {
                Ok(DriveProtocolPoll::Budget) => return Some(TlsProxyPoll::Pending),
                Ok(DriveProtocolPoll::Complete(TlsPlaintextWrite::Accepted(0))) => {
                    let _progress = drive.apply_state_change(|| {
                        self.server_buf_pending_offset = 0;
                        self.server_buf_pending_len = 0;
                    });
                    return Some(TlsProxyPoll::Pending);
                }
                Ok(DriveProtocolPoll::Complete(TlsPlaintextWrite::Accepted(len))) => {
                    made_progress = true;
                    self.server_buf_pending_offset += len;
                    if let Some(poll) =
                        guest.poll_ciphertext_output(proxy_id, buffers, drive, GuestTlsOutputWait::ProtocolOutput)
                    {
                        return Some(poll);
                    }
                }
                Ok(DriveProtocolPoll::Complete(TlsPlaintextWrite::BlockedByPendingCiphertext)) => {
                    if let Some(poll) =
                        guest.poll_ciphertext_output(proxy_id, buffers, drive, GuestTlsOutputWait::ProtocolOutput)
                    {
                        return Some(poll);
                    }
                    if !made_progress {
                        return Some(TlsProxyPoll::error(
                            proxy_id,
                            "guest TLS plaintext blocked by pending ciphertext, but no ciphertext was available",
                        ));
                    }
                    return Some(TlsProxyPoll::Pending);
                }
                Err(error) => return Some(TlsProxyPoll::error(proxy_id, error.to_string())),
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "used directly by focused relay tests without constructing a full proxy"
    )]
    pub(super) fn forward_plaintext_to_server(
        guest_tls: &mut TlsServerSession,
        filter: &mut Http1Filter,
        buffer: &mut ByteBuf,
        output: &mut ByteBuf,
        output_offset: &mut usize,
        server_pending: &mut WriteQueue,
        buffers: &BufferPool,
        drive: &mut DriveTurn<'_>,
    ) -> io::Result<RelayStep> {
        match Self::queue_server_plaintext(server_pending, output, output_offset, buffers, drive) {
            QueueStep::Progress | QueueStep::Empty => {}
            QueueStep::Budget => return Ok(RelayStep::Budget),
            QueueStep::Blocked => return Ok(RelayStep::Blocked),
            QueueStep::ProgressBlocked => return Ok(RelayStep::ProgressBlocked),
        }
        let mut made_progress = false;
        loop {
            let read = drive.drive_protocol_op(buffers.limits().tls_relay_buffer_capacity, |limit| {
                buffer.as_mut_vec().resize(limit, 0);
                guest_tls
                    .read_plaintext_some(&mut buffer.as_mut_vec()[..limit])
                    .map(|read| match read {
                        TlsPlaintextRead::Plaintext(n) => DriveProtocolOp::Progress {
                            bytes: n,
                            value: TlsPlaintextRead::Plaintext(n),
                        },
                        TlsPlaintextRead::Blocked => DriveProtocolOp::NoProgress {
                            value: TlsPlaintextRead::Blocked,
                        },
                        TlsPlaintextRead::Closed => DriveProtocolOp::NoProgress {
                            value: TlsPlaintextRead::Closed,
                        },
                    })
            })?;
            let n = match read {
                DriveProtocolPoll::Budget => {
                    return Ok(if made_progress {
                        RelayStep::Progress
                    } else {
                        RelayStep::Blocked
                    });
                }
                DriveProtocolPoll::Complete(TlsPlaintextRead::Plaintext(n)) => n,
                DriveProtocolPoll::Complete(TlsPlaintextRead::Blocked) => break,
                DriveProtocolPoll::Complete(TlsPlaintextRead::Closed) => {
                    return Ok(if made_progress {
                        RelayStep::ProgressClosed
                    } else {
                        RelayStep::Closed
                    });
                }
            };
            made_progress = true;
            let has_output = filter.push(&buffer.as_slice()[..n], output.as_mut_vec())?;
            if has_output {
                match Self::queue_server_plaintext(server_pending, output, output_offset, buffers, drive) {
                    QueueStep::Progress | QueueStep::Empty => {}
                    QueueStep::Budget => {
                        return Ok(if made_progress {
                            RelayStep::Progress
                        } else {
                            RelayStep::Budget
                        });
                    }
                    QueueStep::Blocked => return Ok(RelayStep::Blocked),
                    QueueStep::ProgressBlocked => return Ok(RelayStep::ProgressBlocked),
                }
            }
        }
        Ok(if made_progress {
            RelayStep::Progress
        } else {
            RelayStep::Blocked
        })
    }

    pub(super) fn queue_server_plaintext(
        server_pending: &mut WriteQueue,
        output: &mut ByteBuf,
        output_offset: &mut usize,
        buffers: &BufferPool,
        drive: &mut DriveTurn<'_>,
    ) -> QueueStep {
        let mut made_progress = false;
        loop {
            match drive.queue_protocol_output(
                server_pending,
                output,
                output_offset,
                buffers,
                buffers.limits().tls_relay_buffer_capacity,
            ) {
                DriveProtocolPoll::Budget => {
                    return if made_progress {
                        QueueStep::Progress
                    } else {
                        QueueStep::Budget
                    };
                }
                DriveProtocolPoll::Complete(Ok(DriveProtocolOutput::Bytes)) => made_progress = true,
                DriveProtocolPoll::Complete(Ok(DriveProtocolOutput::Empty)) => {
                    return if made_progress {
                        QueueStep::Progress
                    } else {
                        QueueStep::Empty
                    };
                }
                DriveProtocolPoll::Complete(Err(_exhausted)) => {
                    return if made_progress {
                        QueueStep::ProgressBlocked
                    } else {
                        QueueStep::Blocked
                    };
                }
            }
        }
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

struct CompletedGuestTlsHandshake {
    policy: TlsEgressPolicy,
    intercept: InterceptedTls,
    guest_tls: Box<TlsServerSession>,
    tls_out: ByteBuf,
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

    fn flush_ciphertext(&mut self, buffers: &BufferPool, drive: &mut DriveTurn<'_>) -> io::Result<FlushTls> {
        if self.output.is_empty() && self.connection.wants_write() {
            let drain = drive.drive_protocol_op(buffers.tcp_byte_capacity(), |limit| {
                let mut output = BoundedVecWriter::new(self.output.as_mut_vec());
                self.connection
                    .drain_ciphertext_to(&mut output, limit)
                    .map(|step| match step {
                        TlsCiphertextDrain::Progress(bytes) => DriveProtocolOp::Progress { bytes, value: () },
                        TlsCiphertextDrain::Blocked | TlsCiphertextDrain::Empty => {
                            DriveProtocolOp::NoProgress { value: () }
                        }
                    })
            })?;
            match drain {
                DriveProtocolPoll::Complete(()) => {}
                DriveProtocolPoll::Budget => return Ok(FlushTls::Budget),
            }
        }
        match drive.take_protocol_output(self.output, buffers, buffers.tcp_byte_capacity()) {
            DriveProtocolPoll::Budget => Ok(FlushTls::Budget),
            DriveProtocolPoll::Complete(Ok(Some(bytes))) => Ok(FlushTls::Bytes(bytes)),
            DriveProtocolPoll::Complete(Ok(None)) => Ok(FlushTls::Empty),
            DriveProtocolPoll::Complete(Err(_exhausted)) => Ok(FlushTls::Blocked),
        }
    }

    fn poll_ciphertext_output(
        &mut self,
        proxy: TcpProxyId,
        buffers: &BufferPool,
        drive: &mut DriveTurn<'_>,
        output_wait: GuestTlsOutputWait,
    ) -> Option<TlsProxyPoll> {
        match self.flush_ciphertext(buffers, drive) {
            Ok(FlushTls::Bytes(bytes)) => Some(TlsProxyPoll::Bytes(bytes)),
            Ok(FlushTls::Blocked) => {
                output_wait.record(drive);
                Some(TlsProxyPoll::Pending)
            }
            Ok(FlushTls::Budget) => Some(TlsProxyPoll::Pending),
            Ok(FlushTls::Empty) => None,
            Err(error) => Some(TlsProxyPoll::error(proxy, error.to_string())),
        }
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

fn feed_guest_tls_step(
    connection: &mut TlsServerSession,
    bytes: &[u8],
    drive: &mut DriveTurn<'_>,
) -> io::Result<DriveProtocolPoll<usize>> {
    drive.drive_protocol_op(bytes.len().min(GUEST_TLS_FEED_CHUNK_BYTES), |feed_len| {
        feed_guest_tls(connection, &bytes[..feed_len])?;
        Ok::<_, io::Error>(DriveProtocolOp::Progress {
            bytes: feed_len,
            value: feed_len,
        })
    })
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

#[cfg(test)]
mod tests {
    use agentdp_crypto::test_support::connected_tls_pair;

    use super::{FlushTls, GuestTlsSide};
    use crate::buffers::BufferPool;
    use crate::drive::{DriveBudget, DriveReport, DriveTurn};
    use crate::network::NetworkLimits;

    #[test]
    fn guest_tls_flush_does_not_report_empty_when_budget_blocks_ciphertext() {
        let (_client, mut guest_tls) = connected_tls_pair().expect("TLS pair should connect");
        guest_tls.queue_close_notify();

        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        let mut output = buffers.try_tcp_byte().expect("prewarmed byte buffer");
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 0,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();

        let flush = {
            let mut drive = DriveTurn::new(&mut budget, &mut report);
            GuestTlsSide::new(&mut guest_tls, &mut output)
                .flush_ciphertext(&buffers, &mut drive)
                .expect("TLS flush should not fail")
        };

        assert!(matches!(flush, FlushTls::Budget));
        assert!(report.budget_exhausted());
        assert!(!report.made_progress());
    }
}
