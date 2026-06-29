use std::io;
use std::net::SocketAddr;

use agentdp_crypto::{
    TlsCiphertextDrain, TlsCiphertextRead, TlsClientConfig, TlsClientSession, TlsPlaintextRead, TlsPlaintextWrite,
};

use crate::buffers::WriteQueue;
use crate::connectors::tcp::TcpConnector;
use crate::drive::{DriveProtocolOp, DriveProtocolPoll, DriveTransportOp, DriveTransportPoll, DriveTurn};
use crate::network::TcpProxyId;
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorInterest, ReactorTcpStream, RegisteredTcpStream, RegisteringTcpStream};
use crate::runtime::NetworkRuntime;

pub(super) struct TlsUpstream<R: ReactorBackend> {
    pub(super) stream: RegisteredTcpStream<R>,
    pub(super) connection: TlsClientSession,
    connect: TlsConnectState,
    write: TlsWriteState,
    #[cfg(any(test, feature = "simulation"))]
    pub(super) stats: TlsUpstreamStats,
}

#[cfg(any(test, feature = "simulation"))]
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct TlsUpstreamStats {
    pub(super) tls_bytes_read: usize,
    pub(super) plaintext_bytes_read: usize,
    pub(super) eof_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TlsConnectState {
    Connecting,
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TlsWriteState {
    Open { application_ciphertext_pending: bool },
    CloseNotifyPending,
    Finished,
}

pub(super) enum TlsDrive {
    Ready,
    Pending,
}

pub(super) enum TlsPlaintextDrive {
    Plaintext(usize),
    Eof,
    Blocked,
    Budget,
}

const TLS_HANDSHAKE_READ_CHUNK_BYTES: usize = 16 * 1024;

impl<R> TlsUpstream<R>
where
    R: ReactorBackend,
{
    pub(super) fn connect(
        proxy: TcpProxyId,
        dst: SocketAddr,
        server_name: &str,
        client_config: &TlsClientConfig,
        runtime: &mut impl NetworkRuntime<Reactor = R>,
    ) -> io::Result<Self> {
        let stream = runtime.tcp_connector().connect_tcp_stream(dst)?;
        let connection = TlsClientSession::connect(client_config, server_name).map_err(io::Error::other)?;
        let stream = RegisteringTcpStream::new(
            runtime.reactor_mut(),
            stream,
            ReactorItemId::TcpProxy { proxy },
            ReactorInterest::ReadWrite,
        )?
        .commit();
        Ok(Self {
            stream,
            connection,
            connect: TlsConnectState::Connecting,
            write: TlsWriteState::Open {
                application_ciphertext_pending: false,
            },
            #[cfg(any(test, feature = "simulation"))]
            stats: TlsUpstreamStats::default(),
        })
    }

    pub(super) const fn write_finished(&self) -> bool {
        // This is the application plaintext write side. TLS close_notify bytes may still need
        // reactor write readiness while the state is CloseNotifyPending.
        !matches!(self.write, TlsWriteState::Open { .. })
    }

    #[cfg(test)]
    pub(super) const fn mark_write_finished_for_test(&mut self) {
        self.write = TlsWriteState::Finished;
    }

    pub(super) const fn mark_connect_ready(&mut self) {
        self.connect = TlsConnectState::Ready;
    }

    pub(super) fn has_reactor_write_work(&self) -> bool {
        matches!(self.connect, TlsConnectState::Connecting) || self.transport_wants_write()
    }

    pub(super) fn deregister(&mut self, reactor: &mut R) {
        self.stream.deregister(reactor);
    }

    pub(super) fn drive_handshake(&mut self, reactor: &R, drive: &mut DriveTurn<'_>) -> io::Result<TlsDrive> {
        if matches!(self.connect, TlsConnectState::Connecting) {
            drive.wait_for_reactor_read_write();
            return Ok(TlsDrive::Pending);
        }
        if let Some(error) = self.stream.source().take_error()? {
            return Err(error);
        }
        while self.connection.is_handshaking() || self.connection.wants_write() {
            // Keep handshake IO phased explicitly. `complete_io` can read and write in one call,
            // which makes it too easy to ingest TLS bytes without a caller-visible drain point.
            let mut step_progress = false;
            let mut blocked_read = false;
            let blocked_write = if self.connection.wants_write() {
                match self.drain_ciphertext_ready(drive, usize::MAX) {
                    Ok(DriveTransportPoll::Complete(TlsCiphertextDrain::Progress(bytes))) => {
                        step_progress |= bytes > 0;
                        false
                    }
                    Ok(DriveTransportPoll::Complete(TlsCiphertextDrain::Blocked) | DriveTransportPoll::Pending) => {
                        self.connection.wants_write()
                    }
                    Ok(DriveTransportPoll::Complete(TlsCiphertextDrain::Empty)) => false,
                    Ok(DriveTransportPoll::Budget) => return Ok(TlsDrive::Pending),
                    Err(error) => {
                        return Err(io::Error::new(
                            error.kind(),
                            format!("upstream TLS handshake write failed: {error}"),
                        ));
                    }
                }
            } else {
                false
            };
            if self.connection.is_handshaking() {
                match self.read_ciphertext_ready(drive, TLS_HANDSHAKE_READ_CHUNK_BYTES) {
                    Ok(DriveTransportPoll::Complete(TlsCiphertextRead::Read(_read))) => {
                        step_progress = true;
                    }
                    Ok(DriveTransportPoll::Complete(TlsCiphertextRead::Blocked) | DriveTransportPoll::Pending) => {
                        blocked_read = true;
                    }
                    Ok(DriveTransportPoll::Complete(TlsCiphertextRead::Closed)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "upstream TLS handshake closed before completion",
                        ));
                    }
                    Ok(DriveTransportPoll::Budget) => return Ok(TlsDrive::Pending),
                    Err(error) => {
                        return Err(io::Error::new(
                            error.kind(),
                            format!("upstream TLS handshake read failed: {error}"),
                        ));
                    }
                }
            }
            if !step_progress {
                self.update_interest(reactor, false)?;
                match (blocked_read, blocked_write) {
                    (true, true) | (false, false) => drive.wait_for_reactor_read_write(),
                    (true, false) => drive.wait_for_reactor_read(),
                    (false, true) => drive.wait_for_reactor_write(),
                }
                return Ok(TlsDrive::Pending);
            }
        }
        self.update_interest(reactor, false)?;
        Ok(TlsDrive::Ready)
    }

    pub(super) fn write_pending_plaintext(
        &mut self,
        pending: &mut WriteQueue,
        reactor: &R,
        drive: &mut DriveTurn<'_>,
    ) -> io::Result<()> {
        if !matches!(self.write, TlsWriteState::Open { .. }) {
            if !pending.is_empty() {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            return self.flush_pending_ciphertext(reactor, drive);
        }
        if pending.is_empty() {
            if self.connection.wants_write() {
                return self.flush_pending_ciphertext(reactor, drive);
            }
            self.set_application_ciphertext_pending(false);
            return Ok(());
        }
        while let Some(bytes) = pending.front_slice() {
            if self.connection.wants_write() {
                match self.flush_tls(reactor, drive)? {
                    DriveTransportPoll::Complete(flushed) => {
                        if !flushed && self.connection.wants_write() {
                            break;
                        }
                    }
                    DriveTransportPoll::Pending | DriveTransportPoll::Budget => break,
                }
                if self.connection.wants_write() {
                    break;
                }
            }
            match drive.drive_protocol_op(bytes.len(), |write_len| {
                self.connection
                    .write_plaintext_some(&bytes[..write_len])
                    .map_err(|error| {
                        io::Error::new(error.kind(), format!("upstream TLS plaintext write failed: {error}"))
                    })
                    .map(|write| match write {
                        TlsPlaintextWrite::Accepted(len) => DriveProtocolOp::Progress {
                            bytes: len,
                            value: TlsPlaintextWrite::Accepted(len),
                        },
                        TlsPlaintextWrite::BlockedByPendingCiphertext => DriveProtocolOp::NoProgress {
                            value: TlsPlaintextWrite::BlockedByPendingCiphertext,
                        },
                    })
            })? {
                DriveProtocolPoll::Complete(TlsPlaintextWrite::Accepted(len)) => {
                    self.set_application_ciphertext_pending(self.connection.wants_write());
                    if !pending.advance_front(len) {
                        break;
                    }
                }
                DriveProtocolPoll::Complete(TlsPlaintextWrite::BlockedByPendingCiphertext)
                | DriveProtocolPoll::Budget => break,
            }
        }
        let plaintext_pending = !pending.is_empty();
        if self.connection.wants_write() || plaintext_pending {
            drive.wait_for_reactor_write();
        }
        self.update_interest(reactor, plaintext_pending).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("upstream TLS write interest update failed: {error}"),
            )
        })?;
        Ok(())
    }

    pub(super) fn finish_write(&mut self, reactor: &R, drive: &mut DriveTurn<'_>) -> io::Result<()> {
        if !matches!(self.write, TlsWriteState::Open { .. }) {
            return Ok(());
        }
        if self.connection.wants_write() {
            match self.flush_tls(reactor, drive) {
                Ok(DriveTransportPoll::Complete(_)) => {
                    if self.transport_wants_write() {
                        drive.wait_for_reactor_write();
                    }
                }
                Ok(DriveTransportPoll::Pending | DriveTransportPoll::Budget) => return Ok(()),
                Err(error) if is_benign_shutdown_write_error(&error, self.application_ciphertext_pending()) => {
                    self.write = TlsWriteState::Finished;
                    self.update_interest(reactor, false)?;
                    return Ok(());
                }
                Err(error) => {
                    return Err(io::Error::new(
                        error.kind(),
                        format!("upstream TLS pre-close flush failed: {error}"),
                    ));
                }
            }
            if self.connection.wants_write() {
                return Ok(());
            }
        }
        self.connection.queue_close_notify();
        self.write = if self.connection.wants_write() {
            TlsWriteState::CloseNotifyPending
        } else {
            TlsWriteState::Finished
        };
        match self.flush_tls(reactor, drive) {
            Ok(DriveTransportPoll::Complete(_)) => {
                self.write = if self.connection.wants_write() {
                    TlsWriteState::CloseNotifyPending
                } else {
                    TlsWriteState::Finished
                };
                if matches!(self.write, TlsWriteState::CloseNotifyPending) {
                    drive.wait_for_reactor_write();
                }
                Ok(())
            }
            Ok(DriveTransportPoll::Pending | DriveTransportPoll::Budget) => Ok(()),
            Err(error) if is_closed_write_error(&error) => {
                self.write = TlsWriteState::Finished;
                self.update_interest(reactor, false)?;
                Ok(())
            }
            Err(error) => Err(io::Error::new(
                error.kind(),
                format!("upstream TLS close flush failed: {error}"),
            )),
        }
    }

    pub(super) fn flush_tls(&mut self, reactor: &R, drive: &mut DriveTurn<'_>) -> io::Result<DriveTransportPoll<bool>> {
        let flushed = match self.drain_ciphertext_ready(drive, usize::MAX)? {
            DriveTransportPoll::Complete(TlsCiphertextDrain::Progress(bytes)) => bytes > 0,
            DriveTransportPoll::Complete(TlsCiphertextDrain::Blocked) | DriveTransportPoll::Pending => {
                return Ok(DriveTransportPoll::Pending);
            }
            DriveTransportPoll::Complete(TlsCiphertextDrain::Empty) => false,
            DriveTransportPoll::Budget => return Ok(DriveTransportPoll::Budget),
        };
        if matches!(self.write, TlsWriteState::Open { .. }) {
            self.set_application_ciphertext_pending(self.connection.wants_write());
        } else {
            self.write = if self.connection.wants_write() {
                TlsWriteState::CloseNotifyPending
            } else {
                TlsWriteState::Finished
            };
        }
        self.update_interest(reactor, false)
            .map(|()| DriveTransportPoll::Complete(flushed))
    }

    fn flush_pending_ciphertext(&mut self, reactor: &R, drive: &mut DriveTurn<'_>) -> io::Result<()> {
        if !self.transport_wants_write() {
            self.set_application_ciphertext_pending(false);
            return Ok(());
        }
        let flushed = match self.flush_tls(reactor, drive) {
            Ok(flushed) => flushed,
            Err(error) if !matches!(self.write, TlsWriteState::Open { .. }) && is_closed_write_error(&error) => {
                self.write = TlsWriteState::Finished;
                self.update_interest(reactor, false)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let DriveTransportPoll::Complete(_) = flushed else {
            return Ok(());
        };
        if self.transport_wants_write() {
            drive.wait_for_reactor_write();
        }
        Ok(())
    }

    pub(super) fn park_read(&mut self, reactor: &R, plaintext_write_pending: bool) -> io::Result<()> {
        let interest = if self.transport_wants_write() || plaintext_write_pending {
            ReactorInterest::Writable
        } else {
            ReactorInterest::Disabled
        };
        self.reregister_interest(reactor, interest)
    }

    pub(super) fn read_plaintext(
        &mut self,
        output: &mut [u8],
        reactor: &R,
        plaintext_write_pending: bool,
        drive: &mut DriveTurn<'_>,
    ) -> io::Result<TlsPlaintextDrive> {
        let mut outcome = match self
            .connection
            .read_plaintext_some(output)
            .map_err(|error| io::Error::new(error.kind(), format!("upstream TLS plaintext read failed: {error}")))?
        {
            TlsPlaintextRead::Plaintext(len) => TlsReadOutcome::plaintext(len, 0, false),
            TlsPlaintextRead::Closed => TlsReadOutcome::eof(0, false),
            TlsPlaintextRead::Blocked => TlsReadOutcome::blocked(false),
        };
        if outcome.state == TlsReadState::Blocked && !output.is_empty() {
            match self.read_ciphertext_ready(drive, output.len())? {
                DriveTransportPoll::Complete(TlsCiphertextRead::Read(read)) => {
                    outcome.tls_bytes_read = outcome.tls_bytes_read.saturating_add(read);
                    outcome.update_interest = true;
                    match self.connection.read_plaintext_some(output).map_err(|error| {
                        io::Error::new(error.kind(), format!("upstream TLS plaintext read failed: {error}"))
                    })? {
                        TlsPlaintextRead::Plaintext(len) => {
                            outcome.plaintext_bytes_read = len;
                            outcome.state = TlsReadState::Plaintext(len);
                        }
                        TlsPlaintextRead::Closed => {
                            outcome.eof_count = 1;
                            outcome.state = TlsReadState::Eof;
                        }
                        TlsPlaintextRead::Blocked => {}
                    }
                }
                DriveTransportPoll::Complete(TlsCiphertextRead::Blocked) | DriveTransportPoll::Pending => {
                    outcome.update_interest = true;
                }
                DriveTransportPoll::Complete(TlsCiphertextRead::Closed) => {
                    outcome.eof_count = 1;
                    outcome.state = TlsReadState::Eof;
                    outcome.update_interest = true;
                }
                DriveTransportPoll::Budget => {
                    return Ok(TlsPlaintextDrive::Budget);
                }
            }
        }
        #[cfg(any(test, feature = "simulation"))]
        {
            self.stats.tls_bytes_read = self.stats.tls_bytes_read.saturating_add(outcome.tls_bytes_read);
            self.stats.plaintext_bytes_read = self
                .stats
                .plaintext_bytes_read
                .saturating_add(outcome.plaintext_bytes_read);
            self.stats.eof_count = self.stats.eof_count.saturating_add(outcome.eof_count);
        }
        if outcome.update_interest {
            self.set_application_ciphertext_pending(self.connection.wants_write());
            self.update_interest(reactor, plaintext_write_pending)?;
        }
        match outcome.state {
            TlsReadState::Plaintext(len) => Ok(TlsPlaintextDrive::Plaintext(len)),
            TlsReadState::Eof => Ok(TlsPlaintextDrive::Eof),
            TlsReadState::Blocked => Ok(TlsPlaintextDrive::Blocked),
        }
    }

    fn update_interest(&mut self, reactor: &R, plaintext_write_pending: bool) -> io::Result<()> {
        let interest = if self.transport_wants_write() || plaintext_write_pending {
            ReactorInterest::ReadWrite
        } else {
            ReactorInterest::Readable
        };
        self.reregister_interest(reactor, interest)
    }

    fn reregister_interest(&mut self, reactor: &R, interest: ReactorInterest) -> io::Result<()> {
        self.stream.reregister(reactor, interest)
    }

    fn drain_ciphertext_ready(
        &mut self,
        drive: &mut DriveTurn<'_>,
        available_bytes: usize,
    ) -> io::Result<DriveTransportPoll<TlsCiphertextDrain>> {
        let (stream, io) = self.stream.source_and_io_mut();
        drive.transport_write_ready(io, available_bytes, |limit| {
            self.connection
                .drain_ciphertext_to(stream, limit)
                .map(|drain| match drain {
                    TlsCiphertextDrain::Progress(bytes) => DriveTransportOp::Progress { bytes, value: drain },
                    TlsCiphertextDrain::Blocked => DriveTransportOp::WouldBlock { value: drain },
                    TlsCiphertextDrain::Empty => DriveTransportOp::NoProgress { value: drain },
                })
        })
    }

    fn read_ciphertext_ready(
        &mut self,
        drive: &mut DriveTurn<'_>,
        available_bytes: usize,
    ) -> io::Result<DriveTransportPoll<TlsCiphertextRead>> {
        let (stream, io) = self.stream.source_and_io_mut();
        drive.transport_read_ready(io, available_bytes, |limit| {
            self.connection
                .read_ciphertext_bounded(stream, limit)
                .map(|read| match read {
                    TlsCiphertextRead::Read(bytes) => DriveTransportOp::Progress { bytes, value: read },
                    TlsCiphertextRead::Blocked => DriveTransportOp::WouldBlock { value: read },
                    TlsCiphertextRead::Closed => DriveTransportOp::NoProgress { value: read },
                })
        })
    }

    fn transport_wants_write(&self) -> bool {
        match self.write {
            TlsWriteState::Open { .. } => self.connection.wants_write(),
            TlsWriteState::CloseNotifyPending => true,
            TlsWriteState::Finished => false,
        }
    }

    const fn application_ciphertext_pending(&self) -> bool {
        match self.write {
            TlsWriteState::Open {
                application_ciphertext_pending,
            } => application_ciphertext_pending,
            TlsWriteState::CloseNotifyPending | TlsWriteState::Finished => false,
        }
    }

    const fn set_application_ciphertext_pending(&mut self, pending: bool) {
        if matches!(self.write, TlsWriteState::Open { .. }) {
            self.write = TlsWriteState::Open {
                application_ciphertext_pending: pending,
            };
        }
    }

    pub(super) const fn io(&self) -> crate::readiness::IoSlotState {
        self.stream.io()
    }

    pub(super) const fn mark_reactor_ready(&mut self, readable: bool, writable: bool) {
        self.stream.mark_reactor_ready(readable, writable);
    }
}

fn is_closed_write_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset | io::ErrorKind::NotConnected
    )
}

fn is_benign_shutdown_write_error(error: &io::Error, application_ciphertext_pending: bool) -> bool {
    is_closed_write_error(error) && !application_ciphertext_pending
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TlsReadOutcome {
    state: TlsReadState,
    tls_bytes_read: usize,
    plaintext_bytes_read: usize,
    eof_count: usize,
    update_interest: bool,
}

impl TlsReadOutcome {
    const fn plaintext(len: usize, tls_bytes_read: usize, update_interest: bool) -> Self {
        Self {
            state: TlsReadState::Plaintext(len),
            tls_bytes_read,
            plaintext_bytes_read: len,
            eof_count: 0,
            update_interest,
        }
    }

    const fn blocked(update_interest: bool) -> Self {
        Self {
            state: TlsReadState::Blocked,
            tls_bytes_read: 0,
            plaintext_bytes_read: 0,
            eof_count: 0,
            update_interest,
        }
    }

    const fn eof(tls_bytes_read: usize, update_interest: bool) -> Self {
        Self {
            state: TlsReadState::Eof,
            tls_bytes_read,
            plaintext_bytes_read: 0,
            eof_count: 1,
            update_interest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TlsReadState {
    Plaintext(usize),
    Eof,
    Blocked,
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::io::{self, Read, Write};
    use std::net::SocketAddr;
    use std::rc::Rc;

    use agentdp_crypto::test_support::connected_tls_pair;
    use agentdp_crypto::{TlsCiphertextRead, TlsClientSession, TlsPlaintextRead, TlsPlaintextWrite};

    use crate::buffers::{BufferPool, ByteBuf, WriteQueue};
    use crate::drive::{DriveBudget, DriveReport, DriveTurn};
    use crate::guest::{GuestIoSource, TransportError};
    use crate::network::{NetworkLimits, TcpProxyId};
    use crate::reactor::{
        ReactorBackend, ReactorInterest, ReactorItemId, ReactorReady, ReactorTcpListener, ReactorTcpStream,
        ReactorUdpSocket, ReactorWake, RegisteringTcpStream,
    };

    use super::{TlsConnectState, TlsPlaintextDrive, TlsUpstream, TlsWriteState, is_benign_shutdown_write_error};

    #[test]
    fn buffered_close_notify_is_not_blocked() {
        let (mut client, mut server) = connected_tls_pair().expect("TLS pair should connect");
        let mut ciphertext = Vec::new();
        assert_eq!(
            server
                .write_plaintext_some(b"ok")
                .expect("server should accept response plaintext"),
            TlsPlaintextWrite::Accepted(2)
        );
        server.queue_close_notify();
        let _drain = server
            .drain_ciphertext_to(&mut ciphertext, usize::MAX)
            .expect("server should serialize plaintext and close_notify");
        feed_client_ciphertext(&mut client, &ciphertext).expect("client should accept TLS records");

        let mut output = [0_u8; 16];
        assert_eq!(
            client
                .read_plaintext_some(&mut output)
                .expect("plaintext should be buffered"),
            TlsPlaintextRead::Plaintext(2)
        );
        assert_eq!(&output[..2], b"ok");
        assert_eq!(
            client
                .read_plaintext_some(&mut output)
                .expect("close_notify should be buffered"),
            TlsPlaintextRead::Closed
        );
    }

    #[test]
    fn shutdown_write_error_is_not_benign_with_pending_application_ciphertext() {
        let error = io::Error::from(io::ErrorKind::BrokenPipe);

        assert!(is_benign_shutdown_write_error(&error, false));
        assert!(!is_benign_shutdown_write_error(&error, true));
    }

    #[test]
    fn tls_read_plaintext_does_not_ingest_without_output_capacity() {
        let (client, mut server) = connected_tls_pair().expect("TLS pair should connect");
        let mut inbound_tls = Vec::new();
        assert_eq!(
            server
                .write_plaintext_some(b"response")
                .expect("server should accept response plaintext"),
            TlsPlaintextWrite::Accepted(b"response".len())
        );
        let _drain = server
            .drain_ciphertext_to(&mut inbound_tls, usize::MAX)
            .expect("server should serialize response TLS");
        let stats = CountingStreamStats::default();
        let mut upstream = test_upstream(client, CountingTlsStream::with_readable(stats.clone(), inbound_tls));
        let reactor = TestReactor;
        upstream.mark_reactor_ready(true, false);
        let mut output = [];

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (read, _report) = with_drive(&mut budget, |drive| {
            upstream.read_plaintext(&mut output, &reactor, false, drive)
        });

        assert!(matches!(
            read.expect("TLS read drive should not fail"),
            TlsPlaintextDrive::Blocked
        ));
        assert_eq!(stats.reads(), 0);
    }

    #[test]
    fn tls_write_would_block_does_not_retry_on_read_readiness_only() {
        let buffers = BufferPool::new(NetworkLimits::default());
        buffers.prewarm_instance_network();
        let (client, _server) = connected_tls_pair().expect("TLS pair should connect");
        let stats = CountingStreamStats::default();
        let mut upstream = test_upstream(client, CountingTlsStream::new(stats.clone()));
        let reactor = TestReactor;
        upstream.mark_reactor_ready(false, true);
        let mut pending = WriteQueue::new();
        pending.push(byte_buf(&buffers, b"request"));

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, _report) = with_drive(&mut budget, |drive| {
            upstream.write_pending_plaintext(&mut pending, &reactor, drive)
        });
        assert_eq!(stats.writes(), 0, "first call only accepts plaintext into rustls");

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (result, report) = with_drive(&mut budget, |drive| {
            upstream.write_pending_plaintext(&mut pending, &reactor, drive)
        });
        result.expect("TLS write drive should not fail");
        assert!(report.wait().contains(crate::drive::DriveWait::REACTOR_WRITE));
        assert_eq!(stats.writes(), 1);
        assert!(
            !upstream.io().can_write(),
            "TLS write WouldBlock must clear write readiness"
        );

        upstream.mark_reactor_ready(true, false);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_result, _report) = with_drive(&mut budget, |drive| {
            upstream.write_pending_plaintext(&mut pending, &reactor, drive)
        });
        assert_eq!(stats.writes(), 1, "read readiness must not admit TLS transport writes");
    }

    #[test]
    fn tls_read_would_block_does_not_retry_on_write_readiness_only() {
        let (client, _server) = connected_tls_pair().expect("TLS pair should connect");
        let stats = CountingStreamStats::default();
        let mut upstream = test_upstream(client, CountingTlsStream::new(stats.clone()));
        let reactor = TestReactor;
        upstream.mark_reactor_ready(true, false);
        let mut output = [0_u8; 32];

        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (read, report) = with_drive(&mut budget, |drive| {
            upstream.read_plaintext(&mut output, &reactor, false, drive)
        });
        assert!(matches!(
            read.expect("TLS read drive should not fail"),
            TlsPlaintextDrive::Blocked
        ));
        assert!(report.wait().contains(crate::drive::DriveWait::REACTOR_READ));
        assert_eq!(stats.reads(), 1);
        assert!(
            !upstream.io().can_read(),
            "TLS read WouldBlock must clear read readiness"
        );

        upstream.mark_reactor_ready(false, true);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let (_read, _report) = with_drive(&mut budget, |drive| {
            upstream.read_plaintext(&mut output, &reactor, false, drive)
        });
        assert_eq!(stats.reads(), 1, "write readiness must not admit TLS transport reads");
    }

    fn with_drive<T>(budget: &mut DriveBudget, f: impl FnOnce(&mut DriveTurn<'_>) -> T) -> (T, DriveReport) {
        let mut report = DriveReport::new();
        let result = {
            let mut drive = DriveTurn::new(budget, &mut report);
            f(&mut drive)
        };
        (result, report)
    }

    fn byte_buf(buffers: &BufferPool, bytes: &[u8]) -> ByteBuf {
        let mut output = buffers
            .try_byte_with_capacity(bytes.len())
            .expect("test byte buffer should allocate");
        output.extend_from_slice(bytes);
        output
    }

    fn test_upstream(client: TlsClientSession, stream: CountingTlsStream) -> TlsUpstream<TestReactor> {
        let mut reactor = TestReactor;
        let stream = RegisteringTcpStream::new(
            &mut reactor,
            stream,
            ReactorItemId::TcpProxy {
                proxy: TcpProxyId(9001),
            },
            ReactorInterest::ReadWrite,
        )
        .expect("test upstream stream should register")
        .commit();
        TlsUpstream {
            stream,
            connection: client,
            connect: TlsConnectState::Ready,
            write: TlsWriteState::Open {
                application_ciphertext_pending: false,
            },
            stats: super::TlsUpstreamStats::default(),
        }
    }

    fn feed_client_ciphertext(client: &mut TlsClientSession, mut ciphertext: &[u8]) -> io::Result<()> {
        while !ciphertext.is_empty() {
            let before = ciphertext.len();
            match client.read_ciphertext_bounded(&mut ciphertext, before)? {
                TlsCiphertextRead::Read(_read) => {}
                TlsCiphertextRead::Blocked => {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "client TLS blocked while consuming in-memory ciphertext",
                    ));
                }
                TlsCiphertextRead::Closed => return Err(io::ErrorKind::UnexpectedEof.into()),
            }
            if ciphertext.len() == before {
                return Err(io::ErrorKind::WriteZero.into());
            }
        }
        Ok(())
    }

    #[derive(Debug, Clone, Default)]
    struct CountingStreamStats {
        reads: Rc<Cell<usize>>,
        writes: Rc<Cell<usize>>,
    }

    impl CountingStreamStats {
        fn reads(&self) -> usize {
            self.reads.get()
        }

        fn writes(&self) -> usize {
            self.writes.get()
        }
    }

    struct CountingTlsStream {
        stats: CountingStreamStats,
        readable: Vec<u8>,
        read_offset: usize,
    }

    impl CountingTlsStream {
        fn new(stats: CountingStreamStats) -> Self {
            Self {
                stats,
                readable: Vec::new(),
                read_offset: 0,
            }
        }

        fn with_readable(stats: CountingStreamStats, readable: Vec<u8>) -> Self {
            Self {
                stats,
                readable,
                read_offset: 0,
            }
        }
    }

    impl Read for CountingTlsStream {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            self.stats.reads.set(self.stats.reads.get() + 1);
            let readable = &self.readable[self.read_offset..];
            if readable.is_empty() {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let len = output.len().min(readable.len());
            output[..len].copy_from_slice(&readable[..len]);
            self.read_offset += len;
            Ok(len)
        }
    }

    impl Write for CountingTlsStream {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            self.stats.writes.set(self.stats.writes.get() + 1);
            Err(io::ErrorKind::WouldBlock.into())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl ReactorTcpStream for CountingTlsStream {
        fn connect(_addr: SocketAddr) -> io::Result<Self> {
            Ok(Self::new(CountingStreamStats::default()))
        }

        fn set_nodelay(&self, _nodelay: bool) -> io::Result<()> {
            Ok(())
        }

        fn take_error(&self) -> io::Result<Option<io::Error>> {
            Ok(None)
        }

        fn shutdown_write(&self) -> io::Result<()> {
            Ok(())
        }
    }

    struct TestTcpListener;

    impl ReactorTcpListener for TestTcpListener {
        type Stream = CountingTlsStream;

        fn bind(_addr: SocketAddr) -> io::Result<Self> {
            Ok(Self)
        }

        fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)> {
            Err(io::ErrorKind::WouldBlock.into())
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
        }
    }

    struct TestUdpSocket;

    impl ReactorUdpSocket for TestUdpSocket {
        fn bind(_addr: SocketAddr) -> io::Result<Self> {
            Ok(Self)
        }

        fn from_std(_socket: std::net::UdpSocket) -> Self {
            Self
        }

        fn send(&self, _bytes: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::WouldBlock.into())
        }

        fn recv(&self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::ErrorKind::WouldBlock.into())
        }

        fn send_to(&self, _bytes: &[u8], _target: SocketAddr) -> io::Result<usize> {
            Err(io::ErrorKind::WouldBlock.into())
        }

        fn recv_from(&self, _buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            Err(io::ErrorKind::WouldBlock.into())
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
        }
    }

    #[derive(Debug, Clone)]
    struct TestWake;

    impl ReactorWake for TestWake {
        fn wake(&self) -> io::Result<()> {
            Ok(())
        }
    }

    struct TestReactor;

    impl ReactorBackend for TestReactor {
        type Wake = TestWake;
        type TcpListener = TestTcpListener;
        type TcpStream = CountingTlsStream;
        type UdpSocket = TestUdpSocket;

        fn wake_handle(&self) -> Self::Wake {
            TestWake
        }

        fn register_tcp_listener(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpListener,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> io::Result<()> {
            Ok(())
        }

        fn register_tcp_stream(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpStream,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> io::Result<()> {
            Ok(())
        }

        fn register_udp_socket(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> io::Result<()> {
            Ok(())
        }

        fn reregister_tcp_stream(
            &self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpStream,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> io::Result<()> {
            Ok(())
        }

        fn reregister_udp_socket(
            &self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
            _interest: ReactorInterest,
        ) -> io::Result<()> {
            Ok(())
        }

        fn deregister_tcp_listener(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpListener,
            _item: ReactorItemId,
        ) -> io::Result<()> {
            Ok(())
        }

        fn deregister_tcp_stream(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::TcpStream,
            _item: ReactorItemId,
        ) -> io::Result<()> {
            Ok(())
        }

        fn deregister_udp_socket(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: &mut Self::UdpSocket,
            _item: ReactorItemId,
        ) -> io::Result<()> {
            Ok(())
        }

        fn register_guest_source(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: GuestIoSource<'_>,
            _item: ReactorItemId,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        fn reregister_guest_source(
            &self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: GuestIoSource<'_>,
            _item: ReactorItemId,
            _writable: bool,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        fn deregister_guest_source(
            &mut self,
            _registration: crate::reactor::ReactorRegistrationToken,
            _source: GuestIoSource<'_>,
            _item: ReactorItemId,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        fn ready_into(
            &mut self,
            _output: &mut Vec<ReactorReady>,
            _timeout: Option<std::time::Duration>,
        ) -> io::Result<()> {
            Ok(())
        }
    }
}
