use std::io::{self, Read, Write};
use std::net::SocketAddr;

use agentdp_crypto::{
    TlsCiphertextDrain, TlsCiphertextRead, TlsClientConfig, TlsClientSession, TlsPlaintextRead, TlsPlaintextWrite,
};

use crate::buffers::{PumpStep, WriteQueue};
use crate::connectors::tcp::TcpConnector;
use crate::network::TcpProxyId;
use crate::reactor::ReactorItemId;
use crate::reactor::{ReactorBackend, ReactorInterest, ReactorTcpStream};
use crate::runtime::NetworkRuntime;

pub(super) struct TlsUpstream<R: ReactorBackend> {
    pub(super) stream: R::TcpStream,
    pub(super) proxy: TcpProxyId,
    pub(super) connection: TlsClientSession,
    pub(super) connect_ready: bool,
    pub(super) write_finished: bool,
    application_ciphertext_pending: bool,
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

pub(super) enum TlsDrive {
    Ready,
    Progress,
    Blocked,
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
        let mut stream = runtime.tcp_connector().connect_tcp_stream(dst)?;
        runtime.reactor_mut().register_tcp_stream(
            &mut stream,
            ReactorItemId::TcpProxy { proxy },
            ReactorInterest::ReadWrite,
        )?;
        let connection = TlsClientSession::connect(client_config, server_name).map_err(io::Error::other)?;
        Ok(Self {
            stream,
            proxy,
            connection,
            connect_ready: false,
            write_finished: false,
            application_ciphertext_pending: false,
            #[cfg(any(test, feature = "simulation"))]
            stats: TlsUpstreamStats::default(),
        })
    }

    #[cfg(test)]
    pub(super) const fn is_connect_ready(&self) -> bool {
        self.connect_ready
    }

    pub(super) const fn write_finished(&self) -> bool {
        self.write_finished
    }

    pub(super) const fn mark_connect_ready(&mut self) {
        self.connect_ready = true;
    }

    pub(super) fn deregister(&mut self, reactor: &mut R) {
        let _deregistered =
            reactor.deregister_tcp_stream(&mut self.stream, ReactorItemId::TcpProxy { proxy: self.proxy });
    }

    pub(super) fn drive_handshake(&mut self, reactor: &R) -> io::Result<TlsDrive> {
        if !self.connect_ready {
            return Ok(TlsDrive::Blocked);
        }
        if let Some(error) = self.stream.take_error()? {
            return Err(error);
        }
        let mut made_progress = false;
        while self.connection.is_handshaking() || self.connection.wants_write() {
            // Keep handshake IO phased explicitly. `complete_io` can read and write in one call,
            // which makes it too easy to ingest TLS bytes without a caller-visible drain point.
            let mut step_progress = false;
            match flush_bounded_tls_output(&mut self.connection, &mut self.stream) {
                Ok(flushed) => {
                    step_progress |= flushed;
                    made_progress |= flushed;
                }
                Err(error) => {
                    return Err(io::Error::new(
                        error.kind(),
                        format!("upstream TLS handshake write failed: {error}"),
                    ));
                }
            }
            if self.connection.is_handshaking() {
                match self
                    .connection
                    .read_ciphertext_bounded(&mut self.stream, TLS_HANDSHAKE_READ_CHUNK_BYTES)
                {
                    Ok(TlsCiphertextRead::Read(_read)) => {
                        step_progress = true;
                        made_progress = true;
                    }
                    Ok(TlsCiphertextRead::Blocked) => {}
                    Ok(TlsCiphertextRead::Closed) => {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "upstream TLS handshake closed before completion",
                        ));
                    }
                    Err(error) => {
                        return Err(io::Error::new(
                            error.kind(),
                            format!("upstream TLS handshake read failed: {error}"),
                        ));
                    }
                }
            }
            if !step_progress {
                self.update_interest(reactor)?;
                return Ok(if made_progress {
                    TlsDrive::Progress
                } else {
                    TlsDrive::Blocked
                });
            }
        }
        self.update_interest(reactor)?;
        Ok(TlsDrive::Ready)
    }

    pub(super) fn write_pending_plaintext(&mut self, pending: &mut WriteQueue, reactor: &R) -> io::Result<PumpStep> {
        if self.write_finished {
            return if pending.is_empty() {
                Ok(PumpStep::Blocked)
            } else {
                Err(io::ErrorKind::BrokenPipe.into())
            };
        }
        if pending.is_empty() && !self.application_ciphertext_pending {
            return Ok(PumpStep::Blocked);
        }
        let mut made_progress = false;
        while let Some(bytes) = pending.front_slice() {
            match write_bounded_tls_plaintext(&mut self.connection, &mut self.stream, bytes).map_err(|error| {
                io::Error::new(error.kind(), format!("upstream TLS plaintext write failed: {error}"))
            })? {
                TlsPlaintextWrite::Accepted(len) => {
                    made_progress = true;
                    self.application_ciphertext_pending = self.connection.wants_write();
                    if !pending.advance_front(len) {
                        break;
                    }
                }
                TlsPlaintextWrite::BlockedByPendingCiphertext => break,
            }
        }
        let flushed = if self.application_ciphertext_pending {
            let flushed = flush_bounded_tls_output(&mut self.connection, &mut self.stream)?;
            self.application_ciphertext_pending = self.connection.wants_write();
            flushed
        } else {
            false
        };
        self.update_interest(reactor).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("upstream TLS write interest update failed: {error}"),
            )
        })?;
        Ok(if made_progress || flushed {
            PumpStep::Progress
        } else {
            PumpStep::Blocked
        })
    }

    pub(super) fn finish_write(&mut self, reactor: &R) -> io::Result<()> {
        if self.write_finished {
            return Ok(());
        }
        if self.connection.wants_write() {
            match self.flush_tls(reactor) {
                Ok(()) => {
                    self.application_ciphertext_pending =
                        self.application_ciphertext_pending && self.connection.wants_write();
                }
                Err(error) if is_benign_shutdown_write_error(&error, self.application_ciphertext_pending) => {
                    self.write_finished = true;
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
        self.write_finished = true;
        self.connection.queue_close_notify();
        match self.flush_tls(reactor) {
            Ok(()) => Ok(()),
            Err(error) if is_benign_shutdown_write_error(&error, self.application_ciphertext_pending) => Ok(()),
            Err(error) => Err(io::Error::new(
                error.kind(),
                format!("upstream TLS close flush failed: {error}"),
            )),
        }
    }

    pub(super) fn flush_tls(&mut self, reactor: &R) -> io::Result<()> {
        let _flushed = flush_bounded_tls_output(&mut self.connection, &mut self.stream)?;
        self.update_interest(reactor)
    }

    pub(super) fn read_plaintext(&mut self, output: &mut [u8], reactor: &R) -> io::Result<usize> {
        let outcome = read_bounded_tls_plaintext(&mut self.connection, &mut self.stream, output)
            .map_err(|error| io::Error::new(error.kind(), format!("upstream TLS plaintext read failed: {error}")))?;
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
            self.update_interest(reactor)?;
        }
        match outcome.state {
            TlsReadState::Plaintext(len) => Ok(len),
            TlsReadState::Eof => Ok(0),
            TlsReadState::Blocked => Err(io::ErrorKind::WouldBlock.into()),
        }
    }

    fn update_interest(&mut self, reactor: &R) -> io::Result<()> {
        let interest = if self.connection.wants_write() || self.connection.is_handshaking() {
            ReactorInterest::ReadWrite
        } else {
            ReactorInterest::Readable
        };
        reactor.reregister_tcp_stream(
            &mut self.stream,
            ReactorItemId::TcpProxy { proxy: self.proxy },
            interest,
        )
    }
}

pub(super) fn write_bounded_tls_plaintext(
    connection: &mut TlsClientSession,
    stream: &mut impl Write,
    bytes: &[u8],
) -> io::Result<TlsPlaintextWrite> {
    // The caller's WriteQueue owns application backpressure. Do not accept more
    // plaintext into rustls while previous TLS records are still transport-blocked.
    if connection.wants_write() {
        let _flushed = flush_bounded_tls_output(connection, stream)?;
        if connection.wants_write() {
            return Ok(TlsPlaintextWrite::BlockedByPendingCiphertext);
        }
    }
    let written = connection.write_plaintext_some(bytes)?;
    let _flushed = flush_bounded_tls_output(connection, stream)?;
    Ok(written)
}

fn flush_bounded_tls_output(connection: &mut TlsClientSession, stream: &mut impl Write) -> io::Result<bool> {
    match connection.drain_ciphertext_to(stream)? {
        TlsCiphertextDrain::Progress => Ok(true),
        TlsCiphertextDrain::Blocked | TlsCiphertextDrain::Empty => Ok(false),
    }
}

fn is_closed_write_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset | io::ErrorKind::NotConnected
    )
}

pub(super) fn is_benign_shutdown_write_error(error: &io::Error, application_ciphertext_pending: bool) -> bool {
    is_closed_write_error(error) && !application_ciphertext_pending
}

pub(super) fn read_bounded_tls_plaintext(
    connection: &mut TlsClientSession,
    stream: &mut impl Read,
    output: &mut [u8],
) -> io::Result<TlsReadOutcome> {
    match read_buffered_plaintext(connection, output)? {
        BufferedPlaintext::Plaintext(len) => return Ok(TlsReadOutcome::plaintext(len, 0, false)),
        BufferedPlaintext::Closed => return Ok(TlsReadOutcome::eof(0, false)),
        BufferedPlaintext::Blocked => {}
    }
    let mut outcome = TlsReadOutcome::blocked(false);
    let mut remaining_tls_read = bounded_tls_read_limit(output.len());
    while remaining_tls_read > 0 {
        match connection.read_ciphertext_bounded(stream, remaining_tls_read) {
            Ok(TlsCiphertextRead::Closed) => {
                outcome.eof_count = 1;
                outcome.state = TlsReadState::Eof;
                outcome.update_interest = true;
                return Ok(outcome);
            }
            Ok(TlsCiphertextRead::Read(read)) => {
                outcome.tls_bytes_read = outcome.tls_bytes_read.saturating_add(read);
                outcome.update_interest = true;
                remaining_tls_read = remaining_tls_read.saturating_sub(read);
                match read_buffered_plaintext(connection, output)? {
                    BufferedPlaintext::Plaintext(len) => {
                        outcome.plaintext_bytes_read = len;
                        outcome.state = TlsReadState::Plaintext(len);
                        return Ok(outcome);
                    }
                    BufferedPlaintext::Closed => {
                        outcome.eof_count = 1;
                        outcome.state = TlsReadState::Eof;
                        return Ok(outcome);
                    }
                    BufferedPlaintext::Blocked => {}
                }
            }
            Ok(TlsCiphertextRead::Blocked) => {
                outcome.update_interest = true;
                match read_buffered_plaintext(connection, output)? {
                    BufferedPlaintext::Plaintext(len) => {
                        outcome.plaintext_bytes_read = len;
                        outcome.state = TlsReadState::Plaintext(len);
                        return Ok(outcome);
                    }
                    BufferedPlaintext::Closed => {
                        outcome.eof_count = 1;
                        outcome.state = TlsReadState::Eof;
                    }
                    BufferedPlaintext::Blocked => {}
                }
                return Ok(outcome);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(outcome)
}

fn read_buffered_plaintext(connection: &mut TlsClientSession, output: &mut [u8]) -> io::Result<BufferedPlaintext> {
    match connection.read_plaintext_some(output)? {
        TlsPlaintextRead::Plaintext(len) => Ok(BufferedPlaintext::Plaintext(len)),
        TlsPlaintextRead::Blocked => Ok(BufferedPlaintext::Blocked),
        TlsPlaintextRead::Closed => Ok(BufferedPlaintext::Closed),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BufferedPlaintext {
    Plaintext(usize),
    Blocked,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TlsReadOutcome {
    pub(super) state: TlsReadState,
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
pub(super) enum TlsReadState {
    Plaintext(usize),
    Eof,
    Blocked,
}

fn bounded_tls_read_limit(plaintext_capacity: usize) -> usize {
    if plaintext_capacity == 0 {
        0
    } else {
        plaintext_capacity.saturating_add(2048).max(2048)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use agentdp_crypto::test_support::connected_tls_pair;
    use agentdp_crypto::{TlsCiphertextRead, TlsClientSession, TlsPlaintextWrite};

    use super::{BufferedPlaintext, read_buffered_plaintext};

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
            .drain_ciphertext_to(&mut ciphertext)
            .expect("server should serialize plaintext and close_notify");
        feed_client_ciphertext(&mut client, &ciphertext).expect("client should accept TLS records");

        let mut output = [0_u8; 16];
        assert_eq!(
            read_buffered_plaintext(&mut client, &mut output).expect("plaintext should be buffered"),
            BufferedPlaintext::Plaintext(2)
        );
        assert_eq!(&output[..2], b"ok");
        assert_eq!(
            read_buffered_plaintext(&mut client, &mut output).expect("close_notify should be buffered"),
            BufferedPlaintext::Closed
        );
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
}
