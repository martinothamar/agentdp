use std::io::{self, Read as _, Write as _};
use std::sync::Arc;

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use thiserror::Error;

use crate::provider::install_default_provider;

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("invalid TLS server name {name}: {source}")]
    InvalidServerName {
        name: String,
        #[source]
        source: rustls::pki_types::InvalidDnsNameError,
    },
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Clone)]
pub struct TlsClientConfig {
    inner: Arc<rustls::ClientConfig>,
}

impl TlsClientConfig {
    /// # Errors
    ///
    /// Returns an error when one of the additional root CA PEMs is malformed.
    pub fn with_platform_roots(additional_root_ca_pems: &[String]) -> io::Result<Self> {
        install_default_provider();
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        for pem in additional_root_ca_pems {
            root_store
                .add(CertificateDer::from_pem_slice(pem.as_bytes()).map_err(io::Error::other)?)
                .map_err(io::Error::other)?;
        }
        Ok(Self {
            inner: Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth(),
            ),
        })
    }
}

impl std::fmt::Debug for TlsClientConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("TlsClientConfig").finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct TlsServerConfig {
    inner: Arc<rustls::ServerConfig>,
}

struct BoundedWrite<'a> {
    inner: &'a mut dyn io::Write,
    remaining: usize,
}

impl<'a> BoundedWrite<'a> {
    const fn new(inner: &'a mut dyn io::Write, limit: usize) -> Self {
        Self {
            inner,
            remaining: limit,
        }
    }
}

impl io::Write for BoundedWrite<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let len = bytes.len().min(self.remaining);
        let written = self.inner.write(&bytes[..len])?;
        self.remaining = self.remaining.saturating_sub(written);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl TlsServerConfig {
    pub(crate) fn from_single_cert(
        chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
    ) -> io::Result<Self> {
        install_default_provider();
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, private_key)
            .map_err(io::Error::other)?;
        Ok(Self {
            inner: Arc::new(config),
        })
    }
}

impl std::fmt::Debug for TlsServerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("TlsServerConfig").finish_non_exhaustive()
    }
}

pub struct TlsClientSession {
    inner: rustls::ClientConnection,
    peer_has_closed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsPlaintextWrite {
    Accepted(usize),
    BlockedByPendingCiphertext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsPlaintextRead {
    Plaintext(usize),
    Blocked,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsCiphertextDrain {
    Progress(usize),
    Blocked,
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsCiphertextRead {
    Read(usize),
    Blocked,
    Closed,
}

impl TlsClientSession {
    /// # Errors
    ///
    /// Returns an error when the server name is invalid or rustls rejects the new session.
    pub fn connect(config: &TlsClientConfig, server_name: &str) -> Result<Self, TlsError> {
        let server_name =
            ServerName::try_from(server_name.to_owned()).map_err(|source| TlsError::InvalidServerName {
                name: server_name.to_owned(),
                source,
            })?;
        Ok(Self {
            inner: rustls::ClientConnection::new(config.inner.clone(), server_name)?,
            peer_has_closed: false,
        })
    }

    #[must_use]
    pub fn is_handshaking(&self) -> bool {
        self.inner.is_handshaking()
    }

    #[must_use]
    pub fn wants_write(&self) -> bool {
        self.inner.wants_write()
    }

    #[must_use]
    pub const fn peer_has_closed(&self) -> bool {
        self.peer_has_closed
    }

    /// Queues TLS close-notify data.
    ///
    /// The caller must keep draining ciphertext with [`Self::drain_ciphertext_to`]
    /// for the close-notify alert to reach the transport.
    pub fn queue_close_notify(&mut self) {
        self.inner.send_close_notify();
    }

    /// # Errors
    ///
    /// Returns an error when TLS bytes cannot be read or processed.
    pub fn read_ciphertext_bounded<T: io::Read>(
        &mut self,
        input: &mut T,
        limit: usize,
    ) -> io::Result<TlsCiphertextRead> {
        if limit == 0 {
            return Ok(TlsCiphertextRead::Blocked);
        }
        let mut input = input.take(u64::try_from(limit).unwrap_or(u64::MAX));
        match self.inner.read_tls(&mut input) {
            Ok(0) => Ok(TlsCiphertextRead::Closed),
            Ok(read) => {
                let state = self.inner.process_new_packets().map_err(io::Error::other)?;
                self.peer_has_closed |= state.peer_has_closed();
                Ok(TlsCiphertextRead::Read(read))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(TlsCiphertextRead::Blocked),
            Err(error) => Err(error),
        }
    }

    /// # Errors
    ///
    /// Returns an error when rustls rejects the plaintext write.
    pub fn write_plaintext_some(&mut self, bytes: &[u8]) -> io::Result<TlsPlaintextWrite> {
        if self.inner.wants_write() {
            return Ok(TlsPlaintextWrite::BlockedByPendingCiphertext);
        }
        match self.inner.writer().write(bytes) {
            Ok(0) if !bytes.is_empty() => Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => Ok(TlsPlaintextWrite::Accepted(written)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                Ok(TlsPlaintextWrite::BlockedByPendingCiphertext)
            }
            Err(error) => Err(error),
        }
    }

    /// Drains up to `limit` pending TLS ciphertext bytes into `output`.
    ///
    /// # Errors
    ///
    /// Returns an error when rustls cannot serialize pending TLS bytes to `output`.
    pub fn drain_ciphertext_to(&mut self, output: &mut dyn io::Write, limit: usize) -> io::Result<TlsCiphertextDrain> {
        if !self.inner.wants_write() {
            return Ok(TlsCiphertextDrain::Empty);
        }
        if limit == 0 {
            return Ok(TlsCiphertextDrain::Blocked);
        }
        let mut bytes_written = 0_usize;
        let mut output = BoundedWrite::new(output, limit);
        while self.inner.wants_write() {
            match self.inner.write_tls(&mut output) {
                Ok(written) if written > 0 => bytes_written = bytes_written.saturating_add(written),
                Ok(_empty) => {
                    return Ok(if bytes_written > 0 {
                        TlsCiphertextDrain::Progress(bytes_written)
                    } else {
                        TlsCiphertextDrain::Blocked
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(if bytes_written > 0 {
                        TlsCiphertextDrain::Progress(bytes_written)
                    } else {
                        TlsCiphertextDrain::Blocked
                    });
                }
                Err(error) => return Err(error),
            }
        }
        Ok(TlsCiphertextDrain::Progress(bytes_written))
    }

    /// # Errors
    ///
    /// Returns an error when the TLS stream has failed.
    pub fn read_plaintext_some(&mut self, output: &mut [u8]) -> io::Result<TlsPlaintextRead> {
        if output.is_empty() {
            return Ok(TlsPlaintextRead::Blocked);
        }
        match self.inner.reader().read(output) {
            Ok(len) if len > 0 => Ok(TlsPlaintextRead::Plaintext(len)),
            Ok(_empty) => Ok(TlsPlaintextRead::Closed),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(TlsPlaintextRead::Blocked),
            Err(error) => Err(error),
        }
    }
}

pub struct TlsServerSession {
    inner: rustls::ServerConnection,
    peer_has_closed: bool,
}

impl TlsServerSession {
    /// # Errors
    ///
    /// Returns an error when rustls rejects the new server session.
    pub fn accept(config: &TlsServerConfig) -> Result<Self, TlsError> {
        Ok(Self {
            inner: rustls::ServerConnection::new(config.inner.clone())?,
            peer_has_closed: false,
        })
    }

    #[must_use]
    pub fn is_handshaking(&self) -> bool {
        self.inner.is_handshaking()
    }

    #[must_use]
    pub fn wants_write(&self) -> bool {
        self.inner.wants_write()
    }

    #[must_use]
    pub const fn peer_has_closed(&self) -> bool {
        self.peer_has_closed
    }

    /// Queues TLS close-notify data.
    ///
    /// The caller must keep draining ciphertext with [`Self::drain_ciphertext_to`]
    /// for the close-notify alert to reach the transport.
    pub fn queue_close_notify(&mut self) {
        self.inner.send_close_notify();
    }

    /// # Errors
    ///
    /// Returns an error when the TLS bytes cannot be parsed or processed by rustls.
    pub fn accept_ciphertext_bounded(&mut self, bytes: &[u8]) -> io::Result<TlsCiphertextRead> {
        if bytes.is_empty() {
            return Ok(TlsCiphertextRead::Blocked);
        }
        let mut input = bytes;
        match self.inner.read_tls(&mut input) {
            Ok(0) => Ok(TlsCiphertextRead::Closed),
            Ok(read) => {
                let state = self.inner.process_new_packets().map_err(io::Error::other)?;
                self.peer_has_closed |= state.peer_has_closed();
                Ok(TlsCiphertextRead::Read(read))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(TlsCiphertextRead::Blocked),
            Err(error) => Err(error),
        }
    }

    /// Drains up to `limit` pending TLS ciphertext bytes into `output`.
    ///
    /// # Errors
    ///
    /// Returns an error when rustls cannot serialize pending TLS bytes to `output`.
    pub fn drain_ciphertext_to(&mut self, output: &mut dyn io::Write, limit: usize) -> io::Result<TlsCiphertextDrain> {
        if !self.inner.wants_write() {
            return Ok(TlsCiphertextDrain::Empty);
        }
        if limit == 0 {
            return Ok(TlsCiphertextDrain::Blocked);
        }
        let mut bytes_written = 0_usize;
        let mut output = BoundedWrite::new(output, limit);
        while self.inner.wants_write() {
            match self.inner.write_tls(&mut output) {
                Ok(written) if written > 0 => bytes_written = bytes_written.saturating_add(written),
                Ok(_empty) => {
                    return Ok(if bytes_written > 0 {
                        TlsCiphertextDrain::Progress(bytes_written)
                    } else {
                        TlsCiphertextDrain::Blocked
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(if bytes_written > 0 {
                        TlsCiphertextDrain::Progress(bytes_written)
                    } else {
                        TlsCiphertextDrain::Blocked
                    });
                }
                Err(error) => return Err(error),
            }
        }
        Ok(TlsCiphertextDrain::Progress(bytes_written))
    }

    /// # Errors
    ///
    /// Returns an error when the TLS stream has failed.
    pub fn read_plaintext_some(&mut self, output: &mut [u8]) -> io::Result<TlsPlaintextRead> {
        if output.is_empty() {
            return Ok(TlsPlaintextRead::Blocked);
        }
        match self.inner.reader().read(output) {
            Ok(len) if len > 0 => Ok(TlsPlaintextRead::Plaintext(len)),
            Ok(_empty) => Ok(TlsPlaintextRead::Closed),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(TlsPlaintextRead::Blocked),
            Err(error) => Err(error),
        }
    }

    /// # Errors
    ///
    /// Returns an error when rustls rejects the plaintext write.
    pub fn write_plaintext_some(&mut self, bytes: &[u8]) -> io::Result<TlsPlaintextWrite> {
        if self.inner.wants_write() {
            return Ok(TlsPlaintextWrite::BlockedByPendingCiphertext);
        }
        match self.inner.writer().write(bytes) {
            Ok(0) if !bytes.is_empty() => Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => Ok(TlsPlaintextWrite::Accepted(written)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                Ok(TlsPlaintextWrite::BlockedByPendingCiphertext)
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TlsCiphertextDrain, TlsCiphertextRead, TlsPlaintextRead, TlsPlaintextWrite};
    use crate::test_support::connected_tls_pair;

    #[test]
    fn plaintext_write_is_blocked_while_ciphertext_is_pending() -> Result<(), Box<dyn std::error::Error>> {
        let (mut client, _server) = connected_tls_pair()?;

        assert_eq!(
            client.write_plaintext_some(b"hello")?,
            TlsPlaintextWrite::Accepted(b"hello".len())
        );
        assert_eq!(
            client.write_plaintext_some(b"again")?,
            TlsPlaintextWrite::BlockedByPendingCiphertext
        );
        Ok(())
    }

    #[test]
    fn ciphertext_drain_distinguishes_blocked_from_empty() -> Result<(), Box<dyn std::error::Error>> {
        let (mut client, _server) = connected_tls_pair()?;
        assert_eq!(
            client.drain_ciphertext_to(&mut Vec::new(), usize::MAX)?,
            TlsCiphertextDrain::Empty
        );

        assert_eq!(
            client.write_plaintext_some(b"hello")?,
            TlsPlaintextWrite::Accepted(b"hello".len())
        );
        let mut blocked = CapacityWriter::new(0);
        assert_eq!(
            client.drain_ciphertext_to(&mut blocked, usize::MAX)?,
            TlsCiphertextDrain::Blocked
        );
        Ok(())
    }

    #[test]
    fn bounded_ciphertext_read_never_exceeds_limit() -> Result<(), Box<dyn std::error::Error>> {
        let (mut client, mut server) = connected_tls_pair()?;

        assert_eq!(
            server.write_plaintext_some(b"response")?,
            TlsPlaintextWrite::Accepted(b"response".len())
        );
        let mut ciphertext = Vec::new();
        assert!(matches!(
            server.drain_ciphertext_to(&mut ciphertext, usize::MAX)?,
            TlsCiphertextDrain::Progress(_)
        ));

        let first = match client.read_ciphertext_bounded(&mut ciphertext.as_slice(), 1)? {
            TlsCiphertextRead::Read(read) => read,
            other => return Err(format!("expected bounded ciphertext read, got {other:?}").into()),
        };
        assert_eq!(first, 1);
        Ok(())
    }

    #[test]
    fn plaintext_read_distinguishes_blocked_from_plaintext() -> Result<(), Box<dyn std::error::Error>> {
        let (mut client, mut server) = connected_tls_pair()?;

        let mut output = [0; 16];
        assert_eq!(client.read_plaintext_some(&mut output)?, TlsPlaintextRead::Blocked);

        assert_eq!(
            server.write_plaintext_some(b"response")?,
            TlsPlaintextWrite::Accepted(b"response".len())
        );
        let mut ciphertext = Vec::new();
        assert!(matches!(
            server.drain_ciphertext_to(&mut ciphertext, usize::MAX)?,
            TlsCiphertextDrain::Progress(_)
        ));
        assert!(matches!(
            client.read_ciphertext_bounded(&mut ciphertext.as_slice(), ciphertext.len())?,
            TlsCiphertextRead::Read(_)
        ));
        assert_eq!(
            client.read_plaintext_some(&mut output)?,
            TlsPlaintextRead::Plaintext(b"response".len())
        );
        Ok(())
    }

    #[test]
    fn peer_has_closed_tracks_received_close_notify() -> Result<(), Box<dyn std::error::Error>> {
        let (mut client, mut server) = connected_tls_pair()?;
        assert!(!client.peer_has_closed());

        server.queue_close_notify();
        let mut ciphertext = Vec::new();
        assert!(matches!(
            server.drain_ciphertext_to(&mut ciphertext, usize::MAX)?,
            TlsCiphertextDrain::Progress(_)
        ));
        assert!(matches!(
            client.read_ciphertext_bounded(&mut ciphertext.as_slice(), ciphertext.len())?,
            TlsCiphertextRead::Read(_)
        ));

        assert!(client.peer_has_closed());
        Ok(())
    }

    struct CapacityWriter {
        capacity: usize,
        len: usize,
    }

    impl CapacityWriter {
        const fn new(capacity: usize) -> Self {
            Self { capacity, len: 0 }
        }
    }

    impl std::io::Write for CapacityWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let available = self.capacity.saturating_sub(self.len);
            let written = available.min(bytes.len());
            self.len += written;
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
