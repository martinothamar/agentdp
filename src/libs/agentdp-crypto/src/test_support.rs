use std::time::Duration;

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::{
    CertificateAuthority, CertificateAuthorityPem, CertificateValidity, TlsCiphertextRead, TlsClientConfig,
    TlsClientSession, TlsServerConfig, TlsServerSession,
};

/// Creates a connected client/server TLS session pair for tests.
///
/// # Errors
///
/// Returns an error if certificate generation, TLS configuration, session creation, or the
/// handshake transfer fails.
pub fn connected_tls_pair() -> Result<(TlsClientSession, TlsServerSession), Box<dyn std::error::Error>> {
    let ca_pem = CertificateAuthorityPem::generate()?;
    let ca = CertificateAuthority::load(&ca_pem.cert_pem, &ca_pem.key_pem)?;
    let server_config = ca.server_config_for_host(
        "allowed.test",
        CertificateValidity::valid_for(Duration::from_hours(1), Duration::from_mins(1)),
    )?;
    let client_config = TlsClientConfig::with_platform_roots(&[ca_pem.cert_pem])?;
    let mut client = TlsClientSession::connect(&client_config, "allowed.test")?;
    let mut server = TlsServerSession::accept(&server_config)?;

    for _step in 0..64 {
        transfer_client_to_server(&mut client, &mut server)?;
        transfer_server_to_client(&mut server, &mut client)?;
        if !client.is_handshaking() && !server.is_handshaking() && !client.wants_write() && !server.wants_write() {
            return Ok((client, server));
        }
    }

    Err("TLS handshake did not complete".into())
}

/// Builds a test client config that trusts one PEM-encoded root certificate.
///
/// # Errors
///
/// Returns an error if the root certificate PEM is malformed.
pub fn tls_client_config_with_root_pem(root_ca_pem: &str) -> std::io::Result<TlsClientConfig> {
    TlsClientConfig::with_platform_roots(&[root_ca_pem.to_owned()])
}

/// Builds a test server config from PEM-encoded certificate chain and private key material.
///
/// # Errors
///
/// Returns an error if the PEM material is malformed or rustls rejects the server config.
pub fn tls_server_config_from_pem(cert_pem: &str, key_pem: &str) -> std::io::Result<TlsServerConfig> {
    let cert = CertificateDer::from_pem_slice(cert_pem.as_bytes()).map_err(std::io::Error::other)?;
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).map_err(std::io::Error::other)?;
    TlsServerConfig::from_single_cert(vec![cert], key)
}

fn transfer_client_to_server(client: &mut TlsClientSession, server: &mut TlsServerSession) -> std::io::Result<()> {
    let mut ciphertext = Vec::new();
    let _drain = client.drain_ciphertext_to(&mut ciphertext, usize::MAX)?;
    feed_server_ciphertext(server, &ciphertext)
}

fn transfer_server_to_client(server: &mut TlsServerSession, client: &mut TlsClientSession) -> std::io::Result<()> {
    let mut ciphertext = Vec::new();
    let _drain = server.drain_ciphertext_to(&mut ciphertext, usize::MAX)?;
    let mut remaining = ciphertext.as_slice();
    while !remaining.is_empty() {
        let limit = remaining.len();
        match client.read_ciphertext_bounded(&mut remaining, limit)? {
            TlsCiphertextRead::Read(_read) => {}
            TlsCiphertextRead::Blocked => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "client TLS blocked while consuming in-memory ciphertext",
                ));
            }
            TlsCiphertextRead::Closed => return Err(std::io::ErrorKind::UnexpectedEof.into()),
        }
    }
    Ok(())
}

/// Feeds serialized TLS records into a server session until all bytes are consumed or rustls blocks.
///
/// # Errors
///
/// Returns an error if rustls rejects the ciphertext or reports that the TLS stream closed.
pub fn feed_server_ciphertext(server: &mut TlsServerSession, mut ciphertext: &[u8]) -> std::io::Result<()> {
    while !ciphertext.is_empty() {
        match server.accept_ciphertext_bounded(ciphertext)? {
            TlsCiphertextRead::Read(read) => {
                ciphertext = &ciphertext[read..];
            }
            TlsCiphertextRead::Blocked => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "server TLS blocked while consuming in-memory ciphertext",
                ));
            }
            TlsCiphertextRead::Closed => return Err(std::io::ErrorKind::UnexpectedEof.into()),
        }
    }
    Ok(())
}
