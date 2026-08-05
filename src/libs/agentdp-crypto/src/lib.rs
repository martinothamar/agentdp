#![forbid(unsafe_code)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]

mod ca;
mod provider;
#[cfg(any(test, feature = "fixtures"))]
pub mod test_support;
mod tls;

pub use ca::{CertificateAuthority, CertificateAuthorityError, CertificateAuthorityPem, CertificateValidity};
pub use provider::install_default_provider;
pub use tls::{
    TlsCiphertextDrain, TlsCiphertextRead, TlsClientConfig, TlsClientSession, TlsError, TlsPlaintextRead,
    TlsPlaintextWrite, TlsServerConfig, TlsServerSession,
};
