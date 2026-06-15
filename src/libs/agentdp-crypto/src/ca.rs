use std::time::{Duration, SystemTime};

use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use thiserror::Error;
use time::OffsetDateTime;

use crate::provider::install_default_provider;
use crate::tls::TlsServerConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertificateAuthorityPem {
    pub cert_pem: String,
    pub key_pem: String,
}

impl CertificateAuthorityPem {
    /// # Errors
    ///
    /// Returns an error when key or certificate generation fails.
    pub fn generate() -> Result<Self, CertificateAuthorityError> {
        let key = KeyPair::generate()?;
        let cert = ca_params().self_signed(&key)?;
        Ok(Self {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
        })
    }
}

#[derive(Debug, Error)]
pub enum CertificateAuthorityError {
    #[error("failed to generate TLS certificate authority: {0}")]
    Generate(#[from] rcgen::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CertificateValidity {
    pub not_before: SystemTime,
    pub not_after: SystemTime,
}

impl CertificateValidity {
    #[must_use]
    pub fn valid_for(duration: Duration, refresh_margin: Duration) -> Self {
        let now = SystemTime::now();
        Self {
            not_before: now.checked_sub(refresh_margin).unwrap_or(SystemTime::UNIX_EPOCH),
            not_after: now + duration,
        }
    }
}

pub struct CertificateAuthority {
    issuer: Issuer<'static, KeyPair>,
    cert_der: CertificateDer<'static>,
}

impl CertificateAuthority {
    /// # Errors
    ///
    /// Returns an error when the PEM-encoded CA material is malformed.
    pub fn load(cert_pem: &str, key_pem: &str) -> Result<Self, std::io::Error> {
        install_default_provider();
        let key = KeyPair::from_pem(key_pem).map_err(std::io::Error::other)?;
        let cert_der = CertificateDer::from_pem_slice(cert_pem.as_bytes()).map_err(std::io::Error::other)?;
        let issuer = Issuer::new(ca_params(), key);
        Ok(Self { issuer, cert_der })
    }

    /// # Errors
    ///
    /// Returns an error when certificate generation or TLS config construction fails.
    pub fn server_config_for_host(
        &self,
        host: &str,
        validity: CertificateValidity,
    ) -> Result<TlsServerConfig, std::io::Error> {
        let mut params = CertificateParams::new(vec![host.to_owned()]).map_err(std::io::Error::other)?;
        let mut dn = rcgen::DistinguishedName::new();
        dn.push(rcgen::DnType::CommonName, host);
        params.distinguished_name = dn;
        params.is_ca = IsCa::ExplicitNoCa;
        params.use_authority_key_identifier_extension = true;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::KeyEncipherment];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.not_before = offset_date_time(validity.not_before);
        params.not_after = offset_date_time(validity.not_after);

        let key = KeyPair::generate().map_err(std::io::Error::other)?;
        let cert = params.signed_by(&key, &self.issuer).map_err(std::io::Error::other)?;
        let chain = vec![CertificateDer::from(cert.der().to_vec()), self.cert_der.clone()];
        let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));
        TlsServerConfig::from_single_cert(chain, private_key)
    }
}

fn offset_date_time(time: SystemTime) -> OffsetDateTime {
    OffsetDateTime::from(time)
}

pub(crate) fn ca_params() -> CertificateParams {
    let mut params = CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String("agentdp TLS CA".to_owned()),
    );
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{CertificateAuthority, CertificateAuthorityPem, CertificateValidity, ca_params};

    #[test]
    fn generates_pem_encoded_ca_material() -> Result<(), Box<dyn std::error::Error>> {
        let ca = CertificateAuthorityPem::generate()?;

        assert!(ca.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(ca.key_pem.contains("BEGIN PRIVATE KEY"));
        Ok(())
    }

    #[test]
    fn ca_params_are_ca_capable() {
        let params = ca_params();

        assert!(matches!(params.is_ca, rcgen::IsCa::Ca(_)));
        assert!(params.key_usages.contains(&rcgen::KeyUsagePurpose::KeyCertSign));
    }

    #[test]
    fn ca_signs_server_config() -> Result<(), Box<dyn std::error::Error>> {
        let ca = CertificateAuthorityPem::generate()?;
        let ca = CertificateAuthority::load(&ca.cert_pem, &ca.key_pem)?;

        let _config = ca.server_config_for_host(
            "allowed.test",
            CertificateValidity::valid_for(Duration::from_hours(1), Duration::from_mins(1)),
        )?;
        Ok(())
    }
}
