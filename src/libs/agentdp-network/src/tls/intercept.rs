use std::io;
use std::time::{Duration, SystemTime};

use agentdp_crypto::{CertificateAuthority, CertificateValidity, TlsClientConfig, TlsServerConfig};

use crate::clock::NetworkClock;
use crate::network::{ApplicationPolicy, BlockReason, EgressDecision, TlsEgressPolicy};
use crate::policy::{Authority, NetworkPolicy};

const CERT_VALIDITY: Duration = Duration::from_hours(24);
const CERT_REFRESH_MARGIN: Duration = Duration::from_mins(5);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsInterceptConfig {
    pub ca_cert_pem: String,
    pub ca_key_pem: String,
    pub upstream_root_ca_pems: Vec<String>,
    pub intercepted_ports: Vec<u16>,
    pub bypass_hosts: Vec<String>,
}

impl TlsInterceptConfig {
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        !self.ca_cert_pem.is_empty() && !self.ca_key_pem.is_empty()
    }

    #[must_use]
    pub fn intercepts_port(&self, port: u16) -> bool {
        self.intercepted_ports.contains(&port)
    }
}

pub(crate) struct TlsIntercept {
    config: TlsInterceptConfig,
    state: Option<TlsInterceptState>,
}

struct TlsInterceptState {
    ca: CertificateAuthority,
    client_config: TlsClientConfig,
    bypass_hosts: Vec<String>,
    certs: Vec<(Authority, CachedServerCert)>,
}

struct CachedServerCert {
    expires_at: SystemTime,
    server_config: TlsServerConfig,
}

impl TlsIntercept {
    pub(crate) const fn new(config: TlsInterceptConfig) -> Self {
        Self { config, state: None }
    }

    pub(crate) fn intercepts_port(&self, port: u16) -> bool {
        self.config.is_enabled() && self.config.intercepts_port(port)
    }

    pub(crate) fn tls_egress_policy(
        &mut self,
        dst: std::net::SocketAddr,
        authorities: Vec<Authority>,
        policy: &NetworkPolicy,
        clock: &impl NetworkClock,
    ) -> io::Result<TlsEgressPolicy> {
        let state = self.state()?;
        let mut decisions = Vec::with_capacity(authorities.len());
        let mut server_configs = Vec::with_capacity(authorities.len());
        for authority in authorities {
            if policy.egress.restricts_authorities() && !policy.egress.allows_authority(&authority) {
                continue;
            }
            upsert_authority_value(
                &mut server_configs,
                authority.clone(),
                state.server_config_for(&authority, clock)?,
            );
            upsert_authority_value(
                &mut decisions,
                authority.clone(),
                EgressDecision {
                    application: ApplicationPolicy::Http1 {
                        secrets: policy.secrets.clone(),
                        authority,
                    },
                },
            );
        }
        Ok(TlsEgressPolicy {
            dst,
            client_config: state.client_config.clone(),
            bypass_hosts: state.bypass_hosts.clone(),
            server_configs,
            decisions,
            fallback: tls_fallback_decision(policy),
        })
    }

    fn state(&mut self) -> io::Result<&mut TlsInterceptState> {
        if self.state.is_none() {
            self.state = Some(TlsInterceptState::new(&self.config)?);
        }
        self.state
            .as_mut()
            .ok_or_else(|| io::Error::other("TLS intercept state was not initialized"))
    }
}

impl TlsInterceptState {
    fn new(config: &TlsInterceptConfig) -> io::Result<Self> {
        let ca = CertificateAuthority::load(&config.ca_cert_pem, &config.ca_key_pem)?;
        let client_config = TlsClientConfig::with_platform_roots(&config.upstream_root_ca_pems)?;
        Ok(Self {
            ca,
            client_config,
            bypass_hosts: config.bypass_hosts.iter().map(|host| normalize_host(host)).collect(),
            certs: Vec::new(),
        })
    }

    fn server_config_for(&mut self, authority: &Authority, clock: &impl NetworkClock) -> io::Result<TlsServerConfig> {
        let existing = self.certs.iter().position(|(candidate, _cert)| candidate == authority);
        if let Some(index) = existing
            && let Some((_authority, cert)) = self.certs.get(index)
            && cert
                .expires_at
                .duration_since(clock.system_time())
                .is_ok_and(|remaining| remaining > CERT_REFRESH_MARGIN)
        {
            return Ok(cert.server_config.clone());
        }
        let cert = generate_server_cert(authority.as_str(), &self.ca)?;
        let server_config = cert.server_config.clone();
        if let Some(index) = existing {
            self.certs[index] = (authority.clone(), cert);
        } else {
            self.certs.push((authority.clone(), cert));
        }
        Ok(server_config)
    }
}

fn upsert_authority_value<T>(entries: &mut Vec<(Authority, T)>, authority: Authority, value: T) {
    if let Some((_authority, existing)) = entries.iter_mut().find(|(candidate, _value)| candidate == &authority) {
        *existing = value;
        return;
    }
    entries.push((authority, value));
}

fn tls_fallback_decision(policy: &NetworkPolicy) -> EgressDecision {
    let application = if policy.egress.restricts_authorities() {
        ApplicationPolicy::Block {
            reason: BlockReason::AuthorityNotAllowed,
        }
    } else {
        ApplicationPolicy::Raw
    };
    EgressDecision { application }
}

fn generate_server_cert(host: &str, ca: &CertificateAuthority) -> io::Result<CachedServerCert> {
    let validity = CertificateValidity::valid_for(CERT_VALIDITY, CERT_REFRESH_MARGIN);
    let server_config = ca.server_config_for_host(host, validity)?;
    Ok(CachedServerCert {
        expires_at: validity.not_after,
        server_config,
    })
}

fn normalize_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use crate::clock::SystemClock;
    use crate::network::{ApplicationPolicy, BlockReason};
    use crate::policy::{Authority, EgressPolicy, NetworkPolicy};
    use agentdp_crypto::CertificateAuthorityPem;

    use super::{TlsIntercept, TlsInterceptConfig, normalize_host};

    #[test]
    fn config_requires_ca_material_and_matching_port() {
        let mut config = config("", "");
        config.intercepted_ports = vec![443];

        assert!(!config.is_enabled());
        assert!(config.intercepts_port(443));
        assert!(!TlsIntercept::new(config).intercepts_port(443));
    }

    #[test]
    fn tls_egress_policy_builds_configs_for_allowed_authorities_and_blocks_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let ca = CertificateAuthorityPem::generate()?;
        let mut intercept = TlsIntercept::new(config(&ca.cert_pem, &ca.key_pem));
        let policy = NetworkPolicy::new(EgressPolicy::allow_all().with_allowed_authority("allowed.test"));

        let egress = intercept.tls_egress_policy(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443),
            vec![Authority::new("allowed.test"), Authority::new("blocked.test")],
            &policy,
            &SystemClock,
        )?;

        assert_eq!(egress.server_configs.len(), 1);
        assert!(egress.server_config_for(&Authority::new("allowed.test")).is_some());
        assert!(egress.decision_for(&Authority::new("allowed.test")).is_some());
        assert_eq!(egress.bypass_hosts, vec!["bypass.test"]);
        assert!(matches!(
            egress.fallback.application,
            ApplicationPolicy::Block {
                reason: BlockReason::AuthorityNotAllowed
            }
        ));
        Ok(())
    }

    #[test]
    fn server_certificate_config_is_cached() -> Result<(), Box<dyn std::error::Error>> {
        let ca = CertificateAuthorityPem::generate()?;
        let mut intercept = TlsIntercept::new(config(&ca.cert_pem, &ca.key_pem));
        let policy = NetworkPolicy::new(EgressPolicy::allow_all());
        let authority = Authority::new("allowed.test");

        let first = intercept.tls_egress_policy(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443),
            vec![authority.clone()],
            &policy,
            &SystemClock,
        )?;
        let second = intercept.tls_egress_policy(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443),
            vec![authority.clone()],
            &policy,
            &SystemClock,
        )?;

        assert!(first.server_config_for(&authority).is_some());
        assert!(second.server_config_for(&authority).is_some());
        assert_eq!(intercept.state.as_ref().map(|state| state.certs.len()), Some(1));
        assert!(matches!(second.fallback.application, ApplicationPolicy::Raw));
        Ok(())
    }

    #[test]
    fn invalid_ca_material_is_reported() {
        let mut intercept = TlsIntercept::new(config("not a cert", "not a key"));
        let error = intercept.tls_egress_policy(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443),
            vec![Authority::new("allowed.test")],
            &NetworkPolicy::new(EgressPolicy::allow_all()),
            &SystemClock,
        );

        assert!(error.is_err());
    }

    #[test]
    fn normalizes_bypass_hosts() {
        assert_eq!(normalize_host("Bypass.TEST."), "bypass.test");
    }

    fn config(cert: &str, key: &str) -> TlsInterceptConfig {
        TlsInterceptConfig {
            ca_cert_pem: cert.to_owned(),
            ca_key_pem: key.to_owned(),
            upstream_root_ca_pems: Vec::new(),
            intercepted_ports: vec![443],
            bypass_hosts: vec!["Bypass.TEST.".to_owned()],
        }
    }
}
