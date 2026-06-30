use std::collections::BTreeSet;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use smoltcp::wire::Ipv4Address;
use thiserror::Error;

const CLOUD_METADATA_IP: Ipv4Address = Ipv4Address::new(169, 254, 169, 254);

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("egress destination {0} is blocked by default policy")]
    BlockedDestination(IpAddr),
    #[error("egress destination {destination} is not in the allowed host set; resolved host: {host}")]
    HostNotAllowed { destination: IpAddr, host: String },
    #[error("egress host `{0}` is not allowed for mediated secret substitution")]
    UnauthorizedSecretHost(String),
    #[error("unresolved mediated secret placeholder was present in egress payload")]
    UnresolvedSecretPlaceholder,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct EgressPolicy {
    allowed_authorities: BTreeSet<String>,
    allow_private_destinations: bool,
}

impl EgressPolicy {
    #[must_use]
    pub const fn default_deny_private() -> Self {
        Self {
            allowed_authorities: BTreeSet::new(),
            allow_private_destinations: false,
        }
    }

    #[must_use]
    pub const fn allow_all() -> Self {
        Self {
            allowed_authorities: BTreeSet::new(),
            allow_private_destinations: true,
        }
    }

    #[must_use]
    pub fn with_allowed_authority(mut self, authority: impl AsRef<str>) -> Self {
        self.allowed_authorities.insert(normalized_host(authority.as_ref()));
        self
    }

    #[must_use]
    pub fn allows_authority(&self, authority: &Authority) -> bool {
        self.allowed_authorities.contains(authority.as_str())
    }

    #[must_use]
    pub fn restricts_authorities(&self) -> bool {
        !self.allowed_authorities.is_empty()
    }

    /// # Errors
    ///
    /// Returns an error when `destination` is blocked by this policy.
    pub fn check_destination(&self, destination: IpAddr) -> Result<(), Error> {
        if !self.allow_private_destinations && blocked_destination(destination) {
            Err(Error::BlockedDestination(destination))
        } else {
            Ok(())
        }
    }

    /// # Errors
    ///
    /// Returns an error when the destination host is not allowed by this policy.
    pub fn check_destination_authority(&self, destination: IpAddr, authority: Option<&Authority>) -> Result<(), Error> {
        if !self.restricts_authorities() {
            return Ok(());
        }
        if authority.is_some_and(|authority| self.allows_authority(authority)) {
            return Ok(());
        }
        Err(Error::HostNotAllowed {
            destination,
            host: authority.map_or_else(|| "<unknown>".to_owned(), ToString::to_string),
        })
    }
}

fn blocked_destination(destination: IpAddr) -> bool {
    match destination {
        IpAddr::V4(address) => {
            let [a, b, c, d] = address.octets();
            let address = Ipv4Address::new(a, b, c, d);
            address.is_loopback()
                || address.is_private()
                || address.is_link_local()
                || address.is_multicast()
                || address.is_broadcast()
                || address.is_unspecified()
                || is_cgnat(address)
                || address == CLOUD_METADATA_IP
        }
        IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4_mapped() {
                return blocked_destination(IpAddr::V4(mapped));
            }
            address.is_loopback()
                || address.is_unicast_link_local()
                || address.is_unique_local()
                || address.is_multicast()
                || address.is_unspecified()
        }
    }
}

fn is_cgnat(address: Ipv4Address) -> bool {
    let [a, b, _, _] = address.octets();
    a == 100 && (64..=127).contains(&b)
}

pub(crate) fn normalized_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Authority(String);

impl Authority {
    #[must_use]
    pub fn new(host: impl AsRef<str>) -> Self {
        Self(normalized_host(host.as_ref()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Authority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkPolicy {
    pub egress: EgressPolicy,
    pub secrets: RuntimeSecrets,
}

impl NetworkPolicy {
    #[must_use]
    pub const fn new(egress: EgressPolicy) -> Self {
        Self {
            egress,
            secrets: RuntimeSecrets::new(),
        }
    }

    #[must_use]
    pub fn with_secrets(mut self, secrets: RuntimeSecrets) -> Self {
        self.secrets = secrets;
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeSecrets {
    bindings: Vec<RuntimeSecret>,
}

impl RuntimeSecrets {
    #[must_use]
    pub const fn new() -> Self {
        Self { bindings: Vec::new() }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    pub fn insert(&mut self, secret: RuntimeSecret) {
        self.bindings.push(secret);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &RuntimeSecret> {
        self.bindings.iter()
    }

    pub(crate) fn changed_authorities(&self, next: &Self) -> BTreeSet<Authority> {
        let mut changed = BTreeSet::new();
        for current in &self.bindings {
            if next.binding_for_placeholder(&current.placeholder) != Some(current) {
                current.add_authorities_to(&mut changed);
            }
        }
        for next in &next.bindings {
            if self.binding_for_placeholder(&next.placeholder) != Some(next) {
                next.add_authorities_to(&mut changed);
            }
        }
        changed
    }

    fn binding_for_placeholder(&self, placeholder: &str) -> Option<&RuntimeSecret> {
        self.bindings.iter().find(|secret| secret.placeholder == placeholder)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeSecret {
    pub placeholder: String,
    value: SecretValue,
    scope: SecretScope,
}

impl std::fmt::Debug for RuntimeSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeSecret")
            .field("placeholder", &self.placeholder)
            .field("value", &self.value)
            .field("scope", &self.scope)
            .finish()
    }
}

impl RuntimeSecret {
    #[must_use]
    pub fn new(
        placeholder: impl Into<String>,
        value: impl Into<String>,
        authorities: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            placeholder: placeholder.into(),
            value: SecretValue(value.into()),
            scope: SecretScope::new(authorities),
        }
    }

    #[must_use]
    pub fn allows_authority(&self, authority: &Authority) -> bool {
        self.scope.allows_authority(authority)
    }

    pub(crate) fn value(&self) -> &str {
        &self.value.0
    }

    fn add_authorities_to(&self, authorities: &mut BTreeSet<Authority>) {
        self.scope.add_authorities_to(authorities);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretScope {
    allowed_authorities: BTreeSet<Authority>,
}

impl SecretScope {
    #[must_use]
    pub fn new(authorities: impl IntoIterator<Item = String>) -> Self {
        Self {
            allowed_authorities: authorities.into_iter().map(Authority::new).collect(),
        }
    }

    #[must_use]
    pub fn allows_authority(&self, authority: &Authority) -> bool {
        self.allowed_authorities.contains(authority)
    }

    fn add_authorities_to(&self, authorities: &mut BTreeSet<Authority>) {
        authorities.extend(self.allowed_authorities.iter().cloned());
    }
}

#[derive(Clone, PartialEq, Eq)]
struct SecretValue(String);

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use proptest::prelude::*;

    use super::{Authority, EgressPolicy, Error, RuntimeSecret, RuntimeSecrets, SecretScope, normalized_host};

    #[test]
    fn default_policy_blocks_private_special_and_metadata_destinations() {
        let policy = EgressPolicy::default_deny_private();
        for destination in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
            IpAddr::V4(Ipv4Addr::BROADCAST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            IpAddr::V6(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 1)),
        ] {
            assert_eq!(
                policy.check_destination(destination),
                Err(Error::BlockedDestination(destination))
            );
        }
    }

    #[test]
    fn allow_all_policy_allows_private_destinations() {
        assert_eq!(
            EgressPolicy::allow_all().check_destination(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            Ok(())
        );
    }

    #[test]
    fn authority_allow_list_is_normalized() {
        let policy = EgressPolicy::default_deny_private().with_allowed_authority("Example.TEST.");

        assert!(policy.restricts_authorities());
        assert!(policy.allows_authority(&Authority::new("example.test")));
        assert_eq!(
            policy.check_destination_authority(
                IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
                Some(&Authority::new("blocked.test")),
            ),
            Err(Error::HostNotAllowed {
                destination: IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
                host: "blocked.test".to_owned(),
            })
        );
    }

    #[test]
    fn runtime_secret_debug_redacts_value_and_scope_normalizes_authorities() {
        let secret = RuntimeSecret::new("AGENTDP_SECRET_TOKEN", "sensitive", ["Allowed.TEST.".to_owned()]);

        assert!(secret.allows_authority(&Authority::new("allowed.test")));
        assert!(!format!("{secret:?}").contains("sensitive"));
    }

    #[test]
    fn runtime_secrets_preserve_inserted_bindings() {
        let mut secrets = RuntimeSecrets::new();
        assert!(secrets.is_empty());
        secrets.insert(RuntimeSecret::new("A", "B", ["allowed.test".to_owned()]));
        assert!(!secrets.is_empty());
        assert_eq!(secrets.iter().count(), 1);
    }

    #[test]
    fn runtime_secrets_changed_authorities_only_reports_changed_bindings() {
        let mut current = RuntimeSecrets::new();
        current.insert(RuntimeSecret::new("A", "old-a", ["a.test".to_owned()]));
        current.insert(RuntimeSecret::new("B", "same-b", ["b.test".to_owned()]));

        let mut next = RuntimeSecrets::new();
        next.insert(RuntimeSecret::new("A", "new-a", ["a.test".to_owned()]));
        next.insert(RuntimeSecret::new("B", "same-b", ["b.test".to_owned()]));
        next.insert(RuntimeSecret::new("C", "new-c", ["c.test".to_owned()]));

        let changed = current.changed_authorities(&next);

        assert!(changed.contains(&Authority::new("a.test")));
        assert!(changed.contains(&Authority::new("c.test")));
        assert!(!changed.contains(&Authority::new("b.test")));
    }

    #[test]
    fn runtime_secret_scope_normalizes_authorities() {
        let scope = SecretScope::new(["Example.TEST.".to_owned()]);
        assert!(scope.allows_authority(&Authority::new("example.test")));
    }

    proptest! {
        #[test]
        fn host_normalization_is_idempotent(host in "[A-Za-z0-9.-]{1,64}") {
            let once = normalized_host(&host);
            let twice = normalized_host(&once);
            prop_assert_eq!(once, twice);
        }
    }
}
