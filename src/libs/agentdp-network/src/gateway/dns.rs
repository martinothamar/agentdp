use std::net::IpAddr;
use std::time::{Duration, Instant};

use crate::application::{DnsQuestion, dns_address_records, dns_question};
use crate::clock::NetworkClock;
use crate::network::NetworkLimits;
use crate::policy::{Authority, EgressPolicy, normalized_host};

pub(super) const fn dns_attribution_capacity(limits: &NetworkLimits) -> usize {
    limits
        .tcp_proxy_limit
        .saturating_add(limits.udp_proxy_limit)
        .saturating_add(limits.ingress_udp_peer_limit)
}

#[derive(Debug)]
pub(super) struct DnsAttribution {
    pending: Vec<DnsQuestion>,
    records: Vec<DnsAttributionRecord>,
    capacity: usize,
}

#[derive(Debug)]
struct DnsAttributionRecord {
    address: IpAddr,
    host: String,
    expires: Instant,
}

impl DnsAttribution {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            pending: Vec::with_capacity(capacity),
            records: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub(super) fn record_query(&mut self, payload: &[u8]) {
        let Some(question) = dns_question(payload) else {
            return;
        };
        if self.pending.contains(&question) {
            return;
        }
        if self.pending.len() < self.capacity {
            self.pending.push(question);
        }
    }

    pub(super) fn record_response(&mut self, payload: &[u8], clock: &impl NetworkClock) {
        let Some(question) = dns_question(payload) else {
            return;
        };
        let Some(index) = self.pending.iter().position(|pending| pending == &question) else {
            return;
        };
        let question = self.pending.remove(index);
        let records = dns_address_records(payload);
        self.record(&question.host, records.addresses, records.ttl, clock);
    }

    pub(super) fn record(&mut self, host: &str, addresses: Vec<IpAddr>, ttl: Duration, clock: &impl NetworkClock) {
        let expires = clock.now() + ttl.max(Duration::from_secs(1));
        let host = normalized_host(host);
        for address in addresses {
            if let Some(record) = self
                .records
                .iter_mut()
                .find(|record| record.address == address && record.host == host)
            {
                record.expires = expires;
                continue;
            }
            if self.records.len() < self.capacity {
                self.records.push(DnsAttributionRecord {
                    address,
                    host: host.clone(),
                    expires,
                });
            }
        }
    }

    pub(super) fn hosts_for_ip(&mut self, address: IpAddr, clock: &impl NetworkClock) -> Vec<String> {
        let now = clock.now();
        self.records.retain(|record| record.expires > now);
        self.records
            .iter()
            .filter(|record| record.address == address)
            .map(|record| record.host.clone())
            .collect()
    }

    pub(super) fn has_allowed_authority_for_ip(
        &mut self,
        address: IpAddr,
        policy: &EgressPolicy,
        clock: &impl NetworkClock,
    ) -> bool {
        if !policy.restricts_authorities() {
            return true;
        }
        let now = clock.now();
        self.records.retain(|record| record.expires > now);
        self.records
            .iter()
            .filter(|record| record.address == address)
            .map(|record| Authority::new(&record.host))
            .any(|authority| policy.allows_authority(&authority))
    }
}
