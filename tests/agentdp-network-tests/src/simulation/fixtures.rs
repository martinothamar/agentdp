use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use agentdp_network::{EgressPolicy, InstanceNetworkConfig, NetworkPolicy, RuntimeSecrets, TlsInterceptConfig};

use super::packets::{
    ETHERTYPE_ARP, ETHERTYPE_IPV4, GATEWAY_IP, GATEWAY_MAC, GUEST_IP, GUEST_MAC, ICMP_ECHO_REPLY, IP_PROTOCOL_ICMP,
    dns_a_query, internet_checksum, read_u16, udp_datagram,
};
pub(super) use super::packets::{arp_request, icmp_echo_request};
use super::{DriveBudget, Error, GuestLink, Result, Simulator, SteppedNetwork};
use crate::case_support::{mediated_network_addresses, mediated_network_mac};

pub(super) const HOST: &str = "allowed.test";
pub(super) const BLOCKED_HOST: &str = "blocked.test";
pub(super) const BYPASS_HOST: &str = "bypass.test";
pub(super) const PLACEHOLDER: &str = "AGENTDP_SECRET_TOKEN";
pub(super) const UNKNOWN_PLACEHOLDER: &str = "AGENTDP_SECRET_UNKNOWN";
pub(super) const SECRET_VALUE: &str = "substituted-token";
pub(super) const DNS_UPSTREAM: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 53);
pub(super) const UPSTREAM_IP: Ipv4Addr = Ipv4Addr::new(10, 73, 0, 20);
pub(super) const HTTP_PORT: u16 = 80;
pub(super) const HTTPS_PORT: u16 = 443;

pub(super) fn verify_arp_reply(frame: &[u8]) -> Result<()> {
    expect_eq("ARP frame length", &frame.len(), &42)?;
    expect_slice_eq("ethernet destination", &frame[0..6], &GUEST_MAC)?;
    expect_slice_eq("ethernet source", &frame[6..12], &GATEWAY_MAC)?;
    expect_eq("ethernet type", &read_u16(frame, 12), &ETHERTYPE_ARP)?;
    expect_eq("ARP hardware type", &read_u16(frame, 14), &0x0001)?;
    expect_eq("ARP protocol type", &read_u16(frame, 16), &0x0800)?;
    expect_eq("ARP hardware length", &frame[18], &6)?;
    expect_eq("ARP protocol length", &frame[19], &4)?;
    expect_eq("ARP operation", &read_u16(frame, 20), &0x0002)?;
    expect_slice_eq("ARP sender MAC", &frame[22..28], &GATEWAY_MAC)?;
    expect_slice_eq("ARP sender IP", &frame[28..32], &GATEWAY_IP)?;
    expect_slice_eq("ARP target MAC", &frame[32..38], &GUEST_MAC)?;
    expect_slice_eq("ARP target IP", &frame[38..42], &GUEST_IP)
}

pub(super) fn attribute_named_host_to_upstream<N>(
    sim: &mut Simulator,
    running: &mut N,
    guest_link: &GuestLink,
    host: &str,
) -> Result<()>
where
    N: SteppedNetwork,
{
    let query = dns_a_query(host, 0x5151)?;
    guest_link.send_to_network(udp_datagram(GATEWAY_IP, 53_100, 53, &query)?)?;
    let response = sim.drive_until_network_frame(
        running,
        guest_link,
        "guest DNS attribution",
        DriveBudget {
            max_steps: 1024,
            ..DriveBudget::default()
        },
    )?;
    expect_eq("DNS response ethertype", &read_u16(&response, 12), &ETHERTYPE_IPV4)
}

pub(super) fn tls_network_config_for(
    mediated_ca: &agentdp_crypto::CertificateAuthorityPem,
    upstream_roots: &[String],
    allowed_hosts: &[&str],
    secrets: RuntimeSecrets,
    bypass_hosts: &[&str],
) -> InstanceNetworkConfig {
    let egress = allowed_hosts.iter().fold(EgressPolicy::allow_all(), |policy, host| {
        policy.with_allowed_authority(host)
    });
    let mut config = InstanceNetworkConfig::new(mediated_network_addresses(), mediated_network_mac(), egress.clone());
    config.policy = NetworkPolicy::new(egress).with_secrets(secrets);
    config.dns_upstream = DNS_UPSTREAM;
    config.tls = Some(TlsInterceptConfig {
        ca_cert_pem: mediated_ca.cert_pem.clone(),
        ca_key_pem: mediated_ca.key_pem.clone(),
        upstream_root_ca_pems: upstream_roots.to_vec(),
        intercepted_ports: vec![HTTPS_PORT],
        bypass_hosts: bypass_hosts.iter().map(|host| (*host).to_owned()).collect(),
    });
    config
}

pub(super) const fn upstream_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(UPSTREAM_IP), HTTPS_PORT)
}

pub(super) const fn http_upstream_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(UPSTREAM_IP), HTTP_PORT)
}

pub(super) fn verify_icmp_echo_reply(frame: &[u8], identifier: u16, sequence: u16, payload: &[u8]) -> Result<()> {
    if frame.len() < 42 {
        return Err(Error::new(format!(
            "ICMP echo frame length: expected at least 42, got {}",
            frame.len()
        )));
    }
    expect_slice_eq("ethernet destination", &frame[0..6], &GUEST_MAC)?;
    expect_slice_eq("ethernet source", &frame[6..12], &GATEWAY_MAC)?;
    expect_eq("ethernet type", &read_u16(frame, 12), &ETHERTYPE_IPV4)?;

    let ip = &frame[14..];
    let header_len = usize::from(ip[0] & 0x0f) * 4;
    expect_eq("IPv4 header length", &header_len, &20)?;
    expect_eq("IPv4 version", &(ip[0] >> 4), &4)?;
    expect_eq(
        "IPv4 total length",
        &(read_u16(ip, 2) as usize),
        &(header_len + 8 + payload.len()),
    )?;
    expect_eq("IPv4 protocol", &ip[9], &IP_PROTOCOL_ICMP)?;
    expect_slice_eq("IPv4 source", &ip[12..16], &GATEWAY_IP)?;
    expect_slice_eq("IPv4 destination", &ip[16..20], &GUEST_IP)?;
    expect_eq("IPv4 checksum", &internet_checksum(&ip[..header_len]), &0)?;

    let icmp_end = header_len + 8 + payload.len();
    if ip.len() < icmp_end {
        return Err(Error::new(format!(
            "ICMP payload length: expected at least {icmp_end}, got {}",
            ip.len()
        )));
    }
    let icmp = &ip[header_len..icmp_end];
    expect_eq("ICMP type", &icmp[0], &ICMP_ECHO_REPLY)?;
    expect_eq("ICMP code", &icmp[1], &0)?;
    expect_eq("ICMP checksum", &internet_checksum(icmp), &0)?;
    expect_eq("ICMP identifier", &read_u16(icmp, 4), &identifier)?;
    expect_eq("ICMP sequence", &read_u16(icmp, 6), &sequence)?;
    expect_slice_eq("ICMP payload", &icmp[8..], payload)
}

pub(super) fn expect_eq<T>(name: &str, actual: &T, expected: &T) -> Result<()>
where
    T: std::fmt::Debug + PartialEq,
{
    if actual == expected {
        Ok(())
    } else {
        Err(Error::new(format!("{name}: expected {expected:?}, got {actual:?}")))
    }
}

pub(super) fn expect_slice_eq(name: &str, actual: &[u8], expected: &[u8]) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(Error::new(format!(
            "{name}: expected {expected:02x?}, got {actual:02x?}"
        )))
    }
}
