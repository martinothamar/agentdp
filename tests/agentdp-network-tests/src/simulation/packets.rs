use super::{Error, Result};

pub const GUEST_MAC: [u8; 6] = agentdp_core::mediated_network::DEFAULT_PROFILE.guest_mac.octets();
pub const GATEWAY_MAC: [u8; 6] = agentdp_core::mediated_network::DEFAULT_PROFILE.gateway_mac.octets();
pub const BROADCAST_MAC: [u8; 6] = [0xff; 6];
pub const GUEST_IP: [u8; 4] = agentdp_core::mediated_network::DEFAULT_PROFILE.guest_ipv4.octets();
pub const GATEWAY_IP: [u8; 4] = agentdp_core::mediated_network::DEFAULT_PROFILE.gateway_ipv4.octets();

pub const ETHERTYPE_ARP: u16 = 0x0806;
pub const ETHERTYPE_IPV4: u16 = 0x0800;
pub const IP_PROTOCOL_ICMP: u8 = 1;
pub const IP_PROTOCOL_UDP: u8 = 17;
pub const ICMP_ECHO_REPLY: u8 = 0;
const ICMP_ECHO_REQUEST: u8 = 8;

#[must_use]
pub fn arp_request() -> Vec<u8> {
    let mut frame = ethernet_header(BROADCAST_MAC, GUEST_MAC, ETHERTYPE_ARP);
    frame.extend_from_slice(&0x0001_u16.to_be_bytes());
    frame.extend_from_slice(&ETHERTYPE_IPV4.to_be_bytes());
    frame.push(6);
    frame.push(4);
    frame.extend_from_slice(&0x0001_u16.to_be_bytes());
    frame.extend_from_slice(&GUEST_MAC);
    frame.extend_from_slice(&GUEST_IP);
    frame.extend_from_slice(&[0; 6]);
    frame.extend_from_slice(&GATEWAY_IP);
    frame
}

/// # Errors
///
/// Returns an error when the generated IPv4 packet would exceed the IPv4 length field.
pub fn icmp_echo_request(identifier: u16, sequence: u16, payload: &[u8]) -> Result<Vec<u8>> {
    let mut icmp = Vec::with_capacity(8 + payload.len());
    icmp.push(ICMP_ECHO_REQUEST);
    icmp.push(0);
    icmp.extend_from_slice(&0_u16.to_be_bytes());
    icmp.extend_from_slice(&identifier.to_be_bytes());
    icmp.extend_from_slice(&sequence.to_be_bytes());
    icmp.extend_from_slice(payload);
    let icmp_checksum = checksum(&icmp);
    write_checksum(&mut icmp, 2, icmp_checksum);

    let mut packet = ipv4_packet(GUEST_IP, GATEWAY_IP, IP_PROTOCOL_ICMP, &icmp)?;
    let mut frame = ethernet_header(GATEWAY_MAC, GUEST_MAC, ETHERTYPE_IPV4);
    frame.append(&mut packet);
    Ok(frame)
}

/// # Errors
///
/// Returns an error when the generated UDP packet would exceed the IPv4 or UDP length field.
pub fn udp_datagram(dst_ip: [u8; 4], src_port: u16, dst_port: u16, payload: &[u8]) -> Result<Vec<u8>> {
    let udp_len = u16::try_from(8 + payload.len())
        .map_err(|error| Error::new(format!("UDP datagram length must fit in u16: {error}")))?;
    let mut udp = Vec::with_capacity(usize::from(udp_len));
    udp.extend_from_slice(&src_port.to_be_bytes());
    udp.extend_from_slice(&dst_port.to_be_bytes());
    udp.extend_from_slice(&udp_len.to_be_bytes());
    udp.extend_from_slice(&0_u16.to_be_bytes());
    udp.extend_from_slice(payload);
    let checksum = udp_checksum(GUEST_IP, dst_ip, udp_len, &udp);
    write_checksum(&mut udp, 6, checksum);

    let mut packet = ipv4_packet(GUEST_IP, dst_ip, IP_PROTOCOL_UDP, &udp)?;
    let mut frame = ethernet_header(GATEWAY_MAC, GUEST_MAC, ETHERTYPE_IPV4);
    frame.append(&mut packet);
    Ok(frame)
}

/// # Errors
///
/// Returns an error when a DNS label is too long or the query packet cannot be encoded.
pub fn dns_a_query(host: &str, transaction_id: u16) -> Result<Vec<u8>> {
    let mut query = Vec::new();
    query.extend_from_slice(&transaction_id.to_be_bytes());
    query.extend_from_slice(&0x0100_u16.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    query.extend_from_slice(&0_u16.to_be_bytes());
    query.extend_from_slice(&0_u16.to_be_bytes());
    query.extend_from_slice(&0_u16.to_be_bytes());
    push_dns_name(&mut query, host)?;
    query.extend_from_slice(&1_u16.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    Ok(query)
}

/// # Errors
///
/// Returns an error when a DNS label is too long or the response packet cannot be encoded.
pub fn dns_a_response(host: &str, transaction_id: u16, address: [u8; 4], ttl: u32) -> Result<Vec<u8>> {
    let mut response = Vec::new();
    response.extend_from_slice(&transaction_id.to_be_bytes());
    response.extend_from_slice(&0x8180_u16.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    push_dns_name(&mut response, host)?;
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&[0xc0, 0x0c]);
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&ttl.to_be_bytes());
    response.extend_from_slice(&4_u16.to_be_bytes());
    response.extend_from_slice(&address);
    Ok(response)
}

#[must_use]
pub fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([bytes[offset], bytes[offset + 1]])
}

#[must_use]
pub fn internet_checksum(bytes: &[u8]) -> u16 {
    checksum(bytes)
}

fn ethernet_header(dst: [u8; 6], src: [u8; 6], ethertype: u16) -> Vec<u8> {
    let mut frame = Vec::with_capacity(14);
    frame.extend_from_slice(&dst);
    frame.extend_from_slice(&src);
    frame.extend_from_slice(&ethertype.to_be_bytes());
    frame
}

fn push_dns_name(packet: &mut Vec<u8>, host: &str) -> Result<()> {
    for label in host.trim_end_matches('.').split('.') {
        let len = u8::try_from(label.len())
            .map_err(|error| Error::new(format!("DNS label length must fit in u8: {error}")))?;
        if len > 63 {
            return Err(Error::new(format!("DNS label is longer than 63 bytes: {label}")));
        }
        packet.push(len);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0);
    Ok(())
}

fn ipv4_packet(src: [u8; 4], dst: [u8; 4], protocol: u8, payload: &[u8]) -> Result<Vec<u8>> {
    let total_len = u16::try_from(20 + payload.len())
        .map_err(|error| Error::new(format!("IPv4 packet length must fit in u16: {error}")))?;
    let mut packet = Vec::with_capacity(usize::from(total_len));
    packet.push(0x45);
    packet.push(0);
    packet.extend_from_slice(&total_len.to_be_bytes());
    packet.extend_from_slice(&0x1000_u16.to_be_bytes());
    packet.extend_from_slice(&0_u16.to_be_bytes());
    packet.push(64);
    packet.push(protocol);
    packet.extend_from_slice(&0_u16.to_be_bytes());
    packet.extend_from_slice(&src);
    packet.extend_from_slice(&dst);
    let header_checksum = checksum(&packet[..20]);
    write_checksum(&mut packet, 10, header_checksum);
    packet.extend_from_slice(payload);
    Ok(packet)
}

fn write_checksum(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn udp_checksum(src: [u8; 4], dst: [u8; 4], udp_len: u16, udp: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(12 + udp.len());
    pseudo.extend_from_slice(&src);
    pseudo.extend_from_slice(&dst);
    pseudo.push(0);
    pseudo.push(IP_PROTOCOL_UDP);
    pseudo.extend_from_slice(&udp_len.to_be_bytes());
    pseudo.extend_from_slice(udp);
    checksum(&pseudo)
}

fn checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0_u32;
    for chunk in bytes.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]])
        } else {
            u16::from(chunk[0]) << 8
        };
        sum += u32::from(word);
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    let folded = match u16::try_from(sum) {
        Ok(folded) => folded,
        Err(_error) => unreachable!("checksum sum is folded into 16 bits"),
    };
    !folded
}
