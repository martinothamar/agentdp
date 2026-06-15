use std::net::Ipv4Addr;

pub(crate) fn dns_query(id: u16, host: &str, qtype: u16) -> Vec<u8> {
    let mut packet = dns_header(id, 0x0100, 1, 0);
    push_dns_name(&mut packet, host);
    packet.extend_from_slice(&qtype.to_be_bytes());
    packet.extend_from_slice(&1_u16.to_be_bytes());
    packet
}

pub(crate) fn dns_a_response(id: u16, host: &str, addr: Ipv4Addr, ttl: u32) -> Vec<u8> {
    let mut packet = dns_header(id, 0x8180, 1, 1);
    push_dns_name(&mut packet, host);
    packet.extend_from_slice(&1_u16.to_be_bytes());
    packet.extend_from_slice(&1_u16.to_be_bytes());
    packet.extend_from_slice(&[0xc0, 0x0c]);
    packet.extend_from_slice(&1_u16.to_be_bytes());
    packet.extend_from_slice(&1_u16.to_be_bytes());
    packet.extend_from_slice(&ttl.to_be_bytes());
    packet.extend_from_slice(&4_u16.to_be_bytes());
    packet.extend_from_slice(&addr.octets());
    packet
}

pub(crate) fn dns_header(id: u16, flags: u16, questions: u16, answers: u16) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(&id.to_be_bytes());
    packet.extend_from_slice(&flags.to_be_bytes());
    packet.extend_from_slice(&questions.to_be_bytes());
    packet.extend_from_slice(&answers.to_be_bytes());
    packet.extend_from_slice(&0_u16.to_be_bytes());
    packet.extend_from_slice(&0_u16.to_be_bytes());
    packet
}

pub(crate) fn push_dns_name(packet: &mut Vec<u8>, host: &str) {
    for label in host.split('.') {
        packet.push(u8::try_from(label.len()).unwrap_or(u8::MAX));
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0);
}

pub(crate) fn tcp_dns_frame(message: &[u8]) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&u16_len(message.len()).to_be_bytes());
    frame.extend_from_slice(message);
    frame
}

pub(crate) fn u16_len(value: usize) -> u16 {
    u16::try_from(value).unwrap_or(u16::MAX)
}
