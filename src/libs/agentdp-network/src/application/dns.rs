use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

const TCP_DNS_PENDING_LIMIT: usize = 16;
const TCP_DNS_FRAME_BUFFER_CAPACITY: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DnsQuestion {
    pub(crate) id: u16,
    pub(crate) host: String,
    pub(crate) qtype: u16,
    pub(crate) qclass: u16,
}

pub(crate) fn dns_question(payload: &[u8]) -> Option<DnsQuestion> {
    let parts = dns_question_parts(payload)?;
    let (host, _offset) = read_dns_name(payload, 12)?;
    Some(DnsQuestion {
        id: parts.id,
        host,
        qtype: parts.qtype,
        qclass: parts.qclass,
    })
}

pub(crate) fn has_dns_question(payload: &[u8]) -> bool {
    dns_question_parts(payload).is_some()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DnsAddressRecords {
    pub(crate) addresses: Vec<IpAddr>,
    pub(crate) ttl: Duration,
}

pub(crate) fn dns_address_records(payload: &[u8]) -> DnsAddressRecords {
    let records = dns_addresses_with_ttl(payload);
    let ttl = records
        .iter()
        .map(|record| record.ttl)
        .min()
        .unwrap_or(Duration::from_mins(1));
    let mut addresses = Vec::with_capacity(records.len());
    addresses.extend(records.into_iter().map(|record| record.addr));
    DnsAddressRecords { addresses, ttl }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DnsResolution {
    pub(crate) host: String,
    pub(crate) addresses: Vec<IpAddr>,
    pub(crate) ttl: Duration,
}

pub(crate) struct TcpDnsTracker {
    guest: DnsTcpFrames,
    upstream: DnsTcpFrames,
    pending: Vec<DnsQuestion>,
}

impl Default for TcpDnsTracker {
    fn default() -> Self {
        Self {
            guest: DnsTcpFrames::default(),
            upstream: DnsTcpFrames::default(),
            pending: Vec::with_capacity(TCP_DNS_PENDING_LIMIT),
        }
    }
}

impl TcpDnsTracker {
    pub(crate) fn record_queries(&mut self, bytes: &[u8]) {
        let pending = &mut self.pending;
        self.guest.for_each_message(bytes, |message| {
            if let Some(question) = dns_question(message) {
                push_pending(pending, question);
            }
        });
    }

    pub(crate) fn response(&mut self, bytes: &[u8]) -> Option<DnsResolution> {
        let mut resolution = None;
        let pending = &mut self.pending;
        self.upstream.for_each_message(bytes, |message| {
            if resolution.is_some() {
                return;
            }
            let Some(question) = dns_question(message) else {
                return;
            };
            let Some(question) = remove_pending(pending, &question) else {
                return;
            };
            let records = dns_address_records(message);
            resolution = Some(DnsResolution {
                host: question.host,
                addresses: records.addresses,
                ttl: records.ttl,
            });
        });
        resolution
    }
}

fn push_pending(pending: &mut Vec<DnsQuestion>, question: DnsQuestion) {
    if pending.contains(&question) {
        return;
    }
    if pending.len() == pending.capacity() {
        let _evicted = pending.remove(0);
    }
    pending.push(question);
}

fn remove_pending(pending: &mut Vec<DnsQuestion>, question: &DnsQuestion) -> Option<DnsQuestion> {
    let index = pending.iter().position(|pending| pending == question)?;
    Some(pending.remove(index))
}

struct DnsTcpFrames {
    buffer: Vec<u8>,
}

impl Default for DnsTcpFrames {
    fn default() -> Self {
        Self {
            buffer: Vec::with_capacity(TCP_DNS_FRAME_BUFFER_CAPACITY),
        }
    }
}

impl DnsTcpFrames {
    fn for_each_message(&mut self, bytes: &[u8], mut f: impl FnMut(&[u8])) {
        if self.buffer.len().saturating_add(bytes.len()) > TCP_DNS_FRAME_BUFFER_CAPACITY {
            self.buffer.clear();
            return;
        }
        self.buffer.extend_from_slice(bytes);
        let mut offset = 0;
        while self.buffer.len().saturating_sub(offset) >= 2 {
            let len = usize::from(u16::from_be_bytes([self.buffer[offset], self.buffer[offset + 1]]));
            let start = offset + 2;
            let end = start + len;
            if end > self.buffer.len() {
                break;
            }
            f(&self.buffer[start..end]);
            offset = end;
        }
        self.buffer.drain(..offset);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AddressRecord {
    addr: IpAddr,
    ttl: Duration,
}

fn dns_addresses_with_ttl(payload: &[u8]) -> Vec<AddressRecord> {
    let mut accepted_names = dns_primary_question_hosts(payload);
    let records = dns_answer_records(payload);
    let mut cname_edges = Vec::with_capacity(records.len());
    cname_edges.extend(
        records
            .iter()
            .filter(|record| record.record_type == 5 && record.class == 1)
            .filter_map(|record| {
                read_dns_name(payload, record.data_offset).map(|(target, _)| (record.name.clone(), target))
            }),
    );
    loop {
        let mut changed = false;
        for (name, target) in &cname_edges {
            if accepted_names.contains(name) && !accepted_names.contains(target) {
                accepted_names.push(target.clone());
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    records
        .iter()
        .filter(|record| record.class == 1 && accepted_names.contains(&record.name))
        .filter_map(address_record)
        .collect()
}

#[derive(Debug, Clone)]
struct DnsRecord<'a> {
    name: String,
    record_type: u16,
    class: u16,
    ttl: Duration,
    data: &'a [u8],
    data_offset: usize,
}

fn dns_primary_question_hosts(payload: &[u8]) -> Vec<String> {
    dns_question(payload).map_or_else(Vec::new, |question| Vec::from([question.host]))
}

fn dns_answer_records(payload: &[u8]) -> Vec<DnsRecord<'_>> {
    if payload.len() < 12 {
        return Vec::new();
    }
    let questions = u16::from_be_bytes([payload[4], payload[5]]);
    let answers = u16::from_be_bytes([payload[6], payload[7]]);
    let mut offset = 12;
    for _ in 0..questions {
        let Some(next) = read_dns_name(payload, offset).and_then(|(_, offset)| offset.checked_add(4)) else {
            return Vec::new();
        };
        offset = next;
    }
    let mut records = Vec::with_capacity(usize::from(answers));
    for _ in 0..answers {
        let Some((name, next)) = read_dns_name(payload, offset) else {
            return records;
        };
        offset = next;
        let Some(header_end) = offset.checked_add(10) else {
            return records;
        };
        if header_end > payload.len() {
            return records;
        }
        let record_type = u16::from_be_bytes([payload[offset], payload[offset + 1]]);
        let class = u16::from_be_bytes([payload[offset + 2], payload[offset + 3]]);
        let ttl = u32::from_be_bytes([
            payload[offset + 4],
            payload[offset + 5],
            payload[offset + 6],
            payload[offset + 7],
        ]);
        let length = usize::from(u16::from_be_bytes([payload[offset + 8], payload[offset + 9]]));
        offset = header_end;
        let Some(data_end) = offset.checked_add(length) else {
            return records;
        };
        if data_end > payload.len() {
            return records;
        }
        records.push(DnsRecord {
            name,
            record_type,
            class,
            ttl: Duration::from_secs(u64::from(ttl.max(1))),
            data: &payload[offset..data_end],
            data_offset: offset,
        });
        offset = data_end;
    }
    records
}

fn address_record(record: &DnsRecord<'_>) -> Option<AddressRecord> {
    if record.record_type == 1 && record.data.len() == 4 {
        return Some(AddressRecord {
            addr: IpAddr::V4(Ipv4Addr::new(
                record.data[0],
                record.data[1],
                record.data[2],
                record.data[3],
            )),
            ttl: record.ttl,
        });
    }
    if record.record_type == 28 && record.data.len() == 16 {
        let mut octets = [0_u8; 16];
        octets.copy_from_slice(record.data);
        return Some(AddressRecord {
            addr: IpAddr::V6(Ipv6Addr::from(octets)),
            ttl: record.ttl,
        });
    }
    None
}

fn read_dns_name(payload: &[u8], offset: usize) -> Option<(String, usize)> {
    let mut name = String::new();
    let offset = scan_dns_name(payload, offset, |label| {
        if !name.is_empty() {
            name.push('.');
        }
        name.extend(label.chars().map(|char| char.to_ascii_lowercase()));
    })?;
    if name.is_empty() {
        return None;
    }
    Some((name, offset))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DnsQuestionParts {
    id: u16,
    qtype: u16,
    qclass: u16,
}

fn dns_question_parts(payload: &[u8]) -> Option<DnsQuestionParts> {
    if payload.len() < 12 || u16::from_be_bytes([payload[4], payload[5]]) == 0 {
        return None;
    }
    let offset = scan_dns_name(payload, 12, |_label| {})?;
    let end = offset.checked_add(4)?;
    if end > payload.len() {
        return None;
    }
    Some(DnsQuestionParts {
        id: u16::from_be_bytes([payload[0], payload[1]]),
        qtype: u16::from_be_bytes([payload[offset], payload[offset + 1]]),
        qclass: u16::from_be_bytes([payload[offset + 2], payload[offset + 3]]),
    })
}

fn scan_dns_name(payload: &[u8], offset: usize, mut visit_label: impl FnMut(&str)) -> Option<usize> {
    let mut labels = 0_usize;
    let mut cursor = offset;
    let mut end_offset = None;
    let mut jumps = 0;
    loop {
        let length = *payload.get(cursor)?;
        if length == 0 {
            cursor = cursor.checked_add(1)?;
            break;
        }
        if length & 0b1100_0000 == 0b1100_0000 {
            let next = *payload.get(cursor.checked_add(1)?)?;
            end_offset.get_or_insert(cursor.checked_add(2)?);
            cursor = usize::from(u16::from(length & 0b0011_1111) << 8 | u16::from(next));
            jumps += 1;
            if jumps > 16 {
                return None;
            }
            continue;
        }
        if length & 0b1100_0000 != 0 {
            return None;
        }
        cursor = cursor.checked_add(1)?;
        let length = usize::from(length);
        let end = cursor.checked_add(length)?;
        if end > payload.len() {
            return None;
        }
        visit_label(std::str::from_utf8(&payload[cursor..end]).ok()?);
        labels = labels.checked_add(1)?;
        cursor = end;
    }
    if labels == 0 {
        return None;
    }
    Some(end_offset.unwrap_or(cursor))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::time::Duration;

    use proptest::prelude::*;

    use crate::test_support::unit::{dns_a_response, dns_header, dns_query, push_dns_name, tcp_dns_frame, u16_len};

    use super::{DnsTcpFrames, TCP_DNS_FRAME_BUFFER_CAPACITY, TcpDnsTracker, dns_address_records, dns_question};

    #[test]
    fn parses_dns_question_case_insensitively() {
        let packet = dns_query(0x1234, "Example.TEST", 28);
        let question = dns_question(&packet);

        assert_eq!(question.as_ref().map(|q| q.id), Some(0x1234));
        assert_eq!(question.as_ref().map(|q| q.host.as_str()), Some("example.test"));
        assert_eq!(question.as_ref().map(|q| q.qtype), Some(28));
        assert_eq!(question.as_ref().map(|q| q.qclass), Some(1));
    }

    #[test]
    fn extracts_cname_address_chain_and_minimum_ttl() {
        let packet = dns_response_with_cname();
        let records = dns_address_records(&packet);

        assert_eq!(
            records.addresses,
            vec![
                IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
                IpAddr::V6(Ipv6Addr::from([
                    0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
                ])),
            ]
        );
        assert_eq!(records.ttl, Duration::from_secs(9));
    }

    #[test]
    fn tcp_tracker_waits_for_matching_query_and_complete_frames() {
        let query = dns_query(0x5101, "allowed.test", 1);
        let response = dns_a_response(0x5101, "allowed.test", Ipv4Addr::new(10, 73, 0, 42), 60);
        let mut tracker = TcpDnsTracker::default();
        let query_frame = tcp_dns_frame(&query);
        let response_frame = tcp_dns_frame(&response);

        tracker.record_queries(&query_frame[..3]);
        assert_eq!(tracker.response(&response_frame), None);

        tracker.record_queries(&query_frame[3..]);
        assert_eq!(tracker.response(&response_frame[..5]), None);
        let resolution = tracker.response(&response_frame[5..]);

        assert_eq!(resolution.as_ref().map(|r| r.host.as_str()), Some("allowed.test"));
        assert_eq!(
            resolution.as_ref().map(|r| r.addresses.as_slice()),
            Some([IpAddr::V4(Ipv4Addr::new(10, 73, 0, 42))].as_slice())
        );
        assert_eq!(resolution.as_ref().map(|r| r.ttl), Some(Duration::from_mins(1)));
    }

    #[test]
    fn tcp_frame_buffer_discards_oversized_incomplete_message() {
        let mut frames = DnsTcpFrames::default();
        let oversized = vec![0_u8; TCP_DNS_FRAME_BUFFER_CAPACITY + 1];
        let mut messages = 0;

        frames.for_each_message(&oversized, |_message| messages += 1);

        assert_eq!(messages, 0);
        assert!(frames.buffer.is_empty());
    }

    #[test]
    fn malformed_name_compression_is_rejected() {
        let mut packet = dns_query(1, "loop.test", 1);
        packet[12] = 0xc0;
        packet[13] = 12;

        assert_eq!(dns_question(&packet), None);
    }

    proptest! {
        #[test]
        fn arbitrary_packets_do_not_panic(packet in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _question = dns_question(&packet);
            let _records = dns_address_records(&packet);
        }
    }

    fn dns_response_with_cname() -> Vec<u8> {
        let mut packet = dns_header(0x9001, 0x8180, 1, 3);
        push_dns_name(&mut packet, "example.test");
        packet.extend_from_slice(&1_u16.to_be_bytes());
        packet.extend_from_slice(&1_u16.to_be_bytes());

        packet.extend_from_slice(&[0xc0, 0x0c]);
        packet.extend_from_slice(&5_u16.to_be_bytes());
        packet.extend_from_slice(&1_u16.to_be_bytes());
        packet.extend_from_slice(&30_u32.to_be_bytes());
        let mut cname = Vec::new();
        push_dns_name(&mut cname, "real.test");
        packet.extend_from_slice(&u16_len(cname.len()).to_be_bytes());
        packet.extend_from_slice(&cname);

        push_dns_name(&mut packet, "real.test");
        packet.extend_from_slice(&1_u16.to_be_bytes());
        packet.extend_from_slice(&1_u16.to_be_bytes());
        packet.extend_from_slice(&42_u32.to_be_bytes());
        packet.extend_from_slice(&4_u16.to_be_bytes());
        packet.extend_from_slice(&[203, 0, 113, 7]);

        push_dns_name(&mut packet, "real.test");
        packet.extend_from_slice(&28_u16.to_be_bytes());
        packet.extend_from_slice(&1_u16.to_be_bytes());
        packet.extend_from_slice(&9_u32.to_be_bytes());
        packet.extend_from_slice(&16_u16.to_be_bytes());
        packet.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        packet
    }
}
