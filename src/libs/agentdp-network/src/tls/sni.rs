pub(crate) fn extract_sni(data: &[u8]) -> Option<String> {
    if data.len() < 5 || data[0] != 0x16 {
        return None;
    }
    let record_len = u16::from_be_bytes([data[3], data[4]]) as usize;
    let record = data.get(5..5 + record_len)?;
    if record.first() != Some(&0x01) || record.len() < 4 {
        return None;
    }
    let handshake_len = ((record[1] as usize) << 16) | ((record[2] as usize) << 8) | record[3] as usize;
    let hello = record.get(4..4 + handshake_len)?;
    if hello.len() < 34 {
        return None;
    }
    let mut pos = 34;

    let session_len = *hello.get(pos)? as usize;
    pos += 1 + session_len;
    let suites_len = u16::from_be_bytes([*hello.get(pos)?, *hello.get(pos + 1)?]) as usize;
    pos += 2 + suites_len;
    let compression_len = *hello.get(pos)? as usize;
    pos += 1 + compression_len;
    let extensions_total_len = u16::from_be_bytes([*hello.get(pos)?, *hello.get(pos + 1)?]) as usize;
    pos += 2;

    let end = pos + extensions_total_len;
    while pos + 4 <= end && pos + 4 <= hello.len() {
        let extension_type = u16::from_be_bytes([hello[pos], hello[pos + 1]]);
        let extension_data_len = u16::from_be_bytes([hello[pos + 2], hello[pos + 3]]) as usize;
        pos += 4;
        if extension_type == 0 {
            return parse_sni_extension(hello.get(pos..pos + extension_data_len)?);
        }
        pos += extension_data_len;
    }
    None
}

fn parse_sni_extension(data: &[u8]) -> Option<String> {
    if data.len() < 2 {
        return None;
    }
    let list_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    let list = data.get(2..2 + list_len)?;
    let mut pos = 0;
    while pos + 3 <= list.len() {
        let name_type = list[pos];
        let name_len = u16::from_be_bytes([list[pos + 1], list[pos + 2]]) as usize;
        pos += 3;
        if name_type == 0 {
            return String::from_utf8(list.get(pos..pos + name_len)?.to_vec()).ok();
        }
        pos += name_len;
    }
    None
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::extract_sni;

    #[test]
    fn extracts_sni_from_minimal_client_hello() {
        let hello = client_hello("allowed.test");

        assert_eq!(extract_sni(&hello).as_deref(), Some("allowed.test"));
    }

    #[test]
    fn rejects_non_client_hello_records() {
        assert_eq!(extract_sni(&[]), None);
        assert_eq!(extract_sni(&[0x15, 0x03, 0x03, 0x00, 0x00]), None);
        let mut hello = client_hello("allowed.test");
        hello[5] = 0x02;
        assert_eq!(extract_sni(&hello), None);
    }

    proptest! {
        #[test]
        fn arbitrary_records_do_not_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _sni = extract_sni(&bytes);
        }
    }

    fn client_hello(host: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&[0x03, 0x03]);
        body.extend_from_slice(&[0_u8; 32]);
        body.push(0);
        body.extend_from_slice(&2_u16.to_be_bytes());
        body.extend_from_slice(&0x1301_u16.to_be_bytes());
        body.push(1);
        body.push(0);

        let host = host.as_bytes();
        let mut sni = Vec::new();
        sni.extend_from_slice(&usize_to_u16(host.len() + 3).to_be_bytes());
        sni.push(0);
        sni.extend_from_slice(&usize_to_u16(host.len()).to_be_bytes());
        sni.extend_from_slice(host);

        let mut extensions = Vec::new();
        extensions.extend_from_slice(&0_u16.to_be_bytes());
        extensions.extend_from_slice(&usize_to_u16(sni.len()).to_be_bytes());
        extensions.extend_from_slice(&sni);
        body.extend_from_slice(&usize_to_u16(extensions.len()).to_be_bytes());
        body.extend_from_slice(&extensions);

        let mut handshake = Vec::new();
        handshake.push(0x01);
        handshake.extend_from_slice(&u24_bytes(body.len()));
        handshake.extend_from_slice(&body);

        let mut record = Vec::new();
        record.extend_from_slice(&[0x16, 0x03, 0x03]);
        record.extend_from_slice(&usize_to_u16(handshake.len()).to_be_bytes());
        record.extend_from_slice(&handshake);
        record
    }

    fn usize_to_u16(value: usize) -> u16 {
        u16::try_from(value).unwrap_or(u16::MAX)
    }

    fn u24_bytes(value: usize) -> [u8; 3] {
        let bytes = u32::try_from(value).unwrap_or(u32::MAX).to_be_bytes();
        [bytes[1], bytes[2], bytes[3]]
    }
}
