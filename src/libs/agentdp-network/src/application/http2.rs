pub(crate) fn looks_like_h2c(bytes: &[u8]) -> bool {
    bytes.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::looks_like_h2c;

    #[test]
    fn recognizes_http2_preface() {
        assert!(looks_like_h2c(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\nextra"));
        assert!(!looks_like_h2c(b"PRI * HTTP/2.0"));
    }
}
