pub(crate) fn looks_like_http3_candidate(bytes: &[u8]) -> bool {
    // TODO: Replace this QUIC initial placeholder with ALPN-aware HTTP/3 detection once
    // QUIC connection classification exists.
    super::quic::looks_like_quic_initial(bytes)
}

#[cfg(test)]
mod tests {
    use super::looks_like_http3_candidate;

    #[test]
    fn delegates_to_quic_initial_classifier() {
        assert!(looks_like_http3_candidate(&[0xc0]));
        assert!(!looks_like_http3_candidate(&[0x00]));
    }
}
