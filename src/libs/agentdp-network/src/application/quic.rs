pub(crate) fn looks_like_quic_initial(bytes: &[u8]) -> bool {
    bytes.first().is_some_and(|first| first & 0b1100_0000 == 0b1100_0000)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::looks_like_quic_initial;

    #[test]
    fn recognizes_long_header_prefix() {
        assert!(looks_like_quic_initial(&[0xc0]));
        assert!(!looks_like_quic_initial(&[0x40]));
        assert!(!looks_like_quic_initial(&[]));
    }

    proptest! {
        #[test]
        fn classification_matches_top_two_bits(first in any::<u8>()) {
            prop_assert_eq!(looks_like_quic_initial(&[first]), first & 0b1100_0000 == 0b1100_0000);
        }
    }
}
