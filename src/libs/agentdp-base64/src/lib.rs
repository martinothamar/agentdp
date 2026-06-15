#![deny(clippy::dbg_macro)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const DECODE_PADDING: u8 = 0xFE;
const INVALID_ENCODING: u8 = 0xFF;

const fn build_encode_pair_table() -> [[u8; 2]; 4096] {
    let mut table = [[0_u8; 2]; 4096];
    let mut i = 0usize;

    while i < table.len() {
        table[i] = [ALPHABET[(i >> 6) & 0x3f], ALPHABET[i & 0x3f]];
        i += 1;
    }

    table
}

const fn build_decode_table() -> [u8; 256] {
    let mut table = [INVALID_ENCODING; 256];
    let mut i = 0u8;

    while i < 26 {
        table[(b'A' + i) as usize] = i;
        table[(b'a' + i) as usize] = i + 26;
        i += 1;
    }

    i = 0;
    while i < 10 {
        table[(b'0' + i) as usize] = i + 52;
        i += 1;
    }

    table[b'+' as usize] = 62;
    table[b'/' as usize] = 63;
    table[b'=' as usize] = DECODE_PADDING;

    table
}

const ENCODE_PAIR_TABLE: [[u8; 2]; 4096] = build_encode_pair_table();
const DECODE_TABLE: [u8; 256] = build_decode_table();
const FAST_BLOCKS: usize = 4;
const FAST_INPUT_BYTES: usize = FAST_BLOCKS * 6;
const FAST_OUTPUT_BYTES: usize = FAST_BLOCKS * 8;
const LOW_SIX_BITS: u64 = 0x3f;

/// Returns the encoded output length for `input_len` bytes using standard padded base64.
#[inline]
#[must_use]
pub const fn encoded_len(input_len: usize) -> usize {
    input_len.div_ceil(3) * 4
}

/// Returns the decoded output length implied by standard padded base64 length and trailing padding.
///
/// This validates the input length and padding shape only. `decode` validates the alphabet and
/// rejects padding that appears before the final quantum.
#[inline]
#[must_use]
pub fn decoded_len(input: &[u8]) -> Option<usize> {
    if !input.len().is_multiple_of(4) {
        return None;
    }

    let full_len = (input.len() / 4) * 3;
    let padding = match input {
        [.., b'=', b'='] => 2,
        [.., b'='] => 1,
        _ => 0,
    };

    Some(full_len - padding)
}

/// Encodes bytes into caller-owned UTF-8 output bytes using standard padded base64.
///
/// Returns the number of bytes written, or `None` if `output` is too small.
#[inline]
#[must_use]
pub fn encode(input: &[u8], output: &mut [u8]) -> Option<usize> {
    let needed = encoded_len(input.len());
    if output.len() < needed {
        return None;
    }

    let mut input_idx = 0usize;
    let mut output_idx = 0usize;

    let last_fast_index = input.len().saturating_sub(FAST_INPUT_BYTES + 2);
    if last_fast_index > 0 {
        while input_idx <= last_fast_index {
            let input_chunk = &input[input_idx..input_idx + FAST_INPUT_BYTES + 2];
            let output_chunk = &mut output[output_idx..output_idx + FAST_OUTPUT_BYTES];

            let input_u64 = read_u64_be(input_chunk);
            output_chunk[0] = ALPHABET[((input_u64 >> 58) & LOW_SIX_BITS) as usize];
            output_chunk[1] = ALPHABET[((input_u64 >> 52) & LOW_SIX_BITS) as usize];
            output_chunk[2] = ALPHABET[((input_u64 >> 46) & LOW_SIX_BITS) as usize];
            output_chunk[3] = ALPHABET[((input_u64 >> 40) & LOW_SIX_BITS) as usize];
            output_chunk[4] = ALPHABET[((input_u64 >> 34) & LOW_SIX_BITS) as usize];
            output_chunk[5] = ALPHABET[((input_u64 >> 28) & LOW_SIX_BITS) as usize];
            output_chunk[6] = ALPHABET[((input_u64 >> 22) & LOW_SIX_BITS) as usize];
            output_chunk[7] = ALPHABET[((input_u64 >> 16) & LOW_SIX_BITS) as usize];

            let input_u64 = read_u64_be(&input_chunk[6..]);
            output_chunk[8] = ALPHABET[((input_u64 >> 58) & LOW_SIX_BITS) as usize];
            output_chunk[9] = ALPHABET[((input_u64 >> 52) & LOW_SIX_BITS) as usize];
            output_chunk[10] = ALPHABET[((input_u64 >> 46) & LOW_SIX_BITS) as usize];
            output_chunk[11] = ALPHABET[((input_u64 >> 40) & LOW_SIX_BITS) as usize];
            output_chunk[12] = ALPHABET[((input_u64 >> 34) & LOW_SIX_BITS) as usize];
            output_chunk[13] = ALPHABET[((input_u64 >> 28) & LOW_SIX_BITS) as usize];
            output_chunk[14] = ALPHABET[((input_u64 >> 22) & LOW_SIX_BITS) as usize];
            output_chunk[15] = ALPHABET[((input_u64 >> 16) & LOW_SIX_BITS) as usize];

            let input_u64 = read_u64_be(&input_chunk[12..]);
            output_chunk[16] = ALPHABET[((input_u64 >> 58) & LOW_SIX_BITS) as usize];
            output_chunk[17] = ALPHABET[((input_u64 >> 52) & LOW_SIX_BITS) as usize];
            output_chunk[18] = ALPHABET[((input_u64 >> 46) & LOW_SIX_BITS) as usize];
            output_chunk[19] = ALPHABET[((input_u64 >> 40) & LOW_SIX_BITS) as usize];
            output_chunk[20] = ALPHABET[((input_u64 >> 34) & LOW_SIX_BITS) as usize];
            output_chunk[21] = ALPHABET[((input_u64 >> 28) & LOW_SIX_BITS) as usize];
            output_chunk[22] = ALPHABET[((input_u64 >> 22) & LOW_SIX_BITS) as usize];
            output_chunk[23] = ALPHABET[((input_u64 >> 16) & LOW_SIX_BITS) as usize];

            let input_u64 = read_u64_be(&input_chunk[18..]);
            output_chunk[24] = ALPHABET[((input_u64 >> 58) & LOW_SIX_BITS) as usize];
            output_chunk[25] = ALPHABET[((input_u64 >> 52) & LOW_SIX_BITS) as usize];
            output_chunk[26] = ALPHABET[((input_u64 >> 46) & LOW_SIX_BITS) as usize];
            output_chunk[27] = ALPHABET[((input_u64 >> 40) & LOW_SIX_BITS) as usize];
            output_chunk[28] = ALPHABET[((input_u64 >> 34) & LOW_SIX_BITS) as usize];
            output_chunk[29] = ALPHABET[((input_u64 >> 28) & LOW_SIX_BITS) as usize];
            output_chunk[30] = ALPHABET[((input_u64 >> 22) & LOW_SIX_BITS) as usize];
            output_chunk[31] = ALPHABET[((input_u64 >> 16) & LOW_SIX_BITS) as usize];

            input_idx += FAST_INPUT_BYTES;
            output_idx += FAST_OUTPUT_BYTES;
        }
    }

    let full_len = input.len() / 3 * 3;

    while input_idx < full_len {
        let byte0 = input[input_idx];
        let byte1 = input[input_idx + 1];
        let byte2 = input[input_idx + 2];

        let pair0 = ENCODE_PAIR_TABLE[(usize::from(byte0) << 4) | usize::from(byte1 >> 4)];
        let pair1 = ENCODE_PAIR_TABLE[(usize::from(byte1 & 0x0f) << 8) | usize::from(byte2)];

        output[output_idx] = pair0[0];
        output[output_idx + 1] = pair0[1];
        output[output_idx + 2] = pair1[0];
        output[output_idx + 3] = pair1[1];

        input_idx += 3;
        output_idx += 4;
    }

    match input.len() - full_len {
        0 => {}
        1 => {
            let byte0 = input[input_idx];
            let pair = ENCODE_PAIR_TABLE[usize::from(byte0) << 4];
            output[output_idx] = pair[0];
            output[output_idx + 1] = pair[1];
            output[output_idx + 2] = b'=';
            output[output_idx + 3] = b'=';
        }
        2 => {
            let byte0 = input[input_idx];
            let byte1 = input[input_idx + 1];
            let pair0 = ENCODE_PAIR_TABLE[(usize::from(byte0) << 4) | usize::from(byte1 >> 4)];
            let pair1 = ENCODE_PAIR_TABLE[usize::from(byte1 & 0x0f) << 8];
            output[output_idx] = pair0[0];
            output[output_idx + 1] = pair0[1];
            output[output_idx + 2] = pair1[0];
            output[output_idx + 3] = b'=';
        }
        _ => unreachable!("base64 tail length is modulo 3"),
    }

    Some(needed)
}

#[inline]
fn read_u64_be(input: &[u8]) -> u64 {
    let Ok(bytes) = input[..8].try_into() else {
        unreachable!("base64 fast path reads eight-byte chunks")
    };
    u64::from_be_bytes(bytes)
}

/// Decodes standard padded base64 bytes into a caller-owned output buffer.
///
/// Returns the number of bytes written, or `None` if the input is malformed or `output` is too small.
#[inline]
#[must_use]
pub fn decode(input: &[u8], output: &mut [u8]) -> Option<usize> {
    let decoded_len = decoded_len(input)?;
    if output.len() < decoded_len {
        return None;
    }
    if input.is_empty() {
        return Some(0);
    }

    let final_input_idx = input.len() - 4;
    let input_unrolled_len = final_input_idx - (final_input_idx % 32);
    let mut input_idx = 0usize;

    while input_idx < input_unrolled_len {
        let output_idx = input_idx / 4 * 3;
        let input_chunk = &input[input_idx..input_idx + 32];
        let output_chunk = &mut output[output_idx..output_idx + 24];

        decode_chunk_8(&input_chunk[..8], &mut output_chunk[..6])?;
        decode_chunk_8(&input_chunk[8..16], &mut output_chunk[6..12])?;
        decode_chunk_8(&input_chunk[16..24], &mut output_chunk[12..18])?;
        decode_chunk_8(&input_chunk[24..32], &mut output_chunk[18..24])?;

        input_idx += 32;
    }

    while input_idx < final_input_idx {
        let output_idx = input_idx / 4 * 3;
        decode_chunk_4(
            &input[input_idx..input_idx + 4],
            &mut output[output_idx..output_idx + 3],
        )?;
        input_idx += 4;
    }

    let output_idx = final_input_idx / 4 * 3;
    let byte0 = DECODE_TABLE[usize::from(input[final_input_idx])];
    let byte1 = DECODE_TABLE[usize::from(input[final_input_idx + 1])];
    let byte2 = DECODE_TABLE[usize::from(input[final_input_idx + 2])];
    let byte3 = DECODE_TABLE[usize::from(input[final_input_idx + 3])];

    if byte0 >= 64 || byte1 >= 64 {
        return None;
    }

    if byte2 == DECODE_PADDING {
        if byte3 != DECODE_PADDING {
            return None;
        }
        output[output_idx] = (byte0 << 2) | (byte1 >> 4);
        return Some(output_idx + 1);
    }

    if byte2 >= 64 {
        return None;
    }

    output[output_idx] = (byte0 << 2) | (byte1 >> 4);
    output[output_idx + 1] = (byte1 << 4) | (byte2 >> 2);

    if byte3 == DECODE_PADDING {
        return Some(output_idx + 2);
    }

    if byte3 >= 64 {
        return None;
    }

    output[output_idx + 2] = (byte2 << 6) | byte3;
    Some(output_idx + 3)
}

#[inline]
fn decode_chunk_8(input: &[u8], output: &mut [u8]) -> Option<()> {
    let byte0 = DECODE_TABLE[usize::from(input[0])];
    let byte1 = DECODE_TABLE[usize::from(input[1])];
    let byte2 = DECODE_TABLE[usize::from(input[2])];
    let byte3 = DECODE_TABLE[usize::from(input[3])];
    let byte4 = DECODE_TABLE[usize::from(input[4])];
    let byte5 = DECODE_TABLE[usize::from(input[5])];
    let byte6 = DECODE_TABLE[usize::from(input[6])];
    let byte7 = DECODE_TABLE[usize::from(input[7])];

    if (byte0 | byte1 | byte2 | byte3 | byte4 | byte5 | byte6 | byte7) >= 64 {
        return None;
    }
    let accum = (u64::from(byte0) << 58)
        | (u64::from(byte1) << 52)
        | (u64::from(byte2) << 46)
        | (u64::from(byte3) << 40)
        | (u64::from(byte4) << 34)
        | (u64::from(byte5) << 28)
        | (u64::from(byte6) << 22)
        | (u64::from(byte7) << 16);

    output[..6].copy_from_slice(&accum.to_be_bytes()[..6]);
    Some(())
}

#[inline]
fn decode_chunk_4(input: &[u8], output: &mut [u8]) -> Option<()> {
    let byte0 = DECODE_TABLE[usize::from(input[0])];
    let byte1 = DECODE_TABLE[usize::from(input[1])];
    let byte2 = DECODE_TABLE[usize::from(input[2])];
    let byte3 = DECODE_TABLE[usize::from(input[3])];

    if (byte0 | byte1 | byte2 | byte3) >= 64 {
        return None;
    }

    output[0] = (byte0 << 2) | (byte1 >> 4);
    output[1] = (byte1 << 4) | (byte2 >> 2);
    output[2] = (byte2 << 6) | byte3;
    Some(())
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use proptest::prelude::*;

    use super::{decode, decoded_len, encode, encoded_len};

    #[test]
    fn encodes_standard_padding_vectors() {
        let cases: &[(&[u8], &str)] = &[
            (&b""[..], ""),
            (&b"f"[..], "Zg=="),
            (&b"fo"[..], "Zm8="),
            (&b"foo"[..], "Zm9v"),
            (&b"foob"[..], "Zm9vYg=="),
            (&b"fooba"[..], "Zm9vYmE="),
            (&b"foobar"[..], "Zm9vYmFy"),
            (&[0xff, 0xee][..], "/+4="),
            (&[0xff, 0xee, 0xdd][..], "/+7d"),
        ];

        for (input, expected) in cases {
            let mut output = vec![0u8; encoded_len(input.len())];
            assert_eq!(encode(input, &mut output), Some(expected.len()));
            assert_eq!(&output, expected.as_bytes());
        }
    }

    #[test]
    fn decodes_standard_padding_vectors() {
        let cases: &[(&str, &[u8])] = &[
            ("", &b""[..]),
            ("Zg==", &b"f"[..]),
            ("Zm8=", &b"fo"[..]),
            ("Zm9v", &b"foo"[..]),
            ("Zm9vYg==", &b"foob"[..]),
            ("Zm9vYmE=", &b"fooba"[..]),
            ("Zm9vYmFy", &b"foobar"[..]),
            ("/+4=", &[0xff, 0xee][..]),
            ("/+7d", &[0xff, 0xee, 0xdd][..]),
        ];

        for (input, expected) in cases {
            let mut output = vec![0u8; decoded_len(input.as_bytes()).unwrap_or_default()];
            let decoded_len = decode(input.as_bytes(), &mut output);
            assert_eq!(decoded_len, Some(expected.len()));
            assert_eq!(&output[..decoded_len.unwrap_or_default()], *expected);
        }
    }

    #[test]
    fn length_helpers_size_buffers() {
        assert_eq!(encoded_len(0), 0);
        assert_eq!(encoded_len(1), 4);
        assert_eq!(encoded_len(2), 4);
        assert_eq!(encoded_len(3), 4);
        assert_eq!(encoded_len(4), 8);
        assert_eq!(encoded_len(5), 8);

        assert_eq!(decoded_len(b""), Some(0));
        assert_eq!(decoded_len(b"Zg=="), Some(1));
        assert_eq!(decoded_len(b"Zm8="), Some(2));
        assert_eq!(decoded_len(b"Zm9v"), Some(3));
        assert_eq!(decoded_len(b"Zm9vYg=="), Some(4));
        assert!(decoded_len(b"Zg").is_none());
        assert!(decoded_len(b"Zg=").is_none());
    }

    #[test]
    fn rejects_invalid_base64() {
        let mut output = [0u8; 8];
        assert!(decode(b"Zg=", &mut output).is_none());
        assert!(decode(b"Z===", &mut output).is_none());
        assert!(decode(b"Zm=8", &mut output).is_none());
        assert!(decode(b"!!!!", &mut output).is_none());
        assert!(decode(b"Zg===", &mut output).is_none());
        assert!(decode(b"=Zm8", &mut output).is_none());
        assert!(decode(b"AA==AA==", &mut output).is_none());
    }

    #[test]
    fn requires_sufficient_output_buffer() {
        assert_eq!(encode(b"f", &mut [0u8; 3]), None);
        assert_eq!(decode(b"Zm9vYmFy", &mut [0u8; 2]), None);
    }

    proptest! {
        #[test]
        fn matches_reference_encoder(input in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let expected = STANDARD.encode(&input);
            let mut encoded = vec![0u8; encoded_len(input.len())];
            prop_assert_eq!(encode(&input, &mut encoded), Some(expected.len()));
            prop_assert_eq!(&encoded, expected.as_bytes());
        }

        #[test]
        fn decode_roundtrips_encoded_input(input in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let encoded = STANDARD.encode(&input);
            let mut decoded = vec![0u8; decoded_len(encoded.as_bytes()).unwrap_or_default()];
            let decoded_len = decode(encoded.as_bytes(), &mut decoded);
            prop_assert_eq!(decoded_len, Some(input.len()));
            prop_assert_eq!(&decoded[..decoded_len.unwrap_or_default()], input.as_slice());
        }

        #[test]
        fn encode_requires_exact_or_larger_output(input in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let needed = encoded_len(input.len());
            if needed > 0 {
                let mut too_small = vec![0u8; needed - 1];
                prop_assert_eq!(encode(&input, &mut too_small), None);
            }

            let mut exact = vec![0u8; needed];
            prop_assert_eq!(encode(&input, &mut exact), Some(needed));
            prop_assert_eq!(exact, STANDARD.encode(&input).into_bytes());
        }

        #[test]
        fn decode_requires_exact_or_larger_output(input in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let encoded = STANDARD.encode(&input);
            let needed = decoded_len(encoded.as_bytes()).expect("reference encoder emits padded base64");
            if needed > 0 {
                let mut too_small = vec![0u8; needed - 1];
                prop_assert_eq!(decode(encoded.as_bytes(), &mut too_small), None);
            }

            let mut exact = vec![0u8; needed];
            let written = decode(encoded.as_bytes(), &mut exact);
            prop_assert_eq!(written, Some(input.len()));
            prop_assert_eq!(&exact[..written.unwrap_or_default()], input.as_slice());
        }

        #[test]
        fn rejects_invalid_base64_alphabet(
            input in proptest::collection::vec(any::<u8>(), 1..4096),
            index in any::<usize>(),
        ) {
            let mut encoded = STANDARD.encode(&input).into_bytes();
            let index = index % encoded.len();
            encoded[index] = b'!';
            let mut output = vec![0u8; decoded_len(&encoded).unwrap_or_default()];

            prop_assert!(decode(&encoded, &mut output).is_none());
        }

        #[test]
        fn rejects_padding_before_final_quantum(
            input in proptest::collection::vec(any::<u8>(), 7..4096),
            index in any::<usize>(),
        ) {
            let mut encoded = STANDARD.encode(&input).into_bytes();
            let non_final = encoded.len().saturating_sub(4);
            prop_assume!(non_final > 0);
            encoded[index % non_final] = b'=';
            let mut output = vec![0u8; decoded_len(&encoded).unwrap_or_default()];

            prop_assert!(decode(&encoded, &mut output).is_none());
        }

        #[test]
        fn decode_acceptance_matches_reference_for_quad_aligned_bytes(
            chunks in proptest::collection::vec(any::<[u8; 4]>(), 0..1024),
        ) {
            let input = chunks.into_iter().flatten().collect::<Vec<_>>();

            let expected = reference_decode(&input);
            let mut output = vec![0u8; decoded_len(&input).unwrap_or_default()];
            let actual = decode(&input, &mut output).is_some();

            prop_assert_eq!(actual, expected);
        }
    }

    fn reference_decode(input: &[u8]) -> bool {
        let mut output = vec![0u8; base64::decoded_len_estimate(input.len())];
        STANDARD.decode_slice(input, &mut output).is_ok()
    }
}
