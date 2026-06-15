#![no_main]

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let expected = STANDARD.encode(input);
    let mut output = vec![0u8; agentdp_base64::encoded_len(input.len())];
    assert_eq!(agentdp_base64::encode(input, &mut output), Some(expected.len()));
    assert_eq!(output, expected.as_bytes());
});
