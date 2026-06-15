#![no_main]

use agentdp_protocol::client_server::{decode_request, decode_server_message};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    if let Ok(line) = std::str::from_utf8(input) {
        let _ = decode_request(line);
        let _ = decode_server_message(line);
    }
});
