#![no_main]

use agentdp_protocol::server_guest::{decode_guest_message_line, decode_host_message_line};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let _ = decode_guest_message_line(input);
    let _ = decode_host_message_line(input);
});
