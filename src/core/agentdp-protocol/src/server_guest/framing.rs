use crate::{Error, jsonl};

use super::{GuestMessage, HostMessage};

/// Encodes a guest control-channel message as one compact JSON object followed by a newline.
///
/// # Errors
///
/// Returns an error when the value cannot be serialized.
pub fn encode_guest_message_line(message: &GuestMessage) -> Result<Vec<u8>, Error> {
    jsonl::encode(message)
}

/// Encodes a host control-channel message as one compact JSON object followed by a newline.
///
/// # Errors
///
/// Returns an error when the value cannot be serialized.
pub fn encode_host_message_line(message: &HostMessage) -> Result<Vec<u8>, Error> {
    jsonl::encode(message)
}

/// Decodes a guest control-channel JSONL message sent by the guest.
///
/// # Errors
///
/// Returns an error when the line is not a valid guest message.
pub fn decode_guest_message_line(line: &[u8]) -> Result<GuestMessage, Error> {
    jsonl::decode(line)
}

/// Decodes a host control-channel JSONL message sent by the host.
///
/// # Errors
///
/// Returns an error when the line is not a valid host message.
pub fn decode_host_message_line(line: &[u8]) -> Result<HostMessage, Error> {
    jsonl::decode(line)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::Error;

    use super::{
        decode_guest_message_line, decode_host_message_line, encode_guest_message_line, encode_host_message_line,
    };
    use crate::server_guest::{
        GUEST_CONTROL_PROTOCOL_VERSION, GuestHello, GuestMessage, GuestMessageKind, GuestdRole, HostAccept,
        HostMessage, HostMessageKind,
    };

    #[test]
    fn encoded_guest_messages_end_with_newline() {
        let line = encode_guest_message_line(&guest_hello("msg_0")).expect("encode guest line");

        assert!(line.ends_with(b"\n"));
        assert_eq!(
            decode_guest_message_line(&line).expect("decode guest line"),
            guest_hello("msg_0")
        );
    }

    #[test]
    fn rejects_invalid_json_payloads() {
        let line = br#"{"id":"msg_0","type":"guest.hello","payload":"#;

        let error = decode_guest_message_line(line).expect_err("invalid json payload must fail");

        assert!(matches!(error, Error::Decode(_)));
    }

    #[test]
    fn rejects_unknown_guest_message_types() {
        let line = br#"{"id":"msg_0","type":"guest.nope","payload":{}}"#;

        let error = decode_guest_message_line(line).expect_err("unknown guest message type must fail");

        assert!(matches!(error, Error::Decode(_)));
    }

    #[test]
    fn rejects_unknown_host_message_types() {
        let line = br#"{"id":"msg_0","type":"host.nope","payload":{}}"#;

        let error = decode_host_message_line(line).expect_err("unknown host message type must fail");

        assert!(matches!(error, Error::Decode(_)));
    }

    proptest! {
        #[test]
        fn arbitrary_guest_messages_round_trip(message in guest_message()) {
            let line = encode_guest_message_line(&message).expect("encode guest line");

            prop_assert!(line.ends_with(b"\n"));
            prop_assert_eq!(decode_guest_message_line(&line).expect("decode guest line"), message);
        }

        #[test]
        fn arbitrary_host_messages_round_trip(message in host_message()) {
            let line = encode_host_message_line(&message).expect("encode host line");

            prop_assert!(line.ends_with(b"\n"));
            prop_assert_eq!(decode_host_message_line(&line).expect("decode host line"), message);
        }

        #[test]
        fn arbitrary_bytes_do_not_panic(input in prop::collection::vec(any::<u8>(), 0..512)) {
            let _guest_result = decode_guest_message_line(&input);
            let _host_result = decode_host_message_line(&input);
        }
    }

    fn guest_message() -> impl Strategy<Value = GuestMessage> {
        (
            bounded_text(),
            bounded_text(),
            bounded_text(),
            bounded_text(),
            bounded_text(),
            bounded_text(),
            bounded_text(),
        )
            .prop_map(|(id, guestd_version, manifest, instance, os, hostname, user)| {
                GuestMessage::new(
                    id,
                    GuestMessageKind::Hello(GuestHello {
                        protocol_version: GUEST_CONTROL_PROTOCOL_VERSION,
                        guestd_role: GuestdRole::System,
                        guestd_version,
                        manifest,
                        instance,
                        os,
                        hostname,
                        user,
                    }),
                )
            })
    }

    fn host_message() -> impl Strategy<Value = HostMessage> {
        (bounded_text(), bounded_text()).prop_map(|(id, instance)| {
            HostMessage::new(
                id,
                HostMessageKind::Accept(HostAccept {
                    instance,
                    protocol_version: GUEST_CONTROL_PROTOCOL_VERSION,
                }),
            )
        })
    }

    fn bounded_text() -> impl Strategy<Value = String> {
        "[A-Za-z0-9._/ -]{0,48}"
    }

    fn guest_hello(id: &str) -> GuestMessage {
        GuestMessage::new(
            id,
            GuestMessageKind::Hello(GuestHello {
                protocol_version: GUEST_CONTROL_PROTOCOL_VERSION,
                guestd_role: GuestdRole::System,
                guestd_version: "0.1.0".to_owned(),
                manifest: "basic".to_owned(),
                instance: "basic-0".to_owned(),
                os: "linux".to_owned(),
                hostname: "basic-0".to_owned(),
                user: "agent".to_owned(),
            }),
        )
    }
}
