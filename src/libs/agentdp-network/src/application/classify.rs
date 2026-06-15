#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplicationProtocol {
    Dns,
    Http1,
    Http2,
    Grpc,
    WebSocket,
    Quic,
    Http3,
    Raw,
    NeedMoreBytes,
}

pub(crate) fn classify_plain_tcp(bytes: &[u8]) -> ApplicationProtocol {
    // TODO: Replace this preface-only HTTP/2 signal with stream-aware classification once
    // HTTP/2 routing/substitution is implemented. Correct HTTP/2 handling needs frame
    // decoding and per-stream metadata, not just the connection preface.
    if super::http2::looks_like_h2c(bytes) {
        return ApplicationProtocol::Http2;
    }
    if !bytes.contains(&b'\n') {
        return ApplicationProtocol::NeedMoreBytes;
    }
    if super::http1::is_websocket_upgrade_request(bytes) {
        return ApplicationProtocol::WebSocket;
    }
    if super::http1::is_grpc_request(bytes) {
        return ApplicationProtocol::Grpc;
    }
    if super::http1::looks_like_http1(bytes) {
        return ApplicationProtocol::Http1;
    }
    ApplicationProtocol::Raw
}

pub(crate) fn classify_udp_datagram(bytes: &[u8]) -> ApplicationProtocol {
    if super::dns::has_dns_question(bytes) {
        return ApplicationProtocol::Dns;
    }
    // TODO: Replace this QUIC long-header signal with connection-aware classification once
    // HTTP/3 routing/substitution is implemented. Correct HTTP/3 handling needs QUIC
    // connection state, TLS ALPN, and stream metadata.
    if super::http3::looks_like_http3_candidate(bytes) {
        return ApplicationProtocol::Http3;
    }
    if super::quic::looks_like_quic_initial(bytes) {
        return ApplicationProtocol::Quic;
    }
    ApplicationProtocol::Raw
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::test_support::unit::dns_query;

    use super::{ApplicationProtocol, classify_plain_tcp, classify_udp_datagram};

    #[test]
    fn classifies_plain_tcp_protocols() {
        assert_eq!(
            classify_plain_tcp(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"),
            ApplicationProtocol::Http2
        );
        assert_eq!(classify_plain_tcp(b"GET /"), ApplicationProtocol::NeedMoreBytes);
        assert_eq!(
            classify_plain_tcp(b"GET /ws HTTP/1.1\r\nHost: example.test\r\nUpgrade: websocket\r\n\r\n"),
            ApplicationProtocol::WebSocket
        );
        assert_eq!(
            classify_plain_tcp(b"POST /svc HTTP/1.1\r\nContent-Type: application/grpc\r\n\r\n"),
            ApplicationProtocol::Grpc
        );
        assert_eq!(
            classify_plain_tcp(b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n"),
            ApplicationProtocol::Http1
        );
        assert_eq!(classify_plain_tcp(b"not http\n"), ApplicationProtocol::Raw);
    }

    #[test]
    fn classifies_udp_protocols() {
        assert_eq!(
            classify_udp_datagram(&dns_query(7, "example.test", 1)),
            ApplicationProtocol::Dns
        );
        assert_eq!(classify_udp_datagram(&[0xc0, 0, 0, 0]), ApplicationProtocol::Http3);
        assert_eq!(classify_udp_datagram(b"hello"), ApplicationProtocol::Raw);
    }

    proptest! {
        #[test]
        fn arbitrary_datagrams_do_not_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _protocol = classify_udp_datagram(&bytes);
            let _protocol = classify_plain_tcp(&bytes);
        }
    }
}
