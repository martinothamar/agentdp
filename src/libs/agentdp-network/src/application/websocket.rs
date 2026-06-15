pub(crate) fn is_switching_protocols_response(headers: &[u8]) -> bool {
    let Ok(headers) = std::str::from_utf8(headers) else {
        return false;
    };
    let mut lines = headers.lines();
    let Some(status_line) = lines.next() else {
        return false;
    };
    let mut parts = status_line.split_whitespace();
    if parts.next() != Some("HTTP/1.1") || parts.next() != Some("101") {
        return false;
    }
    lines.any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("upgrade") && value.trim().eq_ignore_ascii_case("websocket")
        })
    })
}

#[cfg(test)]
mod tests {
    use super::is_switching_protocols_response;

    #[test]
    fn recognizes_switching_protocols_response() {
        assert!(is_switching_protocols_response(
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n"
        ));
        assert!(!is_switching_protocols_response(
            b"HTTP/1.1 200 OK\r\nUpgrade: websocket\r\n\r\n"
        ));
        assert!(!is_switching_protocols_response(b"\xff"));
    }
}
