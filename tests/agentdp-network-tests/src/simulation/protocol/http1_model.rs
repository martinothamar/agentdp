#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseTrigger {
    CompleteRequest,
    Headers,
}

#[derive(Debug, Default)]
pub(crate) struct HttpResponseCompletion {
    request: HttpResponseRequest,
    body: Option<HttpResponseBodyCompletion>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum HttpResponseRequest {
    Head,
    Connect,
    #[default]
    Other,
}

#[derive(Debug)]
enum HttpResponseBodyCompletion {
    HeadersOnly { complete_at: usize },
    ContentLength { complete_at: usize },
    Chunked { cursor: usize },
    CloseDelimited,
}

enum HttpResponseBody {
    Informational,
    HeadersOnly,
    ContentLength(usize),
    Chunked,
    CloseDelimited,
}

pub(crate) fn http_message_complete(bytes: &[u8]) -> bool {
    request_headers_len(bytes).is_some()
}

pub(crate) fn http_request_complete(bytes: &[u8]) -> bool {
    request_message_len(bytes).is_some()
}

pub(crate) fn response_ready_count(bytes: &[u8], trigger: ResponseTrigger) -> usize {
    match trigger {
        ResponseTrigger::CompleteRequest => complete_request_count(bytes),
        ResponseTrigger::Headers => request_headers_count(bytes),
    }
}

pub(crate) fn complete_request_count(mut bytes: &[u8]) -> usize {
    let mut count = 0_usize;
    while let Some(len) = request_message_len(bytes) {
        count = count.saturating_add(1);
        bytes = &bytes[len..];
    }
    count
}

pub(crate) fn request_headers_count(mut bytes: &[u8]) -> usize {
    let mut count = 0_usize;
    while let Some(header_len) = request_headers_len(bytes) {
        count = count.saturating_add(1);
        let Some(message_len) = request_message_len(bytes) else {
            break;
        };
        bytes = if message_len <= header_len {
            &bytes[header_len..]
        } else {
            &bytes[message_len..]
        };
    }
    count
}

pub(crate) fn request_message_len(bytes: &[u8]) -> Option<usize> {
    let header_len = request_headers_len(bytes)?;
    let headers = String::from_utf8_lossy(&bytes[..header_len]);
    if is_chunked(headers.as_ref()) {
        return chunked_message_len(&bytes[header_len..]).map(|body_len| header_len + body_len);
    }
    let Some(content_length) = content_length(headers.as_ref()) else {
        return Some(header_len);
    };
    let len = header_len + content_length;
    (bytes.len() >= len).then_some(len)
}

pub(crate) fn request_headers_len(bytes: &[u8]) -> Option<usize> {
    find_header_end(bytes).map(|header_end| header_end + b"\r\n\r\n".len())
}

pub(crate) fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

pub(crate) struct HttpSecretSubstitution<'a> {
    pub(crate) placeholder: &'a [u8],
    pub(crate) value: &'a [u8],
}

pub(crate) fn model_intercepted_http_request<'a>(
    request: &[u8],
    substitutions: impl IntoIterator<Item = HttpSecretSubstitution<'a>>,
) -> Vec<u8> {
    let Some(header_end) = find_header_end(request) else {
        return request.to_vec();
    };
    let body_offset = header_end + b"\r\n\r\n".len();
    let original_body_len = request.len().saturating_sub(body_offset);
    let substitute_body = should_substitute_body(&request[..body_offset], original_body_len);
    let substitutions = substitutions.into_iter().collect::<Vec<_>>();
    let mut modeled = request.to_vec();

    if substitute_body {
        for substitution in substitutions {
            replace_all(&mut modeled, substitution.placeholder, substitution.value);
        }
        if let Some(body_offset) = find_header_end(&modeled).map(|header_end| header_end + b"\r\n\r\n".len()) {
            let body_len = modeled.len().saturating_sub(body_offset);
            if body_len != original_body_len {
                update_content_length(&mut modeled, body_len);
            }
        }
        return modeled;
    }

    let mut headers = modeled[..body_offset].to_vec();
    for substitution in substitutions {
        replace_all(&mut headers, substitution.placeholder, substitution.value);
    }
    modeled.splice(..body_offset, headers);
    modeled
}

impl HttpResponseCompletion {
    pub(crate) fn for_request(request: &[u8]) -> Self {
        Self {
            request: http_response_request(request),
            body: None,
        }
    }

    pub(crate) fn is_complete(&mut self, bytes: &[u8]) -> bool {
        if self.body.is_none() {
            let mut cursor = 0;
            loop {
                let Some(header_len) = response_headers_len(&bytes[cursor..]).map(|len| cursor + len) else {
                    return false;
                };
                let headers = String::from_utf8_lossy(&bytes[cursor..header_len]);
                let Some(body) = response_body(self.request, headers.as_ref()) else {
                    return false;
                };
                self.body = match body {
                    HttpResponseBody::Informational => {
                        cursor = header_len;
                        continue;
                    }
                    HttpResponseBody::HeadersOnly => Some(HttpResponseBodyCompletion::HeadersOnly {
                        complete_at: header_len,
                    }),
                    HttpResponseBody::ContentLength(content_length) => {
                        Some(HttpResponseBodyCompletion::ContentLength {
                            complete_at: header_len + content_length,
                        })
                    }
                    HttpResponseBody::Chunked => Some(HttpResponseBodyCompletion::Chunked { cursor: header_len }),
                    HttpResponseBody::CloseDelimited => Some(HttpResponseBodyCompletion::CloseDelimited),
                };
                break;
            }
        }

        let Some(body) = self.body.as_mut() else {
            return false;
        };
        match body {
            HttpResponseBodyCompletion::HeadersOnly { complete_at }
            | HttpResponseBodyCompletion::ContentLength { complete_at } => bytes.len() >= *complete_at,
            HttpResponseBodyCompletion::Chunked { cursor } => chunked_response_complete_from(bytes, cursor),
            HttpResponseBodyCompletion::CloseDelimited => false,
        }
    }

    pub(crate) fn is_complete_on_eof(&mut self, bytes: &[u8]) -> bool {
        self.is_complete(bytes) || matches!(self.body, Some(HttpResponseBodyCompletion::CloseDelimited))
    }
}

pub(crate) fn http_response_body(bytes: &[u8]) -> Option<Vec<u8>> {
    http_response_body_for_request(b"", bytes)
}

pub(crate) fn http_response_body_for_request(request: &[u8], bytes: &[u8]) -> Option<Vec<u8>> {
    let request = http_response_request(request);
    let mut cursor = 0;
    loop {
        let header_len = response_headers_len(&bytes[cursor..]).map(|len| cursor + len)?;
        let headers = String::from_utf8_lossy(&bytes[cursor..header_len]);
        match response_body(request, headers.as_ref())? {
            HttpResponseBody::Informational => {
                cursor = header_len;
            }
            HttpResponseBody::HeadersOnly => return (bytes.len() == header_len).then(Vec::new),
            HttpResponseBody::ContentLength(content_length) => {
                let body = &bytes[header_len..];
                return (body.len() >= content_length).then(|| body[..content_length].to_vec());
            }
            HttpResponseBody::Chunked => return chunked_body(&bytes[header_len..]),
            HttpResponseBody::CloseDelimited => return Some(bytes[header_len..].to_vec()),
        }
    }
}

fn http_response_request(request: &[u8]) -> HttpResponseRequest {
    let Some(header_len) = request_headers_len(request) else {
        return HttpResponseRequest::Other;
    };
    let Ok(headers) = std::str::from_utf8(&request[..header_len]) else {
        return HttpResponseRequest::Other;
    };
    let Some(method) = headers.lines().next().and_then(|line| line.split_whitespace().next()) else {
        return HttpResponseRequest::Other;
    };
    match method {
        method if method.eq_ignore_ascii_case("HEAD") => HttpResponseRequest::Head,
        method if method.eq_ignore_ascii_case("CONNECT") => HttpResponseRequest::Connect,
        _ => HttpResponseRequest::Other,
    }
}

fn response_headers_len(bytes: &[u8]) -> Option<usize> {
    request_headers_len(bytes)
}

fn response_body(request: HttpResponseRequest, headers: &str) -> Option<HttpResponseBody> {
    let status = response_status(headers)?;
    if status == 101 {
        return Some(HttpResponseBody::HeadersOnly);
    }
    if (100..200).contains(&status) {
        return Some(HttpResponseBody::Informational);
    }
    if request == HttpResponseRequest::Head || status == 204 || status == 304 {
        return Some(HttpResponseBody::HeadersOnly);
    }
    if request == HttpResponseRequest::Connect && (200..300).contains(&status) {
        return Some(HttpResponseBody::HeadersOnly);
    }
    if is_chunked(headers) {
        Some(HttpResponseBody::Chunked)
    } else if let Some(length) = content_length(headers) {
        Some(HttpResponseBody::ContentLength(length))
    } else {
        Some(HttpResponseBody::CloseDelimited)
    }
}

fn response_status(headers: &str) -> Option<u16> {
    let line = headers.lines().next()?;
    let mut parts = line.split_whitespace();
    let version = parts.next()?;
    if !version.starts_with("HTTP/") {
        return None;
    }
    parts.next()?.parse().ok()
}

fn content_length(headers: &str) -> Option<usize> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse::<usize>().ok())
            .flatten()
    })
}

fn is_chunked(headers: &str) -> bool {
    headers.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("transfer-encoding")
                && value
                    .split(',')
                    .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        })
    })
}

fn should_substitute_body(headers: &[u8], body_len: usize) -> bool {
    !has_header_value(headers, "transfer-encoding", |value| {
        value
            .split(',')
            .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
    }) && !has_header_value(headers, "expect", |value| {
        value
            .split(',')
            .any(|expectation| expectation.trim().eq_ignore_ascii_case("100-continue"))
    }) && body_len <= 1024 * 1024
}

fn has_header_value(headers: &[u8], name: &str, predicate: impl Fn(&str) -> bool) -> bool {
    let Ok(headers) = std::str::from_utf8(headers) else {
        return false;
    };
    headers.lines().any(|line| {
        line.split_once(':')
            .is_some_and(|(header, value)| header.eq_ignore_ascii_case(name) && predicate(value.trim()))
    })
}

fn replace_all(bytes: &mut Vec<u8>, needle: &[u8], replacement: &[u8]) {
    if needle.is_empty() {
        return;
    }
    let mut output = Vec::with_capacity(bytes.len());
    let mut cursor = bytes.as_slice();
    while let Some(index) = cursor.windows(needle.len()).position(|window| window == needle) {
        output.extend_from_slice(&cursor[..index]);
        output.extend_from_slice(replacement);
        cursor = &cursor[index + needle.len()..];
    }
    output.extend_from_slice(cursor);
    *bytes = output;
}

fn update_content_length(request: &mut Vec<u8>, body_len: usize) {
    let Some(header_end) = find_header_end(request) else {
        return;
    };
    let Ok(headers) = std::str::from_utf8(&request[..header_end]) else {
        return;
    };
    let mut output = String::with_capacity(headers.len());
    for (index, line) in headers.split("\r\n").enumerate() {
        if index > 0 {
            output.push_str("\r\n");
        }
        if line
            .split_once(':')
            .is_some_and(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        {
            output.push_str("Content-Length: ");
            output.push_str(&body_len.to_string());
        } else {
            output.push_str(line);
        }
    }
    let mut modeled = output.into_bytes();
    modeled.extend_from_slice(b"\r\n\r\n");
    modeled.extend_from_slice(&request[header_end + b"\r\n\r\n".len()..]);
    *request = modeled;
}

fn chunked_response_complete_from(bytes: &[u8], cursor: &mut usize) -> bool {
    loop {
        let Some(remaining) = bytes.get(*cursor..) else {
            return false;
        };
        let Some(line_end) = find_crlf(remaining) else {
            return false;
        };
        let size_line = &bytes[*cursor..*cursor + line_end];
        let Ok(size_line) = std::str::from_utf8(size_line) else {
            return false;
        };
        let Some(size) = chunk_size(size_line) else {
            return false;
        };
        let data_start = *cursor + line_end + 2;
        if size == 0 {
            return complete_trailers(&bytes[data_start..]);
        }
        let Some(after_data) = data_start.checked_add(size + 2) else {
            return false;
        };
        if bytes.len() < after_data || &bytes[data_start + size..after_data] != b"\r\n" {
            return false;
        }
        *cursor = after_data;
    }
}

fn chunked_message_len(body: &[u8]) -> Option<usize> {
    parse_chunked_body(body, |_| {}).map(|parsed| parsed.message_len)
}

fn chunked_body(body: &[u8]) -> Option<Vec<u8>> {
    let mut output = Vec::new();
    parse_chunked_body(body, |chunk| output.extend_from_slice(chunk)).map(|_parsed| output)
}

struct ParsedChunkedBody {
    message_len: usize,
}

fn parse_chunked_body(body: &[u8], mut on_chunk: impl FnMut(&[u8])) -> Option<ParsedChunkedBody> {
    let mut cursor = 0;
    loop {
        let line_end = find_crlf(&body[cursor..])?;
        let size_line = std::str::from_utf8(&body[cursor..cursor + line_end]).ok()?;
        let size = chunk_size(size_line)?;
        cursor += line_end + 2;
        if size == 0 {
            let trailers = body.get(cursor..)?;
            let trailer_len = trailers_len(trailers)?;
            return Some(ParsedChunkedBody {
                message_len: cursor + trailer_len,
            });
        }
        let after_data = cursor.checked_add(size + 2)?;
        if body.len() < after_data || &body[cursor + size..after_data] != b"\r\n" {
            return None;
        }
        on_chunk(&body[cursor..cursor + size]);
        cursor = after_data;
    }
}

fn chunk_size(size_line: &str) -> Option<usize> {
    size_line
        .split(';')
        .next()
        .and_then(|size| usize::from_str_radix(size.trim(), 16).ok())
}

fn complete_trailers(bytes: &[u8]) -> bool {
    trailers_len(bytes).is_some()
}

fn trailers_len(bytes: &[u8]) -> Option<usize> {
    if bytes.starts_with(b"\r\n") {
        return Some(2);
    }
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

fn find_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|window| window == b"\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_without_body_completes_at_headers() {
        let request = b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\ntrailing";

        assert_eq!(request_headers_len(request), Some(38));
        assert_eq!(request_message_len(request), Some(38));
        assert_eq!(complete_request_count(request), 1);
    }

    #[test]
    fn fixed_body_request_waits_for_full_body() {
        let partial = b"POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nabc";
        let complete = b"POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nabcde";

        assert_eq!(request_message_len(partial), None);
        assert_eq!(request_message_len(complete), Some(43));
    }

    #[test]
    fn body_header_delimiter_does_not_count_as_second_request() {
        let request = b"POST / HTTP/1.1\r\nContent-Length: 12\r\n\r\nabc\r\n\r\ndef12";

        assert_eq!(response_ready_count(request, ResponseTrigger::Headers), 1);
        assert_eq!(complete_request_count(request), 1);
    }

    #[test]
    fn pipelined_requests_count_independently() {
        let requests = b"GET /a HTTP/1.1\r\nHost: example.test\r\n\r\nGET /b HTTP/1.1\r\nHost: example.test\r\n\r\n";

        assert_eq!(response_ready_count(requests, ResponseTrigger::Headers), 2);
        assert_eq!(response_ready_count(requests, ResponseTrigger::CompleteRequest), 2);
    }

    #[test]
    fn chunked_request_waits_for_terminal_chunk() {
        let complete = b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n0\r\n\r\n";
        let incomplete = b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n";

        assert_eq!(request_message_len(complete), Some(61));
        assert_eq!(request_message_len(incomplete), None);
    }

    #[test]
    fn chunk_data_header_delimiter_does_not_count_as_second_request() {
        let request = b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n7\r\nab\r\n\r\nc\r\n0\r\n\r\n";

        assert_eq!(response_ready_count(request, ResponseTrigger::Headers), 1);
        assert_eq!(complete_request_count(request), 1);
    }

    #[test]
    fn content_length_response_completion_and_body() {
        let response = b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhelloextra";
        let mut completion = HttpResponseCompletion::default();

        assert!(completion.is_complete(response));
        assert_eq!(http_response_body(response), Some(b"hello".to_vec()));
    }

    #[test]
    fn chunked_response_completion_and_body() {
        let response = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nhel\r\n2\r\nlo\r\n0\r\n\r\n";
        let mut completion = HttpResponseCompletion::default();

        assert!(completion.is_complete(response));
        assert_eq!(http_response_body(response), Some(b"hello".to_vec()));
    }

    #[test]
    fn head_response_body_rejects_extra_bytes_after_headers() {
        let request = b"HEAD / HTTP/1.1\r\nHost: example.test\r\n\r\n";
        let response = b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello";

        assert_eq!(http_response_body_for_request(request, response), None);
    }

    #[test]
    fn informational_response_waits_for_final_response() {
        let response = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello";
        let mut completion = HttpResponseCompletion::default();

        assert!(completion.is_complete(response));
        assert_eq!(http_response_body(response), Some(b"hello".to_vec()));
    }

    #[test]
    fn no_body_status_rejects_extra_response_bytes() {
        let response = b"HTTP/1.1 204 No Content\r\ncontent-length: 5\r\n\r\nhello";
        let mut completion = HttpResponseCompletion::default();

        assert!(completion.is_complete(response));
        assert_eq!(http_response_body(response), None);
    }

    #[test]
    fn close_delimited_response_completes_on_eof() {
        let response = b"HTTP/1.1 200 OK\r\n\r\nstreamed";
        let mut completion = HttpResponseCompletion::default();

        assert!(!completion.is_complete(response));
        assert!(completion.is_complete_on_eof(response));
        assert_eq!(http_response_body(response), Some(b"streamed".to_vec()));
    }
}
