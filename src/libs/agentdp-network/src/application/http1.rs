use std::collections::VecDeque;
use std::io;

use super::{raw, websocket};
use crate::RuntimeSecrets;
use crate::buffers::BufferPool;

const RELAY_BUF_SIZE: usize = 16_384;
const MAX_BUFFERED_HTTP_HEADERS: usize = 64 * 1024;
const MAX_BUFFERED_HTTP_REQUEST: usize = 1024 * 1024;

pub(crate) fn looks_like_http1(bytes: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    let Some(line) = text.lines().next() else {
        return false;
    };
    let mut parts = line.split_whitespace();
    let (Some(method), Some(_target), Some(version)) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    valid_http_token(method) && (version == "HTTP/1.0" || version == "HTTP/1.1")
}

pub(crate) fn is_websocket_upgrade_request(bytes: &[u8]) -> bool {
    looks_like_http1(bytes) && is_protocol_upgrade(bytes)
}

pub(crate) fn is_grpc_request(bytes: &[u8]) -> bool {
    // TODO: Classify gRPC through the HTTP/2 path once that path exists. This only catches
    // impossible/legacy HTTP/1.x-shaped requests and does not make gRPC support correct.
    looks_like_http1(bytes) && header_value(bytes, "content-type").is_some_and(super::grpc::content_type_is_grpc)
}

pub(crate) struct Http1Filter {
    secrets: RuntimeSecrets,
    host: String,
    pending: Vec<u8>,
    header_rewrite: raw::HeaderRewriteScratch,
    body: BodyState,
    upgrade_pending: bool,
    response_tracker: Http1ResponseTracker,
    server_pending: Vec<u8>,
}

struct Http1ResponseTracker {
    pending: Vec<u8>,
    requests: VecDeque<Http1ResponseRequest>,
    state: ResponseState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Http1ResponseRequest {
    Head,
    Connect,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Http1ResponseEof {
    Complete,
    Incomplete,
    Tunnel,
}

impl Http1Filter {
    pub(crate) fn new(secrets: RuntimeSecrets, host: String, buffers: &BufferPool) -> Self {
        Self {
            secrets,
            host,
            pending: Vec::with_capacity(RELAY_BUF_SIZE),
            header_rewrite: raw::HeaderRewriteScratch::new(buffers),
            body: BodyState::Headers,
            upgrade_pending: false,
            response_tracker: Http1ResponseTracker::new(),
            server_pending: Vec::with_capacity(RELAY_BUF_SIZE),
        }
    }

    pub(crate) fn push(&mut self, input: &[u8], output: &mut Vec<u8>) -> io::Result<bool> {
        output.clear();
        if self.upgrade_pending {
            self.pending.extend_from_slice(input);
            self.ensure_pending_limit()?;
            return Ok(false);
        }
        let mut cursor = 0;
        while cursor < input.len() {
            match &mut self.body {
                BodyState::Headers => {
                    self.pending.extend_from_slice(&input[cursor..]);
                    cursor = input.len();
                    self.flush_pending_requests(output)?;
                    self.ensure_pending_limit()?;
                }
                BodyState::Fixed { remaining } => {
                    let take = (*remaining).min(input.len() - cursor);
                    raw::copy(&input[cursor..cursor + take], output);
                    cursor += take;
                    *remaining -= take;
                    if *remaining == 0 {
                        self.body = BodyState::Headers;
                    }
                }
                BodyState::Chunked(parser) => {
                    let (take, complete) = parser.consume(&input[cursor..])?;
                    raw::copy(&input[cursor..cursor + take], output);
                    cursor += take;
                    if complete {
                        self.body = BodyState::Headers;
                    }
                    if take == 0 {
                        break;
                    }
                }
                BodyState::Raw => {
                    raw::process(&input[cursor..], output)?;
                    cursor = input.len();
                }
            }
        }
        Ok(!output.is_empty())
    }

    pub(crate) fn observe_response(&mut self, input: &[u8]) -> io::Result<()> {
        self.response_tracker.observe(input)
    }

    pub(crate) fn response_eof(&self) -> Http1ResponseEof {
        self.response_tracker.eof()
    }

    pub(crate) fn observe_server_plaintext(&mut self, input: &[u8], output: &mut Vec<u8>) -> io::Result<bool> {
        output.clear();
        if !self.upgrade_pending {
            return Ok(false);
        }
        self.server_pending.extend_from_slice(input);
        if self.server_pending.len() > RELAY_BUF_SIZE {
            self.upgrade_pending = false;
            self.server_pending.clear();
            return Ok(false);
        }
        let Some(boundary) = header_boundary(&self.server_pending) else {
            return Ok(false);
        };
        let accepted = websocket::is_switching_protocols_response(&self.server_pending[..boundary]);
        self.upgrade_pending = false;
        self.server_pending.clear();
        if accepted {
            self.body = BodyState::Raw;
            raw::process(&self.pending, output)?;
            self.pending.clear();
        } else {
            self.flush_pending_requests(output)?;
            self.ensure_pending_limit()?;
        }
        Ok(!output.is_empty())
    }

    fn flush_pending_requests(&mut self, output: &mut Vec<u8>) -> io::Result<()> {
        while let Some(boundary) = header_boundary(&self.pending) {
            let body_offset = boundary + b"\r\n\r\n".len();
            let headers = &self.pending[..body_offset];
            let response_request = response_request(headers);
            let secret_mode = if request_authority_matches(&self.pending[..body_offset], self.host.as_str()) {
                SecretMode::Substitute
            } else {
                SecretMode::None
            };
            match request_body(&self.pending[..body_offset]) {
                RequestBody::None => {
                    self.response_tracker.expect_response_for(response_request);
                    substitute_http_for_host_into(
                        &self.secrets,
                        self.host.as_str(),
                        &self.pending[..body_offset],
                        secret_mode.body_mode(),
                        output,
                        &mut self.header_rewrite,
                    )?;
                    self.pending.drain(..body_offset);
                }
                RequestBody::Fixed(length) if length <= MAX_BUFFERED_HTTP_REQUEST => {
                    if is_expect_continue(&self.pending[..body_offset]) {
                        self.response_tracker.expect_response_for(response_request);
                        if !self.stream_fixed_request_body(length, body_offset, secret_mode, output)? {
                            break;
                        }
                        continue;
                    }
                    let request_len = body_offset + length;
                    if self.pending.len() < request_len {
                        break;
                    }
                    self.response_tracker.expect_response_for(response_request);
                    substitute_http_for_host_into(
                        &self.secrets,
                        self.host.as_str(),
                        &self.pending[..request_len],
                        secret_mode.body_mode(),
                        output,
                        &mut self.header_rewrite,
                    )?;
                    self.pending.drain(..request_len);
                }
                RequestBody::Fixed(length) => {
                    self.response_tracker.expect_response_for(response_request);
                    if !self.stream_fixed_request_body(length, body_offset, secret_mode, output)? {
                        break;
                    }
                }
                RequestBody::Chunked => {
                    self.response_tracker.expect_response_for(response_request);
                    substitute_http_for_host_into(
                        &self.secrets,
                        self.host.as_str(),
                        &self.pending[..body_offset],
                        secret_mode.header_mode(),
                        output,
                        &mut self.header_rewrite,
                    )?;
                    let mut parser = ChunkedParser::default();
                    let (body_take, complete) = parser.consume(&self.pending[body_offset..])?;
                    raw::copy(&self.pending[body_offset..body_offset + body_take], output);
                    self.pending.drain(..body_offset + body_take);
                    if !complete {
                        self.body = BodyState::Chunked(parser);
                        break;
                    }
                }
                RequestBody::Upgrade => {
                    self.response_tracker.expect_response_for(response_request);
                    substitute_http_for_host_into(
                        &self.secrets,
                        self.host.as_str(),
                        &self.pending[..body_offset],
                        secret_mode.header_mode(),
                        output,
                        &mut self.header_rewrite,
                    )?;
                    self.pending.drain(..body_offset);
                    self.upgrade_pending = true;
                    self.server_pending.clear();
                    break;
                }
            }
        }
        Ok(())
    }

    fn stream_fixed_request_body(
        &mut self,
        length: usize,
        body_offset: usize,
        secret_mode: SecretMode,
        output: &mut Vec<u8>,
    ) -> io::Result<bool> {
        substitute_http_for_host_into(
            &self.secrets,
            self.host.as_str(),
            &self.pending[..body_offset],
            secret_mode.header_mode(),
            output,
            &mut self.header_rewrite,
        )?;
        let available_body = self.pending.len() - body_offset;
        let body_take = length.min(available_body);
        raw::copy(&self.pending[body_offset..body_offset + body_take], output);
        self.pending.drain(..body_offset + body_take);
        if body_take < length {
            self.body = BodyState::Fixed {
                remaining: length - body_take,
            };
        }
        Ok(body_take == length)
    }

    fn ensure_pending_limit(&self) -> io::Result<()> {
        if header_boundary(&self.pending).is_none() {
            if self.pending.len() > MAX_BUFFERED_HTTP_HEADERS {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "buffered HTTP headers exceeded limit",
                ));
            }
        } else if self.pending.len() > MAX_BUFFERED_HTTP_REQUEST {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "buffered HTTP request exceeded limit",
            ));
        }
        Ok(())
    }
}

impl Http1ResponseTracker {
    fn new() -> Self {
        Self {
            pending: Vec::with_capacity(RELAY_BUF_SIZE),
            requests: VecDeque::new(),
            state: ResponseState::Headers,
        }
    }

    fn expect_response_for(&mut self, request: Http1ResponseRequest) {
        self.requests.push_back(request);
    }

    fn observe(&mut self, input: &[u8]) -> io::Result<()> {
        let mut cursor = 0;
        while cursor < input.len() {
            match &mut self.state {
                ResponseState::Headers => {
                    self.pending.extend_from_slice(&input[cursor..]);
                    cursor = input.len();
                    self.flush_pending_responses()?;
                    self.ensure_response_pending_limit()?;
                }
                ResponseState::Fixed { remaining } => {
                    let take = (*remaining).min(input.len() - cursor);
                    cursor += take;
                    *remaining -= take;
                    if *remaining == 0 {
                        self.state = ResponseState::Headers;
                    }
                }
                ResponseState::Chunked(parser) => {
                    let (take, complete) = parser.consume(&input[cursor..])?;
                    cursor += take;
                    if complete {
                        self.state = ResponseState::Headers;
                    }
                    if take == 0 {
                        break;
                    }
                }
                ResponseState::CloseDelimited | ResponseState::Tunnel => {
                    cursor = input.len();
                }
            }
        }
        Ok(())
    }

    fn eof(&self) -> Http1ResponseEof {
        match &self.state {
            ResponseState::Headers if self.pending.is_empty() && self.requests.is_empty() => Http1ResponseEof::Complete,
            ResponseState::CloseDelimited if self.requests.is_empty() => Http1ResponseEof::Complete,
            ResponseState::Tunnel => Http1ResponseEof::Tunnel,
            ResponseState::Headers
            | ResponseState::Fixed { .. }
            | ResponseState::Chunked(_)
            | ResponseState::CloseDelimited => Http1ResponseEof::Incomplete,
        }
    }

    fn flush_pending_responses(&mut self) -> io::Result<()> {
        while let Some(boundary) = header_boundary(&self.pending) {
            let body_offset = boundary + b"\r\n\r\n".len();
            let headers = &self.pending[..body_offset];
            let request = self.requests.front().copied().unwrap_or(Http1ResponseRequest::Other);
            let (body, consumes_request) = response_body(request, headers)?;
            if consumes_request {
                let _request = self.requests.pop_front();
            }
            self.pending.drain(..body_offset);
            match body {
                ResponseBody::None => {}
                ResponseBody::Fixed(length) => {
                    let take = length.min(self.pending.len());
                    self.pending.drain(..take);
                    if take < length {
                        self.state = ResponseState::Fixed {
                            remaining: length - take,
                        };
                        break;
                    }
                }
                ResponseBody::Chunked => {
                    let mut parser = ChunkedParser::default();
                    let (take, complete) = parser.consume(&self.pending)?;
                    self.pending.drain(..take);
                    if !complete {
                        self.state = ResponseState::Chunked(parser);
                        break;
                    }
                }
                ResponseBody::CloseDelimited => {
                    self.state = ResponseState::CloseDelimited;
                    self.pending.clear();
                    break;
                }
                ResponseBody::Tunnel => {
                    self.state = ResponseState::Tunnel;
                    self.pending.clear();
                    break;
                }
            }
        }
        Ok(())
    }

    fn ensure_response_pending_limit(&self) -> io::Result<()> {
        if header_boundary(&self.pending).is_none() && self.pending.len() > MAX_BUFFERED_HTTP_HEADERS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "buffered HTTP response headers exceeded limit",
            ));
        }
        Ok(())
    }
}

enum BodyState {
    Headers,
    Fixed { remaining: usize },
    Chunked(ChunkedParser),
    Raw,
}

enum ResponseState {
    Headers,
    Fixed { remaining: usize },
    Chunked(ChunkedParser),
    CloseDelimited,
    Tunnel,
}

enum RequestBody {
    None,
    Fixed(usize),
    Chunked,
    Upgrade,
}

enum ResponseBody {
    None,
    Fixed(usize),
    Chunked,
    CloseDelimited,
    Tunnel,
}

fn request_body(headers: &[u8]) -> RequestBody {
    if is_protocol_upgrade(headers) {
        RequestBody::Upgrade
    } else if is_chunked(headers) {
        RequestBody::Chunked
    } else if let Some(length) = content_length(headers) {
        RequestBody::Fixed(length)
    } else {
        RequestBody::None
    }
}

fn response_request(headers: &[u8]) -> Http1ResponseRequest {
    let Ok(headers) = std::str::from_utf8(headers) else {
        return Http1ResponseRequest::Other;
    };
    let Some(line) = headers.lines().next() else {
        return Http1ResponseRequest::Other;
    };
    let Some(method) = line.split_whitespace().next() else {
        return Http1ResponseRequest::Other;
    };
    if method.eq_ignore_ascii_case("HEAD") {
        Http1ResponseRequest::Head
    } else if method.eq_ignore_ascii_case("CONNECT") {
        Http1ResponseRequest::Connect
    } else {
        Http1ResponseRequest::Other
    }
}

fn response_body(request: Http1ResponseRequest, headers: &[u8]) -> io::Result<(ResponseBody, bool)> {
    let status = response_status(headers)?;
    if status == 101 {
        return Ok((ResponseBody::Tunnel, true));
    }
    if (100..200).contains(&status) {
        return Ok((ResponseBody::None, false));
    }
    if request == Http1ResponseRequest::Head || status == 204 || status == 304 {
        return Ok((ResponseBody::None, true));
    }
    if request == Http1ResponseRequest::Connect && (200..300).contains(&status) {
        return Ok((ResponseBody::Tunnel, true));
    }
    if is_chunked(headers) {
        Ok((ResponseBody::Chunked, true))
    } else if let Some(length) = content_length(headers) {
        Ok((ResponseBody::Fixed(length), true))
    } else {
        Ok((ResponseBody::CloseDelimited, true))
    }
}

#[derive(Default)]
struct ChunkedParser {
    state: ChunkedState,
    line: Vec<u8>,
}

#[derive(Default)]
enum ChunkedState {
    #[default]
    Size,
    Data {
        remaining: usize,
    },
    DataCr,
    DataLf,
    Trailer,
    Complete,
}

impl ChunkedParser {
    fn consume(&mut self, input: &[u8]) -> io::Result<(usize, bool)> {
        let mut consumed = 0;
        for &byte in input {
            consumed += 1;
            self.push_byte(byte)?;
            if matches!(self.state, ChunkedState::Complete) {
                return Ok((consumed, true));
            }
        }
        Ok((consumed, false))
    }

    fn push_byte(&mut self, byte: u8) -> io::Result<()> {
        match self.state {
            ChunkedState::Size => {
                self.line.push(byte);
                ensure_chunk_scratch_limit(&self.line)?;
                if byte == b'\n' {
                    self.state = parse_chunk_size(&self.line).map_or(ChunkedState::Trailer, |size| {
                        self.line.clear();
                        if size == 0 {
                            ChunkedState::Trailer
                        } else {
                            ChunkedState::Data { remaining: size }
                        }
                    });
                }
            }
            ChunkedState::Data { remaining } => {
                if remaining <= 1 {
                    self.state = ChunkedState::DataCr;
                } else {
                    self.state = ChunkedState::Data {
                        remaining: remaining - 1,
                    };
                }
            }
            ChunkedState::DataCr => {
                self.state = if byte == b'\r' {
                    ChunkedState::DataLf
                } else {
                    ChunkedState::Trailer
                };
            }
            ChunkedState::DataLf => {
                self.state = if byte == b'\n' {
                    ChunkedState::Size
                } else {
                    ChunkedState::Trailer
                };
            }
            ChunkedState::Trailer => {
                self.line.push(byte);
                ensure_chunk_scratch_limit(&self.line)?;
                if self.line.ends_with(b"\r\n\r\n") || self.line == b"\r\n" {
                    self.state = ChunkedState::Complete;
                }
            }
            ChunkedState::Complete => {}
        }
        Ok(())
    }
}

fn ensure_chunk_scratch_limit(line: &[u8]) -> io::Result<()> {
    if line.len() > MAX_BUFFERED_HTTP_HEADERS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "buffered HTTP chunk metadata exceeded limit",
        ));
    }
    Ok(())
}

fn parse_chunk_size(line: &[u8]) -> Option<usize> {
    let line = std::str::from_utf8(line).ok()?.trim();
    let size = line.split(';').next()?.trim();
    usize::from_str_radix(size, 16).ok()
}

#[derive(Clone, Copy)]
enum SecretMode {
    Substitute,
    None,
}

impl SecretMode {
    const fn body_mode(self) -> BodyMode {
        match self {
            Self::Substitute => BodyMode::Substitute,
            Self::None => BodyMode::None,
        }
    }

    const fn header_mode(self) -> BodyMode {
        match self {
            Self::Substitute => BodyMode::HeadersOnly,
            Self::None => BodyMode::None,
        }
    }
}

#[derive(Clone, Copy)]
enum BodyMode {
    Substitute,
    HeadersOnly,
    None,
}

fn substitute_http_for_host_into(
    secrets: &RuntimeSecrets,
    host: &str,
    input: &[u8],
    body_mode: BodyMode,
    output: &mut Vec<u8>,
    header_rewrite: &mut raw::HeaderRewriteScratch,
) -> io::Result<bool> {
    match body_mode {
        BodyMode::Substitute => substitute_http_request_for_host_into(secrets, host, input, output, header_rewrite),
        BodyMode::HeadersOnly => substitute_http_headers_for_host_into(secrets, host, input, output, header_rewrite),
        BodyMode::None => raw::process(input, output),
    }
}

fn substitute_http_request_for_host_into(
    secrets: &RuntimeSecrets,
    host: &str,
    input: &[u8],
    output: &mut Vec<u8>,
    header_rewrite: &mut raw::HeaderRewriteScratch,
) -> io::Result<bool> {
    let Some(boundary) = header_boundary(input) else {
        raw::ensure_no_placeholders_for_disallowed_host(secrets, host, input).map_err(io::Error::other)?;
        output.extend_from_slice(input);
        return Ok(!output.is_empty());
    };
    let body_offset = boundary + b"\r\n\r\n".len();
    let (headers, body) = input.split_at(body_offset);
    let headers =
        raw::substitute_http_header_bytes_for_host(secrets, host, headers, header_rewrite).map_err(io::Error::other)?;
    let body = raw::substitute_body_bytes_for_host(secrets, host, body);
    if matches!(headers, std::borrow::Cow::Borrowed(_)) && matches!(body, std::borrow::Cow::Borrowed(_)) {
        output.extend_from_slice(input);
        return Ok(!output.is_empty());
    }

    let mut headers = headers.into_owned();
    let body = body.as_ref();
    if body.len() != input.len() - body_offset {
        update_content_length(&mut headers, body.len(), header_rewrite.rewrite());
    }
    output.extend_from_slice(&headers);
    output.extend_from_slice(body);
    Ok(!output.is_empty())
}

fn substitute_http_headers_for_host_into(
    secrets: &RuntimeSecrets,
    host: &str,
    input: &[u8],
    output: &mut Vec<u8>,
    header_rewrite: &mut raw::HeaderRewriteScratch,
) -> io::Result<bool> {
    let Some(boundary) = header_boundary(input) else {
        raw::ensure_no_placeholders_for_disallowed_host(secrets, host, input).map_err(io::Error::other)?;
        output.extend_from_slice(input);
        return Ok(!output.is_empty());
    };
    let body_offset = boundary + b"\r\n\r\n".len();
    let (headers, body) = input.split_at(body_offset);
    let headers =
        raw::substitute_http_header_bytes_for_host(secrets, host, headers, header_rewrite).map_err(io::Error::other)?;
    output.extend_from_slice(headers.as_ref());
    output.extend_from_slice(body);
    Ok(!output.is_empty())
}

fn update_content_length(headers: &mut Vec<u8>, body_len: usize, scratch: &mut Vec<u8>) {
    let Ok(text) = std::str::from_utf8(headers) else {
        return;
    };
    scratch.clear();
    for (index, line) in text.split("\r\n").enumerate() {
        if index > 0 {
            scratch.extend_from_slice(b"\r\n");
        }
        if line
            .split_once(':')
            .is_some_and(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        {
            scratch.extend_from_slice(b"Content-Length: ");
            extend_decimal(scratch, body_len);
        } else {
            scratch.extend_from_slice(line.as_bytes());
        }
    }
    std::mem::swap(headers, scratch);
}

fn extend_decimal(output: &mut Vec<u8>, value: usize) {
    const DIGITS: &[u8; 10] = b"0123456789";

    let mut buffer = [0_u8; 39];
    let mut cursor = buffer.len();
    let mut value = value;
    loop {
        cursor -= 1;
        buffer[cursor] = DIGITS[value % 10];
        value /= 10;
        if value == 0 {
            break;
        }
    }
    output.extend_from_slice(&buffer[cursor..]);
}

fn header_boundary(data: &[u8]) -> Option<usize> {
    data.windows(b"\r\n\r\n".len()).position(|window| window == b"\r\n\r\n")
}

fn content_length(headers: &[u8]) -> Option<usize> {
    let headers = std::str::from_utf8(headers).ok()?;
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())
            .flatten()
    })
}

fn is_chunked(headers: &[u8]) -> bool {
    let Ok(headers) = std::str::from_utf8(headers) else {
        return false;
    };
    headers.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("transfer-encoding")
                && value
                    .split(',')
                    .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        })
    })
}

fn is_expect_continue(headers: &[u8]) -> bool {
    let Ok(headers) = std::str::from_utf8(headers) else {
        return false;
    };
    headers.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("expect")
                && value
                    .split(',')
                    .any(|expectation| expectation.trim().eq_ignore_ascii_case("100-continue"))
        })
    })
}

fn is_protocol_upgrade(headers: &[u8]) -> bool {
    let Ok(headers) = std::str::from_utf8(headers) else {
        return false;
    };
    header_value_text(headers, "upgrade").is_some_and(|value| value.trim().eq_ignore_ascii_case("websocket"))
}

fn response_status(headers: &[u8]) -> io::Result<u16> {
    let headers =
        std::str::from_utf8(headers).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    let Some(line) = headers.lines().next() else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty HTTP response"));
    };
    let mut parts = line.split_whitespace();
    let (Some(version), Some(status)) = (parts.next(), parts.next()) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed HTTP response status line",
        ));
    };
    if version != "HTTP/1.0" && version != "HTTP/1.1" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported HTTP response version",
        ));
    }
    status
        .parse()
        .map_err(|error: std::num::ParseIntError| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

fn header_value<'a>(headers: &'a [u8], expected_name: &str) -> Option<&'a str> {
    let headers = std::str::from_utf8(headers).ok()?;
    header_value_text(headers, expected_name)
}

fn header_value_text<'a>(headers: &'a str, expected_name: &str) -> Option<&'a str> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case(expected_name).then_some(value.trim())
    })
}

fn request_authority_matches(headers: &[u8], expected_host: &str) -> bool {
    let Ok(headers) = std::str::from_utf8(headers) else {
        return false;
    };
    let mut lines = headers.lines();
    let Some(request_line) = lines.next() else {
        return false;
    };
    let mut parts = request_line.split_whitespace();
    let (Some(method), Some(target), Some(version), None) = (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    if !valid_http_token(method) || !(version == "HTTP/1.0" || version == "HTTP/1.1") {
        return false;
    }
    let mut host = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("host") {
            if host.is_some() {
                return false;
            }
            host = normalize_authority(value.trim());
        }
    }
    let Some(target_authority) = request_target_authority(method, target) else {
        return false;
    };
    let target_host = match &target_authority {
        RequestTargetAuthority::OriginForm => None,
        RequestTargetAuthority::Authority(host) => Some(host.as_str()),
    };
    if let (Some(host), Some(target_host)) = (host.as_deref(), target_host)
        && host != target_host
    {
        return false;
    }
    let expected_host = normalize_host(expected_host);
    target_host
        .or(host.as_deref())
        .is_some_and(|host| host == expected_host)
}

fn valid_http_token(token: &str) -> bool {
    !token.is_empty()
        && token.bytes().all(|byte| {
            matches!(
                byte,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
                    | b'0'..=b'9'
                    | b'A'..=b'Z'
                    | b'a'..=b'z'
            )
        })
}

enum RequestTargetAuthority {
    OriginForm,
    Authority(String),
}

fn request_target_authority(method: &str, target: &str) -> Option<RequestTargetAuthority> {
    if method.eq_ignore_ascii_case("CONNECT") {
        return normalize_authority(target).map(RequestTargetAuthority::Authority);
    }
    if !target.contains("://") {
        return Some(RequestTargetAuthority::OriginForm);
    }
    absolute_form_host(target).map(RequestTargetAuthority::Authority)
}

fn absolute_form_host(target: &str) -> Option<String> {
    let (scheme, rest) = target.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return None;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    normalize_authority(authority)
}

fn normalize_authority(authority: &str) -> Option<String> {
    let authority = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, _) = rest.split_once(']')?;
        return Some(host.trim_end_matches('.').to_ascii_lowercase());
    }
    let host = authority.split_once(':').map_or(authority, |(host, _)| host);
    (!host.is_empty()).then(|| normalize_host(host))
}

fn normalize_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use crate::RuntimeSecrets;
    use crate::buffers::BufferPool;

    use super::{
        Http1Filter, Http1ResponseEof, Http1ResponseRequest, Http1ResponseTracker, MAX_BUFFERED_HTTP_HEADERS,
        content_length, is_chunked, is_expect_continue, is_grpc_request, is_websocket_upgrade_request,
        looks_like_http1, request_authority_matches,
    };

    #[test]
    fn detects_http1_websocket_and_grpc_requests() {
        assert!(looks_like_http1(b"GET / HTTP/1.1\r\nHost: allowed.test\r\n\r\n"));
        assert!(!looks_like_http1(b"BAD METHOD / HTTP/1.1\r\n\r\n"));
        assert!(is_websocket_upgrade_request(
            b"GET /ws HTTP/1.1\r\nUpgrade: websocket\r\nHost: allowed.test\r\n\r\n"
        ));
        assert!(is_grpc_request(
            b"POST /svc HTTP/1.1\r\nContent-Type: application/grpc; proto=1\r\n\r\n"
        ));
    }

    #[test]
    fn parses_request_body_headers() {
        assert_eq!(
            content_length(b"POST / HTTP/1.1\r\nContent-Length: 17\r\n\r\n"),
            Some(17)
        );
        assert!(is_chunked(
            b"POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n"
        ));
        assert!(is_expect_continue(b"POST / HTTP/1.1\r\nExpect: 100-continue\r\n\r\n"));
    }

    #[test]
    fn request_authority_matches_origin_absolute_and_connect_forms() {
        assert!(request_authority_matches(
            b"GET /path HTTP/1.1\r\nHost: Allowed.TEST.\r\n\r\n",
            "allowed.test"
        ));
        assert!(request_authority_matches(
            b"GET https://allowed.test:443/path HTTP/1.1\r\nHost: allowed.test:443\r\n\r\n",
            "allowed.test"
        ));
        assert!(request_authority_matches(
            b"CONNECT allowed.test:443 HTTP/1.1\r\nHost: allowed.test\r\n\r\n",
            "allowed.test"
        ));
        assert!(!request_authority_matches(
            b"GET https://blocked.test/path HTTP/1.1\r\nHost: allowed.test\r\n\r\n",
            "allowed.test"
        ));
        assert!(!request_authority_matches(
            b"GET /path HTTP/1.1\r\nHost: allowed.test\r\nHost: duplicate.test\r\n\r\n",
            "allowed.test"
        ));
    }

    #[test]
    fn chunked_body_metadata_is_bounded_after_headers_are_processed() {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        let mut filter = Http1Filter::new(RuntimeSecrets::new(), "allowed.test".to_owned(), &buffers);
        let mut request = b"POST /upload HTTP/1.1\r\nHost: allowed.test\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        request.extend(std::iter::repeat_n(b'a', MAX_BUFFERED_HTTP_HEADERS + 1));
        let mut output = Vec::new();

        let error = filter
            .push(&request, &mut output)
            .expect_err("chunk metadata should be bounded");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("chunk metadata"));
    }

    #[test]
    fn response_tracker_marks_fixed_length_response_complete() {
        let mut tracker = Http1ResponseTracker::new();

        tracker
            .observe(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
            .unwrap();

        assert_eq!(tracker.eof(), Http1ResponseEof::Complete);
    }

    #[test]
    fn response_tracker_marks_fixed_length_response_incomplete() {
        let mut tracker = Http1ResponseTracker::new();

        tracker
            .observe(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhe")
            .unwrap();

        assert_eq!(tracker.eof(), Http1ResponseEof::Incomplete);
    }

    #[test]
    fn response_tracker_marks_missing_response_incomplete() {
        let mut tracker = Http1ResponseTracker::new();
        tracker.expect_response_for(Http1ResponseRequest::Other);

        assert_eq!(tracker.eof(), Http1ResponseEof::Incomplete);
    }

    #[test]
    fn response_tracker_marks_missing_final_response_after_informational_response_incomplete() {
        let mut tracker = Http1ResponseTracker::new();
        tracker.expect_response_for(Http1ResponseRequest::Other);

        tracker.observe(b"HTTP/1.1 100 Continue\r\n\r\n").unwrap();

        assert_eq!(tracker.eof(), Http1ResponseEof::Incomplete);
    }

    #[test]
    fn response_tracker_treats_head_response_content_length_as_complete() {
        let mut tracker = Http1ResponseTracker::new();
        tracker.expect_response_for(Http1ResponseRequest::Head);

        tracker
            .observe(b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\n\r\n")
            .unwrap();

        assert_eq!(tracker.eof(), Http1ResponseEof::Complete);
    }

    #[test]
    fn response_tracker_treats_close_delimited_response_eof_as_complete() {
        let mut tracker = Http1ResponseTracker::new();

        tracker.observe(b"HTTP/1.1 200 OK\r\n\r\nstreamed").unwrap();

        assert_eq!(tracker.eof(), Http1ResponseEof::Complete);
    }

    #[test]
    fn response_tracker_marks_close_delimited_response_with_missing_pipelined_response_incomplete() {
        let mut tracker = Http1ResponseTracker::new();
        tracker.expect_response_for(Http1ResponseRequest::Other);
        tracker.expect_response_for(Http1ResponseRequest::Other);

        tracker.observe(b"HTTP/1.1 200 OK\r\n\r\nstreamed").unwrap();

        assert_eq!(tracker.eof(), Http1ResponseEof::Incomplete);
    }

    #[test]
    fn response_tracker_marks_websocket_upgrade_as_tunnel() {
        let mut tracker = Http1ResponseTracker::new();

        tracker
            .observe(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n")
            .unwrap();

        assert_eq!(tracker.eof(), Http1ResponseEof::Tunnel);
    }
}
