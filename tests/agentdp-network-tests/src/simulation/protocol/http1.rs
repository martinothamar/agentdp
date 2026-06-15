use std::cell::Cell;
use std::cell::RefCell;
use std::fmt::Write as _;
use std::io;
use std::io::Write as _;
use std::rc::Rc;
use std::time::Duration;

use agentdp_crypto::{TlsClientSession, TlsServerConfig, TlsServerSession};
use agentdp_network::test_support::simulation::{SimTcpHandler, SimTcpResponse};

use super::super::{
    DriveBudget, DriveGuestProgress, GuestLink, Result, Simulator, SmolTcpGuest, SteppedNetwork, TcpHandle,
};
use super::http1_model::{HttpResponseCompletion, ResponseTrigger, complete_request_count, response_ready_count};
use super::tcp::tcp_response_handler;
use super::tls::{
    TestTlsIdentity, client_tls, drive_client_tls_io, drive_tls_until, feed_server_tls, flush_client_tls,
    flush_server_tls, read_plaintext_into, write_client_plaintext_limited, write_server_plaintext,
};

pub(crate) const HTTP_DRIVE_BUDGET: DriveBudget = DriveBudget {
    max_steps: 16_384,
    step_time: Duration::from_millis(1),
};
pub(crate) const TLS_PLAINTEXT_WRITE_BYTES_PER_STEP: usize = 16 * 1024;

#[derive(Debug, Default)]
pub(crate) struct TlsTranscript {
    pub(crate) request: Vec<u8>,
    pub(crate) websocket_message: Option<Vec<u8>>,
}

#[derive(Clone, Copy)]
pub(crate) struct HttpsRequestRoundtrip<'a> {
    pub(crate) host: &'a str,
    pub(crate) ca_pem: &'a str,
    pub(crate) request: &'a [u8],
    pub(crate) plaintext_write_limit: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct HttpsRequestsRoundtrip<'a> {
    pub(crate) host: &'a str,
    pub(crate) ca_pem: &'a str,
    pub(crate) requests: &'a [Vec<u8>],
}

pub(crate) struct TlsHttpUpstream {
    pub(crate) root_ca_pem: String,
    inner: Rc<RefCell<TlsHttpUpstreamState>>,
}

pub(crate) struct TlsRawHttpUpstream {
    inner: Rc<RefCell<TlsRawHttpUpstreamState>>,
}

pub(crate) struct PlainHttpUpstream {
    inner: Rc<RefCell<PlainHttpUpstreamState>>,
}

pub(crate) struct HttpsClientFlowSpec<'a> {
    pub(crate) tcp: TcpHandle,
    pub(crate) host: &'a str,
    pub(crate) ca_pem: &'a str,
    pub(crate) requests: Vec<Vec<u8>>,
    pub(crate) expected_response: Vec<u8>,
}

pub(crate) struct HttpsClientFlow {
    tcp: TcpHandle,
    tls: TlsClientSession,
    requests: Vec<Vec<u8>>,
    expected_response: Vec<u8>,
    response: Vec<u8>,
    response_completion: HttpResponseCompletion,
    current_response_start: usize,
    request_index: usize,
    written: usize,
    tls_bytes_flushed: usize,
    complete: bool,
}

struct TlsHttpUpstreamState {
    tls: TlsServerSession,
    responses: Vec<Http1Response>,
    transcript: Rc<RefCell<TlsTranscript>>,
    responses_sent: usize,
    close_after_response: bool,
    response_trigger: ResponseTrigger,
}

struct PlainHttpUpstreamState {
    response: Http1Response,
    transcript: Rc<RefCell<TlsTranscript>>,
    close_after_response: bool,
    responses_sent: usize,
}

struct TlsRawHttpUpstreamState {
    server_config: TlsServerConfig,
    tls: Option<TlsServerSession>,
    responses: Vec<RawHttpResponse>,
    transcript: Rc<RefCell<TlsTranscript>>,
    close_after_response: bool,
    responses_sent: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Http1Response {
    status: &'static str,
    headers: Vec<(&'static str, &'static str)>,
    declared_content_length: Option<usize>,
    body: &'static [u8],
    framing: Http1ResponseFraming,
    delivery: Http1ResponseDelivery,
    connection: Http1Connection,
}

#[derive(Debug, Clone, Copy)]
enum Http1ResponseFraming {
    ContentLength,
    Chunked { chunk_size: usize },
}

#[derive(Debug, Clone, Copy)]
enum Http1ResponseDelivery {
    Immediate,
    Segmented { segment_size: usize },
}

#[derive(Debug, Clone, Copy)]
enum Http1Connection {
    Close,
    KeepAlive,
}

#[derive(Debug, Clone)]
pub(crate) struct RawHttpResponse {
    pub(crate) plaintext: Vec<u8>,
    pub(crate) segment_size: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawHttpConnection {
    Close,
    KeepAlive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawHttpResponseFraming {
    ContentLength,
    Chunked { chunk_size: usize },
}

impl RawHttpConnection {
    const fn header_value(self) -> &'static str {
        match self {
            Self::Close => "close",
            Self::KeepAlive => "keep-alive",
        }
    }
}

impl RawHttpResponse {
    pub(crate) fn response(
        body: &[u8],
        framing: RawHttpResponseFraming,
        connection: RawHttpConnection,
        segment_size: Option<usize>,
    ) -> Self {
        Self {
            plaintext: http_response_wire(body, framing, connection),
            segment_size,
        }
    }

    pub(crate) fn head_response(
        body_len: usize,
        framing: RawHttpResponseFraming,
        connection: RawHttpConnection,
        segment_size: Option<usize>,
    ) -> Self {
        Self {
            plaintext: http_head_response_wire(body_len, framing, connection),
            segment_size,
        }
    }
}

impl Http1Response {
    pub(crate) const fn ok(body: &'static [u8]) -> Self {
        Self {
            status: "200 OK",
            headers: Vec::new(),
            declared_content_length: None,
            body,
            framing: Http1ResponseFraming::ContentLength,
            delivery: Http1ResponseDelivery::Immediate,
            connection: Http1Connection::Close,
        }
    }

    pub(crate) const fn chunked(body: &'static [u8], chunk_size: usize) -> Self {
        Self {
            status: "200 OK",
            headers: Vec::new(),
            declared_content_length: None,
            body,
            framing: Http1ResponseFraming::Chunked { chunk_size },
            delivery: Http1ResponseDelivery::Immediate,
            connection: Http1Connection::Close,
        }
    }

    pub(crate) const fn with_declared_content_length(body: &'static [u8], declared_len: usize) -> Self {
        Self {
            status: "200 OK",
            headers: Vec::new(),
            declared_content_length: Some(declared_len),
            body,
            framing: Http1ResponseFraming::ContentLength,
            delivery: Http1ResponseDelivery::Immediate,
            connection: Http1Connection::Close,
        }
    }

    pub(crate) const fn keep_alive(mut self) -> Self {
        self.connection = Http1Connection::KeepAlive;
        self
    }

    pub(crate) const fn segmented(mut self, segment_size: usize) -> Self {
        self.delivery = Http1ResponseDelivery::Segmented { segment_size };
        self
    }

    pub(crate) const fn body(&self) -> &'static [u8] {
        self.body
    }

    pub(crate) fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut output = Vec::new();
        self.write_to(&mut output)?;
        Ok(output)
    }

    fn write_to(&self, output: &mut impl io::Write) -> io::Result<()> {
        write!(output, "HTTP/1.1 {}\r\n", self.status)?;
        for (name, value) in &self.headers {
            write!(output, "{name}: {value}\r\n")?;
        }
        match self.framing {
            Http1ResponseFraming::ContentLength => {
                write!(
                    output,
                    "content-length: {}\r\n",
                    self.declared_content_length.unwrap_or(self.body.len())
                )?;
                self.write_connection_header(output)?;
                output.write_all(b"\r\n")?;
                output.write_all(self.body)
            }
            Http1ResponseFraming::Chunked { chunk_size } => {
                output.write_all(b"transfer-encoding: chunked\r\n")?;
                self.write_connection_header(output)?;
                output.write_all(b"\r\n")?;
                for chunk in self.body.chunks(chunk_size.max(1)) {
                    write!(output, "{:x}\r\n", chunk.len())?;
                    output.write_all(chunk)?;
                    output.write_all(b"\r\n")?;
                }
                output.write_all(b"0\r\n\r\n")
            }
        }
    }

    fn write_connection_header(&self, output: &mut impl io::Write) -> io::Result<()> {
        match self.connection {
            Http1Connection::Close => output.write_all(b"connection: close\r\n"),
            Http1Connection::KeepAlive => output.write_all(b"connection: keep-alive\r\n"),
        }
    }
}

impl TlsRawHttpUpstream {
    pub(crate) fn new(
        server_config: &TlsServerConfig,
        responses: Vec<RawHttpResponse>,
        close_after_response: bool,
    ) -> Self {
        Self {
            inner: Rc::new(RefCell::new(TlsRawHttpUpstreamState {
                server_config: server_config.clone(),
                tls: None,
                responses,
                transcript: Rc::new(RefCell::new(TlsTranscript::default())),
                close_after_response,
                responses_sent: 0,
            })),
        }
    }

    pub(crate) fn transcript(&self) -> Rc<RefCell<TlsTranscript>> {
        Rc::clone(&self.inner.borrow().transcript)
    }

    pub(crate) fn handler(&self) -> SimTcpHandler {
        let inner = Rc::clone(&self.inner);
        tcp_response_handler(move |bytes| inner.borrow_mut().handle(bytes))
    }
}

impl TlsRawHttpUpstreamState {
    fn handle(&mut self, bytes: &[u8]) -> io::Result<SimTcpResponse> {
        if self.tls.is_none() {
            self.tls = Some(TlsServerSession::accept(&self.server_config).map_err(io::Error::other)?);
        }
        let tls = self
            .tls
            .as_mut()
            .ok_or_else(|| io::Error::other("missing raw TLS HTTP upstream session"))?;
        feed_server_tls(tls, bytes)?;
        read_plaintext_into(tls, &mut self.transcript.borrow_mut().request)?;

        let mut response_chunks = Vec::new();
        flush_server_tls_chunk(tls, &mut response_chunks)?;
        let response_ready_count = complete_request_count(&self.transcript.borrow().request).min(self.responses.len());
        for response in &self.responses[self.responses_sent..response_ready_count] {
            write_raw_response_tls(tls, response, &mut response_chunks)?;
        }
        self.responses_sent = response_ready_count;

        if self.close_after_response && self.responses_sent >= self.responses.len() {
            return Ok(SimTcpResponse {
                bytes: concat_chunks(response_chunks),
                followup_bytes: Vec::new(),
                close: true,
                reset: false,
            });
        }
        Ok(SimTcpResponse::from_ordered_chunks(response_chunks))
    }
}

fn write_raw_response_tls(
    tls: &mut TlsServerSession,
    response: &RawHttpResponse,
    chunks: &mut Vec<Vec<u8>>,
) -> io::Result<()> {
    let segment_size = response.segment_size.unwrap_or(response.plaintext.len()).max(1);
    for segment in response.plaintext.chunks(segment_size) {
        write_server_tls_plaintext_fully(tls, segment, chunks)?;
    }
    Ok(())
}

fn write_server_tls_plaintext_fully(
    tls: &mut TlsServerSession,
    plaintext: &[u8],
    chunks: &mut Vec<Vec<u8>>,
) -> io::Result<()> {
    let mut offset = 0_usize;
    while offset < plaintext.len() {
        let mut output = Vec::new();
        let written = write_server_plaintext(tls, &plaintext[offset..], &mut output)?;
        offset = offset.saturating_add(written);
        push_tls_chunk(output, chunks);
        if written == 0 {
            break;
        }
    }
    flush_server_tls_chunk(tls, chunks)
}

fn flush_server_tls_chunk(tls: &mut TlsServerSession, chunks: &mut Vec<Vec<u8>>) -> io::Result<()> {
    let mut output = Vec::new();
    flush_server_tls(tls, &mut output)?;
    push_tls_chunk(output, chunks);
    Ok(())
}

fn push_tls_chunk(output: Vec<u8>, chunks: &mut Vec<Vec<u8>>) {
    if !output.is_empty() {
        chunks.push(output);
    }
}

fn concat_chunks(chunks: Vec<Vec<u8>>) -> Vec<u8> {
    let len = chunks.iter().map(Vec::len).sum();
    let mut output = Vec::with_capacity(len);
    for chunk in chunks {
        output.extend_from_slice(&chunk);
    }
    output
}

fn http_response_wire(body: &[u8], framing: RawHttpResponseFraming, connection: RawHttpConnection) -> Vec<u8> {
    let mut output = Vec::new();
    match framing {
        RawHttpResponseFraming::ContentLength => {
            let _ = write!(
                output,
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: {}\r\n\r\n",
                body.len(),
                connection.header_value()
            );
            output.extend_from_slice(body);
        }
        RawHttpResponseFraming::Chunked { chunk_size } => {
            let _ = write!(
                output,
                "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: {}\r\n\r\n",
                connection.header_value()
            );
            write_chunked_body(&mut output, body, chunk_size);
        }
    }
    output
}

fn http_head_response_wire(body_len: usize, framing: RawHttpResponseFraming, connection: RawHttpConnection) -> Vec<u8> {
    let mut output = Vec::new();
    match framing {
        RawHttpResponseFraming::ContentLength => {
            let _ = write!(
                output,
                "HTTP/1.1 200 OK\r\ncontent-length: {body_len}\r\nconnection: {}\r\n\r\n",
                connection.header_value()
            );
        }
        RawHttpResponseFraming::Chunked { .. } => {
            let _ = write!(
                output,
                "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: {}\r\n\r\n",
                connection.header_value()
            );
        }
    }
    output
}

pub(crate) fn write_chunked_body(output: &mut Vec<u8>, body: &[u8], chunk_size: usize) {
    for chunk in body.chunks(chunk_size.max(1)) {
        let _ = write!(output, "{:x}\r\n", chunk.len());
        output.extend_from_slice(chunk);
        output.extend_from_slice(b"\r\n");
    }
    output.extend_from_slice(b"0\r\n\r\n");
}

impl TlsHttpUpstream {
    pub(crate) fn with_response(response: Http1Response) -> Result<Self> {
        Self::with_responses([response])
    }

    pub(crate) fn with_response_after_headers(response: Http1Response) -> Result<Self> {
        Self::with_responses_trigger([response], false, ResponseTrigger::Headers)
    }

    pub(crate) fn with_response_after_headers_and_close(response: Http1Response) -> Result<Self> {
        Self::with_responses_trigger([response], true, ResponseTrigger::Headers)
    }

    pub(crate) fn with_responses(responses: impl IntoIterator<Item = Http1Response>) -> Result<Self> {
        Self::with_responses_trigger(responses, false, ResponseTrigger::CompleteRequest)
    }

    pub(crate) fn with_response_and_close(response: Http1Response) -> Result<Self> {
        Self::with_responses_trigger([response], true, ResponseTrigger::CompleteRequest)
    }

    fn with_responses_trigger(
        responses: impl IntoIterator<Item = Http1Response>,
        close_after_response: bool,
        response_trigger: ResponseTrigger,
    ) -> Result<Self> {
        let identity = TestTlsIdentity::fixed_upstream()?;
        Ok(Self {
            root_ca_pem: identity.root_ca_pem,
            inner: Rc::new(RefCell::new(TlsHttpUpstreamState {
                tls: TlsServerSession::accept(&identity.server_config)
                    .map_err(|error| super::super::Error::from_display("create TLS HTTP upstream", error))?,
                responses: responses.into_iter().collect(),
                transcript: Rc::new(RefCell::new(TlsTranscript::default())),
                responses_sent: 0,
                close_after_response,
                response_trigger,
            })),
        })
    }

    pub(crate) fn transcript(&self) -> Rc<RefCell<TlsTranscript>> {
        Rc::clone(&self.inner.borrow().transcript)
    }

    pub(crate) fn handler(&self) -> SimTcpHandler {
        let inner = Rc::clone(&self.inner);
        tcp_response_handler(move |bytes| inner.borrow_mut().handle(bytes))
    }
}

impl TlsHttpUpstreamState {
    fn handle(&mut self, bytes: &[u8]) -> io::Result<SimTcpResponse> {
        feed_server_tls(&mut self.tls, bytes)?;
        read_plaintext_into(&mut self.tls, &mut self.transcript.borrow_mut().request)?;
        let response_ready_count = self.response_ready_count();
        let mut response_chunks = Vec::new();
        for index in self.responses_sent..response_ready_count {
            let selected = index.min(self.responses.len().saturating_sub(1));
            let Some(http_response) = self.responses.get(selected) else {
                break;
            };
            let mut wire = Vec::new();
            http_response.write_to(&mut wire)?;
            let tls_response = write_response_tls(&mut self.tls, &wire, http_response.delivery)?;
            response_chunks.extend(tls_response.into_ordered_chunks());
        }
        self.responses_sent = response_ready_count;
        let mut tls_output = Vec::new();
        flush_server_tls(&mut self.tls, &mut tls_output)?;
        if !tls_output.is_empty() {
            response_chunks.push(tls_output);
        }
        let mut response = SimTcpResponse::from_ordered_chunks(response_chunks);
        response.close = self.close_after_response && self.responses_sent >= self.responses.len();
        Ok(response)
    }

    fn response_ready_count(&self) -> usize {
        response_ready_count(&self.transcript.borrow().request, self.response_trigger)
    }
}

fn write_response_tls(
    tls: &mut TlsServerSession,
    plaintext: &[u8],
    delivery: Http1ResponseDelivery,
) -> io::Result<SimTcpResponse> {
    match delivery {
        Http1ResponseDelivery::Immediate => {
            let mut output = Vec::new();
            write_server_plaintext(tls, plaintext, &mut output)?;
            Ok(SimTcpResponse::bytes(output))
        }
        Http1ResponseDelivery::Segmented { segment_size } => {
            let mut chunks = Vec::new();
            for segment in plaintext.chunks(segment_size.max(1)) {
                let mut output = Vec::new();
                write_server_plaintext(tls, segment, &mut output)?;
                if !output.is_empty() {
                    chunks.push(output);
                }
            }
            if chunks.is_empty() {
                Ok(SimTcpResponse::default())
            } else {
                let first = chunks.remove(0);
                Ok(SimTcpResponse::segmented(first, chunks))
            }
        }
    }
}

impl PlainHttpUpstream {
    pub(crate) fn new(response_body: &'static [u8]) -> Self {
        Self::with_close_after_response(response_body, false)
    }

    pub(crate) fn with_close_after_response(response_body: &'static [u8], close_after_response: bool) -> Self {
        Self {
            inner: Rc::new(RefCell::new(PlainHttpUpstreamState {
                response: Http1Response::ok(response_body),
                transcript: Rc::new(RefCell::new(TlsTranscript::default())),
                close_after_response,
                responses_sent: 0,
            })),
        }
    }

    pub(crate) fn transcript(&self) -> Rc<RefCell<TlsTranscript>> {
        Rc::clone(&self.inner.borrow().transcript)
    }

    pub(crate) fn handler(&self) -> SimTcpHandler {
        let inner = Rc::clone(&self.inner);
        tcp_response_handler(move |bytes| inner.borrow_mut().handle(bytes))
    }
}

impl PlainHttpUpstreamState {
    fn handle(&mut self, bytes: &[u8]) -> io::Result<SimTcpResponse> {
        self.transcript.borrow_mut().request.extend_from_slice(bytes);
        let mut output = Vec::new();
        let completed_requests = complete_request_count(&self.transcript.borrow().request);
        for _request in self.responses_sent..completed_requests {
            self.response.write_to(&mut output)?;
        }
        self.responses_sent = completed_requests;
        Ok(SimTcpResponse {
            bytes: output,
            followup_bytes: Vec::new(),
            close: self.close_after_response,
            reset: false,
        })
    }
}

pub(crate) fn http_request<N>(
    sim: &mut Simulator,
    guest: &mut SmolTcpGuest,
    running: &mut N,
    tcp: TcpHandle,
    request: &[u8],
) -> Result<Vec<u8>>
where
    N: SteppedNetwork,
{
    guest.write_all(running, tcp, request)?;
    let mut response = Vec::new();
    let mut completion = HttpResponseCompletion::for_request(request);
    sim.drive_guest_until(
        guest,
        running,
        "plaintext HTTP response",
        HTTP_DRIVE_BUDGET,
        |guest, _running| {
            response.extend_from_slice(&guest.read_available_bytes(tcp)?);
            Ok(http_response_is_complete(
                &mut completion,
                &response,
                guest.tcp_may_recv(tcp),
            ))
        },
    )?;
    Ok(response)
}

pub(crate) fn http_response_is_complete(
    completion: &mut HttpResponseCompletion,
    response: &[u8],
    tcp_may_recv: bool,
) -> bool {
    completion.is_complete(response) || (!tcp_may_recv && completion.is_complete_on_eof(response))
}

pub(crate) fn https_request_with_hook<N>(
    sim: &mut Simulator,
    guest: &mut SmolTcpGuest,
    running: &mut N,
    guest_link: &GuestLink,
    tcp: TcpHandle,
    roundtrip: HttpsRequestRoundtrip<'_>,
    after_tls_handshake: impl FnOnce(),
) -> Result<Vec<u8>>
where
    N: SteppedNetwork,
{
    let mut tls = client_tls(roundtrip.host, roundtrip.ca_pem)?;
    drive_tls_until(
        sim,
        guest,
        running,
        tcp,
        &mut tls,
        "TLS handshake",
        |tls, _plaintext| !tls.is_handshaking(),
    )?;
    after_tls_handshake();
    let mut written = 0;
    let mut response = Vec::new();
    let mut completion = HttpResponseCompletion::for_request(roundtrip.request);
    let request_plaintext_written = Cell::new(0_usize);
    let client_tls_bytes_flushed = Cell::new(0_usize);
    let client_tls_bytes_read = Cell::new(0_usize);
    let response_plaintext_read = Cell::new(0_usize);
    let progress = Cell::new(0_usize);
    sim.drive_guest_until_with_progress(
        guest,
        running,
        DriveGuestProgress {
            label: "HTTPS request response",
            budget: HTTP_DRIVE_BUDGET,
        },
        |guest, running| {
            write_client_plaintext_limited(
                &mut tls,
                roundtrip.request,
                &mut written,
                roundtrip.plaintext_write_limit,
            )?;
            request_plaintext_written.set(written);
            let io = drive_client_tls_io(guest, running, tcp, &mut tls, &mut response)?;
            client_tls_bytes_flushed.set(client_tls_bytes_flushed.get().saturating_add(io.flushed));
            client_tls_bytes_read.set(client_tls_bytes_read.get().saturating_add(io.ciphertext_read));
            response_plaintext_read.set(response.len());
            progress.set(
                request_plaintext_written
                    .get()
                    .saturating_add(client_tls_bytes_flushed.get())
                    .saturating_add(response_plaintext_read.get()),
            );
            let response_complete = http_response_is_complete(&mut completion, &response, guest.tcp_may_recv(tcp));
            Ok(written == roundtrip.request.len() && !tls.wants_write() && response_complete)
        },
        || progress.get(),
        |output| {
            let _ = writeln!(output, "  phase: HTTPS request response");
            let _ = writeln!(
                output,
                "  request_plaintext_bytes_accepted: {}/{}",
                request_plaintext_written.get(),
                roundtrip.request.len()
            );
            let _ = writeln!(output, "  client_tls_bytes_flushed: {}", client_tls_bytes_flushed.get());
            let _ = writeln!(output, "  client_tls_bytes_read: {}", client_tls_bytes_read.get());
            let _ = writeln!(
                output,
                "  response_plaintext_bytes_read: {}",
                response_plaintext_read.get()
            );
        },
    )?;
    drain_tls_response_after_completion(sim, guest, running, guest_link, tcp, &mut tls, &mut response)?;
    Ok(response)
}

pub(crate) fn https_request_read_after_upload_with_hook<N>(
    sim: &mut Simulator,
    guest: &mut SmolTcpGuest,
    running: &mut N,
    guest_link: &GuestLink,
    tcp: TcpHandle,
    roundtrip: HttpsRequestRoundtrip<'_>,
    after_tls_handshake: impl FnOnce(),
) -> Result<Vec<u8>>
where
    N: SteppedNetwork,
{
    let mut tls = client_tls(roundtrip.host, roundtrip.ca_pem)?;
    drive_tls_until(
        sim,
        guest,
        running,
        tcp,
        &mut tls,
        "TLS handshake",
        |tls, _plaintext| !tls.is_handshaking(),
    )?;
    after_tls_handshake();

    let mut written = 0;
    let request_plaintext_written = Cell::new(0_usize);
    let client_tls_bytes_flushed = Cell::new(0_usize);
    let progress = Cell::new(0_usize);
    sim.drive_guest_until_with_progress(
        guest,
        running,
        DriveGuestProgress {
            label: "HTTPS request upload",
            budget: HTTP_DRIVE_BUDGET,
        },
        |guest, running| {
            write_client_plaintext_limited(
                &mut tls,
                roundtrip.request,
                &mut written,
                roundtrip.plaintext_write_limit,
            )?;
            request_plaintext_written.set(written);
            let flushed = flush_client_tls(guest, running, tcp, &mut tls)?;
            client_tls_bytes_flushed.set(client_tls_bytes_flushed.get().saturating_add(flushed));
            progress.set(
                request_plaintext_written
                    .get()
                    .saturating_add(client_tls_bytes_flushed.get()),
            );
            Ok(written == roundtrip.request.len() && !tls.wants_write())
        },
        || progress.get(),
        |output| {
            let _ = writeln!(output, "  phase: HTTPS request upload");
            let _ = writeln!(
                output,
                "  request_plaintext_bytes_accepted: {}/{}",
                request_plaintext_written.get(),
                roundtrip.request.len()
            );
            let _ = writeln!(output, "  client_tls_bytes_flushed: {}", client_tls_bytes_flushed.get());
        },
    )?;

    let mut response = Vec::new();
    let mut completion = HttpResponseCompletion::for_request(roundtrip.request);
    let response_plaintext_read = Cell::new(0_usize);
    sim.drive_guest_until_with_progress(
        guest,
        running,
        DriveGuestProgress {
            label: "HTTPS response after upload",
            budget: HTTP_DRIVE_BUDGET,
        },
        |guest, running| {
            let _io = drive_client_tls_io(guest, running, tcp, &mut tls, &mut response)?;
            response_plaintext_read.set(response.len());
            Ok(http_response_is_complete(
                &mut completion,
                &response,
                guest.tcp_may_recv(tcp),
            ))
        },
        || response_plaintext_read.get(),
        |output| {
            let _ = writeln!(output, "  phase: HTTPS response after upload");
            let _ = writeln!(
                output,
                "  response_plaintext_bytes_read: {}",
                response_plaintext_read.get()
            );
        },
    )?;
    drain_tls_response_after_completion(sim, guest, running, guest_link, tcp, &mut tls, &mut response)?;
    Ok(response)
}

pub(crate) fn https_requests_with_hook<N>(
    sim: &mut Simulator,
    guest: &mut SmolTcpGuest,
    running: &mut N,
    tcp: TcpHandle,
    roundtrip: HttpsRequestsRoundtrip<'_>,
    after_tls_handshake: impl FnOnce(),
) -> Result<Vec<Vec<u8>>>
where
    N: SteppedNetwork,
{
    let mut tls = client_tls(roundtrip.host, roundtrip.ca_pem)?;
    drive_tls_until(
        sim,
        guest,
        running,
        tcp,
        &mut tls,
        "TLS handshake",
        |tls, _plaintext| !tls.is_handshaking(),
    )?;
    after_tls_handshake();
    let mut responses = Vec::with_capacity(roundtrip.requests.len());
    for request in roundtrip.requests {
        let mut written = 0;
        let mut response = Vec::new();
        let mut completion = HttpResponseCompletion::for_request(request);
        let request_plaintext_written = Cell::new(0_usize);
        let client_tls_bytes_flushed = Cell::new(0_usize);
        let response_plaintext_read = Cell::new(0_usize);
        let progress = Cell::new(0_usize);
        sim.drive_guest_until_with_progress(
            guest,
            running,
            DriveGuestProgress {
                label: "HTTPS request response",
                budget: HTTP_DRIVE_BUDGET,
            },
            |guest, running| {
                write_client_plaintext_limited(&mut tls, request, &mut written, TLS_PLAINTEXT_WRITE_BYTES_PER_STEP)?;
                request_plaintext_written.set(written);
                let io = drive_client_tls_io(guest, running, tcp, &mut tls, &mut response)?;
                client_tls_bytes_flushed.set(client_tls_bytes_flushed.get().saturating_add(io.flushed));
                response_plaintext_read.set(response.len());
                progress.set(
                    request_plaintext_written
                        .get()
                        .saturating_add(client_tls_bytes_flushed.get())
                        .saturating_add(response_plaintext_read.get()),
                );
                let response_complete = http_response_is_complete(&mut completion, &response, guest.tcp_may_recv(tcp));
                Ok(written == request.len() && !tls.wants_write() && response_complete)
            },
            || progress.get(),
            |output| {
                let _ = writeln!(output, "  phase: HTTPS request response");
                let _ = writeln!(
                    output,
                    "  request_plaintext_bytes_accepted: {}/{}",
                    request_plaintext_written.get(),
                    request.len()
                );
                let _ = writeln!(output, "  client_tls_bytes_flushed: {}", client_tls_bytes_flushed.get());
                let _ = writeln!(
                    output,
                    "  response_plaintext_bytes_read: {}",
                    response_plaintext_read.get()
                );
            },
        )?;
        responses.push(response);
    }
    Ok(responses)
}

pub(crate) fn drain_tls_response_after_completion<N>(
    sim: &mut Simulator,
    guest: &mut SmolTcpGuest,
    running: &mut N,
    guest_link: &GuestLink,
    tcp: TcpHandle,
    tls: &mut TlsClientSession,
    response: &mut Vec<u8>,
) -> Result<()>
where
    N: SteppedNetwork,
{
    const LABEL: &str = "HTTPS response drain after completion";

    for _attempt in 0..4 {
        let _quiescence = sim.drive_guest_network_until_quiescent(
            guest,
            running,
            guest_link,
            LABEL,
            DriveBudget {
                max_steps: 4096,
                ..DriveBudget::default()
            },
        )?;
        let io = drive_client_tls_io(guest, running, tcp, tls, response)?;
        if io.ciphertext_read == 0 {
            return Ok(());
        }
    }
    Err(super::super::Error::new(format!(
        "{LABEL}: guest still had TLS bytes after repeated quiescence"
    )))
}

impl HttpsClientFlow {
    pub(crate) fn new(spec: HttpsClientFlowSpec<'_>) -> Result<Self> {
        let tls = client_tls(spec.host, spec.ca_pem)?;
        let response_completion = response_completion_for(spec.requests.first().map(Vec::as_slice));
        Ok(Self {
            tcp: spec.tcp,
            tls,
            requests: spec.requests,
            expected_response: spec.expected_response,
            response: Vec::new(),
            response_completion,
            current_response_start: 0,
            request_index: 0,
            written: 0,
            tls_bytes_flushed: 0,
            complete: false,
        })
    }

    pub(crate) fn drive_step<N>(&mut self, guest: &mut SmolTcpGuest, running: &mut N) -> Result<()>
    where
        N: SteppedNetwork,
    {
        if self.complete {
            return Ok(());
        }
        if self.tls.is_handshaking() {
            let io = drive_client_tls_io(guest, running, self.tcp, &mut self.tls, &mut self.response)?;
            self.tls_bytes_flushed = self.tls_bytes_flushed.saturating_add(io.flushed);
            return Ok(());
        }
        let Some(request) = self.requests.get(self.request_index) else {
            self.complete = true;
            return Ok(());
        };
        let request_len = request.len();
        write_client_plaintext_limited(
            &mut self.tls,
            request,
            &mut self.written,
            TLS_PLAINTEXT_WRITE_BYTES_PER_STEP,
        )?;
        let io = drive_client_tls_io(guest, running, self.tcp, &mut self.tls, &mut self.response)?;
        self.tls_bytes_flushed = self.tls_bytes_flushed.saturating_add(io.flushed);
        if !self.expected_response.starts_with(&self.response) {
            return Err(super::super::Error::new(format!(
                "HTTPS response diverged at {} bytes",
                self.response.len(),
            )));
        }
        if self.written == request_len
            && !self.tls.wants_write()
            && http_response_is_complete(
                &mut self.response_completion,
                &self.response[self.current_response_start..],
                guest.tcp_may_recv(self.tcp),
            )
        {
            self.request_index = self.request_index.saturating_add(1);
            self.written = 0;
            self.current_response_start = self.response.len();
            self.response_completion =
                response_completion_for(self.requests.get(self.request_index).map(Vec::as_slice));
        }
        self.complete = self.request_index == self.requests.len();
        Ok(())
    }

    pub(crate) fn drain_after_completion<N>(
        &mut self,
        sim: &mut Simulator,
        guest: &mut SmolTcpGuest,
        running: &mut N,
        guest_link: &GuestLink,
    ) -> Result<()>
    where
        N: SteppedNetwork,
    {
        if self.complete {
            drain_tls_response_after_completion(
                sim,
                guest,
                running,
                guest_link,
                self.tcp,
                &mut self.tls,
                &mut self.response,
            )?;
        }
        Ok(())
    }

    pub(crate) const fn is_complete(&self) -> bool {
        self.complete
    }

    pub(crate) const fn progress(&self) -> usize {
        self.written
            .saturating_add(self.tls_bytes_flushed)
            .saturating_add(self.response.len())
    }

    pub(crate) fn response(&self) -> &[u8] {
        &self.response
    }

    pub(crate) fn expected_response(&self) -> &[u8] {
        &self.expected_response
    }

    pub(crate) const fn written(&self) -> usize {
        self.written
    }

    pub(crate) const fn tcp(&self) -> TcpHandle {
        self.tcp
    }

    pub(crate) fn tls_established(&self) -> bool {
        !self.tls.is_handshaking()
    }

    pub(crate) const fn request_complete(&self) -> bool {
        self.request_index == self.requests.len()
    }

    pub(crate) const fn response_complete(&self) -> bool {
        self.complete
    }
}

fn response_completion_for(request: Option<&[u8]>) -> HttpResponseCompletion {
    request.map_or_else(HttpResponseCompletion::default, |request| {
        HttpResponseCompletion::for_request(request)
    })
}
