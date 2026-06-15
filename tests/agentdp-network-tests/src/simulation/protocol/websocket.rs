use std::cell::{Cell, RefCell};
use std::fmt::Write as _;
use std::io;
use std::rc::Rc;

use agentdp_crypto::{TlsClientSession, TlsServerConfig, TlsServerSession};
use agentdp_network::test_support::simulation::SimTcpHandler;
use sha1::{Digest as _, Sha1};

use super::super::{DriveGuestProgress, Error, Result, Simulator, SmolTcpGuest, SteppedNetwork, TcpHandle};
use super::http1::{TLS_PLAINTEXT_WRITE_BYTES_PER_STEP, TlsTranscript};
use super::http1_model::{find_header_end, http_message_complete, http_request_complete};
use super::tcp::tcp_handler;
use super::tls::{
    TestTlsIdentity, client_tls, drive_client_tls_io, drive_tls_until, feed_server_tls, flush_server_tls,
    read_plaintext_into, write_client_plaintext_limited, write_server_plaintext,
};

const SERVER_TLS_PLAINTEXT_WRITE_BYTES: usize = 4096;

const PLACEHOLDER: &str = "AGENTDP_SECRET_TOKEN";

pub(crate) struct TlsWssUpstream {
    pub(crate) root_ca_pem: String,
    inner: Rc<RefCell<TlsWssUpstreamState>>,
}

pub(crate) struct WssClientFlowSpec<'a> {
    pub(crate) tcp: TcpHandle,
    pub(crate) host: &'a str,
    pub(crate) ca_pem: &'a str,
    pub(crate) message: Vec<u8>,
    pub(crate) expected_response: Vec<u8>,
    pub(crate) fragmented: bool,
    pub(crate) close_after_response: bool,
}

pub(crate) struct WssClientFlow {
    tcp: TcpHandle,
    tls: TlsClientSession,
    stage: WssClientFlowStage,
    upgrade_request: Vec<u8>,
    upgrade_written: usize,
    upgrade_response: Vec<u8>,
    frames: Vec<Vec<u8>>,
    frame_index: usize,
    frame_offset: usize,
    response: Vec<u8>,
    response_message: Vec<u8>,
    expected_response: Vec<u8>,
    close_after_response: bool,
    tls_bytes_flushed: usize,
    complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WssClientFlowStage {
    TlsHandshake,
    Upgrade,
    Frames,
    Closing,
    Complete,
}

struct TlsWssUpstreamState {
    server_config: TlsServerConfig,
    tls: Option<TlsServerSession>,
    transcript: Rc<RefCell<TlsTranscript>>,
    behavior: TlsWssBehavior,
}

enum TlsWssBehavior {
    Accept {
        response_message: Vec<u8>,
        response_fragmented: bool,
        state: WssAcceptState,
    },
    Reject {
        followup_response_body: Vec<u8>,
        state: WssRejectState,
    },
}

struct WssAcceptState {
    upgraded: bool,
    response_sent: bool,
    close_after_response: bool,
    parser: WebSocketMessageParser,
}

#[derive(Default)]
struct WssRejectState {
    rejection_boundary: Option<usize>,
    response_sent: bool,
}

impl TlsWssUpstream {
    pub(crate) fn new(response_message: impl Into<Vec<u8>>, close_after_response: bool) -> Result<Self> {
        Self::with_response_fragmentation(response_message, close_after_response, false)
    }

    pub(crate) fn with_response_fragmentation(
        response_message: impl Into<Vec<u8>>,
        close_after_response: bool,
        response_fragmented: bool,
    ) -> Result<Self> {
        Self::with_behavior(TlsWssBehavior::Accept {
            response_message: response_message.into(),
            response_fragmented,
            state: WssAcceptState {
                upgraded: false,
                response_sent: false,
                close_after_response,
                parser: WebSocketMessageParser::default(),
            },
        })
    }

    pub(crate) fn reject_upgrade(followup_response_body: impl Into<Vec<u8>>) -> Result<Self> {
        Self::with_behavior(TlsWssBehavior::Reject {
            followup_response_body: followup_response_body.into(),
            state: WssRejectState::default(),
        })
    }

    fn with_behavior(behavior: TlsWssBehavior) -> Result<Self> {
        let identity = TestTlsIdentity::fixed_upstream()?;
        Ok(Self {
            root_ca_pem: identity.root_ca_pem,
            inner: Rc::new(RefCell::new(TlsWssUpstreamState {
                server_config: identity.server_config,
                tls: None,
                transcript: Rc::new(RefCell::new(TlsTranscript::default())),
                behavior,
            })),
        })
    }

    pub(crate) fn transcript(&self) -> Rc<RefCell<TlsTranscript>> {
        Rc::clone(&self.inner.borrow().transcript)
    }

    pub(crate) fn handler(&self) -> SimTcpHandler {
        let inner = Rc::clone(&self.inner);
        tcp_handler(move |bytes, output| inner.borrow_mut().handle(bytes, output))
    }
}

impl TlsWssUpstreamState {
    fn handle(&mut self, bytes: &[u8], output: &mut Vec<u8>) -> io::Result<()> {
        if self.tls.is_none() {
            self.tls = Some(TlsServerSession::accept(&self.server_config).map_err(io::Error::other)?);
        }
        let tls = self
            .tls
            .as_mut()
            .ok_or_else(|| io::Error::other("missing WSS upstream TLS session"))?;
        feed_server_tls(tls, bytes)?;
        let mut plaintext = Vec::new();
        read_plaintext_into(tls, &mut plaintext)?;
        if !plaintext.is_empty() {
            match &mut self.behavior {
                TlsWssBehavior::Accept {
                    response_message,
                    response_fragmented,
                    state,
                } => handle_accept_plaintext(
                    tls,
                    &self.transcript,
                    response_message,
                    *response_fragmented,
                    state,
                    &plaintext,
                    output,
                )?,
                TlsWssBehavior::Reject {
                    followup_response_body,
                    state,
                } => {
                    handle_rejected_upgrade_plaintext(
                        tls,
                        &self.transcript,
                        followup_response_body,
                        state,
                        &plaintext,
                        output,
                    )?;
                }
            }
        }
        flush_server_tls(tls, output)
    }
}

impl WssClientFlow {
    pub(crate) fn new(spec: WssClientFlowSpec<'_>) -> Result<Self> {
        let tls = client_tls(spec.host, spec.ca_pem)?;
        Ok(Self {
            tcp: spec.tcp,
            tls,
            stage: WssClientFlowStage::TlsHandshake,
            upgrade_request: wss_upgrade_request(spec.host).into_bytes(),
            upgrade_written: 0,
            upgrade_response: Vec::new(),
            frames: client_text_frames(&spec.message, spec.fragmented)
                .map_err(|error| Error::from_display("build workload WSS frames", error))?,
            frame_index: 0,
            frame_offset: 0,
            response: Vec::new(),
            response_message: Vec::new(),
            expected_response: spec.expected_response,
            close_after_response: spec.close_after_response,
            tls_bytes_flushed: 0,
            complete: false,
        })
    }

    pub(crate) fn drive_step<N>(&mut self, guest: &mut SmolTcpGuest, running: &mut N) -> Result<()>
    where
        N: SteppedNetwork,
    {
        match self.stage {
            WssClientFlowStage::TlsHandshake => {
                let io = drive_client_tls_io(guest, running, self.tcp, &mut self.tls, &mut self.response)?;
                self.tls_bytes_flushed = self.tls_bytes_flushed.saturating_add(io.flushed);
                if !self.tls.is_handshaking() {
                    self.stage = WssClientFlowStage::Upgrade;
                }
                return Ok(());
            }
            WssClientFlowStage::Upgrade => {
                write_client_plaintext_limited(
                    &mut self.tls,
                    &self.upgrade_request,
                    &mut self.upgrade_written,
                    TLS_PLAINTEXT_WRITE_BYTES_PER_STEP,
                )?;
                let io = drive_client_tls_io(guest, running, self.tcp, &mut self.tls, &mut self.upgrade_response)?;
                self.tls_bytes_flushed = self.tls_bytes_flushed.saturating_add(io.flushed);
                if http_message_complete(&self.upgrade_response) {
                    contains(
                        "workload WSS upgrade response",
                        &self.upgrade_response,
                        b"101 Switching Protocols",
                    )?;
                    self.stage = WssClientFlowStage::Frames;
                }
                return Ok(());
            }
            WssClientFlowStage::Frames => {}
            WssClientFlowStage::Closing => {
                let io = drive_client_tls_io(guest, running, self.tcp, &mut self.tls, &mut self.response)?;
                self.tls_bytes_flushed = self.tls_bytes_flushed.saturating_add(io.flushed);
                self.complete_if_peer_closed()?;
                return Ok(());
            }
            WssClientFlowStage::Complete => return Ok(()),
        }

        while let Some(frame) = self.frames.get(self.frame_index) {
            write_client_plaintext_limited(
                &mut self.tls,
                frame,
                &mut self.frame_offset,
                TLS_PLAINTEXT_WRITE_BYTES_PER_STEP,
            )?;
            if self.frame_offset < frame.len() {
                break;
            }
            self.frame_index = self.frame_index.saturating_add(1);
            self.frame_offset = 0;
        }
        let io = drive_client_tls_io(guest, running, self.tcp, &mut self.tls, &mut self.response)?;
        self.tls_bytes_flushed = self.tls_bytes_flushed.saturating_add(io.flushed);
        match parse_server_text_frame(&self.response) {
            Ok(message) => {
                self.response_message = message;
                if self.close_after_response {
                    self.stage = WssClientFlowStage::Closing;
                    self.complete_if_peer_closed()?;
                } else {
                    self.stage = WssClientFlowStage::Complete;
                    self.complete = true;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {}
            Err(error) => return Err(Error::from_display("parse workload WSS response", error)),
        }
        Ok(())
    }

    fn complete_if_peer_closed(&mut self) -> Result<()> {
        if self.tls.peer_has_closed() {
            if self.response_message != self.expected_response {
                return Err(Error::new(format!(
                    "WSS response mismatch before TLS close; observed={:02x?} expected={:02x?}",
                    self.response_message, self.expected_response
                )));
            }
            self.stage = WssClientFlowStage::Complete;
            self.complete = true;
        }
        Ok(())
    }

    pub(crate) const fn is_complete(&self) -> bool {
        self.complete
    }

    pub(crate) const fn progress(&self) -> usize {
        self.frame_index
            .saturating_mul(1024 * 1024)
            .saturating_add(self.frame_offset)
            .saturating_add(self.tls_bytes_flushed)
            .saturating_add(self.response.len())
    }

    pub(crate) fn response_message(&self) -> &[u8] {
        &self.response_message
    }

    pub(crate) fn expected_response(&self) -> &[u8] {
        &self.expected_response
    }

    pub(crate) const fn frame_index(&self) -> usize {
        self.frame_index
    }

    pub(crate) const fn tcp(&self) -> TcpHandle {
        self.tcp
    }

    pub(crate) fn tls_established(&self) -> bool {
        self.stage != WssClientFlowStage::TlsHandshake
    }

    pub(crate) const fn request_complete(&self) -> bool {
        matches!(self.stage, WssClientFlowStage::Complete)
    }

    pub(crate) const fn response_complete(&self) -> bool {
        self.complete
    }
}

fn handle_accept_plaintext(
    tls: &mut TlsServerSession,
    transcript: &Rc<RefCell<TlsTranscript>>,
    response_message: &[u8],
    response_fragmented: bool,
    state: &mut WssAcceptState,
    plaintext: &[u8],
    output: &mut Vec<u8>,
) -> io::Result<()> {
    if !state.upgraded {
        transcript.borrow_mut().request.extend_from_slice(plaintext);
        if http_message_complete(&transcript.borrow().request) {
            let key = websocket_key(&transcript.borrow().request)?;
            write_server_tls_plaintext(tls, upgrade_response(&key).as_bytes(), output)?;
            state.upgraded = true;
        }
    } else if !state.response_sent
        && let Some(message) = state.parser.push(plaintext)?
    {
        transcript.borrow_mut().websocket_message = Some(message);
        for frame in server_text_frames(response_message, response_fragmented)? {
            write_server_tls_plaintext(tls, &frame, output)?;
        }
        state.response_sent = true;
        if state.close_after_response {
            tls.queue_close_notify();
        }
    }
    Ok(())
}

fn write_server_tls_plaintext(tls: &mut TlsServerSession, plaintext: &[u8], output: &mut Vec<u8>) -> io::Result<()> {
    for chunk in plaintext.chunks(SERVER_TLS_PLAINTEXT_WRITE_BYTES) {
        let _accepted = write_server_plaintext(tls, chunk, output)?;
    }
    Ok(())
}

fn handle_rejected_upgrade_plaintext(
    tls: &mut TlsServerSession,
    transcript: &Rc<RefCell<TlsTranscript>>,
    followup_response_body: &[u8],
    state: &mut WssRejectState,
    plaintext: &[u8],
    output: &mut Vec<u8>,
) -> io::Result<()> {
    transcript.borrow_mut().request.extend_from_slice(plaintext);
    let transcript = transcript.borrow();
    if state.rejection_boundary.is_none()
        && let Some(header_end) = find_header_end(&transcript.request)
    {
        state.rejection_boundary = Some(header_end + b"\r\n\r\n".len());
        write_server_tls_plaintext(tls, b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n", output)?;
    }
    if let Some(boundary) = state.rejection_boundary {
        let followup = &transcript.request[boundary..];
        if !state.response_sent && http_request_complete(followup) {
            let mut plaintext = Vec::new();
            write_http_ok(&mut plaintext, followup_response_body)?;
            write_server_tls_plaintext(tls, &plaintext, output)?;
            state.response_sent = true;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) struct WssRoundtrip<'a> {
    pub(crate) host: &'a str,
    pub(crate) ca_pem: &'a str,
    pub(crate) message: &'a [u8],
    pub(crate) fragmented: bool,
}

pub(crate) fn wss_roundtrip_with_hook<N>(
    sim: &mut Simulator,
    guest: &mut SmolTcpGuest,
    running: &mut N,
    tcp: TcpHandle,
    request: WssRoundtrip<'_>,
    after_upgrade: impl FnOnce(),
) -> Result<Vec<u8>>
where
    N: SteppedNetwork,
{
    let mut tls = connect_wss_tls(sim, guest, running, tcp, request.host, request.ca_pem)?;
    let upgrade = drive_wss_upgrade(sim, guest, running, tcp, &mut tls, request.host)?;
    contains("WSS upgrade response", &upgrade, b"101 Switching Protocols")?;
    after_upgrade();
    let frames = client_text_frames(request.message, request.fragmented)
        .map_err(|error| Error::from_display("build WSS client frames", error))?;
    let mut frame_index = 0;
    let mut frame_offset = 0;
    let mut response = Vec::new();
    let sent_frame_index = Cell::new(0_usize);
    let sent_frame_offset = Cell::new(0_usize);
    let client_tls_bytes_flushed = Cell::new(0_usize);
    let response_plaintext_read = Cell::new(0_usize);
    let progress = Cell::new(0_usize);
    sim.drive_guest_until_with_progress(
        guest,
        running,
        DriveGuestProgress {
            label: "WSS message",
            budget: super::http1::HTTP_DRIVE_BUDGET,
        },
        |guest, running| {
            while let Some(frame) = frames.get(frame_index) {
                write_client_plaintext_limited(&mut tls, frame, &mut frame_offset, TLS_PLAINTEXT_WRITE_BYTES_PER_STEP)?;
                if frame_offset < frame.len() {
                    break;
                }
                frame_index += 1;
                frame_offset = 0;
            }
            sent_frame_index.set(frame_index);
            sent_frame_offset.set(frame_offset);
            let io = drive_client_tls_io(guest, running, tcp, &mut tls, &mut response)?;
            client_tls_bytes_flushed.set(client_tls_bytes_flushed.get().saturating_add(io.flushed));
            response_plaintext_read.set(response.len());
            progress.set(
                sent_frame_index
                    .get()
                    .saturating_mul(1024 * 1024)
                    .saturating_add(sent_frame_offset.get())
                    .saturating_add(client_tls_bytes_flushed.get())
                    .saturating_add(response_plaintext_read.get()),
            );
            Ok(frame_index == frames.len() && parse_server_text_frame(&response).is_ok())
        },
        || progress.get(),
        |output| {
            let _ = writeln!(output, "  phase: WSS message");
            let _ = writeln!(
                output,
                "  websocket_frames_accepted: {}/{}",
                sent_frame_index.get(),
                frames.len()
            );
            let _ = writeln!(output, "  current_frame_offset: {}", sent_frame_offset.get());
            let _ = writeln!(output, "  client_tls_bytes_flushed: {}", client_tls_bytes_flushed.get());
            let _ = writeln!(
                output,
                "  response_plaintext_bytes_read: {}",
                response_plaintext_read.get()
            );
        },
    )?;
    parse_server_text_frame(&response).map_err(|error| Error::from_display("parse WSS response", error))
}

pub(crate) fn wss_rejected_upgrade_roundtrip<N>(
    sim: &mut Simulator,
    guest: &mut SmolTcpGuest,
    running: &mut N,
    tcp: TcpHandle,
    request: WssRejectedUpgradeRoundtrip<'_>,
) -> Result<Vec<u8>>
where
    N: SteppedNetwork,
{
    let mut tls = connect_wss_tls(sim, guest, running, tcp, request.host, request.ca_pem)?;
    let request_bytes = [wss_upgrade_request(request.host).as_bytes(), request.followup_request].concat();
    let mut written = 0_usize;
    let mut response = Vec::new();
    let client_tls_bytes_flushed = Cell::new(0_usize);
    let response_plaintext_read = Cell::new(0_usize);
    let progress = Cell::new(0_usize);
    sim.drive_guest_until_with_progress(
        guest,
        running,
        DriveGuestProgress {
            label: "WSS rejection followup",
            budget: super::http1::HTTP_DRIVE_BUDGET,
        },
        |guest, running| {
            write_client_plaintext_limited(
                &mut tls,
                &request_bytes,
                &mut written,
                TLS_PLAINTEXT_WRITE_BYTES_PER_STEP,
            )?;
            let io = drive_client_tls_io(guest, running, tcp, &mut tls, &mut response)?;
            client_tls_bytes_flushed.set(client_tls_bytes_flushed.get().saturating_add(io.flushed));
            response_plaintext_read.set(response.len());
            progress.set(
                client_tls_bytes_flushed
                    .get()
                    .saturating_add(response_plaintext_read.get()),
            );
            Ok(response
                .windows(request.followup_response_body.len())
                .any(|window| window == request.followup_response_body))
        },
        || progress.get(),
        |output| {
            let _ = writeln!(output, "  phase: WSS rejection followup");
            let _ = writeln!(output, "  client_tls_bytes_flushed: {}", client_tls_bytes_flushed.get());
            let _ = writeln!(
                output,
                "  response_plaintext_bytes_read: {}",
                response_plaintext_read.get()
            );
        },
    )?;
    Ok(response)
}

#[derive(Clone, Copy)]
pub(crate) struct WssRejectedUpgradeRoundtrip<'a> {
    pub(crate) host: &'a str,
    pub(crate) ca_pem: &'a str,
    pub(crate) followup_request: &'a [u8],
    pub(crate) followup_response_body: &'a [u8],
}

fn connect_wss_tls<N>(
    sim: &mut Simulator,
    guest: &mut SmolTcpGuest,
    running: &mut N,
    tcp: TcpHandle,
    host: &str,
    ca_pem: &str,
) -> Result<TlsClientSession>
where
    N: SteppedNetwork,
{
    let mut tls = client_tls(host, ca_pem)?;
    drive_tls_until(
        sim,
        guest,
        running,
        tcp,
        &mut tls,
        "TLS handshake",
        |tls, _plaintext| !tls.is_handshaking(),
    )?;
    Ok(tls)
}

fn drive_wss_upgrade<N>(
    sim: &mut Simulator,
    guest: &mut SmolTcpGuest,
    running: &mut N,
    tcp: TcpHandle,
    tls: &mut TlsClientSession,
    host: &str,
) -> Result<Vec<u8>>
where
    N: SteppedNetwork,
{
    let upgrade_request = wss_upgrade_request(host);
    let mut written = 0_usize;
    let mut response = Vec::new();
    let progress = Cell::new(0_usize);
    let written_len = Cell::new(0_usize);
    let response_len = Cell::new(0_usize);
    sim.drive_guest_until_with_progress(
        guest,
        running,
        DriveGuestProgress {
            label: "WSS upgrade",
            budget: super::http1::HTTP_DRIVE_BUDGET,
        },
        |guest, running| {
            write_client_plaintext_limited(
                tls,
                upgrade_request.as_bytes(),
                &mut written,
                TLS_PLAINTEXT_WRITE_BYTES_PER_STEP,
            )?;
            let io = drive_client_tls_io(guest, running, tcp, tls, &mut response)?;
            written_len.set(written);
            response_len.set(response.len());
            progress.set(written.saturating_add(response.len()).saturating_add(io.flushed));
            Ok(written == upgrade_request.len() && http_message_complete(&response))
        },
        || progress.get(),
        |output| {
            let _ = writeln!(output, "  phase: WSS upgrade");
            let _ = writeln!(
                output,
                "  upgrade_request_plaintext_accepted: {}/{}",
                written_len.get(),
                upgrade_request.len()
            );
            let _ = writeln!(output, "  upgrade_response_plaintext_read: {}", response_len.get());
        },
    )?;
    Ok(response)
}

fn websocket_key(request: &[u8]) -> io::Result<String> {
    let request = std::str::from_utf8(request).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    for line in request.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("sec-websocket-key") {
            return Ok(value.trim().to_owned());
        }
    }
    Err(io::Error::new(io::ErrorKind::InvalidData, "missing Sec-WebSocket-Key"))
}

fn upgrade_response(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let digest = hasher.finalize();
    let mut accept = vec![0u8; agentdp_base64::encoded_len(digest.len())];
    let Some(written) = agentdp_base64::encode(&digest, &mut accept) else {
        unreachable!("base64 output was pre-sized")
    };
    accept.truncate(written);
    let Ok(accept) = String::from_utf8(accept) else {
        unreachable!("base64 output is ASCII")
    };
    format!(
        "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    )
}

pub(crate) fn wss_upgrade_request(host: &str) -> String {
    let key = "dGhlIHNhbXBsZSBub25jZQ==";
    format!(
        "GET /ws HTTP/1.1\r\nHost: {host}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nAuthorization: Bearer {PLACEHOLDER}\r\n\r\n"
    )
}

fn write_http_ok(output: &mut impl io::Write, body: &[u8]) -> io::Result<()> {
    write!(
        output,
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )?;
    output.write_all(body)
}

pub(crate) fn client_text_frames(payload: &[u8], fragmented: bool) -> io::Result<Vec<Vec<u8>>> {
    if !fragmented || payload.len() < 2 {
        return Ok(vec![client_frame(true, 1, payload)?]);
    }
    let first_len = payload.len() / 3;
    let second_len = payload.len() / 3;
    Ok(vec![
        client_frame(false, 1, &payload[..first_len])?,
        client_frame(false, 0, &payload[first_len..first_len + second_len])?,
        client_frame(true, 0, &payload[first_len + second_len..])?,
    ])
}

fn client_frame(fin: bool, opcode: u8, payload: &[u8]) -> io::Result<Vec<u8>> {
    let mask = [0x11, 0x22, 0x33, 0x44];
    let mut frame = Vec::with_capacity(14 + payload.len());
    frame.push((if fin { 0x80 } else { 0 }) | opcode);
    write_len(&mut frame, true, payload.len())?;
    frame.extend_from_slice(&mask);
    for (index, byte) in payload.iter().enumerate() {
        frame.push(byte ^ mask[index % 4]);
    }
    Ok(frame)
}

fn server_text_frames(payload: &[u8], fragmented: bool) -> io::Result<Vec<Vec<u8>>> {
    if !fragmented || payload.len() < 2 {
        return Ok(vec![server_frame(true, 1, payload)?]);
    }
    let first_len = payload.len() / 3;
    let second_len = payload.len() / 3;
    Ok(vec![
        server_frame(false, 1, &payload[..first_len])?,
        server_frame(false, 0, &payload[first_len..first_len + second_len])?,
        server_frame(true, 0, &payload[first_len + second_len..])?,
    ])
}

fn server_frame(fin: bool, opcode: u8, payload: &[u8]) -> io::Result<Vec<u8>> {
    let mut frame = Vec::with_capacity(10 + payload.len());
    frame.push((if fin { 0x80 } else { 0 }) | opcode);
    write_len(&mut frame, false, payload.len())?;
    frame.extend_from_slice(payload);
    Ok(frame)
}

pub(crate) fn parse_server_text_frame(frame: &[u8]) -> io::Result<Vec<u8>> {
    WebSocketMessageParser::server()
        .push(frame)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated server text frame"))
}

struct WebSocketMessageParser {
    pending: Vec<u8>,
    message: Vec<u8>,
    in_fragmented_text: bool,
    expect_mask: bool,
}

impl Default for WebSocketMessageParser {
    fn default() -> Self {
        Self::client()
    }
}

impl WebSocketMessageParser {
    const fn client() -> Self {
        Self {
            pending: Vec::new(),
            message: Vec::new(),
            in_fragmented_text: false,
            expect_mask: true,
        }
    }

    const fn server() -> Self {
        Self {
            pending: Vec::new(),
            message: Vec::new(),
            in_fragmented_text: false,
            expect_mask: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.pending.extend_from_slice(bytes);
        loop {
            let Some(parsed) = parse_frame(&self.pending, self.expect_mask)? else {
                return Ok(None);
            };
            self.pending.drain(..parsed.consumed);
            match parsed.opcode {
                1 if parsed.fin => return Ok(Some(parsed.payload)),
                1 => {
                    self.in_fragmented_text = true;
                    self.message.extend_from_slice(&parsed.payload);
                }
                0 if self.in_fragmented_text => {
                    self.message.extend_from_slice(&parsed.payload);
                    if parsed.fin {
                        self.in_fragmented_text = false;
                        return Ok(Some(std::mem::take(&mut self.message)));
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsupported WebSocket frame",
                    ));
                }
            }
        }
    }
}

struct ParsedFrame {
    fin: bool,
    opcode: u8,
    consumed: usize,
    payload: Vec<u8>,
}

fn parse_frame(frame: &[u8], expect_mask: bool) -> io::Result<Option<ParsedFrame>> {
    if frame.len() < 2 {
        return Ok(None);
    }
    let fin = frame[0] & 0x80 != 0;
    let opcode = frame[0] & 0x0f;
    let masked = frame[1] & 0x80 != 0;
    if masked != expect_mask {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected WebSocket mask bit",
        ));
    }
    let mut cursor = 2;
    let len = match frame[1] & 0x7f {
        len @ 0..=125 => usize::from(len),
        126 => {
            let Some(bytes) = frame.get(cursor..cursor + 2) else {
                return Ok(None);
            };
            cursor += 2;
            usize::from(u16::from_be_bytes([bytes[0], bytes[1]]))
        }
        127 => {
            let Some(bytes) = frame.get(cursor..cursor + 8) else {
                return Ok(None);
            };
            cursor += 8;
            usize::try_from(u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        }
        _ => unreachable!(),
    };
    let mask = if expect_mask {
        let Some(mask) = frame.get(cursor..cursor + 4) else {
            return Ok(None);
        };
        cursor += 4;
        Some([mask[0], mask[1], mask[2], mask[3]])
    } else {
        None
    };
    let Some(payload) = frame.get(cursor..cursor + len) else {
        return Ok(None);
    };
    let payload = mask.map_or_else(
        || payload.to_vec(),
        |mask| {
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % 4])
                .collect()
        },
    );
    Ok(Some(ParsedFrame {
        fin,
        opcode,
        consumed: cursor + len,
        payload,
    }))
}

fn write_len(frame: &mut Vec<u8>, masked: bool, len: usize) -> io::Result<()> {
    let mask = if masked { 0x80 } else { 0 };
    if len <= 125 {
        let len = u8::try_from(len).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        frame.push(mask | len);
    } else if let Ok(len) = u16::try_from(len) {
        frame.push(mask | 0x7e);
        frame.extend_from_slice(&len.to_be_bytes());
    } else {
        let len = u64::try_from(len).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        frame.push(mask | 0x7f);
        frame.extend_from_slice(&len.to_be_bytes());
    }
    Ok(())
}

fn contains(name: &str, actual: &[u8], expected: &[u8]) -> Result<()> {
    if actual.windows(expected.len()).any(|window| window == expected) {
        Ok(())
    } else {
        Err(Error::new(format!(
            "{name}: expected to contain {expected:02x?}, got {actual:02x?}"
        )))
    }
}
