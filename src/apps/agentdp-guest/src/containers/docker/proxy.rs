#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::io::{Cursor, Read};
use std::path::PathBuf;

use agentdp_platform::ca::{CA_ENV_VARS_KEY, ca_env_vars_csv, ca_env_vars_from_env};
use serde_json::{Map, Value};
use tar::{Archive, Builder, Header};
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::{Error, Result};

mod os;

const HEADER_LIMIT: usize = 64 * 1024;
const BODY_LIMIT: usize = 256 * 1024 * 1024;
const CA_CONTAINER_PATH: &str = "/run/agentdp/ca/ca-bundle.pem";
const CA_CONTEXT_PATH: &str = ".agentdp/ca-bundle.crt";

#[derive(Debug)]
pub(crate) struct Config {
    pub listen: PathBuf,
    pub upstream: PathBuf,
    pub ca: PathBuf,
}

pub(crate) async fn run(config: Config) -> Result<()> {
    os::run(config).await
}

pub(crate) fn default_listen_path() -> PathBuf {
    os::default_listen_path()
}

pub(crate) fn default_upstream_path() -> PathBuf {
    os::default_upstream_path()
}

pub(crate) fn default_ca_path() -> PathBuf {
    super::os::CONFIG.ca_bundle_path()
}

#[derive(Clone)]
struct CaConfig {
    pem: String,
    host_path: String,
}

async fn read_request_head(client: &mut (impl AsyncRead + Unpin), pending: &mut Vec<u8>) -> Result<Option<RawRequest>> {
    let mut bytes = std::mem::take(pending);
    let header_end = loop {
        if let Some(end) = find_header_end(&bytes) {
            break end;
        }
        let mut chunk = [0; 4096];
        let read = client.read(&mut chunk).await?;
        if read == 0 {
            if bytes.is_empty() {
                return Ok(None);
            }
            return Err(Error::Message("Docker client closed mid-request".to_owned()));
        }
        bytes.extend_from_slice(&chunk[..read]);
        if bytes.len() > HEADER_LIMIT {
            return Err(Error::Message("Docker request headers exceeded proxy limit".to_owned()));
        }
    };
    RawRequest::parse(&bytes, header_end).map(Some)
}

async fn prepare_request(
    client: &mut (impl AsyncRead + Unpin),
    request: RawRequest,
    ca: &CaConfig,
) -> Result<PreparedRequest> {
    let Some(kind) = mutation_kind(&request) else {
        let Some(length) = request.content_length else {
            return Ok(prepare_unknown_length_request(request));
        };
        if length > BODY_LIMIT {
            return Ok(PreparedRequest::Response {
                bytes: response(
                    "413 Payload Too Large",
                    "agentdp docker proxy request exceeded local body limit\n",
                ),
                close_client: true,
            });
        }
        let (body, trailing) = read_fixed_body(client, &request, length).await?;
        if request.is_hijack() {
            return Ok(PreparedRequest::Forward {
                bytes: request.with_original_body(&body),
                trailing,
                tunnel: true,
            });
        }
        return Ok(PreparedRequest::Forward {
            bytes: request.with_body_close(&body),
            trailing,
            tunnel: false,
        });
    };
    let (body, trailing) = match request.content_length {
        Some(length) => {
            if length > BODY_LIMIT {
                return Ok(PreparedRequest::Response {
                    bytes: response(
                        "413 Payload Too Large",
                        "agentdp docker proxy cannot inject CA bundle into a Docker API request over the local body limit\n",
                    ),
                    close_client: true,
                });
            }
            read_fixed_body(client, &request, length).await?
        }
        None if request.is_chunked() => read_chunked_body(client, &request).await?,
        None => {
            return Ok(PreparedRequest::Response {
                bytes: response(
                    "501 Not Implemented",
                    "agentdp docker proxy cannot inject CA bundle into Docker API mutation requests without a supported body encoding\n",
                ),
                close_client: true,
            });
        }
    };

    let mutation = match kind {
        MutationKind::ContainerCreate => inject_container_create(&body, &ca.host_path),
        MutationKind::Build => rewrite_build_context(&body, &dockerfile_query_path(&request.path), &ca.pem),
    };
    let mutated = match mutation {
        Ok(mutated) => mutated,
        Err(error) => {
            eprintln!(
                "guestd docker proxy: passthrough mutation_error path={} error={error}",
                request.path
            );
            return Ok(PreparedRequest::Forward {
                bytes: request.with_body_close(&body),
                trailing,
                tunnel: false,
            });
        }
    };

    let Some(mutated) = mutated else {
        eprintln!(
            "guestd docker proxy: passthrough mutation_skipped path={}",
            request.path
        );
        return Ok(PreparedRequest::Forward {
            bytes: request.with_body_close(&body),
            trailing,
            tunnel: false,
        });
    };

    let bytes = request.with_body_close(&mutated);
    eprintln!("guestd docker proxy: mutated path={}", request.path);
    Ok(PreparedRequest::Forward {
        bytes,
        trailing,
        tunnel: false,
    })
}

async fn copy_response(
    upstream: &mut agentdp_platform::socket::AsyncLocalSocket,
    client: &mut agentdp_platform::socket::AsyncLocalSocket,
) -> Result<()> {
    let mut bytes = Vec::new();
    let header_end = loop {
        if let Some(end) = find_header_end(&bytes) {
            break end;
        }
        let mut chunk = [0; 4096];
        let read = upstream.read(&mut chunk).await?;
        if read == 0 {
            return Err(Error::Message(
                "Docker upstream closed before response headers".to_owned(),
            ));
        }
        bytes.extend_from_slice(&chunk[..read]);
        if bytes.len() > HEADER_LIMIT {
            return Err(Error::Message(
                "Docker response headers exceeded proxy limit".to_owned(),
            ));
        }
    };
    let head = bytes[..header_end].to_vec();
    let body_remainder = bytes[header_end..].to_vec();
    let content_length = response_content_length(&head)?;
    let chunked = response_is_chunked(&head)?;
    client.write_all(&head).await?;
    if let Some(length) = content_length {
        return copy_content_length_response(upstream, client, body_remainder, length).await;
    }
    if chunked {
        return copy_chunked_response(upstream, client, body_remainder).await;
    }
    let _bytes = agentdp_platform::socket::copy_local_socket(upstream, client).await?;
    Ok(())
}

async fn copy_content_length_response(
    upstream: &mut agentdp_platform::socket::AsyncLocalSocket,
    client: &mut agentdp_platform::socket::AsyncLocalSocket,
    body_remainder: Vec<u8>,
    length: usize,
) -> Result<()> {
    let buffered = body_remainder.len().min(length);
    if buffered != 0 {
        client.write_all(&body_remainder[..buffered]).await?;
    }
    let mut remaining = length.saturating_sub(buffered);
    let mut chunk = [0; 8192];
    while remaining != 0 {
        let read = upstream.read(&mut chunk).await?;
        if read == 0 {
            return Err(Error::Message("Docker upstream closed mid-response body".to_owned()));
        }
        let forward = read.min(remaining);
        client.write_all(&chunk[..forward]).await?;
        remaining -= forward;
    }
    Ok(())
}

async fn copy_chunked_response(
    upstream: &mut agentdp_platform::socket::AsyncLocalSocket,
    client: &mut agentdp_platform::socket::AsyncLocalSocket,
    body_remainder: Vec<u8>,
) -> Result<()> {
    let mut bytes = body_remainder;
    let mut position = 0;
    let mut written = 0;
    write_new_response_bytes(client, &bytes, &mut written).await?;
    loop {
        let line_end = loop {
            if let Some(offset) = find_crlf(&bytes[position..]) {
                break position + offset;
            }
            read_more_response(upstream, client, &mut bytes, &mut written).await?;
        };
        let line = std::str::from_utf8(&bytes[position..line_end])
            .map_err(|error| Error::Message(format!("Docker response chunk header was not UTF-8: {error}")))?;
        let size_text = line.split_once(';').map_or(line, |(size, _)| size).trim();
        let size = usize::from_str_radix(size_text, 16).map_err(|error| {
            Error::Message(format!("Docker response chunk size was invalid `{size_text}`: {error}"))
        })?;
        position = line_end + 2;
        if size == 0 {
            loop {
                if let Some(end) = find_chunk_trailer_end(&bytes[position..]) {
                    let complete = position + end;
                    if written > complete {
                        return Ok(());
                    }
                    client.write_all(&bytes[written..complete]).await?;
                    return Ok(());
                }
                read_more_response(upstream, client, &mut bytes, &mut written).await?;
            }
        }
        if position.saturating_add(size) > BODY_LIMIT {
            return Err(Error::Message(
                "Docker chunked response body exceeded proxy limit".to_owned(),
            ));
        }
        while bytes.len() < position + size + 2 {
            read_more_response(upstream, client, &mut bytes, &mut written).await?;
        }
        position += size;
        if bytes.get(position..position + 2) != Some(b"\r\n") {
            return Err(Error::Message(
                "Docker response chunk was missing trailing CRLF".to_owned(),
            ));
        }
        position += 2;
    }
}

async fn read_more_response(
    upstream: &mut agentdp_platform::socket::AsyncLocalSocket,
    client: &mut agentdp_platform::socket::AsyncLocalSocket,
    bytes: &mut Vec<u8>,
    written: &mut usize,
) -> Result<()> {
    let mut chunk = [0; 8192];
    let read = upstream.read(&mut chunk).await?;
    if read == 0 {
        return Err(Error::Message("Docker upstream closed mid-response body".to_owned()));
    }
    bytes.extend_from_slice(&chunk[..read]);
    write_new_response_bytes(client, bytes, written).await
}

async fn write_new_response_bytes(
    client: &mut agentdp_platform::socket::AsyncLocalSocket,
    bytes: &[u8],
    written: &mut usize,
) -> Result<()> {
    if *written < bytes.len() {
        client.write_all(&bytes[*written..]).await?;
        *written = bytes.len();
    }
    Ok(())
}

fn response_content_length(head: &[u8]) -> Result<Option<usize>> {
    let head = std::str::from_utf8(head)
        .map_err(|error| Error::Message(format!("Docker response headers were not UTF-8: {error}")))?;
    for line in head.trim_end_matches("\r\n").split("\r\n").skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse::<usize>()
                .map(Some)
                .map_err(|error| Error::Message(format!("Docker response Content-Length was invalid: {error}")));
        }
    }
    Ok(None)
}

fn response_is_chunked(head: &[u8]) -> Result<bool> {
    let head = std::str::from_utf8(head)
        .map_err(|error| Error::Message(format!("Docker response headers were not UTF-8: {error}")))?;
    Ok(head
        .trim_end_matches("\r\n")
        .split("\r\n")
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("transfer-encoding"))
        .any(|(_, value)| {
            value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        }))
}

fn prepare_unknown_length_request(request: RawRequest) -> PreparedRequest {
    let tunnel = matches!(request.method.as_str(), "POST" | "PUT" | "PATCH") || request.is_hijack();
    let (bytes, trailing) = if tunnel {
        (request.into_original_bytes(), Vec::new())
    } else {
        (request.with_body_close(&[]), request.body_remainder)
    };
    PreparedRequest::Forward {
        bytes,
        trailing,
        tunnel,
    }
}

async fn read_fixed_body(
    client: &mut (impl AsyncRead + Unpin),
    request: &RawRequest,
    length: usize,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut trailing = Vec::new();
    let mut body = if request.body_remainder.len() > length {
        trailing.extend_from_slice(&request.body_remainder[length..]);
        request.body_remainder[..length].to_vec()
    } else {
        request.body_remainder.clone()
    };
    if body.len() < length {
        let original_len = body.len();
        body.resize(length, 0);
        client.read_exact(&mut body[original_len..]).await?;
    }
    Ok((body, trailing))
}

async fn read_chunked_body(client: &mut (impl AsyncRead + Unpin), request: &RawRequest) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut bytes = request.body_remainder.clone();
    let mut position = 0;
    let mut body = Vec::new();
    loop {
        let line_end = loop {
            if let Some(offset) = find_crlf(&bytes[position..]) {
                break position + offset;
            }
            read_more(client, &mut bytes).await?;
        };
        let line = std::str::from_utf8(&bytes[position..line_end])
            .map_err(|error| Error::Message(format!("Docker chunk header was not UTF-8: {error}")))?;
        let size_text = line.split_once(';').map_or(line, |(size, _)| size).trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|error| Error::Message(format!("Docker chunk size was invalid `{size_text}`: {error}")))?;
        position = line_end + 2;
        if size == 0 {
            loop {
                if let Some(end) = find_chunk_trailer_end(&bytes[position..]) {
                    let trailing = bytes[position + end..].to_vec();
                    return Ok((body, trailing));
                }
                read_more(client, &mut bytes).await?;
            }
        }
        if body.len().saturating_add(size) > BODY_LIMIT {
            return Err(Error::Message(
                "Docker chunked request body exceeded proxy limit".to_owned(),
            ));
        }
        while bytes.len() < position + size + 2 {
            read_more(client, &mut bytes).await?;
        }
        body.extend_from_slice(&bytes[position..position + size]);
        position += size;
        if bytes.get(position..position + 2) != Some(b"\r\n") {
            return Err(Error::Message("Docker chunk was missing trailing CRLF".to_owned()));
        }
        position += 2;
    }
}

async fn read_more(client: &mut (impl AsyncRead + Unpin), bytes: &mut Vec<u8>) -> Result<()> {
    let mut chunk = [0; 8192];
    let read = client.read(&mut chunk).await?;
    if read == 0 {
        return Err(Error::Message("Docker client closed mid-request body".to_owned()));
    }
    bytes.extend_from_slice(&chunk[..read]);
    Ok(())
}

fn response(status: &str, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn find_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|window| window == b"\r\n")
}

fn find_chunk_trailer_end(bytes: &[u8]) -> Option<usize> {
    if bytes.starts_with(b"\r\n") {
        Some(2)
    } else {
        find_header_end(bytes)
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

enum PreparedRequest {
    Forward {
        bytes: Vec<u8>,
        trailing: Vec<u8>,
        tunnel: bool,
    },
    Response {
        bytes: Vec<u8>,
        close_client: bool,
    },
}

struct RawRequest {
    method: String,
    path: String,
    request_line: String,
    headers: Vec<HeaderLine>,
    content_length: Option<usize>,
    head: Vec<u8>,
    body_remainder: Vec<u8>,
}

impl RawRequest {
    fn parse(bytes: &[u8], header_end: usize) -> Result<Self> {
        let head = bytes[..header_end].to_vec();
        let body_remainder = bytes[header_end..].to_vec();
        let head_text = std::str::from_utf8(&head)
            .map_err(|error| Error::Message(format!("Docker request headers were not UTF-8: {error}")))?;
        let mut lines = head_text.trim_end_matches("\r\n").split("\r\n");
        let request_line = lines
            .next()
            .ok_or_else(|| Error::Message("Docker request was missing request line".to_owned()))?
            .to_owned();
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts
            .next()
            .ok_or_else(|| Error::Message("Docker request was missing method".to_owned()))?
            .to_owned();
        let path = request_parts
            .next()
            .ok_or_else(|| Error::Message("Docker request was missing path".to_owned()))?
            .to_owned();
        let mut headers = Vec::new();
        let mut content_length = None;
        for line in lines {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse::<usize>().ok();
            }
            headers.push(HeaderLine {
                name: name.to_owned(),
                value: value.trim_start().to_owned(),
            });
        }

        Ok(Self {
            method,
            path,
            request_line,
            headers,
            content_length,
            head,
            body_remainder,
        })
    }

    fn into_original_bytes(self) -> Vec<u8> {
        let mut bytes = self.head;
        bytes.extend_from_slice(&self.body_remainder);
        bytes
    }

    fn with_original_body(&self, body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.head.len() + body.len());
        bytes.extend_from_slice(&self.head);
        bytes.extend_from_slice(body);
        bytes
    }

    fn with_body_close(&self, body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.head.len() + body.len());
        bytes.extend_from_slice(self.request_line.as_bytes());
        bytes.extend_from_slice(b"\r\n");
        for header in &self.headers {
            if header.name.eq_ignore_ascii_case("content-length")
                || header.name.eq_ignore_ascii_case("transfer-encoding")
                || header.name.eq_ignore_ascii_case("trailer")
                || header.name.eq_ignore_ascii_case("connection")
            {
                continue;
            }
            bytes.extend_from_slice(header.name.as_bytes());
            bytes.extend_from_slice(b": ");
            bytes.extend_from_slice(header.value.as_bytes());
            bytes.extend_from_slice(b"\r\n");
        }
        bytes.extend_from_slice(format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes());
        bytes.extend_from_slice(body);
        bytes
    }

    fn is_chunked(&self) -> bool {
        self.headers
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case("transfer-encoding"))
            .any(|header| {
                header
                    .value
                    .split(',')
                    .any(|value| value.trim().eq_ignore_ascii_case("chunked"))
            })
    }

    fn is_hijack(&self) -> bool {
        let upgrades_connection = self
            .headers
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case("connection"))
            .any(|header| {
                header
                    .value
                    .split(',')
                    .any(|value| value.trim().eq_ignore_ascii_case("upgrade"))
            });
        upgrades_connection
            || self
                .headers
                .iter()
                .any(|header| header.name.eq_ignore_ascii_case("upgrade"))
    }
}

struct HeaderLine {
    name: String,
    value: String,
}

#[derive(Debug, Clone, Copy)]
enum MutationKind {
    ContainerCreate,
    Build,
}

fn mutation_kind(request: &RawRequest) -> Option<MutationKind> {
    if request.method != "POST" {
        return None;
    }
    let path = request.path.split('?').next().unwrap_or(request.path.as_str());
    if path.ends_with("/containers/create") {
        Some(MutationKind::ContainerCreate)
    } else if path.ends_with("/build") {
        Some(MutationKind::Build)
    } else {
        None
    }
}

fn inject_container_create(body: &[u8], ca_host_path: &str) -> Result<Option<Vec<u8>>> {
    let mut value = serde_json::from_slice::<Value>(body)?;
    let Value::Object(object) = &mut value else {
        return Ok(None);
    };
    inject_env(object);
    inject_bind(object, ca_host_path);
    Ok(Some(serde_json::to_vec(&value)?))
}

fn inject_env(object: &mut Map<String, Value>) {
    let env = array_field(object, "Env");
    let Some(env) = env else {
        return;
    };
    let env_vars = ca_env_vars_from_env();
    for key in &env_vars {
        upsert_env(env, key, CA_CONTAINER_PATH);
    }
    upsert_env(env, CA_ENV_VARS_KEY, &ca_env_vars_csv(&env_vars));
}

fn upsert_env(env: &mut Vec<Value>, key: &str, value: &str) {
    let prefix = format!("{key}=");
    if env
        .iter()
        .any(|entry| entry.as_str().is_some_and(|entry| entry.starts_with(&prefix)))
    {
        return;
    }
    env.push(Value::String(format!("{prefix}{value}")));
}

fn inject_bind(object: &mut Map<String, Value>, ca_host_path: &str) {
    let host_config = object_field(object, "HostConfig");
    let Some(host_config) = host_config else {
        return;
    };
    let binds = array_field(host_config, "Binds");
    let Some(binds) = binds else {
        return;
    };
    let bind = format!("{ca_host_path}:{CA_CONTAINER_PATH}:ro");
    if !binds.iter().any(|entry| entry.as_str() == Some(bind.as_str())) {
        binds.push(Value::String(bind));
    }
}

fn object_field<'a>(object: &'a mut Map<String, Value>, key: &str) -> Option<&'a mut Map<String, Value>> {
    let field = object.entry(key).or_insert_with(|| Value::Object(Map::new()));
    if field.is_null() {
        *field = Value::Object(Map::new());
    }
    field.as_object_mut()
}

fn array_field<'a>(object: &'a mut Map<String, Value>, key: &str) -> Option<&'a mut Vec<Value>> {
    let field = object.entry(key).or_insert_with(|| Value::Array(Vec::new()));
    if field.is_null() {
        *field = Value::Array(Vec::new());
    }
    field.as_array_mut()
}

fn rewrite_build_context(body: &[u8], dockerfile_path: &str, ca: &str) -> Result<Option<Vec<u8>>> {
    let mut archive = Archive::new(Cursor::new(body));
    let mut output = Vec::new();
    let mut builder = Builder::new(&mut output);
    let mut found_dockerfile = false;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_string_lossy().into_owned();
        let normalized = normalize_path_text(&path);
        let mut contents = Vec::new();
        entry.read_to_end(&mut contents)?;
        if normalized.as_deref() == Some(dockerfile_path) {
            let dockerfile = String::from_utf8(contents).map_err(|error| {
                Error::Message(format!("Dockerfile `{dockerfile_path}` was not valid UTF-8: {error}"))
            })?;
            contents = inject_dockerfile_ca(&dockerfile).into_bytes();
            found_dockerfile = true;
        }

        let mut header = entry.header().clone();
        header.set_size(u64::try_from(contents.len()).unwrap_or(u64::MAX));
        header.set_cksum();
        builder.append(&header, Cursor::new(contents))?;
    }

    if !found_dockerfile {
        return Ok(None);
    }

    append_ca_entry(&mut builder, ca.as_bytes())?;
    builder.finish()?;
    drop(builder);
    Ok(Some(output))
}

fn dockerfile_query_path(path: &str) -> String {
    let Some((_, query)) = path.split_once('?') else {
        return "Dockerfile".to_owned();
    };
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if key == "dockerfile" {
            return normalize_dockerfile_query_value(value).unwrap_or_else(|| "Dockerfile".to_owned());
        }
    }
    "Dockerfile".to_owned()
}

fn normalize_path_text(path: &str) -> Option<String> {
    if path.is_empty() || path.starts_with('/') || path.contains('\\') {
        return None;
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => return None,
            part => parts.push(part),
        }
    }
    if parts.is_empty() { None } else { Some(parts.join("/")) }
}

fn normalize_dockerfile_query_value(value: &str) -> Option<String> {
    let decoded = percent_decode(value)?;
    normalize_path_text(&decoded)
}

fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                output.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = hex_value(bytes[index + 1])?;
                let low = hex_value(bytes[index + 2])?;
                output.push((high << 4) | low);
                index += 3;
            }
            byte => {
                output.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(output).ok()
}

const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn append_ca_entry(builder: &mut Builder<&mut Vec<u8>>, ca: &[u8]) -> Result<()> {
    let mut header = Header::new_gnu();
    header.set_path(CA_CONTEXT_PATH)?;
    header.set_size(u64::try_from(ca.len()).unwrap_or(u64::MAX));
    header.set_mode(0o644);
    header.set_cksum();
    builder.append(&header, Cursor::new(ca))?;
    Ok(())
}

fn inject_dockerfile_ca(input: &str) -> String {
    super::super::core::build_ca::inject_ca(input, &context_copy_instruction())
}

fn context_copy_instruction() -> String {
    format!(
        "COPY {CA_CONTEXT_PATH} {}",
        super::super::core::build_ca::CA_CONTAINER_PATH
    )
}

#[cfg(test)]
mod tests {
    use super::super::super::core::build_ca;
    use super::*;
    #[cfg(unix)]
    use tokio::net::UnixStream;
    #[cfg(target_os = "linux")]
    use tokio::time::{Duration, timeout};

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn direct_bind_refuses_public_docker_socket() {
        let error = os::bind_listener_for_test(&default_listen_path())
            .await
            .expect_err("direct /run/docker.sock bind must fail");

        assert!(
            error.to_string().contains("must be provided by socket activation"),
            "{error}"
        );
    }

    #[test]
    fn container_create_injects_ca_mount_and_common_tls_env() {
        let body = br#"{"Image":"node","Env":["FOO=bar"],"HostConfig":{"Binds":["/tmp/a:/tmp/a:ro"]}}"#;

        let output = inject_container_create(body, "/var/lib/agentdp/ca/ca-bundle.pem")
            .expect("inject")
            .expect("mutated");
        let json = serde_json::from_slice::<Value>(&output).expect("json");

        let env = json["Env"].as_array().expect("env");
        assert_common_tls_env(env);
        assert_ca_env_vars_list(env);
        let binds = json["HostConfig"]["Binds"].as_array().expect("binds");
        assert!(
            binds
                .iter()
                .any(|value| value == "/var/lib/agentdp/ca/ca-bundle.pem:/run/agentdp/ca/ca-bundle.pem:ro")
        );
    }

    #[test]
    fn container_create_treats_null_env_and_binds_as_empty() {
        let body = br#"{"Image":"node","Env":null,"HostConfig":{"Binds":null}}"#;

        let output = inject_container_create(body, "/var/lib/agentdp/ca/ca-bundle.pem")
            .expect("inject")
            .expect("mutated");
        let json = serde_json::from_slice::<Value>(&output).expect("json");

        assert_common_tls_env(json["Env"].as_array().expect("env"));
        assert_eq!(
            json["HostConfig"]["Binds"],
            serde_json::json!(["/var/lib/agentdp/ca/ca-bundle.pem:/run/agentdp/ca/ca-bundle.pem:ro"])
        );
    }

    #[test]
    fn container_create_preserves_existing_tls_env() {
        let body = br#"{"Image":"node","Env":["SSL_CERT_FILE=/custom/ca.pem"],"HostConfig":{"Binds":[]}}"#;

        let output = inject_container_create(body, "/var/lib/agentdp/ca/ca-bundle.pem")
            .expect("inject")
            .expect("mutated");
        let json = serde_json::from_slice::<Value>(&output).expect("json");
        let env = json["Env"].as_array().expect("env");

        assert!(env.iter().any(|value| value == "SSL_CERT_FILE=/custom/ca.pem"));
        assert!(
            !env.iter()
                .any(|value| value == "SSL_CERT_FILE=/run/agentdp/ca/ca-bundle.pem")
        );
        assert!(
            env.iter()
                .any(|value| value == "CURL_CA_BUNDLE=/run/agentdp/ca/ca-bundle.pem")
        );
    }

    #[test]
    fn container_create_treats_null_host_config_as_empty_object() {
        let body = br#"{"Image":"node","HostConfig":null}"#;

        let output = inject_container_create(body, "/tmp/test-ca.pem")
            .expect("inject")
            .expect("mutated");
        let json = serde_json::from_slice::<Value>(&output).expect("json");

        assert_eq!(
            json["HostConfig"]["Binds"],
            serde_json::json!(["/tmp/test-ca.pem:/run/agentdp/ca/ca-bundle.pem:ro"])
        );
    }

    #[test]
    fn build_context_rewrites_requested_dockerfile() {
        let body = tar_with_file("docker/app.Dockerfile", b"FROM alpine\nRUN true\n");

        let output = rewrite_build_context(&body, "docker/app.Dockerfile", "CA PEM")
            .expect("rewrite")
            .expect("mutated");
        let files = read_tar_files(&output);

        let dockerfile = String::from_utf8(
            files
                .iter()
                .find(|(path, _)| path == "docker/app.Dockerfile")
                .expect("dockerfile")
                .1
                .clone(),
        )
        .expect("utf8");
        assert!(dockerfile.contains(build_ca::INJECTION_MARKER));
        assert!(
            files
                .iter()
                .any(|(path, contents)| path == CA_CONTEXT_PATH && contents == b"CA PEM")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_request_preserves_buffered_next_request_after_mutation() {
        let body = br#"{"Image":"node"}"#;
        let next = b"GET /v1.44/version HTTP/1.1\r\nHost: docker\r\n\r\n";
        let request = request_with_body_and_trailing("POST /v1.44/containers/create HTTP/1.1", body, next);
        let (mut client, _server) = UnixStream::pair().expect("stream pair");

        let prepared = prepare_request(&mut client, request, &ca_config())
            .await
            .expect("prepare");

        match prepared {
            PreparedRequest::Forward {
                bytes,
                trailing,
                tunnel,
            } => {
                assert!(!tunnel);
                assert_eq!(trailing, next);
                let text = String::from_utf8(bytes).expect("utf8");
                assert!(text.contains("Connection: close"));
                assert!(text.contains("NODE_EXTRA_CA_CERTS=/run/agentdp/ca/ca-bundle.pem"));
                assert!(text.contains("SSL_CERT_FILE=/run/agentdp/ca/ca-bundle.pem"));
            }
            PreparedRequest::Response { .. } => panic!("expected forwarded request"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_request_preserves_buffered_next_request_after_no_body_request() {
        let next = b"POST /v1.44/containers/create HTTP/1.1\r\nHost: docker\r\nContent-Length: 2\r\n\r\n{}";
        let bytes = [b"GET /v1.44/version HTTP/1.1\r\nHost: docker\r\n\r\n".as_slice(), next].concat();
        let request = RawRequest::parse(&bytes, find_header_end(&bytes).expect("header end")).expect("request");
        let (mut client, _server) = UnixStream::pair().expect("stream pair");

        let prepared = prepare_request(&mut client, request, &ca_config())
            .await
            .expect("prepare");

        match prepared {
            PreparedRequest::Forward {
                bytes,
                trailing,
                tunnel,
            } => {
                assert!(!tunnel);
                assert_eq!(trailing, next);
                let text = String::from_utf8(bytes).expect("utf8");
                assert!(text.starts_with("GET /v1.44/version HTTP/1.1\r\n"));
                assert!(text.contains("Content-Length: 0"));
                assert!(text.contains("Connection: close"));
            }
            PreparedRequest::Response { .. } => panic!("expected forwarded request"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn chunked_build_request_is_decoded_and_rewritten() {
        let body = tar_with_file("Dockerfile", b"FROM alpine\nRUN true\n");
        let chunked = chunked_body(&body);
        let request = request_with_chunked_body("POST /v1.44/build HTTP/1.1", &chunked);
        let (mut client, _server) = UnixStream::pair().expect("stream pair");

        let prepared = prepare_request(&mut client, request, &ca_config())
            .await
            .expect("prepare");

        match prepared {
            PreparedRequest::Forward {
                bytes,
                trailing,
                tunnel,
            } => {
                assert!(!tunnel);
                assert!(trailing.is_empty());
                let body_start = find_header_end(&bytes).expect("body");
                let head = std::str::from_utf8(&bytes[..body_start]).expect("utf8");
                assert!(head.contains("Content-Length: "));
                assert!(!head.contains("Transfer-Encoding"));
                let files = read_tar_files(&bytes[body_start..]);
                assert!(
                    files
                        .iter()
                        .any(|(path, contents)| path == CA_CONTEXT_PATH && contents == b"CA PEM")
                );
            }
            PreparedRequest::Response { .. } => panic!("expected forwarded request"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn hijack_request_preserves_upgrade_headers_and_tunnels() {
        let body = br#"{"Detach":false,"Tty":true}"#;
        let request = request_with_headers_and_body(
            "POST /v1.44/exec/id/start HTTP/1.1",
            "Connection: Upgrade\r\nUpgrade: tcp\r\n",
            body,
        );
        let (mut client, _server) = UnixStream::pair().expect("stream pair");

        let prepared = prepare_request(&mut client, request, &ca_config())
            .await
            .expect("prepare");

        match prepared {
            PreparedRequest::Forward { bytes, tunnel, .. } => {
                assert!(tunnel);
                let text = String::from_utf8(bytes).expect("utf8");
                assert!(text.contains("Connection: Upgrade"));
                assert!(text.contains("Upgrade: tcp"));
            }
            PreparedRequest::Response { .. } => panic!("expected forwarded request"),
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn proxy_closes_non_tunnel_client_after_content_length_response() {
        let root = test_socket_root("proxy-response");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let listen = root.join("docker.sock");
        let upstream = root.join("upstream.sock");
        let ca = root.join("ca.pem");
        tokio::fs::write(&ca, "CA PEM").await.expect("write ca");
        let upstream_listener = agentdp_platform::socket::bind_local_socket(&upstream)
            .await
            .expect("bind upstream");
        let proxy = tokio::spawn(os::run(Config {
            listen: listen.clone(),
            upstream: upstream.clone(),
            ca,
        }));
        wait_for_socket(&listen).await;

        let upstream_task = tokio::spawn(async move {
            let mut stream = upstream_listener.accept().await.expect("accept upstream");
            let mut request = [0_u8; 512];
            let read = stream.read(&mut request).await.expect("read request");
            assert!(
                std::str::from_utf8(&request[..read])
                    .expect("request utf8")
                    .starts_with("GET /v1.44/containers/json HTTP/1.1"),
            );
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n[]")
                .await
                .expect("write response");
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let mut client = agentdp_platform::socket::connect_local_socket(&listen)
            .await
            .expect("connect client");
        client
            .write_all(b"GET /v1.44/containers/json HTTP/1.1\r\nHost: docker\r\n\r\n")
            .await
            .expect("write request");
        client.shutdown_write().await.expect("shutdown request");
        let mut response = Vec::new();

        timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .expect("proxy did not close client after complete response")
            .expect("read response");

        assert_eq!(
            std::str::from_utf8(&response).expect("response utf8"),
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n[]"
        );
        upstream_task.abort();
        proxy.abort();
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn proxy_does_not_half_close_upstream_before_response() {
        let root = test_socket_root("proxy-upstream-half-close");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let listen = root.join("docker.sock");
        let upstream = root.join("upstream.sock");
        let ca = root.join("ca.pem");
        tokio::fs::write(&ca, "CA PEM").await.expect("write ca");
        let upstream_listener = agentdp_platform::socket::bind_local_socket(&upstream)
            .await
            .expect("bind upstream");
        let proxy = tokio::spawn(os::run(Config {
            listen: listen.clone(),
            upstream: upstream.clone(),
            ca,
        }));
        wait_for_socket(&listen).await;

        let upstream_task = tokio::spawn(async move {
            let mut stream = upstream_listener.accept().await.expect("accept upstream");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 128];
            loop {
                let read = stream.read(&mut chunk).await.expect("read request");
                assert_ne!(read, 0, "upstream was closed before request headers completed");
                request.extend_from_slice(&chunk[..read]);
                if find_header_end(&request).is_some() {
                    break;
                }
            }

            let mut extra = [0_u8; 1];
            let status = match timeout(Duration::from_millis(50), stream.read(&mut extra)).await {
                Ok(Ok(0)) => "499 status code 499",
                Ok(Ok(_)) => "400 unexpected request body",
                Ok(Err(error)) => panic!("upstream read failed: {error}"),
                Err(_) => "200 OK",
            };
            let body = if status == "200 OK" { "OK" } else { "" };
            stream
                .write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes())
                .await
                .expect("write response");
        });

        let mut client = agentdp_platform::socket::connect_local_socket(&listen)
            .await
            .expect("connect client");
        client
            .write_all(b"GET /v1.54/version HTTP/1.1\r\nHost: docker\r\n\r\n")
            .await
            .expect("write request");
        client.shutdown_write().await.expect("shutdown request");
        let mut response = Vec::new();

        timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .expect("proxy did not close client after complete response")
            .expect("read response");

        assert!(
            std::str::from_utf8(&response)
                .expect("response utf8")
                .starts_with("HTTP/1.1 200 OK"),
            "{}",
            std::str::from_utf8(&response).expect("response utf8")
        );
        upstream_task.await.expect("upstream task");
        proxy.abort();
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[test]
    fn dockerfile_query_path_decodes_relative_path() {
        assert_eq!(
            dockerfile_query_path("/v1.44/build?dockerfile=docker%2Fapp.Dockerfile&t=example"),
            "docker/app.Dockerfile"
        );
    }

    #[test]
    fn dockerfile_query_path_rejects_non_archive_paths() {
        assert_eq!(
            dockerfile_query_path("/v1.44/build?dockerfile=..%2FDockerfile"),
            "Dockerfile"
        );
        assert_eq!(
            dockerfile_query_path("/v1.44/build?dockerfile=dir%5CDockerfile"),
            "Dockerfile"
        );
        assert_eq!(
            dockerfile_query_path("/v1.44/build?dockerfile=%2Ftmp%2FDockerfile"),
            "Dockerfile"
        );
    }

    fn request_with_body_and_trailing(request_line: &str, body: &[u8], trailing: &[u8]) -> RawRequest {
        request_with_headers_body_and_trailing(request_line, "", body, trailing)
    }

    fn request_with_headers_and_body(request_line: &str, headers: &str, body: &[u8]) -> RawRequest {
        request_with_headers_body_and_trailing(request_line, headers, body, &[])
    }

    fn request_with_headers_body_and_trailing(
        request_line: &str,
        headers: &str,
        body: &[u8],
        trailing: &[u8],
    ) -> RawRequest {
        let mut bytes = format!(
            "{request_line}\r\nHost: docker\r\n{headers}Content-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        bytes.extend_from_slice(body);
        bytes.extend_from_slice(trailing);
        RawRequest::parse(&bytes, find_header_end(&bytes).expect("header end")).expect("request")
    }

    fn request_with_chunked_body(request_line: &str, body: &[u8]) -> RawRequest {
        let mut bytes = format!("{request_line}\r\nHost: docker\r\nTransfer-Encoding: chunked\r\n\r\n").into_bytes();
        bytes.extend_from_slice(body);
        RawRequest::parse(&bytes, find_header_end(&bytes).expect("header end")).expect("request")
    }

    fn ca_config() -> CaConfig {
        CaConfig {
            pem: "CA PEM".to_owned(),
            host_path: "/var/lib/agentdp/ca/ca-bundle.pem".to_owned(),
        }
    }

    fn chunked_body(body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for chunk in body.chunks(1024) {
            bytes.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            bytes.extend_from_slice(chunk);
            bytes.extend_from_slice(b"\r\n");
        }
        bytes.extend_from_slice(b"0\r\n\r\n");
        bytes
    }

    #[cfg(target_os = "linux")]
    fn test_socket_root(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("agentdp-docker-proxy-{name}-{}", std::process::id()))
    }

    #[cfg(target_os = "linux")]
    async fn wait_for_socket(path: &std::path::Path) {
        for _ in 0..100 {
            if agentdp_platform::socket::connect_local_socket(path).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("proxy socket did not become ready: {}", path.display());
    }

    fn assert_common_tls_env(env: &[Value]) {
        for key in agentdp_platform::ca::DEFAULT_CA_ENV_VARS {
            assert!(
                env.iter()
                    .any(|value| value == &format!("{key}=/run/agentdp/ca/ca-bundle.pem")),
                "missing {key}"
            );
        }
    }

    fn assert_ca_env_vars_list(env: &[Value]) {
        assert!(
            env.iter().any(|value| value
                == &format!(
                    "{CA_ENV_VARS_KEY}={}",
                    agentdp_platform::ca::default_ca_env_vars_csv()
                )),
            "missing {CA_ENV_VARS_KEY}"
        );
    }

    fn tar_with_file(path: &str, contents: &[u8]) -> Vec<u8> {
        let mut output = Vec::new();
        {
            let mut builder = Builder::new(&mut output);
            let mut header = Header::new_gnu();
            header.set_path(path).expect("path");
            header.set_size(u64::try_from(contents.len()).expect("size"));
            header.set_mode(0o644);
            header.set_cksum();
            builder.append(&header, Cursor::new(contents)).expect("append");
            builder.finish().expect("finish");
        }
        output
    }

    fn read_tar_files(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
        let mut archive = Archive::new(Cursor::new(bytes));
        let entries = archive.entries().expect("entries");
        entries
            .map(|entry| {
                let mut entry = entry.expect("entry");
                let path = entry.path().expect("path").to_string_lossy().into_owned();
                let mut contents = Vec::new();
                entry.read_to_end(&mut contents).expect("read");
                (path, contents)
            })
            .collect()
    }
}
