use std::borrow::Cow;
use std::io;

use crate::buffers::BufferPool;
use crate::{Authority, Error, RuntimeSecrets};

const SECRET_PLACEHOLDER_PREFIX: &[u8] = b"AGENTDP_SECRET_";

pub(super) struct HeaderRewriteScratch {
    decoded: Vec<u8>,
    encoded: Vec<u8>,
    rewrite: Vec<u8>,
}

impl HeaderRewriteScratch {
    pub(super) fn new(buffers: &BufferPool) -> Self {
        let capacity = buffers.limits().small_byte_capacity;
        Self {
            decoded: Vec::with_capacity(capacity),
            encoded: Vec::with_capacity(capacity),
            rewrite: Vec::with_capacity(capacity),
        }
    }

    pub(super) const fn rewrite(&mut self) -> &mut Vec<u8> {
        &mut self.rewrite
    }
}

pub(crate) fn process(input: &[u8], output: &mut Vec<u8>) -> io::Result<bool> {
    reject_unresolved_secret_placeholders(input).map_err(io::Error::other)?;
    output.extend_from_slice(input);
    Ok(!output.is_empty())
}

pub(crate) fn copy(input: &[u8], output: &mut Vec<u8>) -> bool {
    output.extend_from_slice(input);
    !output.is_empty()
}

pub(super) fn substitute_bytes_for_host<'b>(
    secrets: &RuntimeSecrets,
    host: &str,
    payload: &'b [u8],
) -> Result<Cow<'b, [u8]>, Error> {
    let authority = Authority::new(host);
    let mut output = None;
    for binding in secrets.iter() {
        let current = output.as_deref().unwrap_or(payload);
        if !current
            .windows(binding.placeholder.len())
            .any(|window| window == binding.placeholder.as_bytes())
        {
            continue;
        }
        if !binding.allows_authority(&authority) {
            return Err(Error::UnauthorizedSecretHost(host.to_owned()));
        }
        output = Some(replace_bytes(
            current,
            binding.placeholder.as_bytes(),
            binding.value().as_bytes(),
        ));
    }
    let result = output.map_or(Cow::Borrowed(payload), Cow::Owned);
    reject_unresolved_secret_placeholders(result.as_ref())?;
    Ok(result)
}

pub(super) fn substitute_body_bytes_for_host<'b>(
    secrets: &RuntimeSecrets,
    host: &str,
    payload: &'b [u8],
) -> Cow<'b, [u8]> {
    let authority = Authority::new(host);
    let mut output = None;
    for binding in secrets.iter() {
        if !binding.allows_authority(&authority) {
            continue;
        }
        let current = output.as_deref().unwrap_or(payload);
        if !current
            .windows(binding.placeholder.len())
            .any(|window| window == binding.placeholder.as_bytes())
        {
            continue;
        }
        output = Some(replace_bytes(
            current,
            binding.placeholder.as_bytes(),
            binding.value().as_bytes(),
        ));
    }
    output.map_or(Cow::Borrowed(payload), Cow::Owned)
}

pub(super) fn substitute_http_header_bytes_for_host<'b>(
    secrets: &RuntimeSecrets,
    host: &str,
    headers: &'b [u8],
    scratch: &mut HeaderRewriteScratch,
) -> Result<Cow<'b, [u8]>, Error> {
    let raw = substitute_bytes_for_host(secrets, host, headers)?;
    let current = raw.as_ref();
    let Some(decoded_basic) = substitute_basic_auth_headers_for_host(secrets, host, current, scratch)? else {
        return Ok(match raw {
            Cow::Borrowed(_) => Cow::Borrowed(headers),
            Cow::Owned(headers) => Cow::Owned(headers),
        });
    };
    Ok(Cow::Owned(decoded_basic))
}

fn substitute_basic_auth_headers_for_host(
    secrets: &RuntimeSecrets,
    host: &str,
    headers: &[u8],
    scratch: &mut HeaderRewriteScratch,
) -> Result<Option<Vec<u8>>, Error> {
    let mut output = None;
    let mut start = 0;
    while start < headers.len() {
        let end = headers[start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(headers.len(), |index| start + index + 1);
        let line = &headers[start..end];
        if let Some(substituted) = substitute_basic_auth_header_line_for_host(secrets, host, line, scratch)? {
            output
                .get_or_insert_with(|| {
                    let mut output = Vec::with_capacity(headers.len());
                    output.extend_from_slice(&headers[..start]);
                    output
                })
                .extend_from_slice(&substituted);
        } else if let Some(output) = &mut output {
            output.extend_from_slice(line);
        }
        start = end;
    }
    Ok(output)
}

fn substitute_basic_auth_header_line_for_host(
    secrets: &RuntimeSecrets,
    host: &str,
    line: &[u8],
    scratch: &mut HeaderRewriteScratch,
) -> Result<Option<Vec<u8>>, Error> {
    let content_len = if line.ends_with(b"\r\n") {
        line.len() - b"\r\n".len()
    } else if line.ends_with(b"\n") {
        line.len() - b"\n".len()
    } else {
        line.len()
    };
    let (content, ending) = line.split_at(content_len);
    let Some(colon) = content.iter().position(|byte| *byte == b':') else {
        return Ok(None);
    };
    if !content[..colon].eq_ignore_ascii_case(b"authorization") {
        return Ok(None);
    }

    let mut value_start = colon + 1;
    while content
        .get(value_start)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        value_start += 1;
    }
    let value = &content[value_start..];
    if value.len() < b"Basic ".len() || !value[..b"Basic ".len()].eq_ignore_ascii_case(b"Basic ") {
        return Ok(None);
    }
    let token_start = value_start + b"Basic ".len();
    let token_end = trim_ascii_whitespace_end(content, token_start);
    let token = &content[token_start..token_end];
    let Some(decoded_len) = agentdp_base64::decoded_len(token) else {
        return Ok(None);
    };
    scratch.decoded.resize(decoded_len, 0);
    let Some(decoded_len) = agentdp_base64::decode(token, scratch.decoded.as_mut_slice()) else {
        return Ok(None);
    };
    let decoded = &scratch.decoded.as_slice()[..decoded_len];

    let substituted = substitute_bytes_for_host(secrets, host, decoded)?;
    if matches!(substituted, Cow::Borrowed(_)) {
        return Ok(None);
    }

    let encoded_len = agentdp_base64::encoded_len(substituted.len());
    scratch.encoded.resize(encoded_len, 0);
    let Some(encoded_len) = agentdp_base64::encode(substituted.as_ref(), scratch.encoded.as_mut_slice()) else {
        unreachable!("base64 output was pre-sized")
    };
    let mut output =
        Vec::with_capacity(content.len() + encoded_len.saturating_sub(token_end - token_start) + ending.len());
    output.extend_from_slice(&content[..token_start]);
    output.extend_from_slice(&scratch.encoded.as_slice()[..encoded_len]);
    output.extend_from_slice(&content[token_end..]);
    output.extend_from_slice(ending);
    Ok(Some(output))
}

pub(super) fn ensure_no_placeholders_for_disallowed_host(
    secrets: &RuntimeSecrets,
    host: &str,
    payload: &[u8],
) -> Result<(), Error> {
    let authority = Authority::new(host);
    for binding in secrets.iter() {
        if !binding.allows_authority(&authority)
            && payload
                .windows(binding.placeholder.len())
                .any(|window| window == binding.placeholder.as_bytes())
        {
            return Err(Error::UnauthorizedSecretHost(host.to_owned()));
        }
    }
    Ok(())
}

pub(crate) fn reject_unresolved_secret_placeholders(payload: &[u8]) -> Result<(), Error> {
    if find_bytes(payload, SECRET_PLACEHOLDER_PREFIX).is_some() {
        return Err(Error::UnresolvedSecretPlaceholder);
    }
    Ok(())
}

fn replace_bytes(haystack: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(haystack.len());
    let mut remaining = haystack;
    while let Some(index) = find_bytes(remaining, needle) {
        output.extend_from_slice(&remaining[..index]);
        output.extend_from_slice(replacement);
        remaining = &remaining[index + needle.len()..];
    }
    output.extend_from_slice(remaining);
    output
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack.windows(needle.len()).position(|window| window == needle)
}

fn trim_ascii_whitespace_end(input: &[u8], start: usize) -> usize {
    let mut end = input.len();
    while end > start && matches!(input[end - 1], b' ' | b'\t') {
        end -= 1;
    }
    end
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use proptest::prelude::*;

    use crate::buffers::BufferPool;
    use crate::{Error, RuntimeSecret, RuntimeSecrets};

    use super::{
        HeaderRewriteScratch, process, reject_unresolved_secret_placeholders, substitute_bytes_for_host,
        substitute_http_header_bytes_for_host,
    };

    fn test_buffers() -> BufferPool {
        BufferPool::default()
    }

    #[test]
    fn process_rejects_unresolved_secret_placeholders() {
        let mut output = Vec::new();

        let error = process(b"token=AGENTDP_SECRET_TOKEN", &mut output);

        assert!(error.is_err());
        assert!(output.is_empty());
    }

    #[test]
    fn substitutes_authorized_secret_values() -> Result<(), Error> {
        let secrets = secrets();

        let substituted = substitute_bytes_for_host(&secrets, "Allowed.TEST.", b"Bearer AGENTDP_SECRET_TOKEN")?;

        assert_eq!(substituted.as_ref(), b"Bearer substituted-token");
        assert!(matches!(substituted, Cow::Owned(_)));
        Ok(())
    }

    #[test]
    fn rejects_secret_for_unauthorized_host() {
        let secrets = secrets();

        let error = substitute_bytes_for_host(&secrets, "blocked.test", b"Bearer AGENTDP_SECRET_TOKEN");

        assert_eq!(error, Err(Error::UnauthorizedSecretHost("blocked.test".to_owned())));
    }

    #[test]
    fn substitutes_basic_authorization_payload() -> Result<(), Error> {
        let secrets = secrets();
        let buffers = test_buffers();
        let mut scratch = HeaderRewriteScratch::new(&buffers);
        let token = encode_base64(b"user:AGENTDP_SECRET_TOKEN");
        let headers = format!("GET / HTTP/1.1\r\nAuthorization: Basic {token} \t\r\n\r\n");

        let substituted =
            substitute_http_header_bytes_for_host(&secrets, "allowed.test", headers.as_bytes(), &mut scratch)?;
        let text = String::from_utf8_lossy(substituted.as_ref());

        assert!(!text.contains("AGENTDP_SECRET_TOKEN"));
        assert!(text.contains(&encode_base64(b"user:substituted-token")));
        Ok(())
    }

    #[test]
    fn borrowed_when_no_secret_matches() -> Result<(), Error> {
        let secrets = secrets();

        let substituted = substitute_bytes_for_host(&secrets, "allowed.test", b"plain payload")?;

        assert!(matches!(substituted, Cow::Borrowed(_)));
        assert_eq!(substituted.as_ref(), b"plain payload");
        Ok(())
    }

    proptest! {
        #[test]
        fn payload_without_secret_prefix_is_accepted(payload in proptest::collection::vec(0_u8..=127, 0..256)) {
            prop_assume!(payload.windows(b"AGENTDP_SECRET_".len()).all(|window| window != b"AGENTDP_SECRET_"));
            prop_assert_eq!(reject_unresolved_secret_placeholders(&payload), Ok(()));
        }
    }

    fn secrets() -> RuntimeSecrets {
        let mut secrets = RuntimeSecrets::new();
        secrets.insert(RuntimeSecret::new(
            "AGENTDP_SECRET_TOKEN",
            "substituted-token",
            ["allowed.test".to_owned()],
        ));
        secrets
    }

    fn encode_base64(input: &[u8]) -> String {
        let mut output = vec![0u8; agentdp_base64::encoded_len(input.len())];
        let Some(written) = agentdp_base64::encode(input, &mut output) else {
            unreachable!("base64 output was pre-sized")
        };
        output.truncate(written);
        String::from_utf8(output).unwrap_or_default()
    }
}
