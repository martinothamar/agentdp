use serde::Serialize;

use crate::{Error, Request, ServerMessage};

/// Encodes a protocol value as one JSON object followed by a newline.
///
/// # Errors
///
/// Returns an error when the value cannot be serialized.
pub fn encode_line(value: &impl Serialize) -> Result<String, Error> {
    let mut line = serde_json::to_string(value).map_err(Error::Encode)?;
    line.push('\n');
    Ok(line)
}

/// Decodes a JSONL request line.
///
/// # Errors
///
/// Returns an error when the line is not a valid request object.
pub fn decode_request(line: &str) -> Result<Request, Error> {
    serde_json::from_str(line).map_err(Error::Decode)
}

/// Decodes a JSONL server message line.
///
/// # Errors
///
/// Returns an error when the line is not a valid server message object.
pub fn decode_server_message(line: &str) -> Result<ServerMessage, Error> {
    serde_json::from_str(line).map_err(Error::Decode)
}
