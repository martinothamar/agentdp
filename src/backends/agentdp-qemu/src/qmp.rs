use std::path::Path;
use std::time::Duration;

use agentdp_platform as platform;
use agentdp_protocol::Error as ProtocolError;
use agentdp_protocol::jsonl::{self, JsonLineReader, ReadJsonLine};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncRead;

const QMP_MAX_LINE_BYTES: usize = 1024 * 1024;
const QMP_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Executes one QMP command against a QEMU QMP Unix-domain socket.
///
/// # Errors
///
/// Returns an error when the socket cannot be opened, QMP negotiation fails, or
/// QEMU reports an error response for the command.
pub async fn execute(socket: &Path, command: &str) -> std::io::Result<()> {
    let socket = platform::socket::connect_local_socket(socket)
        .await
        .map_err(|error| match error {
            platform::socket::LocalSocketError::Unsupported => {
                std::io::Error::new(std::io::ErrorKind::Unsupported, error)
            }
            platform::socket::LocalSocketError::Io(error) => error,
        })?;
    let (mut stream, mut writer) = socket.split();
    let mut reader = JsonLineReader::new(QMP_MAX_LINE_BYTES);
    let mut frame = Vec::new();
    read_greeting(&mut reader, &mut stream, &mut frame).await?;
    execute_raw(&mut reader, &mut stream, &mut writer, &mut frame, "qmp_capabilities").await?;
    execute_raw(&mut reader, &mut stream, &mut writer, &mut frame, command).await
}

async fn read_greeting<R>(reader: &mut JsonLineReader, stream: &mut R, frame: &mut Vec<u8>) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
{
    read_qmp_with_timeout::<QmpGreeting, _>(reader, stream, frame)
        .await?
        .map_or_else(
            || {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "QMP socket closed before greeting",
                ))
            },
            |_greeting| Ok(()),
        )
}

async fn execute_raw<R>(
    reader: &mut JsonLineReader,
    stream: &mut R,
    writer: &mut platform::socket::AsyncLocalSocketWriter,
    frame: &mut Vec<u8>,
    command: &str,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
{
    jsonl::encode_into(&QmpCommand { execute: command }, frame).map_err(protocol_io_error)?;
    writer.write_all(frame).await?;
    writer.flush().await?;
    read_command_response(reader, stream, frame).await
}

async fn read_command_response<R>(
    reader: &mut JsonLineReader,
    stream: &mut R,
    frame: &mut Vec<u8>,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
{
    loop {
        let Some(message) = read_qmp_with_timeout::<QmpResponse, _>(reader, stream, frame).await? else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "QMP socket closed before command response",
            ));
        };
        if let Some(error) = message.error {
            return Err(std::io::Error::other(format!("QMP returned error: {error}")));
        }
        if message.return_value.is_some() {
            return Ok(());
        }
    }
}

async fn read_qmp_with_timeout<T, R>(
    reader: &mut JsonLineReader,
    stream: &mut R,
    frame: &mut Vec<u8>,
) -> std::io::Result<Option<T>>
where
    T: serde::de::DeserializeOwned,
    R: AsyncRead + Unpin,
{
    tokio::time::timeout(QMP_READ_TIMEOUT, read_qmp(reader, stream, frame))
        .await
        .map_err(|_elapsed| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("QMP response was not received within {}s", QMP_READ_TIMEOUT.as_secs()),
            )
        })?
}

async fn read_qmp<T, R>(reader: &mut JsonLineReader, stream: &mut R, frame: &mut Vec<u8>) -> std::io::Result<Option<T>>
where
    T: serde::de::DeserializeOwned,
    R: AsyncRead + Unpin,
{
    match jsonl::read::<T, _>(reader, stream, frame)
        .await
        .map_err(protocol_io_error)?
    {
        ReadJsonLine::Value(message) => Ok(Some(message)),
        ReadJsonLine::Eof => Ok(None),
    }
}

fn protocol_io_error(error: ProtocolError) -> std::io::Error {
    match error {
        ProtocolError::Read(source) => source,
        source => std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    }
}

#[derive(Debug, Deserialize)]
struct QmpGreeting {
    #[serde(rename = "QMP")]
    _qmp: Value,
}

#[derive(Debug, Serialize)]
struct QmpCommand<'a> {
    execute: &'a str,
}

#[derive(Debug, Deserialize)]
struct QmpResponse {
    #[serde(rename = "return")]
    return_value: Option<Value>,
    error: Option<QmpError>,
    #[serde(default)]
    _event: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QmpError {
    #[serde(rename = "class")]
    error_class: Option<String>,
    desc: Option<String>,
}

impl std::fmt::Display for QmpError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.error_class, &self.desc) {
            (Some(error_class), Some(desc)) => write!(formatter, "{error_class}: {desc}"),
            (Some(error_class), None) => formatter.write_str(error_class),
            (None, Some(desc)) => formatter.write_str(desc),
            (None, None) => formatter.write_str("unknown QMP error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{QMP_MAX_LINE_BYTES, QmpCommand, read_command_response, read_greeting};
    use agentdp_protocol::jsonl::{self, JsonLineReader};

    #[test]
    fn qmp_command_encodes_execute_field() {
        let line = jsonl::encode(&QmpCommand { execute: "quit" }).expect("encode command");

        assert_eq!(
            line,
            br#"{"execute":"quit"}"#.to_vec().into_iter().chain([b'\n']).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn qmp_greeting_requires_qmp_field() {
        let mut source: &[u8] = br#"{"QMP":{"version":{"qemu":{"major":9}}}}"#;
        let mut reader = JsonLineReader::new(QMP_MAX_LINE_BYTES);
        let mut frame = Vec::new();

        read_greeting(&mut reader, &mut source, &mut frame)
            .await
            .expect("read greeting");

        let mut source: &[u8] = br#"{"event":"RESET"}"#;
        let mut reader = JsonLineReader::new(QMP_MAX_LINE_BYTES);
        let mut frame = Vec::new();

        read_greeting(&mut reader, &mut source, &mut frame)
            .await
            .expect_err("missing QMP field must fail");
    }

    #[tokio::test]
    async fn qmp_command_response_skips_events_until_return() {
        let mut source: &[u8] = br#"{"event":"STOP"}
{"event":"RESUME"}
{"event":"RESET"}
{"event":"STOP"}
{"event":"RESUME"}
{"event":"RESET"}
{"event":"STOP"}
{"event":"RESUME"}
{"event":"RESET"}
{"return":{}}
"#;
        let mut reader = JsonLineReader::new(QMP_MAX_LINE_BYTES);
        let mut frame = Vec::new();

        read_command_response(&mut reader, &mut source, &mut frame)
            .await
            .expect("read command response");
    }

    #[tokio::test]
    async fn qmp_command_response_reports_errors() {
        let mut source: &[u8] = br#"{"error":{"class":"CommandNotFound","desc":"unknown command"}}"#;
        let mut reader = JsonLineReader::new(QMP_MAX_LINE_BYTES);
        let mut frame = Vec::new();

        let error = read_command_response(&mut reader, &mut source, &mut frame)
            .await
            .expect_err("QMP error response must fail");

        assert!(error.to_string().contains("CommandNotFound: unknown command"));
    }

    #[tokio::test]
    async fn qmp_command_response_rejects_eof_before_response() {
        let mut source: &[u8] = br#"{"event":"STOP"}
"#;
        let mut reader = JsonLineReader::new(QMP_MAX_LINE_BYTES);
        let mut frame = Vec::new();

        let error = read_command_response(&mut reader, &mut source, &mut frame)
            .await
            .expect_err("EOF before return must fail");

        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn qmp_command_response_times_out_without_response() {
        let (_writer, mut stream) = tokio::io::duplex(128);
        let mut reader = JsonLineReader::new(QMP_MAX_LINE_BYTES);
        let mut frame = Vec::new();

        let error = read_command_response(&mut reader, &mut stream, &mut frame)
            .await
            .expect_err("missing response must time out");

        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }
}
