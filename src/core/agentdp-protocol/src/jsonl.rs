use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::Error;

pub const DEFAULT_MAX_LINE_BYTES: usize = usize::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadJsonLine<T> {
    Value(T),
    Eof,
}

#[derive(Debug)]
pub struct JsonLineReader {
    pending: Vec<u8>,
    max_line_bytes: usize,
}

impl Default for JsonLineReader {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_LINE_BYTES)
    }
}

impl JsonLineReader {
    #[must_use]
    pub const fn new(max_line_bytes: usize) -> Self {
        Self {
            pending: Vec::new(),
            max_line_bytes,
        }
    }

    #[must_use]
    pub const fn buffered_len(&self) -> usize {
        self.pending.len()
    }

    /// Reads one newline-terminated frame, preserving partial data across calls.
    ///
    /// # Errors
    ///
    /// Returns an error when the source read fails or the line exceeds the
    /// configured maximum frame size.
    pub async fn read_line<R>(&mut self, reader: &mut R, line: &mut Vec<u8>) -> Result<bool, Error>
    where
        R: AsyncRead + Unpin,
    {
        line.clear();
        self.validate_next_frame_size()?;
        if self.take_line(line) {
            return Ok(true);
        }

        let mut buffer = [0_u8; 8192];
        loop {
            let read = reader.read(&mut buffer).await.map_err(Error::Read)?;
            if read == 0 {
                self.validate_next_frame_size()?;
                return Ok(self.take_eof(line));
            }
            self.extend(&buffer[..read]);
            self.validate_next_frame_size()?;
            if self.take_line(line) {
                return Ok(true);
            }
        }
    }

    fn extend(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
    }

    fn validate_next_frame_size(&self) -> Result<(), Error> {
        let frame_len = self
            .pending
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(self.pending.len(), |end| end + 1);
        if frame_len > self.max_line_bytes {
            return Err(Error::Frame(format!(
                "JSONL frame exceeded {} bytes",
                self.max_line_bytes
            )));
        }
        Ok(())
    }

    fn take_line(&mut self, line: &mut Vec<u8>) -> bool {
        let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') else {
            return false;
        };
        line.extend(self.pending.drain(..=end));
        true
    }

    fn take_eof(&mut self, line: &mut Vec<u8>) -> bool {
        if self.pending.is_empty() {
            false
        } else {
            line.append(&mut self.pending);
            true
        }
    }
}

/// Encodes a protocol message as compact JSON followed by a newline.
///
/// # Errors
///
/// Returns an error when the value cannot be serialized.
pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, Error> {
    let mut line = Vec::new();
    encode_into(value, &mut line)?;
    Ok(line)
}

/// Encodes a protocol message into a caller-owned buffer.
///
/// # Errors
///
/// Returns an error when the value cannot be serialized.
pub fn encode_into<T: Serialize + ?Sized>(value: &T, line: &mut Vec<u8>) -> Result<(), Error> {
    line.clear();
    serde_json::to_writer(&mut *line, value).map_err(Error::Encode)?;
    line.push(b'\n');
    Ok(())
}

/// Decodes one JSONL frame into a protocol message.
///
/// # Errors
///
/// Returns an error when the line is not valid JSON for `T`.
pub fn decode<T: DeserializeOwned>(line: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(line).map_err(Error::Decode)
}

/// Reads and decodes one JSONL frame.
///
/// # Errors
///
/// Returns an error when reading fails or the frame cannot be decoded.
pub async fn read<T, R>(
    reader: &mut JsonLineReader,
    stream: &mut R,
    line: &mut Vec<u8>,
) -> Result<ReadJsonLine<T>, Error>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin,
{
    if reader.read_line(stream, line).await? {
        decode(line).map(ReadJsonLine::Value)
    } else {
        Ok(ReadJsonLine::Eof)
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use proptest::prelude::*;
    use serde::{Deserialize, Serialize};
    use tokio::io::AsyncWriteExt as _;
    use tokio::io::ReadBuf;

    use super::{JsonLineReader, ReadJsonLine, decode, encode, encode_into, read};
    use crate::Error;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Message {
        id: String,
        body: String,
    }

    #[test]
    fn encode_appends_newline() {
        let message = Message {
            id: "msg_0".to_owned(),
            body: "hello".to_owned(),
        };

        let line = encode(&message).expect("encode line");

        assert!(line.ends_with(b"\n"));
        assert_eq!(decode::<Message>(&line).expect("decode line"), message);
    }

    #[test]
    fn encode_into_reuses_buffer_capacity() {
        let message = Message {
            id: "msg_0".to_owned(),
            body: "hello".to_owned(),
        };
        let mut line = Vec::with_capacity(256);
        let capacity = line.capacity();

        encode_into(&message, &mut line).expect("encode line");

        assert!(line.ends_with(b"\n"));
        assert_eq!(line.capacity(), capacity);
        assert_eq!(decode::<Message>(&line).expect("decode line"), message);
    }

    #[tokio::test]
    async fn reader_returns_multiple_buffered_lines() {
        let mut source: &[u8] = b"{\"id\":\"one\",\"body\":\"a\"}\n{\"id\":\"two\",\"body\":\"b\"}\n";
        let mut reader = JsonLineReader::default();

        let mut line = Vec::new();

        assert!(reader.read_line(&mut source, &mut line).await.expect("read first line"));
        assert_eq!(decode::<Message>(&line).expect("decode first").id, "one");
        assert!(reader.buffered_len() > 0);

        assert!(
            reader
                .read_line(&mut source, &mut line)
                .await
                .expect("read second line")
        );
        assert_eq!(decode::<Message>(&line).expect("decode second").id, "two");
    }

    #[tokio::test]
    async fn cancelled_read_preserves_partial_frame() {
        let (mut writer, mut stream) = tokio::io::duplex(128);
        let mut reader = JsonLineReader::default();
        let mut line = Vec::new();
        writer.write_all(br#"{"id":"msg_0""#).await.expect("write partial");

        let timed_out = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            read::<Message, _>(&mut reader, &mut stream, &mut line),
        )
        .await;

        assert!(timed_out.is_err());
        writer.write_all(br#","body":"hello"}"#).await.expect("write rest");
        writer.write_all(b"\n").await.expect("write newline");
        let message = read::<Message, _>(&mut reader, &mut stream, &mut line)
            .await
            .expect("read complete");

        assert_eq!(
            message,
            ReadJsonLine::Value(Message {
                id: "msg_0".to_owned(),
                body: "hello".to_owned(),
            })
        );
    }

    #[tokio::test]
    async fn default_reader_accepts_large_frames() {
        let message = Message {
            id: "msg_0".to_owned(),
            body: "x".repeat(1024 * 1024 + 1),
        };
        let encoded = encode(&message).expect("encode message");
        let mut source = encoded.as_slice();
        let mut reader = JsonLineReader::default();
        let mut line = Vec::new();

        assert!(reader.read_line(&mut source, &mut line).await.expect("read line"));

        assert_eq!(decode::<Message>(&line).expect("decode line"), message);
    }

    #[tokio::test]
    async fn eof_returns_pending_frame_once() {
        let mut source: &[u8] = br#"{"id":"msg_0","body":"hello"}"#;
        let mut reader = JsonLineReader::default();
        let mut line = Vec::new();

        let read = reader.read_line(&mut source, &mut line).await.expect("read line");

        assert!(read);
        assert_eq!(decode::<Message>(&line).expect("decode line").id, "msg_0");
        assert!(!reader.read_line(&mut source, &mut line).await.expect("read eof"));
    }

    #[tokio::test]
    async fn rejects_oversized_frames() {
        let mut source: &[u8] = b"abcd\n";
        let mut reader = JsonLineReader::new(3);
        let mut line = Vec::new();

        let error = reader
            .read_line(&mut source, &mut line)
            .await
            .expect_err("oversized line must fail");

        assert!(matches!(error, Error::Frame(_)));
    }

    #[tokio::test]
    async fn limit_applies_to_next_frame_not_buffered_read_ahead() {
        let mut source: &[u8] = b"123456789\nx\n";
        let mut reader = JsonLineReader::new(10);
        let mut line = Vec::new();

        assert!(reader.read_line(&mut source, &mut line).await.expect("read first line"));
        assert_eq!(line, b"123456789\n");
        assert!(
            reader
                .read_line(&mut source, &mut line)
                .await
                .expect("read second line")
        );
        assert_eq!(line, b"x\n");
    }

    #[tokio::test]
    async fn rejects_oversized_frame_already_buffered_after_valid_frame() {
        let mut source: &[u8] = b"a\n1234\n";
        let mut reader = JsonLineReader::new(3);
        let mut line = Vec::new();

        assert!(reader.read_line(&mut source, &mut line).await.expect("read first line"));
        assert_eq!(line, b"a\n");
        let error = reader
            .read_line(&mut source, &mut line)
            .await
            .expect_err("oversized buffered line must fail");

        assert!(matches!(error, Error::Frame(_)));
    }

    #[test]
    fn rejects_invalid_json() {
        let error = decode::<Message>(br#"{"id":"msg_0","body":"#).expect_err("invalid json must fail");

        assert!(matches!(error, Error::Decode(_)));
    }

    proptest! {
        #[test]
        fn chunk_boundaries_preserve_lines(lines in line_bodies(), chunk_sizes in chunk_sizes()) {
            let input = lines_to_input(&lines, true);
            let expected = lines
                .into_iter()
                .map(|mut line| {
                    line.push(b'\n');
                    line
                })
                .collect::<Vec<_>>();

            let actual = block_on(read_all_lines(JsonLineReader::default(), chunked(&input, &chunk_sizes)))
                .expect("read lines");

            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn eof_returns_final_unterminated_frame_once(
            lines in line_bodies(),
            tail in line_body(),
            chunk_sizes in chunk_sizes(),
        ) {
            let mut input = lines_to_input(&lines, true);
            input.extend_from_slice(&tail);
            let mut expected = lines
                .into_iter()
                .map(|mut line| {
                    line.push(b'\n');
                    line
                })
                .collect::<Vec<_>>();
            if !tail.is_empty() {
                expected.push(tail);
            }

            let actual = block_on(read_all_lines(JsonLineReader::default(), chunked(&input, &chunk_sizes)))
                .expect("read lines");

            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn max_frame_size_depends_on_next_frame_not_read_ahead(
            mut first in line_body_with_len(0, 15),
            mut second in line_body_with_len(0, 15),
            chunk_sizes in chunk_sizes(),
        ) {
            first.push(b'\n');
            second.push(b'\n');
            let limit = first.len().max(second.len());
            let input = [first.clone(), second.clone()].concat();

            let actual = block_on(read_all_lines(JsonLineReader::new(limit), chunked(&input, &chunk_sizes)))
                .expect("read lines");

            prop_assert_eq!(actual, vec![first, second]);
        }

        #[test]
        fn oversized_frame_rejects_independent_of_read_boundaries(
            valid in line_body_with_len(0, 8),
            oversized in line_body_with_len(8, 16),
            chunk_sizes in chunk_sizes(),
        ) {
            let limit = 8;
            let mut valid_line = valid;
            valid_line.push(b'\n');
            let mut oversized_line = oversized;
            oversized_line.push(b'\n');
            prop_assume!(oversized_line.len() > limit);
            let input = [valid_line.clone(), oversized_line].concat();

            let error = block_on(read_all_lines(JsonLineReader::new(limit), chunked(&input, &chunk_sizes)))
                .expect_err("oversized line must fail");

            prop_assert!(matches!(error, Error::Frame(_)));
        }

        #[test]
        fn arbitrary_bytes_do_not_panic(input in prop::collection::vec(any::<u8>(), 0..512), chunk_sizes in chunk_sizes()) {
            let _result = block_on(read_all_lines(JsonLineReader::new(128), chunked(&input, &chunk_sizes)));
        }
    }

    fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("build test runtime")
            .block_on(future)
    }

    async fn read_all_lines(mut reader: JsonLineReader, mut source: ChunkedRead) -> Result<Vec<Vec<u8>>, Error> {
        let mut lines = Vec::new();
        let mut line = Vec::new();
        while reader.read_line(&mut source, &mut line).await? {
            lines.push(line.clone());
        }
        Ok(lines)
    }

    #[derive(Debug)]
    struct ChunkedRead {
        chunks: Vec<Vec<u8>>,
        index: usize,
    }

    impl tokio::io::AsyncRead for ChunkedRead {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let Some(chunk) = self.chunks.get(self.index) else {
                return Poll::Ready(Ok(()));
            };
            buffer.put_slice(chunk);
            self.index += 1;
            Poll::Ready(Ok(()))
        }
    }

    fn chunked(input: &[u8], chunk_sizes: &[usize]) -> ChunkedRead {
        let mut chunks = Vec::new();
        let mut offset = 0;
        for size in chunk_sizes.iter().copied().cycle() {
            if offset >= input.len() {
                break;
            }
            let end = (offset + size).min(input.len());
            chunks.push(input[offset..end].to_vec());
            offset = end;
        }
        ChunkedRead { chunks, index: 0 }
    }

    fn lines_to_input(lines: &[Vec<u8>], terminate_all: bool) -> Vec<u8> {
        let mut input = Vec::new();
        for line in lines {
            input.extend_from_slice(line);
            if terminate_all {
                input.push(b'\n');
            }
        }
        input
    }

    fn line_bodies() -> impl Strategy<Value = Vec<Vec<u8>>> {
        prop::collection::vec(line_body(), 0..20)
    }

    fn line_body() -> impl Strategy<Value = Vec<u8>> {
        line_body_with_len(0, 64)
    }

    fn line_body_with_len(min: usize, max: usize) -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(non_newline_byte(), min..=max)
    }

    fn non_newline_byte() -> impl Strategy<Value = u8> {
        prop_oneof![0_u8..10, 11_u8..=u8::MAX]
    }

    fn chunk_sizes() -> impl Strategy<Value = Vec<usize>> {
        prop::collection::vec(1_usize..32, 1..64)
    }
}
