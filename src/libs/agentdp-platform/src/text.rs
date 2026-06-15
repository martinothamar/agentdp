#[derive(Default)]
pub struct Utf8Stream {
    pending: Vec<u8>,
}

impl Utf8Stream {
    #[must_use]
    pub const fn new() -> Self {
        Self { pending: Vec::new() }
    }

    pub fn push(&mut self, chunk: &[u8]) -> Option<String> {
        self.pending.extend_from_slice(chunk);
        let mut output = String::new();
        let mut cursor = 0;
        while cursor < self.pending.len() {
            match std::str::from_utf8(&self.pending[cursor..]) {
                Ok(text) => {
                    output.push_str(text);
                    cursor = self.pending.len();
                    break;
                }
                Err(error) => {
                    let valid_end = cursor + error.valid_up_to();
                    if valid_end > cursor {
                        output.push_str(valid_utf8_prefix(&self.pending[cursor..valid_end]));
                    }
                    if let Some(invalid) = error.error_len() {
                        output.push('\u{fffd}');
                        cursor = valid_end + invalid;
                    } else {
                        cursor = valid_end;
                        break;
                    }
                }
            }
        }
        if cursor == self.pending.len() {
            self.pending.clear();
        } else if cursor > 0 {
            self.pending.drain(..cursor);
        }
        (!output.is_empty()).then_some(output)
    }

    pub fn finish(&mut self) -> Option<String> {
        if self.pending.is_empty() {
            return None;
        }
        Some(String::from_utf8_lossy(&std::mem::take(&mut self.pending)).into_owned())
    }
}

fn valid_utf8_prefix(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::Utf8Stream;

    #[test]
    fn utf8_stream_preserves_multibyte_characters_split_across_chunks() {
        let text = "aø🙂b";
        let chunks = [&text.as_bytes()[..2], &text.as_bytes()[2..5], &text.as_bytes()[5..]];
        let mut stream = Utf8Stream::default();
        let mut output = String::new();

        for chunk in chunks {
            if let Some(chunk) = stream.push(chunk) {
                output.push_str(&chunk);
            }
        }
        if let Some(chunk) = stream.finish() {
            output.push_str(&chunk);
        }

        assert_eq!(output, text);
    }

    #[test]
    fn utf8_stream_flushes_incomplete_trailing_bytes_lossily() {
        let mut stream = Utf8Stream::default();

        assert_eq!(stream.push(&[0xf0, 0x9f]), None);
        assert_eq!(stream.finish().as_deref(), Some("\u{fffd}"));
    }

    #[test]
    fn utf8_stream_replaces_invalid_bytes_without_losing_valid_text() {
        let mut stream = Utf8Stream::default();

        assert_eq!(
            stream.push(&[0xff, b'a', 0xff, b'b']).as_deref(),
            Some("\u{fffd}a\u{fffd}b")
        );
        assert_eq!(stream.finish(), None);
    }
}
