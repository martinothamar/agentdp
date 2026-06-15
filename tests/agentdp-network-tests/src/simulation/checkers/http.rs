use super::super::protocol::http1_model::http_response_body_for_request;
use super::super::{Error, Result, ScenarioReport, TranscriptRole};
use super::{Checker, required_transcript};

pub(in super::super) struct HttpResponseBodyEquals {
    role: TranscriptRole,
    transcript: &'static str,
    request: Vec<u8>,
    expected: Vec<u8>,
}

impl HttpResponseBodyEquals {
    #[must_use]
    pub(in super::super) fn guest_for_request(
        transcript: &'static str,
        request: impl Into<Vec<u8>>,
        expected: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            role: TranscriptRole::Guest,
            transcript,
            request: request.into(),
            expected: expected.into(),
        }
    }
}

impl Checker for HttpResponseBodyEquals {
    fn name(&self) -> &'static str {
        "http response body equals"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        let actual = required_transcript(report, self.role, self.transcript)?;
        let Some(body) = http_response_body_for_request(&self.request, actual) else {
            return Err(Error::new(format!(
                "{:?} transcript {:?}: expected complete HTTP response body, got {}",
                self.role,
                self.transcript,
                bytes_summary(actual)
            )));
        };
        if body == self.expected {
            Ok(())
        } else {
            Err(Error::new(format!(
                "{:?} transcript {:?}: expected HTTP response body {}, got {}",
                self.role,
                self.transcript,
                bytes_summary(&self.expected),
                bytes_summary(&body)
            )))
        }
    }
}

fn bytes_summary(bytes: &[u8]) -> String {
    const PREVIEW_LEN: usize = 64;
    let preview = &bytes[..bytes.len().min(PREVIEW_LEN)];
    format!("len={} preview={preview:02x?}", bytes.len())
}
