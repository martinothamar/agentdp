use super::super::{Error, Result, ScenarioReport, TranscriptRole};
use super::{Checker, contains, required_transcript};

pub(in super::super) struct TranscriptContains {
    role: TranscriptRole,
    transcript: &'static str,
    expected: Vec<u8>,
}

impl TranscriptContains {
    #[must_use]
    pub(in super::super) fn guest(transcript: &'static str, expected: impl Into<Vec<u8>>) -> Self {
        Self {
            role: TranscriptRole::Guest,
            transcript,
            expected: expected.into(),
        }
    }
}

impl Checker for TranscriptContains {
    fn name(&self) -> &'static str {
        "transcript contains"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        let actual = required_transcript(report, self.role, self.transcript)?;
        if contains(actual, &self.expected) {
            Ok(())
        } else {
            Err(Error::new(format!(
                "{:?} transcript {:?}: expected to contain {:02x?}, got {:02x?}",
                self.role, self.transcript, self.expected, actual
            )))
        }
    }
}

pub(in super::super) struct TranscriptEquals {
    role: TranscriptRole,
    transcript: &'static str,
    expected: Vec<u8>,
}

impl TranscriptEquals {
    #[must_use]
    pub(in super::super) fn guest(transcript: &'static str, expected: impl Into<Vec<u8>>) -> Self {
        Self {
            role: TranscriptRole::Guest,
            transcript,
            expected: expected.into(),
        }
    }

    #[must_use]
    pub(in super::super) fn upstream(transcript: &'static str, expected: impl Into<Vec<u8>>) -> Self {
        Self {
            role: TranscriptRole::Upstream,
            transcript,
            expected: expected.into(),
        }
    }
}

impl Checker for TranscriptEquals {
    fn name(&self) -> &'static str {
        "transcript equals"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        let actual = required_transcript(report, self.role, self.transcript)?;
        if actual == self.expected {
            Ok(())
        } else {
            Err(Error::new(format!(
                "{:?} transcript {:?}: expected {:02x?}, got {:02x?}",
                self.role, self.transcript, self.expected, actual
            )))
        }
    }
}
