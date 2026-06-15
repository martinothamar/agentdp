use super::super::{Error, Result, ScenarioReport, TranscriptRole};
use super::{Checker, contains};

pub(in super::super) struct NoSecretLeak {
    unexpected: Vec<Vec<u8>>,
}

impl NoSecretLeak {
    #[must_use]
    pub(in super::super) fn new(unexpected: impl IntoIterator<Item = impl Into<Vec<u8>>>) -> Self {
        Self {
            unexpected: unexpected.into_iter().map(Into::into).collect(),
        }
    }
}

impl Checker for NoSecretLeak {
    fn name(&self) -> &'static str {
        "no secret leak"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        for transcript in report
            .transcripts
            .iter()
            .filter(|transcript| transcript.role == TranscriptRole::Upstream)
        {
            for unexpected in &self.unexpected {
                if contains(&transcript.bytes, unexpected) {
                    return Err(Error::new(format!(
                        "upstream transcript {:?}: expected not to contain {:02x?}, got {:02x?}",
                        transcript.name, unexpected, transcript.bytes
                    )));
                }
            }
        }
        Ok(())
    }
}
