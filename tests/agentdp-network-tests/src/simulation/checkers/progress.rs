use super::super::{Error, Result, ScenarioReport};
use super::Checker;

pub(in super::super) struct ProgressComplete;

impl Checker for ProgressComplete {
    fn name(&self) -> &'static str {
        "progress_complete"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        for observation in &report.progress {
            if !observation.complete {
                return Err(Error::new(format!(
                    "phase {:?} incomplete; observed_bytes={} expected_bytes={}",
                    observation.phase, observation.observed_bytes, observation.expected_bytes
                )));
            }
            if observation.observed_bytes != observation.expected_bytes {
                return Err(Error::new(format!(
                    "phase {:?} byte mismatch; observed_bytes={} expected_bytes={}",
                    observation.phase, observation.observed_bytes, observation.expected_bytes
                )));
            }
        }
        Ok(())
    }
}
