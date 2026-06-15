use super::super::{Error, Result, ScenarioReport};
use super::Checker;

pub(in super::super) struct Quiescent;

impl Checker for Quiescent {
    fn name(&self) -> &'static str {
        "quiescent"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        if report.quiescence.is_quiescent() {
            Ok(())
        } else {
            Err(Error::new(format!("expected quiescence, got {:?}", report.quiescence)))
        }
    }
}
