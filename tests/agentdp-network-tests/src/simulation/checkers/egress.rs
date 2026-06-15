use super::super::{Error, Result, ScenarioReport};
use super::Checker;

pub(in super::super) struct NoUnexpectedEgressErrors;

impl Checker for NoUnexpectedEgressErrors {
    fn name(&self) -> &'static str {
        "no unexpected egress errors"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        let egress_errors = report.final_status.telemetry.egress_errors;
        if egress_errors == 0 {
            Ok(())
        } else {
            Err(Error::new(format!("expected no egress errors, got {egress_errors}")))
        }
    }
}

pub(in super::super) struct ExpectedEgressError;

impl Checker for ExpectedEgressError {
    fn name(&self) -> &'static str {
        "expected egress error"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        let egress_errors = report.final_status.telemetry.egress_errors;
        if egress_errors > 0 {
            Ok(())
        } else {
            Err(Error::new("expected at least one egress error"))
        }
    }
}
