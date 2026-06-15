mod egress;
mod http;
mod link_trace;
mod progress;
mod quiescence;
mod secrets;
mod telemetry;
mod transcript;

use super::report::CheckerResult;
use super::{Error, Result, ScenarioReport, TranscriptRole};

pub(super) use self::egress::{ExpectedEgressError, NoUnexpectedEgressErrors};
pub(super) use self::http::HttpResponseBodyEquals;
pub(super) use self::link_trace::{LinkTraceContains, LinkTracePrecedes};
pub(super) use self::progress::ProgressComplete;
pub(super) use self::quiescence::Quiescent;
pub(super) use self::secrets::NoSecretLeak;
pub(super) use self::telemetry::TelemetryEquals;
pub(super) use self::transcript::{TranscriptContains, TranscriptEquals};

pub(super) trait Checker {
    fn name(&self) -> &'static str;
    fn check(&self, report: &ScenarioReport) -> Result<()>;
}

pub(super) fn check_all(report: &mut ScenarioReport, checkers: Vec<Box<dyn Checker>>) -> Result<()> {
    for checker in checkers {
        let name = checker.name();
        if let Err(error) = checker.check(report) {
            let failure = error.to_string();
            report.record_checker(CheckerResult::failed(name, failure.clone()));
            return Err(report.error(format!("failed checker {name:?}: {failure}")));
        }
        report.record_checker(CheckerResult::passed(name));
    }
    Ok(())
}

fn required_transcript<'a>(report: &'a ScenarioReport, role: TranscriptRole, name: &str) -> Result<&'a [u8]> {
    report.transcript(role, name).ok_or_else(|| {
        Error::new(format!(
            "missing {:?} transcript {:?}; available={:?}",
            role,
            name,
            report
                .transcripts
                .iter()
                .map(|transcript| (transcript.role, transcript.name))
                .collect::<Vec<_>>()
        ))
    })
}

fn contains(actual: &[u8], expected: &[u8]) -> bool {
    actual.windows(expected.len()).any(|window| window == expected)
}
