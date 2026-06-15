use super::super::{Error, Result, ScenarioReport};
use super::Checker;

#[derive(Default)]
pub(in super::super) struct TelemetryEquals {
    guest_frames_received: Option<u64>,
    host_frames_sent: Option<u64>,
}

impl TelemetryEquals {
    #[must_use]
    pub(in super::super) const fn new() -> Self {
        Self {
            guest_frames_received: None,
            host_frames_sent: None,
        }
    }

    #[must_use]
    pub(in super::super) const fn guest_frames_received(mut self, expected: u64) -> Self {
        self.guest_frames_received = Some(expected);
        self
    }

    #[must_use]
    pub(in super::super) const fn host_frames_sent(mut self, expected: u64) -> Self {
        self.host_frames_sent = Some(expected);
        self
    }
}

impl Checker for TelemetryEquals {
    fn name(&self) -> &'static str {
        "telemetry equals"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        if let Some(expected) = self.guest_frames_received {
            let actual = report.final_status.telemetry.guest_frames_received;
            if actual != expected {
                return Err(Error::new(format!(
                    "guest frames received: expected {expected}, got {actual}"
                )));
            }
        }
        if let Some(expected) = self.host_frames_sent {
            let actual = report.final_status.telemetry.host_frames_sent;
            if actual != expected {
                return Err(Error::new(format!(
                    "host frames sent: expected {expected}, got {actual}"
                )));
            }
        }
        Ok(())
    }
}
