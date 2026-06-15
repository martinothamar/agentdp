use crate::Context;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorReport {
    pub checks: Vec<DoctorCheck>,
}

impl DoctorReport {
    #[must_use]
    pub const fn new() -> Self {
        Self { checks: Vec::new() }
    }

    #[must_use]
    pub fn has_failures(&self) -> bool {
        self.checks
            .iter()
            .any(|check| matches!(check.status, DoctorStatus::Fail))
    }

    pub fn push(&mut self, context: &Context, check: DoctorCheck) {
        context.logger().verbose_with(|| {
            format!(
                "doctor check {} -> {} ({})",
                check.name,
                check.status.label(),
                check.message
            )
        });
        self.checks.push(check);
    }
}

impl Default for DoctorReport {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorCheck {
    pub name: String,
    pub status: DoctorStatus,
    pub message: String,
}

impl DoctorCheck {
    #[must_use]
    pub fn ok(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: DoctorStatus::Ok,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn fail(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: DoctorStatus::Fail,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorStatus {
    Ok,
    Warn,
    Fail,
}

impl DoctorStatus {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DoctorCheck, DoctorReport, DoctorStatus};

    #[test]
    fn reports_no_failures_for_empty_or_passing_checks() {
        assert!(!DoctorReport::new().has_failures());
        assert!(
            !DoctorReport {
                checks: vec![
                    DoctorCheck::ok("qemu", "installed"),
                    DoctorCheck {
                        name: "optional".to_owned(),
                        status: DoctorStatus::Warn,
                        message: "not configured".to_owned(),
                    },
                ],
            }
            .has_failures()
        );
    }

    #[test]
    fn reports_failures_when_any_check_fails() {
        assert!(
            DoctorReport {
                checks: vec![
                    DoctorCheck::ok("qemu", "installed"),
                    DoctorCheck::fail("kvm", "missing"),
                ],
            }
            .has_failures()
        );
    }
}
