use crate::Context;
use crate::platform::{self, PlatformPaths};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorReport {
    pub paths: Option<PlatformPaths>,
    pub checks: Vec<DoctorCheck>,
}

impl DoctorReport {
    #[must_use]
    pub const fn new(paths: Option<PlatformPaths>) -> Self {
        Self {
            paths,
            checks: Vec::new(),
        }
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

#[must_use]
pub fn run_doctor(context: &Context) -> DoctorReport {
    context.logger().verbose("running core doctor checks");
    let paths = context.paths().map_or(None, |paths| Some(paths.clone()));
    let mut report = DoctorReport::new(paths);
    report.push(context, check_host(context));

    match context.paths() {
        Ok(paths) => check_directories(context, paths, &mut report),
        Err(error) => report.push(context, DoctorCheck::fail("agentdp directories", error.to_string())),
    }

    context
        .logger()
        .verbose_with(|| format!("core doctor completed with {} checks", report.checks.len()));
    report
}

fn check_host(context: &Context) -> DoctorCheck {
    let host = context.host_target();
    if matches!(host, platform::HostTarget::Linux | platform::HostTarget::Wsl2) {
        DoctorCheck::ok("Linux/WSL2 host", host.label())
    } else {
        DoctorCheck::fail(
            "Linux/WSL2 host",
            format!(
                "{} is not supported; only Linux and WSL2 hosts are supported in the first implementation",
                host.label()
            ),
        )
    }
}

fn check_directories(context: &Context, paths: &PlatformPaths, report: &mut DoctorReport) {
    for (name, path) in paths.entries() {
        match platform::ensure_writable_directory(path) {
            Ok(()) => report.push(
                context,
                DoctorCheck::ok(name, format!("{} is writable", path.display())),
            ),
            Err(error) => report.push(
                context,
                DoctorCheck::fail(name, format!("{} is not writable: {error}", path.display())),
            ),
        }
    }
}
