use std::process::ExitCode;

use agentdp_core::Context;
use agentdp_core::backend::BackendKind;
use agentdp_core::doctor::{DoctorCheck, DoctorReport, run_doctor};
use agentdp_protocol::{RequestKind, ServerDoctorParams, ServerDoctorResult};
use clap::Args;

use crate::server_client;

#[derive(Debug, Args)]
pub struct Command;

pub fn run(_command: &Command, context: &Context) -> ExitCode {
    let mut report = run_doctor(context);
    check_server_and_backend(context, &mut report, BackendKind::local_default());
    print_doctor_report(&report);
    if report.has_failures() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn check_server_and_backend(context: &Context, report: &mut DoctorReport, backend: BackendKind) {
    let Some(paths) = report.paths.clone() else {
        return;
    };

    let ping = match server_client::ensure_running(context, &paths) {
        Ok(ping) => {
            report.push(
                context,
                DoctorCheck::ok(
                    "agentdp-server",
                    format!("responded to server.ping on {}", ping.socket.display()),
                ),
            );
            ping
        }
        Err(error) => {
            report.push(context, DoctorCheck::fail("agentdp-server", error.to_string()));
            return;
        }
    };

    context.logger().verbose_with(|| {
        format!(
            "agentdp-server pid {} will run {} backend doctor checks",
            ping.pid,
            backend.as_str()
        )
    });
    match server_client::request::<ServerDoctorResult>(
        context,
        &paths,
        RequestKind::ServerDoctor(ServerDoctorParams { backend }),
        None,
    ) {
        Ok(result) => {
            for check in result.checks {
                report.push(
                    context,
                    DoctorCheck {
                        name: check.name,
                        status: if check.status == "ok" {
                            agentdp_core::doctor::DoctorStatus::Ok
                        } else {
                            agentdp_core::doctor::DoctorStatus::Fail
                        },
                        message: check.message,
                    },
                );
            }
        }
        Err(error) => report.push(
            context,
            DoctorCheck::fail(
                format!("{} backend checks", backend.as_str()),
                format!("agentdp-server could not run backend doctor checks: {error}"),
            ),
        ),
    }
}

fn print_doctor_report(report: &DoctorReport) {
    println!("agentdp doctor");
    for check in &report.checks {
        println!("{:<5} {:<24} {}", check.status.label(), check.name, check.message);
    }

    if let Some(paths) = &report.paths {
        println!();
        println!("paths");
        println!("{:<8} {}", "data", paths.data.display());
        println!("{:<8} {}", "config", paths.config.display());
        println!("{:<8} {}", "cache", paths.cache.display());
        println!("{:<8} {}", "runtime", paths.runtime.display());
        println!("{:<8} {}", "logs", paths.logs.display());
        println!("{:<8} {}", "socket", paths.socket_path().display());
    }
}
