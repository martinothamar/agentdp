use std::process::ExitCode;

use agentdp_core::doctor::{DoctorCheck, DoctorReport, DoctorStatus};
use agentdp_core::{Context, layout::AgentdpLayout};
use agentdp_platform as platform;
use agentdp_protocol::client_server::BackendKind;
use agentdp_protocol::client_server::{RequestKind, ServerDoctorParams, ServerDoctorResult};
use clap::Args;

use crate::server_client;

#[derive(Debug, Args)]
pub(crate) struct Command;

pub(crate) async fn run(_command: &Command, context: &Context) -> ExitCode {
    let mut report = DoctorReport::new();
    let layout = check_local_host(context, &mut report).await;
    if let Some(layout) = &layout {
        check_server_and_backend(context, &mut report, layout, BackendKind::Qemu).await;
    }
    print_doctor_report(&report, layout.as_ref());
    if report.has_failures() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn check_local_host(context: &Context, report: &mut DoctorReport) -> Option<AgentdpLayout> {
    let host = platform::host::host_target().await;
    if matches!(
        host,
        platform::host::HostTarget::Linux | platform::host::HostTarget::Wsl2 | platform::host::HostTarget::Windows
    ) {
        report.push(context, DoctorCheck::ok("QEMU host", host.label()));
    } else {
        report.push(
            context,
            DoctorCheck::fail(
                "QEMU host",
                format!("{} is not supported; use Linux, WSL2, or Windows", host.label()),
            ),
        );
    }

    let layout = match AgentdpLayout::resolve() {
        Ok(layout) => layout,
        Err(error) => {
            report.push(context, DoctorCheck::fail("agentdp directories", error.to_string()));
            return None;
        }
    };

    for (name, path) in layout.writable_directories() {
        match platform::fs::ensure_writable_directory(&path).await {
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

    Some(layout)
}

async fn check_server_and_backend(
    context: &Context,
    report: &mut DoctorReport,
    layout: &AgentdpLayout,
    backend: BackendKind,
) {
    let ping = match server_client::ensure_running(context, layout).await {
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
        layout,
        RequestKind::ServerDoctor(ServerDoctorParams { backend }),
        None,
    )
    .await
    {
        Ok(result) => {
            for check in result.checks {
                report.push(
                    context,
                    DoctorCheck {
                        name: check.name,
                        status: if check.status == "ok" {
                            DoctorStatus::Ok
                        } else {
                            DoctorStatus::Fail
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

fn print_doctor_report(report: &DoctorReport, layout: Option<&AgentdpLayout>) {
    println!("agentdp doctor");
    for check in &report.checks {
        println!("{:<5} {:<24} {}", check.status.label(), check.name, check.message);
    }

    if let Some(layout) = layout {
        println!();
        println!("paths");
        println!("{:<10} {}", "root", layout.root().display());
        println!("{:<10} {}", "config", layout.config_dir().display());
        println!("{:<10} {}", "cache", layout.cache_dir().display());
        println!("{:<10} {}", "agents", layout.agents_dir().display());
        println!("{:<10} {}", "socket", layout.socket_path().display());
        println!("{:<10} {}", "server-log", layout.server_log().display());
    }
}
