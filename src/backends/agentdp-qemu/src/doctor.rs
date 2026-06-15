use std::path::PathBuf;

use agentdp_core::Context;
use agentdp_core::doctor::{DoctorCheck, DoctorReport};
use agentdp_platform::ssh::{SSH_KEYGEN_PATH_ENV, SSH_PATH_ENV};
use agentdp_platform::{self as platform, host::KvmStatus};
use tokio::process::Command;

use crate::{disk, system};

pub async fn check_prerequisites(context: &Context, report: &mut DoctorReport) {
    report.push(context, check_acceleration().await);
    report.push(
        context,
        check_binary_with_env(
            "qemu-system-x86_64",
            system::QEMU_SYSTEM_PATH_ENV,
            "QEMU system emulator",
        )
        .await,
    );
    report.push(
        context,
        check_binary_with_env("qemu-img", disk::QEMU_IMG_PATH_ENV, "QEMU image tooling").await,
    );
    report.push(context, check_binary("curl", "base image downloader").await);
    report.push(
        context,
        check_binary_with_env("ssh", SSH_PATH_ENV, "OpenSSH client").await,
    );
    report.push(
        context,
        check_binary_with_env("ssh-keygen", SSH_KEYGEN_PATH_ENV, "OpenSSH key generation").await,
    );
}

async fn check_acceleration() -> DoctorCheck {
    if cfg!(target_os = "windows") {
        return check_whpx().await;
    }
    check_kvm().await
}

async fn check_kvm() -> DoctorCheck {
    match platform::host::kvm_status().await {
        KvmStatus::Usable => DoctorCheck::ok("/dev/kvm", "exists and can be opened read/write"),
        KvmStatus::Missing => {
            DoctorCheck::fail("/dev/kvm", "/dev/kvm does not exist; QEMU/KVM acceleration is required")
        }
        KvmStatus::Unusable(error) => {
            DoctorCheck::fail("/dev/kvm", format!("exists but is not usable by this user: {error}"))
        }
        KvmStatus::Unsupported(host) => DoctorCheck::fail(
            "/dev/kvm",
            format!("KVM is only supported on Linux/WSL2 hosts, not {host}"),
        ),
    }
}

async fn check_whpx() -> DoctorCheck {
    let Some(binary) = qemu_system_binary().await else {
        return DoctorCheck::fail("WHPX", "qemu-system-x86_64 is required to check WHPX acceleration");
    };
    let mut command = Command::new(binary);
    command.args(["-accel", "help"]);
    match platform::command::hide_child_window(&mut command).output().await {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout).to_ascii_lowercase();
            if stdout.lines().any(|line| line.trim() == "whpx") {
                DoctorCheck::ok("WHPX", "QEMU supports Windows Hypervisor Platform acceleration")
            } else {
                DoctorCheck::fail("WHPX", "QEMU does not list whpx in supported accelerators")
            }
        }
        Ok(output) => DoctorCheck::fail(
            "WHPX",
            format!(
                "failed to list QEMU accelerators: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ),
        Err(error) => DoctorCheck::fail("WHPX", format!("failed to run qemu-system-x86_64: {error}")),
    }
}

async fn qemu_system_binary() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(system::QEMU_SYSTEM_PATH_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        && tokio::fs::metadata(&path)
            .await
            .is_ok_and(|metadata| metadata.is_file())
    {
        return Some(path);
    }
    platform::host::find_binary("qemu-system-x86_64")
        .await
        .or(default_windows_qemu_system().await)
}

async fn check_binary_with_env(binary: &'static str, env_var: &'static str, description: &str) -> DoctorCheck {
    if let Some(path) = std::env::var_os(env_var).filter(|value| !value.is_empty()) {
        let path = PathBuf::from(path);
        return if tokio::fs::metadata(&path)
            .await
            .is_ok_and(|metadata| metadata.is_file())
        {
            DoctorCheck::ok(binary, format!("{description}: {} ({env_var})", path.display()))
        } else {
            DoctorCheck::fail(binary, format!("{env_var} points to missing file {}", path.display()))
        };
    }
    check_binary(binary, description).await
}

async fn check_binary(binary: &'static str, description: &str) -> DoctorCheck {
    find_binary(binary).await.map_or_else(
        || DoctorCheck::fail(binary, format!("{description} not found on PATH")),
        |path| DoctorCheck::ok(binary, format!("{description}: {}", path.display())),
    )
}

async fn find_binary(binary: &'static str) -> Option<PathBuf> {
    match platform::host::find_binary(binary).await {
        Some(path) => Some(path),
        None => match binary {
            "qemu-system-x86_64" => default_windows_qemu_system().await,
            "qemu-img" => default_windows_qemu_img().await,
            _ => None,
        },
    }
}

async fn default_windows_qemu_system() -> Option<PathBuf> {
    if !cfg!(windows) {
        return None;
    }
    let path = PathBuf::from(r"C:\Program Files\qemu\qemu-system-x86_64.exe");
    tokio::fs::metadata(&path)
        .await
        .is_ok_and(|metadata| metadata.is_file())
        .then_some(path)
}

async fn default_windows_qemu_img() -> Option<PathBuf> {
    if !cfg!(windows) {
        return None;
    }
    let path = PathBuf::from(r"C:\Program Files\qemu\qemu-img.exe");
    tokio::fs::metadata(&path)
        .await
        .is_ok_and(|metadata| metadata.is_file())
        .then_some(path)
}
