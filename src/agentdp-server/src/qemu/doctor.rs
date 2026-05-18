use std::path::PathBuf;

use agentdp_core::Context;
use agentdp_core::doctor::{DoctorCheck, DoctorReport};
use agentdp_core::platform::ssh::{SSH_KEYGEN_PATH_ENV, SSH_PATH_ENV};
use agentdp_core::platform::{self, KvmStatus};

use super::{disk, system};

pub fn check_prerequisites(context: &Context, report: &mut DoctorReport) {
    report.push(context, check_acceleration());
    report.push(
        context,
        check_binary_with_env(
            "qemu-system-x86_64",
            system::QEMU_SYSTEM_PATH_ENV,
            "QEMU system emulator",
        ),
    );
    report.push(
        context,
        check_binary_with_env("qemu-img", disk::QEMU_IMG_PATH_ENV, "QEMU image tooling"),
    );
    report.push(context, check_binary("curl", "base image downloader"));
    report.push(context, check_binary_with_env("ssh", SSH_PATH_ENV, "OpenSSH client"));
    report.push(
        context,
        check_binary_with_env("ssh-keygen", SSH_KEYGEN_PATH_ENV, "OpenSSH key generation"),
    );
}

fn check_acceleration() -> DoctorCheck {
    if cfg!(target_os = "windows") {
        return check_whpx();
    }
    check_kvm()
}

fn check_kvm() -> DoctorCheck {
    match platform::kvm_status() {
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

fn check_whpx() -> DoctorCheck {
    let Some(binary) = qemu_system_binary() else {
        return DoctorCheck::fail("WHPX", "qemu-system-x86_64 is required to check WHPX acceleration");
    };
    let mut command = std::process::Command::new(binary);
    command.args(["-accel", "help"]);
    match platform::hide_child_window(&mut command).output() {
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

fn qemu_system_binary() -> Option<PathBuf> {
    std::env::var_os(system::QEMU_SYSTEM_PATH_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| platform::find_binary("qemu-system-x86_64"))
        .or_else(default_windows_qemu_system)
        .filter(|path| path.is_file())
}

fn check_binary_with_env(binary: &'static str, env_var: &'static str, description: &str) -> DoctorCheck {
    if let Some(path) = std::env::var_os(env_var).filter(|value| !value.is_empty()) {
        let path = PathBuf::from(path);
        return if path.is_file() {
            DoctorCheck::ok(binary, format!("{description}: {} ({env_var})", path.display()))
        } else {
            DoctorCheck::fail(binary, format!("{env_var} points to missing file {}", path.display()))
        };
    }
    check_binary(binary, description)
}

fn check_binary(binary: &'static str, description: &str) -> DoctorCheck {
    find_binary(binary).map_or_else(
        || DoctorCheck::fail(binary, format!("{description} not found on PATH")),
        |path| DoctorCheck::ok(binary, format!("{description}: {}", path.display())),
    )
}

fn find_binary(binary: &'static str) -> Option<PathBuf> {
    platform::find_binary(binary).or_else(|| match binary {
        "qemu-system-x86_64" => default_windows_qemu_system(),
        "qemu-img" => default_windows_qemu_img(),
        _ => None,
    })
}

fn default_windows_qemu_system() -> Option<PathBuf> {
    cfg!(windows)
        .then(|| PathBuf::from(r"C:\Program Files\qemu\qemu-system-x86_64.exe"))
        .filter(|path| path.is_file())
}

fn default_windows_qemu_img() -> Option<PathBuf> {
    cfg!(windows)
        .then(|| PathBuf::from(r"C:\Program Files\qemu\qemu-img.exe"))
        .filter(|path| path.is_file())
}
