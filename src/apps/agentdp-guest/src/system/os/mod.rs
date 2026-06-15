use crate::Result;
use std::path::Path;
use tokio::process::Command;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod unsupported;
#[cfg(target_os = "windows")]
mod windows;

pub(super) async fn refresh_instance_spec_from_seed(instance_spec: &Path) -> Result<()> {
    platform::refresh_instance_spec_from_seed(instance_spec).await
}

pub(super) fn configure_user_command(command: &mut Command, user: &str, home: &str) -> Result<()> {
    platform::configure_user_command(command, user, home)
}

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
use unsupported as platform;
#[cfg(target_os = "windows")]
use windows as platform;
