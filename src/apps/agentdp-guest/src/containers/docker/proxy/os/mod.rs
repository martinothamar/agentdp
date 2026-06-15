use std::path::PathBuf;

use crate::Result;

use super::Config;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod unsupported;
#[cfg(target_os = "windows")]
mod windows;

pub(super) async fn run(config: Config) -> Result<()> {
    platform::run(config).await
}

pub(super) fn default_listen_path() -> PathBuf {
    platform::default_listen_path()
}

pub(super) fn default_upstream_path() -> PathBuf {
    platform::default_upstream_path()
}

#[cfg(all(test, target_os = "linux"))]
pub(super) async fn bind_listener_for_test(path: &std::path::Path) -> Result<()> {
    platform::bind_listener(path).await.map(|_| ())
}

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
use unsupported as platform;
#[cfg(target_os = "windows")]
use windows as platform;
