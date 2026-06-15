#![deny(clippy::dbg_macro)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]

pub mod ca;
pub mod command;
pub mod dns;
pub mod fs;
pub mod host;
pub mod net;
pub mod process;
pub mod rand;
pub mod socket;
#[cfg(unix)]
pub mod socket_activation;
pub mod ssh;
pub mod text;
pub mod time;
pub mod user;
#[cfg(target_os = "windows")]
mod windows_uds;
