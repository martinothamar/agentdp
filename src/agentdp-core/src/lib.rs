#![forbid(unsafe_code)]
// agentdp-core is library code. Return structured data/errors; CLI crates own user I/O.
#![deny(clippy::dbg_macro)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]

pub mod backend;
pub mod context;
pub mod doctor;
pub mod installation;
pub mod logging;
pub mod manifest;
pub mod platform;
pub mod provisioning;

pub use context::Context;
