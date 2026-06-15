#![forbid(unsafe_code)]
// agentdp-core is library code. Return structured data/errors; CLI crates own user I/O.
#![deny(clippy::dbg_macro)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]

pub mod agent;
pub mod context;
pub mod control_plane;
pub mod doctor;
pub mod layout;
pub mod logging;
pub mod manifest;
pub mod mediated_network;
pub mod provisioning;

pub use context::Context;
