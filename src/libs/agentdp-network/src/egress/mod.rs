//! Guest-relative egress plumbing.
//!
//! In this crate, egress means traffic leaving the guest VM toward an upstream
//! network destination.

pub(crate) mod tcp;
pub(crate) mod udp;
