use std::path::PathBuf;

use crate::{Error, Result};

use super::super::Config;

pub(super) async fn run(_config: Config) -> Result<()> {
    Err(Error::Message(
        "guestd docker proxy is not supported on macos guests".to_owned(),
    ))
}

pub(super) fn default_listen_path() -> PathBuf {
    PathBuf::from("/tmp/agentdp/docker.sock")
}

pub(super) fn default_upstream_path() -> PathBuf {
    PathBuf::from("/tmp/agentdp/docker-upstream.sock")
}
