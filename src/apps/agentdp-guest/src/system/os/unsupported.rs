use crate::{Error, Result};
use std::path::Path;
use tokio::process::Command;

pub(super) async fn refresh_instance_spec_from_seed(_instance_spec: &Path) -> Result<()> {
    Err(Error::Message(
        "guestd system bootstrap is not supported on this guest platform".to_owned(),
    ))
}

pub(super) fn configure_user_command(_command: &mut Command, user: &str, _home: &str) -> Result<()> {
    Err(Error::Message(format!(
        "user bootstrap command setup for {user} is not supported on this guest platform"
    )))
}
