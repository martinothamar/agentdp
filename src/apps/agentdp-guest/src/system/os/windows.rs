use crate::{Error, Result};
use std::path::Path;
use tokio::process::Command;

pub(super) async fn refresh_instance_spec_from_seed(_instance_spec: &Path) -> Result<()> {
    Err(Error::Message(
        "guestd system bootstrap is not supported on windows guests".to_owned(),
    ))
}

pub(super) fn configure_user_command(command: &mut Command, user: &str, home: &str) -> Result<()> {
    agentdp_platform::user::run_as_user(command, user).map_err(|source| {
        Error::Message(format!(
            "failed to configure user bootstrap command for {user}: {source}"
        ))
    })?;
    command
        .env("HOME", home)
        .env("USER", user)
        .env("LOGNAME", user)
        .env("USERNAME", user);
    Ok(())
}
