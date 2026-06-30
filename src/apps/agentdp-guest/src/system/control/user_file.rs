use std::path::{Component, Path, PathBuf};

use agentdp_protocol::server_guest::WriteUserFileCommand;

use crate::{Error, Result};

use super::commands::HostCommandContext;

pub(super) async fn write(payload: serde_json::Value, context: &HostCommandContext) -> Result<bool> {
    let request = serde_json::from_value::<WriteUserFileCommand>(payload)?;
    let relative = validate_user_home_relative_path(&request.path)?;
    let mode = parse_octal_mode(&request.permissions)?;
    let target = Path::new(&context.home).join(relative);
    agentdp_platform::fs::write_user_owned_file(&target, &request.contents, mode, 0o700, &context.user)
        .await
        .map_err(|source| Error::Message(source.to_string()))
}

fn validate_user_home_relative_path(path: &str) -> Result<PathBuf> {
    let path = Path::new(path);
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(Error::Message(
            "user file path must be relative to the agent home".to_owned(),
        ));
    }
    if !path
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(Error::Message(
            "user file path must not contain . or .. components".to_owned(),
        ));
    }
    Ok(path.to_path_buf())
}

fn parse_octal_mode(mode: &str) -> Result<u32> {
    let Some(mode) = mode.strip_prefix('0') else {
        return Err(Error::Message(format!("file mode {mode} must be octal")));
    };
    let value = u32::from_str_radix(mode, 8)
        .map_err(|source| Error::Message(format!("failed to parse file mode 0{mode}: {source}")))?;
    if value > 0o777 {
        return Err(Error::Message(format!("file mode 0{mode} is too broad")));
    }
    Ok(value)
}
