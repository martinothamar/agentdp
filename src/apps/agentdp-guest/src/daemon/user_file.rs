use std::path::{Component, Path, PathBuf};

use tokio::io::AsyncReadExt as _;

use crate::{Error, Result};

pub(crate) struct Config {
    pub(crate) user: String,
    pub(crate) home: PathBuf,
    pub(crate) path: String,
    pub(crate) permissions: String,
}

pub(crate) async fn run(config: Config) -> Result<()> {
    let mut contents = Vec::new();
    tokio::io::stdin().read_to_end(&mut contents).await?;
    let updated = write(config, &contents).await?;
    println!("{}", if updated { "updated" } else { "unchanged" });
    Ok(())
}

pub(crate) async fn write(config: Config, contents: &[u8]) -> Result<bool> {
    let relative = validate_relative_path(&config.path)?;
    let mode = parse_octal_mode(&config.permissions)?;
    let target = config.home.join(relative);
    agentdp_platform::fs::write_user_owned_file(&target, contents, mode, 0o700, &config.user)
        .await
        .map_err(|source| Error::Message(source.to_string()))
}

pub(crate) fn validate_relative_path(path: &str) -> Result<PathBuf> {
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

pub(crate) fn parse_octal_mode(mode: &str) -> Result<u32> {
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
