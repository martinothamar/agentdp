use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use agentdp_platform::fs::write_atomic;
use tokio::fs;
use tokio::process::Command;

use crate::{Error, Result};

const INSTANCE_SPEC_SEED_FILE: &str = "agentdp-instance.json";
const SEED_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) async fn refresh_instance_spec_from_seed(instance_spec: &Path) -> Result<()> {
    let Some(device) = cidata_device().await? else {
        return Ok(());
    };
    let mount_dir = std::env::temp_dir().join(format!("agentdp-seed-{}", std::process::id()));
    fs::create_dir_all(&mount_dir).await?;
    let mount_dir_text = path_text(&mount_dir);

    let mount_result = command_status(
        "mount",
        ["-o", "ro", device.as_str(), mount_dir_text.as_str()],
        SEED_COMMAND_TIMEOUT,
    )
    .await;
    if let Err(error) = mount_result {
        let _ = fs::remove_dir(&mount_dir).await;
        return Err(error);
    }

    let copy_result = copy_seeded_instance_spec(&mount_dir, instance_spec).await;
    let unmount_result = command_status("umount", [mount_dir_text.as_str()], SEED_COMMAND_TIMEOUT).await;
    let _ = fs::remove_dir(&mount_dir).await;

    copy_result?;
    unmount_result?;
    Ok(())
}

pub(super) fn configure_user_command(command: &mut Command, user: &str, home: &str) -> Result<()> {
    agentdp_platform::user::run_as_user(command, user).map_err(|source| {
        Error::Message(format!(
            "failed to configure user bootstrap command for {user}: {source}"
        ))
    })?;
    command.env("HOME", home).env("USER", user).env("LOGNAME", user);
    Ok(())
}

async fn cidata_device() -> Result<Option<String>> {
    for label in ["CIDATA", "cidata"] {
        let output = command_output("blkid", ["-L", label], SEED_COMMAND_TIMEOUT).await?;
        if output.status.success() {
            let device = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if !device.is_empty() {
                return Ok(Some(device));
            }
        }
    }
    Ok(None)
}

async fn copy_seeded_instance_spec(mount_dir: &Path, instance_spec: &Path) -> Result<()> {
    let source = mount_dir.join(INSTANCE_SPEC_SEED_FILE);
    if !fs::try_exists(&source).await? {
        return Ok(());
    }
    let contents = fs::read(&source).await?;
    if let Some(parent) = instance_spec.parent() {
        fs::create_dir_all(parent).await?;
    }
    write_atomic(instance_spec, &contents, 0o644).await?;
    Ok(())
}

async fn command_status<const N: usize>(program: &str, args: [&str; N], timeout: Duration) -> Result<()> {
    let output = command_output(program, args, timeout).await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(Error::Message(format!(
            "{program} exited with status {}; stdout: {}; stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

async fn command_output<const N: usize>(
    program: &str,
    args: [&str; N],
    timeout: Duration,
) -> Result<std::process::Output> {
    let child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .map_or_else(
            |_| {
                Err(Error::Message(format!(
                    "{program} timed out after {}s while refreshing seeded instance spec",
                    timeout.as_secs()
                )))
            },
            |output| output.map_err(Into::into),
        )
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
