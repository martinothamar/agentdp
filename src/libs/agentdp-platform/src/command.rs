use std::path::Path;

use thiserror::Error;
use tokio::process::Command as ProcessCommand;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("{command} failed with status {status}: {stderr}")]
    CommandFailed {
        command: String,
        status: i32,
        stderr: String,
    },
    #[error("failed to run {command}: {source}")]
    RunCommand {
        command: String,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(target_os = "windows")]
pub fn hide_child_window(command: &mut tokio::process::Command) -> &mut tokio::process::Command {
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    command.creation_flags(CREATE_NO_WINDOW)
}

#[cfg(not(target_os = "windows"))]
pub const fn hide_child_window(command: &mut tokio::process::Command) -> &mut tokio::process::Command {
    command
}

/// Runs a command and returns trimmed UTF-8-lossy stdout.
///
/// # Errors
///
/// Returns an error when the process cannot be spawned or exits with a
/// non-zero status.
pub async fn run_capture(command: &str, args: &[&str], cwd: Option<&Path>) -> Result<String, RunError> {
    let mut process = ProcessCommand::new(command);
    process.args(args);
    if let Some(cwd) = cwd {
        process.current_dir(cwd);
    }
    let output = process.output().await.map_err(|source| RunError::RunCommand {
        command: render_command(command, args),
        source,
    })?;
    if !output.status.success() {
        return Err(RunError::CommandFailed {
            command: render_command(command, args),
            status: output.status.code().unwrap_or(1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Runs a command and succeeds only when it exits successfully.
///
/// # Errors
///
/// Returns an error when the process cannot be spawned or exits with a
/// non-zero status.
pub async fn run_status(command: &str, args: &[&str], cwd: Option<&Path>) -> Result<(), RunError> {
    run_capture(command, args, cwd).await.map(|_| ())
}

fn render_command(command: &str, args: &[&str]) -> String {
    let mut output = command.to_owned();
    for arg in args {
        output.push(' ');
        output.push_str(arg);
    }
    output
}
