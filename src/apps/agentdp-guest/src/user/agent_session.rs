use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Mutex;

use super::github_pr::{PrEvent, render_prompt, stable_hash_hex};
use super::paths::RuntimePaths;
use crate::{Error, Result};
use agentdp_platform::command::{run_capture, run_status};
use agentdp_platform::fs::{read_optional_text, remove_file, write_atomic};

#[derive(Debug)]
pub(crate) struct TmuxAgentSession {
    code_dir: PathBuf,
    pane_file: PathBuf,
    state_dir: PathBuf,
    last_pane_capture: Mutex<Option<PaneCapture>>,
    launch_command: String,
    idle_seconds: u64,
}

impl TmuxAgentSession {
    pub(crate) fn new(paths: &RuntimePaths, launch_command: impl Into<String>, idle_seconds: u64) -> Self {
        Self {
            code_dir: paths.code_dir.clone(),
            pane_file: paths.pane_file.clone(),
            state_dir: paths.state_dir.clone(),
            last_pane_capture: Mutex::new(None),
            launch_command: launch_command.into(),
            idle_seconds,
        }
    }

    pub(crate) async fn ensure_session(&self) -> Result<String> {
        ensure_session(&self.code_dir, &self.pane_file, &self.launch_command).await
    }

    pub(super) async fn inject_pr_events_if_idle(&self, events: &[PrEvent]) -> Result<bool> {
        let pane = match read_optional_text(&self.pane_file).await? {
            Some(value) if !value.trim().is_empty() => value.trim().to_owned(),
            _ => return Ok(false),
        };
        if !pane_exists(&pane).await? || !looks_idle(&self.last_pane_capture, self.idle_seconds, &pane).await? {
            return Ok(false);
        }
        inject_prompt(&self.state_dir, &pane, events).await?;
        Ok(true)
    }
}

#[derive(Debug)]
struct PaneCapture {
    pane: String,
    hash: String,
    since: std::time::Instant,
}

async fn ensure_session(code_dir: &Path, pane_file: &Path, command: &str) -> Result<String> {
    let session = std::env::var("AGENTDP_TMUX_SESSION").unwrap_or_else(|_| "agentdp".to_owned());
    if run_status("tmux", &["has-session", "-t", &session], None)
        .await
        .is_err()
    {
        if command == CODEX_SESSION_COMMAND {
            dismiss_known_codex_update_prompt().await?;
        }
        let login_command = login_shell_command(command);
        run_capture(
            "tmux",
            &[
                "new-session",
                "-d",
                "-s",
                &session,
                "-c",
                code_dir
                    .to_str()
                    .ok_or_else(|| Error::Message("AGENTDP_CODE_DIR is not valid UTF-8".to_owned()))?,
                &login_command,
            ],
            None,
        )
        .await?;
    }
    let pane = run_capture(
        "tmux",
        &["display-message", "-p", "-t", &format!("{session}:0.0"), "#{pane_id}"],
        None,
    )
    .await?;
    write_atomic(pane_file, format!("{}\n", pane.trim()).as_bytes(), 0o600).await?;
    Ok(pane.trim().to_owned())
}

pub(crate) const CLAUDE_SESSION_COMMAND: &str = "claude --continue || claude";
pub(crate) const CODEX_SESSION_COMMAND: &str = r#"if find "$HOME/.codex/sessions" -type f -name '*.jsonl' -print -quit 2>/dev/null | grep -q .; then exec codex resume --last; else exec codex; fi"#;

fn login_shell_command(command: &str) -> String {
    format!("exec bash --login -c {}", shell_single_quote(command))
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

async fn dismiss_known_codex_update_prompt() -> Result<()> {
    let path = codex_version_file()?;
    let Some(contents) = read_optional_text(&path).await? else {
        return Ok(());
    };
    let Some(updated) = dismiss_known_codex_update_prompt_in(&contents)? else {
        return Ok(());
    };
    write_atomic(&path, &updated, 0o600).await?;
    Ok(())
}

fn codex_version_file() -> Result<PathBuf> {
    if let Some(codex_home) = std::env::var_os("CODEX_HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(codex_home).join("version.json"));
    }
    let home = std::env::var_os("HOME").ok_or_else(|| Error::Message("HOME must be set".to_owned()))?;
    Ok(PathBuf::from(home).join(".codex/version.json"))
}

fn dismiss_known_codex_update_prompt_in(contents: &str) -> Result<Option<Vec<u8>>> {
    let mut value = serde_json::from_str::<Value>(contents)?;
    let Some(latest_version) = value
        .get("latest_version")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
    else {
        return Ok(None);
    };
    if value.get("dismissed_version").and_then(Value::as_str) == Some(latest_version.as_str()) {
        return Ok(None);
    }
    value["dismissed_version"] = Value::String(latest_version);
    let mut updated = serde_json::to_vec_pretty(&value)?;
    updated.push(b'\n');
    Ok(Some(updated))
}

async fn pane_exists(pane: &str) -> Result<bool> {
    let panes = run_capture("tmux", &["list-panes", "-a", "-F", "#{pane_id}"], None).await?;
    Ok(panes.lines().any(|line| line == pane))
}

async fn looks_idle(last_pane_capture: &Mutex<Option<PaneCapture>>, idle_seconds: u64, pane: &str) -> Result<bool> {
    if idle_seconds == 0 {
        return Ok(true);
    }
    let capture = run_capture("tmux", &["capture-pane", "-p", "-t", pane, "-S", "-80"], None).await?;
    if capture.is_empty() {
        return Ok(false);
    }
    let capture_hash = stable_hash_hex(&capture);
    let mut last = last_pane_capture.lock().await;
    if !matches!(
        last.as_ref(),
        Some(previous) if previous.pane == pane && previous.hash == capture_hash
    ) {
        *last = Some(PaneCapture {
            pane: pane.to_owned(),
            hash: capture_hash,
            since: std::time::Instant::now(),
        });
        return Ok(false);
    }
    Ok(last
        .as_ref()
        .is_some_and(|previous| previous.since.elapsed() >= Duration::from_secs(idle_seconds)))
}

async fn inject_prompt(state_dir: &Path, pane: &str, events: &[PrEvent]) -> Result<()> {
    let prompt_file = state_dir.join(format!("pr-prompt.{}.txt", std::process::id()));
    let prompt = render_prompt(events);
    write_atomic(&prompt_file, prompt.as_bytes(), 0o600).await?;
    run_capture(
        "tmux",
        &[
            "load-buffer",
            "-b",
            "agentdp-pr",
            prompt_file
                .to_str()
                .ok_or_else(|| Error::Message("prompt path is not valid UTF-8".to_owned()))?,
        ],
        None,
    )
    .await?;
    run_capture(
        "tmux",
        &["paste-buffer", "-b", "agentdp-pr", "-t", pane, "-p", "-r"],
        None,
    )
    .await?;
    run_capture("tmux", &["send-keys", "-t", pane, "Enter"], None).await?;
    remove_file(&prompt_file).await.map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use std::process::Command;

    use super::{CLAUDE_SESSION_COMMAND, CODEX_SESSION_COMMAND, dismiss_known_codex_update_prompt_in};

    #[test]
    fn codex_session_command_only_starts_fresh_without_saved_sessions() {
        assert!(CODEX_SESSION_COMMAND.contains("codex resume --last"));
        assert!(CODEX_SESSION_COMMAND.contains("else exec codex"));
        assert!(!CODEX_SESSION_COMMAND.contains("|| codex"));
    }

    #[test]
    fn managed_sessions_start_through_login_shell() {
        let command = super::login_shell_command(CODEX_SESSION_COMMAND);

        assert!(command.starts_with("exec bash --login -c "));
        assert!(command.contains("codex resume --last"));
        assert!(command.contains("'\"'\"'*.jsonl'\"'\"'"));
    }

    #[test]
    fn login_shell_command_quotes_agent_commands() {
        for command in [CODEX_SESSION_COMMAND, CLAUDE_SESSION_COMMAND] {
            let quoted = super::shell_single_quote(command);
            let output = Command::new("sh")
                .arg("-c")
                .arg(format!("printf '%s' {quoted}"))
                .output()
                .expect("run shell");

            assert!(
                output.status.success(),
                "shell failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(String::from_utf8(output.stdout).expect("stdout is utf8"), command);
        }
    }

    #[test]
    fn dismiss_known_codex_update_prompt_sets_dismissed_to_latest() {
        let updated = dismiss_known_codex_update_prompt_in(
            r#"{"latest_version":"0.142.2","last_checked_at":"now","dismissed_version":"0.140.0"}"#,
        )
        .expect("valid json")
        .expect("updated json");
        let value: Value = serde_json::from_slice(&updated).expect("updated json should parse");

        assert_eq!(value["dismissed_version"], "0.142.2");
    }

    #[test]
    fn dismiss_known_codex_update_prompt_is_idempotent() {
        let updated = dismiss_known_codex_update_prompt_in(
            r#"{"latest_version":"0.142.2","last_checked_at":"now","dismissed_version":"0.142.2"}"#,
        )
        .expect("valid json");

        assert!(updated.is_none());
    }
}
