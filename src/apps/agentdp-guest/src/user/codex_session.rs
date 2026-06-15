use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::Mutex;

use super::github_pr::{PrEvent, render_prompt, stable_hash_hex};
use super::paths::RuntimePaths;
use crate::{Error, Result};
use agentdp_platform::command::{run_capture, run_status};
use agentdp_platform::fs::{read_optional_text, remove_file, write_atomic};

#[derive(Debug)]
pub(crate) struct CodexSessionService {
    code_dir: PathBuf,
    pane_file: PathBuf,
    state_dir: PathBuf,
    last_pane_capture: Mutex<Option<PaneCapture>>,
    idle_seconds: u64,
}

impl CodexSessionService {
    pub(crate) fn new(paths: &RuntimePaths, idle_seconds: u64) -> Self {
        Self {
            code_dir: paths.code_dir.clone(),
            pane_file: paths.pane_file.clone(),
            state_dir: paths.state_dir.clone(),
            last_pane_capture: Mutex::new(None),
            idle_seconds,
        }
    }

    pub(crate) async fn ensure_session(&self) -> Result<String> {
        ensure_session(&self.code_dir, &self.pane_file).await
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

async fn ensure_session(code_dir: &Path, pane_file: &Path) -> Result<String> {
    let session = std::env::var("AGENTDP_TMUX_SESSION").unwrap_or_else(|_| "agentdp".to_owned());
    if run_status("tmux", &["has-session", "-t", &session], None)
        .await
        .is_err()
    {
        let command = "codex resume --last || codex";
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
                command,
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
