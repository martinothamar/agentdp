use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use agentdp_platform::command::run_capture;
use agentdp_platform::fs::{files_with_extension, remove_file, write_atomic};

use super::local_protocol::PrListItem;
use super::paths::RuntimePaths;
use crate::user::AgentSessionService;
use crate::{Error, Result};

const MAX_PREVIEW_LENGTH: usize = 220;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct PrRegistry {
    #[serde(default = "registry_version")]
    version: u32,
    #[serde(default)]
    pub prs: Vec<PrEntry>,
}

const fn registry_version() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrEntry {
    pub number: u64,
    pub url: String,
    pub branch: Option<String>,
    pub repo_path: String,
    pub registered_at: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct SeenEvents {
    #[serde(default)]
    pub events: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PrEvent {
    pub id: String,
    pub line: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueuedEvents {
    url: String,
    events: Vec<PrEvent>,
    updated_at: String,
}

#[derive(Debug)]
pub(crate) struct GithubPrService {
    registry: PathBuf,
    seen: PathBuf,
    queue_dir: PathBuf,
    agent_session: Arc<AgentSessionService>,
    poll_seconds: u64,
}

impl GithubPrService {
    pub(crate) fn new(paths: &RuntimePaths, agent_session: Arc<AgentSessionService>, poll_seconds: u64) -> Self {
        Self {
            registry: paths.registry.clone(),
            seen: paths.seen.clone(),
            queue_dir: paths.queue_dir.clone(),
            agent_session,
            poll_seconds,
        }
    }

    pub(crate) const fn poll_seconds(&self) -> u64 {
        self.poll_seconds
    }

    pub(crate) async fn register(&self, target: Option<&str>, cwd: &Path) -> Result<PrEntry> {
        let repo = run_capture("git", &["rev-parse", "--show-toplevel"], Some(cwd)).await?;
        let branch = run_capture("git", &["branch", "--show-current"], Some(cwd)).await?;
        let pr_target = target
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| branch.trim());
        let view = pr_view(pr_target).await?;
        let entry = PrEntry {
            number: json_u64(&view, "number").unwrap_or_default(),
            url: required_json_string(&view, "url")?,
            branch: Some(branch.trim().to_owned()).filter(|value| !value.is_empty()),
            repo_path: repo.trim().to_owned(),
            registered_at: now_marker(),
        };
        let mut registry = self.read_registry().await?;
        registry.prs.retain(|existing| existing.url != entry.url);
        registry.prs.push(entry.clone());
        self.write_registry(&registry).await?;
        self.mark_seen_events_at_registration(&entry.url).await?;
        Ok(entry)
    }

    pub(crate) async fn unregister(&self, target: Option<&str>) -> Result<String> {
        let remove_target = if let Some(target) = target.filter(|value| !value.is_empty()) {
            target.to_owned()
        } else {
            let branch = run_capture("git", &["branch", "--show-current"], None).await?;
            required_json_string(&pr_view(branch.trim()).await?, "url")?
        };
        let mut registry = self.read_registry().await?;
        let before = registry.prs.len();
        registry.prs.retain(|entry| {
            entry.url != remove_target
                && entry.number.to_string() != remove_target
                && entry.branch.as_deref() != Some(&remove_target)
        });
        if registry.prs.len() == before {
            return Err(Error::Message(format!("not registered: {remove_target}")));
        }
        self.write_registry(&registry).await?;
        Ok(remove_target)
    }

    pub(crate) async fn list(&self) -> Result<Vec<PrListItem>> {
        Ok(self
            .read_registry()
            .await?
            .prs
            .into_iter()
            .map(|entry| PrListItem {
                number: entry.number,
                url: entry.url,
                branch: entry.branch,
            })
            .collect())
    }

    pub(crate) async fn poll_once(&self) -> Result<()> {
        for entry in self.read_registry().await?.prs {
            self.handle_pr(&entry).await?;
        }
        self.flush_queued_events().await
    }

    async fn handle_pr(&self, entry: &PrEntry) -> Result<()> {
        if entry.url.is_empty() {
            return Ok(());
        }
        let view = pr_view(&entry.url).await?;
        let events = pr_events(&view, &current_gh_login().await.unwrap_or_default());
        let mut seen = self.read_seen().await?;
        let new_events = events
            .into_iter()
            .filter(|event| !seen.events.contains(&event.id))
            .collect::<Vec<_>>();
        if new_events.is_empty() {
            return Ok(());
        }
        self.queue_events(&entry.url, &new_events).await?;
        for event in new_events {
            seen.events.insert(event.id);
        }
        self.write_seen(&seen).await
    }

    async fn mark_seen_events_at_registration(&self, url: &str) -> Result<()> {
        let view = pr_view(url).await?;
        let mut seen = self.read_seen().await?;
        for event in pr_events(&view, &current_gh_login().await.unwrap_or_default()) {
            seen.events.insert(event.id);
        }
        self.write_seen(&seen).await
    }

    async fn queue_events(&self, url: &str, events: &[PrEvent]) -> Result<()> {
        let file = self.queue_dir.join(format!("{}.json", stable_hash_hex(url)));
        let mut queue = read_json_file(&file).await?.unwrap_or_else(|| QueuedEvents {
            url: url.to_owned(),
            events: Vec::new(),
            updated_at: String::new(),
        });
        let mut by_id = queue
            .events
            .into_iter()
            .map(|event| (event.id.clone(), event))
            .collect::<BTreeMap<_, _>>();
        for event in events {
            by_id.insert(event.id.clone(), event.clone());
        }
        queue = QueuedEvents {
            url: url.to_owned(),
            events: by_id.into_values().collect(),
            updated_at: now_marker(),
        };
        write_json_atomic(&file, &queue, 0o600).await
    }

    async fn flush_queued_events(&self) -> Result<()> {
        let files = json_files(&self.queue_dir).await?;
        let mut events = Vec::new();
        for file in &files {
            if let Some(queue) = read_json_file::<QueuedEvents>(file).await? {
                events.extend(queue.events);
            }
        }
        if events.is_empty() || !self.agent_session.inject_pr_events_if_idle(&events).await? {
            return Ok(());
        }
        for file in files {
            remove_file(&file).await?;
        }
        Ok(())
    }

    async fn read_registry(&self) -> Result<PrRegistry> {
        Ok(read_json_file(&self.registry).await?.unwrap_or_default())
    }

    async fn write_registry(&self, registry: &PrRegistry) -> Result<()> {
        write_json_atomic(&self.registry, registry, 0o600).await
    }

    async fn read_seen(&self) -> Result<SeenEvents> {
        Ok(read_json_file(&self.seen).await?.unwrap_or_default())
    }

    async fn write_seen(&self, seen: &SeenEvents) -> Result<()> {
        write_json_atomic(&self.seen, seen, 0o600).await
    }
}

async fn read_json_file<T>(path: &Path) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    match tokio::fs::read(path).await {
        Ok(contents) => serde_json::from_slice(&contents).map(Some).map_err(Error::from),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn write_json_atomic<T>(path: &Path, value: &T, mode: u32) -> Result<()>
where
    T: Serialize + Sync,
{
    let mut contents = serde_json::to_vec_pretty(value)?;
    contents.push(b'\n');
    write_atomic(path, &contents, mode).await.map_err(Error::from)
}

async fn json_files(path: &Path) -> Result<Vec<PathBuf>> {
    files_with_extension(path, "json").await.map_err(Error::from)
}

async fn pr_view(target: &str) -> Result<Value> {
    let stdout = run_capture(
        "gh",
        &[
            "pr",
            "view",
            target,
            "--json",
            "number,url,headRefName,baseRefName,title,state,updatedAt,reviewDecision,statusCheckRollup,reviews,comments",
        ],
        None,
    )
    .await?;
    serde_json::from_str(&stdout).map_err(Error::from)
}

async fn current_gh_login() -> Result<String> {
    run_capture("gh", &["api", "user", "--jq", ".login"], None)
        .await
        .map_err(Error::from)
}

fn required_json_string(value: &Value, field: &str) -> Result<String> {
    json_string(value, field).ok_or_else(|| Error::Message(format!("gh response missing field {field}")))
}

fn json_string(value: &Value, field: &str) -> Option<String> {
    value.get(field)?.as_str().map(ToOwned::to_owned)
}

fn json_u64(value: &Value, field: &str) -> Option<u64> {
    value.get(field)?.as_u64()
}

fn value_string(value: &Value, field: &str) -> String {
    json_string(value, field).unwrap_or_default()
}

fn pr_events(view: &Value, self_login: &str) -> Vec<PrEvent> {
    let mut events = Vec::new();
    events.extend(failed_check_events(view));
    events.extend(review_events(view, self_login));
    events.extend(comment_events(view, self_login));
    events
}

fn failed_check_events(view: &Value) -> Vec<PrEvent> {
    let Some(checks) = view.get("statusCheckRollup").and_then(Value::as_array) else {
        return Vec::new();
    };
    checks
        .iter()
        .filter_map(|check| {
            let status = value_string(check, "conclusion");
            if !matches!(
                status.as_str(),
                "FAILURE" | "ERROR" | "CANCELLED" | "TIMED_OUT" | "ACTION_REQUIRED"
            ) {
                return None;
            }
            let name = json_string(check, "name")
                .or_else(|| json_string(check, "context"))
                .or_else(|| json_string(check, "workflowName"))
                .unwrap_or_else(|| "unknown".to_owned());
            let details = json_string(check, "detailsUrl")
                .or_else(|| json_string(check, "targetUrl"))
                .map_or_else(String::new, |url| format!(" details={url}"));
            let identity = format!("check_failed:{}:{name}:{status}", value_string(view, "url"));
            Some(PrEvent {
                id: stable_hash_hex(&identity),
                line: format!(
                    "{} event=check_failed status={status} name={}{}",
                    pr_prefix(view),
                    quote(&name),
                    details
                ),
            })
        })
        .collect()
}

fn review_events(view: &Value, self_login: &str) -> Vec<PrEvent> {
    let Some(reviews) = view.get("reviews").and_then(Value::as_array) else {
        return Vec::new();
    };
    reviews
        .iter()
        .filter(|review| !authored_by_self(review, self_login))
        .map(|review| {
            let author = author_login(review);
            let state = value_string(review, "state");
            let submitted_at = value_string(review, "submittedAt");
            let preview = if trusted_author(review) {
                compact_text(&value_string(review, "body"))
            } else {
                String::new()
            };
            let body = if preview.is_empty() {
                String::new()
            } else {
                format!(" body={}", quote(&preview))
            };
            let identity = format!(
                "review:{}:{author}:{state}:{submitted_at}:{preview}",
                value_string(view, "url")
            );
            PrEvent {
                id: stable_hash_hex(&identity),
                line: format!(
                    "{} event=review state={state} author={author} at={submitted_at}{body}",
                    pr_prefix(view)
                ),
            }
        })
        .collect()
}

fn comment_events(view: &Value, self_login: &str) -> Vec<PrEvent> {
    let Some(comments) = view.get("comments").and_then(Value::as_array) else {
        return Vec::new();
    };
    comments
        .iter()
        .filter(|comment| !authored_by_self(comment, self_login))
        .map(|comment| {
            let author = author_login(comment);
            let updated_at = json_string(comment, "updatedAt")
                .or_else(|| json_string(comment, "createdAt"))
                .unwrap_or_else(|| "unknown".to_owned());
            let url = json_string(comment, "url").unwrap_or_else(|| value_string(view, "url"));
            let preview = if trusted_author(comment) {
                compact_text(&value_string(comment, "body"))
            } else {
                String::new()
            };
            let body = if preview.is_empty() {
                String::new()
            } else {
                format!(" body={}", quote(&preview))
            };
            let identity = format!("comment:{url}:{author}:{updated_at}:{preview}");
            PrEvent {
                id: stable_hash_hex(&identity),
                line: format!(
                    "{} event=comment author={author} at={updated_at} comment={url}{body}",
                    pr_prefix(view)
                ),
            }
        })
        .collect()
}

fn authored_by_self(item: &Value, self_login: &str) -> bool {
    !self_login.is_empty() && author_login(item) == self_login
}

fn author_login(item: &Value) -> String {
    item.get("author")
        .and_then(|author| author.get("login"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned()
}

fn trusted_author(item: &Value) -> bool {
    matches!(
        value_string(item, "authorAssociation").as_str(),
        "OWNER" | "MEMBER" | "COLLABORATOR" | "CONTRIBUTOR"
    )
}

fn pr_prefix(view: &Value) -> String {
    let url = value_string(view, "url");
    let number = json_u64(view, "number").map_or_else(|| "?".to_owned(), |number| number.to_string());
    format!("pr=#{number} url={url}")
}

fn quote(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
}

fn compact_text(value: &str) -> String {
    let text = strip_angle_tags(&strip_html_comments_and_details(value))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if text.len() <= MAX_PREVIEW_LENGTH {
        text
    } else {
        let mut end = MAX_PREVIEW_LENGTH.saturating_sub(3);
        while !text.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        format!("{}...", &text[..end])
    }
}

fn strip_html_comments_and_details(value: &str) -> String {
    let mut output = String::new();
    let mut remaining = value;
    loop {
        let comment = remaining.find("<!--");
        let details = remaining.to_ascii_lowercase().find("<details");
        let next = match (comment, details) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        let Some(start) = next else {
            output.push_str(remaining);
            break;
        };
        output.push_str(&remaining[..start]);
        let lower = remaining[start..].to_ascii_lowercase();
        let end_marker = if lower.starts_with("<!--") { "-->" } else { "</details>" };
        let Some(end) = lower.find(end_marker) else {
            break;
        };
        if end_marker == "</details>" {
            output.push_str(" [details omitted] ");
        }
        remaining = &remaining[start + end + end_marker.len()..];
    }
    output
}

fn strip_angle_tags(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut inside = false;
    for character in value.chars() {
        match character {
            '<' => inside = true,
            '>' => {
                inside = false;
                output.push(' ');
            }
            _ if !inside => output.push(character),
            _ => {}
        }
    }
    output
}

pub(super) fn render_prompt(events: &[PrEvent]) -> String {
    let lines = events
        .iter()
        .map(|event| event.line.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    format!("<pr_events>\n{lines}\n</pr_events>\n")
}

pub(super) fn stable_hash_hex(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn now_marker() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or_else(|_| "0".to_owned(), |duration| duration.as_millis().to_string())
}

#[cfg(test)]
mod tests {
    use super::{compact_text, stable_hash_hex};

    #[test]
    fn compact_text_removes_html_and_limits_length() {
        let text = compact_text("hello <!-- hidden --> <details>secret</details> <b>world</b>");

        assert!(text.contains("hello"));
        assert!(text.contains("world"));
        assert!(!text.contains("hidden"));
        assert!(!text.contains("secret"));
    }

    #[test]
    fn compact_text_truncates_at_utf8_boundary() {
        let text = compact_text(&format!("{}• trailing", "a".repeat(215)));

        assert!(text.ends_with("..."));
        assert!(text.len() <= 220);
    }

    #[test]
    fn stable_hash_is_deterministic() {
        assert_eq!(stable_hash_hex("same"), stable_hash_hex("same"));
        assert_ne!(stable_hash_hex("same"), stable_hash_hex("different"));
    }
}
