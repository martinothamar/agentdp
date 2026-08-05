use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use agentdp_core::agent::{AgentInstanceCredentialPhase, AgentInstanceCredentialState};
use agentdp_core::provisioning::host_input::{HostInputRequirements, ManagedHostCredential};
use agentdp_ds::local::{oneshot, spsc};
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use thiserror::Error;
use tokio::task::JoinHandle;

use super::seed::resolve_host_input_file_source;

const CODEX_CREDENTIAL: &str = "codex";
// Keep this OAuth contract aligned with Codex login's ChatGPT refresh flow.
const CODEX_REFRESH_ENDPOINT: &str = "https://auth.openai.com/oauth/token";
const CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const CODEX_REFRESH_ENDPOINT_ENV: &str = "CODEX_REFRESH_TOKEN_URL_OVERRIDE";
const CODEX_CLIENT_ID_ENV: &str = "CODEX_APP_SERVER_LOGIN_CLIENT_ID";
const REFRESH_WINDOW: Duration = Duration::from_hours(1);
const OPAQUE_TOKEN_REFRESH_AGE: Duration = Duration::from_hours(7 * 24);
const TRANSIENT_RETRY_DELAY: Duration = Duration::from_mins(15);
const CONCURRENT_REFRESH_OBSERVE_INTERVAL: Duration = Duration::from_millis(25);
const CONCURRENT_REFRESH_OBSERVE_TIMEOUT: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const COMMAND_CAPACITY: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct CodexOAuthConfig {
    endpoint: String,
    client_id: String,
}

impl CodexOAuthConfig {
    fn from_env() -> Self {
        Self::resolve(
            std::env::var(CODEX_REFRESH_ENDPOINT_ENV).ok(),
            std::env::var(CODEX_CLIENT_ID_ENV).ok(),
        )
    }

    fn resolve(endpoint: Option<String>, client_id: Option<String>) -> Self {
        Self {
            endpoint: endpoint.unwrap_or_else(|| CODEX_REFRESH_ENDPOINT.to_owned()),
            client_id: client_id
                .filter(|client_id| !client_id.trim().is_empty())
                .unwrap_or_else(|| CODEX_CLIENT_ID.to_owned()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct HostCredentialService {
    inner: Rc<ServiceInner>,
}

impl fmt::Debug for HostCredentialService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("HostCredentialService").finish_non_exhaustive()
    }
}

impl HostCredentialService {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            inner: Rc::new(ServiceInner::new()),
        }
    }

    pub(crate) async fn prepare(
        &self,
        requirements: &HostInputRequirements,
    ) -> Result<BTreeMap<String, AgentInstanceCredentialState>, Error> {
        let Some(requirement) = requirements
            .files()
            .iter()
            .find(|requirement| requirement.managed_credential() == Some(ManagedHostCredential::Codex))
        else {
            return Ok(BTreeMap::new());
        };
        let path = resolve_host_input_file_source(requirement.source());
        let state = self.prepare_codex(path).await?;
        Ok(BTreeMap::from([(CODEX_CREDENTIAL.to_owned(), state)]))
    }

    async fn prepare_codex(&self, path: PathBuf) -> Result<AgentInstanceCredentialState, Error> {
        self.inner.start();
        let (respond, receive) = oneshot::channel();
        {
            let mut commands = self.inner.commands.borrow_mut();
            let commands = commands.as_mut().ok_or(Error::Unavailable)?;
            commands
                .try_send(Command::Prepare { path, respond })
                .map_err(|error| match error {
                    spsc::TrySendError::Full(_) => Error::Busy,
                    spsc::TrySendError::Disconnected(_) => Error::Unavailable,
                })?;
        }
        receive.await.map_err(|_| Error::Unavailable)?
    }
}

struct ServiceInner {
    commands: RefCell<Option<spsc::Sender<Command>>>,
    task: RefCell<Option<JoinHandle<()>>>,
}

impl ServiceInner {
    const fn new() -> Self {
        Self {
            commands: RefCell::new(None),
            task: RefCell::new(None),
        }
    }

    fn start(&self) {
        if self.commands.borrow().is_some() {
            return;
        }
        let (commands, receiver) = spsc::bounded(COMMAND_CAPACITY);
        let task = tokio::task::spawn_local(run_actor(receiver, CodexOAuthConfig::from_env()));
        *self.commands.borrow_mut() = Some(commands);
        *self.task.borrow_mut() = Some(task);
    }
}

impl Drop for ServiceInner {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().take() {
            task.abort();
        }
    }
}

enum Command {
    Prepare {
        path: PathBuf,
        respond: oneshot::Sender<Result<AgentInstanceCredentialState, Error>>,
    },
}

async fn run_actor(mut commands: spsc::Receiver<Command>, oauth: CodexOAuthConfig) {
    agentdp_crypto::install_default_provider();
    let client = reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build();
    let mut failures = BTreeMap::new();
    while let Ok(Command::Prepare { path, respond }) = commands.recv().await {
        let result = match &client {
            Ok(client) => prepare_codex_auth(client, &oauth, &path, &mut failures).await,
            Err(error) => Err(Error::BuildClient(error.to_string())),
        };
        respond.try_send(result);
    }
}

#[derive(Debug, Clone)]
struct CachedFailure {
    source_digest: [u8; 32],
    retry_at: Option<Instant>,
    message: String,
}

async fn prepare_codex_auth(
    client: &reqwest::Client,
    oauth: &CodexOAuthConfig,
    path: &Path,
    failures: &mut BTreeMap<PathBuf, CachedFailure>,
) -> Result<AgentInstanceCredentialState, Error> {
    let contents = tokio::fs::read(path).await.map_err(|source| Error::ReadAuth {
        path: path.to_path_buf(),
        source,
    })?;
    let digest = sha2::Sha256::digest(&contents).into();
    let auth = serde_json::from_slice::<serde_json::Value>(&contents).map_err(|source| Error::ParseAuth {
        path: path.to_path_buf(),
        source,
    })?;
    let snapshot = AuthSnapshot::read(&auth);
    let now = agentdp_platform::time::unix_seconds();
    if !snapshot.needs_refresh(now) {
        failures.remove(path);
        return Ok(snapshot.state(AgentInstanceCredentialPhase::Ready, None));
    }
    let Some(refresh_token) = snapshot.refresh_token.map(str::to_owned) else {
        return Ok(snapshot.failure_state(
            "Codex refresh token is missing; sign in again on the host".to_owned(),
            now,
        ));
    };
    if let Some(failure) = failures.get(path)
        && failure.source_digest == digest
        && failure.retry_at.is_none_or(|retry_at| Instant::now() < retry_at)
    {
        return Ok(snapshot.failure_state(failure.message.clone(), now));
    }

    match refresh_codex_token(client, oauth, &refresh_token).await {
        Ok(tokens) => {
            let latest_contents = tokio::fs::read(path).await.map_err(|source| Error::ReadAuth {
                path: path.to_path_buf(),
                source,
            })?;
            let mut latest_auth =
                serde_json::from_slice::<serde_json::Value>(&latest_contents).map_err(|source| Error::ParseAuth {
                    path: path.to_path_buf(),
                    source,
                })?;
            let latest = AuthSnapshot::read(&latest_auth);
            if latest.refresh_token != Some(refresh_token.as_str()) {
                failures.remove(path);
                return Ok(if latest.needs_refresh(now) {
                    latest.failure_state(
                        "Codex host auth changed during refresh; retrying from the new host state".to_owned(),
                        now,
                    )
                } else {
                    latest.state(AgentInstanceCredentialPhase::Ready, None)
                });
            }
            update_auth(&mut latest_auth, tokens)?;
            let mut serialized = serde_json::to_vec_pretty(&latest_auth).map_err(Error::SerializeAuth)?;
            serialized.push(b'\n');
            agentdp_platform::fs::write_atomic(path, &serialized, 0o600)
                .await
                .map_err(|source| Error::WriteAuth {
                    path: path.to_path_buf(),
                    source,
                })?;
            failures.remove(path);
            Ok(AuthSnapshot::read(&latest_auth).state(AgentInstanceCredentialPhase::Ready, None))
        }
        Err(failure) => {
            if failure.is_permanent()
                && let Some(state) = observe_concurrent_host_refresh(path, digest, now).await?
            {
                failures.remove(path);
                return Ok(state);
            }
            let message = failure.message();
            failures.insert(
                path.to_path_buf(),
                CachedFailure {
                    source_digest: digest,
                    retry_at: failure.is_transient().then(|| Instant::now() + TRANSIENT_RETRY_DELAY),
                    message: message.clone(),
                },
            );
            Ok(snapshot.failure_state(message, now))
        }
    }
}

async fn observe_concurrent_host_refresh(
    path: &Path,
    original_digest: [u8; 32],
    now: u64,
) -> Result<Option<AgentInstanceCredentialState>, Error> {
    let deadline = Instant::now() + CONCURRENT_REFRESH_OBSERVE_TIMEOUT;
    loop {
        match read_changed_host_auth(path, original_digest, now).await {
            Ok(Some(state)) => return Ok(Some(state)),
            Ok(None) => {}
            Err(error) => {
                let write_may_be_in_progress = matches!(&error, Error::ReadAuth { .. } | Error::ParseAuth { .. });
                if !write_may_be_in_progress || Instant::now() >= deadline {
                    return Err(error);
                }
            }
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(CONCURRENT_REFRESH_OBSERVE_INTERVAL).await;
    }
}

async fn read_changed_host_auth(
    path: &Path,
    original_digest: [u8; 32],
    now: u64,
) -> Result<Option<AgentInstanceCredentialState>, Error> {
    let contents = tokio::fs::read(path).await.map_err(|source| Error::ReadAuth {
        path: path.to_path_buf(),
        source,
    })?;
    if <[u8; 32]>::from(sha2::Sha256::digest(&contents)) == original_digest {
        return Ok(None);
    }
    let auth = serde_json::from_slice::<serde_json::Value>(&contents).map_err(|source| Error::ParseAuth {
        path: path.to_path_buf(),
        source,
    })?;
    let snapshot = AuthSnapshot::read(&auth);
    Ok(Some(if snapshot.needs_refresh(now) {
        snapshot.failure_state(
            "Codex host auth changed during refresh but still requires refresh".to_owned(),
            now,
        )
    } else {
        snapshot.state(AgentInstanceCredentialPhase::Ready, None)
    }))
}

struct AuthSnapshot<'a> {
    access_token: Option<&'a str>,
    refresh_token: Option<&'a str>,
    last_refresh: Option<&'a str>,
    expires_at: Option<u64>,
}

impl<'a> AuthSnapshot<'a> {
    fn read(auth: &'a serde_json::Value) -> Self {
        let tokens = auth.get("tokens");
        let access_token = tokens
            .and_then(|tokens| tokens.get("access_token"))
            .and_then(|value| value.as_str());
        Self {
            access_token,
            refresh_token: tokens
                .and_then(|tokens| tokens.get("refresh_token"))
                .and_then(|value| value.as_str())
                .filter(|value| !value.is_empty()),
            last_refresh: auth.get("last_refresh").and_then(|value| value.as_str()),
            expires_at: access_token.and_then(jwt_expiration),
        }
    }

    fn needs_refresh(&self, now: u64) -> bool {
        if self.access_token.is_none() {
            return false;
        }
        if let Some(expires_at) = self.expires_at {
            return expires_at <= now.saturating_add(REFRESH_WINDOW.as_secs());
        }
        self.last_refresh
            .and_then(rfc3339_unix_seconds)
            .is_none_or(|last_refresh| last_refresh.saturating_add(OPAQUE_TOKEN_REFRESH_AGE.as_secs()) <= now)
    }

    fn failure_state(&self, error: String, now: u64) -> AgentInstanceCredentialState {
        let phase = if self.expires_at.is_some_and(|expires_at| expires_at <= now) {
            AgentInstanceCredentialPhase::Expired
        } else {
            AgentInstanceCredentialPhase::RefreshFailed
        };
        self.state(phase, Some(error))
    }

    fn state(&self, phase: AgentInstanceCredentialPhase, error: Option<String>) -> AgentInstanceCredentialState {
        AgentInstanceCredentialState {
            phase,
            expires_at_unix_seconds: self.expires_at,
            last_refresh_at: self.last_refresh.map(str::to_owned),
            last_error: error,
        }
    }
}

fn jwt_expiration(token: &str) -> Option<u64> {
    let payload = token.split('.').nth(1)?;
    let mut encoded = payload.replace('-', "+").replace('_', "/").into_bytes();
    encoded.resize(encoded.len().div_ceil(4) * 4, b'=');
    let mut decoded = vec![0; agentdp_base64::decoded_len(&encoded)?];
    let length = agentdp_base64::decode(&encoded, &mut decoded)?;
    serde_json::from_slice::<serde_json::Value>(&decoded[..length])
        .ok()?
        .get("exp")?
        .as_u64()
}

fn rfc3339_unix_seconds(timestamp: &str) -> Option<u64> {
    let parsed = time::OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339).ok()?;
    u64::try_from(parsed.unix_timestamp()).ok()
}

#[derive(Serialize)]
struct RefreshRequest<'a> {
    client_id: &'a str,
    grant_type: &'static str,
    refresh_token: &'a str,
}

#[derive(Deserialize)]
// Field names intentionally mirror the OAuth response schema.
#[allow(clippy::struct_field_names)]
struct RefreshResponse {
    id_token: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
}

async fn refresh_codex_token(
    client: &reqwest::Client,
    oauth: &CodexOAuthConfig,
    refresh_token: &str,
) -> Result<RefreshResponse, RefreshFailure> {
    let response = client
        .post(&oauth.endpoint)
        .json(&RefreshRequest {
            client_id: &oauth.client_id,
            grant_type: "refresh_token",
            refresh_token,
        })
        .send()
        .await
        .map_err(|error| RefreshFailure::Transient(error.to_string()))?;
    let status = response.status();
    if status.is_success() {
        let response = response
            .json::<RefreshResponse>()
            .await
            .map_err(|error| RefreshFailure::Transient(format!("invalid success response: {error}")))?;
        if response.access_token.is_none() {
            return Err(RefreshFailure::Transient(
                "success response did not contain an access token".to_owned(),
            ));
        }
        return Ok(response);
    }
    let body = response.text().await.unwrap_or_default();
    let code = refresh_error_code(&body);
    if status == reqwest::StatusCode::UNAUTHORIZED
        || matches!(
            code.as_deref(),
            Some("refresh_token_expired" | "refresh_token_reused" | "refresh_token_invalidated")
        )
    {
        return Err(RefreshFailure::Permanent(
            code.unwrap_or_else(|| status.as_u16().to_string()),
        ));
    }
    Err(RefreshFailure::Transient(format!("HTTP {}", status.as_u16())))
}

fn refresh_error_code(body: &str) -> Option<String> {
    let body = serde_json::from_str::<serde_json::Value>(body).ok()?;
    match body.get("error") {
        Some(serde_json::Value::Object(error)) => error.get("code").and_then(|value| value.as_str()).map(str::to_owned),
        Some(serde_json::Value::String(code)) => Some(code.clone()),
        _ => body.get("code").and_then(|value| value.as_str()).map(str::to_owned),
    }
}

enum RefreshFailure {
    Permanent(String),
    Transient(String),
}

impl RefreshFailure {
    const fn is_permanent(&self) -> bool {
        matches!(self, Self::Permanent(_))
    }

    const fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }

    fn message(&self) -> String {
        match self {
            Self::Permanent(reason) => {
                format!("Codex OAuth refresh was rejected ({reason}); sign in again on the host")
            }
            Self::Transient(reason) => format!("Codex OAuth refresh failed ({reason})"),
        }
    }
}

fn update_auth(auth: &mut serde_json::Value, response: RefreshResponse) -> Result<(), Error> {
    let auth = auth.as_object_mut().ok_or(Error::InvalidAuthShape)?;
    let tokens = auth
        .get_mut("tokens")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or(Error::InvalidAuthShape)?;
    for (name, value) in [
        ("id_token", response.id_token),
        ("access_token", response.access_token),
        ("refresh_token", response.refresh_token),
    ] {
        if let Some(value) = value {
            tokens.insert(name.to_owned(), serde_json::Value::String(value));
        }
    }
    auth.insert(
        "last_refresh".to_owned(),
        serde_json::Value::String(agentdp_platform::time::rfc3339_utc_now()),
    );
    Ok(())
}

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("host credential refresh queue is full")]
    Busy,
    #[error("host credential refresh service is unavailable")]
    Unavailable,
    #[error("failed to build the Codex OAuth client: {0}")]
    BuildClient(String),
    #[error("failed to read Codex auth at {path}: {source}")]
    ReadAuth {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse Codex auth at {path}: {source}")]
    ParseAuth {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("Codex auth has an invalid token structure")]
    InvalidAuthShape,
    #[error("failed to serialize refreshed Codex auth: {0}")]
    SerializeAuth(serde_json::Error),
    #[error("failed to write refreshed Codex auth at {path}: {source}")]
    WriteAuth {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::{
        AgentInstanceCredentialPhase, AuthSnapshot, CODEX_CLIENT_ID, CODEX_REFRESH_ENDPOINT, CodexOAuthConfig,
        RefreshResponse, jwt_expiration, prepare_codex_auth, refresh_error_code, update_auth,
    };

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn reads_access_token_expiration() {
        let token = "header.eyJleHAiOjQxMDI0NDQ4MDB9.signature";
        assert_eq!(jwt_expiration(token), Some(4_102_444_800));
    }

    #[test]
    fn recognizes_nested_and_top_level_refresh_errors() {
        assert_eq!(
            refresh_error_code(r#"{"error":{"code":"refresh_token_reused"}}"#).as_deref(),
            Some("refresh_token_reused")
        );
        assert_eq!(
            refresh_error_code(r#"{"code":"refresh_token_expired"}"#).as_deref(),
            Some("refresh_token_expired")
        );
    }

    #[test]
    fn codex_oauth_config_uses_nonempty_overrides() {
        assert_eq!(
            CodexOAuthConfig::resolve(
                Some("https://example.test/token".to_owned()),
                Some("custom-client".to_owned()),
            ),
            CodexOAuthConfig {
                endpoint: "https://example.test/token".to_owned(),
                client_id: "custom-client".to_owned(),
            }
        );
        assert_eq!(
            CodexOAuthConfig::resolve(None, Some("  ".to_owned())),
            CodexOAuthConfig {
                endpoint: CODEX_REFRESH_ENDPOINT.to_owned(),
                client_id: CODEX_CLIENT_ID.to_owned(),
            }
        );
    }

    #[test]
    fn updates_only_tokens_returned_by_refresh() {
        let mut auth = serde_json::json!({
            "tokens": {
                "id_token": "old-id",
                "access_token": "old-access",
                "refresh_token": "old-refresh"
            }
        });
        update_auth(
            &mut auth,
            RefreshResponse {
                id_token: None,
                access_token: Some("new-access".to_owned()),
                refresh_token: None,
            },
        )
        .unwrap();
        assert_eq!(auth["tokens"]["id_token"], "old-id");
        assert_eq!(auth["tokens"]["access_token"], "new-access");
        assert_eq!(auth["tokens"]["refresh_token"], "old-refresh");
        assert!(auth["last_refresh"].is_string());
    }

    #[test]
    fn expired_access_token_reports_expired_after_refresh_failure() {
        let auth = serde_json::json!({
            "last_refresh": "2026-01-01T00:00:00Z",
            "tokens": {
                "access_token": "header.eyJleHAiOjEwMDB9.signature",
                "refresh_token": "refresh"
            }
        });
        let state = AuthSnapshot::read(&auth).failure_state("refresh failed".to_owned(), 1_001);
        assert_eq!(state.phase, AgentInstanceCredentialPhase::Expired);
        assert_eq!(state.expires_at_unix_seconds, Some(1_000));
    }

    #[test]
    fn jwt_access_token_refreshes_within_one_hour() {
        let auth = serde_json::json!({
            "last_refresh": "2026-01-01T00:00:00Z",
            "tokens": {
                "access_token": "header.eyJleHAiOjQxMDI0NDQ4MDB9.signature",
                "refresh_token": "refresh"
            }
        });
        let snapshot = AuthSnapshot::read(&auth);

        assert!(!snapshot.needs_refresh(4_102_444_800 - Duration::from_mins(61).as_secs()));
        assert!(snapshot.needs_refresh(4_102_444_800 - Duration::from_hours(1).as_secs()));
    }

    #[tokio::test]
    async fn refreshes_expiring_access_token_and_persists_rotated_tokens() {
        agentdp_crypto::install_default_provider();
        let auth_path = temp_auth_path("refresh-success");
        write_expired_auth(&auth_path).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/oauth/token", listener.local_addr().unwrap());
        let oauth = CodexOAuthConfig::resolve(Some(endpoint), None);
        let response = serde_json::json!({
            "id_token": "new-id",
            "access_token": "header.eyJleHAiOjQxMDI0NDQ4MDB9.signature",
            "refresh_token": "new-refresh"
        })
        .to_string();
        let server = serve_once(&listener, "200 OK", &response);
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let refresh = async {
            prepare_codex_auth(&client, &oauth, &auth_path, &mut BTreeMap::new())
                .await
                .unwrap()
        };
        let ((), state) = tokio::join!(server, refresh);

        assert_eq!(state.phase, AgentInstanceCredentialPhase::Ready);
        assert_eq!(state.expires_at_unix_seconds, Some(4_102_444_800));
        let persisted: serde_json::Value = serde_json::from_slice(&tokio::fs::read(&auth_path).await.unwrap()).unwrap();
        assert_eq!(persisted["tokens"]["id_token"], "new-id");
        assert_eq!(persisted["tokens"]["refresh_token"], "new-refresh");
        assert!(persisted["last_refresh"].is_string());
        let _removed = tokio::fs::remove_dir_all(auth_path.parent().unwrap()).await;
    }

    #[tokio::test]
    async fn permanent_failure_is_not_retried_until_host_auth_changes() {
        agentdp_crypto::install_default_provider();
        let auth_path = temp_auth_path("refresh-rejected");
        write_expired_auth(&auth_path).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/oauth/token", listener.local_addr().unwrap());
        let oauth = CodexOAuthConfig::resolve(Some(endpoint), None);
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let server = async {
            serve_once(
                &listener,
                "401 Unauthorized",
                r#"{"error":{"code":"refresh_token_reused"}}"#,
            )
            .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
        };
        let refresh = async {
            let mut failures = BTreeMap::new();
            let first = prepare_codex_auth(&client, &oauth, &auth_path, &mut failures)
                .await
                .unwrap();
            let second = prepare_codex_auth(&client, &oauth, &auth_path, &mut failures)
                .await
                .unwrap();
            (first, second)
        };
        let ((), (first, second)) = tokio::join!(server, refresh);

        assert_eq!(first.phase, AgentInstanceCredentialPhase::Expired);
        assert_eq!(second, first);
        assert!(
            second
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("sign in again on the host"))
        );
        let _removed = tokio::fs::remove_dir_all(auth_path.parent().unwrap()).await;
    }

    #[tokio::test]
    async fn does_not_overwrite_a_concurrent_host_refresh() {
        agentdp_crypto::install_default_provider();
        let auth_path = temp_auth_path("concurrent-host-refresh");
        write_expired_auth(&auth_path).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/oauth/token", listener.local_addr().unwrap());
        let oauth = CodexOAuthConfig::resolve(Some(endpoint), None);
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let server_path = auth_path.clone();
        let server = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _length = stream.read(&mut request).await.unwrap();
            tokio::fs::write(
                &server_path,
                br#"{"last_refresh":"2026-08-05T10:00:00Z","tokens":{"id_token":"host-id","access_token":"header.eyJleHAiOjQxMDI0NDQ4MDB9.host","refresh_token":"host-refresh"}}"#,
            )
            .await
            .unwrap();
            let body = r#"{"id_token":"agentdp-id","access_token":"header.eyJleHAiOjQxMDI0NDQ4MDB9.agentdp","refresh_token":"agentdp-refresh"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        };
        let refresh = async {
            prepare_codex_auth(&client, &oauth, &auth_path, &mut BTreeMap::new())
                .await
                .unwrap()
        };
        let ((), state) = tokio::join!(server, refresh);

        assert_eq!(state.phase, AgentInstanceCredentialPhase::Ready);
        let persisted: serde_json::Value = serde_json::from_slice(&tokio::fs::read(&auth_path).await.unwrap()).unwrap();
        assert_eq!(persisted["tokens"]["id_token"], "host-id");
        assert_eq!(persisted["tokens"]["refresh_token"], "host-refresh");
        let _removed = tokio::fs::remove_dir_all(auth_path.parent().unwrap()).await;
    }

    #[tokio::test]
    async fn adopts_refresh_persisted_by_host_after_reused_response() {
        agentdp_crypto::install_default_provider();
        let auth_path = temp_auth_path("concurrent-host-refresh-reused");
        write_expired_auth(&auth_path).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/oauth/token", listener.local_addr().unwrap());
        let oauth = CodexOAuthConfig::resolve(Some(endpoint), None);
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let server_path = auth_path.clone();
        let server = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _length = stream.read(&mut request).await.unwrap();
            let body = r#"{"error":{"code":"refresh_token_reused"}}"#;
            let response = format!(
                "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.flush().await.unwrap();
            tokio::fs::write(&server_path, b"").await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            tokio::fs::write(
                &server_path,
                br#"{"last_refresh":"2026-08-05T10:00:00Z","tokens":{"id_token":"host-id","access_token":"header.eyJleHAiOjQxMDI0NDQ4MDB9.host","refresh_token":"host-refresh"}}"#,
            )
            .await
            .unwrap();
        };
        let refresh = async {
            prepare_codex_auth(&client, &oauth, &auth_path, &mut BTreeMap::new())
                .await
                .unwrap()
        };
        let ((), state) = tokio::join!(server, refresh);

        assert_eq!(state.phase, AgentInstanceCredentialPhase::Ready);
        assert_eq!(state.expires_at_unix_seconds, Some(4_102_444_800));
        let _removed = tokio::fs::remove_dir_all(auth_path.parent().unwrap()).await;
    }

    async fn serve_once(listener: &tokio::net::TcpListener, status: &str, body: &str) {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 4096];
        let length = stream.read(&mut request).await.unwrap();
        assert!(String::from_utf8_lossy(&request[..length]).starts_with("POST /oauth/token HTTP/1.1"));
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    }

    async fn write_expired_auth(path: &PathBuf) {
        tokio::fs::create_dir_all(path.parent().unwrap()).await.unwrap();
        tokio::fs::write(
            path,
            br#"{"last_refresh":"2026-01-01T00:00:00Z","tokens":{"id_token":"old-id","access_token":"header.eyJleHAiOjEwMDB9.signature","refresh_token":"old-refresh"}}"#,
        )
        .await
        .unwrap();
    }

    fn temp_auth_path(label: &str) -> PathBuf {
        std::env::temp_dir()
            .join(format!(
                "agentdp-credentials-{label}-{}-{}",
                std::process::id(),
                NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
            ))
            .join("auth.json")
    }
}
