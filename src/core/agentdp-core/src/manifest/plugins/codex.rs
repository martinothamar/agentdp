use serde::{Deserialize, Serialize};

use crate::provisioning::host_input::{
    Error as HostInputError, HostInputFile, HostInputFileSource, HostInputGuestPath, HostInputRequirements,
    HostInputTransform, MaterializationContext, MaterializedHostInput,
};
use crate::provisioning::secrets::{SecretBinding, SecretBindings};

use super::AuthMode;

const CODEX_AUTH_PATH_ENV: &str = "AGENTDP_CODEX_AUTH_PATH";
const CODEX_HOME_ENV: &str = "CODEX_HOME";
const CODEX_AUTH_GUEST_PATH: &str = ".codex/auth.json";
const CODEX_AUTH_HOME_PATH: &str = "auth.json";
const CODEX_AUTH_DEFAULT_HOME_PATH: &str = ".codex/auth.json";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Codex {
    #[serde(default)]
    pub yolo: bool,
    pub auth: AuthMode,
    pub auth_source: Option<CodexAuthSource>,
}

impl Codex {
    pub(super) fn host_input_requirements(&self, requirements: &mut HostInputRequirements) {
        match self.auth {
            AuthMode::Mediated => match self.auth_source.unwrap_or_default() {
                CodexAuthSource::HostAuth => {
                    requirements.add_file(HostInputFile::with_transform(
                        "Codex auth",
                        codex_auth_source(),
                        codex_auth_guest_path(),
                        "0600",
                        &CODEX_AUTH_TRANSFORM,
                    ));
                }
                CodexAuthSource::Env => requirements.allow_mediated_secret_hosts(
                    ["OPENAI_API_KEY", "CODEX_API_KEY"],
                    ["api.openai.com", "chatgpt.com"],
                ),
            },
            AuthMode::CopyFromHost => {
                requirements.copy_custom_env(["OPENAI_API_KEY", "CODEX_API_KEY"]);
                requirements.add_file(HostInputFile::copy(
                    "Codex auth",
                    codex_auth_source(),
                    codex_auth_guest_path(),
                    "0600",
                ));
            }
        }
    }
}

static CODEX_AUTH_TRANSFORM: CodexAuthTransform = CodexAuthTransform;

struct CodexAuthTransform;

impl HostInputTransform for CodexAuthTransform {
    fn name(&self) -> &'static str {
        "codex-auth"
    }

    fn produces_secrets(&self) -> bool {
        true
    }

    fn materialize(
        &self,
        label: &str,
        contents: &[u8],
        context: MaterializationContext<'_>,
    ) -> Result<MaterializedHostInput, HostInputError> {
        materialize_mediated_auth(label, contents, context)
    }
}

fn codex_auth_guest_path() -> HostInputGuestPath {
    HostInputGuestPath::AgentHomeRelative(CODEX_AUTH_GUEST_PATH.to_owned())
}

fn codex_auth_source() -> HostInputFileSource {
    HostInputFileSource::HomeRelative {
        path_env: Some(CODEX_AUTH_PATH_ENV.to_owned()),
        home_env: Some(CODEX_HOME_ENV.to_owned()),
        home_relative_path: CODEX_AUTH_HOME_PATH.to_owned(),
        default_home_relative_path: CODEX_AUTH_DEFAULT_HOME_PATH.to_owned(),
    }
}

fn materialize_mediated_auth(
    label: &str,
    contents: &[u8],
    context: MaterializationContext<'_>,
) -> Result<MaterializedHostInput, HostInputError> {
    let mut json =
        serde_json::from_slice::<serde_json::Value>(contents).map_err(|source| materialize_error(label, source))?;
    let mut secrets = SecretBindings::default();
    placeholderize_auth_value(&mut json, false, "CODEX_AUTH", context, &mut secrets)?;
    let contents = serde_json::to_vec_pretty(&json).map_err(|source| materialize_error(label, source))?;
    Ok(MaterializedHostInput { contents, secrets })
}

fn materialize_error(label: &str, source: impl std::error::Error + Send + Sync + 'static) -> HostInputError {
    HostInputError::Materialize {
        label: label.to_owned(),
        materializer: "codex-auth",
        source: Box::new(source),
    }
}

fn placeholderize_auth_value(
    value: &mut serde_json::Value,
    sensitive_context: bool,
    name_path: &str,
    context: MaterializationContext<'_>,
    secrets: &mut SecretBindings,
) -> Result<(), HostInputError> {
    match value {
        serde_json::Value::Object(object) => {
            for (key, child) in object {
                let child_sensitive = sensitive_context || is_auth_secret_key(key);
                let child_name = format!("{name_path}_{}", secret_name_component(key));
                placeholderize_auth_value(child, child_sensitive, &child_name, context, secrets)?;
            }
        }
        serde_json::Value::Array(values) => {
            for (index, child) in values.iter_mut().enumerate() {
                placeholderize_auth_value(
                    child,
                    sensitive_context,
                    &format!("{name_path}_{index}"),
                    context,
                    secrets,
                )?;
            }
        }
        serde_json::Value::String(secret) if sensitive_context && !secret.is_empty() => {
            let placeholder = auth_placeholder(name_path, context)?;
            let binding = SecretBinding::new_with_placeholder(
                name_path,
                placeholder,
                std::mem::take(secret),
                &codex_auth_hosts(),
            )?;
            secret.clone_from(&binding.placeholder);
            secrets.insert(binding);
        }
        _ => {}
    }
    Ok(())
}

fn auth_placeholder(name_path: &str, context: MaterializationContext<'_>) -> Result<Option<String>, HostInputError> {
    if let Some(placeholder) = context.placeholder_for_name(name_path) {
        return Ok(Some(placeholder.to_owned()));
    }
    if !name_path.ends_with("_ID_TOKEN") {
        return Ok(None);
    }
    let placeholder = SecretBinding::new(name_path, "placeholder-value", &[])?.placeholder;
    Ok(Some(format!(
        "{JWT_PLACEHOLDER_HEADER}.{JWT_PLACEHOLDER_PAYLOAD}.{placeholder}"
    )))
}

const JWT_PLACEHOLDER_HEADER: &str = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0";
const JWT_PLACEHOLDER_PAYLOAD: &str = "eyJzdWIiOiJhZ2VudGRwLW1lZGlhdGVkIiwiZW1haWwiOiJhZ2VudGRwQGV4YW1wbGUuaW52YWxpZCIsImV4cCI6NDEwMjQ0NDgwMCwiaWF0IjowLCJpc3MiOiJhZ2VudGRwLW1lZGlhdGVkIn0";

fn is_auth_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    matches!(
        key.as_str(),
        "token" | "tokens" | "access_token" | "refresh_token" | "id_token" | "api_key"
    ) || key.ends_with("_token")
        || key.ends_with("token")
}

fn codex_auth_hosts() -> Vec<String> {
    ["api.openai.com", "chatgpt.com"]
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn secret_name_component(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CodexAuthSource {
    #[default]
    HostAuth,
    Env,
}
