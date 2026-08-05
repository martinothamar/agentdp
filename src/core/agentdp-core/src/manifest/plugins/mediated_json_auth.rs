use crate::provisioning::host_input::{
    Error as HostInputError, HostInputTransform, ManagedHostCredential, MaterializationContext, MaterializedHostInput,
};
use crate::provisioning::secrets::{SecretBinding, SecretBindings};

/// Transforms a JSON auth file by replacing secret values with mediated
/// placeholders bound to the given hosts.
pub(super) struct MediatedJsonAuthTransform {
    pub(super) name: &'static str,
    pub(super) prefix: &'static str,
    pub(super) hosts: &'static [&'static str],
    pub(super) normalize_expiry: bool,
    pub(super) jwt_access_token_placeholder: bool,
    pub(super) omit_refresh_token: bool,
    pub(super) managed_credential: Option<ManagedHostCredential>,
}

impl HostInputTransform for MediatedJsonAuthTransform {
    fn name(&self) -> &'static str {
        self.name
    }

    fn produces_secrets(&self) -> bool {
        true
    }

    fn managed_credential(&self) -> Option<ManagedHostCredential> {
        self.managed_credential
    }

    fn materialize(
        &self,
        label: &str,
        contents: &[u8],
        context: MaterializationContext<'_>,
    ) -> Result<MaterializedHostInput, HostInputError> {
        let mut json = serde_json::from_slice::<serde_json::Value>(contents)
            .map_err(|source| self.materialize_error(label, source))?;
        let mut secrets = SecretBindings::default();
        self.placeholderize_auth_value(&mut json, false, self.prefix, context, &mut secrets)?;
        let contents = serde_json::to_vec_pretty(&json).map_err(|source| self.materialize_error(label, source))?;
        Ok(MaterializedHostInput { contents, secrets })
    }
}

impl MediatedJsonAuthTransform {
    fn materialize_error(&self, label: &str, source: impl std::error::Error + Send + Sync + 'static) -> HostInputError {
        HostInputError::Materialize {
            label: label.to_owned(),
            materializer: self.name,
            source: Box::new(source),
        }
    }

    fn placeholderize_auth_value(
        &self,
        value: &mut serde_json::Value,
        sensitive_context: bool,
        name_path: &str,
        context: MaterializationContext<'_>,
        secrets: &mut SecretBindings,
    ) -> Result<(), HostInputError> {
        match value {
            serde_json::Value::Object(object) => {
                for (key, child) in object {
                    if self.omit_refresh_token && key.eq_ignore_ascii_case("refresh_token") {
                        *child = serde_json::Value::String(String::new());
                        continue;
                    }
                    if self.normalize_expiry && is_auth_expiry_key(key) && normalize_expiry_value(child) {
                        continue;
                    }
                    let child_sensitive = sensitive_context || is_auth_secret_key(key);
                    let child_name = format!("{name_path}_{}", secret_name_component(key));
                    self.placeholderize_auth_value(child, child_sensitive, &child_name, context, secrets)?;
                }
            }
            serde_json::Value::Array(values) => {
                for (index, child) in values.iter_mut().enumerate() {
                    self.placeholderize_auth_value(
                        child,
                        sensitive_context,
                        &format!("{name_path}_{index}"),
                        context,
                        secrets,
                    )?;
                }
            }
            serde_json::Value::String(secret) if sensitive_context && !secret.is_empty() => {
                let jwt_placeholder = name_path.ends_with("_ID_TOKEN")
                    || (self.jwt_access_token_placeholder && name_path.ends_with("_ACCESS_TOKEN"));
                let placeholder = auth_placeholder(name_path, context, jwt_placeholder)?;
                let binding = SecretBinding::new_with_placeholder(
                    name_path,
                    placeholder,
                    std::mem::take(secret),
                    &self.auth_hosts(),
                )?;
                secret.clone_from(&binding.placeholder);
                secrets.insert(binding);
            }
            _ => {}
        }
        Ok(())
    }

    fn auth_hosts(&self) -> Vec<String> {
        self.hosts.iter().map(|host| (*host).to_owned()).collect()
    }
}

fn auth_placeholder(
    name_path: &str,
    context: MaterializationContext<'_>,
    jwt_placeholder: bool,
) -> Result<Option<String>, HostInputError> {
    if let Some(placeholder) = context.placeholder_for_name(name_path) {
        return Ok(Some(if jwt_placeholder && !looks_like_jwt(placeholder) {
            jwt_placeholder_for(placeholder)
        } else {
            placeholder.to_owned()
        }));
    }
    if !jwt_placeholder {
        return Ok(None);
    }
    let placeholder = SecretBinding::new(name_path, "placeholder-value", &[])?.placeholder;
    Ok(Some(jwt_placeholder_for(&placeholder)))
}

fn jwt_placeholder_for(signature: &str) -> String {
    format!("{JWT_PLACEHOLDER_HEADER}.{JWT_PLACEHOLDER_PAYLOAD}.{signature}")
}

fn looks_like_jwt(value: &str) -> bool {
    value.split('.').count() == 3
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

fn is_auth_expiry_key(key: &str) -> bool {
    matches!(
        key,
        "expires" | "expires_at" | "expiresAt" | "expiry" | "expiration" | "expiration_time" | "expirationTime"
    )
}

fn normalize_expiry_value(value: &mut serde_json::Value) -> bool {
    const NON_EXPIRING_UNIX_SECONDS: u64 = 4_102_444_800;
    match value {
        serde_json::Value::Number(_) => {
            *value = serde_json::Value::Number(serde_json::Number::from(NON_EXPIRING_UNIX_SECONDS));
            true
        }
        serde_json::Value::String(text) => {
            *text = NON_EXPIRING_UNIX_SECONDS.to_string();
            true
        }
        _ => false,
    }
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
