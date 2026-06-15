pub const DEFAULT_CA_ENV_VARS: &[&str] = &[
    "NODE_EXTRA_CA_CERTS",
    "NPM_CONFIG_CAFILE",
    "SSL_CERT_FILE",
    "GIT_SSL_CAINFO",
    "CURL_CA_BUNDLE",
    "REQUESTS_CA_BUNDLE",
];

pub const CA_ENV_VARS_KEY: &str = "AGENTDP_CA_ENV_VARS";

#[must_use]
pub fn default_ca_env_vars_csv() -> String {
    ca_env_vars_csv(default_ca_env_vars())
}

#[must_use]
pub fn default_ca_env_vars() -> Vec<String> {
    DEFAULT_CA_ENV_VARS.iter().map(|key| (*key).to_owned()).collect()
}

#[must_use]
pub fn ca_env_vars_with_extra(extra: &[String]) -> Vec<String> {
    let mut vars = default_ca_env_vars();
    for key in extra {
        if !vars.iter().any(|existing| existing == key) {
            vars.push(key.clone());
        }
    }
    vars
}

#[must_use]
pub fn ca_env_vars_csv(vars: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    vars.into_iter()
        .map(|value| value.as_ref().to_owned())
        .collect::<Vec<_>>()
        .join(",")
}

#[must_use]
pub fn ca_env_vars_from_env() -> Vec<String> {
    let Some(value) = std::env::var_os(CA_ENV_VARS_KEY) else {
        return default_ca_env_vars();
    };
    let value = value.to_string_lossy();
    let vars = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if vars.is_empty() { default_ca_env_vars() } else { vars }
}
