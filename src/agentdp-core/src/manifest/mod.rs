use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Deserializer};
use thiserror::Error;

use crate::Context;

pub mod plugins;

use plugins::Plugins;

const DEFAULT_MANIFEST_NAMES: [&str; 2] = ["agent.yaml", "agent.yml"];

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to read manifest {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse manifest {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("manifest {path} is invalid:\n{errors}")]
    Invalid { path: PathBuf, errors: ValidationErrors },
}

#[derive(Debug, Error)]
pub enum PathError {
    #[error("manifest path does not exist: {0}")]
    ExplicitMissing(PathBuf),
    #[error("no agent.yaml or agent.yml found in {0}")]
    MissingDefault(PathBuf),
    #[error("both agent.yaml and agent.yml exist in {0}; pass -f to choose one")]
    AmbiguousDefault(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationErrors {
    messages: Vec<String>,
}

impl ValidationErrors {
    const fn new(messages: Vec<String>) -> Self {
        Self { messages }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    #[must_use]
    pub fn messages(&self) -> &[String] {
        &self.messages
    }
}

impl Display for ValidationErrors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for message in &self.messages {
            writeln!(formatter, "- {message}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentManifest {
    pub version: u16,
    pub name: String,
    pub image: Image,
    pub user: AgentUser,
    pub resources: Resources,
    pub network: Network,
    pub bootstrap: Bootstrap,
    #[serde(default)]
    pub plugins: Plugins,
}

impl AgentManifest {
    /// Validates the semantic constraints that are not fully expressed by YAML typing.
    ///
    /// # Errors
    ///
    /// Returns all validation failures found in the manifest.
    pub fn validate(&self) -> Result<(), ValidationErrors> {
        let mut errors = Vec::new();

        if self.version != 1 {
            errors.push(format!("version must be 1, got {}", self.version));
        }
        validate_identifier("name", &self.name, &mut errors);
        self.image.validate(&mut errors);
        self.user.validate(&mut errors);
        self.resources.validate(&mut errors);
        self.network.validate(&mut errors);
        self.bootstrap.validate(&mut errors);
        self.plugins.validate(&mut errors);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationErrors::new(errors))
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Image {
    pub os: GuestOs,
}

impl Image {
    fn validate(&self, errors: &mut Vec<String>) {
        if self.os != GuestOs::Archlinux {
            errors.push("image.os must be archlinux".to_owned());
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum GuestOs {
    Archlinux,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentUser {
    pub name: String,
    #[serde(default)]
    pub groups: Vec<String>,
}

impl AgentUser {
    fn validate(&self, errors: &mut Vec<String>) {
        validate_identifier("user.name", &self.name, errors);
        if self.name == "root" {
            errors.push("user.name must not be root".to_owned());
        }
        validate_non_empty_values("user.groups", &self.groups, errors);
        for (index, group) in self.groups.iter().enumerate() {
            validate_identifier(&format!("user.groups[{index}]"), group, errors);
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub cpus: u16,
    pub memory: String,
    pub storage: String,
}

impl Resources {
    fn validate(&self, errors: &mut Vec<String>) {
        if self.cpus == 0 {
            errors.push("resources.cpus must be greater than zero".to_owned());
        }
        validate_size("resources.memory", &self.memory, errors);
        validate_size("resources.storage", &self.storage, errors);
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Network {
    pub mode: NetworkMode,
    #[serde(default)]
    pub ports: BTreeMap<String, GuestPort>,
    #[serde(default)]
    pub allow: NetworkAllow,
    #[serde(default)]
    pub host_aliases: Vec<HostAlias>,
}

impl Network {
    fn validate(&self, errors: &mut Vec<String>) {
        if self.mode != NetworkMode::User {
            errors.push("network.mode must be user".to_owned());
        }

        if self.ports.is_empty() {
            errors.push("network.ports must declare at least one named guest port".to_owned());
        }

        for (name, port) in &self.ports {
            validate_identifier("network.ports key", name, errors);
            port.validate(name, errors);
        }

        self.allow.validate(errors);
        for (index, alias) in self.host_aliases.iter().enumerate() {
            alias.validate(index, errors);
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostAlias {
    pub address: String,
    #[serde(default)]
    pub names: Vec<String>,
}

impl HostAlias {
    fn validate(&self, index: usize, errors: &mut Vec<String>) {
        let field = format!("network.host_aliases[{index}]");
        validate_non_empty(&format!("{field}.address"), &self.address, errors);
        validate_non_empty_values(&format!("{field}.names"), &self.names, errors);
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkMode {
    User,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkAllow {
    Hosts(Vec<String>),
    All,
}

impl Default for NetworkAllow {
    fn default() -> Self {
        Self::Hosts(Vec::new())
    }
}

impl NetworkAllow {
    fn validate(&self, errors: &mut Vec<String>) {
        match self {
            Self::Hosts(hosts) => validate_non_empty_values("network.allow", hosts, errors),
            Self::All => {}
        }
    }
}

impl<'de> Deserialize<'de> for NetworkAllow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match RawNetworkAllow::deserialize(deserializer)? {
            RawNetworkAllow::Keyword(keyword) if keyword == "all" => Ok(Self::All),
            RawNetworkAllow::Keyword(keyword) => Err(serde::de::Error::custom(format!(
                "network.allow must be `all` or a list of hosts, got `{keyword}`"
            ))),
            RawNetworkAllow::Hosts(hosts) => Ok(Self::Hosts(hosts)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawNetworkAllow {
    Keyword(String),
    Hosts(Vec<String>),
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GuestPort {
    pub guest: u16,
    pub protocol: NetworkProtocol,
}

impl GuestPort {
    fn validate(&self, name: &str, errors: &mut Vec<String>) {
        if self.guest == 0 {
            errors.push(format!("network.ports.{name}.guest must be greater than zero"));
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    #[serde(default)]
    pub packages: Vec<String>,
    #[serde(default)]
    pub repos: Vec<Repo>,
    #[serde(default)]
    pub shell: Vec<String>,
    #[serde(default)]
    pub healthchecks: Vec<Healthcheck>,
}

impl Bootstrap {
    fn validate(&self, errors: &mut Vec<String>) {
        validate_non_empty_values("bootstrap.packages", &self.packages, errors);
        validate_non_empty_values("bootstrap.shell", &self.shell, errors);

        for (index, repo) in self.repos.iter().enumerate() {
            repo.validate(index, errors);
        }

        for (index, healthcheck) in self.healthchecks.iter().enumerate() {
            healthcheck.validate(index, errors);
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Repo {
    pub name: Option<String>,
    pub url: String,
    pub path: Option<String>,
    pub upstream: Option<String>,
}

impl Repo {
    fn validate(&self, index: usize, errors: &mut Vec<String>) {
        let field = format!("bootstrap.repos[{index}]");
        if let Some(name) = &self.name {
            validate_identifier(&format!("{field}.name"), name, errors);
        }
        validate_non_empty(&format!("{field}.url"), &self.url, errors);
        if let Some(path) = &self.path {
            validate_relative_path(&format!("{field}.path"), path, errors);
        }
        if let Some(upstream) = &self.upstream {
            validate_non_empty(&format!("{field}.upstream"), upstream, errors);
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Healthcheck {
    pub name: String,
    pub command: Option<String>,
    pub tcp: Option<String>,
    pub timeout: Option<String>,
}

impl Healthcheck {
    fn validate(&self, index: usize, errors: &mut Vec<String>) {
        let field = format!("bootstrap.healthchecks[{index}]");
        validate_identifier(&format!("{field}.name"), &self.name, errors);

        match (self.command.as_ref(), self.tcp.as_ref()) {
            (Some(command), None) => validate_non_empty(&format!("{field}.command"), command, errors),
            (None, Some(tcp)) => validate_tcp_target(&format!("{field}.tcp"), tcp, errors),
            (None, None) => errors.push(format!("{field} must define command or tcp")),
            (Some(_), Some(_)) => errors.push(format!("{field} must define only one of command or tcp")),
        }

        if let Some(timeout) = &self.timeout {
            validate_duration(&format!("{field}.timeout"), timeout, errors);
        }
    }
}

/// Loads, parses, and validates an agent manifest.
///
/// # Errors
///
/// Returns an error if the file cannot be read, the YAML cannot be parsed, or
/// the parsed manifest fails validation.
pub fn load_manifest(context: &Context, path: impl AsRef<Path>) -> Result<AgentManifest, Error> {
    let path = path.as_ref();
    context
        .logger()
        .verbose_with(|| format!("reading manifest {}", path.display()));
    let contents = fs::read_to_string(path).map_err(|source| Error::Read {
        path: path.to_path_buf(),
        source,
    })?;
    context
        .logger()
        .verbose_with(|| format!("parsing manifest {}", path.display()));
    let manifest = serde_yaml::from_str::<AgentManifest>(&contents).map_err(|source| Error::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    if let Err(errors) = manifest.validate() {
        context.logger().verbose_with(|| {
            format!(
                "manifest {} failed validation with {} errors",
                path.display(),
                errors.messages().len()
            )
        });
        return Err(Error::Invalid {
            path: path.to_path_buf(),
            errors,
        });
    }
    context
        .logger()
        .verbose_with(|| format!("manifest {} parsed and validated", path.display()));
    Ok(manifest)
}

/// Resolves an explicit manifest path or searches for the default manifest names.
///
/// # Errors
///
/// Returns an error when the explicit file does not exist, no default manifest
/// exists, or both default manifest names are present.
pub fn resolve_manifest_path(context: &Context, explicit: Option<&Path>, cwd: &Path) -> Result<PathBuf, PathError> {
    if let Some(path) = explicit {
        context
            .logger()
            .verbose_with(|| format!("resolving explicit manifest path {}", path.display()));
        let resolved = if path.is_absolute() {
            path.to_path_buf()
        } else {
            cwd.join(path)
        };
        if resolved.exists() {
            context
                .logger()
                .verbose_with(|| format!("resolved explicit manifest path {}", resolved.display()));
            return Ok(resolved);
        }
        context
            .logger()
            .verbose_with(|| format!("explicit manifest path does not exist: {}", resolved.display()));
        return Err(PathError::ExplicitMissing(resolved));
    }

    context
        .logger()
        .verbose_with(|| format!("searching for default manifest in {}", cwd.display()));
    let matches = DEFAULT_MANIFEST_NAMES
        .iter()
        .map(|name| cwd.join(name))
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    context.logger().verbose_with(|| {
        let matches = matches
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        if matches.is_empty() {
            "default manifest search found no matches".to_owned()
        } else {
            format!("default manifest search found: {matches}")
        }
    });

    match matches.as_slice() {
        [path] => Ok(path.clone()),
        [] => Err(PathError::MissingDefault(cwd.to_path_buf())),
        _ => Err(PathError::AmbiguousDefault(cwd.to_path_buf())),
    }
}

fn validate_identifier(field: &str, value: &str, errors: &mut Vec<String>) {
    validate_non_empty(field, value, errors);

    let valid = value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if !valid {
        errors.push(format!(
            "{field} may contain only ASCII letters, digits, '.', '_', and '-'"
        ));
    }
}

fn validate_non_empty_values(field: &str, values: &[String], errors: &mut Vec<String>) {
    for (index, value) in values.iter().enumerate() {
        validate_non_empty(&format!("{field}[{index}]"), value, errors);
    }
}

fn validate_non_empty(field: &str, value: &str, errors: &mut Vec<String>) {
    if value.trim().is_empty() {
        errors.push(format!("{field} must not be empty"));
    }
}

fn validate_size(field: &str, value: &str, errors: &mut Vec<String>) {
    let value = value.trim();
    validate_non_empty(field, value, errors);

    let digit_count = value.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        errors.push(format!("{field} must start with a positive integer"));
        return;
    }

    let number = &value[..digit_count];
    if number.parse::<u64>().map_or(true, |parsed| parsed == 0) {
        errors.push(format!("{field} must be greater than zero"));
    }

    let suffix = value[digit_count..].to_ascii_uppercase();
    let valid_suffix = matches!(
        suffix.as_str(),
        "" | "K" | "M" | "G" | "T" | "KB" | "MB" | "GB" | "TB" | "KIB" | "MIB" | "GIB" | "TIB"
    );
    if !valid_suffix {
        errors.push(format!("{field} has unsupported size suffix: {suffix}"));
    }
}

fn validate_relative_path(field: &str, value: &str, errors: &mut Vec<String>) {
    validate_non_empty(field, value, errors);

    let path = Path::new(value);
    if path.is_absolute() || value.starts_with('/') || value.starts_with('\\') {
        errors.push(format!("{field} must be relative"));
        return;
    }

    let invalid = path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    });
    if invalid {
        errors.push(format!("{field} must not contain '..' or a root prefix"));
    }
}

fn validate_tcp_target(field: &str, value: &str, errors: &mut Vec<String>) {
    validate_non_empty(field, value, errors);

    let Some((host, port)) = value.rsplit_once(':') else {
        errors.push(format!("{field} must be host:port"));
        return;
    };

    if host.trim().is_empty() {
        errors.push(format!("{field} host must not be empty"));
    }

    match port.parse::<u16>() {
        Ok(0) => errors.push(format!("{field} port must be greater than zero")),
        Ok(_) => {}
        Err(_) => errors.push(format!("{field} port must be a number from 1 to 65535")),
    }
}

fn validate_duration(field: &str, value: &str, errors: &mut Vec<String>) {
    let value = value.trim();
    validate_non_empty(field, value, errors);

    let digit_count = value.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        errors.push(format!("{field} must start with a positive integer"));
        return;
    }

    let number = &value[..digit_count];
    if number.parse::<u64>().map_or(true, |parsed| parsed == 0) {
        errors.push(format!("{field} must be greater than zero"));
    }

    let suffix = &value[digit_count..];
    if !matches!(suffix, "s" | "m" | "h") {
        errors.push(format!("{field} must use an s, m, or h suffix"));
    }
}

#[cfg(test)]
mod tests {
    use agentdp_test_support::manifest;

    use super::plugins::AuthMode;
    use super::{AgentManifest, GuestOs, NetworkAllow};

    #[test]
    fn validates_readme_shape() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::standard()).unwrap();

        assert_eq!(manifest.image.os, GuestOs::Archlinux);
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_network_allow_all() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::network_allow_all()).unwrap();

        assert_eq!(manifest.network.allow, NetworkAllow::All);
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_network_allow_host_list() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::network_allow_hosts()).unwrap();

        assert_eq!(
            manifest.network.allow,
            NetworkAllow::Hosts(vec!["github.com".to_owned(), "api.github.com".to_owned()])
        );
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_copy_from_host_auth_mode() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
version: 1
name: auth-copy
image:
  os: archlinux
user:
  name: agent
resources:
  cpus: 1
  memory: 1G
  storage: 10G
network:
  mode: user
  ports:
    ssh:
      guest: 22
      protocol: tcp
bootstrap: {}
plugins:
  codex:
    auth: copy-from-host
  github:
    auth: copy-from-host
",
        )
        .unwrap();

        assert_eq!(manifest.plugins.codex.as_ref().unwrap().auth, AuthMode::CopyFromHost);
        assert_eq!(manifest.plugins.github.as_ref().unwrap().auth, AuthMode::CopyFromHost);
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn rejects_absolute_repo_path() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::invalid_absolute_repo_path()).unwrap();

        let errors = manifest.validate().unwrap_err();
        assert!(
            errors
                .messages()
                .iter()
                .any(|message| message.contains("must be relative"))
        );
    }
}
