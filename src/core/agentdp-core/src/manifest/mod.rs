use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use agentdp_platform::ca::{CA_ENV_VARS_KEY, ca_env_vars_with_extra};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::Context;

pub mod plugins;

use crate::provisioning::host_input::HostInputRequirements;
use plugins::Plugins;

const DEFAULT_MANIFEST_NAMES: [&str; 2] = ["agent.yaml", "agent.yml"];

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to read current directory: {0}")]
    CurrentDirectory(std::io::Error),
    #[error("{0}")]
    Path(#[from] PathError),
    #[error("manifest path must be absolute: {0}")]
    RelativePath(PathBuf),
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentManifest {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: AgentManifestKind,
    pub metadata: AgentMetadata,
    pub spec: AgentDeploymentSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedAgentManifest {
    source_path: PathBuf,
    value: AgentManifest,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum AgentManifestKind {
    Agent,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentMetadata {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentDeploymentSpec {
    #[serde(default)]
    pub phase: AgentPhase,
    pub replicas: u16,
    pub template: AgentSpec,
}

impl Deref for AgentDeploymentSpec {
    type Target = AgentSpec;

    fn deref(&self) -> &Self::Target {
        &self.template
    }
}

impl DerefMut for AgentDeploymentSpec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.template
    }
}

impl AgentDeploymentSpec {
    fn validate(&self, errors: &mut Vec<String>) {
        self.template.validate(errors);
        self.template.network.validate_host_port_plan(self.replicas, errors);
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum AgentPhase {
    #[default]
    Running,
    Paused,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentSpec {
    pub image: Image,
    pub user: User,
    pub resources: Resources,
    pub network: Network,
    pub bootstrap: Bootstrap,
    pub secrets: Vec<Secret>,
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

        if self.api_version != "agentdp.dev/v1alpha1" {
            errors.push(format!(
                "apiVersion must be agentdp.dev/v1alpha1, got {}",
                self.api_version
            ));
        }
        validate_identifier("metadata.name", &self.metadata.name, &mut errors);
        let mut spec_errors = Vec::new();
        self.spec.validate(&mut spec_errors);
        errors.extend(spec_errors.into_iter().map(|message| format!("spec.{message}")));

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationErrors::new(errors))
        }
    }

    #[must_use]
    pub fn host_input_requirements(&self) -> HostInputRequirements {
        self.spec.template.host_input_requirements()
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.metadata.name
    }

    #[must_use]
    pub const fn replicas(&self) -> u16 {
        self.spec.replicas
    }

    #[must_use]
    pub const fn phase(&self) -> AgentPhase {
        self.spec.phase
    }

    #[must_use]
    pub const fn template(&self) -> &AgentSpec {
        &self.spec.template
    }
}

impl LoadedAgentManifest {
    /// Loads the explicit manifest path or current-directory default.
    ///
    /// # Errors
    ///
    /// Returns an error when path resolution, IO, parsing, or validation fails.
    pub async fn load_from_current_dir(context: &Context, explicit: Option<&Path>) -> Result<Self, Error> {
        let cwd = std::env::current_dir().map_err(Error::CurrentDirectory)?;
        let source_path = resolve_source_path(context, explicit, &cwd).await?;
        Self::load(context, &source_path).await
    }

    /// Loads the current-directory manifest when present.
    ///
    /// # Errors
    ///
    /// Returns an error when explicit path resolution, IO, parsing, or validation fails.
    pub async fn load_optional_from_current_dir(
        context: &Context,
        explicit: Option<&Path>,
    ) -> Result<Option<Self>, Error> {
        let cwd = std::env::current_dir().map_err(Error::CurrentDirectory)?;
        let source_path = match resolve_source_path(context, explicit, &cwd).await {
            Ok(path) => path,
            Err(PathError::MissingDefault(_)) if explicit.is_none() => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Self::load(context, &source_path).await.map(Some)
    }

    /// Loads a manifest from an absolute source path.
    ///
    /// # Errors
    ///
    /// Returns an error when IO, parsing, validation, or path validation fails.
    pub async fn load(context: &Context, source_path: &Path) -> Result<Self, Error> {
        let value = load_manifest_value(context, source_path).await?;
        Self::from_value(source_path, value)
    }

    /// Records the source identity for an already validated manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if the source path is not absolute.
    pub fn from_value(source_path: &Path, value: AgentManifest) -> Result<Self, Error> {
        if !source_path.is_absolute() {
            return Err(Error::RelativePath(source_path.to_path_buf()));
        }
        Ok(Self {
            source_path: source_path.to_path_buf(),
            value,
        })
    }

    #[must_use]
    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    #[must_use]
    pub const fn value(&self) -> &AgentManifest {
        &self.value
    }

    #[must_use]
    pub fn agent_name(&self) -> &str {
        self.value.name()
    }
}

impl AgentSpec {
    fn validate(&self, errors: &mut Vec<String>) {
        self.user.validate(self.image.os, errors);
        self.resources.validate(errors);
        self.network.validate(errors);
        self.bootstrap.validate(errors);
        let host_input_requirements = self.plugins.host_input_requirements();
        validate_secrets(&self.secrets, self.network.mode, &host_input_requirements, errors);
        validate_ca_secret_compatibility(&self.network, &self.secrets, &host_input_requirements, errors);
        self.plugins.validate(&self.network, errors);
    }

    #[must_use]
    pub fn host_input_requirements(&self) -> HostInputRequirements {
        let mut requirements = self.plugins.host_input_requirements();
        for secret in &self.secrets {
            requirements.allow_mediated_secret_hosts_dynamic(
                secret.source_env_name(),
                secret.env.clone(),
                secret.allow_hosts.iter().cloned(),
            );
        }
        requirements
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Ca {
    #[serde(default)]
    pub source: CaSource,
    #[serde(default)]
    pub extra_env_vars: Vec<String>,
}

impl Ca {
    #[must_use]
    pub fn is_active(&self, network_mode: NetworkMode) -> bool {
        match self.source {
            CaSource::Auto => network_mode == NetworkMode::Mediated,
            CaSource::None => false,
            CaSource::Path(_) => true,
        }
    }

    #[must_use]
    pub fn generates_mediated_ca(&self, network_mode: NetworkMode) -> bool {
        self.source == CaSource::Auto && network_mode == NetworkMode::Mediated
    }

    #[must_use]
    pub fn source_path(&self) -> Option<&str> {
        match &self.source {
            CaSource::Path(path) => Some(path),
            CaSource::Auto | CaSource::None => None,
        }
    }

    #[must_use]
    pub fn env_vars(&self) -> Vec<String> {
        ca_env_vars_with_extra(&self.extra_env_vars)
    }

    fn validate(&self, network_mode: NetworkMode, errors: &mut Vec<String>) {
        if let Some(path) = self.source_path() {
            validate_relative_path("network.ca.source", path, errors);
            if network_mode == NetworkMode::Mediated {
                errors.push(
                    "network.ca.source path is only supported with spec.network.mode: user; mediated mode generates its own CA"
                        .to_owned(),
                );
            }
        }
        for (index, name) in self.extra_env_vars.iter().enumerate() {
            validate_env_name(&format!("network.ca.extra_env_vars[{index}]"), name, errors);
            if name == CA_ENV_VARS_KEY {
                errors.push(format!(
                    "network.ca.extra_env_vars[{index}] cannot use reserved env var {CA_ENV_VARS_KEY}"
                ));
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum CaSource {
    #[default]
    Auto,
    None,
    Path(String),
}

impl<'de> Deserialize<'de> for CaSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "auto" => Ok(Self::Auto),
            "none" => Ok(Self::None),
            _ => Ok(Self::Path(value)),
        }
    }
}

impl Serialize for CaSource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Auto => serializer.serialize_str("auto"),
            Self::None => serializer.serialize_str("none"),
            Self::Path(path) => serializer.serialize_str(path),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Secret {
    pub env: String,
    pub from_env: Option<String>,
    pub allow_hosts: Vec<String>,
}

impl Secret {
    #[must_use]
    pub fn source_env_name(&self) -> String {
        self.from_env.clone().unwrap_or_else(|| self.env.clone())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Image {
    pub os: GuestOs,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum GuestOs {
    Archlinux,
    Rocky9,
}

impl GuestOs {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Archlinux => "archlinux",
            Self::Rocky9 => "rocky9",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub name: String,
    pub options: UserOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserOptions {
    Linux(UserLinux),
}

impl Default for UserOptions {
    fn default() -> Self {
        Self::Linux(UserLinux::default())
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UserLinux {
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub group: Option<String>,
    #[serde(default)]
    pub groups: Vec<String>,
}

impl User {
    #[must_use]
    pub const fn linux(&self) -> &UserLinux {
        match &self.options {
            UserOptions::Linux(linux) => linux,
        }
    }

    fn validate(&self, os: GuestOs, errors: &mut Vec<String>) {
        validate_identifier("user.name", &self.name, errors);
        if self.name == "root" {
            errors.push("user.name must not be root".to_owned());
        }
        self.options.validate(os, errors);
    }
}

impl UserOptions {
    const fn guest_os_matches(&self, os: GuestOs) -> bool {
        match self {
            Self::Linux(_) => matches!(os, GuestOs::Archlinux | GuestOs::Rocky9),
        }
    }

    fn validate(&self, os: GuestOs, errors: &mut Vec<String>) {
        if !self.guest_os_matches(os) {
            errors.push(format!("user OS section is not valid for image.os {}", os.name()));
        }
        match self {
            Self::Linux(linux) => linux.validate(errors),
        }
    }
}

impl<'de> Deserialize<'de> for User {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawUser {
            name: String,
            #[serde(default)]
            linux: UserLinux,
        }

        let raw = RawUser::deserialize(deserializer)?;
        Ok(Self {
            name: raw.name,
            options: UserOptions::Linux(raw.linux),
        })
    }
}

impl Serialize for User {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct RawUser<'a> {
            name: &'a str,
            linux: &'a UserLinux,
        }

        RawUser {
            name: &self.name,
            linux: self.linux(),
        }
        .serialize(serializer)
    }
}

impl UserLinux {
    fn validate(&self, errors: &mut Vec<String>) {
        if matches!(self.uid, Some(0)) {
            errors.push("user.linux.uid must not be 0".to_owned());
        }
        if matches!(self.gid, Some(0)) {
            errors.push("user.linux.gid must not be 0".to_owned());
        }
        if let Some(group) = &self.group {
            validate_identifier("user.linux.group", group, errors);
        }
        validate_non_empty_values("user.linux.groups", &self.groups, errors);
        for (index, group) in self.groups.iter().enumerate() {
            validate_identifier(&format!("user.linux.groups[{index}]"), group, errors);
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Network {
    pub mode: NetworkMode,
    #[serde(default)]
    pub ipv6: NetworkIpv6,
    #[serde(default)]
    pub ca: Ca,
    pub ports: BTreeMap<String, GuestPort>,
    #[serde(default)]
    pub allow: NetworkAllow,
    #[serde(default)]
    pub host_aliases: Vec<HostAlias>,
}

impl Network {
    fn validate(&self, errors: &mut Vec<String>) {
        if self.ports.is_empty() {
            errors.push("network.ports must declare at least one named guest port".to_owned());
        }

        self.ca.validate(self.mode, errors);
        for (name, port) in &self.ports {
            validate_identifier("network.ports key", name, errors);
            port.validate(self.mode, name, errors);
        }

        self.allow.validate(errors);
        for (index, alias) in self.host_aliases.iter().enumerate() {
            alias.validate(index, errors);
        }
    }

    fn validate_host_port_plan(&self, replicas: u16, errors: &mut Vec<String>) {
        let mut assigned = BTreeSet::new();
        for (name, port) in &self.ports {
            let Some(base) = port.host else {
                continue;
            };
            for replica in 0..u32::from(replicas) {
                let Some(host) = u32::from(base).checked_add(replica) else {
                    errors.push(format!(
                        "network.ports.{name}.host range exceeds 65535 for replicas: {replicas}"
                    ));
                    break;
                };
                let Ok(host) = u16::try_from(host) else {
                    errors.push(format!(
                        "network.ports.{name}.host range exceeds 65535 for replicas: {replicas}"
                    ));
                    break;
                };
                if !assigned.insert(host) {
                    errors.push(format!(
                        "network.ports.{name}.host assigns overlapping host port {host} across configured replicas"
                    ));
                }
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostAlias {
    pub address: String,
    pub names: Vec<String>,
}

impl HostAlias {
    fn validate(&self, index: usize, errors: &mut Vec<String>) {
        let field = format!("network.host_aliases[{index}]");
        validate_non_empty(&format!("{field}.address"), &self.address, errors);
        validate_non_empty_values(&format!("{field}.names"), &self.names, errors);
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkMode {
    Mediated,
    User,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkIpv6 {
    #[default]
    Auto,
    Enabled,
    Disabled,
}

impl NetworkIpv6 {
    #[must_use]
    pub const fn enabled_for_host(self, host_ipv6: bool) -> bool {
        match self {
            Self::Auto => host_ipv6,
            Self::Enabled => true,
            Self::Disabled => false,
        }
    }
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

impl Serialize for NetworkAllow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Hosts(hosts) => hosts.serialize(serializer),
            Self::All => serializer.serialize_str("all"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawNetworkAllow {
    Keyword(String),
    Hosts(Vec<String>),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GuestPort {
    pub guest: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<u16>,
    pub protocol: NetworkProtocol,
}

impl GuestPort {
    fn validate(&self, mode: NetworkMode, name: &str, errors: &mut Vec<String>) {
        if self.guest == 0 {
            errors.push(format!("network.ports.{name}.guest must be greater than zero"));
        }
        if matches!(self.host, Some(0)) {
            errors.push(format!("network.ports.{name}.host must be greater than zero"));
        }
        if mode == NetworkMode::User && self.host.is_none() {
            errors.push(format!(
                "network.ports.{name}.host is required when using user networking"
            ));
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    #[serde(default)]
    pub package_update: bool,
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
        validate_unique_repo_paths(&self.repos, errors);

        for (index, healthcheck) in self.healthchecks.iter().enumerate() {
            healthcheck.validate(index, errors);
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Repo {
    pub name: Option<String>,
    pub url: String,
    pub path: Option<String>,
    pub upstream: Option<String>,
}

impl Repo {
    pub(crate) fn checkout_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| repo_name_from_url(&self.url))
    }

    pub(crate) fn checkout_path(&self) -> String {
        normalize_relative_path(&self.path.clone().unwrap_or_else(|| self.checkout_name()))
    }

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

fn validate_unique_repo_paths(repos: &[Repo], errors: &mut Vec<String>) {
    let mut paths = BTreeMap::<String, usize>::new();
    for (index, repo) in repos.iter().enumerate() {
        let path = repo.checkout_path();
        if let Some(first_index) = paths.insert(path.clone(), index) {
            errors.push(format!(
                "bootstrap.repos[{index}] resolves to duplicate checkout path `{path}` first declared by bootstrap.repos[{first_index}]"
            ));
        }
    }
}

fn normalize_relative_path(value: &str) -> String {
    let components = value
        .split('/')
        .filter(|component| !component.is_empty() && *component != ".")
        .collect::<Vec<_>>();
    if components.is_empty() {
        ".".to_owned()
    } else {
        components.join("/")
    }
}

fn repo_name_from_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    let name = trimmed
        .rsplit(['/', ':'])
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("repo");
    name.strip_suffix(".git").unwrap_or(name).to_owned()
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Healthcheck {
    Command {
        name: String,
        command: String,
        timeout: Option<String>,
    },
    Tcp {
        name: String,
        target: String,
        timeout: Option<String>,
    },
    Http {
        name: String,
        method: String,
        url: String,
        timeout: Option<String>,
    },
}

impl Healthcheck {
    fn validate(&self, index: usize, errors: &mut Vec<String>) {
        let field = format!("bootstrap.healthchecks[{index}]");
        validate_identifier(&format!("{field}.name"), self.name(), errors);

        match self {
            Self::Command { command, .. } => {
                validate_non_empty(&format!("{field}.command"), command, errors);
            }
            Self::Tcp { target, .. } => validate_tcp_target(&format!("{field}.target"), target, errors),
            Self::Http { method, url, .. } => {
                validate_http_method(&format!("{field}.method"), method, errors);
                validate_http_url(&format!("{field}.url"), url, errors);
            }
        }

        if let Some(timeout) = self.timeout() {
            validate_duration(&format!("{field}.timeout"), timeout, errors);
        }
    }

    fn name(&self) -> &str {
        match self {
            Self::Command { name, .. } | Self::Tcp { name, .. } | Self::Http { name, .. } => name,
        }
    }

    fn timeout(&self) -> Option<&str> {
        match self {
            Self::Command { timeout, .. } | Self::Tcp { timeout, .. } | Self::Http { timeout, .. } => {
                timeout.as_deref()
            }
        }
    }
}

/// Loads, parses, and validates an agent manifest.
///
/// # Errors
///
/// Returns an error if the file cannot be read, the YAML cannot be parsed, or
/// the parsed manifest fails validation.
async fn load_manifest_value(context: &Context, path: &Path) -> Result<AgentManifest, Error> {
    context
        .logger()
        .verbose_with(|| format!("reading manifest {}", path.display()));
    let contents = tokio::fs::read_to_string(path).await.map_err(|source| Error::Read {
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

async fn resolve_source_path(context: &Context, explicit: Option<&Path>, cwd: &Path) -> Result<PathBuf, PathError> {
    if let Some(path) = explicit {
        context
            .logger()
            .verbose_with(|| format!("resolving explicit manifest path {}", path.display()));
        let resolved = if path.is_absolute() {
            path.to_path_buf()
        } else {
            cwd.join(path)
        };
        if tokio::fs::try_exists(&resolved).await.unwrap_or(false) {
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
    let mut matches = Vec::new();
    for name in DEFAULT_MANIFEST_NAMES {
        let path = cwd.join(name);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            matches.push(path);
        }
    }
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

fn validate_secrets(
    secrets: &[Secret],
    network_mode: NetworkMode,
    plugin_requirements: &HostInputRequirements,
    errors: &mut Vec<String>,
) {
    if !secrets.is_empty() && network_mode != NetworkMode::Mediated {
        errors.push("secrets require spec.network.mode to be mediated".to_owned());
    }

    let mut source_names = std::collections::BTreeSet::new();
    let mut guest_names = std::collections::BTreeSet::new();
    for (index, secret) in secrets.iter().enumerate() {
        let field = format!("secrets[{index}]");
        validate_env_name(&format!("{field}.env"), &secret.env, errors);
        if let Some(from_env) = &secret.from_env {
            validate_env_name(&format!("{field}.from_env"), from_env, errors);
        }
        validate_non_empty_values(&format!("{field}.allow_hosts"), &secret.allow_hosts, errors);
        if secret.allow_hosts.is_empty() {
            errors.push(format!("{field}.allow_hosts must declare at least one host"));
        }
        let source = secret.source_env_name();
        if !source_names.insert(source.clone()) {
            errors.push(format!("secrets source env `{source}` is declared more than once"));
        }
        if !guest_names.insert(secret.env.clone()) {
            errors.push(format!("secrets guest env `{}` is declared more than once", secret.env));
        }
        for name in [&source, &secret.env] {
            if plugin_requirements.copies_custom_env_name(name)
                || !plugin_requirements.mediated_secret_allowed_hosts_for(name).is_empty()
            {
                errors.push(format!(
                    "{field} uses plugin-owned env `{name}`; configure that plugin auth mode instead"
                ));
            }
        }
    }
}

fn validate_ca_secret_compatibility(
    network: &Network,
    secrets: &[Secret],
    plugin_requirements: &HostInputRequirements,
    errors: &mut Vec<String>,
) {
    if network.mode == NetworkMode::Mediated
        && network.ca.source == CaSource::None
        && (!secrets.is_empty() || plugin_requirements.has_mediated_secret_inputs())
    {
        errors.push("network.ca.source: none cannot be used with mediated secrets".to_owned());
    }
}

fn validate_env_name(field: &str, value: &str, errors: &mut Vec<String>) {
    validate_non_empty(field, value, errors);

    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return;
    };
    let valid_first = first.is_ascii_alphabetic() || first == b'_';
    let valid_rest = bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if !valid_first || !valid_rest {
        errors.push(format!(
            "{field} must be a shell environment name using ASCII letters, digits, and '_' and must not start with a digit"
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

    if value.starts_with('/') {
        errors.push(format!("{field} must be relative"));
        return;
    }

    if value.contains('\\') {
        errors.push(format!("{field} must use '/' separators"));
        return;
    }

    if value.split('/').any(|component| component == "..") {
        errors.push(format!("{field} must not contain '..'"));
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

fn validate_http_method(field: &str, value: &str, errors: &mut Vec<String>) {
    validate_non_empty(field, value, errors);
    let valid = value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
            )
    });
    if !valid {
        errors.push(format!("{field} must be an HTTP token"));
    }
}

fn validate_http_url(field: &str, value: &str, errors: &mut Vec<String>) {
    validate_non_empty(field, value, errors);

    let Some(rest) = value.strip_prefix("http://") else {
        errors.push(format!("{field} must start with http://"));
        return;
    };
    let authority = rest.split_once('/').map_or(rest, |(authority, _path)| authority);
    validate_tcp_target(&format!("{field} authority"), authority, errors);
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

    use super::{AgentManifest, CaSource, GuestOs, NetworkAllow, NetworkIpv6, NetworkMode};

    #[test]
    fn validates_readme_shape() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::standard()).unwrap();

        assert_eq!(manifest.spec.image.os, GuestOs::Archlinux);
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn rejects_flat_pre_controller_manifest_shape() {
        let error = serde_yaml::from_str::<AgentManifest>(
            r"
version: 1
name: old-shape
image:
  os: archlinux
user:
  name: agent
resources:
  cpus: 1
  memory: 1G
  storage: 10G
network:
  mode: mediated
  ports:
    ssh:
      guest: 22
      protocol: tcp
bootstrap: {}
",
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("unknown field `version`"), "{error}");
    }

    #[test]
    fn accepts_zero_replicas_as_stopped_desired_state() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: zero-replicas
spec:
  phase: Running
  replicas: 0
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap();

        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_host_port_base_for_multiple_replicas() {
        let manifest = manifest_with_ports(
            5,
            r"
        code_server:
          guest: 4090
          host: 4090
          protocol: tcp
        ssh:
          guest: 22
          host: 4100
          protocol: tcp
",
        );

        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn rejects_overlapping_host_port_ranges_across_replicas() {
        let manifest = manifest_with_ports(
            5,
            r"
        code_server:
          guest: 4090
          host: 4090
          protocol: tcp
        ssh:
          guest: 22
          host: 4093
          protocol: tcp
",
        );
        let error = manifest.validate().unwrap_err().to_string();

        assert!(
            error.contains("spec.network.ports.ssh.host assigns overlapping host port 4093"),
            "{error}"
        );
    }

    #[test]
    fn rejects_host_port_range_overflow_across_replicas() {
        let manifest = manifest_with_ports(
            5,
            r"
        ssh:
          guest: 22
          host: 65534
          protocol: tcp
",
        );
        let error = manifest.validate().unwrap_err().to_string();

        assert!(
            error.contains("spec.network.ports.ssh.host range exceeds 65535 for replicas: 5"),
            "{error}"
        );
    }

    #[test]
    fn rejects_user_network_ports_without_host_port_base() {
        let mut manifest = manifest_with_ports(
            1,
            r"
        ssh:
          guest: 22
          protocol: tcp
",
        );
        manifest.spec.template.network.mode = NetworkMode::User;
        let error = manifest.validate().unwrap_err().to_string();

        assert!(
            error.contains("spec.network.ports.ssh.host is required when using user networking"),
            "{error}"
        );
    }

    #[test]
    fn rejects_missing_network_ports_field() {
        let error = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: missing-ports
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("missing field `ports`"), "{error}");
    }

    #[test]
    fn rejects_secret_without_allow_hosts_field() {
        let error = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: missing-secret-hosts
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets:
      - env: STUDIO_DEV_API_KEY
    plugins: {}
",
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("missing field `allow_hosts`"), "{error}");
    }

    #[test]
    fn rejects_host_alias_without_names_field() {
        let error = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: missing-host-alias-names
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
      host_aliases:
        - address: 127.0.0.1
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("missing field `names`"), "{error}");
    }

    #[test]
    fn validation_errors_report_empty_state() {
        assert!(super::ValidationErrors::new(Vec::new()).is_empty());
        assert!(!super::ValidationErrors::new(vec!["invalid".to_owned()]).is_empty());
    }

    #[test]
    fn accepts_network_allow_all() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::network_allow_all()).unwrap();

        assert_eq!(manifest.spec.network.allow, NetworkAllow::All);
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_network_allow_host_list() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::network_allow_hosts()).unwrap();

        assert_eq!(
            manifest.spec.network.allow,
            NetworkAllow::Hosts(vec!["github.com".to_owned(), "api.github.com".to_owned()])
        );
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_network_ipv6_modes() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: ipv6-network
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ipv6: disabled
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap();

        assert_eq!(manifest.spec.network.ipv6, NetworkIpv6::Disabled);
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_user_network_mode() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: user-network
spec:
  phase: Running
  replicas: 1
  template:
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
          host: 2222
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap();

        assert_eq!(manifest.spec.network.mode, NetworkMode::User);
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_ca_extra_env_vars() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: ca-extra-env
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ca:
        extra_env_vars:
          - STUDIO_CA_BUNDLE
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap();

        assert_eq!(manifest.spec.network.ca.source, CaSource::Auto);
        assert!(manifest.spec.network.ca.is_active(NetworkMode::Mediated));
        assert!(
            manifest
                .spec
                .network
                .ca
                .env_vars()
                .iter()
                .any(|name| name == "STUDIO_CA_BUNDLE")
        );
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn rejects_reserved_ca_env_var_name() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: reserved-ca-env
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ca:
        extra_env_vars:
          - AGENTDP_CA_ENV_VARS
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap();

        let errors = manifest.validate().unwrap_err();

        assert!(errors.messages().iter().any(|message| {
            message == "spec.network.ca.extra_env_vars[0] cannot use reserved env var AGENTDP_CA_ENV_VARS"
        }));
    }

    #[test]
    fn accepts_user_network_ca_source() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: ca-source
spec:
  phase: Running
  replicas: 1
  template:
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
      ca:
        source: data/ca/corp.pem
      ports:
        ssh:
          guest: 22
          host: 2222
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap();

        assert_eq!(manifest.spec.network.ca.source_path(), Some("data/ca/corp.pem"));
        assert!(manifest.spec.network.ca.is_active(NetworkMode::User));
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn rejects_invalid_ca_source_paths() {
        for (source, expected) in [
            ("/etc/ssl/certs/corp.pem", "spec.network.ca.source must be relative"),
            ("../corp.pem", "spec.network.ca.source must not contain '..'"),
            ("data\\corp.pem", "spec.network.ca.source must use '/' separators"),
        ] {
            let yaml = r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: invalid-ca-source
spec:
  phase: Running
  replicas: 1
  template:
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
      ca:
        source: SOURCE
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
"
            .replace("SOURCE", source);
            let manifest = serde_yaml::from_str::<AgentManifest>(&yaml).unwrap();

            let errors = manifest.validate().unwrap_err();

            assert!(
                errors.messages().iter().any(|message| message == expected),
                "{source}: {:?}",
                errors.messages()
            );
        }
    }

    #[test]
    fn rejects_ca_source_in_mediated_mode() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: invalid-ca-source
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ca:
        source: data/ca/corp.pem
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap();

        let errors = manifest.validate().unwrap_err();

        assert!(errors.messages().iter().any(|message| {
            message == "spec.network.ca.source path is only supported with spec.network.mode: user; mediated mode generates its own CA"
        }));
    }

    #[test]
    fn rejects_disabled_ca_with_top_level_mediated_secrets() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: disabled-ca-secret
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ca:
        source: none
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets:
      - env: STUDIO_DEV_API_KEY
        allow_hosts:
          - dev.altinn.studio
    plugins: {}
",
        )
        .unwrap();

        let errors = manifest.validate().unwrap_err();

        assert!(
            errors
                .messages()
                .iter()
                .any(|message| message == "spec.network.ca.source: none cannot be used with mediated secrets")
        );
    }

    #[test]
    fn rejects_disabled_ca_with_plugin_mediated_secrets() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: disabled-ca-plugin-secret
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ca:
        source: none
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      github:
        auth: mediated
    secrets: []
",
        )
        .unwrap();

        let errors = manifest.validate().unwrap_err();

        assert!(
            errors
                .messages()
                .iter()
                .any(|message| message == "spec.network.ca.source: none cannot be used with mediated secrets")
        );
    }

    #[test]
    fn accepts_rocky9_with_numeric_agent_user() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: rhel-podman
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: rocky9
    user:
      name: agent
      linux:
        uid: 1199049453
        gid: 1199000513
        group: domain-users
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap();

        assert_eq!(manifest.spec.image.os, GuestOs::Rocky9);
        assert_eq!(manifest.spec.user.linux().uid, Some(1_199_049_453));
        assert_eq!(manifest.spec.user.linux().gid, Some(1_199_000_513));
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_copy_from_host_auth_mode() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: auth-copy
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
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
    secrets: []
",
        )
        .unwrap();

        assert!(manifest.spec.plugins.host_input_requirements().copies_custom_env());
    }

    #[test]
    fn git_plugin_declares_identity_env_requirements() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: git-config
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      git:
        user:
          name:
            from_env: GIT_USER_NAME
          email:
            from_env: GIT_USER_EMAIL
        defaults:
          init_default_branch: main
          autocrlf: false
    secrets: []
",
        )
        .unwrap();

        let requirements = manifest.spec.plugins.host_input_requirements();

        assert!(requirements.copies_custom_env_name("GIT_USER_NAME"));
        assert!(requirements.copies_custom_env_name("GIT_USER_EMAIL"));
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn plugins_declare_host_input_secret_destinations() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: mediated-auth
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      codex:
        auth: mediated
      github:
        auth: mediated
    secrets: []
",
        )
        .unwrap();

        let requirements = manifest.host_input_requirements();

        assert_eq!(
            requirements.mediated_secret_allowed_hosts(),
            vec![
                "api.github.com".to_owned(),
                "github.com".to_owned(),
                "objects.githubusercontent.com".to_owned(),
            ]
        );
        let file = requirements.files().first().unwrap();
        assert_eq!(file.label(), "Codex auth");
        assert_eq!(file.permissions(), "0600");
        let materialized = file
            .materialize(
                br#"{"tokens":{"access_token":"access-secret"},"profile":"keep-me"}"#,
                crate::provisioning::host_input::MaterializationContext::default(),
            )
            .unwrap();
        let auth_json = String::from_utf8(materialized.contents).unwrap();
        assert!(auth_json.contains("\"profile\": \"keep-me\""));
        assert!(!auth_json.contains("access-secret"));
        assert!(!materialized.secrets.is_empty());
    }

    #[test]
    fn claude_plugin_declares_host_input_secret_destinations() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: mediated-claude-auth
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      claude:
        auth: mediated
        auth_source: host-auth
    secrets: []
",
        )
        .unwrap();

        let requirements = manifest.host_input_requirements();

        let file = requirements.files().first().unwrap();
        assert_eq!(file.label(), "Claude auth");
        assert_eq!(file.permissions(), "0600");
        let materialized = file
            .materialize(
                br#"{"claudeAiOauth":{"accessToken":"access-secret","refreshToken":"refresh-secret","expiresAt":123,"scopes":["user:inference"],"subscriptionType":"max"}}"#,
                crate::provisioning::host_input::MaterializationContext::default(),
            )
            .unwrap();
        let auth_json = String::from_utf8(materialized.contents).unwrap();
        assert!(auth_json.contains("\"subscriptionType\": \"max\""));
        assert!(auth_json.contains("\"expiresAt\": 123"));
        assert!(!auth_json.contains("access-secret"));
        assert!(!auth_json.contains("refresh-secret"));
        assert!(!materialized.secrets.is_empty());
        for binding in materialized.secrets.iter() {
            assert!(binding.allows_host("api.anthropic.com"));
            assert!(binding.allows_host("console.anthropic.com"));
            assert!(binding.allows_host("claude.ai"));
            assert!(!binding.allows_host("example.com"));
        }
    }

    #[test]
    fn claude_plugin_env_auth_scopes_anthropic_hosts() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: mediated-claude-env
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      claude:
        auth: mediated
        auth_source: env
    secrets: []
",
        )
        .unwrap();

        let requirements = manifest.host_input_requirements();

        assert!(requirements.files().is_empty());
        let anthropic_hosts = vec![
            "api.anthropic.com".to_owned(),
            "claude.ai".to_owned(),
            "console.anthropic.com".to_owned(),
        ];
        assert_eq!(
            requirements.mediated_secret_allowed_hosts_for("ANTHROPIC_API_KEY"),
            anthropic_hosts
        );
        assert_eq!(
            requirements.mediated_secret_allowed_hosts_for("CLAUDE_CODE_OAUTH_TOKEN"),
            anthropic_hosts
        );
    }

    #[test]
    fn rejects_claude_and_codex_together() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: claude-and-codex
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      claude:
        auth: mediated
      codex:
        auth: mediated
    secrets: []
",
        )
        .unwrap();

        let errors = manifest.validate().unwrap_err();

        assert!(errors.messages().iter().any(|message| {
            message
                == "spec.plugins.claude and plugins.codex cannot both be enabled: both manage the agent tmux session"
        }));
    }

    #[test]
    fn top_level_secrets_declare_host_input_secret_destinations() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: mediated-secrets
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets:
      - env: STUDIO_DEV_API_KEY
        from_env: ALTINN_DEV_KEY
        allow_hosts:
          - dev.altinn.studio
    plugins: {}
",
        )
        .unwrap();

        let requirements = manifest.host_input_requirements();

        assert!(manifest.validate().is_ok());
        assert_eq!(
            requirements.mediated_secret_allowed_hosts_for("ALTINN_DEV_KEY"),
            vec!["dev.altinn.studio".to_owned()]
        );
        assert_eq!(
            requirements.mediated_secret_guest_name_for("ALTINN_DEV_KEY"),
            Some("STUDIO_DEV_API_KEY")
        );
    }

    #[test]
    fn accepts_tailscale_serve_plugin_routes() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r#"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: tailscale-routes
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        code_server:
          guest: 4090
          protocol: tcp
    bootstrap: {}
    plugins:
      tailscale_serve:
        routes:
          - service: code_server
            host_template: "{service}-{instance}-{agent}"
            path: /agents/{agent}/instances/{instance}/services/{service}
            mode: direct
    secrets: []
"#,
        )
        .unwrap();

        let tailscale_serve = manifest.spec.plugins.tailscale_serve.as_ref().unwrap();
        assert_eq!(tailscale_serve.routes.len(), 1);
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_browser_playwright_plugin() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: browser-agent
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      browser:
        playwright:
          install: npm-global
          browser_package: chromium
          executable_path: /usr/bin/chromium
          viewport: 1440x900
    secrets: []
",
        )
        .unwrap();

        assert!(manifest.spec.plugins.browser.as_ref().unwrap().playwright.is_some());
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn accepts_code_server_extension_removal() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: code-server-agent
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        code_server:
          guest: 4090
          protocol: tcp
    bootstrap: {}
    plugins:
      code_server:
        remove_extensions:
          - github.copilot
          - github.copilot-chat
    secrets: []
",
        )
        .unwrap();

        assert_eq!(
            manifest.spec.plugins.code_server.as_ref().unwrap().remove_extensions,
            ["github.copilot", "github.copilot-chat"]
        );
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn rejects_duplicate_resolved_repo_checkout_paths() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: duplicate-repos
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap:
      repos:
        - url: https://github.com/example/app.git
          path: app
        - url: https://github.com/example/other.git
          path: ./app/
    secrets: []
    plugins: {}
",
        )
        .unwrap();

        let errors = manifest.validate().unwrap_err();

        assert!(
            errors
                .messages()
                .iter()
                .any(|message| message.contains("duplicate checkout path `app`"))
        );
    }

    #[test]
    fn rejects_repo_paths_with_backslash_separators() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: invalid-repo-path
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap:
      repos:
        - url: https://github.com/example/app.git
          path: app\svc
    secrets: []
    plugins: {}
",
        )
        .unwrap();

        let errors = manifest.validate().unwrap_err();

        assert!(
            errors
                .messages()
                .iter()
                .any(|message| message.contains("must use '/' separators"))
        );
    }

    #[test]
    fn rejects_top_level_secrets_without_mediated_network() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: user-secret
spec:
  phase: Running
  replicas: 1
  template:
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
    secrets:
      - env: STUDIO_DEV_API_KEY
        allow_hosts:
          - dev.altinn.studio
    plugins: {}
",
        )
        .unwrap();

        let errors = manifest.validate().unwrap_err();

        assert!(
            errors
                .messages()
                .iter()
                .any(|message| message == "spec.secrets require spec.network.mode to be mediated")
        );
    }

    #[test]
    fn rejects_top_level_secrets_for_plugin_owned_env_names() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: plugin-secret-conflict
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets:
      - env: GITHUB_TOKEN
        allow_hosts:
          - example.com
    plugins:
      github:
        auth: mediated
",
        )
        .unwrap();

        let errors = manifest.validate().unwrap_err();

        assert!(
            errors
                .messages()
                .iter()
                .any(|message| message.contains("uses plugin-owned env `GITHUB_TOKEN`"))
        );
    }

    #[test]
    fn rejects_tailscale_serve_routes_for_unknown_ports() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: tailscale-routes
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      tailscale_serve:
        routes:
          - service: code_server
    secrets: []
",
        )
        .unwrap();

        let errors = manifest.validate().unwrap_err();
        assert!(
            errors
                .messages()
                .iter()
                .any(|message| message.contains("references unknown network port `code_server`"))
        );
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

    fn manifest_with_ports(replicas: u16, ports: &str) -> AgentManifest {
        serde_yaml::from_str::<AgentManifest>(&format!(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: test
spec:
  phase: Running
  replicas: {replicas}
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
{ports}
    bootstrap: {{}}
    secrets: []
    plugins: {{}}
"
        ))
        .unwrap()
    }
}
