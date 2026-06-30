use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use thiserror::Error;

use super::secrets::{SecretBinding, SecretBindings};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostInputRequirements {
    mediated_env_secrets: BTreeMap<String, MediatedEnvSecret>,
    required_mediated_env_groups: Vec<BTreeSet<String>>,
    copied_env_names: BTreeSet<String>,
    files: Vec<HostInputFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MediatedEnvSecret {
    guest_name: String,
    allowed_hosts: BTreeSet<String>,
}

impl HostInputRequirements {
    pub(crate) fn allow_mediated_secret_hosts(
        &mut self,
        names: impl IntoIterator<Item = &'static str>,
        hosts: impl IntoIterator<Item = &'static str>,
    ) {
        let names = names.into_iter().map(str::to_owned).collect::<BTreeSet<_>>();
        let hosts = hosts.into_iter().map(str::to_owned).collect::<BTreeSet<_>>();
        for name in &names {
            self.mediated_env_secrets
                .entry(name.clone())
                .or_insert_with(|| MediatedEnvSecret {
                    guest_name: name.clone(),
                    allowed_hosts: BTreeSet::new(),
                })
                .allowed_hosts
                .extend(hosts.iter().cloned());
        }
        self.required_mediated_env_groups.push(names);
    }

    pub fn allow_mediated_secret_hosts_dynamic(
        &mut self,
        source_name: impl Into<String>,
        guest_name: impl Into<String>,
        hosts: impl IntoIterator<Item = String>,
    ) {
        let source_name = source_name.into();
        self.mediated_env_secrets
            .entry(source_name.clone())
            .or_insert_with(|| MediatedEnvSecret {
                guest_name: guest_name.into(),
                allowed_hosts: BTreeSet::new(),
            })
            .allowed_hosts
            .extend(hosts);
        self.required_mediated_env_groups.push(BTreeSet::from([source_name]));
    }

    pub(crate) fn copy_custom_env(&mut self, names: impl IntoIterator<Item = impl Into<String>>) {
        self.copied_env_names.extend(names.into_iter().map(Into::into));
    }

    pub(crate) fn add_file(&mut self, file: HostInputFile) {
        self.files.push(file);
    }

    #[must_use]
    pub fn mediated_secret_allowed_hosts(&self) -> Vec<String> {
        self.mediated_env_secrets
            .values()
            .flat_map(|secret| secret.allowed_hosts.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    #[must_use]
    pub fn mediated_secret_allowed_hosts_for(&self, name: &str) -> Vec<String> {
        self.mediated_env_secrets
            .get(name)
            .map(|secret| secret.allowed_hosts.iter().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn mediated_secret_guest_name_for(&self, name: &str) -> Option<&str> {
        self.mediated_env_secrets
            .get(name)
            .map(|secret| secret.guest_name.as_str())
    }

    #[must_use]
    pub fn copies_custom_env(&self) -> bool {
        !self.copied_env_names.is_empty()
    }

    #[must_use]
    pub fn copies_custom_env_name(&self, name: &str) -> bool {
        self.copied_env_names.contains(name)
    }

    #[must_use]
    pub fn files(&self) -> &[HostInputFile] {
        &self.files
    }

    #[must_use]
    pub fn has_same_mediated_auth_bindings(&self, other: &Self) -> bool {
        let mut required_env_groups = self.required_mediated_env_groups.clone();
        required_env_groups.sort();
        required_env_groups.dedup();
        let mut other_required_env_groups = other.required_mediated_env_groups.clone();
        other_required_env_groups.sort();
        other_required_env_groups.dedup();
        self.mediated_env_secrets.len() == other.mediated_env_secrets.len()
            && self.mediated_env_secrets.iter().all(|(source, secret)| {
                other
                    .mediated_env_secrets
                    .get(source)
                    .is_some_and(|other| secret.guest_name == other.guest_name)
            })
            && required_env_groups == other_required_env_groups
            && self
                .files
                .iter()
                .filter(|file| file.produces_secrets())
                .eq(other.files.iter().filter(|file| file.produces_secrets()))
    }

    /// Materializes a host `.env` file for guest bootstrap.
    ///
    /// # Errors
    ///
    /// Returns an error when an environment variable is requested for both
    /// copied and mediated auth, or when a mediated secret binding is invalid.
    pub fn materialize_custom_env(
        &self,
        contents: &[u8],
        context: MaterializationContext<'_>,
    ) -> Result<MaterializedHostInput, Error> {
        let mut guest_env = Vec::new();
        let mut secrets = SecretBindings::default();
        let mut materialized_mediated_names = BTreeSet::new();
        for line in String::from_utf8_lossy(contents).lines() {
            let Some(assignment) = parse_custom_env_assignment(line) else {
                guest_env.extend_from_slice(line.as_bytes());
                guest_env.push(b'\n');
                continue;
            };
            let name = assignment.name;
            let mediated = self.mediated_env_secrets.get(name);
            if self.copies_custom_env_name(name) && mediated.is_some() {
                return Err(Error::ConflictingCustomEnvAuth { name: name.to_owned() });
            }
            let Some(mediated) = mediated else {
                guest_env.extend_from_slice(line.as_bytes());
                guest_env.push(b'\n');
                continue;
            };
            materialized_mediated_names.insert(name.to_owned());
            let allowed_hosts = mediated.allowed_hosts.iter().cloned().collect::<Vec<_>>();
            let guest_name = mediated.guest_name.as_str();
            let placeholder = context.placeholder_for_name(guest_name).map(str::to_owned);
            let binding = SecretBinding::new_with_placeholder(
                guest_name,
                placeholder,
                unquote_env_value(assignment.value.trim()),
                &allowed_hosts,
            )?;
            guest_env.extend_from_slice(assignment.prefix.as_bytes());
            guest_env.extend_from_slice(guest_name.as_bytes());
            guest_env.push(b'=');
            guest_env.extend_from_slice(binding.placeholder.as_bytes());
            guest_env.push(b'\n');
            secrets.insert(binding);
        }
        for group in &self.required_mediated_env_groups {
            if group.is_disjoint(&materialized_mediated_names) {
                return Err(Error::MissingCustomEnvAuth {
                    names: group.iter().cloned().collect::<Vec<_>>().join(", "),
                });
            }
        }
        Ok(MaterializedHostInput {
            contents: guest_env,
            secrets,
        })
    }

    #[must_use]
    pub fn has_mediated_secret_inputs(&self) -> bool {
        !self.mediated_env_secrets.is_empty() || self.files.iter().any(HostInputFile::produces_secrets)
    }

    #[must_use]
    pub fn has_mediated_env_secret_inputs(&self) -> bool {
        !self.mediated_env_secrets.is_empty()
    }
}

#[derive(Clone, Copy, Default)]
pub struct MaterializationContext<'a> {
    existing_secrets: Option<&'a SecretBindings>,
}

impl<'a> MaterializationContext<'a> {
    #[must_use]
    pub const fn new(existing_secrets: &'a SecretBindings) -> Self {
        Self {
            existing_secrets: Some(existing_secrets),
        }
    }

    #[must_use]
    pub fn placeholder_for_name(&self, name: &str) -> Option<&str> {
        self.existing_secrets
            .and_then(|secrets| secrets.placeholder_for_name(name))
    }
}

pub trait HostInputTransform: Sync {
    fn name(&self) -> &'static str;

    fn produces_secrets(&self) -> bool {
        false
    }

    /// Materializes host bytes into guest seed bytes and optional mediated secrets.
    ///
    /// # Errors
    ///
    /// Returns an error when the input bytes cannot be parsed or transformed
    /// according to the plugin-owned host input contract.
    fn materialize(
        &self,
        label: &str,
        contents: &[u8],
        context: MaterializationContext<'_>,
    ) -> Result<MaterializedHostInput, Error>;
}

#[derive(Clone, Copy)]
enum HostInputTransformRef {
    Copy,
    Plugin(&'static dyn HostInputTransform),
}

impl HostInputTransformRef {
    fn name(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Plugin(transform) => transform.name(),
        }
    }

    fn produces_secrets(self) -> bool {
        match self {
            Self::Copy => false,
            Self::Plugin(transform) => transform.produces_secrets(),
        }
    }

    fn materialize(
        self,
        label: &str,
        contents: &[u8],
        context: MaterializationContext<'_>,
    ) -> Result<MaterializedHostInput, Error> {
        match self {
            Self::Copy => Ok(MaterializedHostInput {
                contents: contents.to_vec(),
                secrets: SecretBindings::default(),
            }),
            Self::Plugin(transform) => transform.materialize(label, contents, context),
        }
    }
}

#[derive(Clone)]
pub struct HostInputFile {
    label: String,
    source: HostInputFileSource,
    guest_path: HostInputGuestPath,
    permissions: String,
    transform: HostInputTransformRef,
}

impl fmt::Debug for HostInputFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HostInputFile")
            .field("label", &self.label)
            .field("source", &self.source)
            .field("guest_path", &self.guest_path)
            .field("permissions", &self.permissions)
            .field("transform", &self.transform.name())
            .finish()
    }
}

impl PartialEq for HostInputFile {
    fn eq(&self, other: &Self) -> bool {
        self.label == other.label
            && self.source == other.source
            && self.guest_path == other.guest_path
            && self.permissions == other.permissions
            && self.transform.name() == other.transform.name()
    }
}

impl Eq for HostInputFile {}

impl HostInputFile {
    #[must_use]
    pub(crate) fn copy(
        label: impl Into<String>,
        source: HostInputFileSource,
        guest_path: HostInputGuestPath,
        permissions: impl Into<String>,
    ) -> Self {
        Self {
            label: label.into(),
            source,
            guest_path,
            permissions: permissions.into(),
            transform: HostInputTransformRef::Copy,
        }
    }

    #[must_use]
    pub(crate) fn with_transform(
        label: impl Into<String>,
        source: HostInputFileSource,
        guest_path: HostInputGuestPath,
        permissions: impl Into<String>,
        transform: &'static dyn HostInputTransform,
    ) -> Self {
        Self {
            label: label.into(),
            source,
            guest_path,
            permissions: permissions.into(),
            transform: HostInputTransformRef::Plugin(transform),
        }
    }

    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    #[must_use]
    pub const fn source(&self) -> &HostInputFileSource {
        &self.source
    }

    #[must_use]
    pub const fn guest_path(&self) -> &HostInputGuestPath {
        &self.guest_path
    }

    #[must_use]
    pub fn permissions(&self) -> &str {
        &self.permissions
    }

    #[must_use]
    pub fn produces_secrets(&self) -> bool {
        self.transform.produces_secrets()
    }

    /// Materializes this host input for seed-file emission.
    ///
    /// # Errors
    ///
    /// Returns an error when the source bytes cannot be parsed or transformed
    /// according to this input's provisioning contract.
    pub fn materialize(
        &self,
        contents: &[u8],
        context: MaterializationContext<'_>,
    ) -> Result<MaterializedHostInput, Error> {
        self.transform.materialize(&self.label, contents, context)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostInputFileSource {
    HomeRelative {
        path_env: Option<String>,
        home_env: Option<String>,
        home_relative_path: String,
        default_home_relative_path: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostInputGuestPath {
    AgentHomeRelative(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedHostInput {
    pub contents: Vec<u8>,
    pub secrets: SecretBindings,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to materialize host input {label} with {materializer}: {source}")]
    Materialize {
        label: String,
        materializer: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("custom env variable {name} is requested for both copy-from-host and mediated auth")]
    ConflictingCustomEnvAuth { name: String },
    #[error("custom env must contain at least one mediated auth variable from: {names}")]
    MissingCustomEnvAuth { names: String },
    #[error("{0}")]
    Secret(#[from] super::secrets::Error),
}

struct CustomEnvAssignment<'a> {
    prefix: &'static str,
    name: &'a str,
    value: &'a str,
}

fn parse_custom_env_assignment(line: &str) -> Option<CustomEnvAssignment<'_>> {
    let trimmed = line.trim();
    let (prefix, assignment) = match trimmed.strip_prefix("export") {
        Some(rest) if rest.starts_with(char::is_whitespace) => ("export ", rest.trim_start()),
        _ => ("", trimmed),
    };
    let (name, value) = assignment.split_once('=')?;
    let name = name.trim();
    (!name.is_empty()).then_some(CustomEnvAssignment { prefix, name, value })
}

fn unquote_env_value(value: &str) -> String {
    let quoted = (value.starts_with('"') && value.ends_with('"')) || (value.starts_with('\'') && value.ends_with('\''));
    if quoted && value.len() >= 2 {
        value[1..value.len() - 1].to_owned()
    } else {
        value.to_owned()
    }
}
