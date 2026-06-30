use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("secret binding name must not be empty")]
    EmptySecretName,
    #[error("secret binding {0} has an empty value")]
    EmptySecretValue(String),
    #[error("failed to generate secret placeholder: {0}")]
    RandomPlaceholder(agentdp_platform::rand::Error),
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SecretBinding {
    pub name: String,
    pub placeholder: String,
    #[serde(default)]
    value: SecretValue,
    pub allowed_hosts: BTreeSet<String>,
}

#[derive(Clone, Default, PartialEq, Eq)]
enum SecretValue {
    Present(String),
    #[default]
    Redacted,
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl Serialize for SecretValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("<redacted>")
    }
}

impl<'de> Deserialize<'de> for SecretValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _redacted = String::deserialize(deserializer)?;
        Ok(Self::Redacted)
    }
}

impl fmt::Debug for SecretBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretBinding")
            .field("name", &self.name)
            .field("placeholder", &self.placeholder)
            .field("value", &"<redacted>")
            .field("allowed_hosts", &self.allowed_hosts)
            .finish()
    }
}

impl SecretBinding {
    /// Creates a host-owned secret binding with a guest-visible placeholder.
    ///
    /// # Errors
    ///
    /// Returns an error when the binding name or value is empty.
    pub fn new(name: impl Into<String>, value: impl Into<String>, allowed_hosts: &[String]) -> Result<Self, Error> {
        Self::new_with_placeholder(name, None, value, allowed_hosts)
    }

    /// Creates a host-owned secret binding with an explicit guest-visible placeholder.
    ///
    /// # Errors
    ///
    /// Returns an error when the binding name, placeholder, or value is empty.
    pub fn new_with_placeholder(
        name: impl Into<String>,
        placeholder: Option<String>,
        value: impl Into<String>,
        allowed_hosts: &[String],
    ) -> Result<Self, Error> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::EmptySecretName);
        }
        let value = value.into();
        if value.is_empty() {
            return Err(Error::EmptySecretValue(name));
        }
        let placeholder = match placeholder {
            Some(placeholder) if placeholder.is_empty() => return Err(Error::EmptySecretValue(name)),
            Some(placeholder) => placeholder,
            None => placeholder_for(&name)?,
        };
        Ok(Self {
            placeholder,
            name,
            value: SecretValue::Present(value),
            allowed_hosts: allowed_hosts.iter().map(|host| normalized_host(host)).collect(),
        })
    }

    #[must_use]
    pub fn allows_host(&self, host: &str) -> bool {
        self.allowed_hosts.contains(&normalized_host(host))
    }

    #[must_use]
    pub fn value(&self) -> Option<&str> {
        match &self.value {
            SecretValue::Present(value) => Some(value),
            SecretValue::Redacted => None,
        }
    }

    #[must_use]
    pub fn redacted(&self) -> Self {
        let mut binding = self.clone();
        binding.value = SecretValue::Redacted;
        binding
    }
}

#[derive(Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SecretBindings {
    bindings: BTreeMap<String, SecretBinding>,
}

impl fmt::Debug for SecretBindings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretBindings")
            .field("bindings", &self.bindings)
            .finish()
    }
}

impl SecretBindings {
    /// Builds secret bindings from dotenv-style `NAME=value` bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if any parsed binding has an empty name or value.
    pub fn from_env_bytes(contents: &[u8], allowed_hosts: &[String]) -> Result<Self, Error> {
        let text = String::from_utf8_lossy(contents);
        let mut bindings = Self::default();
        for line in text.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((name, value)) = line.split_once('=') else {
                continue;
            };
            bindings.insert(SecretBinding::new(
                name.trim(),
                unquote_env_value(value.trim()),
                allowed_hosts,
            )?);
        }
        Ok(bindings)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    #[must_use]
    pub fn max_placeholder_len(&self) -> usize {
        self.bindings
            .values()
            .map(|binding| binding.placeholder.len())
            .max()
            .unwrap_or(0)
    }

    pub fn insert(&mut self, binding: SecretBinding) {
        self.bindings.insert(binding.placeholder.clone(), binding);
    }

    pub fn extend(&mut self, bindings: Self) {
        self.bindings.extend(bindings.bindings);
    }

    #[must_use]
    pub fn redacted(&self) -> Self {
        let bindings = self
            .bindings
            .iter()
            .map(|(placeholder, binding)| (placeholder.clone(), binding.redacted()))
            .collect();
        Self { bindings }
    }

    #[must_use]
    pub fn contains_placeholder(&self, placeholder: &str) -> bool {
        self.bindings.contains_key(placeholder)
    }

    pub fn iter(&self) -> impl Iterator<Item = &SecretBinding> {
        self.bindings.values()
    }

    #[must_use]
    pub fn placeholder_for_name(&self, name: &str) -> Option<&str> {
        self.bindings
            .values()
            .find(|binding| binding.name == name)
            .map(|binding| binding.placeholder.as_str())
    }

    #[must_use]
    pub fn guest_env_contents(&self) -> Vec<u8> {
        let mut contents = String::new();
        for binding in self.bindings.values() {
            contents.push_str(&binding.name);
            contents.push('=');
            contents.push_str(&binding.placeholder);
            contents.push('\n');
        }
        contents.into_bytes()
    }
}

fn placeholder_for(name: &str) -> Result<String, Error> {
    let normalized = name
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'0'..=b'9' => char::from(byte),
            b'a'..=b'z' => char::from(byte.to_ascii_uppercase()),
            _ => '_',
        })
        .collect::<String>();
    let mut bytes = [0_u8; 16];
    agentdp_platform::rand::fill(&mut bytes).map_err(Error::RandomPlaceholder)?;
    let mut suffix = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        suffix.push(hex_digit(byte >> 4));
        suffix.push(hex_digit(byte & 0x0f));
    }
    Ok(format!("AGENTDP_SECRET_{normalized}_{suffix}"))
}

const fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'A' + (value - 10)) as char,
        _ => '?',
    }
}

fn unquote_env_value(value: &str) -> String {
    let quoted = (value.starts_with('"') && value.ends_with('"')) || (value.starts_with('\'') && value.ends_with('\''));
    if quoted && value.len() >= 2 {
        value[1..value.len() - 1].to_owned()
    } else {
        value.to_owned()
    }
}

fn normalized_host(host: &str) -> String {
    host.to_ascii_lowercase()
}
