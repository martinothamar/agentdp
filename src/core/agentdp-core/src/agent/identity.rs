use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("agent name must contain only ASCII letters, digits, '.', '_', and '-'")]
    InvalidAgentName,
    #[error("instance name must contain only ASCII letters, digits, '.', '_', and '-'")]
    InvalidInstanceName,
    #[error("agent instance id must be an unsigned integer")]
    InvalidAgentInstanceId,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentName(Arc<str>);

impl AgentName {
    #[must_use]
    pub fn new(value: impl AsRef<str>) -> Self {
        Self(Arc::from(value.as_ref()))
    }

    /// Parses and validates an agent name.
    ///
    /// # Errors
    ///
    /// Returns an error when the name is empty or contains unsupported bytes.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, IdentityError> {
        let value = value.as_ref();
        validate_identifier(value)
            .then(|| Self::new(value))
            .ok_or(IdentityError::InvalidAgentName)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for AgentName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for AgentName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("AgentName").field(&self.as_str()).finish()
    }
}

impl fmt::Display for AgentName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for AgentName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AgentName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstanceName(Arc<str>);

impl InstanceName {
    #[must_use]
    pub fn new(value: impl AsRef<str>) -> Self {
        Self(Arc::from(value.as_ref()))
    }

    /// Parses and validates an instance name.
    ///
    /// # Errors
    ///
    /// Returns an error when the name is empty or contains unsupported bytes.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, IdentityError> {
        let value = value.as_ref();
        validate_identifier(value)
            .then(|| Self::new(value))
            .ok_or(IdentityError::InvalidInstanceName)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for InstanceName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for InstanceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("InstanceName").field(&self.as_str()).finish()
    }
}

impl fmt::Display for InstanceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for InstanceName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for InstanceName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentBaseKey(Arc<str>);

impl AgentBaseKey {
    #[must_use]
    pub fn new(value: impl AsRef<str>) -> Self {
        Self(Arc::from(value.as_ref()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for AgentBaseKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for AgentBaseKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("AgentBaseKey").field(&self.as_str()).finish()
    }
}

impl fmt::Display for AgentBaseKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for AgentBaseKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AgentBaseKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::new(String::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct AgentInstanceId(u32);

impl AgentInstanceId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Parses an instance id from a path/API component.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is not an unsigned integer.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, IdentityError> {
        value
            .as_ref()
            .parse::<u32>()
            .map(Self)
            .map_err(|_| IdentityError::InvalidAgentInstanceId)
    }

    #[must_use]
    pub fn path_component(self) -> String {
        self.0.to_string()
    }
}

impl fmt::Display for AgentInstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

fn validate_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::{AgentName, InstanceName};

    #[test]
    fn agent_name_deserialization_rejects_invalid_identifiers() {
        let error = serde_json::from_str::<AgentName>(r#""bad/name""#).unwrap_err();

        assert!(error.to_string().contains("agent name"));
    }

    #[test]
    fn instance_name_deserialization_rejects_invalid_identifiers() {
        let error = serde_json::from_str::<InstanceName>(r#""bad/name""#).unwrap_err();

        assert!(error.to_string().contains("instance name"));
    }
}
