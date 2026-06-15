use std::fmt;

const REDACTED_SECRET: &str = "<redacted>";

#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretString {
    value: String,
}

impl SecretString {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self { value: value.into() }
    }

    #[must_use]
    pub const fn empty() -> Self {
        Self { value: String::new() }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.value
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(REDACTED_SECRET)
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(REDACTED_SECRET)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(REDACTED_SECRET)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        if value == REDACTED_SECRET {
            return Ok(Self::empty());
        }
        Ok(Self::new(value))
    }
}

#[cfg(all(test, feature = "serde"))]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::SecretString;

    #[test]
    fn debug_and_display_redact_secret() {
        let secret = SecretString::new("actual-secret");

        assert_eq!(format!("{secret:?}"), "<redacted>");
        assert_eq!(secret.to_string(), "<redacted>");
    }

    #[test]
    fn serde_serializes_redacted_value() {
        #[derive(Serialize)]
        struct Document {
            secret: SecretString,
        }

        let json = serde_json::to_string(&Document {
            secret: SecretString::new("actual-secret"),
        })
        .unwrap();

        assert!(json.contains("<redacted>"));
        assert!(!json.contains("actual-secret"));
    }

    #[test]
    fn serde_deserializes_into_exposed_secret() {
        #[derive(Deserialize)]
        struct Document {
            secret: SecretString,
        }

        let document = serde_json::from_str::<Document>(r#"{"secret":"actual-secret"}"#).unwrap();

        assert_eq!(document.secret.expose_secret(), "actual-secret");
    }

    #[test]
    fn serde_deserializes_redacted_marker_as_empty() {
        #[derive(Deserialize)]
        struct Document {
            secret: SecretString,
        }

        let document = serde_json::from_str::<Document>(r#"{"secret":"<redacted>"}"#).unwrap();

        assert!(document.secret.is_empty());
    }
}
