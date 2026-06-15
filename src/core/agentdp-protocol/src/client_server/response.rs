use serde::de::{self, DeserializeOwned};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;

use crate::Error;

#[derive(Debug, Clone, Serialize)]
pub struct Response {
    id: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Box<RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorObject>,
}

impl Response {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub const fn is_ok(&self) -> bool {
        self.ok
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !self.ok
    }

    #[must_use]
    pub const fn error(&self) -> Option<&ErrorObject> {
        self.error.as_ref()
    }

    #[must_use]
    pub fn into_error(self) -> Option<ErrorObject> {
        self.error
    }

    #[must_use]
    pub(crate) fn success(id: impl Into<String>, result: impl Serialize) -> Self {
        let id = id.into();
        let result = match serde_json::value::to_raw_value(&result) {
            Ok(result) => result,
            Err(error) => return Self::failure(id, "result_encode_failed", error.to_string()),
        };
        Self {
            id,
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    #[must_use]
    pub(crate) fn failure(id: impl Into<String>, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            ok: false,
            result: None,
            error: Some(ErrorObject {
                code: code.into(),
                message: message.into(),
            }),
        }
    }

    /// Decodes the success result body into the caller-selected type.
    ///
    /// # Errors
    ///
    /// Returns an error when the response has no result body or when the body
    /// does not match the requested type.
    pub fn result<T: DeserializeOwned>(self) -> Result<T, Error> {
        let value = self.result.ok_or(Error::MissingResult)?;
        serde_json::from_str(value.get()).map_err(Error::ResultDecode)
    }
}

impl<'de> Deserialize<'de> for Response {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let decoded = ResponseEnvelope::deserialize(deserializer)?;
        match (decoded.ok, decoded.result, decoded.error) {
            (true, Some(result), None) => Ok(Self {
                id: decoded.id,
                ok: true,
                result: Some(result),
                error: None,
            }),
            (false, None, Some(error)) => Ok(Self {
                id: decoded.id,
                ok: false,
                result: None,
                error: Some(error),
            }),
            (true, None, None) => Err(de::Error::custom("successful response requires result payload")),
            (false, None, None) => Err(de::Error::custom("failed response requires error payload")),
            (true, None | Some(_), Some(_)) => {
                Err(de::Error::custom("successful response does not accept error payload"))
            }
            (false, Some(_), None | Some(_)) => {
                Err(de::Error::custom("failed response does not accept result payload"))
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseEnvelope {
    id: String,
    ok: bool,
    result: Option<Box<RawValue>>,
    error: Option<ErrorObject>,
}

#[must_use]
pub fn invalid_request(message: impl Into<String>) -> Response {
    Response::failure("unknown", "invalid_request", message)
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ErrorObject {
    pub code: String,
    pub message: String,
}
