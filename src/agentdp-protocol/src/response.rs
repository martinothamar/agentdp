use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::Error;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Response {
    pub id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Box<RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorObject>,
}

impl Response {
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

#[must_use]
pub fn invalid_request(message: impl Into<String>) -> Response {
    Response::failure("unknown", "invalid_request", message)
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ErrorObject {
    pub code: String,
    pub message: String,
}
