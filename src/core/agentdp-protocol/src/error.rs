use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to encode protocol message: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("failed to decode protocol message: {0}")]
    Decode(#[source] serde_json::Error),
    #[error("failed to read protocol frame: {0}")]
    Read(#[source] std::io::Error),
    #[error("invalid protocol frame: {0}")]
    Frame(String),
    #[error("failed to decode protocol result: {0}")]
    ResultDecode(#[source] serde_json::Error),
    #[error("success response omitted result body")]
    MissingResult,
}
