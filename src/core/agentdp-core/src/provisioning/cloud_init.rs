use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudInitSeed {
    pub meta_data: String,
    pub network_config: String,
    pub user_data: String,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to render cloud-init meta-data: {0}")]
    MetaData(#[source] serde_yaml::Error),
    #[error("failed to render cloud-init user-data: {0}")]
    UserData(#[source] serde_yaml::Error),
}

#[derive(Debug, Serialize)]
struct MetaData<'a> {
    #[serde(rename = "instance-id")]
    instance_id: &'a str,
    #[serde(rename = "local-hostname")]
    local_hostname: String,
}

/// Renders cloud-init `NoCloud` metadata for an instance.
///
/// # Errors
///
/// Returns an error when the metadata cannot be serialized as YAML.
pub fn render_meta_data(instance_id: &str, hostname: &str) -> Result<String, Error> {
    let meta_data = MetaData {
        instance_id,
        local_hostname: hostname_from_name(hostname),
    };
    serde_yaml::to_string(&meta_data).map_err(Error::MetaData)
}

fn hostname_from_name(name: &str) -> String {
    name.bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || byte == b'-' {
                char::from(byte)
            } else {
                '-'
            }
        })
        .collect()
}
