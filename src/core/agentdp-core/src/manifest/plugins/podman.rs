use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Podman {
    #[serde(default)]
    pub compose: bool,
    #[serde(default)]
    pub docker_api: bool,
}
