use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Docker {
    #[serde(default)]
    pub compose: bool,
    #[serde(default)]
    pub buildx: bool,
    #[serde(default)]
    pub healthcheck: bool,
}
