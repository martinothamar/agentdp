use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Node {
    #[serde(default)]
    pub from_mise: bool,
    #[serde(default)]
    pub corepack: bool,
}
