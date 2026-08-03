use serde::{Deserialize, Serialize};

use crate::manifest::{Network, NetworkProtocol};

pub const SERVICE: &str = "agent_host";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentHost {}

impl AgentHost {
    pub(super) fn validate(network: &Network, errors: &mut Vec<String>) {
        match network.ports.get(SERVICE) {
            Some(port) if port.protocol != NetworkProtocol::Tcp => {
                errors.push(format!(
                    "plugins.agent_host requires network port `{SERVICE}` to use TCP"
                ));
            }
            Some(_) => {}
            None => errors.push(format!("plugins.agent_host requires network port `{SERVICE}`")),
        }
    }
}
