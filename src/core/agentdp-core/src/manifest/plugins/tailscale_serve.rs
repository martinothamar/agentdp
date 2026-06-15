use serde::{Deserialize, Serialize};

use crate::manifest::Network;

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TailscaleServe {
    #[serde(default)]
    pub routes: Vec<Route>,
}

impl TailscaleServe {
    pub(super) fn validate(&self, network: &Network, errors: &mut Vec<String>) {
        if self.routes.is_empty() {
            errors.push("plugins.tailscale_serve.routes must declare at least one route".to_owned());
        }

        for (index, route) in self.routes.iter().enumerate() {
            route.validate(index, network, errors);
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub service: String,
    #[serde(default)]
    pub host_template: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub mode: RouteMode,
}

impl Route {
    fn validate(&self, index: usize, network: &Network, errors: &mut Vec<String>) {
        let field = format!("plugins.tailscale_serve.routes[{index}]");
        super::super::validate_identifier(&format!("{field}.service"), &self.service, errors);
        if !network.ports.contains_key(&self.service) {
            errors.push(format!(
                "{field}.service references unknown network port `{}`",
                self.service
            ));
        }
        if let Some(host_template) = &self.host_template {
            super::super::validate_non_empty(&format!("{field}.host_template"), host_template, errors);
        }
        if let Some(path) = &self.path {
            super::super::validate_non_empty(&format!("{field}.path"), path, errors);
            if !path.starts_with('/') {
                errors.push(format!("{field}.path must start with '/'"));
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RouteMode {
    #[default]
    Direct,
    Proxy,
}

impl std::fmt::Display for RouteMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Direct => "direct",
            Self::Proxy => "proxy",
        })
    }
}
