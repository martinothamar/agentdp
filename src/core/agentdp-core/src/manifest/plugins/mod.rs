pub mod browser;
pub mod code_server;
pub mod codex;
pub mod docker;
pub mod dotnet;
pub mod git;
pub mod github;
pub mod go;
pub mod mise;
pub mod node;
pub mod podman;
pub mod tailscale_serve;

use serde::{Deserialize, Serialize};

use crate::provisioning::host_input::HostInputRequirements;

use super::Network;

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Plugins {
    pub docker: Option<docker::Docker>,
    pub dotnet: Option<dotnet::DotNet>,
    pub git: Option<git::Git>,
    pub go: Option<go::Go>,
    pub mise: Option<mise::Mise>,
    pub node: Option<node::Node>,
    pub podman: Option<podman::Podman>,
    pub browser: Option<browser::Browser>,
    pub codex: Option<codex::Codex>,
    pub github: Option<github::GitHub>,
    pub tailscale_serve: Option<tailscale_serve::TailscaleServe>,
    pub code_server: Option<code_server::CodeServer>,
}

impl Plugins {
    pub(super) fn validate(&self, network: &Network, errors: &mut Vec<String>) {
        if let Some(mise) = &self.mise {
            mise.validate(errors);
        }
        if let Some(dotnet) = &self.dotnet {
            dotnet.validate(errors);
        }
        if let Some(go) = &self.go {
            go.validate(errors);
        }
        if let Some(git) = &self.git {
            git.validate(errors);
        }
        if let Some(browser) = &self.browser {
            browser.validate(errors);
        }
        if let Some(code_server) = &self.code_server {
            code_server.validate(errors);
        }
        if let Some(tailscale_serve) = &self.tailscale_serve {
            tailscale_serve.validate(network, errors);
        }
    }

    #[must_use]
    pub fn host_input_requirements(&self) -> HostInputRequirements {
        let mut requirements = HostInputRequirements::default();
        if let Some(codex) = &self.codex {
            codex.host_input_requirements(&mut requirements);
        }
        if let Some(github) = &self.github {
            github.host_input_requirements(&mut requirements);
        }
        if let Some(git) = &self.git {
            git.host_input_requirements(&mut requirements);
        }
        requirements
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AuthMode {
    Mediated,
    CopyFromHost,
}
