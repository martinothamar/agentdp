mod browser;
mod claude;
mod code_server;
mod codex;
mod docker;
mod dotnet;
mod git;
mod github;
mod go;
mod mise;
mod node;
mod podman;

use crate::manifest::plugins::Plugins;

use super::bootstrap::ProvisioningBuilder;

pub(super) fn apply(plugins: &Plugins, builder: &mut ProvisioningBuilder<'_>) {
    if let Some(docker) = &plugins.docker {
        docker.apply(builder);
    }
    if let Some(mise) = &plugins.mise {
        mise.apply(builder);
    }
    if let Some(node) = &plugins.node {
        node.apply(builder);
    }
    if let Some(podman) = &plugins.podman {
        podman.apply(builder);
    }
    if let Some(browser) = &plugins.browser {
        browser.apply(builder);
    }
    if let Some(dotnet) = &plugins.dotnet {
        dotnet.apply(builder);
    }
    if let Some(git) = &plugins.git {
        git.apply(builder);
    }
    if let Some(go) = &plugins.go {
        go.apply(builder);
    }
    if let Some(claude) = &plugins.claude {
        claude.apply(builder);
    }
    browser::apply_claude_integration(plugins, builder);
    if let Some(codex) = &plugins.codex {
        codex.apply(builder);
    }
    browser::apply_codex_integration(plugins, builder);
    if let Some(github) = &plugins.github {
        github.apply(builder);
    }
    if let Some(code_server) = &plugins.code_server {
        code_server.apply(builder);
    }
}

pub(super) fn apply_runtime_requirements(builder: &mut ProvisioningBuilder<'_>) {
    mise::apply_requirements(builder);
}

trait Plugin {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>);
}
