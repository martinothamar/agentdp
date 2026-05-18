mod codex;
mod docker;
mod github;
mod mise;
mod vscode;

use crate::manifest::plugins::Plugins;

use super::bootstrap::ProvisioningBuilder;

pub(super) fn apply(plugins: &Plugins, builder: &mut ProvisioningBuilder<'_>) {
    if let Some(docker) = &plugins.docker {
        docker.apply(builder);
    }
    if let Some(mise) = &plugins.mise {
        mise.apply(builder);
    }
    if let Some(codex) = &plugins.codex {
        codex.apply(builder);
    }
    if let Some(github) = &plugins.github {
        github.apply(builder);
    }
    if let Some(vscode) = &plugins.vscode {
        vscode.apply(builder);
    }
}

trait Plugin {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>);
}
