use crate::manifest::plugins::AuthMode;
use crate::manifest::plugins::github::GitHub;

use super::Plugin;
use crate::provisioning::bootstrap::ProvisioningBuilder;

impl Plugin for GitHub {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        builder.add_package("github-cli");
        match self.auth {
            AuthMode::CopyFromHost | AuthMode::Mediated if self.setup_git => builder.add_agent_shell(
                "if [ -f \"$HOME/.config/gh/hosts.yml\" ]; then gh auth setup-git >/dev/null || true; fi",
            ),
            _ => {}
        }
    }
}
