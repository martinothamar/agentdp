use crate::manifest::plugins::mise::Mise;

use super::Plugin;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::shell;

impl Plugin for Mise {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        builder.add_package("mise");
        if self.packages.is_empty() {
            return;
        }

        let mut lines = vec!["if command -v mise >/dev/null 2>&1; then".to_owned()];
        for package in &self.packages {
            lines.push(format!("  mise use --global {}", shell::single_quote(package)));
        }
        lines.push("fi".to_owned());
        builder.add_agent_shell(lines.join("\n"));
    }
}
