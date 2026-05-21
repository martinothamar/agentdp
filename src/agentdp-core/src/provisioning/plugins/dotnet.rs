use crate::manifest::plugins::dotnet::DotNet;

use super::Plugin;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::shell;

impl Plugin for DotNet {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        if self.from_mise {
            builder.add_package("mise");
        }
        if self.tools.is_empty() {
            return;
        }

        let mut lines = Vec::new();
        for tool in &self.tools {
            lines.push(format!(
                "dotnet tool install --global {} || dotnet tool update --global {}",
                shell::single_quote(tool),
                shell::single_quote(tool)
            ));
        }
        if self.tools.iter().any(|tool| tool == "dotnet-sos") {
            lines.push("dotnet sos install || true".to_owned());
        }
        builder.add_agent_shell(lines.join("\n"));
    }
}
