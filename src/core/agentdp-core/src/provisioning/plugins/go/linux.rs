use crate::manifest::plugins::go::Go;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

pub(super) fn apply(plugin: &Go, builder: &mut ProvisioningBuilder<'_>) {
    if plugin.from_mise {
        builder.require_mise();
    }
    if plugin.tools.is_empty() {
        return;
    }

    let depends_on = if plugin.from_mise {
        ["plugin.mise"]
    } else {
        ["system.agent_user"]
    };
    let install = plugin
        .tools
        .iter()
        .map(|tool| format!("go install {}", shell::single_quote(tool)))
        .collect::<Vec<_>>()
        .join("\n");

    builder.add_base_user_step(
        "plugin.go.tools",
        "Install Go tools",
        depends_on,
        [BootstrapStepResource::AgentHome],
        install,
    );
}
