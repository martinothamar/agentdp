use crate::manifest::plugins::dotnet::DotNet;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

pub(super) fn apply(plugin: &DotNet, builder: &mut ProvisioningBuilder<'_>) {
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
        .map(|tool| {
            let tool = shell::single_quote(tool);
            format!("dotnet tool install --global {tool} || dotnet tool update --global {tool}")
        })
        .collect::<Vec<_>>()
        .join("\n");

    builder.add_base_user_step(
        "plugin.dotnet.tools",
        "Install .NET tools",
        depends_on,
        [BootstrapStepResource::AgentHome],
        install,
    );
    if plugin.tools.iter().any(|tool| tool == "dotnet-sos") {
        builder.add_base_user_step(
            "plugin.dotnet.sos",
            "Configure .NET SOS",
            ["plugin.dotnet.tools"],
            [BootstrapStepResource::AgentHome],
            "dotnet sos install || true",
        );
    }
}
