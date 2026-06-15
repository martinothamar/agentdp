use crate::manifest::plugins::node::Node;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

const MISE_NODE_PACKAGE: &str = "node@lts";

pub(super) fn apply(plugin: &Node, builder: &mut ProvisioningBuilder<'_>) {
    if plugin.from_mise {
        builder.require_mise_package(MISE_NODE_PACKAGE);
    } else {
        builder.add_package("nodejs");
        builder.add_package("npm");
    }

    if plugin.corepack {
        if plugin.from_mise {
            builder.add_base_user_step(
                "plugin.node.corepack",
                "Enable Node Corepack",
                ["plugin.mise"],
                [
                    BootstrapStepResource::AgentHome,
                    BootstrapStepResource::Mise,
                    BootstrapStepResource::NpmGlobal,
                ],
                render_corepack_setup(),
            );
        } else {
            builder.add_base_system_step(
                "plugin.node.corepack",
                "Enable Node Corepack",
                ["system.packages"],
                [BootstrapStepResource::NpmGlobal],
                render_corepack_setup(),
            );
        }
    }
}

fn render_corepack_setup() -> String {
    let mut script = shell::ShellScript::new();
    script.line("corepack enable");
    script.line("if command -v mise >/dev/null 2>&1; then");
    script.line("  mise reshim node");
    script.line("fi");
    script.render()
}
