use crate::manifest::GuestOs;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

const MISE_SHIMS_SUFFIX: &str = ".local/share/mise/shims";
const MISE_DOTNET_INSTALLS_SUFFIX: &str = ".local/share/mise/installs/dotnet";

pub(super) fn apply_requirements(builder: &mut ProvisioningBuilder<'_>) {
    match builder.guest_os() {
        GuestOs::Archlinux => builder.add_package("mise"),
        GuestOs::Rocky9 => builder.add_package("curl"),
    }
    builder.add_agent_shell_env(render_mise_agent_env(builder.guest_layout().agent_home));

    builder.add_base_user_step(
        "plugin.mise",
        "Install mise runtimes",
        ["system.agent_user"],
        [BootstrapStepResource::AgentHome, BootstrapStepResource::Mise],
        render_mise_setup(builder.guest_os(), &dedupe(builder.mise_packages())),
    );
}

fn render_mise_setup(guest_os: GuestOs, packages: &[String]) -> String {
    let mut lines = Vec::new();
    if guest_os == GuestOs::Rocky9 {
        lines.push("if ! command -v mise >/dev/null 2>&1; then".to_owned());
        lines.push("  curl https://mise.run | sh".to_owned());
        lines.push("fi".to_owned());
    }
    lines.push("if command -v mise >/dev/null 2>&1; then".to_owned());
    for package in packages {
        lines.push(format!("  mise use --global {}", shell::single_quote(package)));
    }
    lines.push("  mise reshim".to_owned());
    lines.push("fi".to_owned());
    lines.join("\n")
}

fn render_mise_agent_env(agent_home: &str) -> String {
    let shims = format!("{agent_home}/{MISE_SHIMS_SUFFIX}");
    let dotnet = format!("{agent_home}/{MISE_DOTNET_INSTALLS_SUFFIX}");
    format!(
        concat!(
            "agentdp_prepend_path {shims}\n",
            "\n",
            "if [ -d {dotnet} ]; then\n",
            "  agentdp_dotnet_root=\"$(find {dotnet} -mindepth 1 -maxdepth 1 -type d | sort | tail -n 1)\"\n",
            "  if [ -n \"$agentdp_dotnet_root\" ]; then\n",
            "    export DOTNET_ROOT=\"$agentdp_dotnet_root\"\n",
            "    export DOTNET_ROOT_X64=\"$DOTNET_ROOT\"\n",
            "  fi\n",
            "  unset agentdp_dotnet_root\n",
            "fi"
        ),
        shims = shell::single_quote(&shims),
        dotnet = shell::single_quote(&dotnet),
    )
}

fn dedupe(packages: &[String]) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    packages
        .iter()
        .filter(|package| seen.insert((*package).clone()))
        .cloned()
        .collect()
}
