use crate::manifest::User;
use crate::manifest::plugins::code_server::CodeServer;

use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::{paths, shell};
use crate::provisioning::template;
use agentdp_protocol::server_guest::BootstrapStepResource;

const CODE_SERVER_SERVICE: &str = include_str!("resources/linux/code-server.service");

pub(super) fn apply(plugin: &CodeServer, builder: &mut ProvisioningBuilder<'_>) {
    let Some(guest_port) = builder.guest_port("code_server") else {
        return;
    };
    let user_name = builder.agent_user().name.clone();
    let primary_group = linux_primary_group(builder.agent_user()).to_owned();
    let layout = builder.guest_layout();
    builder.add_package("curl");
    builder.add_base_system_step(
        "plugin.code_server.setup",
        "Install code-server",
        ["system.agent_user"],
        [BootstrapStepResource::Systemd],
        render_code_server_setup(
            &user_name,
            &primary_group,
            layout.agent_home,
            layout.code_dir,
            guest_port,
        ),
    );
    if let Some(settings) = &plugin.settings
        && let Some(guest_path) = guest_home_seed_path(layout.agent_home, settings)
    {
        builder.add_instance_system_step(
            "plugin.code_server.settings_owner",
            "Prepare code-server settings",
            ["system.agent_user"],
            [BootstrapStepResource::AgentHome],
            render_code_server_settings_owner(&user_name, &primary_group, &guest_path),
        );
    }
    if !plugin.trusted_domains.is_empty() {
        builder.add_instance_system_step(
            "plugin.code_server.config",
            "Configure code-server",
            ["plugin.code_server.setup"],
            [BootstrapStepResource::AgentHome],
            render_code_server_config(&user_name, &primary_group, layout.agent_home, &plugin.trusted_domains),
        );
    }
    if !plugin.extensions.is_empty() {
        builder.add_base_user_step(
            "plugin.code_server.extensions",
            "Install code-server extensions",
            ["plugin.code_server.setup"],
            [BootstrapStepResource::AgentHome],
            render_extension_install(&plugin.extensions),
        );
    }
    if !plugin.remove_extensions.is_empty() {
        builder.add_base_user_step(
            "plugin.code_server.remove_extensions",
            "Remove code-server extensions",
            ["plugin.code_server.setup"],
            [BootstrapStepResource::AgentHome],
            render_extension_removal(&plugin.remove_extensions),
        );
    }
    if plugin.restart_after_bootstrap {
        builder.add_instance_system_step(
            "plugin.code_server.restart",
            "Restart code-server",
            ["plugin.code_server.setup"],
            [BootstrapStepResource::Systemd],
            "systemctl restart code-server.service >/dev/null 2>&1 || true",
        );
    }
}

fn render_extension_install(extensions: &[String]) -> String {
    extensions
        .iter()
        .map(|extension| {
            format!(
                "code-server --install-extension {} || true",
                shell::single_quote(extension)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_extension_removal(extensions: &[String]) -> String {
    let mut script = shell::ShellScript::new();
    for extension in extensions {
        script.line(format!(
            "code-server --uninstall-extension {} >/dev/null 2>&1 || true",
            shell::single_quote(extension)
        ));
        script.line(format!(
            "rm -rf \"$HOME/.local/share/code-server/extensions/{}\"*",
            shell::double_quoted_fragment(extension)
        ));
    }
    script.render()
}

fn linux_primary_group(user: &User) -> &str {
    user.linux().group.as_deref().unwrap_or(&user.name)
}

fn render_code_server_setup(user: &str, group: &str, agent_home: &str, code_dir: &str, guest_port: u16) -> String {
    let unit = template::render(
        CODE_SERVER_SERVICE,
        &[
            ("{{user}}", user.to_owned()),
            ("{{group}}", group.to_owned()),
            ("{{code_dir}}", code_dir.to_owned()),
            ("{{agent_home}}", agent_home.to_owned()),
            ("{{guest_port}}", guest_port.to_string()),
        ],
    );
    let mut script = shell::ShellScript::new();
    script.line("if ! command -v code-server >/dev/null 2>&1; then");
    script.line(format!(
        "  curl -fsSL https://code-server.dev/install.sh | sh -s -- --method=standalone --prefix={}",
        shell::single_quote(paths::USR_LOCAL_PREFIX)
    ));
    script.line("fi");
    script.line("cat >/etc/systemd/system/code-server.service <<'EOF'");
    script.block(&unit);
    script.line("EOF");
    script.line("systemctl daemon-reload");
    script.line("systemctl enable --now code-server.service");
    script.render()
}

fn render_code_server_config(user: &str, group: &str, agent_home: &str, trusted_domains: &[String]) -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "install -d -o {} -g {} -m 0700 {}/.config/code-server",
        shell::single_quote(user),
        shell::single_quote(group),
        shell::single_quote(agent_home)
    ));
    script.line(format!(
        "cat >{}/.config/code-server/config.yaml <<'EOF'",
        shell::single_quote(agent_home)
    ));
    script.line("link-protection-trusted-domains:");
    for domain in trusted_domains {
        script.line(format!("  - \"{}\"", shell::double_quoted_fragment(domain)));
    }
    script.line("EOF");
    script.line(format!(
        "chown {}:{} {}/.config/code-server/config.yaml",
        shell::single_quote(user),
        shell::single_quote(group),
        shell::single_quote(agent_home)
    ));
    script.render()
}

fn render_code_server_settings_owner(user: &str, group: &str, settings_path: &str) -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!("if [ -f {} ]; then", shell::single_quote(settings_path)));
    script.line(format!(
        "  chown {}:{} {}",
        shell::single_quote(user),
        shell::single_quote(group),
        shell::single_quote(settings_path)
    ));
    script.line(format!("  chmod 0600 {}", shell::single_quote(settings_path)));
    script.line("fi");
    script.render()
}

fn guest_home_seed_path(agent_home: &str, settings: &str) -> Option<String> {
    let settings = settings.replace('\\', "/");
    let relative = settings
        .strip_prefix("data/home/")
        .or_else(|| settings.strip_prefix("data/home"))?;
    if relative.is_empty() {
        Some(agent_home.to_owned())
    } else {
        Some(format!("{}/{}", agent_home.trim_end_matches('/'), relative))
    }
}
