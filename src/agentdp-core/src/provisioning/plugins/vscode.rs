use crate::manifest::plugins::vscode::VsCode;

use super::Plugin;
use crate::provisioning::bootstrap::{AgentUserPlan, ProvisioningBuilder};
use crate::provisioning::{AGENT_HOME, CODE_DIR};
use crate::provisioning::{shell, templates};

impl Plugin for VsCode {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        let Some(guest_port) = builder.guest_port("code-server") else {
            return;
        };
        builder.add_package("curl");
        builder.add_root_shell(render_code_server_setup(builder.agent_user(), guest_port));
        if !self.trusted_domains.is_empty() {
            builder.add_root_shell(render_code_server_config(builder.agent_user(), &self.trusted_domains));
        }
        for extension in &self.extensions {
            builder.add_agent_shell(format!(
                "code-server --install-extension {} || true",
                shell::single_quote(extension)
            ));
        }
        if self.restart_after_bootstrap {
            builder.add_post_bootstrap_root_shell("systemctl restart code-server.service >/dev/null 2>&1 || true");
        }
    }
}

fn render_code_server_setup(user: &AgentUserPlan, guest_port: u16) -> String {
    let unit = shell::render_template(
        templates::CODE_SERVER_SERVICE,
        &[
            ("{{user}}", user.name.clone()),
            ("{{group}}", user.name.clone()),
            ("{{code_dir}}", CODE_DIR.to_owned()),
            ("{{agent_home}}", AGENT_HOME.to_owned()),
            ("{{guest_port}}", guest_port.to_string()),
        ],
    );
    let mut script = shell::ShellScript::new();
    script.block(
        "if ! command -v code-server >/dev/null 2>&1; then
  curl -fsSL https://code-server.dev/install.sh | sh -s -- --method=standalone --prefix=/usr/local
fi",
    );
    script.line("cat >/etc/systemd/system/code-server.service <<'EOF'");
    script.block(&unit);
    script.line("EOF");
    script.line("systemctl daemon-reload");
    script.line("systemctl enable --now code-server.service");
    script.render()
}

fn render_code_server_config(user: &AgentUserPlan, trusted_domains: &[String]) -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "install -d -o {} -g {} -m 0700 {}/.config/code-server",
        shell::single_quote(&user.name),
        shell::single_quote(&user.name),
        shell::single_quote(&user.home)
    ));
    script.line(format!(
        "cat >{}/.config/code-server/config.yaml <<'EOF'",
        shell::single_quote(&user.home)
    ));
    script.line("link-protection-trusted-domains:");
    for domain in trusted_domains {
        script.line(format!("  - \"{}\"", shell::double_quoted_fragment(domain)));
    }
    script.line("EOF");
    script.line(format!(
        "chown {}:{} {}/.config/code-server/config.yaml",
        shell::single_quote(&user.name),
        shell::single_quote(&user.name),
        shell::single_quote(&user.home)
    ));
    script.render()
}
