use crate::manifest::plugins::git::{Defaults, Git, User};
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

pub(super) fn apply(plugin: &Git, builder: &mut ProvisioningBuilder<'_>) {
    builder.add_package("git");
    if plugin.user.is_none() && plugin.defaults == Defaults::default() {
        return;
    }

    builder.add_instance_user_step(
        "plugin.git.config",
        "Configure git",
        ["system.agent_user"],
        [BootstrapStepResource::AgentHome],
        render_git_config(plugin, builder.runtime_env_path()),
    );
}

fn render_git_config(plugin: &Git, custom_env_path: &str) -> String {
    let mut script = shell::ShellScript::new();
    if plugin.user.is_some() {
        script.line(format!("custom_env={}", shell::single_quote(custom_env_path)));
        script.line("if [ -f \"$custom_env\" ]; then");
        script.line("  set -a");
        script.line("  # shellcheck source=/dev/null");
        script.line("  . \"$custom_env\"");
        script.line("  set +a");
        script.line("fi");
    }
    if let Some(user) = &plugin.user {
        script.block(&render_git_user(user));
    }
    if let Some(init_default_branch) = &plugin.defaults.init_default_branch {
        script.line(format!(
            "git config --global init.defaultBranch {}",
            shell::single_quote(init_default_branch)
        ));
    }
    if let Some(autocrlf) = plugin.defaults.autocrlf {
        script.line(format!("git config --global core.autocrlf {autocrlf}"));
    }
    if plugin.user.is_some() {
        script.line("unset custom_env");
    }
    script.render()
}

fn render_git_user(user: &User) -> String {
    let mut script = shell::ShellScript::new();
    config_from_env(&mut script, "user.name", &user.name.from_env);
    config_from_env(&mut script, "user.email", &user.email.from_env);
    script.render()
}

fn config_from_env(script: &mut shell::ShellScript, key: &str, env: &str) {
    script.line(format!("if [ -n \"${{{env}:-}}\" ]; then"));
    script.line(format!("  git config --global {key} \"${{{env}}}\""));
    script.line("fi");
}
