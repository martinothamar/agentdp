use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::GuestToolSeed;
use agentdp_protocol::server_guest::BootstrapStepResource;

use super::{paths, shell, templates};

const GUESTCTL_SOURCE: &str = "/run/agentdp/bin/guestctl.gz";

pub(in crate::provisioning) const GUEST_TOOL_SEEDS: &[GuestToolSeed] = &[
    GuestToolSeed {
        name: "guestd",
        guest_path: paths::GUESTD_PATH,
        permissions: "0755",
        compress: false,
    },
    GuestToolSeed {
        name: "guestctl",
        guest_path: GUESTCTL_SOURCE,
        permissions: "0644",
        compress: true,
    },
];

pub(in crate::provisioning) fn apply(builder: &mut ProvisioningBuilder<'_>) {
    builder.add_base_system_step(
        "system.guest_tooling",
        "Install guest tooling",
        ["system.packages", "system.agent_user"],
        [BootstrapStepResource::GuestTooling, BootstrapStepResource::Systemd],
        install_guest_tooling(builder.agent_user().name.as_str()),
    );
}

fn install_guest_tooling(user: &str) -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "install -d -m 0755 {} /etc/systemd/user",
        shell::single_quote(paths::USR_LOCAL_BIN)
    ));
    script.line(format!("test -x {}", shell::single_quote(paths::GUESTD_PATH)));
    script.line(format!(
        "gzip -dc {} >{}",
        shell::single_quote(GUESTCTL_SOURCE),
        shell::single_quote(paths::GUESTCTL_PATH)
    ));
    script.line(format!("chmod 0755 {}", shell::single_quote(paths::GUESTCTL_PATH)));
    script.line("cat >/etc/systemd/user/guestd.service <<'EOF'");
    script.block(templates::GUESTD_SERVICE);
    script.line("EOF");
    script.line(format!(
        "if command -v loginctl >/dev/null 2>&1; then\n  loginctl enable-linger {} || true\nfi",
        shell::single_quote(user)
    ));
    script.render()
}

pub(in crate::provisioning) fn enable_guestd_service() -> String {
    let mut script = shell::ShellScript::new();
    script.line("export XDG_RUNTIME_DIR=\"${XDG_RUNTIME_DIR:-/run/user/$(id -u)}\"");
    script.line("if command -v loginctl >/dev/null 2>&1; then");
    script.line("  sudo -n loginctl enable-linger \"$USER\" || true");
    script.line("fi");
    script.line("if command -v systemctl >/dev/null 2>&1; then");
    script.line("  sudo -n systemctl start \"user@$(id -u).service\" || true");
    script.line("fi");
    script.line("for _ in $(seq 1 30); do");
    script.line("  [ -S \"$XDG_RUNTIME_DIR/bus\" ] && break");
    script.line("  sleep 1");
    script.line("done");
    script.line("test -S \"$XDG_RUNTIME_DIR/bus\"");
    script.line("systemctl --user daemon-reload");
    script.line("systemctl --user stop guestd.service >/dev/null 2>&1 || true");
    script.line("systemctl --user enable --now guestd.service");
    script.line("systemctl --user is-active --quiet guestd.service");
    script.render()
}

#[cfg(test)]
mod tests {
    #[test]
    fn user_guestd_start_waits_for_user_systemd_bus() {
        let script = super::enable_guestd_service();

        assert!(script.contains("systemctl start \"user@$(id -u).service\""));
        assert!(script.contains("test -S \"$XDG_RUNTIME_DIR/bus\""));
        assert!(script.contains("systemctl --user enable --now guestd.service"));
    }
}
