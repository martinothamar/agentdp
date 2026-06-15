use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

pub(super) const CA_BUNDLE_PATH: &str = "/var/lib/agentdp/ca/ca-bundle.pem";

pub(in crate::provisioning) fn apply(builder: &mut ProvisioningBuilder<'_>) {
    builder.add_instance_system_step(
        "system.ca_bundle",
        "Install CA bundle",
        ["system.prep"],
        [BootstrapStepResource::PackageManager],
        builder.ca_bundle_install_command(),
    );
}

pub(super) fn install_seeded_ca_bundle(cert_pem: &str, install_command: &str) -> String {
    let mut script = shell::ShellScript::new();
    script.line("install -d -m 0755 /var/lib/agentdp/ca");
    script.line(format!("cat >{CA_BUNDLE_PATH} <<'AGENTDP_CA_BUNDLE'"));
    script.line(cert_pem.trim_end());
    script.line("AGENTDP_CA_BUNDLE");
    script.line(format!("chmod 0644 {CA_BUNDLE_PATH}"));
    script.block(&render_selinux_container_label());
    script.block(install_command);
    script.render()
}

fn render_selinux_container_label() -> String {
    let mut script = shell::ShellScript::new();
    script.line("if command -v selinuxenabled >/dev/null 2>&1 && selinuxenabled; then");
    script.line("  if command -v semanage >/dev/null 2>&1 && command -v restorecon >/dev/null 2>&1; then");
    script.line("    semanage fcontext -a -t container_file_t '/var/lib/agentdp/ca/ca-bundle\\.pem' 2>/dev/null || semanage fcontext -m -t container_file_t '/var/lib/agentdp/ca/ca-bundle\\.pem' || true");
    script.line("    restorecon /var/lib/agentdp/ca/ca-bundle.pem || true");
    script.line("  elif command -v chcon >/dev/null 2>&1; then");
    script.line("    chcon -t container_file_t /var/lib/agentdp/ca/ca-bundle.pem || true");
    script.line("  fi");
    script.line("fi");
    script.render()
}
