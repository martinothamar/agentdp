use crate::manifest::plugins::docker::Docker;
use crate::provisioning::bootstrap::{HealthcheckKind, HealthcheckPlan, ProvisioningBuilder};
use crate::provisioning::guest_os::linux::{paths, shell, systemd};
use agentdp_platform::ca::CA_ENV_VARS_KEY;
use agentdp_protocol::server_guest::BootstrapStepResource;

pub(super) fn apply(plugin: &Docker, builder: &mut ProvisioningBuilder<'_>) {
    builder.add_package("docker");
    builder.add_required_user_group("docker");
    if plugin.compose {
        builder.add_package("docker-compose");
    }
    if plugin.buildx {
        builder.add_package("docker-buildx");
    }
    let service_dependencies = if builder.ca_enabled() {
        builder.add_instance_system_step(
            "plugin.docker.proxy",
            "Configure Docker CA proxy",
            ["system.packages", "system.guest_tooling", "system.ca_bundle"],
            [BootstrapStepResource::Systemd],
            render_proxy_setup(&builder.ca_env_vars()),
        );
        vec!["plugin.docker.proxy"]
    } else {
        vec!["system.packages"]
    };
    builder.add_instance_system_step(
        "plugin.docker.service",
        "Start Docker service",
        service_dependencies,
        [BootstrapStepResource::Systemd],
        systemd::enable_service_if_present("docker.service"),
    );
    if plugin.healthcheck {
        builder.add_healthcheck_if_absent(HealthcheckPlan {
            name: "docker".to_owned(),
            kind: HealthcheckKind::Command {
                command: "docker ps".to_owned(),
            },
            timeout: Some("60s".to_owned()),
        });
    }
}

fn render_proxy_setup(ca_env_vars: &[String]) -> String {
    let mut script = shell::ShellScript::new();
    script.line("run_docker_proxy_setup() {");
    script.line("  echo \"+ $*\" >&2");
    script.line("  timeout 30s \"$@\"");
    script.line("}");
    script.line("diagnose_docker_proxy_setup() {");
    script.line("  echo '--- docker proxy diagnostics: units ---' >&2");
    script.line("  systemctl --no-pager --full status docker.socket docker.service agentdp-docker-proxy.socket agentdp-docker-proxy.service >&2 || true");
    script.line("  echo '--- docker proxy diagnostics: proxy journal ---' >&2");
    script.line("  journalctl --no-pager -u agentdp-docker-proxy.service -n 80 >&2 || true");
    script.line("  echo '--- docker proxy diagnostics: docker journal ---' >&2");
    script.line("  journalctl --no-pager -u docker.service -n 80 >&2 || true");
    script.line("}");
    script.line(
        "install -d -m 0755 /etc/docker /etc/systemd/system/docker.socket.d /etc/systemd/system /run/agentdp/docker",
    );
    script.line(format!(
        "ln -sf {} {}",
        shell::single_quote(paths::GUESTCTL_PATH),
        shell::single_quote("/usr/local/bin/docker")
    ));
    script.line("rm -f /etc/systemd/system/docker.service.d/agentdp-proxy.conf");
    script.line("if [ -f /etc/systemd/system/docker.service ] && grep -q 'RuntimeDirectory=agentdp/docker' /etc/systemd/system/docker.service; then");
    script.line("  rm -f /etc/systemd/system/docker.service");
    script.line("fi");
    script.line("if [ -f /etc/docker/daemon.json ] && [ \"$(tr -d '[:space:]' </etc/docker/daemon.json)\" = '{\"hosts\":[\"unix:///run/agentdp/docker/docker.sock\"]}' ]; then");
    script.line("  rm -f /etc/docker/daemon.json");
    script.line("fi");
    script.line("cat >/etc/systemd/system/docker.socket.d/agentdp-private.conf <<'EOF'");
    script.line("[Socket]");
    script.line("ListenStream=");
    script.line("ListenStream=/run/agentdp/docker/docker.sock");
    script.line("SocketUser=root");
    script.line("SocketMode=0600");
    script.blank();
    script.line("EOF");
    script.line("cat >/etc/systemd/system/agentdp-docker-proxy.service <<'EOF'");
    script.line("[Unit]");
    script.line("Description=agentdp Docker CA bundle socket proxy");
    script.line("Wants=docker.service");
    script.line("After=docker.service");
    script.blank();
    script.line("[Service]");
    script.line("Type=simple");
    script.line(format!(
        "Environment={CA_ENV_VARS_KEY}={}",
        agentdp_platform::ca::ca_env_vars_csv(ca_env_vars)
    ));
    script.line(format!(
        "ExecStart={} docker-proxy --upstream /run/agentdp/docker/docker.sock --ca /var/lib/agentdp/ca/ca-bundle.pem",
        paths::GUESTD_PATH
    ));
    script.line("Restart=always");
    script.line("RestartSec=2");
    script.line("StandardOutput=journal");
    script.line("StandardError=journal");
    script.line("EOF");
    script.line("cat >/etc/systemd/system/agentdp-docker-proxy.socket <<'EOF'");
    script.line("[Unit]");
    script.line("Description=agentdp Docker CA bundle socket");
    script.blank();
    script.line("[Socket]");
    script.line("ListenStream=/run/docker.sock");
    script.line("SocketUser=root");
    script.line("SocketGroup=docker");
    script.line("SocketMode=0660");
    script.blank();
    script.line("[Install]");
    script.line("WantedBy=sockets.target");
    script.line("EOF");
    script.line("run_docker_proxy_setup systemctl daemon-reload");
    script.line(
        "timeout 60s systemctl stop agentdp-docker-proxy.service agentdp-docker-proxy.socket docker.service docker.socket >/dev/null 2>&1 || true",
    );
    script.line("if [ -S /run/docker.sock ]; then rm -f /run/docker.sock; fi");
    script.line("if [ -S /run/agentdp/docker/docker.sock ]; then rm -f /run/agentdp/docker/docker.sock; fi");
    script.line("run_docker_proxy_setup systemctl reset-failed || true");
    script.line("run_docker_proxy_setup systemctl enable --now docker.socket");
    script.line("run_docker_proxy_setup systemctl enable --now docker.service");
    script.line("run_docker_proxy_setup systemctl enable --now agentdp-docker-proxy.socket");
    script.line("test -S /run/docker.sock");
    script.line("test -S /run/agentdp/docker/docker.sock");
    script.line("if [ \"$(stat -c '%a:%U:%G' /run/docker.sock)\" != '660:root:docker' ]; then");
    script.line("  echo \"unexpected /run/docker.sock ownership/mode: $(stat -c '%a:%U:%G' /run/docker.sock)\" >&2");
    script.line("  exit 1");
    script.line("fi");
    script.line(format!(
        "if ! run_docker_proxy_setup {} ps >/dev/null; then",
        shell::single_quote("/usr/local/bin/docker")
    ));
    script.line("  diagnose_docker_proxy_setup");
    script.line("  exit 1");
    script.line("fi");
    script.render()
}
