use crate::manifest::GuestOs;
use crate::manifest::plugins::podman::Podman;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::{paths, shell};
use agentdp_platform::ca::{CA_ENV_VARS_KEY, ca_env_vars_csv};
use agentdp_protocol::server_guest::BootstrapStepResource;

pub(super) fn apply(plugin: &Podman, builder: &mut ProvisioningBuilder<'_>) {
    for package in packages(builder.guest_os(), plugin) {
        builder.add_package(package);
    }
    if plugin.compose && matches!(builder.guest_os(), GuestOs::Rocky9) {
        builder.add_base_system_step(
            "plugin.podman.compose",
            "Install Rocky Podman Compose provider",
            ["system.packages"],
            [BootstrapStepResource::PackageManager],
            render_rocky_compose_install(),
        );
    }

    let mut docker_api_dependencies = vec!["system.agent_user"];
    if builder.ca_enabled() {
        builder.add_instance_user_step(
            "plugin.podman.ca_bundle",
            "Configure Podman CA defaults",
            ["system.agent_user", "system.ca_bundle"],
            [],
            render_ca_bundle_setup(&builder.ca_env_vars()),
        );
        docker_api_dependencies.push("plugin.podman.ca_bundle");
    }

    if plugin.docker_api {
        builder.add_agent_shell_env(render_docker_api_env(
            builder.guest_layout().agent_home,
            &builder.agent_user().name,
        ));
        builder.add_instance_user_step(
            "plugin.podman.docker_api",
            "Configure rootless Podman Docker API",
            docker_api_dependencies,
            [BootstrapStepResource::Systemd],
            render_docker_api_setup(),
        );
    }
}

fn packages(guest_os: GuestOs, plugin: &Podman) -> Vec<&'static str> {
    let mut packages = match guest_os {
        GuestOs::Archlinux => vec!["podman", "fuse-overlayfs", "slirp4netns"],
        GuestOs::Rocky9 => vec!["podman", "crun", "fuse-overlayfs", "slirp4netns", "shadow-utils"],
    };
    if plugin.compose && matches!(guest_os, GuestOs::Archlinux) {
        packages.push("podman-compose");
    }
    if plugin.docker_api {
        packages.push("podman-docker");
    }
    packages
}

fn render_rocky_compose_install() -> String {
    let mut script = shell::ShellScript::new();
    script.line("dnf -y install epel-release || dnf -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm");
    script.line("dnf -y install podman-compose");
    script.render()
}

fn render_ca_bundle_setup(ca_env_vars: &[String]) -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "sudo ln -sf {} {}",
        shell::single_quote(paths::GUESTCTL_PATH),
        shell::single_quote("/usr/local/bin/podman")
    ));
    script.blank();
    script.line("install -d -m 0755 \"$HOME/.config/containers/containers.conf.d\"");
    script.line("cat >\"$HOME/.config/containers/containers.conf.d/agentdp-ca-bundle.conf\" <<'EOF'");
    script.line("[containers]");
    script.line(format!("env={}", podman_ca_env(ca_env_vars)));
    script.line("volumes=[\"/var/lib/agentdp/ca/ca-bundle.pem:/run/agentdp/ca/ca-bundle.pem:ro\"]");
    script.line("EOF");
    script.render()
}

fn render_docker_api_setup() -> String {
    let mut script = shell::ShellScript::new();
    script.line("if command -v loginctl >/dev/null 2>&1; then");
    script.line("  sudo loginctl enable-linger \"$USER\" || true");
    script.line("fi");
    script.blank();
    script.line("export XDG_RUNTIME_DIR=\"${XDG_RUNTIME_DIR:-/run/user/$(id -u)}\"");
    script.line("export DOCKER_HOST=\"unix://$XDG_RUNTIME_DIR/podman/podman.sock\"");
    script.line("systemctl --user daemon-reload || true");
    script.line("systemctl --user enable --now podman.socket");
    script.line("/usr/bin/podman info >/dev/null");
    script.line("docker ps >/dev/null");
    script.render()
}

fn render_docker_api_env(agent_home: &str, agent_user: &str) -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "if [ \"${{HOME:-}}\" = {} ] || [ \"${{USER:-}}\" = {} ]; then",
        shell::single_quote(agent_home),
        shell::single_quote(agent_user)
    ));
    script.line(format!("  {}", docker_host_profile_export()));
    script.line("fi");
    script.render()
}

fn podman_ca_env(ca_env_vars: &[String]) -> String {
    let mut values = ca_env_vars
        .iter()
        .map(|key| format!("\"{key}=/run/agentdp/ca/ca-bundle.pem\""))
        .collect::<Vec<_>>();
    values.push(format!("\"{CA_ENV_VARS_KEY}={}\"", ca_env_vars_csv(ca_env_vars)));
    format!("[{}]", values.join(", "))
}

fn docker_host_profile_export() -> String {
    [
        "export DOCKER_HOST=\"unix://",
        "${",
        "XDG_RUNTIME_DIR:-/run/user/$(id -u)}/",
        "podman/podman.sock",
        "\"",
    ]
    .concat()
}
