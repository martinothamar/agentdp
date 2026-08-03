use crate::manifest::{AgentManifest, GuestOs, User};
use agentdp_platform::ca::{CA_ENV_VARS_KEY, ca_env_vars_csv};
use agentdp_protocol::server_guest::BootstrapStepResource;

use crate::provisioning::bootstrap::{
    BootstrapGraphError, BootstrapRenderInput, BootstrapStep, BootstrapStepPhase, BootstrapStepPlacement,
    RenderedBootstrapPlan, RenderedBootstrapStep, RepoCheckout, bootstrap_step_spec,
};
use crate::provisioning::template;

use super::{
    AGENT_HOME, CODE_DIR, CUSTOM_BOOTSTRAP_PATH, CUSTOM_ENV_PATH, PERSISTENT_CUSTOM_ENV_PATH, guest_tooling, paths,
    shell, templates,
};

pub(in crate::provisioning) fn render_complete_bootstrap_plan(
    input: &BootstrapRenderInput,
) -> Result<RenderedBootstrapPlan, BootstrapGraphError> {
    RenderedBootstrapPlan::new(
        input.packages.clone(),
        input.repos.clone(),
        input.shell.clone(),
        input.healthchecks.clone(),
        render_complete_bootstrap_steps(input),
    )
}

pub(in crate::provisioning) fn render_complete_bootstrap_steps(
    input: &BootstrapRenderInput,
) -> Vec<RenderedBootstrapStep> {
    let system_extension_steps = input
        .steps
        .iter()
        .filter(|step| step.phase == BootstrapStepPhase::System)
        .collect::<Vec<_>>();
    let user_extension_steps = input
        .steps
        .iter()
        .filter(|step| step.phase == BootstrapStepPhase::User)
        .collect::<Vec<_>>();

    let mut steps = base_system_steps(input);
    steps.push(rendered_step(
        INSTANCE_SYSTEM_STEP,
        "system.runtime_env",
        "Materialize runtime environment",
        ["system.agent_user"],
        [BootstrapStepResource::Systemd],
        120,
        render_runtime_env_install(),
    ));
    let system_dependencies = with_step_ids(["system.runtime_env"], &system_extension_steps);
    steps.extend(
        system_extension_steps
            .iter()
            .map(|step| render_extension_step(step, std::iter::empty::<&str>())),
    );
    steps.push(rendered_step(
        INSTANCE_SYSTEM_STEP,
        "system.user_handoff",
        "Finalize agent home ownership",
        system_dependencies,
        [BootstrapStepResource::AgentHome],
        300,
        render_agent_home_handoff(&input.manifest.spec.user),
    ));
    let repository_dependencies = with_step_ids(["system.user_handoff"], &user_extension_steps);
    steps.extend(user_extension_steps.iter().map(|step| render_user_extension_step(step)));
    let after_repositories = extend_repository_steps(&mut steps, &input.repos, repository_dependencies);
    steps.extend(final_user_steps(after_repositories, &input.shell));
    steps
}

fn base_system_steps(input: &BootstrapRenderInput) -> Vec<RenderedBootstrapStep> {
    vec![
        rendered_step(
            BASE_SYSTEM_PREP_STEP,
            "system.prep",
            "Prepare system",
            std::iter::empty::<&str>(),
            [BootstrapStepResource::Systemd],
            300,
            render_system_prep(input),
        ),
        rendered_step(
            BASE_SYSTEM_STEP,
            "system.packages",
            "Install system packages",
            ["system.prep"],
            [BootstrapStepResource::PackageManager],
            1800,
            render_package_install(input.guest_os.os(), &input.packages),
        ),
        rendered_step(
            BASE_SYSTEM_STEP,
            "system.agent_user",
            "Prepare agent user",
            ["system.packages"],
            [BootstrapStepResource::UserDb],
            300,
            render_agent_user(input.manifest, &input.required_user_groups),
        ),
    ]
}

fn extend_repository_steps(
    steps: &mut Vec<RenderedBootstrapStep>,
    repos: &[RepoCheckout],
    repository_dependencies: Vec<String>,
) -> Vec<String> {
    let repository_step_ids = repos
        .iter()
        .enumerate()
        .map(|(index, repo)| repository_step_id(index, repo))
        .collect::<Vec<_>>();
    steps.extend(repos.iter().enumerate().map(|(index, repo)| {
        rendered_step(
            INSTANCE_USER_STEP,
            &repository_step_ids[index],
            &format!("Materialize repository {}", repo.name),
            repository_dependencies.clone(),
            std::iter::empty(),
            900,
            render_repository(repo),
        )
    }));
    if repository_step_ids.is_empty() {
        repository_dependencies
    } else {
        repository_step_ids
    }
}

fn final_user_steps(
    after_repositories: impl IntoIterator<Item = impl Into<String>>,
    shell: &[String],
) -> [RenderedBootstrapStep; 4] {
    [
        rendered_step(
            INSTANCE_USER_STEP,
            "user.custom_bootstrap",
            "Run custom bootstrap hook",
            after_repositories,
            [
                BootstrapStepResource::AgentHome,
                BootstrapStepResource::CodeDir,
                BootstrapStepResource::Mise,
                BootstrapStepResource::NpmGlobal,
            ],
            900,
            render_custom_bootstrap_hook(),
        ),
        rendered_step(
            INSTANCE_USER_STEP,
            "user.manifest_shell",
            "Run manifest shell commands",
            ["user.custom_bootstrap"],
            [
                BootstrapStepResource::AgentHome,
                BootstrapStepResource::CodeDir,
                BootstrapStepResource::Mise,
                BootstrapStepResource::NpmGlobal,
            ],
            900,
            render_user_commands(shell),
        ),
        rendered_step(
            INSTANCE_USER_STEP,
            "user.disable_cloud_init",
            "Disable cloud-init for subsequent boots",
            ["user.manifest_shell"],
            [BootstrapStepResource::Systemd],
            120,
            render_disable_cloud_init(),
        ),
        rendered_step(
            INSTANCE_USER_STEP,
            "user.guestd",
            "Start user guest daemon",
            ["user.disable_cloud_init"],
            [BootstrapStepResource::Systemd],
            120,
            guest_tooling::enable_guestd_service(),
        ),
    ]
}

fn with_step_ids(base: impl IntoIterator<Item = impl Into<String>>, steps: &[&BootstrapStep]) -> Vec<String> {
    base.into_iter()
        .map(Into::into)
        .chain(steps.iter().map(|step| step.id.clone()))
        .collect()
}

fn render_extension_step(
    step: &BootstrapStep,
    extra_dependencies: impl IntoIterator<Item = impl Into<String>>,
) -> RenderedBootstrapStep {
    let mut depends_on = step.depends_on.clone();
    depends_on.extend(extra_dependencies.into_iter().map(Into::into));
    RenderedBootstrapStep {
        spec: bootstrap_step_spec(
            &step.id,
            &step.label,
            step.phase,
            depends_on,
            step.resources.iter().copied(),
            step.timeout_seconds,
        ),
        placement: step.placement,
        contents: render_step_script(step.phase, true, &step.contents),
    }
}

fn render_user_extension_step(step: &BootstrapStep) -> RenderedBootstrapStep {
    match step.placement {
        BootstrapStepPlacement::Base => render_extension_step(step, std::iter::empty::<&str>()),
        BootstrapStepPlacement::Instance => render_extension_step(step, ["system.user_handoff"]),
    }
}

#[derive(Clone, Copy)]
struct StepRenderMode {
    phase: BootstrapStepPhase,
    placement: BootstrapStepPlacement,
    source_env: bool,
}

const BASE_SYSTEM_STEP: StepRenderMode = StepRenderMode {
    phase: BootstrapStepPhase::System,
    placement: BootstrapStepPlacement::Base,
    source_env: true,
};
const BASE_SYSTEM_PREP_STEP: StepRenderMode = StepRenderMode {
    phase: BootstrapStepPhase::System,
    placement: BootstrapStepPlacement::Base,
    source_env: false,
};
const INSTANCE_SYSTEM_STEP: StepRenderMode = StepRenderMode {
    phase: BootstrapStepPhase::System,
    placement: BootstrapStepPlacement::Instance,
    source_env: true,
};
const INSTANCE_USER_STEP: StepRenderMode = StepRenderMode {
    phase: BootstrapStepPhase::User,
    placement: BootstrapStepPlacement::Instance,
    source_env: true,
};

fn rendered_step(
    mode: StepRenderMode,
    id: &str,
    label: &str,
    depends_on: impl IntoIterator<Item = impl Into<String>>,
    resources: impl IntoIterator<Item = BootstrapStepResource>,
    timeout_seconds: u64,
    contents: impl AsRef<str>,
) -> RenderedBootstrapStep {
    RenderedBootstrapStep {
        spec: bootstrap_step_spec(id, label, mode.phase, depends_on, resources, timeout_seconds),
        placement: mode.placement,
        contents: render_step_script(mode.phase, mode.source_env, contents.as_ref()),
    }
}

fn render_step_script(phase: BootstrapStepPhase, source_env: bool, contents: &str) -> String {
    let mut script = shell::ShellScript::new();
    script.line("#!/usr/bin/env bash");
    script.line("set -euo pipefail");
    match phase {
        BootstrapStepPhase::System => {
            script.line("export HOME=\"${HOME:-/root}\"");
        }
        BootstrapStepPhase::User => {
            script.line(format!("export HOME=\"${{HOME:-{AGENT_HOME}}}\""));
        }
    }
    if source_env {
        script.line(source_agent_shell_env());
    }
    script.blank();
    if contents.trim().is_empty() {
        script.line(":");
    } else {
        script.block(contents);
    }
    script.render()
}

fn render_system_prep(input: &BootstrapRenderInput) -> String {
    let user = &input.manifest.spec.user;
    let primary_group = linux_primary_group(user);
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "mkdir -p {} {}",
        shell::single_quote(AGENT_HOME),
        shell::single_quote(CODE_DIR)
    ));
    script.line(format!(
        "chown -R {}:{} {}",
        shell::single_quote(&user.name),
        shell::single_quote(primary_group),
        shell::single_quote(AGENT_HOME)
    ));
    script.blank();
    script.block(&render_agent_env_install(input));
    script.line(source_agent_shell_env());
    script.blank();
    for block in input
        .guest_os
        .root_setup(user, &input.manifest.spec.network.host_aliases)
    {
        script.block(&block);
        script.blank();
    }
    script.render()
}

fn render_agent_user(manifest: &AgentManifest, required_groups: &[String]) -> String {
    let user = &manifest.spec.user;
    let primary_group = linux_primary_group(user);
    let mut script = shell::ShellScript::new();
    script.block(&render_agent_sudoers_install(user));
    script.blank();
    script.line(format!(
        "chown -R {}:{} {} {}",
        shell::single_quote(&user.name),
        shell::single_quote(primary_group),
        shell::single_quote(AGENT_HOME),
        shell::single_quote(CODE_DIR)
    ));
    script.blank();
    script.block(&render_agent_ssh_access(user));
    script.blank();
    for group in dedupe_user_groups(&user.linux().groups, required_groups) {
        script.line(format!(
            "if getent group {} >/dev/null 2>&1 && id {} >/dev/null 2>&1; then",
            shell::single_quote(group),
            shell::single_quote(&user.name)
        ));
        script.line(format!(
            "  usermod -aG {} {}",
            shell::single_quote(group),
            shell::single_quote(&user.name)
        ));
        script.line("fi");
    }
    script.render()
}

fn source_agent_shell_env() -> String {
    let env = shell::single_quote(paths::AGENT_SHELL_ENV_PATH);
    format!(". {env}")
}

fn render_agent_ssh_access(user: &User) -> String {
    let primary_group = linux_primary_group(user);
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "if [ -d {} ]; then",
        shell::single_quote(&format!("{AGENT_HOME}/.ssh"))
    ));
    script.line(format!(
        "  chown -R {}:{} {}",
        shell::single_quote(&user.name),
        shell::single_quote(primary_group),
        shell::single_quote(&format!("{AGENT_HOME}/.ssh"))
    ));
    script.line(format!(
        "  chmod 0700 {}",
        shell::single_quote(&format!("{AGENT_HOME}/.ssh"))
    ));
    script.line(format!(
        "  if [ -f {} ]; then",
        shell::single_quote(&format!("{AGENT_HOME}/.ssh/authorized_keys"))
    ));
    script.line(format!(
        "    chmod 0600 {}",
        shell::single_quote(&format!("{AGENT_HOME}/.ssh/authorized_keys"))
    ));
    script.line("  fi");
    script.line("fi");
    script.line(
        "if command -v getenforce >/dev/null 2>&1 && [ \"$(getenforce 2>/dev/null || true)\" != Disabled ]; then",
    );
    script.line("  if command -v semanage >/dev/null 2>&1 && command -v restorecon >/dev/null 2>&1; then");
    script.line(format!(
        "    semanage fcontext -a -t user_home_dir_t {} 2>/dev/null || semanage fcontext -m -t user_home_dir_t {} || true",
        shell::single_quote(AGENT_HOME),
        shell::single_quote(AGENT_HOME)
    ));
    script.line(format!(
        "    semanage fcontext -a -t user_home_t {} 2>/dev/null || semanage fcontext -m -t user_home_t {} || true",
        shell::single_quote(&format!("{AGENT_HOME}/.*")),
        shell::single_quote(&format!("{AGENT_HOME}/.*"))
    ));
    script.line(format!(
        "    semanage fcontext -a -t ssh_home_t {} 2>/dev/null || semanage fcontext -m -t ssh_home_t {} || true",
        shell::single_quote(&format!("{AGENT_HOME}/\\.ssh(/.*)?")),
        shell::single_quote(&format!("{AGENT_HOME}/\\.ssh(/.*)?"))
    ));
    script.line(format!("    restorecon -RF {}", shell::single_quote(AGENT_HOME)));
    script.line("  elif command -v chcon >/dev/null 2>&1; then");
    script.line(format!(
        "    chcon -R -t user_home_t {} || true",
        shell::single_quote(AGENT_HOME)
    ));
    script.line(format!(
        "    chcon -t user_home_dir_t {} || true",
        shell::single_quote(AGENT_HOME)
    ));
    script.line(format!(
        "    if [ -d {} ]; then chcon -R -t ssh_home_t {} || true; fi",
        shell::single_quote(&format!("{AGENT_HOME}/.ssh")),
        shell::single_quote(&format!("{AGENT_HOME}/.ssh"))
    ));
    script.line("  fi");
    script.line("fi");
    script.render()
}

fn render_agent_home_handoff(user: &User) -> String {
    let primary_group = linux_primary_group(user);
    format!(
        "chown -R {}:{} {} {}",
        shell::single_quote(&user.name),
        shell::single_quote(primary_group),
        shell::single_quote(AGENT_HOME),
        shell::single_quote(CODE_DIR)
    )
}

fn dedupe_user_groups<'a>(groups: &'a [String], required_groups: &'a [String]) -> Vec<&'a str> {
    let mut deduped = Vec::new();
    for group in groups.iter().chain(required_groups) {
        if !deduped.contains(&group.as_str()) {
            deduped.push(group.as_str());
        }
    }
    deduped
}

fn render_package_install(os: GuestOs, packages: &[String]) -> String {
    if packages.is_empty() {
        return String::new();
    }
    let packages = packages
        .iter()
        .map(|package| shell::single_quote(package))
        .collect::<Vec<_>>()
        .join(" ");
    match os {
        GuestOs::Archlinux => format!("pacman -Sy --noconfirm {packages}"),
        GuestOs::Rocky9 => format!("dnf -y install {packages}"),
    }
}

fn repository_step_id(index: usize, repo: &RepoCheckout) -> String {
    format!("user.repository.{index}.{}", step_id_fragment(&repo.name))
}

fn step_id_fragment(value: &str) -> String {
    let mut fragment = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    while fragment.contains("__") {
        fragment = fragment.replace("__", "_");
    }
    fragment.trim_matches('_').to_owned()
}

fn render_repository(repo: &RepoCheckout) -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "target=\"$AGENTDP_CODE_DIR/{}\"",
        shell::double_quoted_fragment(&repo.path)
    ));
    script.line("install -d -m 0755 \"$(dirname \"$target\")\"");
    script.line("if [ -d \"$target/.git\" ]; then");
    script.line("  git -C \"$target\" fetch --all --prune");
    script.line("else");
    script.line(format!("  git clone {} \"$target\"", shell::single_quote(&repo.url)));
    script.line("fi");
    if let Some(upstream) = &repo.upstream {
        script.line("if ! git -C \"$target\" remote get-url upstream >/dev/null 2>&1; then");
        script.line(format!(
            "  git -C \"$target\" remote add upstream {}",
            shell::single_quote(upstream)
        ));
        script.line("fi");
    }
    script.render()
}

fn render_user_commands(commands: &[String]) -> String {
    commands.join("\n")
}

fn render_disable_cloud_init() -> String {
    let mut script = shell::ShellScript::new();
    script.line("sudo -n install -d -m 0755 /etc/cloud");
    script.line("sudo -n touch /etc/cloud/cloud-init.disabled");
    script.line("if command -v systemctl >/dev/null 2>&1; then");
    script.line("  for unit in cloud-init-local.service cloud-init.service cloud-init-network.service cloud-config.service cloud-final.service cloud-init.target; do");
    script.line("    sudo -n systemctl disable \"$unit\" >/dev/null 2>&1 || true");
    script.line("  done");
    script.line("fi");
    script.render()
}

fn render_runtime_env_install() -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "if [ -f {} ]; then",
        shell::single_quote(PERSISTENT_CUSTOM_ENV_PATH)
    ));
    script.line(format!("  install -d -m 0755 {}", shell::single_quote("/run/agentdp")));
    script.line(format!(
        "  install -m 0644 -o root -g root {} {}",
        shell::single_quote(PERSISTENT_CUSTOM_ENV_PATH),
        shell::single_quote(CUSTOM_ENV_PATH)
    ));
    script.line("  cat >/etc/systemd/system/agentdp-runtime-env.service <<'AGENTDP_RUNTIME_ENV_SERVICE'");
    script.block(&format!(
        "[Unit]
Description=Materialize agentdp runtime env
After=local-fs.target

[Service]
Type=oneshot
ExecStart=/usr/bin/install -D -m 0644 -o root -g root {PERSISTENT_CUSTOM_ENV_PATH} {CUSTOM_ENV_PATH}

[Install]
WantedBy=multi-user.target"
    ));
    script.line("AGENTDP_RUNTIME_ENV_SERVICE");
    script.line("  install -d -m 0755 /etc/systemd/system/user@.service.d");
    script.line("  cat >/etc/systemd/system/user@.service.d/agentdp-runtime-env.conf <<'AGENTDP_USER_ENV_ORDERING'");
    script.block(
        "[Unit]
Requires=agentdp-runtime-env.service
After=agentdp-runtime-env.service",
    );
    script.line("AGENTDP_USER_ENV_ORDERING");
    script.line("  systemctl daemon-reload");
    script.line("  systemctl enable --now agentdp-runtime-env.service");
    script.line("fi");
    script.render()
}

fn render_custom_bootstrap_hook() -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "if [ -f {} ]; then",
        shell::single_quote(CUSTOM_BOOTSTRAP_PATH)
    ));
    script.line(format!("  . {}", shell::single_quote(CUSTOM_BOOTSTRAP_PATH)));
    script.line("fi");
    script.render()
}

fn render_agent_env_install(input: &BootstrapRenderInput) -> String {
    let user = &input.manifest.spec.user;
    let runtime_env = render_runtime_env_source();
    let ca_env = render_ca_env(input.manifest);
    let plugin_env = indent_shell_block(&input.agent_shell_env.join("\n"));
    let agent_shell_env = template::render(
        templates::AGENT_SHELL_ENV,
        &[
            ("{{agent_user_raw}}", shell::double_quoted_fragment(&user.name)),
            ("{{agent_home_raw}}", shell::double_quoted_fragment(AGENT_HOME)),
            ("{{code_dir}}", shell::single_quote(CODE_DIR)),
            ("{{usr_local_bin}}", paths::USR_LOCAL_BIN.to_owned()),
            ("{{agent_runtime_env}}", runtime_env),
            ("{{agent_ca_env}}", ca_env),
            ("{{agent_plugin_env}}", plugin_env),
        ],
    );
    let agent_env = template::render(
        templates::AGENT_ENV,
        &[
            ("{{agent_home}}", shell::single_quote(AGENT_HOME)),
            ("{{agent_shell_env}}", shell::single_quote(paths::AGENT_SHELL_ENV_PATH)),
        ],
    );
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "install -d -m 0755 {} {} /etc/profile.d",
        shell::single_quote(paths::USR_LOCAL_BIN),
        shell::single_quote(paths::AGENTDP_LIB_DIR)
    ));
    script.line(format!(
        "cat >{} <<'EOF'",
        shell::single_quote(paths::AGENT_SHELL_ENV_PATH)
    ));
    script.block(&agent_shell_env);
    script.line("EOF");
    script.line(format!(
        "chmod 0644 {}",
        shell::single_quote(paths::AGENT_SHELL_ENV_PATH)
    ));
    script.line(format!("cat >{} <<'EOF'", shell::single_quote(paths::AGENT_ENV_PATH)));
    script.block(&agent_env);
    script.line("EOF");
    script.line(format!("chmod 0755 {}", shell::single_quote(paths::AGENT_ENV_PATH)));
    script.line("cat >\"/etc/profile.d/agentdp-agent.sh\" <<'EOF'");
    script.line(source_agent_shell_env());
    script.line("EOF");
    script.line("chmod 0644 \"/etc/profile.d/agentdp-agent.sh\"");
    script.render()
}

fn indent_shell_block(block: &str) -> String {
    let mut indented = String::with_capacity(block.len());
    for (index, line) in block.lines().enumerate() {
        if index > 0 {
            indented.push('\n');
        }
        if !line.is_empty() {
            indented.push_str("  ");
            indented.push_str(line);
        }
    }
    indented
}

fn render_agent_sudoers_install(user: &User) -> String {
    let mut script = shell::ShellScript::new();
    script.line("if command -v sudo >/dev/null 2>&1; then");
    script.line("  install -d -m 0750 /etc/sudoers.d");
    script.line(format!(
        "  printf '%s ALL=(ALL) NOPASSWD:ALL\\n' {} >/etc/sudoers.d/agentdp-agent",
        shell::single_quote(&user.name)
    ));
    script.line("  chmod 0440 /etc/sudoers.d/agentdp-agent");
    script.line("fi");
    script.render()
}

fn linux_primary_group(user: &User) -> &str {
    user.linux().group.as_deref().unwrap_or(&user.name)
}

fn render_runtime_env_source() -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!("if [ -r {} ]; then", shell::single_quote(CUSTOM_ENV_PATH)));
    script.line("  set -a");
    script.line("  # shellcheck source=/dev/null");
    script.line(format!("  . {}", shell::single_quote(CUSTOM_ENV_PATH)));
    script.line("  set +a");
    script.line("fi");
    script.render()
}

fn render_ca_env(manifest: &AgentManifest) -> String {
    if !manifest.spec.network.ca.is_active(manifest.spec.network.mode) {
        return String::new();
    }
    let env_vars = manifest.spec.network.ca.env_vars();
    let mut script = shell::ShellScript::new();
    script.line("if [ -r /var/lib/agentdp/ca/ca-bundle.pem ]; then");
    script.line(format!(
        "  export {CA_ENV_VARS_KEY}={}",
        shell::single_quote(&ca_env_vars_csv(&env_vars))
    ));
    for key in env_vars {
        script.line(format!("  export {key}=/var/lib/agentdp/ca/ca-bundle.pem"));
    }
    script.line("fi");
    script.render()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::process::Command;

    use super::{CODE_DIR, render_runtime_env_install, template, templates};
    use crate::provisioning::guest_os::linux::{paths, shell};

    #[test]
    fn agent_shell_env_prioritizes_local_shims_over_system_binaries() {
        let script = template::render(
            templates::AGENT_SHELL_ENV,
            &[
                ("{{agent_user_raw}}", "agent".to_owned()),
                ("{{agent_home_raw}}", "/data/home".to_owned()),
                ("{{code_dir}}", shell::single_quote(CODE_DIR)),
                ("{{usr_local_bin}}", paths::USR_LOCAL_BIN.to_owned()),
                ("{{agent_runtime_env}}", String::new()),
                ("{{agent_ca_env}}", String::new()),
                ("{{agent_plugin_env}}", String::new()),
            ],
        );
        let dir = std::env::temp_dir().join(format!("agentdp-agent-shell-env-test-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("env.sh");
        fs::write(&path, script).expect("write env script");

        let command = format!(
            ". {}; printf '%s\\n' \"$PATH\"",
            shell::single_quote(&path.display().to_string())
        );
        let output = Command::new("sh")
            .arg("-c")
            .arg(command)
            .env("HOME", "/data/home")
            .env("USER", "agent")
            .env("PATH", "/bin:/usr/local/sbin:/usr/local/bin:/usr/bin")
            .output()
            .expect("run shell");
        let _ = fs::remove_dir_all(&dir);
        assert!(
            output.status.success(),
            "shell failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let path = String::from_utf8(output.stdout).expect("PATH is utf8");
        let entries = path.trim().split(':').collect::<Vec<_>>();
        let local_bin = entries
            .iter()
            .position(|entry| *entry == paths::USR_LOCAL_BIN)
            .expect("PATH contains /usr/local/bin");
        let bin = entries
            .iter()
            .position(|entry| *entry == "/bin")
            .expect("PATH contains /bin");
        assert!(local_bin < bin, "expected /usr/local/bin before /bin, got {path:?}");
    }

    #[test]
    fn runtime_environment_precedes_lingering_user_managers() {
        let script = render_runtime_env_install();

        assert!(script.contains("/etc/systemd/system/user@.service.d/agentdp-runtime-env.conf"));
        assert!(script.contains("Requires=agentdp-runtime-env.service"));
        assert!(script.contains("After=agentdp-runtime-env.service"));
    }
}
