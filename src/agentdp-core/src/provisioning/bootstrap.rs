use std::collections::BTreeSet;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

use crate::manifest::{AgentManifest, Healthcheck, Repo};

use super::{AGENT_HOME, CODE_DIR, shell, templates};

const CUSTOM_BOOTSTRAP_PATH: &str = "/run/agentdp/bootstrap.sh";
const CUSTOM_ENV_PATH: &str = "/run/agentdp/.env";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapPlan {
    pub user: AgentUserPlan,
    pub packages: Vec<String>,
    pub repos: Vec<RepoCheckout>,
    pub shell: Vec<String>,
    pub healthchecks: Vec<HealthcheckPlan>,
    pub script: String,
}

impl BootstrapPlan {
    pub(crate) fn from_manifest(manifest: &AgentManifest) -> Self {
        ProvisioningBuilder::from_manifest(manifest).build()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentUserPlan {
    pub name: String,
    pub home: String,
    pub groups: Vec<String>,
}

pub(super) struct ProvisioningBuilder<'a> {
    manifest: &'a AgentManifest,
    user: AgentUserPlan,
    packages: Vec<String>,
    repos: Vec<RepoCheckout>,
    healthchecks: Vec<HealthcheckPlan>,
    root_shell: Vec<String>,
    agent_shell: Vec<String>,
}

impl<'a> ProvisioningBuilder<'a> {
    fn from_manifest(manifest: &'a AgentManifest) -> Self {
        let repos = manifest
            .bootstrap
            .repos
            .iter()
            .map(RepoCheckout::from_manifest)
            .collect::<Vec<_>>();
        let healthchecks = manifest
            .bootstrap
            .healthchecks
            .iter()
            .map(HealthcheckPlan::from_manifest)
            .collect::<Vec<_>>();

        let mut packages = manifest.bootstrap.packages.clone();
        packages.push("sudo".to_owned());
        if !repos.is_empty() {
            packages.push("git".to_owned());
        }

        Self {
            manifest,
            user: AgentUserPlan {
                name: manifest.user.name.clone(),
                home: AGENT_HOME.to_owned(),
                groups: manifest.user.groups.clone(),
            },
            packages,
            repos,
            healthchecks,
            root_shell: Vec::new(),
            agent_shell: Vec::new(),
        }
    }

    fn build(mut self) -> BootstrapPlan {
        super::plugins::apply(&self.manifest.plugins, &mut self);
        self.add_agent_shell_lines(self.manifest.bootstrap.shell.iter().cloned());
        self.user.groups = dedupe(self.user.groups);
        let packages = dedupe(self.packages);

        let script = render_bootstrap_script(
            self.manifest,
            &self.user,
            &packages,
            &self.repos,
            &self.healthchecks,
            &self.root_shell,
            &self.agent_shell,
        );

        BootstrapPlan {
            user: self.user,
            packages,
            repos: self.repos,
            shell: self.agent_shell,
            healthchecks: self.healthchecks,
            script,
        }
    }

    pub(super) fn add_package(&mut self, package: impl Into<String>) {
        self.packages.push(package.into());
    }

    pub(super) fn add_root_shell(&mut self, command: impl Into<String>) {
        self.root_shell.push(command.into());
    }

    pub(super) fn add_agent_shell(&mut self, command: impl Into<String>) {
        self.agent_shell.push(command.into());
    }

    pub(super) fn add_user_group(&mut self, group: impl Into<String>) {
        self.user.groups.push(group.into());
    }

    pub(super) fn add_healthcheck_if_absent(&mut self, healthcheck: HealthcheckPlan) {
        if !self
            .healthchecks
            .iter()
            .any(|existing| existing.name == healthcheck.name)
        {
            self.healthchecks.push(healthcheck);
        }
    }

    fn add_agent_shell_lines(&mut self, commands: impl IntoIterator<Item = String>) {
        self.agent_shell.extend(commands);
    }

    pub(super) fn guest_port(&self, name: &str) -> Option<u16> {
        self.manifest.network.ports.get(name).map(|port| port.guest)
    }

    pub(super) const fn agent_user(&self) -> &AgentUserPlan {
        &self.user
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoCheckout {
    pub name: String,
    pub url: String,
    pub path: String,
}

impl RepoCheckout {
    fn from_manifest(repo: &Repo) -> Self {
        let name = repo.name.clone().unwrap_or_else(|| repo_name_from_url(&repo.url));
        let path = repo.path.clone().unwrap_or_else(|| name.clone());
        Self {
            name,
            url: repo.url.clone(),
            path,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthcheckPlan {
    pub name: String,
    pub kind: HealthcheckKind,
    pub timeout: Option<String>,
}

impl HealthcheckPlan {
    fn from_manifest(healthcheck: &Healthcheck) -> Self {
        let kind = match (&healthcheck.command, &healthcheck.tcp) {
            (Some(command), _) => HealthcheckKind::Command(command.clone()),
            (None, Some(tcp)) => HealthcheckKind::Tcp(tcp.clone()),
            (None, None) => HealthcheckKind::Command(String::new()),
        };
        Self {
            name: healthcheck.name.clone(),
            kind,
            timeout: healthcheck.timeout.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthcheckKind {
    Command(String),
    Tcp(String),
}

impl std::fmt::Display for HealthcheckKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Command(command) => write!(formatter, "command {command}"),
            Self::Tcp(target) => write!(formatter, "tcp {target}"),
        }
    }
}

fn render_bootstrap_script(
    manifest: &AgentManifest,
    user: &AgentUserPlan,
    packages: &[String],
    repos: &[RepoCheckout],
    healthchecks: &[HealthcheckPlan],
    root_shell: &[String],
    agent_shell: &[String],
) -> String {
    let mut script = shell::ShellScript::new();
    script.block(&render_bootstrap_preamble(user));
    script.blank();
    script.block(&render_agent_env_install(user));
    script.blank();

    for group in &user.groups {
        script.line(format!(
            "if getent group {} >/dev/null 2>&1 && id \"$AGENTDP_USER\" >/dev/null 2>&1; then",
            shell::single_quote(group)
        ));
        script.line(format!(
            "  usermod -aG {} \"$AGENTDP_USER\"",
            shell::single_quote(group)
        ));
        script.line("fi");
    }

    script.blank();
    script.block(templates::BOOTSTRAP_HELPERS);
    script.blank();
    script.block(&render_harness_information(
        manifest,
        packages,
        repos,
        healthchecks,
        agent_shell,
    ));
    script.blank();

    if !root_shell.is_empty() {
        for block in root_shell {
            script.block(block);
        }
        script.line("chown -R \"$AGENTDP_USER:$AGENTDP_USER\" \"$AGENTDP_HOME\"");
        script.blank();
    }

    script.block(&render_custom_bootstrap_hook());
    script.blank();

    for repo in repos {
        script.line(format!(
            "target=\"$AGENTDP_CODE_DIR/{}\"",
            shell::double_quoted_fragment(&repo.path)
        ));
        script.block(&repo_checkout_block(repo));
        script.blank();
    }

    for command in agent_shell {
        script.line(format!("run_agent {}", shell::single_quote(command)));
    }
    script.blank();
    script.render()
}

fn render_custom_bootstrap_hook() -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "AGENTDP_CUSTOM_BOOTSTRAP={}",
        shell::single_quote(CUSTOM_BOOTSTRAP_PATH)
    ));
    script.line(format!("AGENTDP_CUSTOM_ENV={}", shell::single_quote(CUSTOM_ENV_PATH)));
    script.line("cleanup_agentdp_custom_env() {");
    script.line("  rm -f \"$AGENTDP_CUSTOM_ENV\"");
    script.line("}");
    script.line("trap cleanup_agentdp_custom_env EXIT");
    script.line("if [ -f \"$AGENTDP_CUSTOM_BOOTSTRAP\" ]; then");
    script.line("  if [ -f \"$AGENTDP_CUSTOM_ENV\" ]; then");
    script.line("    set -a");
    script.line("    # shellcheck source=/dev/null");
    script.line("    . \"$AGENTDP_CUSTOM_ENV\"");
    script.line("    set +a");
    script.line("  fi");
    script.line("  . \"$AGENTDP_CUSTOM_BOOTSTRAP\"");
    script.line("fi");
    script.line("cleanup_agentdp_custom_env");
    script.line("trap - EXIT");
    script.render()
}

fn render_harness_information(
    manifest: &AgentManifest,
    packages: &[String],
    repos: &[RepoCheckout],
    healthchecks: &[HealthcheckPlan],
    agent_shell: &[String],
) -> String {
    let info = harness_information(manifest, packages, repos, healthchecks, agent_shell);
    let encoded_info = BASE64.encode(info);
    let mut script = shell::ShellScript::new();
    script.line("target=\"$AGENTDP_HOME/.codex/AGENTS.md\"");
    script.line("start=\"<!-- agentdp-info:start -->\"");
    script.line("end=\"<!-- agentdp-info:end -->\"");
    script.line("tmp=\"$(mktemp)\"");
    script.line("install -d -o \"$AGENTDP_USER\" -g \"$AGENTDP_USER\" -m 0755 \"$(dirname \"$target\")\"");
    script.line("touch \"$target\"");
    script.line("chown \"$AGENTDP_USER:$AGENTDP_USER\" \"$target\"");
    script.block(
        "awk -v start=\"$start\" -v end=\"$end\" '
  $0 == start { skip = 1; next }
  $0 == end { skip = 0; next }
  !skip { print }
' \"$target\" >\"$tmp\"",
    );
    script.line("{");
    script.line("  cat \"$tmp\"");
    script.line("  if [ -s \"$tmp\" ]; then printf '\\n'; fi");
    script.line("  printf '%s\\n' \"$start\"");
    script.line("  cat <<'AGENTDP_INFO_B64' | base64 -d");
    script.block(&encoded_info);
    script.line("AGENTDP_INFO_B64");
    script.line("  printf '%s\\n' \"$end\"");
    script.line("} >\"$target\"");
    script.line("chown \"$AGENTDP_USER:$AGENTDP_USER\" \"$target\"");
    script.line("rm -f \"$tmp\"");
    script.render()
}

fn harness_information(
    manifest: &AgentManifest,
    packages: &[String],
    repos: &[RepoCheckout],
    healthchecks: &[HealthcheckPlan],
    agent_shell: &[String],
) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(2048);
    output.push_str("# agentdp generated context\n\n");
    output.push_str("This section is generated by agentdp. Edit source seed files beside `agent.yaml`, not generated guest files.\n\n");
    output.push_str("## Guest\n\n");
    let _ = writeln!(&mut output, "- manifest: {}", manifest.name);
    let _ = writeln!(&mut output, "- image.os: {:?}", manifest.image.os);
    output.push_str("- home: /data/home\n");
    output.push_str("- code: /data/home/code\n\n");
    output.push_str("## Packages\n\n");
    for package in packages {
        let _ = writeln!(&mut output, "- {package}");
    }
    output.push_str("\n## Repositories\n\n");
    for repo in repos {
        let _ = writeln!(&mut output, "- {} -> {CODE_DIR}/{}", repo.url, repo.path);
    }
    output.push_str("\n## Healthchecks\n\n");
    for healthcheck in healthchecks {
        let _ = writeln!(
            &mut output,
            "- {}: {} timeout={}",
            healthcheck.name,
            healthcheck.kind,
            healthcheck.timeout.as_deref().unwrap_or("<default>")
        );
    }
    output.push_str("\n## Bootstrap Shell\n\n");
    for command in agent_shell {
        let _ = writeln!(&mut output, "- `{command}`");
    }
    output
}

fn render_bootstrap_preamble(user: &AgentUserPlan) -> String {
    shell::render_template(
        templates::BOOTSTRAP_PREAMBLE,
        &[
            ("{{agent_user}}", shell::single_quote(&user.name)),
            ("{{agent_home}}", shell::single_quote(&user.home)),
            ("{{code_dir}}", shell::single_quote(CODE_DIR)),
        ],
    )
}

fn render_agent_env_install(user: &AgentUserPlan) -> String {
    let agent_env = shell::render_template(
        templates::AGENT_ENV,
        &[
            ("{{agent_home}}", shell::single_quote(&user.home)),
            ("{{agent_home_raw}}", shell::double_quoted_fragment(&user.home)),
            ("{{code_dir}}", shell::single_quote(CODE_DIR)),
        ],
    );
    let agent_profile = shell::render_template(
        templates::AGENT_PROFILE,
        &[
            ("{{agent_user_raw}}", shell::double_quoted_fragment(&user.name)),
            ("{{agent_home}}", shell::single_quote(&user.home)),
            ("{{agent_home_raw}}", shell::double_quoted_fragment(&user.home)),
            ("{{code_dir}}", shell::single_quote(CODE_DIR)),
        ],
    );
    let mut script = shell::ShellScript::new();
    script.line("install -d -m 0755 \"$(dirname \"$AGENTDP_AGENT_ENV\")\"");
    script.line("cat >\"$AGENTDP_AGENT_ENV\" <<'EOF'");
    script.block(&agent_env);
    script.line("EOF");
    script.line("chmod 0755 \"$AGENTDP_AGENT_ENV\"");
    script.line("cat >\"/etc/profile.d/agentdp-agent.sh\" <<'EOF'");
    script.block(&agent_profile);
    script.line("EOF");
    script.line("chmod 0644 \"/etc/profile.d/agentdp-agent.sh\"");
    script.render()
}

fn repo_checkout_block(repo: &RepoCheckout) -> String {
    [
        "mkdir -p \"$(dirname \"$target\")\"".to_owned(),
        "chown -R \"$AGENTDP_USER:$AGENTDP_USER\" \"$(dirname \"$target\")\"".to_owned(),
        "if [ ! -d \"$target/.git\" ]; then".to_owned(),
        format!("  clone_repo {} \"$target\"", shell::single_quote(&repo.url)),
        "fi".to_owned(),
        "run_agent_args git -C \"$target\" config core.preloadIndex true".to_owned(),
        "run_agent_args git -C \"$target\" config core.untrackedCache true".to_owned(),
        "run_agent_args git -C \"$target\" update-index --test-untracked-cache >/dev/null 2>&1 || true".to_owned(),
    ]
    .join("\n")
}

fn repo_name_from_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    let name = trimmed
        .rsplit(['/', ':'])
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("repo");
    name.strip_suffix(".git").unwrap_or(name).to_owned()
}

fn dedupe(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values.into_iter().filter(|value| seen.insert(value.clone())).collect()
}

#[cfg(test)]
mod tests {
    use crate::manifest::AgentManifest;
    use crate::manifest::Repo;
    use crate::provisioning::bootstrap::{AgentUserPlan, RepoCheckout, render_bootstrap_script};
    use crate::provisioning::shell;

    #[test]
    fn repo_defaults_come_from_git_url() {
        let cases = [
            ("https://github.com/example/repo.git", "repo"),
            ("https://github.com/example/repo", "repo"),
            ("git@github.com:example/nested-repo.git", "nested-repo"),
        ];

        for (url, expected_name) in cases {
            let checkout = RepoCheckout::from_manifest(&Repo {
                name: None,
                url: url.to_owned(),
                path: None,
            });
            assert_eq!(checkout.name, expected_name);
            assert_eq!(checkout.path, expected_name);
        }
    }

    #[test]
    fn explicit_repo_name_and_path_win_over_url_defaults() {
        let checkout = RepoCheckout::from_manifest(&Repo {
            name: Some("display-name".to_owned()),
            url: "https://github.com/example/repo.git".to_owned(),
            path: Some("custom/path".to_owned()),
        });

        assert_eq!(checkout.name, "display-name");
        assert_eq!(checkout.path, "custom/path");
    }

    #[test]
    fn empty_bootstrap_inputs_render_minimal_script() {
        let script = render_bootstrap_script(&manifest(), &agent_user(), &[], &[], &[], &[], &[]);

        assert!(script.contains("set -euo pipefail"));
        assert!(script.contains("AGENTDP_USER='agent'"));
        assert!(script.contains("mkdir -p \"$AGENTDP_HOME\" \"$AGENTDP_CODE_DIR\""));
        assert!(!script.contains("mise use --global"));
        assert!(!script.contains("target=\"$AGENTDP_CODE_DIR/"));
    }

    fn agent_user() -> AgentUserPlan {
        AgentUserPlan {
            name: "agent".to_owned(),
            home: "/data/home".to_owned(),
            groups: vec!["docker".to_owned()],
        }
    }

    fn manifest() -> AgentManifest {
        serde_yaml::from_str(agentdp_test_support::manifest::minimal()).unwrap()
    }

    #[test]
    fn bootstrap_script_quotes_shell_values() {
        let repos = [RepoCheckout {
            name: "quoted".to_owned(),
            url: "https://example.com/quote's.git".to_owned(),
            path: "nested/$repo\"name".to_owned(),
        }];
        let agent_shell = [format!("mise use --global {}", shell::single_quote("node'20"))];

        let script = render_bootstrap_script(&manifest(), &agent_user(), &[], &repos, &[], &[], &agent_shell);

        assert!(script.contains("mise use --global"));
        assert!(script.contains("node"));
        assert!(script.contains("20"));
        assert!(script.contains(&format!(
            "target=\"$AGENTDP_CODE_DIR/{}\"",
            shell::double_quoted_fragment("nested/$repo\"name")
        )));
        assert!(script.contains(&format!(
            "clone_repo {} \"$target\"",
            shell::single_quote("https://example.com/quote's.git")
        )));
    }
}
