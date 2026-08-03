use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use agentdp_protocol::server_guest::BootstrapStep as BootstrapStepSpec;
pub use agentdp_protocol::server_guest::BootstrapStepPhase;
use agentdp_protocol::server_guest::BootstrapStepResource;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::manifest::{AgentManifest, GuestOs, Healthcheck, Repo};

use super::ProvisioningPlan;
use super::guest_os::{GuestLayout, GuestOsAdapter};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RenderedBootstrapPlan {
    pub packages: Vec<String>,
    pub repos: Vec<RepoCheckout>,
    pub shell: Vec<String>,
    pub healthchecks: Vec<HealthcheckPlan>,
    pub steps: Vec<RenderedBootstrapStep>,
}

impl RenderedBootstrapPlan {
    pub(crate) fn new(
        packages: Vec<String>,
        repos: Vec<RepoCheckout>,
        shell: Vec<String>,
        healthchecks: Vec<HealthcheckPlan>,
        steps: Vec<RenderedBootstrapStep>,
    ) -> Result<Self, BootstrapGraphError> {
        let plan = Self {
            packages,
            repos,
            shell,
            healthchecks,
            steps,
        };
        plan.validate_graph()?;
        Ok(plan)
    }

    pub(super) fn for_placement(&self, placement: BootstrapStepPlacement) -> Result<Self, BootstrapGraphError> {
        let step_ids = self
            .steps
            .iter()
            .filter(|step| step.placement == placement)
            .map(|step| step.id.as_str())
            .collect::<BTreeSet<_>>();
        let steps = self
            .steps
            .iter()
            .filter(|step| step.placement == placement)
            .cloned()
            .map(|mut step| {
                step.spec
                    .depends_on
                    .retain(|dependency| step_ids.contains(dependency.as_str()));
                step
            })
            .collect();
        let base = placement == BootstrapStepPlacement::Base;
        let instance = placement == BootstrapStepPlacement::Instance;
        Self::new(
            include_if(base, self.packages.clone()),
            include_if(instance, self.repos.clone()),
            include_if(instance, self.shell.clone()),
            include_if(instance, self.healthchecks.clone()),
            steps,
        )
    }

    fn validate_graph(&self) -> Result<(), BootstrapGraphError> {
        let mut steps = BTreeMap::new();
        for step in &self.steps {
            if steps.insert(step.id.as_str(), step).is_some() {
                return Err(BootstrapGraphError::DuplicateStep { step: step.id.clone() });
            }
        }
        for step in &self.steps {
            for dependency in &step.depends_on {
                if step.phase == BootstrapStepPhase::System
                    && steps
                        .get(dependency.as_str())
                        .is_some_and(|dependency| dependency.phase == BootstrapStepPhase::User)
                {
                    return Err(BootstrapGraphError::InvalidPhaseDependency {
                        step: step.id.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
        }

        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        for step in &self.steps {
            visit_step(step.id.as_str(), &steps, &mut visiting, &mut visited)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RenderedBootstrapStep {
    #[serde(flatten)]
    pub spec: BootstrapStepSpec,
    #[serde(skip)]
    pub(super) placement: BootstrapStepPlacement,
    pub contents: String,
}

impl Deref for RenderedBootstrapStep {
    type Target = BootstrapStepSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BootstrapStepPlacement {
    Base,
    Instance,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BootstrapGraphError {
    #[error("bootstrap step `{step}` is duplicated")]
    DuplicateStep { step: String },
    #[error("bootstrap step `{step}` depends on missing step `{dependency}`")]
    MissingDependency { step: String, dependency: String },
    #[error("system bootstrap step `{step}` must not depend on user bootstrap step `{dependency}`")]
    InvalidPhaseDependency { step: String, dependency: String },
    #[error("bootstrap step graph has a dependency cycle involving `{step}`")]
    Cycle { step: String },
}

fn visit_step<'a>(
    step: &'a str,
    steps: &BTreeMap<&'a str, &'a RenderedBootstrapStep>,
    visiting: &mut BTreeSet<&'a str>,
    visited: &mut BTreeSet<&'a str>,
) -> Result<(), BootstrapGraphError> {
    if visited.contains(step) {
        return Ok(());
    }
    if !visiting.insert(step) {
        return Err(BootstrapGraphError::Cycle { step: step.to_owned() });
    }

    let Some(rendered_step) = steps.get(step) else {
        return Err(BootstrapGraphError::MissingDependency {
            step: step.to_owned(),
            dependency: step.to_owned(),
        });
    };
    for dependency in &rendered_step.depends_on {
        if !steps.contains_key(dependency.as_str()) {
            return Err(BootstrapGraphError::MissingDependency {
                step: step.to_owned(),
                dependency: dependency.clone(),
            });
        }
        visit_step(dependency, steps, visiting, visited)?;
    }
    visiting.remove(step);
    visited.insert(step);
    Ok(())
}

fn include_if<T: Default>(include: bool, value: T) -> T {
    if include { value } else { T::default() }
}

pub(super) fn bootstrap_step_spec(
    id: impl Into<String>,
    label: impl Into<String>,
    phase: BootstrapStepPhase,
    depends_on: impl IntoIterator<Item = impl Into<String>>,
    resources: impl IntoIterator<Item = BootstrapStepResource>,
    timeout_seconds: u64,
) -> BootstrapStepSpec {
    let id = id.into();
    BootstrapStepSpec {
        script: format!("steps/{id}.sh"),
        id,
        label: label.into(),
        phase,
        depends_on: depends_on.into_iter().map(Into::into).collect(),
        resources: resources.into_iter().collect(),
        working_directory: "/".to_owned(),
        timeout_seconds,
    }
}

pub(super) struct BootstrapRenderInput<'a> {
    pub(super) manifest: &'a AgentManifest,
    pub(super) packages: Vec<String>,
    pub(super) required_user_groups: Vec<String>,
    pub(super) agent_shell_env: Vec<String>,
    pub(super) repos: Vec<RepoCheckout>,
    pub(super) shell: Vec<String>,
    pub(super) healthchecks: Vec<HealthcheckPlan>,
    pub(super) steps: Vec<BootstrapStep>,
    pub(super) guest_os: GuestOsAdapter,
}

pub(super) struct ProvisioningBuilder<'a> {
    manifest: &'a AgentManifest,
    guest_os: GuestOsAdapter,
    packages: Vec<String>,
    required_user_groups: Vec<String>,
    agent_shell_env: Vec<String>,
    repos: Vec<RepoCheckout>,
    healthchecks: Vec<HealthcheckPlan>,
    steps: Vec<BootstrapStep>,
    mise_required: bool,
    mise_packages: Vec<String>,
}

impl<'a> ProvisioningBuilder<'a> {
    pub(super) fn render_input(manifest: &'a AgentManifest, plan: &'a ProvisioningPlan) -> BootstrapRenderInput<'a> {
        let guest_os = GuestOsAdapter::for_os(plan.image.os);
        let mut builder = Self::for_render(manifest, guest_os);
        guest_os.apply_base(&mut builder);
        super::plugins::apply(&manifest.spec.plugins, &mut builder);
        super::plugins::apply_runtime_requirements(&mut builder);
        builder.finish()
    }

    fn for_render(manifest: &'a AgentManifest, guest_os: GuestOsAdapter) -> Self {
        let repos = manifest
            .spec
            .bootstrap
            .repos
            .iter()
            .map(RepoCheckout::from_manifest)
            .collect::<Vec<_>>();
        let mut packages = manifest.spec.bootstrap.packages.clone();
        packages.extend(guest_os.base_packages(!repos.is_empty()));

        Self {
            manifest,
            guest_os,
            packages,
            required_user_groups: Vec::new(),
            agent_shell_env: Vec::new(),
            repos,
            healthchecks: manifest
                .spec
                .bootstrap
                .healthchecks
                .iter()
                .map(HealthcheckPlan::from_manifest)
                .collect(),
            steps: Vec::new(),
            mise_required: false,
            mise_packages: Vec::new(),
        }
    }

    fn finish(self) -> BootstrapRenderInput<'a> {
        let packages = dedupe(self.packages);
        let required_user_groups = dedupe(self.required_user_groups);

        BootstrapRenderInput {
            manifest: self.manifest,
            packages,
            required_user_groups,
            agent_shell_env: self.agent_shell_env,
            repos: self.repos,
            shell: self.manifest.spec.bootstrap.shell.clone(),
            healthchecks: self.healthchecks,
            steps: self.steps,
            guest_os: self.guest_os,
        }
    }

    pub(super) fn add_package(&mut self, package: impl Into<String>) {
        self.packages.push(package.into());
    }

    pub(super) fn add_required_user_group(&mut self, group: impl Into<String>) {
        self.required_user_groups.push(group.into());
    }

    pub(super) fn add_agent_shell_env(&mut self, contents: impl Into<String>) {
        self.agent_shell_env.push(contents.into());
    }

    pub(super) fn add_base_system_step(
        &mut self,
        id: impl Into<String>,
        label: impl Into<String>,
        depends_on: impl IntoIterator<Item = &'static str>,
        resources: impl IntoIterator<Item = BootstrapStepResource>,
        contents: impl Into<String>,
    ) {
        self.steps.push(bootstrap_step(
            id,
            label,
            BootstrapStepPhase::System,
            BootstrapStepPlacement::Base,
            depends_on,
            resources,
            contents,
        ));
    }

    pub(super) fn add_base_user_step(
        &mut self,
        id: impl Into<String>,
        label: impl Into<String>,
        depends_on: impl IntoIterator<Item = &'static str>,
        resources: impl IntoIterator<Item = BootstrapStepResource>,
        contents: impl Into<String>,
    ) {
        self.steps.push(bootstrap_step(
            id,
            label,
            BootstrapStepPhase::User,
            BootstrapStepPlacement::Base,
            depends_on,
            resources,
            contents,
        ));
    }

    pub(super) fn add_instance_system_step(
        &mut self,
        id: impl Into<String>,
        label: impl Into<String>,
        depends_on: impl IntoIterator<Item = &'static str>,
        resources: impl IntoIterator<Item = BootstrapStepResource>,
        contents: impl Into<String>,
    ) {
        self.steps.push(bootstrap_step(
            id,
            label,
            BootstrapStepPhase::System,
            BootstrapStepPlacement::Instance,
            depends_on,
            resources,
            contents,
        ));
    }

    pub(super) fn add_instance_user_step(
        &mut self,
        id: impl Into<String>,
        label: impl Into<String>,
        depends_on: impl IntoIterator<Item = &'static str>,
        resources: impl IntoIterator<Item = BootstrapStepResource>,
        contents: impl Into<String>,
    ) {
        self.steps.push(bootstrap_step(
            id,
            label,
            BootstrapStepPhase::User,
            BootstrapStepPlacement::Instance,
            depends_on,
            resources,
            contents,
        ));
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

    pub(super) fn guest_port(&self, name: &str) -> Option<u16> {
        self.manifest.spec.network.ports.get(name).map(|port| port.guest)
    }

    pub(super) const fn agent_user(&self) -> &crate::manifest::User {
        &self.manifest.spec.template.user
    }

    pub(super) const fn code_dir(&self) -> &'static str {
        self.guest_os.capabilities().layout.code_dir
    }

    pub(super) const fn runtime_env_path(&self) -> &'static str {
        self.guest_os.capabilities().layout.runtime_env
    }

    pub(super) const fn guest_layout(&self) -> GuestLayout {
        self.guest_os.capabilities().layout
    }

    pub(super) const fn guest_os(&self) -> GuestOs {
        self.guest_os.os()
    }

    pub(super) fn ca_enabled(&self) -> bool {
        self.manifest.spec.network.ca.is_active(self.manifest.spec.network.mode)
    }

    pub(super) fn ca_env_vars(&self) -> Vec<String> {
        self.manifest.spec.network.ca.env_vars()
    }

    pub(super) const fn requires_mise(&self) -> bool {
        self.mise_required
    }

    pub(super) const fn require_mise(&mut self) {
        self.mise_required = true;
    }

    pub(super) fn require_mise_package(&mut self, package: impl Into<String>) {
        self.require_mise();
        self.mise_packages.push(package.into());
    }

    pub(super) fn mise_packages(&self) -> &[String] {
        &self.mise_packages
    }

    pub(super) const fn ca_bundle_install_command(&self) -> &'static str {
        self.guest_os.ca_bundle_install()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BootstrapStep {
    pub(super) id: String,
    pub(super) label: String,
    pub(super) phase: BootstrapStepPhase,
    pub(super) placement: BootstrapStepPlacement,
    pub(super) depends_on: Vec<String>,
    pub(super) resources: Vec<BootstrapStepResource>,
    pub(super) timeout_seconds: u64,
    pub(super) contents: String,
}

fn bootstrap_step(
    id: impl Into<String>,
    label: impl Into<String>,
    phase: BootstrapStepPhase,
    placement: BootstrapStepPlacement,
    depends_on: impl IntoIterator<Item = &'static str>,
    resources: impl IntoIterator<Item = BootstrapStepResource>,
    contents: impl Into<String>,
) -> BootstrapStep {
    BootstrapStep {
        id: id.into(),
        label: label.into(),
        phase,
        placement,
        depends_on: depends_on.into_iter().map(str::to_owned).collect(),
        resources: resources.into_iter().collect(),
        timeout_seconds: 900,
        contents: contents.into(),
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RepoCheckout {
    pub name: String,
    pub url: String,
    pub path: String,
    pub upstream: Option<String>,
}

impl RepoCheckout {
    fn from_manifest(repo: &Repo) -> Self {
        Self {
            name: repo.checkout_name(),
            url: repo.url.clone(),
            path: repo.checkout_path(),
            upstream: repo.upstream.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HealthcheckPlan {
    pub name: String,
    #[serde(flatten)]
    pub kind: HealthcheckKind,
    pub timeout: Option<String>,
}

impl HealthcheckPlan {
    fn from_manifest(healthcheck: &Healthcheck) -> Self {
        let (name, kind, timeout) = match healthcheck {
            Healthcheck::Command { name, command, timeout } => (
                name,
                HealthcheckKind::Command {
                    command: command.clone(),
                },
                timeout,
            ),
            Healthcheck::Tcp { name, target, timeout } => {
                (name, HealthcheckKind::Tcp { target: target.clone() }, timeout)
            }
            Healthcheck::Http {
                name,
                method,
                url,
                timeout,
            } => (
                name,
                HealthcheckKind::Http {
                    method: method.clone(),
                    url: url.clone(),
                },
                timeout,
            ),
        };
        Self {
            name: name.clone(),
            kind,
            timeout: timeout.clone(),
        }
    }
}

impl<'de> Deserialize<'de> for HealthcheckPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Command {
                name: String,
                command: String,
                timeout: Option<String>,
            },
            Tcp {
                name: String,
                target: String,
                timeout: Option<String>,
            },
            Http {
                name: String,
                method: String,
                url: String,
                timeout: Option<String>,
            },
        }

        let (name, kind, timeout) = match Wire::deserialize(deserializer)? {
            Wire::Command { name, command, timeout } => (name, HealthcheckKind::Command { command }, timeout),
            Wire::Tcp { name, target, timeout } => (name, HealthcheckKind::Tcp { target }, timeout),
            Wire::Http {
                name,
                method,
                url,
                timeout,
            } => (name, HealthcheckKind::Http { method, url }, timeout),
        };

        Ok(Self { name, kind, timeout })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HealthcheckKind {
    Command { command: String },
    Tcp { target: String },
    Http { method: String, url: String },
}

impl std::fmt::Display for HealthcheckKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Command { command } => write!(formatter, "command {command}"),
            Self::Tcp { target } => write!(formatter, "tcp {target}"),
            Self::Http { method, url } => write!(formatter, "http {method} {url}"),
        }
    }
}

fn dedupe(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values.into_iter().filter(|value| seen.insert(value.clone())).collect()
}

#[cfg(test)]
mod tests {
    use agentdp_test_support::snapshot;

    use crate::manifest::{AgentManifest, Repo, UserOptions};
    use crate::provisioning::bootstrap::{
        BootstrapGraphError, BootstrapRenderInput, BootstrapStepPhase, BootstrapStepPlacement, HealthcheckKind,
        HealthcheckPlan, RenderedBootstrapPlan, RenderedBootstrapStep, RepoCheckout, bootstrap_step_spec,
    };
    use crate::provisioning::guest_os::GuestOsAdapter;
    use crate::provisioning::guest_os::linux::bootstrap::render_complete_bootstrap_steps;

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
                upstream: None,
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
            path: Some("./custom/path/".to_owned()),
            upstream: None,
        });

        assert_eq!(checkout.name, "display-name");
        assert_eq!(checkout.path, "custom/path");
    }

    #[test]
    fn healthcheck_plan_round_trips_persisted_yaml() {
        let plan = HealthcheckPlan {
            name: "code_server".to_owned(),
            kind: HealthcheckKind::Http {
                method: "GET".to_owned(),
                url: "http://127.0.0.1:4090/".to_owned(),
            },
            timeout: Some("60s".to_owned()),
        };

        let yaml = serde_yaml::to_string(&plan).unwrap();
        assert!(yaml.contains("kind: http"));

        let decoded: HealthcheckPlan = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(decoded, plan);

        let unknown = "
kind: http
name: code_server
method: GET
url: http://127.0.0.1:4090/
timeout: 60s
unexpected: value
";
        assert!(serde_yaml::from_str::<HealthcheckPlan>(unknown).is_err());
    }

    #[test]
    fn empty_bootstrap_inputs_render_minimal_script() {
        let manifest = manifest();
        let input = bootstrap_render_input(&manifest, Vec::new(), Vec::new());
        let steps = render_complete_bootstrap_steps(&input);

        assert_bootstrap_snapshot("empty_bootstrap_inputs_render_minimal_script", &steps);
    }

    fn manifest() -> AgentManifest {
        serde_yaml::from_str(agentdp_test_support::manifest::minimal()).unwrap()
    }

    #[test]
    fn bootstrap_steps_quote_shell_values() {
        let repos = [RepoCheckout {
            name: "quoted".to_owned(),
            url: "https://example.com/quote's.git".to_owned(),
            path: "nested/$repo\"name".to_owned(),
            upstream: Some("https://example.com/upstream's.git".to_owned()),
        }];
        let manifest_shell = ["printf '%s\n' 'node'".to_owned()];

        let mut manifest = manifest();
        let UserOptions::Linux(linux) = &mut manifest.spec.user.options;
        linux.groups.push("docker".to_owned());
        let input = bootstrap_render_input(&manifest, repos.to_vec(), manifest_shell.to_vec());
        let steps = render_complete_bootstrap_steps(&input);

        assert_bootstrap_snapshot("bootstrap_steps_quote_shell_values", &steps);
    }

    #[test]
    fn bootstrap_graph_validation_rejects_invalid_graphs() {
        assert_invalid_graph(
            vec![test_step("same", []), test_step("same", [])],
            BootstrapGraphError::DuplicateStep {
                step: "same".to_owned(),
            },
        );
        assert_invalid_graph(
            vec![test_step("child", ["missing"])],
            BootstrapGraphError::MissingDependency {
                step: "child".to_owned(),
                dependency: "missing".to_owned(),
            },
        );
        assert_invalid_graph(
            vec![test_step("a", ["b"]), test_step("b", ["a"])],
            BootstrapGraphError::Cycle { step: "a".to_owned() },
        );
        assert_invalid_graph(
            vec![
                test_step("system", ["user"]),
                test_step_with_phase("user", [], BootstrapStepPhase::User),
            ],
            BootstrapGraphError::InvalidPhaseDependency {
                step: "system".to_owned(),
                dependency: "user".to_owned(),
            },
        );
    }

    fn bootstrap_render_input(
        manifest: &AgentManifest,
        repos: Vec<RepoCheckout>,
        shell: Vec<String>,
    ) -> BootstrapRenderInput<'_> {
        BootstrapRenderInput {
            manifest,
            packages: Vec::new(),
            required_user_groups: Vec::new(),
            agent_shell_env: Vec::new(),
            repos,
            shell,
            healthchecks: Vec::new(),
            steps: Vec::new(),
            guest_os: GuestOsAdapter::for_os(manifest.spec.image.os),
        }
    }

    fn assert_bootstrap_snapshot(name: &str, steps: &[super::RenderedBootstrapStep]) {
        let rendered = serde_yaml::to_string(steps).expect("serialize bootstrap steps snapshot");
        snapshot::assert_topic(env!("CARGO_MANIFEST_DIR"), "bootstrap", name, &rendered);
    }

    fn bootstrap_plan(
        steps: impl IntoIterator<Item = RenderedBootstrapStep>,
    ) -> Result<RenderedBootstrapPlan, BootstrapGraphError> {
        RenderedBootstrapPlan::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            steps.into_iter().collect(),
        )
    }

    fn assert_invalid_graph(steps: Vec<RenderedBootstrapStep>, expected: BootstrapGraphError) {
        assert_eq!(bootstrap_plan(steps), Err(expected));
    }

    fn test_step<const N: usize>(id: &str, depends_on: [&str; N]) -> RenderedBootstrapStep {
        test_step_with_phase(id, depends_on, BootstrapStepPhase::System)
    }

    fn test_step_with_phase<const N: usize>(
        id: &str,
        depends_on: [&str; N],
        phase: BootstrapStepPhase,
    ) -> RenderedBootstrapStep {
        RenderedBootstrapStep {
            spec: bootstrap_step_spec(id, id, phase, depends_on, std::iter::empty(), 60),
            placement: BootstrapStepPlacement::Base,
            contents: "#!/usr/bin/env bash\n:\n".to_owned(),
        }
    }
}
