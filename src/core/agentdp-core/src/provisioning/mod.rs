pub mod bootstrap;
pub mod cloud_init;
pub mod guest_os;
pub mod host_input;
pub mod image;
mod plugins;
pub mod secrets;
mod template;

use crate::manifest::AgentManifest;
use agentdp_protocol::server_guest::GuestInstancePaths;
use serde::Serialize;

use bootstrap::{BootstrapGraphError, BootstrapStepPlacement, RenderedBootstrapPlan};
use guest_os::GuestOsAdapter;
use image::{CatalogImage, ImageCatalog, ImageRequest};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProvisioningPlan {
    pub image: CatalogImage,
    pub hostname: String,
    pub guest_paths: GuestInstancePaths,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProvisioningOptions {
    pub hostname: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedFile {
    pub path: String,
    pub contents: Vec<u8>,
    pub permissions: String,
    pub owner: Option<String>,
}

impl ProvisioningPlan {
    /// Builds the backend-neutral provisioning plan for a manifest with
    /// environment-specific material supplied by the outer backend.
    #[must_use]
    pub fn from_manifest(manifest: &AgentManifest, options: &ProvisioningOptions) -> Self {
        let image = ImageCatalog::resolve(ImageRequest::from_manifest(manifest));
        let hostname = options.hostname.as_deref().unwrap_or_else(|| manifest.name());
        Self {
            image,
            hostname: hostname.to_owned(),
            guest_paths: GuestOsAdapter::for_os(image.os).instance_paths(),
        }
    }

    /// Renders the base-image bootstrap plan.
    ///
    /// # Errors
    ///
    /// Returns an error if generated bootstrap steps do not form a valid graph.
    pub fn render_base_bootstrap(
        &self,
        manifest: &AgentManifest,
    ) -> Result<RenderedBootstrapPlan, BootstrapGraphError> {
        self.render_complete_bootstrap_plan(manifest)?
            .for_placement(BootstrapStepPlacement::Base)
    }

    /// Renders the concrete-instance bootstrap plan.
    ///
    /// # Errors
    ///
    /// Returns an error if generated bootstrap steps do not form a valid graph.
    pub fn render_instance_bootstrap(
        &self,
        manifest: &AgentManifest,
    ) -> Result<RenderedBootstrapPlan, BootstrapGraphError> {
        self.render_complete_bootstrap_plan(manifest)?
            .for_placement(BootstrapStepPlacement::Instance)
    }

    fn render_complete_bootstrap_plan(
        &self,
        manifest: &AgentManifest,
    ) -> Result<RenderedBootstrapPlan, BootstrapGraphError> {
        assert_eq!(
            self.image.os, manifest.spec.image.os,
            "provisioning plan image OS must match manifest OS"
        );
        GuestOsAdapter::for_os(self.image.os).render_complete_bootstrap_plan(manifest, self)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use agentdp_test_support::{manifest, snapshot};
    use proptest::prelude::*;
    use serde::Serialize;

    use crate::manifest::AgentManifest;
    use crate::provisioning::bootstrap::{BootstrapStepPlacement, RenderedBootstrapPlan};
    use crate::provisioning::cloud_init::CloudInitSeed;
    use crate::provisioning::guest_os::linux::cloud_init::CloudInitOptions;
    use crate::provisioning::{ProvisioningOptions, ProvisioningPlan};

    #[test]
    fn builds_provisioning_plan_from_manifest() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::standard()).unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());

        assert_provisioning_snapshot("builds_plan_from_standard_manifest", &plan);
    }

    #[test]
    fn codex_github_manifest_installs_guest_tooling() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::standard()).unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());

        assert_bootstrap_split_snapshot("codex_github_manifest_installs_guest_tooling", &manifest, &plan);
    }

    #[test]
    fn user_network_plan_without_ca_does_not_install_ca_bundle() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: user-network
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: user
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      codex:
        auth: copy-from-host
      github:
        auth: copy-from-host
    secrets: []
",
        )
        .unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());

        assert_bootstrap_split_snapshot(
            "user_network_plan_without_ca_does_not_install_ca_bundle",
            &manifest,
            &plan,
        );
    }

    #[test]
    fn git_plugin_configures_user_identity_and_defaults() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: git-config
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      git:
        user:
          name:
            from_env: GIT_USER_NAME
          email:
            from_env: GIT_USER_EMAIL
        defaults:
          init_default_branch: main
          autocrlf: false
    secrets: []
",
        )
        .unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());

        assert_bootstrap_split_snapshot("git_plugin_configures_user_identity_and_defaults", &manifest, &plan);
    }

    #[test]
    fn code_server_plugin_removes_extensions() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: code-server-agent
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        code_server:
          guest: 4090
          protocol: tcp
    bootstrap: {}
    plugins:
      code_server:
        remove_extensions:
          - github.copilot
          - github.copilot-chat
    secrets: []
",
        )
        .unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());

        assert_bootstrap_split_snapshot("code_server_plugin_removes_extensions", &manifest, &plan);
    }

    #[test]
    fn podman_plugin_configures_ca_bundle_defaults() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: podman-agent
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: rocky9
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      podman: {}
    secrets: []
",
        )
        .unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());

        assert_bootstrap_split_snapshot("podman_plugin_configures_ca_bundle_defaults", &manifest, &plan);
    }

    #[test]
    fn podman_plugin_configures_rootless_docker_api() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: podman-agent
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: rocky9
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      podman:
        docker_api: true
    secrets: []
",
        )
        .unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());

        assert_bootstrap_split_snapshot("podman_plugin_configures_rootless_docker_api", &manifest, &plan);
    }

    #[test]
    fn podman_plugin_compose_installs_arch_compose_package() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: podman-agent
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      podman:
        compose: true
    secrets: []
",
        )
        .unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());
        let rendered = plan.render_base_bootstrap(&manifest).expect("render base bootstrap");

        assert!(rendered.packages.contains(&"podman-compose".to_owned()));
    }

    #[test]
    fn podman_plugin_compose_installs_rocky_compose_provider_from_epel() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: podman-agent
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: rocky9
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      podman:
        compose: true
    secrets: []
",
        )
        .unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());
        let rendered = plan.render_base_bootstrap(&manifest).expect("render base bootstrap");
        let compose_step = rendered
            .steps
            .iter()
            .find(|step| step.id == "plugin.podman.compose")
            .expect("compose install step");

        assert!(!rendered.packages.contains(&"podman-compose".to_owned()));
        assert!(compose_step.contents.contains("dnf -y install epel-release"));
        assert!(compose_step.contents.contains("dnf -y install podman-compose"));
    }

    #[test]
    fn ca_extra_env_vars_flow_to_runtime_injection() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: ca-extra-env
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ca:
        extra_env_vars:
          - STUDIO_CA_BUNDLE
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      docker: {}
      podman: {}
    secrets: []
",
        )
        .unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());
        let rendered = plan
            .render_instance_bootstrap(&manifest)
            .expect("render instance bootstrap");
        let contents = rendered
            .steps
            .iter()
            .map(|step| step.contents.as_str())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(contents.contains("STUDIO_CA_BUNDLE=/run/agentdp/ca/ca-bundle.pem"));
        assert!(contents.contains("AGENTDP_CA_ENV_VARS=NODE_EXTRA_CA_CERTS,NPM_CONFIG_CAFILE,SSL_CERT_FILE,GIT_SSL_CAINFO,CURL_CA_BUNDLE,REQUESTS_CA_BUNDLE,STUDIO_CA_BUNDLE"));
        assert!(rendered.steps.iter().any(|step| step.id == "system.ca_bundle"));
        assert!(rendered.steps.iter().any(|step| step.id == "plugin.docker.proxy"));
        assert!(rendered.steps.iter().any(|step| step.id == "plugin.podman.ca_bundle"));
    }

    #[test]
    fn user_network_ca_source_enables_ca_steps() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: user-ca
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: user
      ca:
        source: data/ca/corp.pem
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      docker: {}
    secrets: []
",
        )
        .unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());
        let rendered = plan
            .render_instance_bootstrap(&manifest)
            .expect("render instance bootstrap");

        assert!(rendered.steps.iter().any(|step| step.id == "system.ca_bundle"));
        assert!(rendered.steps.iter().any(|step| step.id == "plugin.docker.proxy"));
    }

    #[test]
    fn node_plugin_configures_mise_and_corepack() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: node-agent
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      node:
        from_mise: true
        corepack: true
    secrets: []
",
        )
        .unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());

        assert_bootstrap_split_snapshot("node_plugin_configures_mise_and_corepack", &manifest, &plan);
    }

    #[test]
    fn minimal_linux_guest_gets_tmux_package_and_config() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::minimal()).unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());

        assert_bootstrap_split_snapshot("minimal_linux_guest_gets_tmux_package_and_config", &manifest, &plan);
    }

    #[test]
    fn rocky9_bootstrap_split_uses_rocky_package_manager() {
        let manifest =
            serde_yaml::from_str::<AgentManifest>(&manifest::standard().replace("os: archlinux", "os: rocky9"))
                .unwrap();
        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());

        assert_bootstrap_split_snapshot("rocky9_bootstrap_split_uses_rocky_package_manager", &manifest, &plan);
    }

    #[test]
    fn base_and_instance_bootstrap_splits_are_valid() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::standard()).unwrap();
        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());
        let complete = plan
            .render_complete_bootstrap_plan(&manifest)
            .expect("render complete bootstrap");
        let base = plan.render_base_bootstrap(&manifest).expect("render base bootstrap");
        let instance = plan
            .render_instance_bootstrap(&manifest)
            .expect("render instance bootstrap");
        let base_step_ids = step_ids(&base);
        let instance_step_ids = step_ids(&instance);

        assert_eq!(
            base_step_ids
                .union(&instance_step_ids)
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            step_ids(&complete)
        );
        assert!(base_step_ids.is_disjoint(&instance_step_ids));
        assert!(base.healthchecks.is_empty());
        assert!(base.repos.is_empty());
        assert!(base.shell.is_empty());
        assert!(!base.packages.is_empty());
        assert!(instance.packages.is_empty());
        assert_eq!(instance.repos, complete.repos);
        assert_eq!(instance.shell, manifest.spec.bootstrap.shell.as_slice());
        assert_eq!(instance.healthchecks, complete.healthchecks);
        assert!(base.steps.iter().any(|step| step.id == "system.packages"));
        assert!(base.steps.iter().any(|step| step.id == "system.guest_tooling"));
        assert!(!base.steps.iter().any(|step| step.id == "system.user_handoff"));
        assert!(instance.steps.iter().any(|step| step.id == "system.user_handoff"));
        assert!(instance.steps.iter().any(|step| step.id == "user.guestd"));
        assert_split_dependencies(&base, BootstrapStepPlacement::Base);
        assert_split_dependencies(&instance, BootstrapStepPlacement::Instance);
    }

    proptest! {
        #[test]
        fn valid_manifest_shapes_build_parseable_cloud_init_seed(
            name in identifier(),
            user in user_name(),
            package_names in prop::collection::vec(package_name(), 0..8),
            shell_commands in prop::collection::vec(shell_command(), 0..4),
            port_name in identifier(),
            port in 1_u16..=u16::MAX,
            cpus in 1_u16..16,
            memory_gb in 1_u16..64,
            storage_gb in 1_u16..512,
        ) {
            let input = GeneratedManifestInput {
                name: &name,
                user: &user,
                package_names: &package_names,
                shell_commands: &shell_commands,
                port_name: &port_name,
                port,
                cpus,
                memory_gb,
                storage_gb,
            };
            let manifest_yaml = generated_manifest(&input);
            let manifest = serde_yaml::from_str::<AgentManifest>(&manifest_yaml).expect("parse generated manifest");

            prop_assert!(manifest.validate().is_ok());
            let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());
            let seed = CloudInitSeed::from_plan(&manifest, &plan, &CloudInitOptions::default())
                .expect("render cloud-init");
            let meta_data = serde_yaml::from_str::<serde_yaml::Value>(&seed.meta_data).expect("parse meta-data");

            prop_assert_eq!(&plan.hostname, &name);
            prop_assert_eq!(&meta_data["instance-id"], &serde_yaml::Value::from(name));
            prop_assert!(seed.user_data.starts_with("#cloud-config\n"));
            prop_assert!(serde_yaml::from_str::<serde_yaml::Value>(
                seed.user_data.strip_prefix("#cloud-config\n").expect("cloud-config header"),
            )
            .is_ok());
        }
    }

    fn assert_provisioning_snapshot(name: &str, plan: &ProvisioningPlan) {
        let rendered = serde_yaml::to_string(plan).expect("serialize provisioning plan snapshot");
        snapshot::assert_topic(env!("CARGO_MANIFEST_DIR"), "provisioning", name, &rendered);
    }

    #[derive(Serialize)]
    struct BootstrapSplitSnapshot {
        base: crate::provisioning::bootstrap::RenderedBootstrapPlan,
        instance: crate::provisioning::bootstrap::RenderedBootstrapPlan,
    }

    fn assert_bootstrap_split_snapshot(name: &str, manifest: &AgentManifest, plan: &ProvisioningPlan) {
        let snapshot = BootstrapSplitSnapshot {
            base: plan.render_base_bootstrap(manifest).expect("render base bootstrap"),
            instance: plan
                .render_instance_bootstrap(manifest)
                .expect("render instance bootstrap"),
        };
        let rendered = serde_yaml::to_string(&snapshot).expect("serialize bootstrap split snapshot");
        snapshot::assert_topic(env!("CARGO_MANIFEST_DIR"), "provisioning", name, &rendered);
    }

    fn step_ids(plan: &RenderedBootstrapPlan) -> std::collections::BTreeSet<&str> {
        plan.steps.iter().map(|step| step.id.as_str()).collect()
    }

    fn assert_split_dependencies(plan: &RenderedBootstrapPlan, placement: BootstrapStepPlacement) {
        let step_ids = step_ids(plan);
        for step in &plan.steps {
            assert_eq!(step.placement, placement);
            for dependency in &step.depends_on {
                assert!(
                    step_ids.contains(dependency.as_str()),
                    "{} depends on omitted step {}",
                    step.id,
                    dependency
                );
            }
        }
    }

    struct GeneratedManifestInput<'a> {
        name: &'a str,
        user: &'a str,
        package_names: &'a [String],
        shell_commands: &'a [String],
        port_name: &'a str,
        port: u16,
        cpus: u16,
        memory_gb: u16,
        storage_gb: u16,
    }

    fn generated_manifest(input: &GeneratedManifestInput<'_>) -> String {
        let GeneratedManifestInput {
            name,
            user,
            package_names,
            shell_commands,
            port_name,
            port,
            cpus,
            memory_gb,
            storage_gb,
        } = input;
        let mut output = format!(
            r"apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: {name:?}
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: {user:?}
    resources:
      cpus: {cpus}
      memory: {memory_gb}G
      storage: {storage_gb}G
    network:
      mode: mediated
      ports:
        {port_name:?}:
          guest: {port}
          protocol: tcp
    bootstrap:
"
        );
        push_string_list(&mut output, "packages", package_names);
        push_string_list(&mut output, "shell", shell_commands);
        output.push_str("    secrets: []\n    plugins: {}\n");
        output
    }

    fn push_string_list(output: &mut String, name: &str, values: &[String]) {
        if values.is_empty() {
            let _ = writeln!(output, "      {name}: []");
            return;
        }

        let _ = writeln!(output, "      {name}:");
        for value in values {
            let _ = writeln!(output, "        - {value:?}");
        }
    }

    fn identifier() -> impl Strategy<Value = String> {
        "[A-Za-z0-9._-]{1,24}"
    }

    fn user_name() -> impl Strategy<Value = String> {
        identifier().prop_filter("agent user must not be root", |value| value != "root")
    }

    fn package_name() -> impl Strategy<Value = String> {
        "[A-Za-z0-9._+-]{1,32}"
    }

    fn shell_command() -> impl Strategy<Value = String> {
        "[A-Za-z0-9._/ =:-]{1,64}".prop_filter("shell command must not be blank", |value| !value.trim().is_empty())
    }
}
