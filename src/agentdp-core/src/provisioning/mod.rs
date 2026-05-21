pub mod bootstrap;
pub mod cloud_init;
pub mod image;
mod plugins;
mod shell;
mod templates;

use crate::manifest::AgentManifest;
use thiserror::Error;

use bootstrap::BootstrapPlan;
use cloud_init::CloudInitSeed;
use image::{CatalogImage, ImageCatalog, ImageRequest};

pub const AGENT_HOME: &str = "/data/home";
pub const CODE_DIR: &str = "/data/home/code";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisioningPlan {
    pub image: CatalogImage,
    pub bootstrap: BootstrapPlan,
    pub cloud_init: CloudInitSeed,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProvisioningOptions {
    pub hostname: Option<String>,
    pub ssh_authorized_key: Option<String>,
    pub seed_files: Vec<SeedFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedFile {
    pub path: String,
    pub contents: Vec<u8>,
    pub permissions: String,
    pub owner: Option<String>,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    CloudInit(#[from] cloud_init::Error),
}

impl ProvisioningPlan {
    /// Builds the backend-neutral provisioning plan for a manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if generated provisioning artifacts cannot be serialized.
    pub fn from_manifest(manifest: &AgentManifest) -> Result<Self, Error> {
        Self::from_manifest_with_options(manifest, &ProvisioningOptions::default())
    }

    /// Builds the backend-neutral provisioning plan for a manifest with
    /// environment-specific material supplied by the outer backend.
    ///
    /// # Errors
    ///
    /// Returns an error if generated provisioning artifacts cannot be serialized.
    pub fn from_manifest_with_options(manifest: &AgentManifest, options: &ProvisioningOptions) -> Result<Self, Error> {
        let image = ImageCatalog::resolve(ImageRequest::from_manifest(manifest));
        let hostname = options.hostname.as_deref().unwrap_or(&manifest.name);
        let bootstrap = BootstrapPlan::from_manifest_with_hostname(manifest, hostname);
        let cloud_init = CloudInitSeed::from_plan(manifest, &bootstrap, options)?;
        Ok(Self {
            image,
            bootstrap,
            cloud_init,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use agentdp_test_support::{manifest, snapshot};

    use crate::manifest::AgentManifest;
    use crate::provisioning::ProvisioningPlan;

    #[test]
    fn builds_provisioning_plan_from_manifest() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::standard()).unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest).unwrap();

        snapshot::assert_topic(
            env!("CARGO_MANIFEST_DIR"),
            "provisioning",
            "builds_plan_from_standard_manifest",
            &plan_snapshot(&plan),
        );
    }

    #[test]
    fn codex_github_manifest_installs_pr_loop_tools() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::standard()).unwrap();

        let plan = ProvisioningPlan::from_manifest(&manifest).unwrap();

        assert!(!plan.bootstrap.packages.iter().any(|package| package == "nodejs"));
        assert!(plan.bootstrap.packages.iter().any(|package| package == "mise"));
        assert!(plan.bootstrap.packages.iter().any(|package| package == "tmux"));
        assert!(plan.bootstrap.script.contains("node@lts"));
        assert!(plan.bootstrap.script.contains("[projects.\"/data/home/code\"]"));
        assert!(plan.bootstrap.script.contains("trust_level = \"trusted\""));
        assert!(plan.bootstrap.script.contains("loginctl enable-linger"));
        assert!(plan.bootstrap.script.contains("agentdp-codex-session"));
        assert!(plan.bootstrap.script.contains("agentdp-pr register"));
        assert!(plan.bootstrap.script.contains("agentdp-pr-subscriber.service"));
    }

    fn plan_snapshot(plan: &ProvisioningPlan) -> String {
        let mut output = String::new();
        let _ = writeln!(
            &mut output,
            "image: {:?} {:?} {:?}",
            plan.image.os, plan.image.architecture, plan.image.variant
        );
        let _ = writeln!(&mut output, "packages: {}", plan.bootstrap.packages.join(", "));
        let _ = writeln!(&mut output, "groups: {}", plan.bootstrap.user.groups.join(", "));
        output.push_str("repos:\n");
        for repo in &plan.bootstrap.repos {
            let _ = writeln!(&mut output, "  {} -> {}", repo.url, repo.path);
        }
        output.push_str("healthchecks:\n");
        for healthcheck in &plan.bootstrap.healthchecks {
            let _ = writeln!(
                &mut output,
                "  {}: {} timeout={}",
                healthcheck.name,
                healthcheck.kind,
                healthcheck.timeout.as_deref().unwrap_or("<default>")
            );
        }
        output.push_str("--- meta-data\n");
        output.push_str(&plan.cloud_init.meta_data);
        output.push_str("--- user-data\n");
        output.push_str(&plan.cloud_init.user_data);
        output
    }
}
