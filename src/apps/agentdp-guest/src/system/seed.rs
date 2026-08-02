use std::path::{Path, PathBuf};

use agentdp_protocol::server_guest::{
    BOOTSTRAP_PLAN_VERSION, BootstrapPlan, GUEST_CONTROL_PROTOCOL_VERSION, GUEST_INSTANCE_SPEC_VERSION, GuestHello,
    GuestInstanceSpec, GuestMessage, GuestMessageKind, GuestdRole,
};
use tokio::fs;

use crate::{Error, Result};

use super::Config;
use super::os::refresh_instance_spec_from_seed;

#[derive(Debug)]
pub(super) struct SeedSpec {
    pub(super) plan: BootstrapPlan,
    pub(super) instance: GuestInstanceSpec,
}

impl SeedSpec {
    pub(super) async fn load(config: &Config) -> Result<Self> {
        refresh_instance_spec_from_seed(&config.instance_spec).await?;
        Self::load_local(config).await
    }

    pub(super) async fn load_local(config: &Config) -> Result<Self> {
        let instance = read_instance_spec(&config.instance_spec).await?;
        validate_instance_spec(&instance, &config.instance_spec)?;
        let plan = read_bootstrap_plan(Path::new(&instance.paths.bootstrap_plan)).await?;
        if plan.plan_version != BOOTSTRAP_PLAN_VERSION {
            return Err(Error::Message(format!(
                "unsupported bootstrap plan version {}; expected {BOOTSTRAP_PLAN_VERSION}",
                plan.plan_version
            )));
        }
        validate_bootstrap_plan(&plan, &instance.paths.bootstrap_root)?;
        let manifest = read_seed_file(Path::new(&instance.paths.manifest), "agent manifest").await?;
        validate_seed_inputs(&manifest)?;
        Ok(Self { plan, instance })
    }

    pub(super) fn hello_message(&self) -> GuestMessage {
        GuestMessage::new(
            "msg_0",
            GuestMessageKind::Hello(GuestHello {
                protocol_version: GUEST_CONTROL_PROTOCOL_VERSION,
                guestd_role: GuestdRole::System,
                guestd_version: env!("CARGO_PKG_VERSION").to_owned(),
                manifest: self.instance.manifest.clone(),
                instance: self.instance.instance.clone(),
                os: self.instance.platform.as_str().to_owned(),
                hostname: self.instance.hostname.clone(),
                user: self.instance.user.name.clone(),
            }),
        )
    }

    pub(super) fn control_path(&self) -> PathBuf {
        PathBuf::from(&self.instance.paths.control)
    }

    pub(super) fn bootstrap_state_path(&self) -> PathBuf {
        PathBuf::from(&self.instance.paths.bootstrap_state)
    }

    pub(super) fn bootstrap_root_path(&self) -> PathBuf {
        PathBuf::from(&self.instance.paths.bootstrap_root)
    }

    pub(super) fn user_name(&self) -> &str {
        &self.instance.user.name
    }

    pub(super) fn user_home(&self) -> &str {
        &self.instance.user.home
    }
}

async fn read_bootstrap_plan(path: &Path) -> Result<BootstrapPlan> {
    let contents = read_seed_file(path, "bootstrap plan").await?;
    serde_json::from_str(&contents)
        .map_err(|source| Error::Message(format!("failed to parse bootstrap plan {}: {source}", path.display())))
}

pub(super) fn validate_bootstrap_plan(plan: &BootstrapPlan, bootstrap_root: &str) -> Result<()> {
    for step in &plan.steps {
        validate_script_path(&step.id, &step.script, bootstrap_root)?;
    }
    Ok(())
}

fn validate_instance_spec(instance: &GuestInstanceSpec, path: &Path) -> Result<()> {
    if instance.schema_version != GUEST_INSTANCE_SPEC_VERSION {
        return Err(Error::Message(format!(
            "unsupported instance spec version {} in {}",
            instance.schema_version,
            path.display()
        )));
    }
    validate_seed_path("agent manifest path", &instance.paths.manifest)?;
    validate_seed_path("instance spec path", &instance.paths.instance_spec)?;
    validate_seed_path("bootstrap plan path", &instance.paths.bootstrap_plan)?;
    validate_seed_path("bootstrap root path", &instance.paths.bootstrap_root)?;
    validate_seed_path("bootstrap state path", &instance.paths.bootstrap_state)?;
    validate_seed_path("control channel path", &instance.paths.control)?;
    Ok(())
}

fn validate_seed_inputs(manifest: &str) -> Result<()> {
    if manifest.trim().is_empty() {
        return Err(Error::Message("agent manifest seed is empty".to_owned()));
    }
    Ok(())
}

fn validate_seed_path(label: &str, path: &str) -> Result<()> {
    if path.trim().is_empty() {
        return Err(Error::Message(format!("{label} must not be empty")));
    }
    Ok(())
}

fn validate_script_path(step: &str, script: &str, bootstrap_root: &str) -> Result<()> {
    let path = Path::new(script);
    if script.is_empty() || path.is_absolute() {
        return Err(Error::Message(format!(
            "bootstrap step {step} script must be relative to {bootstrap_root}"
        )));
    }
    if !path
        .components()
        .all(|component| matches!(component, std::path::Component::Normal(_)))
    {
        return Err(Error::Message(format!(
            "bootstrap step {step} script must not contain . or .. path components"
        )));
    }
    Ok(())
}

async fn read_instance_spec(path: &Path) -> Result<GuestInstanceSpec> {
    let contents = read_seed_file(path, "instance spec").await?;
    serde_json::from_str(&contents)
        .map_err(|source| Error::Message(format!("failed to parse instance spec {}: {source}", path.display())))
}

async fn read_seed_file(path: &Path, label: &str) -> Result<String> {
    fs::read_to_string(path)
        .await
        .map_err(|source| Error::Message(format!("failed to read {label} {}: {source}", path.display())))
}
