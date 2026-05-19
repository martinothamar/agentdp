use std::collections::BTreeMap;
use std::path::PathBuf;

use agentdp_core::Context;
use agentdp_core::manifest::{self, AgentManifest, load_manifest};
use agentdp_core::platform::PlatformPaths;
use agentdp_protocol::{InstanceCreateParams, InstanceRef, ProvisioningPlanParams, ProvisioningPlanResult};
use thiserror::Error;

use crate::runtime;

#[derive(Debug, Error)]
pub enum Error {
    #[error("manifest path must be absolute for server provisioning requests: {0}")]
    RelativeManifest(PathBuf),
    #[error("instance must contain only ASCII letters, digits, '.', '_', and '-'")]
    InvalidInstance,
    #[error("{0}")]
    Manifest(#[from] manifest::Error),
    #[error("{0}")]
    Backend(#[from] runtime::Error),
}

#[derive(Debug, Clone)]
pub(super) struct Params {
    pub(super) manifest: PathBuf,
    pub(super) instance: Option<String>,
    pub(super) ports: BTreeMap<String, u16>,
}

impl Params {
    pub(super) fn from_plan(params: &ProvisioningPlanParams) -> Self {
        Self {
            manifest: params.manifest.clone(),
            instance: params.instance.clone(),
            ports: params.ports.clone(),
        }
    }

    pub(super) fn from_create(params: &InstanceCreateParams) -> Self {
        Self {
            manifest: params.manifest.clone(),
            instance: Some(params.instance.clone()),
            ports: params.ports.clone(),
        }
    }

    pub(super) fn from_ref(params: &InstanceRef) -> Self {
        Self {
            manifest: params.manifest.clone(),
            instance: Some(params.instance.clone()),
            ports: BTreeMap::default(),
        }
    }
}

pub fn plan(
    context: &Context,
    params: &ProvisioningPlanParams,
    paths: &PlatformPaths,
) -> Result<ProvisioningPlanResult, Error> {
    let params = Params::from_plan(params);
    validate_manifest_path(&params)?;
    let instance = instance_name(&params)?;
    let manifest = load_manifest_for_params(context, &params)?;
    runtime::Backend::for_manifest(&manifest)
        .plan(context, params.manifest, manifest, instance, paths)
        .map_err(Error::Backend)
}

pub(super) fn validate_manifest_path(params: &Params) -> Result<(), Error> {
    if !params.manifest.is_absolute() {
        return Err(Error::RelativeManifest(params.manifest.clone()));
    }
    Ok(())
}

pub(super) fn instance_name(params: &Params) -> Result<String, Error> {
    let instance = params.instance.clone().unwrap_or_else(|| "preview".to_owned());
    validate_instance_name(&instance)?;
    Ok(instance)
}

pub(super) fn load_manifest_for_params(context: &Context, params: &Params) -> Result<AgentManifest, Error> {
    validate_manifest_path(params)?;
    load_manifest(context, &params.manifest).map_err(Error::Manifest)
}

pub(super) fn validate_instance_name(instance: &str) -> Result<(), Error> {
    if instance.is_empty() {
        return Err(Error::InvalidInstance);
    }

    if instance
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        Ok(())
    } else {
        Err(Error::InvalidInstance)
    }
}
