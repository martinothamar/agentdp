use agentdp_core::Context;
use agentdp_core::manifest;
use agentdp_core::platform::PlatformPaths;
use agentdp_protocol::InstanceRef;

use super::{Error, Instance, provisioning, state};

impl Instance {
    pub fn load_existing(context: &Context, params: &InstanceRef, paths: &PlatformPaths) -> Result<Self, Error> {
        let params = provisioning::Params::from_ref(params);
        let instance = provisioning::instance_name(&params)?;
        let locator_manifest = provisioning::load_manifest_for_params(context, &params)?;
        let instance_dir = paths
            .data
            .join("instances")
            .join(&locator_manifest.name)
            .join(&instance);
        let files = state::files(instance_dir);
        let state = state::read(&files)?;
        let manifest = manifest::load_manifest(context, &files.manifest)?;
        Ok(Self { manifest, files, state })
    }
}
