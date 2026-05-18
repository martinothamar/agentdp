use agentdp_core::Context;
use agentdp_core::platform::PlatformPaths;
use agentdp_protocol::{InstanceCreateParams, InstanceCreateResult};

use crate::runtime;

use super::{Error, Instance, guest_access_result, lock, manifest_result, network_result, ports, provisioning, state};

impl Instance {
    pub fn create_new(context: &Context, params: &InstanceCreateParams, paths: &PlatformPaths) -> Result<Self, Error> {
        Self::create_new_with_backend(context, params, paths, |context, input| {
            runtime::Backend::for_manifest(&input.manifest).create(context, input)
        })
    }

    pub(super) fn create_new_with_backend(
        context: &Context,
        params: &InstanceCreateParams,
        paths: &PlatformPaths,
        create_backend: impl FnOnce(&Context, runtime::CreateInput<'_>) -> Result<runtime::CreateOutput, runtime::Error>,
    ) -> Result<Self, Error> {
        let params = provisioning::Params::from_create(params);
        let instance = provisioning::instance_name(&params)?;
        let manifest = provisioning::load_manifest_for_params(context, &params)?;
        let instance_dir = paths.data.join("instances").join(&manifest.name).join(&instance);
        let files = state::files(instance_dir);
        let _lock = lock::InstanceLock::acquire(&files.instance_dir)?;
        state::ensure_absent(&files)?;
        runtime::Backend::for_manifest(&manifest).ensure_absent(&files)?;

        let port_mappings = ports::assign(&manifest, &params.ports)?;
        let manifest_path = params.manifest;
        let manifest_name = manifest.name.clone();
        let created = create_backend(
            context,
            runtime::CreateInput {
                manifest_path: manifest_path.clone(),
                manifest: manifest.clone(),
                instance: instance.clone(),
                paths,
                files: &files,
            },
        )?;
        let state = state::build(
            &manifest_path,
            manifest_name,
            instance,
            &files,
            port_mappings,
            created.guest_access,
            created.state,
        );
        state::write(&manifest_path, &files, &state)?;

        Ok(Self { manifest, files, state })
    }

    pub fn create_result(&self) -> InstanceCreateResult {
        InstanceCreateResult {
            name: self.name(),
            manifest: manifest_result(&self.state),
            state: self.runtime_path(),
            backend: self.backend().create_details(&self.state.backend),
            network: network_result(&self.state.network),
            guest_access: guest_access_result(self.state.guest_access.as_ref()),
        }
    }
}
