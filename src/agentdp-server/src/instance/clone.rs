use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use agentdp_core::Context;
use agentdp_core::platform;
use agentdp_core::platform::PlatformPaths;
use agentdp_protocol::{InstanceCloneParams, InstanceCloneResult, InstanceRef};

use super::{
    Error, Instance, guest_access_result, lock, manifest_result, network_result, path_text, ports, provisioning, state,
};

impl Instance {
    pub fn clone_existing(
        context: &Context,
        params: &InstanceCloneParams,
        paths: &PlatformPaths,
    ) -> Result<Self, Error> {
        provisioning::validate_instance_name(&params.source)?;
        provisioning::validate_instance_name(&params.target)?;
        if params.source == params.target {
            return Err(Error::CloneSameInstance {
                instance: params.source.clone(),
            });
        }

        let mut source = Self::load_existing(
            context,
            &InstanceRef {
                manifest: params.manifest.clone(),
                instance: params.source.clone(),
            },
            paths,
        )?;
        let _source_lock = source.acquire_lock()?;
        source.reload_state()?;
        if source.state.status == state::InstanceStatus::Running {
            return Err(Error::CloneRunning {
                name: source.name(),
                instance: source.state.instance,
            });
        }

        let target_instance = params.target.clone();
        let target_files = state::files(
            paths
                .data
                .join("instances")
                .join(&source.state.manifest_name)
                .join(&target_instance),
        );
        let _target_lock = lock::InstanceLock::acquire(&target_files.instance_dir)?;
        state::ensure_absent(&target_files)?;
        source.backend().ensure_absent(&target_files)?;

        copy_dir_recursive(&source.files.instance_dir, &target_files.instance_dir)?;
        let mut target_state = source.state.clone();
        target_state.instance.clone_from(&target_instance);
        target_state.status = source.state.status;
        target_state.manifest.copy = path_text(&target_files.manifest);
        let reserved_ports = source
            .state
            .network
            .ports
            .values()
            .map(|port| port.host)
            .collect::<BTreeSet<_>>();
        target_state.network.ports = ports::assign_avoiding(&source.manifest, &params.ports, reserved_ports)?;
        target_state.guest_access = source
            .state
            .guest_access
            .as_ref()
            .map(|access| clone_guest_access(access, &target_files))
            .transpose()?;
        target_state.readiness = None;
        target_state.backend = source.backend().clone_state(
            &source.state.backend,
            &target_files,
            paths,
            &source.state.manifest_name,
            &target_instance,
        );
        state::write_runtime(&target_files, &target_state)?;

        Ok(Self {
            manifest: source.manifest,
            files: target_files,
            state: target_state,
        })
    }

    pub fn clone_result(&self, source: impl Into<String>) -> InstanceCloneResult {
        InstanceCloneResult {
            source: source.into(),
            name: self.name(),
            manifest: manifest_result(&self.state),
            state: self.runtime_path(),
            backend: self.backend().create_details(&self.state.backend),
            network: network_result(&self.state.network),
            guest_access: guest_access_result(self.state.guest_access.as_ref()),
        }
    }
}

fn clone_guest_access(
    access: &state::GuestAccessState,
    files: &state::InstanceFiles,
) -> Result<state::GuestAccessState, Error> {
    let private_key = files
        .instance_dir
        .join("generated")
        .join("qemu")
        .join("ssh")
        .join("agentdp_ed25519");
    platform::restrict_private_file_permissions(&private_key).map_err(|source| {
        Error::RestrictClonedPrivateKeyPermissions {
            path: private_key.clone(),
            source,
        }
    })?;
    Ok(state::GuestAccessState {
        user: access.user.clone(),
        private_key: path_text(&private_key),
        public_key: path_text(&private_key.with_extension("pub")),
    })
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<(), Error> {
    fs::create_dir_all(destination).map_err(|source_error| Error::CopyInstanceDirectory {
        source_path: source.to_path_buf(),
        destination_path: destination.to_path_buf(),
        source: source_error,
    })?;

    for entry in fs::read_dir(source).map_err(|source_error| Error::CopyInstanceDirectory {
        source_path: source.to_path_buf(),
        destination_path: destination.to_path_buf(),
        source: source_error,
    })? {
        let entry = entry.map_err(|source_error| Error::CopyInstanceDirectory {
            source_path: source.to_path_buf(),
            destination_path: destination.to_path_buf(),
            source: source_error,
        })?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type().map_err(|source_error| Error::CopyInstanceDirectory {
            source_path: source_path.clone(),
            destination_path: destination_path.clone(),
            source: source_error,
        })?;
        if file_type.is_dir() {
            copy_dir_recursive(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            copy_file(&source_path, &destination_path)?;
        }
    }
    Ok(())
}

fn copy_file(source_path: &PathBuf, destination_path: &PathBuf) -> Result<(), Error> {
    fs::copy(source_path, destination_path).map_err(|source| Error::CopyInstanceFile {
        source_path: source_path.clone(),
        destination_path: destination_path.clone(),
        source,
    })?;
    Ok(())
}
