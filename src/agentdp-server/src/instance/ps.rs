use std::fs;
use std::path::Path;

use agentdp_core::Context;
use agentdp_core::platform::PlatformPaths;
use agentdp_protocol::{InstanceListItem, InstancePsParams, InstancePsResult};

use crate::runtime;

use super::{Error, path_text, provisioning, state};

pub fn ps(context: &Context, params: &InstancePsParams, paths: &PlatformPaths) -> Result<InstancePsResult, Error> {
    let instances = match &params.manifest {
        Some(manifest_path) => {
            let manifest_params = provisioning::Params {
                manifest: manifest_path.clone(),
                instance: None,
                ports: std::collections::BTreeMap::default(),
            };
            provisioning::validate_manifest_path(&manifest_params)?;
            let manifest = provisioning::load_manifest_for_params(context, &manifest_params)?;
            list_manifest_instances(paths, &manifest.name)?
        }
        None => list_all_instances(paths)?,
    };

    Ok(InstancePsResult { instances })
}

fn list_all_instances(paths: &PlatformPaths) -> Result<Vec<InstanceListItem>, Error> {
    let root = paths.data.join("instances");
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut instances = Vec::new();
    for entry in read_dir(&root)? {
        let entry = entry.map_err(|source| Error::ReadInstanceDirectory {
            path: root.clone(),
            source,
        })?;
        let manifest_dir = entry.path();
        if manifest_dir.is_dir() {
            instances.extend(list_manifest_dir(&manifest_dir)?);
        }
    }
    instances.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(instances)
}

fn list_manifest_instances(paths: &PlatformPaths, manifest_name: &str) -> Result<Vec<InstanceListItem>, Error> {
    let manifest_dir = paths.data.join("instances").join(manifest_name);
    if !manifest_dir.exists() {
        return Ok(Vec::new());
    }
    list_manifest_dir(&manifest_dir)
}

fn list_manifest_dir(manifest_dir: &Path) -> Result<Vec<InstanceListItem>, Error> {
    let mut instances = Vec::new();
    for entry in read_dir(manifest_dir)? {
        let entry = entry.map_err(|source| Error::ReadInstanceDirectory {
            path: manifest_dir.to_path_buf(),
            source,
        })?;
        let instance_dir = entry.path();
        if !instance_dir.is_dir() {
            continue;
        }
        let files = state::files(instance_dir);
        if !files.runtime.exists() {
            continue;
        }
        let state = state::read(&files)?;
        let runtime = runtime::Backend::from_state(&state.backend).runtime_summary(&state.backend);
        instances.push(InstanceListItem {
            name: format!("{}/{}", state.manifest_name, state.instance),
            manifest_name: state.manifest_name,
            instance: state.instance,
            status: state.status.to_string(),
            state: path_text(&files.runtime),
            pid: runtime.pid,
            ready: state.readiness.as_ref().map(|readiness| readiness.ready),
        });
    }
    instances.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(instances)
}

fn read_dir(path: &Path) -> Result<fs::ReadDir, Error> {
    fs::read_dir(path).map_err(|source| Error::ReadInstanceDirectory {
        path: path.to_path_buf(),
        source,
    })
}
