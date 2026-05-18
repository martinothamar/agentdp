use std::fs;
use std::path::{Path, PathBuf};

use agentdp_core::platform;
use agentdp_core::provisioning::ProvisioningPlan;
use thiserror::Error;

use super::seed_media;

#[derive(Debug, Error)]
pub(super) enum Error {
    #[error("failed to create QEMU seed directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write QEMU seed file {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to make QEMU bootstrap script executable {path}: {source}")]
    SetExecutable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    SeedMedia(#[from] seed_media::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SeedArtifacts {
    pub(super) work_dir: PathBuf,
    pub(super) seed_dir: PathBuf,
    pub(super) scripts_dir: PathBuf,
    pub(super) meta_data: PathBuf,
    pub(super) user_data: PathBuf,
    pub(super) bootstrap_script: PathBuf,
    pub(super) seed_media: PathBuf,
}

/// Writes QEMU seed artifacts derived from the shared provisioning plan.
///
/// # Errors
///
/// Returns an error if directories or files cannot be created.
pub(super) fn write_seed_artifacts(work_dir: &Path, plan: &ProvisioningPlan) -> Result<SeedArtifacts, Error> {
    let seed_dir = work_dir.join("seed");
    let scripts_dir = work_dir.join("scripts");
    create_directory(&seed_dir)?;
    create_directory(&scripts_dir)?;

    let meta_data = seed_dir.join("meta-data");
    let user_data = seed_dir.join("user-data");
    let bootstrap_script = scripts_dir.join("bootstrap.sh");
    let seed_media = work_dir.join("seed.img");

    write_file(&meta_data, &plan.cloud_init.meta_data)?;
    write_file(&user_data, &plan.cloud_init.user_data)?;
    write_file(&bootstrap_script, &plan.bootstrap.script)?;
    seed_media::write(&seed_media, &plan.cloud_init.meta_data, &plan.cloud_init.user_data)?;
    platform::set_executable(&bootstrap_script).map_err(|source| Error::SetExecutable {
        path: bootstrap_script.clone(),
        source,
    })?;

    Ok(SeedArtifacts {
        work_dir: work_dir.to_path_buf(),
        seed_dir,
        scripts_dir,
        meta_data,
        user_data,
        bootstrap_script,
        seed_media,
    })
}

fn create_directory(path: &Path) -> Result<(), Error> {
    fs::create_dir_all(path).map_err(|source| Error::CreateDirectory {
        path: path.to_path_buf(),
        source,
    })
}

fn write_file(path: &Path, contents: &str) -> Result<(), Error> {
    fs::write(path, contents).map_err(|source| Error::WriteFile {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use agentdp_core::manifest::AgentManifest;
    use agentdp_core::provisioning::ProvisioningPlan;
    use agentdp_test_support::manifest;

    use super::write_seed_artifacts;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn writes_seed_artifacts_from_provisioning_plan() {
        let temp = TestTempDir::create("qemu-seed");
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::standard()).unwrap();
        let plan = ProvisioningPlan::from_manifest(&manifest).unwrap();

        let artifacts = write_seed_artifacts(temp.path(), &plan).unwrap();

        assert_eq!(
            fs::read_to_string(&artifacts.meta_data).unwrap(),
            plan.cloud_init.meta_data
        );
        assert_eq!(
            fs::read_to_string(&artifacts.user_data).unwrap(),
            plan.cloud_init.user_data
        );
        assert_eq!(
            fs::read_to_string(&artifacts.bootstrap_script).unwrap(),
            plan.bootstrap.script
        );
        assert_eq!(fs::metadata(&artifacts.seed_media).unwrap().len(), 4 * 1024 * 1024);
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn create(name: &str) -> Self {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("agentdp-{name}-{}-{timestamp}-{id}", std::process::id()));
            fs::create_dir(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _result = fs::remove_dir_all(&self.path);
        }
    }
}
