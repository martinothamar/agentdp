use std::path::{Path, PathBuf};

use agentdp_core::provisioning::cloud_init::CloudInitSeed;
use thiserror::Error;

use crate::seed_media;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to create cloud-init seed directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write cloud-init seed file {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    SeedMedia(#[from] seed_media::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedArtifacts {
    pub work_dir: PathBuf,
    pub seed_dir: PathBuf,
    pub meta_data: PathBuf,
    pub network_config: PathBuf,
    pub user_data: PathBuf,
    pub seed_media: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedExtraFile {
    pub name: &'static str,
    pub short_name: [u8; 11],
    pub contents: Vec<u8>,
}

/// Writes cloud-init `NoCloud` seed artifacts for QEMU asynchronously.
///
/// # Errors
///
/// Returns an error if directories or files cannot be created.
pub async fn write_seed_artifacts(work_dir: &Path, cloud_init: &CloudInitSeed) -> Result<SeedArtifacts, Error> {
    write_seed_artifacts_with_extra_files(work_dir, cloud_init, &[]).await
}

/// Writes cloud-init `NoCloud` seed artifacts for QEMU with extra seed-media files.
///
/// # Errors
///
/// Returns an error if directories or files cannot be created.
pub async fn write_seed_artifacts_with_extra_files(
    work_dir: &Path,
    cloud_init: &CloudInitSeed,
    extra_files: &[SeedExtraFile],
) -> Result<SeedArtifacts, Error> {
    let seed_dir = work_dir.join("seed");
    create_directory(&seed_dir).await?;

    let meta_data = seed_dir.join("meta-data");
    let network_config = seed_dir.join("network-config");
    let user_data = seed_dir.join("user-data");
    let seed_media = work_dir.join("seed.img");

    write_file(&meta_data, cloud_init.meta_data.as_bytes()).await?;
    write_file(&network_config, cloud_init.network_config.as_bytes()).await?;
    write_file(&user_data, cloud_init.user_data.as_bytes()).await?;
    for file in extra_files {
        write_file(&seed_dir.join(file.name), &file.contents).await?;
    }
    let extra_media_files = extra_files
        .iter()
        .map(|file| seed_media::ExtraFile {
            long_name: file.name,
            short_name: file.short_name,
            contents: &file.contents,
        })
        .collect::<Vec<_>>();
    seed_media::write_with_extra_files(
        &seed_media,
        &cloud_init.meta_data,
        &cloud_init.network_config,
        &cloud_init.user_data,
        &extra_media_files,
    )
    .await?;

    Ok(SeedArtifacts {
        work_dir: work_dir.to_path_buf(),
        seed_dir,
        meta_data,
        network_config,
        user_data,
        seed_media,
    })
}

async fn create_directory(path: &Path) -> Result<(), Error> {
    tokio::fs::create_dir_all(path)
        .await
        .map_err(|source| Error::CreateDirectory {
            path: path.to_path_buf(),
            source,
        })
}

async fn write_file(path: &Path, contents: &[u8]) -> Result<(), Error> {
    tokio::fs::write(path, contents)
        .await
        .map_err(|source| Error::WriteFile {
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
    use agentdp_core::provisioning::cloud_init::CloudInitSeed;
    use agentdp_core::provisioning::guest_os::linux::cloud_init::CloudInitOptions;
    use agentdp_core::provisioning::{ProvisioningOptions, ProvisioningPlan};
    use agentdp_test_support::manifest;

    use super::write_seed_artifacts;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn writes_explicit_cloud_init_seed_artifacts() {
        let temp = TestTempDir::create("qemu-seed");
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::standard()).unwrap();
        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());
        let cloud_init = CloudInitSeed::from_plan(&manifest, &plan, &CloudInitOptions::default()).unwrap();

        let artifacts = write_seed_artifacts(temp.path(), &cloud_init).await.unwrap();

        assert_eq!(fs::read_to_string(&artifacts.meta_data).unwrap(), cloud_init.meta_data);
        assert_eq!(
            fs::read_to_string(&artifacts.network_config).unwrap(),
            cloud_init.network_config
        );
        assert_eq!(fs::read_to_string(&artifacts.user_data).unwrap(), cloud_init.user_data);
        assert_eq!(fs::metadata(&artifacts.seed_media).unwrap().len(), 32 * 1024 * 1024);
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
