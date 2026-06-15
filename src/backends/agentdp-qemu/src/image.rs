use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use agentdp_core::Context;
use agentdp_core::manifest::GuestOs;
use agentdp_core::provisioning::image::{CatalogImage, ImageArchitecture, ImageVariant};
use agentdp_platform as platform;
use thiserror::Error;
use tokio::process::Command;

const QEMU_IMAGES: &[QemuImageSpec] = &[
    QemuImageSpec {
        catalog: CatalogImage {
            os: GuestOs::Archlinux,
            architecture: ImageArchitecture::X86_64,
            variant: ImageVariant::Cloud,
        },
        qemu: QemuImage {
            url: "https://fastly.mirror.pkgbuild.com/images/latest/Arch-Linux-x86_64-cloudimg.qcow2",
            cache_key: "archlinux-x86_64-cloudimg.qcow2",
            format: "qcow2",
        },
    },
    QemuImageSpec {
        catalog: CatalogImage {
            os: GuestOs::Rocky9,
            architecture: ImageArchitecture::X86_64,
            variant: ImageVariant::Cloud,
        },
        qemu: QemuImage {
            url: "https://download.rockylinux.org/pub/rocky/9/images/x86_64/Rocky-9-GenericCloud-Base.latest.x86_64.qcow2",
            cache_key: "rocky-9-genericcloud-base-latest-x86_64.qcow2",
            format: "qcow2",
        },
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QemuImageSpec {
    catalog: CatalogImage,
    qemu: QemuImage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QemuImage {
    pub url: &'static str,
    pub cache_key: &'static str,
    pub format: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceImage {
    url: &'static str,
    cache_key: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageCachePlan {
    source: SourceImage,
    pub cache_dir: PathBuf,
    pub image_path: PathBuf,
    pub download_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageCacheStatus {
    AlreadyPresent,
    Downloaded,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to create image cache directory {path}: {source}")]
    CreateCacheDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("base image downloader curl was not found on PATH")]
    MissingDownloader,
    #[error("failed to remove partial base image download {path}: {source}")]
    RemovePartial {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to run curl for {url}: {source}")]
    RunDownloader {
        url: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("curl failed for {url}: {stderr}")]
    DownloadFailed { url: &'static str, stderr: String },
    #[error("downloaded base image {path} is empty")]
    EmptyDownload { path: PathBuf },
    #[error("cached base image {path} is empty")]
    EmptyCachedImage { path: PathBuf },
    #[error("failed to read base image metadata {path}: {source}")]
    Metadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to move downloaded base image from {source_path} to {destination_path}: {source}")]
    MoveDownload {
        source_path: PathBuf,
        destination_path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[must_use]
pub fn resolve_image(image: CatalogImage) -> Option<QemuImage> {
    QEMU_IMAGES
        .iter()
        .find(|spec| spec.catalog == image)
        .map(|spec| spec.qemu)
}

#[must_use]
pub fn supports_image(image: CatalogImage) -> bool {
    resolve_image(image).is_some()
}

#[must_use]
pub fn plan_cache(cache_dir: &Path, source: QemuImage) -> ImageCachePlan {
    let source = SourceImage {
        url: source.url,
        cache_key: source.cache_key,
    };
    let cache_dir = cache_dir.to_path_buf();
    let image_path = cache_dir.join(source.cache_key);
    let download_path = cache_dir.join(format!("{}.part", source.cache_key));
    ImageCachePlan {
        source,
        cache_dir,
        image_path,
        download_path,
    }
}

async fn ensure_cache_directory(plan: &ImageCachePlan) -> Result<(), Error> {
    tokio::fs::create_dir_all(&plan.cache_dir)
        .await
        .map_err(|source| Error::CreateCacheDirectory {
            path: plan.cache_dir.clone(),
            source,
        })
}

/// Ensures the QEMU base image described by `plan` exists in the local cache.
///
/// # Errors
///
/// Returns an error if the image cache cannot be created, an existing cached
/// image is invalid, or the image download fails.
pub async fn ensure_cached(context: &Context, plan: &ImageCachePlan) -> Result<ImageCacheStatus, Error> {
    ensure_cache_directory(plan).await?;
    let lock = image_cache_lock(&plan.image_path);
    let _guard = lock.lock().await;

    if tokio::fs::try_exists(&plan.image_path)
        .await
        .map_err(|source| Error::Metadata {
            path: plan.image_path.clone(),
            source,
        })?
    {
        let metadata = tokio::fs::metadata(&plan.image_path)
            .await
            .map_err(|source| Error::Metadata {
                path: plan.image_path.clone(),
                source,
            })?;
        if metadata.len() == 0 {
            return Err(Error::EmptyCachedImage {
                path: plan.image_path.clone(),
            });
        }
        context
            .logger()
            .verbose_with(|| format!("base image already cached at {}", plan.image_path.display()));
        return Ok(ImageCacheStatus::AlreadyPresent);
    }

    if tokio::fs::try_exists(&plan.download_path)
        .await
        .map_err(|source| Error::RemovePartial {
            path: plan.download_path.clone(),
            source,
        })?
    {
        tokio::fs::remove_file(&plan.download_path)
            .await
            .map_err(|source| Error::RemovePartial {
                path: plan.download_path.clone(),
                source,
            })?;
    }

    let curl = platform::host::find_binary("curl")
        .await
        .ok_or(Error::MissingDownloader)?;
    context.logger().info(format!(
        "downloading base image {} to {}",
        plan.source.url,
        plan.image_path.display()
    ));
    let mut command = Command::new(curl);
    command
        .args(["--fail", "--location", "--show-error", "--silent", "--output"])
        .arg(&plan.download_path)
        .arg(plan.source.url);
    command.kill_on_drop(true);
    platform::command::hide_child_window(&mut command);
    let output = command.output().await.map_err(|source| Error::RunDownloader {
        url: plan.source.url,
        source,
    })?;
    if !output.status.success() {
        return Err(Error::DownloadFailed {
            url: plan.source.url,
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }

    let metadata = tokio::fs::metadata(&plan.download_path)
        .await
        .map_err(|source| Error::Metadata {
            path: plan.download_path.clone(),
            source,
        })?;
    if metadata.len() == 0 {
        return Err(Error::EmptyDownload {
            path: plan.download_path.clone(),
        });
    }

    tokio::fs::rename(&plan.download_path, &plan.image_path)
        .await
        .map_err(|source| Error::MoveDownload {
            source_path: plan.download_path.clone(),
            destination_path: plan.image_path.clone(),
            source,
        })?;
    Ok(ImageCacheStatus::Downloaded)
}

fn image_cache_lock(path: &Path) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>> = OnceLock::new();

    let mut locks = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

#[cfg(test)]
mod tests {
    use agentdp_core::manifest::GuestOs;
    use agentdp_core::provisioning::image::{ImageCatalog, ImageRequest};

    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use agentdp_core::Context;

    use crate::image::{ImageCacheStatus, ensure_cached, plan_cache, resolve_image, supports_image};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn maps_catalog_images_to_qcow2_sources() {
        for (os, expected_url, expected_cache_key) in [
            (
                GuestOs::Archlinux,
                "https://fastly.mirror.pkgbuild.com/images/latest/Arch-Linux-x86_64-cloudimg.qcow2",
                "archlinux-x86_64-cloudimg.qcow2",
            ),
            (
                GuestOs::Rocky9,
                "https://download.rockylinux.org/pub/rocky/9/images/x86_64/Rocky-9-GenericCloud-Base.latest.x86_64.qcow2",
                "rocky-9-genericcloud-base-latest-x86_64.qcow2",
            ),
        ] {
            let catalog_image = ImageCatalog::resolve(ImageRequest { os });
            assert!(supports_image(catalog_image));
            let qemu_image = resolve_image(catalog_image).expect("test image should be supported");

            assert_eq!(qemu_image.url, expected_url);
            assert_eq!(qemu_image.cache_key, expected_cache_key);
            assert_eq!(qemu_image.format, "qcow2");
        }
    }

    #[test]
    fn image_cache_lock_is_reused_for_same_path() {
        let first = super::image_cache_lock(Path::new("/tmp/agentdp/cache/images/rocky.qcow2"));
        let second = super::image_cache_lock(Path::new("/tmp/agentdp/cache/images/rocky.qcow2"));
        let other = super::image_cache_lock(Path::new("/tmp/agentdp/cache/images/arch.qcow2"));

        assert!(std::sync::Arc::ptr_eq(&first, &second));
        assert!(!std::sync::Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn plans_image_cache_paths_under_cache_dir() {
        let source = resolve_image(ImageCatalog::resolve(ImageRequest { os: GuestOs::Archlinux })).unwrap();
        let cache_dir = Path::new("/tmp/agentdp/cache/images");

        let plan = plan_cache(cache_dir, source);

        assert_eq!(plan.cache_dir, cache_dir);
        assert_eq!(plan.image_path, cache_dir.join("archlinux-x86_64-cloudimg.qcow2"));
        assert_eq!(
            plan.download_path,
            cache_dir.join("archlinux-x86_64-cloudimg.qcow2.part")
        );
    }

    #[tokio::test]
    async fn uses_existing_non_empty_cached_image() {
        let temp = TestTempDir::create("image-cache");
        let cache_dir = temp.path().join("cache/images");
        let source = resolve_image(ImageCatalog::resolve(ImageRequest { os: GuestOs::Archlinux })).unwrap();
        let plan = plan_cache(&cache_dir, source);
        fs::create_dir_all(&plan.cache_dir).unwrap();
        fs::write(&plan.image_path, b"cached image").unwrap();

        let status = ensure_cached(&Context::quiet(), &plan).await.unwrap();

        assert_eq!(status, ImageCacheStatus::AlreadyPresent);
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
