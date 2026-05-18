use std::fs;
use std::path::PathBuf;
use std::process::Command;

use agentdp_core::Context;
use agentdp_core::manifest::GuestOs;
use agentdp_core::platform::{self, PlatformPaths};
use agentdp_core::provisioning::image::{CatalogImage, ImageArchitecture, ImageVariant};
use thiserror::Error;

pub(super) const ARCHLINUX_X86_64_CLOUDIMG_URL: &str =
    "https://fastly.mirror.pkgbuild.com/images/latest/Arch-Linux-x86_64-cloudimg.qcow2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QemuImage {
    pub(super) url: &'static str,
    pub(super) cache_key: &'static str,
    pub(super) format: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ImageCachePlan {
    pub(super) source: QemuImage,
    pub(super) cache_dir: PathBuf,
    pub(super) image_path: PathBuf,
    pub(super) download_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ImageCacheStatus {
    AlreadyPresent,
    Downloaded,
}

#[derive(Debug, Error)]
pub(super) enum Error {
    #[error("failed to create QEMU image cache directory {path}: {source}")]
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
pub(super) const fn resolve_image(image: CatalogImage) -> QemuImage {
    match (image.os, image.architecture, image.variant) {
        (GuestOs::Archlinux, ImageArchitecture::X86_64, ImageVariant::Cloud) => QemuImage {
            url: ARCHLINUX_X86_64_CLOUDIMG_URL,
            cache_key: "archlinux-x86_64-cloudimg.qcow2",
            format: "qcow2",
        },
    }
}

#[must_use]
pub(super) fn plan_cache(paths: &PlatformPaths, source: QemuImage) -> ImageCachePlan {
    let cache_dir = paths.cache.join("images");
    let image_path = cache_dir.join(source.cache_key);
    let download_path = cache_dir.join(format!("{}.part", source.cache_key));
    ImageCachePlan {
        source,
        cache_dir,
        image_path,
        download_path,
    }
}

pub(super) fn ensure_cache_directory(plan: &ImageCachePlan) -> Result<(), Error> {
    fs::create_dir_all(&plan.cache_dir).map_err(|source| Error::CreateCacheDirectory {
        path: plan.cache_dir.clone(),
        source,
    })
}

pub(super) fn ensure_cached(context: &Context, plan: &ImageCachePlan) -> Result<ImageCacheStatus, Error> {
    ensure_cache_directory(plan)?;
    if plan.image_path.exists() {
        let metadata = fs::metadata(&plan.image_path).map_err(|source| Error::Metadata {
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

    if plan.download_path.exists() {
        fs::remove_file(&plan.download_path).map_err(|source| Error::RemovePartial {
            path: plan.download_path.clone(),
            source,
        })?;
    }

    let curl = platform::find_binary("curl").ok_or(Error::MissingDownloader)?;
    context.logger().info(format!(
        "downloading base image {} to {}",
        plan.source.url,
        plan.image_path.display()
    ));
    let output = Command::new(curl)
        .args(["--fail", "--location", "--show-error", "--silent", "--output"])
        .arg(&plan.download_path)
        .arg(plan.source.url)
        .output()
        .map_err(|source| Error::RunDownloader {
            url: plan.source.url,
            source,
        })?;
    if !output.status.success() {
        return Err(Error::DownloadFailed {
            url: plan.source.url,
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }

    let metadata = fs::metadata(&plan.download_path).map_err(|source| Error::Metadata {
        path: plan.download_path.clone(),
        source,
    })?;
    if metadata.len() == 0 {
        return Err(Error::EmptyDownload {
            path: plan.download_path.clone(),
        });
    }

    fs::rename(&plan.download_path, &plan.image_path).map_err(|source| Error::MoveDownload {
        source_path: plan.download_path.clone(),
        destination_path: plan.image_path.clone(),
        source,
    })?;
    Ok(ImageCacheStatus::Downloaded)
}

#[cfg(test)]
mod tests {
    use agentdp_core::manifest::GuestOs;
    use agentdp_core::platform::PlatformPaths;
    use agentdp_core::provisioning::image::{ImageCatalog, ImageRequest};

    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use agentdp_core::Context;

    use crate::qemu::image::{
        ARCHLINUX_X86_64_CLOUDIMG_URL, ImageCacheStatus, ensure_cached, plan_cache, resolve_image,
    };

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn maps_archlinux_catalog_image_to_qcow2_source() {
        let catalog_image = ImageCatalog::resolve(ImageRequest { os: GuestOs::Archlinux });
        let qemu_image = resolve_image(catalog_image);

        assert_eq!(qemu_image.url, ARCHLINUX_X86_64_CLOUDIMG_URL);
        assert_eq!(qemu_image.cache_key, "archlinux-x86_64-cloudimg.qcow2");
        assert_eq!(qemu_image.format, "qcow2");
    }

    #[test]
    fn plans_image_cache_paths_under_platform_cache() {
        let source = resolve_image(ImageCatalog::resolve(ImageRequest { os: GuestOs::Archlinux }));
        let paths = PlatformPaths {
            data: "/tmp/agentdp-data".into(),
            config: "/tmp/agentdp-config".into(),
            cache: "/tmp/agentdp-cache".into(),
            runtime: "/tmp/agentdp-run".into(),
            logs: "/tmp/agentdp-logs".into(),
        };

        let plan = plan_cache(&paths, source);

        assert_eq!(plan.cache_dir, paths.cache.join("images"));
        assert_eq!(
            plan.image_path,
            paths.cache.join("images/archlinux-x86_64-cloudimg.qcow2")
        );
        assert_eq!(
            plan.download_path,
            paths.cache.join("images/archlinux-x86_64-cloudimg.qcow2.part")
        );
    }

    #[test]
    fn uses_existing_non_empty_cached_image() {
        let temp = TestTempDir::create("image-cache");
        let paths = PlatformPaths {
            data: temp.path().join("data"),
            config: temp.path().join("config"),
            cache: temp.path().join("cache"),
            runtime: temp.path().join("runtime"),
            logs: temp.path().join("logs"),
        };
        let source = resolve_image(ImageCatalog::resolve(ImageRequest { os: GuestOs::Archlinux }));
        let plan = plan_cache(&paths, source);
        fs::create_dir_all(&plan.cache_dir).unwrap();
        fs::write(&plan.image_path, b"cached image").unwrap();

        let status = ensure_cached(&Context::quiet(), &plan).unwrap();

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
