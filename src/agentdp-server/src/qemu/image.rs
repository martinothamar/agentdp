use agentdp_core::manifest::GuestOs;
use agentdp_core::platform::PlatformPaths;
use agentdp_core::provisioning::image::{CatalogImage, ImageArchitecture, ImageVariant};

use crate::backend::image_cache;

pub(super) const ARCHLINUX_X86_64_CLOUDIMG_URL: &str =
    "https://fastly.mirror.pkgbuild.com/images/latest/Arch-Linux-x86_64-cloudimg.qcow2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct QemuImage {
    pub(super) url: &'static str,
    pub(super) cache_key: &'static str,
    pub(super) format: &'static str,
}

pub(super) use image_cache::Plan as ImageCachePlan;
pub(super) use image_cache::Status as ImageCacheStatus;

pub(super) type Error = image_cache::Error;

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
    image_cache::plan(
        paths,
        image_cache::SourceImage {
            url: source.url,
            cache_key: source.cache_key,
        },
    )
}

pub(super) fn ensure_cached(context: &agentdp_core::Context, plan: &ImageCachePlan) -> Result<ImageCacheStatus, Error> {
    image_cache::ensure_cached(context, plan)
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
