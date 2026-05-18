use std::path::{Path, PathBuf};

use agentdp_core::Context;
use agentdp_core::manifest::AgentManifest;
use agentdp_core::platform::PlatformPaths;
use agentdp_core::platform::ssh::SshKeygen;
use agentdp_core::provisioning::{self as core_provisioning, ProvisioningOptions, ProvisioningPlan};
use agentdp_protocol::{
    GuestAccessResult, ProvisioningImageResult, QemuImageResult, QemuProvisioningResult, SeedResult,
};
use thiserror::Error;

use crate::backend::{host_seed, seed, ssh};

use super::image;

#[derive(Debug, Error)]
pub(super) enum Error {
    #[error("{0}")]
    Provisioning(#[from] core_provisioning::Error),
    #[error("{0}")]
    Ssh(#[from] ssh::Error),
    #[error("{0}")]
    Seed(#[from] seed::Error),
    #[error("{0}")]
    Image(#[from] image::Error),
    #[error("{0}")]
    HostSeed(#[from] host_seed::Error),
}

pub(super) fn plan(
    context: &Context,
    manifest_path: PathBuf,
    manifest: AgentManifest,
    instance: String,
    paths: &PlatformPaths,
) -> Result<PlanOutput, Error> {
    Ok(prepare_manifest(context, manifest_path, manifest, instance, paths)?.to_output())
}

fn prepare_manifest(
    context: &Context,
    manifest_path: PathBuf,
    manifest: AgentManifest,
    instance: String,
    paths: &PlatformPaths,
) -> Result<PreparedProvisioning, Error> {
    let ssh_keygen = SshKeygen::resolve().map_err(ssh::Error::from)?;
    prepare_manifest_with_keygen(context, manifest_path, manifest, instance, paths, &ssh_keygen)
}

pub(super) fn prepare_manifest_with_keygen(
    context: &Context,
    manifest_path: PathBuf,
    manifest: AgentManifest,
    instance: String,
    paths: &PlatformPaths,
    ssh_keygen: &SshKeygen,
) -> Result<PreparedProvisioning, Error> {
    context.logger().verbose_with(|| {
        format!(
            "building QEMU provisioning plan from manifest {} for instance {instance}",
            manifest_path.display()
        )
    });
    let work_dir = instance_work_dir(paths, &manifest.name, &instance);
    let guest_access = ssh::generate_guest_access(context, &work_dir, ssh_keygen, &manifest.user.name)?;
    let seed_files = host_seed::collect(context, &manifest_path, &manifest)?;
    let provisioning_plan = ProvisioningPlan::from_manifest_with_options(
        &manifest,
        &ProvisioningOptions {
            ssh_authorized_key: Some(guest_access.public_key_contents.clone()),
            seed_files,
        },
    )?;
    let qemu_image = image::resolve_image(provisioning_plan.image);
    let image_cache = image::plan_cache(paths, qemu_image);
    let seed = seed::write_seed_artifacts(&work_dir, &provisioning_plan)?;

    Ok(PreparedProvisioning {
        manifest_path,
        manifest,
        instance,
        provisioning_plan,
        qemu_image,
        image_cache,
        guest_access,
        seed,
    })
}

#[derive(Debug)]
pub(super) struct PreparedProvisioning {
    pub(super) manifest_path: PathBuf,
    pub(super) manifest: AgentManifest,
    pub(super) instance: String,
    pub(super) provisioning_plan: ProvisioningPlan,
    pub(super) qemu_image: image::QemuImage,
    pub(super) image_cache: image::ImageCachePlan,
    pub(super) guest_access: ssh::GuestAccess,
    pub(super) seed: seed::SeedArtifacts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlanOutput {
    pub(super) manifest: String,
    pub(super) name: String,
    pub(super) instance: String,
    pub(super) image: ProvisioningImageResult,
    pub(super) qemu: QemuProvisioningResult,
    pub(super) work_dir: String,
    pub(super) seed: SeedResult,
    pub(super) guest_access: GuestAccessResult,
}

impl PreparedProvisioning {
    #[must_use]
    fn to_output(&self) -> PlanOutput {
        PlanOutput {
            manifest: path_text(&self.manifest_path),
            name: self.manifest.name.clone(),
            instance: self.instance.clone(),
            image: ProvisioningImageResult {
                os: self.provisioning_plan.image.os_name().to_owned(),
                architecture: self.provisioning_plan.image.architecture_name().to_owned(),
                variant: self.provisioning_plan.image.variant_name().to_owned(),
            },
            qemu: QemuProvisioningResult {
                image: QemuImageResult {
                    url: self.qemu_image.url.to_owned(),
                    cache_key: self.qemu_image.cache_key.to_owned(),
                    format: self.qemu_image.format.to_owned(),
                    cache_path: path_text(&self.image_cache.image_path),
                    download_path: path_text(&self.image_cache.download_path),
                },
            },
            work_dir: path_text(&self.seed.work_dir),
            seed: SeedResult {
                directory: path_text(&self.seed.seed_dir),
                meta_data: path_text(&self.seed.meta_data),
                user_data: path_text(&self.seed.user_data),
                bootstrap_script: path_text(&self.seed.bootstrap_script),
                media: path_text(&self.seed.seed_media),
            },
            guest_access: GuestAccessResult {
                ssh_user: Some(self.guest_access.user.clone()),
                ssh_private_key: Some(path_text(&self.guest_access.private_key)),
                ssh_public_key: Some(path_text(&self.guest_access.public_key)),
            },
        }
    }
}

fn instance_work_dir(paths: &PlatformPaths, manifest_name: &str, instance: &str) -> PathBuf {
    paths
        .data
        .join("instances")
        .join(manifest_name)
        .join(instance)
        .join("generated")
        .join("qemu")
}

fn path_text(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use agentdp_core::Context;
    use agentdp_core::manifest::AgentManifest;
    use agentdp_core::platform::PlatformPaths;
    use agentdp_test_support::manifest;

    use crate::qemu::image::ARCHLINUX_X86_64_CLOUDIMG_URL;
    use agentdp_core::platform::ssh::SshKeygen;

    use crate::qemu::provisioning::prepare_manifest_with_keygen;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn provisioning_plan_writes_qemu_seed_preview() {
        let temp = TestTempDir::create("server-provisioning-plan");
        let manifest = temp.write("agent.yaml", manifest::standard());
        let paths = temp.platform_paths();
        let parsed_manifest = serde_yaml::from_str::<AgentManifest>(&fs::read_to_string(&manifest).unwrap()).unwrap();
        let ssh_keygen = SshKeygen::new(temp.write_fake_ssh_keygen());

        let prepared = prepare_manifest_with_keygen(
            &Context::quiet(),
            manifest,
            parsed_manifest,
            "pr-0".to_owned(),
            &paths,
            &ssh_keygen,
        )
        .unwrap();
        let result = prepared.to_output();

        assert_eq!(result.name, "altinn-studio");
        assert_eq!(result.instance, "pr-0");
        assert_eq!(result.image.os, "archlinux");
        assert_eq!(result.qemu.image.url, ARCHLINUX_X86_64_CLOUDIMG_URL);

        let meta_data = PathBuf::from(result.seed.meta_data);
        let user_data = PathBuf::from(result.seed.user_data);
        let bootstrap_script = PathBuf::from(result.seed.bootstrap_script);

        assert_eq!(
            fs::read_to_string(meta_data).unwrap(),
            "instance-id: altinn-studio\nlocal-hostname: altinn-studio\n"
        );
        let user_data = fs::read_to_string(user_data).unwrap();
        assert!(user_data.starts_with("#cloud-config\n"));
        assert!(user_data.contains("ssh_authorized_keys:"));
        assert!(user_data.contains("ssh-ed25519 AAAATEST agentdp"));
        assert!(fs::read_to_string(bootstrap_script).unwrap().contains("git clone"));
        assert_eq!(result.guest_access.ssh_user.as_deref(), Some("agent"));
        let private_key = result.guest_access.ssh_private_key.unwrap();
        assert!(PathBuf::from(private_key).is_file());
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

        fn write(&self, name: &str, contents: &str) -> PathBuf {
            let path = self.path.join(name);
            fs::write(&path, contents).unwrap();
            path
        }

        fn write_fake_tool(&self, name: &str, contents: &str) -> PathBuf {
            let executable = executable_script_name(name);
            let tool = self.write(&executable, contents);
            agentdp_core::platform::set_executable(&tool).unwrap();
            tool
        }

        fn platform_paths(&self) -> PlatformPaths {
            PlatformPaths {
                data: self.path.join("data"),
                config: self.path.join("config"),
                cache: self.path.join("cache"),
                runtime: self.path.join("runtime"),
                logs: self.path.join("logs"),
            }
        }

        fn write_fake_ssh_keygen(&self) -> PathBuf {
            self.write_fake_tool("ssh-keygen", fake_ssh_keygen_script())
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _result = fs::remove_dir_all(&self.path);
        }
    }

    #[cfg(unix)]
    const fn fake_ssh_keygen_script() -> &'static str {
        "#!/bin/sh\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"-f\" ]; then\n    shift\n    printf 'private key\\n' > \"$1\"\n    printf 'ssh-ed25519 AAAATEST agentdp\\n' > \"$1.pub\"\n    exit 0\n  fi\n  shift\ndone\nexit 1\n"
    }

    #[cfg(windows)]
    const fn fake_ssh_keygen_script() -> &'static str {
        "@echo off\r\necho private key> \"%~8\"\r\necho ssh-ed25519 AAAATEST agentdp> \"%~8.pub\"\r\nexit /b 0\r\n"
    }

    #[cfg(windows)]
    fn executable_script_name(name: &str) -> String {
        format!("{name}.cmd")
    }

    #[cfg(unix)]
    fn executable_script_name(name: &str) -> String {
        name.to_owned()
    }
}
