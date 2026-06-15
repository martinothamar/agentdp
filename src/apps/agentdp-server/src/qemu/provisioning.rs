use std::path::{Path, PathBuf};

use agentdp_core::Context;
use agentdp_core::manifest::{AgentManifest, NetworkMode};
use agentdp_core::mediated_network;
use agentdp_core::provisioning::bootstrap::RenderedBootstrapPlan;
use agentdp_core::provisioning::cloud_init::{self, CloudInitSeed};
use agentdp_core::provisioning::guest_os::linux::cloud_init::CloudInitOptions;
use agentdp_core::provisioning::guest_os::{GuestOsAdapter, system_guestd_service_seed_for_os};
use agentdp_core::provisioning::{ProvisioningPlan, SeedFile};
use agentdp_ds::SecretString;
use agentdp_platform::ssh::SshKeygen;
use agentdp_protocol::server_guest as guest_protocol;
use agentdp_qemu::{image, seed};
use serde::Serialize;
use thiserror::Error;

use super::MediatedCaState;
use crate::agent::AgentManifestContext;
use crate::host::{
    GuestAccess, HostSeedError, HostSshError, collect_guest_tool_seeds, collect_host_seed, generate_guest_access,
};

#[derive(Debug, Error)]
pub(super) enum Error {
    #[error("{0}")]
    CloudInit(#[from] cloud_init::Error),
    #[error("{0}")]
    Ssh(#[from] HostSshError),
    #[error("{0}")]
    Seed(#[from] seed::Error),
    #[error("{0}")]
    Image(#[from] image::Error),
    #[error("{0}")]
    HostSeed(#[from] HostSeedError),
    #[error("failed to read manifest seed source {path}: {source}")]
    ReadManifestSeed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("manifest path has no parent directory: {0}")]
    MissingManifestParent(PathBuf),
    #[error("failed to read CA bundle seed source {path}: {source}")]
    ReadCaSeed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize guest bootstrap plan seed: {0}")]
    SerializeBootstrapPlanSeed(#[source] serde_json::Error),
    #[error("failed to serialize guest instance spec seed: {0}")]
    SerializeInstanceSpecSeed(#[source] serde_json::Error),
    #[error("{0}")]
    CertificateAuthority(#[from] agentdp_crypto::CertificateAuthorityError),
    #[error("failed to write mediated CA private key {path}: {source}")]
    WriteMediatedCaKey {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("QEMU does not support image {os} {architecture} {variant}")]
    UnsupportedImage {
        os: &'static str,
        architecture: &'static str,
        variant: &'static str,
    },
}

pub(super) struct PrepareCreateInput<'a> {
    pub(super) manifest: AgentManifestContext,
    pub(super) instance: String,
    pub(super) provisioning_plan: &'a ProvisioningPlan,
    pub(super) rendered_bootstrap: &'a RenderedBootstrapPlan,
    pub(super) image_cache_dir: &'a Path,
    pub(super) work_dir: &'a Path,
}

pub(super) async fn prepare_create_with_keygen(
    context: &Context,
    input: PrepareCreateInput<'_>,
    ssh_keygen: &SshKeygen,
) -> Result<PreparedProvisioning, Error> {
    let PrepareCreateInput {
        manifest,
        instance,
        provisioning_plan,
        rendered_bootstrap,
        image_cache_dir,
        work_dir,
    } = input;
    let manifest_path = manifest.source_path().to_path_buf();
    let manifest = manifest.value().clone();
    context.logger().verbose_with(|| {
        format!(
            "preparing QEMU seed artifacts from manifest {} for instance {instance}",
            manifest_path.display()
        )
    });
    let guest_access = generate_guest_access(context, work_dir, ssh_keygen, &manifest.spec.user.name).await?;
    let host_seed = collect_host_seed(context, &manifest_path, &manifest).await?;
    let mut seed_files = host_seed.files;
    let guest_layout = GuestOsAdapter::for_os(manifest.spec.image.os).capabilities().layout;
    let mediated_ipv6 = manifest
        .spec
        .network
        .ipv6
        .enabled_for_host(agentdp_platform::net::has_ipv6_egress().await);
    let mediated_ca = if manifest
        .spec
        .network
        .ca
        .generates_mediated_ca(manifest.spec.network.mode)
    {
        let ca = agentdp_crypto::CertificateAuthorityPem::generate()?;
        let key_path = mediated_ca_key_path(work_dir);
        write_mediated_ca_key(&key_path, &SecretString::new(ca.key_pem)).await?;
        let mediated_ca = MediatedCaState::new(ca.cert_pem, key_path.display().to_string());
        seed_files.push(generated_mediated_ca_seed_file(&mediated_ca, guest_layout.ca_bundle));
        mediated_ca
    } else {
        MediatedCaState::default()
    };
    let bootstrap_control_seed = bootstrap_control_seed_files(
        &manifest_path,
        &manifest,
        &instance,
        provisioning_plan,
        rendered_bootstrap,
    )
    .await?;
    seed_files.extend(bootstrap_control_seed.files);
    let cloud_init = CloudInitSeed::from_plan(
        &manifest,
        provisioning_plan,
        &CloudInitOptions {
            ssh_authorized_key: Some(guest_access.public_key_contents.clone()),
            seed_files,
            run_commands: vec![ENABLE_GUESTD_SYSTEM_SERVICE_COMMAND.to_owned()],
            network_mode: None,
            mediated_network: mediated_network::DEFAULT_PROFILE,
            mediated_ipv6,
        },
    )?;
    let (qemu_image, image_cache) = resolve_image_cache(provisioning_plan, image_cache_dir)?;
    let seed = seed::write_seed_artifacts_with_extra_files(
        work_dir,
        &cloud_init,
        &[seed::SeedExtraFile {
            name: GUEST_INSTANCE_SPEC_SEED_FILE,
            short_name: GUEST_INSTANCE_SPEC_SEED_SHORT_NAME,
            contents: bootstrap_control_seed.instance_spec_json.into_bytes(),
        }],
    )
    .await?;

    Ok(PreparedProvisioning {
        manifest,
        provisioning_plan: provisioning_plan.clone(),
        qemu_image,
        image_cache,
        guest_access,
        mediated_secrets: host_seed.secrets,
        mediated_ca,
        seed,
    })
}

pub(super) async fn prepare_base(
    context: &Context,
    input: PrepareCreateInput<'_>,
) -> Result<PreparedBaseProvisioning, Error> {
    let PrepareCreateInput {
        manifest,
        instance,
        provisioning_plan,
        rendered_bootstrap,
        image_cache_dir,
        work_dir,
    } = input;
    let manifest_path = manifest.source_path().to_path_buf();
    let manifest = manifest.value().clone();
    context.logger().verbose_with(|| {
        format!(
            "preparing QEMU seed artifacts from manifest {} for agent base {instance}",
            manifest_path.display()
        )
    });
    let mut seed_files = collect_guest_tool_seeds(context, &manifest).await?;
    let guest_layout = GuestOsAdapter::for_os(manifest.spec.image.os).capabilities().layout;
    add_base_ca_seed(&manifest_path, &manifest, guest_layout.ca_bundle, &mut seed_files).await?;
    let bootstrap_control_seed = bootstrap_control_seed_files(
        &manifest_path,
        &manifest,
        &instance,
        provisioning_plan,
        rendered_bootstrap,
    )
    .await?;
    seed_files.extend(bootstrap_control_seed.files);
    let cloud_init = CloudInitSeed::from_plan(
        &manifest,
        provisioning_plan,
        &CloudInitOptions {
            ssh_authorized_key: None,
            seed_files,
            run_commands: vec![START_GUESTD_SYSTEM_SERVICE_COMMAND.to_owned()],
            network_mode: Some(NetworkMode::User),
            mediated_network: mediated_network::DEFAULT_PROFILE,
            mediated_ipv6: false,
        },
    )?;
    let (qemu_image, image_cache) = resolve_image_cache(provisioning_plan, image_cache_dir)?;
    let seed = seed::write_seed_artifacts_with_extra_files(
        work_dir,
        &cloud_init,
        &[seed::SeedExtraFile {
            name: GUEST_INSTANCE_SPEC_SEED_FILE,
            short_name: GUEST_INSTANCE_SPEC_SEED_SHORT_NAME,
            contents: bootstrap_control_seed.instance_spec_json.into_bytes(),
        }],
    )
    .await?;

    Ok(PreparedBaseProvisioning {
        provisioning_plan: provisioning_plan.clone(),
        qemu_image,
        image_cache,
        seed,
    })
}

#[derive(Debug)]
pub(super) struct PreparedProvisioning {
    pub(super) manifest: AgentManifest,
    pub(super) provisioning_plan: ProvisioningPlan,
    pub(super) qemu_image: image::QemuImage,
    pub(super) image_cache: image::ImageCachePlan,
    pub(super) guest_access: GuestAccess,
    pub(super) mediated_secrets: agentdp_core::provisioning::secrets::SecretBindings,
    pub(super) mediated_ca: MediatedCaState,
    pub(super) seed: seed::SeedArtifacts,
}

#[derive(Debug)]
pub(super) struct PreparedBaseProvisioning {
    pub(super) provisioning_plan: ProvisioningPlan,
    pub(super) qemu_image: image::QemuImage,
    pub(super) image_cache: image::ImageCachePlan,
    pub(super) seed: seed::SeedArtifacts,
}

fn resolve_image_cache(
    provisioning_plan: &ProvisioningPlan,
    image_cache_dir: &Path,
) -> Result<(image::QemuImage, image::ImageCachePlan), Error> {
    let qemu_image = image::resolve_image(provisioning_plan.image).ok_or_else(|| Error::UnsupportedImage {
        os: provisioning_plan.image.os_name(),
        architecture: provisioning_plan.image.architecture_name(),
        variant: provisioning_plan.image.variant_name(),
    })?;
    Ok((qemu_image, image::plan_cache(image_cache_dir, qemu_image)))
}

fn generated_mediated_ca_seed_file(ca: &MediatedCaState, guest_path: &str) -> SeedFile {
    SeedFile {
        path: guest_path.to_owned(),
        contents: ca.cert_pem.as_bytes().to_vec(),
        permissions: "0644".to_owned(),
        owner: Some("root:root".to_owned()),
    }
}

async fn add_base_ca_seed(
    manifest_path: &Path,
    manifest: &AgentManifest,
    guest_path: &str,
    seed_files: &mut Vec<SeedFile>,
) -> Result<(), Error> {
    let Some(source) = manifest.spec.network.ca.source_path() else {
        return Ok(());
    };
    let manifest_dir = manifest_path
        .parent()
        .ok_or_else(|| Error::MissingManifestParent(manifest_path.to_path_buf()))?;
    let path = manifest_dir.join(source);
    let contents = tokio::fs::read(&path).await.map_err(|source| Error::ReadCaSeed {
        path: path.clone(),
        source,
    })?;
    seed_files.push(SeedFile {
        path: guest_path.to_owned(),
        contents,
        permissions: "0644".to_owned(),
        owner: Some("root:root".to_owned()),
    });
    Ok(())
}

pub(super) fn mediated_ca_key_path(instance_dir: &Path) -> PathBuf {
    instance_dir.join("mediated-ca").join("key.pem")
}

async fn write_mediated_ca_key(path: &Path, key: &SecretString) -> Result<(), Error> {
    agentdp_platform::fs::write_atomic(path, key.expose_secret().as_bytes(), 0o600)
        .await
        .map_err(|source| Error::WriteMediatedCaKey {
            path: path.to_path_buf(),
            source,
        })
}

pub(super) const GUEST_INSTANCE_SPEC_SEED_FILE: &str = "agentdp-instance.json";
pub(super) const GUEST_INSTANCE_SPEC_SEED_SHORT_NAME: [u8; 11] = *b"AGENTD~1JSO";
const ENABLE_GUESTD_SYSTEM_SERVICE_COMMAND: &str =
    "systemctl daemon-reload && systemctl enable --now guestd-system.service";
const START_GUESTD_SYSTEM_SERVICE_COMMAND: &str = "systemctl daemon-reload && systemctl start guestd-system.service";

async fn bootstrap_control_seed_files(
    manifest_path: &Path,
    manifest: &AgentManifest,
    instance: &str,
    provisioning_plan: &ProvisioningPlan,
    rendered_bootstrap: &RenderedBootstrapPlan,
) -> Result<BootstrapControlSeed, Error> {
    let manifest_contents = tokio::fs::read(manifest_path)
        .await
        .map_err(|source| Error::ReadManifestSeed {
            path: manifest_path.to_path_buf(),
            source,
        })?;
    let bootstrap_plan = guest_bootstrap_plan(&manifest.spec.user.name, provisioning_plan, rendered_bootstrap);
    let instance_spec = guest_instance_spec(
        manifest.name(),
        instance,
        &provisioning_plan.hostname,
        &manifest.spec.user.name,
        provisioning_plan,
    );

    let instance_spec_json_text =
        guest_instance_spec_seed_text(&instance_spec).map_err(Error::SerializeInstanceSpecSeed)?;

    let mut files = vec![
        root_seed_file(&instance_spec.paths.manifest, manifest_contents, "0644"),
        root_seed_file(
            &instance_spec.paths.bootstrap_plan,
            json_seed_contents(&bootstrap_plan).map_err(Error::SerializeBootstrapPlanSeed)?,
            "0644",
        ),
        root_seed_file(
            &instance_spec.paths.instance_spec,
            instance_spec_json_text.clone().into_bytes(),
            "0644",
        ),
        system_guestd_service_seed_for_os(provisioning_plan.image.os, &instance_spec.paths.instance_spec),
    ];
    files.extend(rendered_bootstrap.steps.iter().map(|step| {
        let permissions = match step.phase {
            guest_protocol::BootstrapStepPhase::System => "0700",
            guest_protocol::BootstrapStepPhase::User => "0755",
        };
        root_seed_file(
            &format!("{}/{}", instance_spec.paths.bootstrap_root, step.script),
            step.contents.as_bytes().to_vec(),
            permissions,
        )
    }));
    Ok(BootstrapControlSeed {
        files,
        instance_spec_json: instance_spec_json_text,
    })
}

struct BootstrapControlSeed {
    files: Vec<SeedFile>,
    instance_spec_json: String,
}

fn guest_bootstrap_plan(
    user: &str,
    provisioning_plan: &ProvisioningPlan,
    bootstrap: &RenderedBootstrapPlan,
) -> guest_protocol::BootstrapPlan {
    let layout = GuestOsAdapter::for_os(provisioning_plan.image.os).capabilities().layout;
    guest_protocol::BootstrapPlan {
        plan_version: guest_protocol::GUEST_CONTROL_PROTOCOL_VERSION,
        user: user.to_owned(),
        home: layout.agent_home.to_owned(),
        code_dir: layout.code_dir.to_owned(),
        steps: bootstrap.steps.iter().map(|step| step.spec.clone()).collect(),
    }
}

fn guest_instance_spec(
    agent: &str,
    instance: &str,
    hostname: &str,
    user: &str,
    provisioning_plan: &ProvisioningPlan,
) -> guest_protocol::GuestInstanceSpec {
    let capabilities = GuestOsAdapter::for_os(provisioning_plan.image.os).capabilities();
    let layout = capabilities.layout;
    guest_protocol::GuestInstanceSpec {
        schema_version: guest_protocol::GUEST_INSTANCE_SPEC_VERSION,
        manifest: agent.to_owned(),
        instance: instance.to_owned(),
        hostname: hostname.to_owned(),
        platform: capabilities.platform,
        user: guest_protocol::GuestInstanceUser {
            name: user.to_owned(),
            home: layout.agent_home.to_owned(),
            code_dir: layout.code_dir.to_owned(),
        },
        paths: provisioning_plan.guest_paths.clone(),
    }
}

pub(super) fn guest_instance_spec_seed_text(
    instance_spec: &guest_protocol::GuestInstanceSpec,
) -> Result<String, serde_json::Error> {
    json_seed_text(instance_spec)
}

fn json_seed_text(value: &impl Serialize) -> Result<String, serde_json::Error> {
    let mut contents = serde_json::to_string_pretty(value)?;
    contents.push('\n');
    Ok(contents)
}

fn json_seed_contents(value: &impl Serialize) -> Result<Vec<u8>, serde_json::Error> {
    Ok(json_seed_text(value)?.into_bytes())
}

fn root_seed_file(path: &str, contents: Vec<u8>, permissions: &str) -> SeedFile {
    SeedFile {
        path: path.to_owned(),
        contents,
        permissions: permissions.to_owned(),
        owner: Some("root:root".to_owned()),
    }
}

#[cfg(test)]
fn path_text(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use super::path_text;

    use agentdp_platform::time;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use agentdp_core::Context;
    use agentdp_core::manifest::{AgentManifest, GuestOs};
    use agentdp_core::provisioning::bootstrap::RenderedBootstrapStep;
    use agentdp_core::provisioning::guest_os::system_guestd_service_seed_for_os;
    use agentdp_core::provisioning::{ProvisioningOptions, ProvisioningPlan};
    use agentdp_test_support::manifest;
    use agentdp_test_support::snapshot;

    use agentdp_platform::ssh::SshKeygen;
    use serde::Serialize;

    use crate::agent::AgentManifestContext;
    use crate::agent::{AgentBaseKey, AgentInstanceId, AgentName, AgentdpLayout};

    use super::{
        PrepareCreateInput, PreparedBaseProvisioning, PreparedProvisioning, prepare_base, prepare_create_with_keygen,
    };

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn create_preparation_writes_seed_artifacts() {
        let temp = TestTempDir::create("server-create-seed");
        let standard_manifest = manifest::standard().replace(
            "  codex:\n    yolo: true\n    auth: mediated\n",
            "  codex:\n    yolo: true\n    auth: mediated\n    auth_source: env\n",
        );
        let manifest = temp.write("agent.yaml", &standard_manifest);
        let parsed_manifest = serde_yaml::from_str::<AgentManifest>(&fs::read_to_string(&manifest).unwrap()).unwrap();
        let prepared = prepare_create_for_test(&temp, manifest, parsed_manifest, "pr-0").await;

        assert!(prepared.guest_access.private_key.is_file());
        assert_qemu_provisioning_snapshot("writes_seed_artifacts", &temp, &prepared);
    }

    #[tokio::test]
    async fn user_network_provisioning_does_not_seed_generated_mediated_ca() {
        let temp = TestTempDir::create("server-user-network-provisioning");
        let manifest = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: user-network
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: user
      ports:
        ssh:
          guest: 22
          host: 4022
          protocol: tcp
    bootstrap: {}
    plugins:
      codex:
        auth: mediated
        auth_source: env
      github:
        auth: mediated
    secrets: []
",
        );
        let parsed_manifest = serde_yaml::from_str::<AgentManifest>(&fs::read_to_string(&manifest).unwrap()).unwrap();
        let prepared = prepare_create_for_test(&temp, manifest, parsed_manifest, "dev-0").await;
        assert_qemu_provisioning_snapshot("user_network_does_not_seed_generated_mediated_ca", &temp, &prepared);
    }

    #[tokio::test]
    async fn base_preparation_seeds_guest_tools_before_system_bootstrap() {
        let temp = TestTempDir::create("server-base-seed-tools");
        let manifest_path = temp.write("agent.yaml", manifest::minimal());
        let parsed_manifest =
            serde_yaml::from_str::<AgentManifest>(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
        let prepared = prepare_base_for_test(&temp, manifest_path, parsed_manifest).await;
        let user_data = fs::read_to_string(&prepared.seed.user_data).unwrap();
        let network_config = fs::read_to_string(&prepared.seed.network_config).unwrap();

        assert!(user_data.contains("- path: /usr/local/bin/guestd"));
        assert!(user_data.contains("- path: /run/agentdp/bin/guestctl.gz"));
        assert!(!user_data.contains("- path: /var/lib/agentdp/ca/ca-bundle.pem"));
        assert!(user_data.contains("systemctl daemon-reload && systemctl start guestd-system.service"));
        assert!(!user_data.contains("systemctl enable --now guestd-system.service"));
        assert!(network_config.contains("type: dhcp"));
        assert!(!network_config.contains("10.73.0.1"));
    }

    async fn prepare_create_for_test(
        temp: &TestTempDir,
        manifest_path: PathBuf,
        manifest: AgentManifest,
        instance: &str,
    ) -> PreparedProvisioning {
        let agentdp_layout = AgentdpLayout::from_root(temp.path.join("agentdp"));
        let files = agentdp_layout
            .instance(&AgentName::new(manifest.name()), AgentInstanceId::new(0))
            .files();
        let provisioning_plan = ProvisioningPlan::from_manifest(
            &manifest,
            &ProvisioningOptions {
                hostname: Some(instance.to_owned()),
            },
        );
        let rendered_bootstrap = provisioning_plan
            .render_instance_bootstrap(&manifest)
            .expect("render instance bootstrap");
        let ssh_keygen = SshKeygen::new(temp.write_fake_ssh_keygen());
        let image_cache_dir = agentdp_layout.image_cache_dir();
        let manifest_context = AgentManifestContext::load(&Context::quiet(), &agentdp_layout, &manifest_path)
            .await
            .expect("manifest context");

        prepare_create_with_keygen(
            &Context::quiet(),
            PrepareCreateInput {
                manifest: manifest_context,
                instance: instance.to_owned(),
                provisioning_plan: &provisioning_plan,
                rendered_bootstrap: &rendered_bootstrap,
                image_cache_dir: &image_cache_dir,
                work_dir: &files.instance_dir,
            },
            &ssh_keygen,
        )
        .await
        .unwrap()
    }

    async fn prepare_base_for_test(
        temp: &TestTempDir,
        manifest_path: PathBuf,
        manifest: AgentManifest,
    ) -> PreparedBaseProvisioning {
        let agentdp_layout = AgentdpLayout::from_root(temp.path.join("agentdp"));
        let files = agentdp_layout
            .agent_base(&AgentName::new(manifest.name()), &AgentBaseKey::new("sha256-test"))
            .files();
        let provisioning_plan = ProvisioningPlan::from_manifest(
            &manifest,
            &ProvisioningOptions {
                hostname: Some(format!("{}-base", manifest.name())),
            },
        );
        let rendered_bootstrap = provisioning_plan
            .render_base_bootstrap(&manifest)
            .expect("render base bootstrap");
        let image_cache_dir = agentdp_layout.image_cache_dir();
        let manifest_context = AgentManifestContext::load(&Context::quiet(), &agentdp_layout, &manifest_path)
            .await
            .expect("manifest context");

        prepare_base(
            &Context::quiet(),
            PrepareCreateInput {
                manifest: manifest_context,
                instance: "agent-base".to_owned(),
                provisioning_plan: &provisioning_plan,
                rendered_bootstrap: &rendered_bootstrap,
                image_cache_dir: &image_cache_dir,
                work_dir: &files.base_dir,
            },
        )
        .await
        .unwrap()
    }

    #[test]
    fn guestd_system_service_starts_system_lifecycle_from_seeded_inputs() {
        let instance_spec_path = "/var/lib/agentdp/spec/instance.json";
        let service_seed = system_guestd_service_seed_for_os(GuestOs::Archlinux, instance_spec_path);
        let service = String::from_utf8(service_seed.contents).unwrap();

        assert_eq!(service_seed.path, "/etc/systemd/system/guestd-system.service");
        assert_eq!(service_seed.permissions, "0644");
        assert_eq!(service_seed.owner.as_deref(), Some("root:root"));
        assert!(service.contains(&format!(
            "ExecStart=/usr/local/bin/guestd system --instance-spec {instance_spec_path}"
        )));
        assert!(!service.contains("ExecStartPre"));
        assert!(!service.contains("cloud-init.service"));
    }

    #[derive(Serialize)]
    struct QemuProvisioningSnapshot<'a> {
        image: QemuProvisioningImageSnapshot<'a>,
        work_dir: String,
        seed_dir: String,
        seed_meta_data: String,
        seed_network_config: String,
        seed_user_data: String,
        bootstrap_steps: &'a [RenderedBootstrapStep],
        generated_mediated_ca_cert_seeded: bool,
        generated_mediated_ca_key_file_written: bool,
    }

    #[derive(Serialize)]
    struct QemuProvisioningImageSnapshot<'a> {
        os: &'a str,
        architecture: &'a str,
        variant: &'a str,
        source_url: &'a str,
        format: &'a str,
    }

    fn assert_qemu_provisioning_snapshot(name: &str, temp: &TestTempDir, prepared: &PreparedProvisioning) {
        let rendered_bootstrap = prepared
            .provisioning_plan
            .render_instance_bootstrap(&prepared.manifest)
            .expect("render instance bootstrap");
        let snapshot = QemuProvisioningSnapshot {
            image: QemuProvisioningImageSnapshot {
                os: prepared.provisioning_plan.image.os_name(),
                architecture: prepared.provisioning_plan.image.architecture_name(),
                variant: prepared.provisioning_plan.image.variant_name(),
                source_url: prepared.qemu_image.url,
                format: prepared.qemu_image.format,
            },
            work_dir: path_text(&prepared.seed.work_dir),
            seed_dir: path_text(&prepared.seed.seed_dir),
            seed_meta_data: fs::read_to_string(&prepared.seed.meta_data).unwrap(),
            seed_network_config: fs::read_to_string(&prepared.seed.network_config).unwrap(),
            seed_user_data: fs::read_to_string(&prepared.seed.user_data).unwrap(),
            bootstrap_steps: &rendered_bootstrap.steps,
            generated_mediated_ca_cert_seeded: !prepared.mediated_ca.cert_pem.is_empty(),
            generated_mediated_ca_key_file_written: !prepared.mediated_ca.key_path.is_empty()
                && Path::new(&prepared.mediated_ca.key_path).is_file(),
        };
        let rendered = serde_yaml::to_string(&snapshot).unwrap();
        let rendered = normalize_snapshot(temp, &rendered);
        snapshot::assert_topic(env!("CARGO_MANIFEST_DIR"), "qemu_provisioning", name, &rendered);
    }

    fn normalize_snapshot(temp: &TestTempDir, contents: &str) -> String {
        let contents = contents.replace(&temp.path.display().to_string(), "$TMP");
        let contents = redact_pem_blocks(&contents);
        redact_seed_file_contents(&contents)
    }

    fn redact_pem_blocks(contents: &str) -> String {
        let mut redacted = String::new();
        let mut in_certificate = false;
        for line in contents.lines() {
            if line.contains("-----BEGIN CERTIFICATE-----") {
                let indent = line.split("-----BEGIN CERTIFICATE-----").next().unwrap_or("");
                redacted.push_str(indent);
                redacted.push_str("$GENERATED_MEDIATED_CA_CERTIFICATE\n");
                in_certificate = true;
                continue;
            }
            if in_certificate {
                if line.contains("-----END CERTIFICATE-----") {
                    in_certificate = false;
                }
                continue;
            }
            redacted.push_str(line);
            redacted.push('\n');
        }
        redacted
    }

    fn redact_seed_file_contents(contents: &str) -> String {
        let mut redacted = String::new();
        for line in contents.lines() {
            if line.trim_start().starts_with("content: ") {
                let indent = line.split("content:").next().unwrap_or("");
                redacted.push_str(indent);
                redacted.push_str("content: $BASE64_CONTENT\n");
                continue;
            }
            redacted.push_str(line);
            redacted.push('\n');
        }
        redacted
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn create(name: &str) -> Self {
            let timestamp = time::unix_nanos();
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
            set_executable(&tool);
            tool
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

    #[cfg(unix)]
    fn set_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt as _;

        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    #[cfg(not(unix))]
    fn set_executable(_path: &Path) {}

    #[cfg(windows)]
    fn executable_script_name(name: &str) -> String {
        format!("{name}.cmd")
    }

    #[cfg(unix)]
    fn executable_script_name(name: &str) -> String {
        name.to_owned()
    }
}
