use crate::manifest::{AgentManifest, NetworkMode};
use crate::mediated_network::MediatedNetworkProfile;
use crate::provisioning::cloud_init::{CloudInitSeed, Error, render_meta_data};
use crate::provisioning::guest_os::{GuestBootOptions, GuestOsAdapter};
use crate::provisioning::{ProvisioningPlan, SeedFile};

mod render;

pub use render::render_network_config;
use render::{PackageConfig, render_user_data};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudInitOptions {
    pub ssh_authorized_key: Option<String>,
    pub seed_files: Vec<SeedFile>,
    pub run_commands: Vec<String>,
    pub network_mode: Option<NetworkMode>,
    pub mediated_network: MediatedNetworkProfile,
    pub mediated_ipv6: bool,
}

impl CloudInitSeed {
    /// Renders cloud-init seed content from the manifest and provisioning context.
    ///
    /// # Errors
    ///
    /// Returns an error if cloud-init metadata or user-data cannot be serialized.
    ///
    /// # Panics
    ///
    /// Panics if the manifest and provisioning plan describe different guest operating systems.
    pub fn from_plan(
        manifest: &AgentManifest,
        plan: &ProvisioningPlan,
        options: &CloudInitOptions,
    ) -> Result<Self, Error> {
        assert_eq!(
            plan.image.os, manifest.spec.image.os,
            "cloud-init seed manifest OS must match provisioning plan OS"
        );

        let instance_id = cloud_init_instance_id(manifest.name(), &plan.hostname);
        Ok(Self {
            meta_data: render_meta_data(&instance_id, &plan.hostname)?,
            network_config: render::render_network_config(
                options.network_mode.unwrap_or(manifest.spec.network.mode),
                options.mediated_network,
                options.mediated_ipv6,
            ),
            user_data: render_user_data(
                &boot_commands(manifest, options),
                PackageConfig {
                    update: manifest.spec.bootstrap.package_update,
                    packages: &[],
                },
                &manifest.spec.user,
                GuestOsAdapter::for_os(plan.image.os).capabilities().layout.agent_home,
                &options.seed_files,
                &options.run_commands,
                options.ssh_authorized_key.as_deref(),
            )?,
        })
    }
}

fn cloud_init_instance_id(manifest_name: &str, hostname: &str) -> String {
    if hostname == manifest_name {
        return manifest_name.to_owned();
    }
    format!("{manifest_name}-{hostname}")
}

fn boot_commands(manifest: &AgentManifest, options: &CloudInitOptions) -> Vec<String> {
    let guest_os = GuestOsAdapter::for_os(manifest.spec.image.os);
    let mut commands = guest_os.pre_user_boot_commands(&manifest.spec.user);
    commands.extend(guest_os.boot_commands(GuestBootOptions {
        install_ca: manifest.spec.network.ca.is_active(manifest.spec.network.mode),
        ca_bundle_command: ca_bundle_boot_command(manifest, options),
    }));
    commands
}

fn ca_bundle_boot_command(manifest: &AgentManifest, options: &CloudInitOptions) -> String {
    let install_command = GuestOsAdapter::for_os(manifest.spec.image.os).ca_bundle_install();
    options
        .seed_files
        .iter()
        .find(|file| file.path == super::ca_bundle::CA_BUNDLE_PATH)
        .and_then(|file| std::str::from_utf8(&file.contents).ok())
        .map_or_else(
            || install_command.to_owned(),
            |cert_pem| super::ca_bundle::install_seeded_ca_bundle(cert_pem, install_command),
        )
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use agentdp_test_support::{manifest, snapshot};
    use serde_yaml::Value;

    use crate::manifest::{AgentManifest, GuestOs, NetworkMode, User, UserOptions};
    use crate::mediated_network::DEFAULT_PROFILE;
    use crate::provisioning::cloud_init::{CloudInitSeed, render_meta_data};
    use crate::provisioning::guest_os::GuestOsAdapter;
    use crate::provisioning::guest_os::linux::cloud_init::CloudInitOptions;
    use crate::provisioning::guest_os::linux::cloud_init::render::{
        PackageConfig, render_network_config, render_user_data,
    };
    use crate::provisioning::{ProvisioningOptions, ProvisioningPlan, SeedFile};

    #[test]
    fn meta_data_is_structured_yaml() {
        let meta_data = render_meta_data("agent.example_1", "pr_0").unwrap();
        let parsed = serde_yaml::from_str::<Value>(&meta_data).unwrap();

        assert_eq!(parsed["instance-id"], Value::from("agent.example_1"));
        assert_eq!(parsed["local-hostname"], Value::from("pr-0"));
    }

    #[test]
    fn cloud_init_instance_id_includes_non_default_hostname() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::minimal()).unwrap();
        let plan = ProvisioningPlan::from_manifest(
            &manifest,
            &ProvisioningOptions {
                hostname: Some("replica-0".to_owned()),
            },
        );
        let seed = CloudInitSeed::from_plan(&manifest, &plan, &CloudInitOptions::default()).unwrap();
        let parsed = serde_yaml::from_str::<Value>(&seed.meta_data).unwrap();

        assert_eq!(parsed["instance-id"], Value::from("altinn-studio-replica-0"));
        assert_eq!(parsed["local-hostname"], Value::from("replica-0"));
    }

    #[test]
    fn user_data_is_cloud_config_yaml() {
        let packages = vec!["git".to_owned(), "package:with:symbols".to_owned()];
        let user_data = render_user_data(
            &[],
            PackageConfig {
                update: false,
                packages: &packages,
            },
            &agent_user(),
            super::super::AGENT_HOME,
            &[],
            &[],
            None,
        )
        .unwrap();

        assert!(user_data.starts_with("#cloud-config\n"));
        let parsed = parse_user_data(&user_data);
        assert!(parsed.get("package_update").is_none());
        assert!(parsed.get("bootcmd").is_none());
        assert_eq!(parsed["packages"][0], Value::from("git"));
        assert_eq!(parsed["packages"][1], Value::from("package:with:symbols"));
        assert_eq!(parsed["users"][0], Value::from("default"));
        assert_eq!(parsed["users"][1]["name"], Value::from("agent"));
        assert_eq!(parsed["users"][1]["sudo"], Value::from("ALL=(ALL) NOPASSWD:ALL"));
        assert_eq!(parsed["users"][1]["shell"], Value::from("/bin/bash"));
        assert_eq!(parsed["write_files"].as_sequence().unwrap().len(), 0);
        assert!(parsed.get("runcmd").is_none());
    }

    #[test]
    fn network_config_sets_static_mediated_network() {
        let parsed =
            serde_yaml::from_str::<Value>(&render_network_config(NetworkMode::Mediated, DEFAULT_PROFILE, false))
                .expect("parse network-config");

        assert_eq!(parsed["version"], Value::from(1));
        assert_eq!(parsed["config"][0]["type"], Value::from("physical"));
        assert_eq!(parsed["config"][0]["name"], Value::from("eth0"));
        assert_eq!(
            parsed["config"][0]["mac_address"],
            Value::from(DEFAULT_PROFILE.guest_mac.to_string())
        );
        assert_eq!(parsed["config"][0]["subnets"][0]["type"], Value::from("static"));
        assert_eq!(
            parsed["config"][0]["subnets"][0]["address"],
            Value::from("10.73.0.10/24")
        );
        assert_eq!(parsed["config"][0]["subnets"][0]["gateway"], Value::from("10.73.0.1"));
        assert_eq!(
            parsed["config"][0]["subnets"][0]["dns_nameservers"][0],
            Value::from("10.73.0.1")
        );
    }

    #[test]
    fn network_config_sets_static_mediated_ipv6_network() {
        let parsed =
            serde_yaml::from_str::<Value>(&render_network_config(NetworkMode::Mediated, DEFAULT_PROFILE, true))
                .expect("parse network-config");

        assert_eq!(
            parsed["config"][0]["subnets"][1]["address"],
            Value::from("fd42:6175:6469:6f::10/64")
        );
        assert_eq!(
            parsed["config"][0]["subnets"][1]["gateway"],
            Value::from("fd42:6175:6469:6f::1")
        );
    }

    #[test]
    fn network_config_sets_user_qemu_dhcp() {
        let parsed = serde_yaml::from_str::<Value>(&render_network_config(NetworkMode::User, DEFAULT_PROFILE, false))
            .expect("parse network-config");

        assert_eq!(parsed["version"], Value::from(1));
        assert_eq!(parsed["config"][0]["type"], Value::from("physical"));
        assert_eq!(parsed["config"][0]["name"], Value::from("eth0"));
        assert_eq!(parsed["config"][0]["subnets"][0]["type"], Value::from("dhcp"));
        assert!(parsed["config"][0]["subnets"][0].get("address").is_none());
        assert!(parsed["config"][0]["subnets"][0].get("gateway").is_none());
        assert!(parsed["config"][0]["subnets"][0].get("dns_nameservers").is_none());
    }

    #[test]
    fn empty_package_list_is_omitted() {
        let user_data = render_user_data(
            &[],
            PackageConfig {
                update: false,
                packages: &[],
            },
            &agent_user(),
            super::super::AGENT_HOME,
            &[],
            &[],
            None,
        )
        .unwrap();
        let parsed = parse_user_data(&user_data);

        assert!(parsed.get("packages").is_none());
    }

    #[test]
    fn package_update_is_explicit_opt_in() {
        let user_data = render_user_data(
            &[],
            PackageConfig {
                update: true,
                packages: &[],
            },
            &agent_user(),
            super::super::AGENT_HOME,
            &[],
            &[],
            None,
        )
        .unwrap();
        let parsed = parse_user_data(&user_data);

        assert_eq!(parsed["package_update"], Value::from(true));
    }

    #[test]
    fn renders_standard_manifest_seed() {
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest::standard()).unwrap();
        let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());
        let seed = CloudInitSeed::from_plan(&manifest, &plan, &CloudInitOptions::default()).unwrap();

        snapshot::assert_topic(
            env!("CARGO_MANIFEST_DIR"),
            "cloud_init",
            "renders_standard_manifest_seed",
            &seed_snapshot(&seed),
        );
    }

    #[test]
    fn arch_user_data_refreshes_pacman_keyring_by_default() {
        let mut manifest = serde_yaml::from_str::<AgentManifest>(agentdp_test_support::manifest::minimal()).unwrap();
        manifest.spec.network.mode = NetworkMode::User;
        let plan = provisioning_plan(&manifest, "smoke");
        let seed = CloudInitSeed::from_plan(&manifest, &plan, &CloudInitOptions::default()).unwrap();
        let parsed = parse_user_data(&seed.user_data);

        assert_eq!(
            parsed["bootcmd"][0],
            Value::from(
                "mkdir -p /etc/systemd/system/systemd-time-wait-sync.service.d && printf '[Service]\\nTimeoutStartSec=30s\\n' >/etc/systemd/system/systemd-time-wait-sync.service.d/agentdp-timeout.conf && systemctl daemon-reload || true"
            )
        );
        assert_eq!(parsed["bootcmd"][1], Value::from("pacman-key --init"));
        assert_eq!(parsed["bootcmd"][2], Value::from("pacman-key --populate archlinux"));
        assert_eq!(
            parsed["bootcmd"][3],
            Value::from("pacman -Sy --noconfirm archlinux-keyring")
        );
        assert_eq!(parsed["bootcmd"].as_sequence().unwrap().len(), 4);
    }

    #[test]
    fn arch_package_update_keeps_cloud_init_package_update_opt_in() {
        let mut manifest = serde_yaml::from_str::<AgentManifest>(agentdp_test_support::manifest::minimal()).unwrap();
        manifest.spec.network.mode = NetworkMode::User;
        manifest.spec.bootstrap.package_update = true;
        let plan = provisioning_plan(&manifest, "smoke");
        let seed = CloudInitSeed::from_plan(&manifest, &plan, &CloudInitOptions::default()).unwrap();
        let parsed = parse_user_data(&seed.user_data);

        assert_eq!(parsed["package_update"], Value::from(true));
        assert_eq!(parsed["bootcmd"][1], Value::from("pacman-key --init"));
        assert_eq!(parsed["bootcmd"][2], Value::from("pacman-key --populate archlinux"));
        assert_eq!(
            parsed["bootcmd"][3],
            Value::from("pacman -Sy --noconfirm archlinux-keyring")
        );
    }

    #[test]
    fn mediated_user_data_installs_ca_without_package_update() {
        let manifest = serde_yaml::from_str::<AgentManifest>(agentdp_test_support::manifest::minimal()).unwrap();
        let plan = provisioning_plan(&manifest, "smoke");
        let seed = CloudInitSeed::from_plan(&manifest, &plan, &CloudInitOptions::default()).unwrap();
        let parsed = parse_user_data(&seed.user_data);

        assert_eq!(
            parsed["bootcmd"][3],
            Value::from(GuestOsAdapter::for_os(manifest.spec.image.os).ca_bundle_install())
        );
        assert_eq!(
            parsed["bootcmd"][4],
            Value::from("pacman -Sy --noconfirm archlinux-keyring")
        );
        assert_eq!(parsed["bootcmd"].as_sequence().unwrap().len(), 5);
    }

    #[test]
    fn mediated_user_data_installs_ca_before_pacman_network_access() {
        let manifest = serde_yaml::from_str::<AgentManifest>(agentdp_test_support::manifest::minimal()).unwrap();
        let plan = provisioning_plan(&manifest, "smoke");
        let seed = CloudInitSeed::from_plan(&manifest, &plan, &CloudInitOptions::default()).unwrap();
        let parsed = parse_user_data(&seed.user_data);

        assert_eq!(
            parsed["bootcmd"][3],
            Value::from(GuestOsAdapter::for_os(manifest.spec.image.os).ca_bundle_install())
        );
        assert_eq!(
            parsed["bootcmd"][4],
            Value::from("pacman -Sy --noconfirm archlinux-keyring")
        );
    }

    #[test]
    fn mediated_user_data_materializes_seeded_ca_before_pacman_network_access() {
        let manifest = serde_yaml::from_str::<AgentManifest>(agentdp_test_support::manifest::minimal()).unwrap();
        let plan = provisioning_plan(&manifest, "smoke");
        let seed = CloudInitSeed::from_plan(
            &manifest,
            &plan,
            &CloudInitOptions {
                seed_files: vec![SeedFile {
                    path: super::super::ca_bundle::CA_BUNDLE_PATH.to_owned(),
                    contents: b"-----BEGIN CERTIFICATE-----\nagentdp-test-ca\n-----END CERTIFICATE-----\n".to_vec(),
                    permissions: "0644".to_owned(),
                    owner: Some("root:root".to_owned()),
                }],
                ..CloudInitOptions::default()
            },
        )
        .unwrap();
        let parsed = parse_user_data(&seed.user_data);
        let ca_command = parsed["bootcmd"][3].as_str().unwrap();

        assert!(ca_command.contains("cat >/var/lib/agentdp/ca/ca-bundle.pem"));
        assert!(ca_command.contains("agentdp-test-ca"));
        assert_eq!(
            parsed["bootcmd"][4],
            Value::from("pacman -Sy --noconfirm archlinux-keyring")
        );
    }

    #[test]
    fn user_data_can_authorize_ssh_key_for_default_user() {
        let user_data = render_user_data(
            &[],
            PackageConfig {
                update: false,
                packages: &[],
            },
            &agent_user(),
            super::super::AGENT_HOME,
            &[],
            &[],
            Some("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITest agentdp"),
        )
        .unwrap();
        let parsed = parse_user_data(&user_data);

        assert_eq!(
            parsed["users"][1]["ssh_authorized_keys"][0],
            Value::from("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITest agentdp")
        );
    }

    #[test]
    fn user_data_can_start_seeded_system_services() {
        let command = "systemctl daemon-reload && systemctl enable --now guestd-system.service".to_owned();
        let user_data = render_user_data(
            &[],
            PackageConfig {
                update: false,
                packages: &[],
            },
            &agent_user(),
            super::super::AGENT_HOME,
            &[],
            std::slice::from_ref(&command),
            None,
        )
        .unwrap();
        let parsed = parse_user_data(&user_data);

        assert_eq!(parsed["runcmd"][0], Value::from(command));
    }

    #[test]
    fn rocky_user_data_precreates_numeric_primary_group_and_user_id() {
        let manifest = serde_yaml::from_str::<AgentManifest>(
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: rhel-podman
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: rocky9
    user:
      name: agent
      linux:
        uid: 1199049453
        gid: 1199000513
        group: domain-users
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        )
        .unwrap();
        let plan = provisioning_plan(&manifest, "rhel-0");
        let seed = CloudInitSeed::from_plan(&manifest, &plan, &CloudInitOptions::default()).unwrap();
        let parsed = parse_user_data(&seed.user_data);

        assert_eq!(
            parsed["bootcmd"][0],
            Value::from(
                "sed -i -E 's/^UID_MAX.*/UID_MAX 2147483647/; s/^GID_MAX.*/GID_MAX 2147483647/' /etc/login.defs"
            )
        );
        assert_eq!(
            parsed["bootcmd"][1],
            Value::from("getent group 'domain-users' >/dev/null 2>&1 || groupadd -g 1199000513 'domain-users'")
        );
        assert_eq!(parsed["users"][1]["uid"], Value::from(1_199_049_453_u64));
        assert_eq!(parsed["users"][1]["primary_group"], Value::from("domain-users"));
    }

    #[test]
    #[should_panic(expected = "cloud-init seed manifest OS must match provisioning plan OS")]
    fn from_plan_rejects_mismatched_manifest_and_plan_os() {
        let plan_manifest = serde_yaml::from_str::<AgentManifest>(agentdp_test_support::manifest::minimal()).unwrap();
        let mut seed_manifest = plan_manifest.clone();
        seed_manifest.spec.image.os = GuestOs::Rocky9;
        let plan = provisioning_plan(&plan_manifest, "smoke");

        let _ = CloudInitSeed::from_plan(&seed_manifest, &plan, &CloudInitOptions::default());
    }

    #[test]
    fn user_data_writes_seed_files_as_base64() {
        let seed_files = [SeedFile {
            path: "/data/home/.codex/AGENTS.md".to_owned(),
            contents: b"agent instructions\n".to_vec(),
            permissions: "0644".to_owned(),
            owner: None,
        }];
        let user_data = render_user_data(
            &[],
            PackageConfig {
                update: false,
                packages: &[],
            },
            &agent_user(),
            super::super::AGENT_HOME,
            &seed_files,
            &[],
            None,
        )
        .unwrap();

        let parsed = parse_user_data(&user_data);
        assert_eq!(
            parsed["write_files"][0]["path"],
            Value::from("/data/home/.codex/AGENTS.md")
        );
        assert_eq!(parsed["write_files"][0]["permissions"], Value::from("0644"));
        assert!(parsed["write_files"][0].get("owner").is_none());
        assert_eq!(parsed["write_files"][0]["encoding"], Value::from("b64"));
        assert_eq!(
            parsed["write_files"][0]["content"],
            Value::from("YWdlbnQgaW5zdHJ1Y3Rpb25zCg==")
        );
    }

    fn parse_user_data(user_data: &str) -> Value {
        serde_yaml::from_str(user_data.strip_prefix("#cloud-config\n").expect("cloud-config header"))
            .expect("parse user-data")
    }

    fn agent_user() -> User {
        User {
            name: "agent".to_owned(),
            options: UserOptions::default(),
        }
    }

    fn provisioning_plan(manifest: &AgentManifest, hostname: &str) -> ProvisioningPlan {
        ProvisioningPlan::from_manifest(
            manifest,
            &ProvisioningOptions {
                hostname: Some(hostname.to_owned()),
            },
        )
    }

    fn seed_snapshot(seed: &CloudInitSeed) -> String {
        let mut output = String::new();
        output.push_str("--- meta-data\n");
        output.push_str(&seed.meta_data);
        let _ = writeln!(&mut output, "--- network-config");
        output.push_str(&seed.network_config);
        let _ = writeln!(&mut output, "--- user-data");
        output.push_str(&seed.user_data);
        output
    }
}
