pub mod linux;

use crate::manifest::{AgentManifest, GuestOs, HostAlias, User};
use agentdp_protocol::server_guest::{GuestInstancePaths, GuestPlatform};

use super::bootstrap::{BootstrapGraphError, ProvisioningBuilder, RenderedBootstrapPlan};
use super::image::CatalogImage;
use super::{ProvisioningPlan, SeedFile};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuestLayout {
    pub agent_home: &'static str,
    pub code_dir: &'static str,
    pub custom_bootstrap: &'static str,
    pub runtime_env: &'static str,
    pub persistent_env: &'static str,
    pub ca_bundle: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuestOsCapabilities {
    pub platform: GuestPlatform,
    pub layout: GuestLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuestToolSeed {
    pub name: &'static str,
    pub guest_path: &'static str,
    pub permissions: &'static str,
    pub compress: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GuestBootOptions {
    pub install_ca: bool,
    pub ca_bundle_command: String,
}

#[must_use]
pub const fn guest_tool_seeds_for_os(os: GuestOs) -> &'static [GuestToolSeed] {
    match os {
        GuestOs::Archlinux | GuestOs::Rocky9 => linux::guest_tool_seeds(),
    }
}

#[must_use]
pub fn system_guestd_service_seed_for_os(os: GuestOs, instance_spec_path: &str) -> SeedFile {
    match os {
        GuestOs::Archlinux | GuestOs::Rocky9 => linux::system_guestd_service_seed(instance_spec_path),
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GuestOsAdapter {
    os: GuestOs,
}

impl GuestOsAdapter {
    #[must_use]
    pub const fn for_os(os: GuestOs) -> Self {
        Self { os }
    }

    #[must_use]
    pub(super) const fn os(self) -> GuestOs {
        self.os
    }

    #[must_use]
    pub const fn capabilities(self) -> GuestOsCapabilities {
        GuestOsCapabilities {
            platform: match self.os {
                GuestOs::Archlinux | GuestOs::Rocky9 => GuestPlatform::Linux,
            },
            layout: self.layout(),
        }
    }

    pub(super) fn base_packages(self, needs_git: bool) -> Vec<String> {
        match self.os {
            GuestOs::Archlinux => linux::arch::base_packages(needs_git),
            GuestOs::Rocky9 => linux::rocky::base_packages(needs_git),
        }
    }

    pub(super) const fn catalog_image(self) -> CatalogImage {
        match self.os {
            GuestOs::Archlinux => linux::arch::catalog_image(),
            GuestOs::Rocky9 => linux::rocky::catalog_image(),
        }
    }

    pub(super) fn pre_user_boot_commands(self, user: &User) -> Vec<String> {
        match self.os {
            GuestOs::Archlinux | GuestOs::Rocky9 => linux::pre_user_boot_commands(user),
        }
    }

    pub(super) fn boot_commands(self, options: GuestBootOptions) -> Vec<String> {
        match self.os {
            GuestOs::Archlinux => linux::arch::boot_commands(options),
            GuestOs::Rocky9 => linux::rocky::boot_commands(options),
        }
    }

    pub(super) fn root_setup(self, user: &User, host_aliases: &[HostAlias]) -> Vec<String> {
        match self.os {
            GuestOs::Archlinux => linux::arch::root_setup(user, host_aliases),
            GuestOs::Rocky9 => linux::rocky::root_setup(user, host_aliases),
        }
    }

    pub(super) const fn ca_bundle_install(self) -> &'static str {
        match self.os {
            GuestOs::Archlinux => linux::arch::ca_bundle_install(),
            GuestOs::Rocky9 => linux::rocky::ca_bundle_install(),
        }
    }

    pub(super) fn apply_base(self, builder: &mut ProvisioningBuilder<'_>) {
        match self.os {
            GuestOs::Archlinux | GuestOs::Rocky9 => linux::apply_base(builder),
        }
    }

    pub(super) fn render_complete_bootstrap_plan(
        self,
        manifest: &AgentManifest,
        plan: &ProvisioningPlan,
    ) -> Result<RenderedBootstrapPlan, BootstrapGraphError> {
        let input = ProvisioningBuilder::render_input(manifest, plan);
        match self.os {
            GuestOs::Archlinux | GuestOs::Rocky9 => linux::bootstrap::render_complete_bootstrap_plan(&input),
        }
    }

    pub(super) fn instance_paths(self) -> GuestInstancePaths {
        match self.os {
            GuestOs::Archlinux | GuestOs::Rocky9 => linux::guest_instance_paths(),
        }
    }

    #[must_use]
    pub(super) const fn layout(self) -> GuestLayout {
        match self.os {
            GuestOs::Archlinux | GuestOs::Rocky9 => linux::guest_layout(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::manifest::{GuestOs, User, UserLinux, UserOptions};
    use crate::provisioning::guest_os::GuestOsAdapter;
    use crate::provisioning::image::{ImageArchitecture, ImageVariant};

    #[test]
    fn arch_adapter_adds_arch_packages_and_linux_root_setup() {
        let adapter = GuestOsAdapter::for_os(GuestOs::Archlinux);

        assert_eq!(
            adapter.base_packages(true),
            vec![
                "sudo".to_owned(),
                "cloud-guest-utils".to_owned(),
                "gptfdisk".to_owned(),
                "gzip".to_owned(),
                "inetutils".to_owned(),
                "tmux".to_owned(),
                "git".to_owned(),
            ]
        );

        let root_setup = adapter.root_setup(&agent_user(), &[]);
        assert!(root_setup.iter().any(|block| block.contains("grow_agentdp_root")));
        assert!(
            root_setup
                .iter()
                .any(|block| block.contains("agentdp-hostname.service"))
        );
        assert!(
            root_setup
                .iter()
                .any(|block| block.contains("/data/home/.tmux.conf") && block.contains("set -g mouse on"))
        );
        assert!(!root_setup.iter().any(|block| block.contains(".codex/AGENTS.md")));
    }

    #[test]
    fn linux_adapters_add_hostname_command_packages() {
        assert!(
            GuestOsAdapter::for_os(GuestOs::Archlinux)
                .base_packages(false)
                .iter()
                .any(|package| package == "inetutils")
        );
        assert!(
            GuestOsAdapter::for_os(GuestOs::Rocky9)
                .base_packages(false)
                .iter()
                .any(|package| package == "hostname")
        );
    }

    #[test]
    fn adapter_resolves_os_catalog_images() {
        for os in [GuestOs::Archlinux, GuestOs::Rocky9] {
            let image = GuestOsAdapter::for_os(os).catalog_image();
            assert_eq!(image.os, os);
            assert_eq!(image.architecture, ImageArchitecture::X86_64);
            assert_eq!(image.variant, ImageVariant::Cloud);
        }
    }

    #[test]
    fn adapter_prepares_high_numeric_linux_user_ids() {
        let user = User {
            name: "agent".to_owned(),
            options: UserOptions::Linux(UserLinux {
                uid: Some(1_199_049_453),
                gid: Some(1_199_000_513),
                group: Some("domain-users".to_owned()),
                groups: Vec::new(),
            }),
        };

        let commands = GuestOsAdapter::for_os(GuestOs::Rocky9).pre_user_boot_commands(&user);

        assert_eq!(
            commands[0],
            "sed -i -E 's/^UID_MAX.*/UID_MAX 2147483647/; s/^GID_MAX.*/GID_MAX 2147483647/' /etc/login.defs"
        );
        assert_eq!(
            commands[1],
            "getent group 'domain-users' >/dev/null 2>&1 || groupadd -g 1199000513 'domain-users'"
        );
    }

    fn agent_user() -> User {
        User {
            name: "agent".to_owned(),
            options: UserOptions::default(),
        }
    }
}
