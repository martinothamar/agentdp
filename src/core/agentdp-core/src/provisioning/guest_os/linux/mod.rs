pub(super) mod arch;
pub(in crate::provisioning) mod bootstrap;
pub(in crate::provisioning) mod ca_bundle;
pub mod cloud_init;
pub(in crate::provisioning) mod guest_tooling;
pub(in crate::provisioning) mod paths;
pub(super) mod rocky;
mod root_setup;
mod services;
pub(in crate::provisioning) mod shell;
pub(in crate::provisioning) mod systemd;
pub(super) mod templates;

use crate::provisioning::SeedFile;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use agentdp_protocol::server_guest::GuestInstancePaths;

use super::{GuestLayout, GuestToolSeed};

pub(super) use paths::{AGENT_HOME, CODE_DIR, CUSTOM_BOOTSTRAP_PATH, CUSTOM_ENV_PATH, PERSISTENT_CUSTOM_ENV_PATH};

pub(super) const fn guest_layout() -> GuestLayout {
    paths::guest_layout()
}

pub(super) fn guest_instance_paths() -> GuestInstancePaths {
    paths::guest_instance_paths()
}

pub(super) const fn guest_tool_seeds() -> &'static [GuestToolSeed] {
    guest_tooling::GUEST_TOOL_SEEDS
}

pub(super) fn system_guestd_service_seed(instance_spec_path: &str) -> SeedFile {
    services::system_guestd_service_seed(instance_spec_path)
}

pub(super) fn apply_base(builder: &mut ProvisioningBuilder<'_>) {
    if builder.ca_enabled() {
        ca_bundle::apply(builder);
    }
    guest_tooling::apply(builder);
}

pub(super) use root_setup::{pre_user_boot_commands, root_setup};
