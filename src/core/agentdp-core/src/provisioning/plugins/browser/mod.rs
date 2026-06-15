use crate::manifest::GuestOs;
use crate::manifest::plugins::Plugins;
use crate::manifest::plugins::browser::Browser;
use crate::provisioning::bootstrap::ProvisioningBuilder;

use super::Plugin;

mod linux;

impl Plugin for Browser {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        match builder.guest_os() {
            GuestOs::Archlinux | GuestOs::Rocky9 => linux::apply(self, builder),
        }
    }
}

pub(super) fn apply_codex_integration(plugins: &Plugins, builder: &mut ProvisioningBuilder<'_>) {
    match builder.guest_os() {
        GuestOs::Archlinux | GuestOs::Rocky9 => linux::apply_codex_integration(plugins, builder),
    }
}
