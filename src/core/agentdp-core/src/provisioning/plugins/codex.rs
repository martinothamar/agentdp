use crate::manifest::GuestOs;
use crate::manifest::plugins::codex::Codex;

use super::Plugin;
use crate::provisioning::bootstrap::ProvisioningBuilder;

mod linux;

impl Plugin for Codex {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        match builder.guest_os() {
            GuestOs::Archlinux | GuestOs::Rocky9 => linux::apply(self, builder),
        }
    }
}
