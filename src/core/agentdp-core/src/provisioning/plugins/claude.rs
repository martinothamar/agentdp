use crate::manifest::GuestOs;
use crate::manifest::plugins::claude::Claude;

use super::Plugin;
use crate::provisioning::bootstrap::ProvisioningBuilder;

mod linux;

impl Plugin for Claude {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        match builder.guest_os() {
            GuestOs::Archlinux | GuestOs::Rocky9 => linux::apply(self, builder),
        }
    }
}
