use crate::manifest::GuestOs;
use crate::manifest::plugins::code_server::CodeServer;
use crate::provisioning::bootstrap::ProvisioningBuilder;

use super::Plugin;

mod linux;

impl Plugin for CodeServer {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        match builder.guest_os() {
            GuestOs::Archlinux | GuestOs::Rocky9 => linux::apply(self, builder),
        }
    }
}
