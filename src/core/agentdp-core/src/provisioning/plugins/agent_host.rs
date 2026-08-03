use crate::manifest::GuestOs;
use crate::manifest::plugins::agent_host::AgentHost;

use super::Plugin;
use crate::provisioning::bootstrap::ProvisioningBuilder;

mod linux;

impl Plugin for AgentHost {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        match builder.guest_os() {
            GuestOs::Archlinux | GuestOs::Rocky9 => linux::apply(builder),
        }
    }
}
