use crate::manifest::GuestOs;
use crate::manifest::plugins::mise::Mise;

use super::Plugin;
use crate::provisioning::bootstrap::ProvisioningBuilder;

mod linux;

impl Plugin for Mise {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        builder.require_mise();
        for package in &self.packages {
            builder.require_mise_package(package.clone());
        }
    }
}

pub(super) fn apply_requirements(builder: &mut ProvisioningBuilder<'_>) {
    if !builder.requires_mise() {
        return;
    }

    match builder.guest_os() {
        GuestOs::Archlinux | GuestOs::Rocky9 => linux::apply_requirements(builder),
    }
}
