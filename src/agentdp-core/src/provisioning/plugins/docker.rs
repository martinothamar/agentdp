use crate::manifest::plugins::docker::Docker;

use super::Plugin;
use crate::provisioning::bootstrap::{HealthcheckKind, HealthcheckPlan, ProvisioningBuilder};
use crate::provisioning::shell;

impl Plugin for Docker {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        builder.add_package("docker");
        if self.compose {
            builder.add_package("docker-compose");
        }
        if self.buildx {
            builder.add_package("docker-buildx");
        }
        builder.add_user_group("docker");
        builder.add_root_shell(shell::enable_systemd_service_if_present("docker.service"));
        if self.healthcheck {
            builder.add_healthcheck_if_absent(HealthcheckPlan {
                name: "docker".to_owned(),
                kind: HealthcheckKind::Command("docker ps".to_owned()),
                timeout: Some("60s".to_owned()),
            });
        }
    }
}
