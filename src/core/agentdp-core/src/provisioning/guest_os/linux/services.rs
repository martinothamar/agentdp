use crate::provisioning::SeedFile;

use super::paths::GUESTD_SYSTEM_SERVICE_PATH;

pub(super) fn system_guestd_service_seed(instance_spec_path: &str) -> SeedFile {
    SeedFile {
        path: GUESTD_SYSTEM_SERVICE_PATH.to_owned(),
        contents: guestd_system_service(instance_spec_path).into_bytes(),
        permissions: "0644".to_owned(),
        owner: Some("root:root".to_owned()),
    }
}

fn guestd_system_service(instance_spec_path: &str) -> String {
    format!(
        "\
[Unit]
Description=agentdp system guest daemon

[Service]
Type=simple
ExecStart=/usr/local/bin/guestd system --instance-spec {instance_spec_path}
StandardOutput=journal+console
StandardError=journal+console
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
"
    )
}
