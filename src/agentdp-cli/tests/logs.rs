#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

mod support;

use support::{fixture::AgentFixture, manifest::valid_manifest, snapshot};

#[test]
fn logs_serial_tail() {
    let fixture = AgentFixture::new("logs-serial-tail", valid_manifest());
    let create = fixture.create_instance(&["ssh:2222", "code-server:24090"]);

    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }

    fixture.write_serial_log("first boot line\nsecond boot line\nthird boot line\n");

    let output = fixture.logs(false, Some(2));

    snapshot::assert(file!(), "serial_tail", &output.render());
}
