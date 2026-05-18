#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

mod support;

use support::{fixture::AgentFixture, manifest::valid_manifest, snapshot};

#[test]
fn status_created_instance() {
    let fixture = AgentFixture::new("status-created-instance", valid_manifest());
    let create = fixture.create_instance(&["ssh:2222", "code-server:24090"]);

    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }

    let output = fixture.status();

    snapshot::assert(file!(), "created_instance", &output.render());
}

#[test]
fn status_stale_running_instance() {
    let fixture = AgentFixture::new("status-stale-running-instance", valid_manifest());
    let create = fixture.create_instance(&["ssh:2222", "code-server:24090"]);

    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }
    fixture.mark_running_with_missing_pid();

    let output = fixture.status();

    snapshot::assert(file!(), "stale_running_instance", &output.render());
}
