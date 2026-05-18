#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

mod support;

use support::{fixture::AgentFixture, manifest::valid_manifest, snapshot};

#[test]
fn ps_lists_created_instance() {
    let fixture = AgentFixture::new("ps-lists-created-instance", valid_manifest());
    let create = fixture.create_instance(&["ssh:2222"]);

    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }

    let output = fixture.ps();

    snapshot::assert(file!(), "lists_created_instance", &output.render());
}
