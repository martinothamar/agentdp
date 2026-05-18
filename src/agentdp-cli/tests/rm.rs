#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

mod support;

use support::{fixture::AgentFixture, manifest::valid_manifest, snapshot};

#[test]
fn rm_created_instance() {
    let fixture = AgentFixture::new("rm-created-instance", valid_manifest());
    let create = fixture.create_instance(&["ssh:2222"]);

    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }

    assert!(fixture.instance_dir().is_dir(), "{}", create.render());

    let output = fixture.rm();

    assert!(!fixture.instance_dir().exists(), "{}", output.render());
    snapshot::assert(file!(), "created_instance", &output.render());
}

#[test]
fn rm_running_instance_fails() {
    let fixture = AgentFixture::new("rm-running-instance", valid_manifest());
    let create = fixture.create_instance(&["ssh:2222"]);

    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }
    fixture.mark_running();

    let output = fixture.rm();

    assert!(fixture.instance_dir().exists(), "{}", output.render());
    snapshot::assert(file!(), "running_instance_fails", &output.render());
}
