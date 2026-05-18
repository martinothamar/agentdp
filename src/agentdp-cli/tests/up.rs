#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

mod support;

use support::{
    fixture::{AgentFixture, QemuSystemMode},
    manifest::no_healthcheck_manifest,
    snapshot,
};

#[test]
fn up_created_instance() {
    let fixture =
        AgentFixture::new("up-created-instance", no_healthcheck_manifest()).with_qemu_system(QemuSystemMode::StaticPid);
    let create = fixture.create_instance(&["ssh:2222", "code-server:24090"]);

    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }

    let output = fixture.up();

    assert!(
        output.stdout().contains("started altinn-studio/pr-0"),
        "{}",
        output.render()
    );
    let runtime_state = fixture.runtime_state();
    assert_eq!(runtime_state.status, "running");
    assert_eq!(runtime_state.qemu().pid, Some(4242));
    snapshot::assert(file!(), "created_instance", &output.render());
}
