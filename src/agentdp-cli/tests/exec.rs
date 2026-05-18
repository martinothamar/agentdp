#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

mod support;

use support::{
    fixture::{AgentFixture, QemuSystemMode},
    manifest::no_healthcheck_manifest,
    snapshot,
};

#[test]
fn exec_runs_guest_command() {
    let fixture = AgentFixture::new("exec-runs-guest-command", no_healthcheck_manifest())
        .with_qemu_system(QemuSystemMode::StaticPid);
    let create = fixture.create_instance(&["ssh:2222"]);

    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }

    let up = fixture.up();
    assert!(up.stdout().contains("started altinn-studio/pr-0"), "{}", up.render());

    let output = fixture.exec(&["printf", "%s\\n", "hello"]);

    snapshot::assert(file!(), "runs_guest_command", &output.render());
}
