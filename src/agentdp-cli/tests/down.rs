#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

mod support;

use std::path::PathBuf;

use support::{
    fixture::{AgentFixture, QemuSystemMode},
    manifest::no_healthcheck_manifest,
    snapshot,
};

#[test]
fn down_running_instance() {
    let fixture = AgentFixture::new("down-running-instance", no_healthcheck_manifest())
        .with_qemu_system(QemuSystemMode::SpawnSleep);
    let create = fixture.create_instance(&["ssh:2222"]);

    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }

    let up = fixture.up();
    assert!(up.stdout().contains("started altinn-studio/pr-0"), "{}", up.render());

    let output = fixture.down();

    let state = fixture.runtime_state();
    assert_eq!(state.status, "stopped");
    assert_eq!(state.qemu().pid, None);
    let pid_file = &state.qemu().pid_file;
    assert!(!PathBuf::from(pid_file.as_str()).exists(), "{}", output.render());
    snapshot::assert(file!(), "running_instance", &output.render());
}

#[test]
fn down_stale_running_instance() {
    let fixture = AgentFixture::new("down-stale-running-instance", no_healthcheck_manifest());
    let create = fixture.create_instance(&["ssh:2222"]);

    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }
    fixture.mark_running_with_missing_pid();

    let output = fixture.down();

    let state = fixture.runtime_state();
    assert_eq!(state.status, "stopped");
    assert_eq!(state.qemu().pid, None);
    snapshot::assert(file!(), "stale_running_instance", &output.render());
}
