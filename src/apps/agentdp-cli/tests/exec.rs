#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use std::time::Duration;

use agentdp_test_support::cli::{
    fixture::{AgentFixture, QemuSystemMode},
    manifest::no_healthcheck_manifest,
    snapshot,
};

#[test]
fn exec_runs_guest_command() {
    let fixture = AgentFixture::new("exec-runs-guest-command", no_healthcheck_manifest())
        .with_qemu_system(QemuSystemMode::SpawnSleep);
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }

    let ready = fixture.wait_ready();
    assert!(ready.stdout().contains("status: Satisfied"), "{}", ready.render());

    let output = fixture.exec(&["printf", "%s\\n", "hello"]);

    snapshot::assert(file!(), "runs_guest_command", &output.render());
}

#[test]
fn exec_runs_while_bootstrap_setup_is_pending() {
    let fixture = AgentFixture::new("exec-runs-during-bootstrap", no_healthcheck_manifest())
        .with_qemu_system(QemuSystemMode::DelayBootstrapFinished);
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }

    let observed = fixture.wait_observed_with_timeout(5);
    assert!(observed.stdout().contains("status: Satisfied"), "{}", observed.render());

    let bootstrap = fixture.wait_agent_status_contains("work:bootstrap", Duration::from_secs(2));
    assert!(bootstrap.stdout().contains("work:bootstrap"), "{}", bootstrap.render());

    let output = fixture.exec(&["printf", "%s\\n", "hello"]);
    assert!(output.stdout().contains("hello from guest"), "{}", output.render());

    snapshot::assert(file!(), "runs_while_bootstrap_setup_is_pending", &output.render());
}
