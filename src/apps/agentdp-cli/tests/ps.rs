#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use std::time::Duration;

use agentdp_test_support::cli::{
    fixture::{AgentFixture, QemuSystemMode},
    manifest::valid_manifest,
    snapshot,
};

#[test]
fn ps_lists_bootstrapping_instance() {
    let fixture = AgentFixture::new("ps-lists-starting-instance-after-observed", valid_manifest())
        .with_qemu_system(QemuSystemMode::DelayBootstrapFinished);
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }
    let bootstrap = fixture.wait_agent_status_contains("work:bootstrap", Duration::from_secs(2));
    assert!(bootstrap.stdout().contains("work:bootstrap"), "{}", bootstrap.render());

    let output = fixture.ps();

    snapshot::assert(file!(), "lists_bootstrapping_instance", &output.render());
}
