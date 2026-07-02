#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use std::time::Duration;

use agentdp_test_support::cli::{
    fixture::{AgentFixture, QemuSystemMode},
    manifest::valid_manifest,
    snapshot,
};

#[test]
fn status_starting_instance_after_observed() {
    let fixture = AgentFixture::new("status-starting-instance-after-observed", valid_manifest())
        .with_qemu_system(QemuSystemMode::DelayBootstrapFinished);
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }
    let observed = fixture.wait_observed();
    assert!(observed.stdout().contains("status: Satisfied"), "{}", observed.render());

    let output = fixture.wait_status_contains("network_runtime:", Duration::from_secs(2));
    assert!(output.stdout().contains("network_runtime:"), "{}", output.render());

    snapshot::assert(file!(), "starting_instance_after_observed", &output.render());
}
