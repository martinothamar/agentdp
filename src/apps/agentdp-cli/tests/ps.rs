#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use agentdp_test_support::cli::{
    fixture::{AgentFixture, QemuSystemMode},
    manifest::valid_manifest,
    snapshot,
};

#[test]
fn ps_lists_starting_instance_after_observed() {
    let fixture = AgentFixture::new("ps-lists-starting-instance-after-observed", valid_manifest())
        .with_qemu_system(QemuSystemMode::DelayBootstrapFinished);
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }
    let observed = fixture.wait_observed();
    assert!(observed.stdout().contains("status: Satisfied"), "{}", observed.render());

    let output = fixture.ps();

    snapshot::assert(file!(), "lists_starting_instance_after_observed", &output.render());
}
