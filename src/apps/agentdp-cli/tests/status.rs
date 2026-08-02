#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use std::time::Duration;

use agentdp_test_support::cli::{
    fixture::{AgentFixture, QemuSystemMode},
    manifest::valid_manifest,
    snapshot,
};

#[test]
fn status_bootstrapping_instance() {
    let fixture = AgentFixture::new("status-starting-instance-after-observed", valid_manifest())
        .with_qemu_system(QemuSystemMode::DelayBootstrapFinished);
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }
    let output = fixture.wait_status_contains("work: bootstrap", Duration::from_secs(2));
    assert!(output.stdout().contains("work: bootstrap"), "{}", output.render());
    assert!(output.stdout().contains("network_runtime:"), "{}", output.render());

    snapshot::assert(file!(), "bootstrapping_instance", &output.render());
}
