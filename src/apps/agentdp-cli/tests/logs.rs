#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use std::time::Duration;

use agentdp_test_support::cli::{fixture::AgentFixture, manifest::valid_manifest, snapshot};

#[test]
fn logs_serial_tail() {
    let fixture = AgentFixture::new("logs-serial-tail", valid_manifest());
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }
    let observed = fixture.wait_observed();
    assert!(observed.stdout().contains("status: Satisfied"), "{}", observed.render());

    fixture.write_serial_log("first boot line\nsecond boot line\nthird boot line\n");

    let output = fixture.logs(false, Some(2));

    snapshot::assert(file!(), "serial_tail", &output.render());
}

#[test]
fn logs_network_events() {
    let fixture = AgentFixture::new("logs-network-events", valid_manifest());
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }
    let observed = fixture.wait_observed();
    assert!(observed.stdout().contains("status: Satisfied"), "{}", observed.render());
    let output =
        fixture.wait_network_logs_contains("lifecycle.state_changed state=traffic-observed", Duration::from_secs(2));
    assert!(
        output
            .stdout()
            .contains("lifecycle.state_changed state=traffic-observed"),
        "{}",
        output.render()
    );

    snapshot::assert(file!(), "network_events", &output.render());
}
