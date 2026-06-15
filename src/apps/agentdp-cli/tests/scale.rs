#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use agentdp_test_support::cli::{fixture::AgentFixture, manifest::no_healthcheck_manifest, snapshot};

#[test]
fn scale_stops_without_deleting_and_scales_back_up() {
    let fixture = AgentFixture::new("scale-stops-without-deleting", no_healthcheck_manifest());
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }

    let ready = fixture.wait_ready();
    assert!(ready.stdout().contains("status: Satisfied"), "{}", ready.render());

    let scaled_down = fixture.scale_agent(0, true);
    assert!(
        scaled_down.stdout().contains("scaled altinn-studio"),
        "{}",
        scaled_down.render()
    );
    assert!(scaled_down.stdout().contains("replicas: 0"), "{}", scaled_down.render());
    assert!(
        scaled_down.stdout().contains("condition: Stopped"),
        "{}",
        scaled_down.render()
    );
    assert!(
        scaled_down.stdout().contains("status: Satisfied"),
        "{}",
        scaled_down.render()
    );

    let stopped_instances = fixture.ps();
    assert!(
        stopped_instances.stdout().contains("altinn-studio/0: stopped pid:none"),
        "{}",
        stopped_instances.render()
    );

    let stopped_status = fixture.agent_status();
    assert!(
        stopped_status.stdout().contains("replicas: 0"),
        "{}",
        stopped_status.render()
    );
    assert!(
        stopped_status.stdout().contains("active replicas: 0"),
        "{}",
        stopped_status.render()
    );
    assert!(
        stopped_status.stdout().contains("deleted: false"),
        "{}",
        stopped_status.render()
    );

    let scaled_up = fixture.scale_agent(2, false);
    assert!(
        scaled_up.stdout().contains("scaled altinn-studio"),
        "{}",
        scaled_up.render()
    );
    assert!(scaled_up.stdout().contains("replicas: 2"), "{}", scaled_up.render());

    let ready = fixture.wait_ready();
    assert!(ready.stdout().contains("status: Satisfied"), "{}", ready.render());

    let ready_status = fixture.agent_status();
    assert!(
        ready_status.stdout().contains("replicas: 2"),
        "{}",
        ready_status.render()
    );
    assert!(
        ready_status.stdout().contains("ready replicas: 2"),
        "{}",
        ready_status.render()
    );
    assert!(
        ready_status.stdout().contains("active replicas: 2"),
        "{}",
        ready_status.render()
    );
    assert!(
        fixture.target_instance_file("1", "instance.yaml").is_file(),
        "{}",
        ready_status.render()
    );

    snapshot::assert(file!(), "zero_replicas", &scaled_down.render());
}
