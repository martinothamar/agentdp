#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use agentdp_test_support::cli::{fixture::AgentFixture, manifest::valid_manifest, snapshot};

#[test]
fn delete_materialized_agent_leaves_tombstone_status() {
    let fixture = AgentFixture::new("delete-materialized-agent", valid_manifest());
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }
    let observed = fixture.wait_observed();
    assert!(observed.stdout().contains("status: Satisfied"), "{}", observed.render());

    let deleted = fixture.delete_agent();
    assert!(
        deleted.stdout().contains("delete altinn-studio"),
        "{}",
        deleted.render()
    );
    assert!(deleted.stdout().contains("phase: Deleting"), "{}", deleted.render());

    let wait = fixture.wait_deleted();
    assert!(wait.stdout().contains("condition: Deleted"), "{}", wait.render());
    assert!(wait.stdout().contains("status: Satisfied"), "{}", wait.render());
    assert!(
        !fixture.instance_dir().exists(),
        "deleted instance directory still exists: {}",
        fixture.instance_dir().display()
    );

    let status = fixture.agent_status();
    assert!(status.stdout().contains("phase: Deleted"), "{}", status.render());
    assert!(status.stdout().contains("deleted: true"), "{}", status.render());
    assert!(status.stdout().contains("instances: none"), "{}", status.render());

    snapshot::assert(file!(), "materialized_agent", &status.render());
}
