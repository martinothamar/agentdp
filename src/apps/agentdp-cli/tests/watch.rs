#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use std::time::Duration;

use agentdp_test_support::cli::{fixture::AgentFixture, manifest::valid_manifest, snapshot};

#[test]
fn watch_streams_initial_agent_document() {
    let fixture = AgentFixture::new("watch-streams-initial-agent-document", valid_manifest());
    let apply = fixture.apply_agent();

    if apply.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &apply.render());
        return;
    }

    let observed = fixture.wait_observed();
    assert!(observed.stdout().contains("status: Satisfied"), "{}", observed.render());

    let output = fixture.watch_agent_json_for(Duration::from_millis(500));

    assert!(
        output.stdout().contains("\"name\":\"altinn-studio\""),
        "{}",
        output.render()
    );
    assert!(output.stdout().contains("\"generation\":1"), "{}", output.render());
    assert!(
        output.stdout().contains("\"observedGeneration\":1"),
        "{}",
        output.render()
    );
}
