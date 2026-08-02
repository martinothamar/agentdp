#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use std::time::Duration;

use agentdp_test_support::cli::{
    fixture::{AgentFixture, QemuSystemMode},
    instance_state,
    manifest::{no_healthcheck_manifest, valid_manifest},
    snapshot,
};

#[test]
fn apply_dry_agent() {
    let fixture = AgentFixture::new("apply-dry-agent", valid_manifest());

    let output = fixture.apply_agent();

    if output.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &output.render());
        return;
    }

    assert!(output.stdout().contains("applied altinn-studio"), "{}", output.render());

    let observed = fixture.wait_observed();
    assert!(observed.stdout().contains("status: Satisfied"), "{}", observed.render());
    assert!(
        fixture.wait_guest_control_command("user_file.write", Duration::from_secs(2)),
        "fake guest did not receive a host-file command over the retained control session"
    );

    assert!(
        fixture.instance_file("instance.yaml").is_file(),
        "{}",
        observed.render()
    );
    assert!(fixture.instance_file("disk.qcow2").is_file(), "{}", observed.render());
    assert!(fixture.instance_file("seed.img").is_file(), "{}", observed.render());
    assert!(
        fixture.instance_file("seed/meta-data").is_file(),
        "{}",
        observed.render()
    );
    assert!(fixture.instance_file("logs").is_dir(), "{}", observed.render());
    assert!(
        fixture.instance_file("ssh/agentdp_ed25519").is_file(),
        "{}",
        observed.render()
    );
    assert!(
        fixture.instance_file("ssh/agentdp_ed25519.pub").is_file(),
        "{}",
        observed.render()
    );

    let state = instance_state::read(&fixture.instance_file("instance.yaml"));
    assert!(state.status.network.ports["ssh"].host.is_some());
    assert_eq!(state.status.network.ports["ssh"].guest, 22);
    let guest_access = state.status.guest_access.as_ref().expect("guest access");
    assert_eq!(guest_access.user, "agent");
    assert!(guest_access.private_key.ends_with("ssh/agentdp_ed25519"));
    assert!(
        instance_state::qemu(&state)
            .monitor_socket
            .ends_with("agents/altinn-studio/instances/0/run/monitor.sock")
    );

    snapshot::assert(file!(), "dry_instance", &output.render());
}

#[test]
fn apply_materializes_manifest_replicas() {
    let manifest = valid_manifest().replace("replicas: 1", "replicas: 2");
    let fixture = AgentFixture::new("apply-two-replicas", &manifest);

    let output = fixture.apply_agent();

    if output.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &output.render());
        return;
    }

    assert!(output.stdout().contains("replicas: 2"), "{}", output.render());

    let observed = fixture.wait_observed();
    assert!(observed.stdout().contains("status: Satisfied"), "{}", observed.render());
    assert!(
        fixture.instance_file("instance.yaml").is_file(),
        "{}",
        observed.render()
    );
    assert!(
        fixture.target_instance_file("1", "instance.yaml").is_file(),
        "{}",
        observed.render()
    );

    snapshot::assert(file!(), "two_replicas", &output.render());
}

#[test]
fn apply_wait_streams_startup_progress_without_snapshot_spam() {
    let fixture = AgentFixture::new("apply-wait-progress", no_healthcheck_manifest());

    let output = fixture.apply_agent_wait();

    if output.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &output.render());
        return;
    }

    assert!(output.stdout().contains("wait altinn-studio"), "{}", output.render());
    assert!(output.stdout().contains("status: Satisfied"), "{}", output.render());
    assert!(!output.stdout().contains("status altinn-studio"), "{}", output.render());

    snapshot::assert(file!(), "wait_progress", &output.render());
}

#[test]
fn apply_wait_for_paused_manifest_waits_until_stopped() {
    let running = no_healthcheck_manifest();
    let paused = running.replace("phase: Running", "phase: Paused");
    let fixture = AgentFixture::new("apply-wait-paused", running);

    let output = fixture.apply_agent_wait();
    if output.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &output.render());
        return;
    }
    assert!(output.stdout().contains("status: Satisfied"), "{}", output.render());

    fixture.update_manifest(&paused);
    let paused_output = fixture.apply_agent_wait();
    assert!(
        paused_output.stdout().contains("phase: Paused"),
        "{}",
        paused_output.render()
    );
    assert!(
        paused_output.stdout().contains("condition: Stopped"),
        "{}",
        paused_output.render()
    );
    assert!(
        paused_output.stdout().contains("status: Satisfied"),
        "{}",
        paused_output.render()
    );

    snapshot::assert(file!(), "wait_paused", &paused_output.render());
}

#[test]
fn apply_updates_desired_state_while_instance_transition_runs() {
    let running = no_healthcheck_manifest();
    let paused = running.replace("phase: Running", "phase: Paused");
    let fixture = AgentFixture::new("apply-pauses-during-start", running).with_qemu_system(QemuSystemMode::DelayStart);
    let output = fixture.apply_agent();

    if output.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &output.render());
        return;
    }

    let starting = fixture.wait_agent_status_contains("work:transition:start", Duration::from_secs(1));
    assert!(
        starting.stdout().contains("work:transition:start"),
        "{}",
        starting.render()
    );

    fixture.update_manifest(&paused);
    let paused_output = fixture.apply_agent();
    assert!(
        paused_output.stdout().contains("phase: Paused"),
        "{}",
        paused_output.render()
    );

    let status = fixture.agent_status();
    assert!(status.stdout().contains("phase: Paused"), "{}", status.render());
    assert!(status.stdout().contains("work:transition:start"), "{}", status.render());

    snapshot::assert(file!(), "pauses_during_start", &paused_output.render());
}
