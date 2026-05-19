#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

mod support;

use support::{fixture::AgentFixture, manifest::valid_manifest, runtime, snapshot};

#[test]
fn clone_created_instance() {
    let fixture = AgentFixture::new("clone-created-instance", valid_manifest());
    let create = fixture.create_instance(&["ssh:2222"]);
    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }

    let output = fixture.clone_instance("pr-1");

    assert!(output.stdout().contains("cloned pr-0 -> altinn-studio/pr-1"));
    assert!(fixture.target_instance_file("pr-1", "runtime.json").is_file());
    assert!(fixture.target_instance_file("pr-1", "disk.qcow2").is_file());
    let state = runtime::read(&fixture.target_instance_file("pr-1", "runtime.json"));
    assert_eq!(state.instance, "pr-1");
    assert_ne!(state.network.ports["ssh"].host, 2222);
    assert!(state.qemu().disk.ends_with("altinn-studio/pr-1/disk.qcow2"));
    assert!(
        state
            .guest_access
            .as_ref()
            .expect("guest access")
            .private_key
            .ends_with("altinn-studio/pr-1/generated/qemu/ssh/agentdp_ed25519")
    );

    snapshot::assert(file!(), "created_instance", &output.render());
}

#[test]
fn clone_created_instance_with_port_override() {
    let fixture = AgentFixture::new("clone-created-instance-port", valid_manifest());
    let create = fixture.create_instance(&["ssh:2222"]);
    if create.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &create.render());
        return;
    }

    let output = fixture.clone_instance_with_ports("pr-1", &["code-server:4091"]);

    assert!(output.stdout().contains("cloned pr-0 -> altinn-studio/pr-1"));
    let state = runtime::read(&fixture.target_instance_file("pr-1", "runtime.json"));
    assert_eq!(state.network.ports["code-server"].host, 4091);
    assert_ne!(state.network.ports["ssh"].host, 2222);
}
