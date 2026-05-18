#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

mod support;

use support::{fixture::AgentFixture, manifest::valid_manifest, runtime, snapshot};

#[test]
fn create_dry_instance() {
    let fixture = AgentFixture::new("create-dry-instance", valid_manifest());

    let output = fixture.create_instance(&["ssh:2222"]);

    if output.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &output.render());
        return;
    }

    assert!(
        output.stdout().contains("created altinn-studio/pr-0"),
        "{}",
        output.render()
    );
    assert!(fixture.instance_file("runtime.json").is_file(), "{}", output.render());
    assert!(fixture.instance_file("disk.qcow2").is_file(), "{}", output.render());
    assert!(
        fixture.instance_file("generated/qemu/seed.img").is_file(),
        "{}",
        output.render()
    );
    assert!(
        fixture.instance_file("generated/qemu/seed/meta-data").is_file(),
        "{}",
        output.render()
    );
    assert!(fixture.instance_file("logs").is_dir(), "{}", output.render());
    assert!(
        fixture.instance_file("generated/qemu/ssh/agentdp_ed25519").is_file(),
        "{}",
        output.render()
    );
    assert!(
        fixture
            .instance_file("generated/qemu/ssh/agentdp_ed25519.pub")
            .is_file(),
        "{}",
        output.render()
    );

    let runtime = runtime::read(&fixture.instance_file("runtime.json"));
    assert_eq!(runtime.network.ports["ssh"].host, 2222);
    assert_eq!(runtime.network.ports["ssh"].guest, 22);
    let guest_access = runtime.guest_access.as_ref().expect("guest access");
    assert_eq!(guest_access.user, "agent");
    assert!(guest_access.private_key.ends_with("generated/qemu/ssh/agentdp_ed25519"));
    assert!(
        runtime
            .qemu()
            .monitor_socket
            .ends_with("agentdp/instances/altinn-studio/pr-0/qemu/monitor.sock")
    );

    snapshot::assert(file!(), "dry_instance", &output.render());
}
