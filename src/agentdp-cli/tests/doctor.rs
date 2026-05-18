#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

mod support;

use support::{command::TestContext, fixture::ServerFixture, snapshot};

#[test]
fn doctor_starts_agentdp_server() {
    let context = TestContext::new("doctor-server");
    let server = ServerFixture::new(&context);

    let output = server.run_doctor(&context);
    let rendered = snapshot::render_io(&doctor_server_lines(output.stdout()), output.stderr());

    if output.socket_permission_denied() {
        snapshot::assert(file!(), "local_socket_permission_denied", &rendered);
        return;
    }

    assert!(output.stdout().contains("agentdp-server"), "{}", output.render());
    assert!(
        output.stdout().contains("responded to server.ping"),
        "{}",
        output.render()
    );
    assert!(
        output.stdout().contains("$TMP/runtime/agentdp/agentdp-server.sock"),
        "{}",
        output.render()
    );

    snapshot::assert(file!(), "starts_agentdp_server", &rendered);
}

fn doctor_server_lines(stdout: &str) -> String {
    let lines = stdout
        .lines()
        .filter(|line| line.contains("agentdp-server") || line.starts_with("socket"))
        .collect::<Vec<_>>();
    let mut output = lines.join("\n");
    output.push('\n');
    output
}
