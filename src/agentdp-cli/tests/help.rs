mod support;

use support::{command::TestContext, snapshot};

#[test]
fn root_help() {
    let context = TestContext::new("help-root");
    snapshot::assert(file!(), "root", &context.run(["--help"]).render());
}

#[test]
fn manifest_help() {
    let context = TestContext::new("help-manifest");
    snapshot::assert(file!(), "manifest", &context.run(["manifest", "--help"]).render());
}

#[test]
fn manifest_validate_help() {
    let context = TestContext::new("help-manifest-validate");
    snapshot::assert(
        file!(),
        "manifest_validate",
        &context.run(["manifest", "validate", "--help"]).render(),
    );
}

#[test]
fn self_help() {
    let context = TestContext::new("help-self");
    snapshot::assert(file!(), "self", &context.run(["self", "--help"]).render());
}

#[test]
fn self_install_help() {
    let context = TestContext::new("help-self-install");
    snapshot::assert(
        file!(),
        "self_install",
        &context.run(["self", "install", "--help"]).render(),
    );
}

#[test]
fn rm_help() {
    let context = TestContext::new("help-rm");
    snapshot::assert(file!(), "rm", &context.run(["rm", "--help"]).render());
}

#[test]
fn status_help() {
    let context = TestContext::new("help-status");
    snapshot::assert(file!(), "status", &context.run(["status", "--help"]).render());
}

#[test]
fn up_help() {
    let context = TestContext::new("help-up");
    snapshot::assert(file!(), "up", &context.run(["up", "--help"]).render());
}

#[test]
fn down_help() {
    let context = TestContext::new("help-down");
    snapshot::assert(file!(), "down", &context.run(["down", "--help"]).render());
}

#[test]
fn exec_help() {
    let context = TestContext::new("help-exec");
    snapshot::assert(file!(), "exec", &context.run(["exec", "--help"]).render());
}

#[test]
fn logs_help() {
    let context = TestContext::new("help-logs");
    snapshot::assert(file!(), "logs", &context.run(["logs", "--help"]).render());
}

#[test]
fn ps_help() {
    let context = TestContext::new("help-ps");
    snapshot::assert(file!(), "ps", &context.run(["ps", "--help"]).render());
}

#[test]
fn shell_help() {
    let context = TestContext::new("help-shell");
    snapshot::assert(file!(), "shell", &context.run(["shell", "--help"]).render());
}
