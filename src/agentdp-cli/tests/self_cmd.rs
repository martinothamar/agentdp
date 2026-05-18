mod support;

use support::{command::TestContext, snapshot};

#[test]
fn installs_to_temp_home() {
    let context = TestContext::new("self-install");
    let output = context.run_install_in_temp_home();
    assert!(context.installed_agentctl_path().is_file());
    snapshot::assert_topic("self_install", "installs_to_temp_home", &output.render());
    snapshot::assert_topic(
        "self_install",
        "installed_help",
        &context.run_installed(["--help"]).render(),
    );
}
