pub fn assert(source_file: &str, name: &str, actual: &str) {
    crate::snapshot::assert(&cli_manifest_dir(), source_file, name, actual);
}

pub fn assert_topic(topic: &str, name: &str, actual: &str) {
    crate::snapshot::assert_topic(&cli_manifest_dir(), topic, name, actual);
}

#[must_use]
pub fn render_command(status: i32, stdout: &str, stderr: &str) -> String {
    crate::snapshot::render_command(status, stdout, stderr)
}

#[must_use]
pub fn render_io(stdout: &str, stderr: &str) -> String {
    crate::snapshot::render_io(stdout, stderr)
}

fn cli_manifest_dir() -> String {
    super::command::repo_root_for_support()
        .join("src/apps/agentdp-cli")
        .display()
        .to_string()
}
