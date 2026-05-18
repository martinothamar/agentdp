pub fn assert(source_file: &str, name: &str, actual: &str) {
    agentdp_test_support::snapshot::assert(env!("CARGO_MANIFEST_DIR"), source_file, name, actual);
}

#[must_use]
pub fn render_command(status: i32, stdout: &str, stderr: &str) -> String {
    agentdp_test_support::snapshot::render_command(status, stdout, stderr)
}

#[must_use]
pub fn render_io(stdout: &str, stderr: &str) -> String {
    agentdp_test_support::snapshot::render_io(stdout, stderr)
}
