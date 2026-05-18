pub(super) const AGENT_ENV: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/provisioning/agent-env.sh"
));

pub(super) const AGENT_PROFILE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/provisioning/agent-profile.sh"
));

pub(super) const BOOTSTRAP_HELPERS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/provisioning/bootstrap-helpers.sh"
));

pub(super) const BOOTSTRAP_PREAMBLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/provisioning/bootstrap-preamble.sh"
));

pub(super) const CODE_SERVER_SERVICE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/provisioning/code-server.service"
));
