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

pub(super) const AGENTDP_CODEX_SESSION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/provisioning/agentdp-codex-session.sh"
));

pub(super) const AGENTDP_PR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/provisioning/agentdp-pr.js"
));

pub(super) const AGENTDP_PR_SUBSCRIBER: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/provisioning/agentdp-pr-subscriber.js"
));

pub(super) const AGENTDP_PR_SUBSCRIBER_SERVICE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/provisioning/agentdp-pr-subscriber.service"
));

pub(super) const AGENTDP_CODEX_SESSION_SERVICE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/provisioning/agentdp-codex-session.service"
));
