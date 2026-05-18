pub const fn valid_manifest() -> &'static str {
    agentdp_test_support::manifest::standard()
}

pub const fn no_healthcheck_manifest() -> &'static str {
    agentdp_test_support::manifest::no_healthchecks()
}
