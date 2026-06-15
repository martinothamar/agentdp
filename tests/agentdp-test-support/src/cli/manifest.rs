pub const fn valid_manifest() -> &'static str {
    crate::manifest::standard()
}

pub const fn no_healthcheck_manifest() -> &'static str {
    crate::manifest::no_healthchecks()
}

pub fn no_healthcheck_user_network_manifest() -> String {
    no_healthcheck_manifest().replace("  mode: mediated\n", "  mode: user\n")
}
