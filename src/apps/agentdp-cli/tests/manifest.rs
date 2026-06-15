use agentdp_test_support::cli::{command::TestContext, manifest::valid_manifest, snapshot};

#[test]
fn validate_explicit_file() {
    let context = TestContext::new("manifest-explicit");
    let manifest = context.write("valid.yaml", valid_manifest());
    snapshot::assert(
        file!(),
        "validate_explicit_file",
        &context
            .run(vec![
                "manifest".to_owned(),
                "validate".to_owned(),
                "-f".to_owned(),
                manifest.display().to_string(),
            ])
            .render(),
    );
}

#[test]
fn validate_default_agent_yaml() {
    let context = TestContext::new("manifest-agent-yaml");
    context.write("agent.yaml", valid_manifest());
    snapshot::assert(
        file!(),
        "validate_default_agent_yaml",
        &context.run_in(context.path(), ["manifest", "validate"]).render(),
    );
}

#[test]
fn validate_default_agent_yml() {
    let context = TestContext::new("manifest-agent-yml");
    context.write("agent.yml", valid_manifest());
    snapshot::assert(
        file!(),
        "validate_default_agent_yml",
        &context.run_in(context.path(), ["manifest", "validate"]).render(),
    );
}

#[test]
fn validate_missing_default_manifest() {
    let context = TestContext::new("manifest-missing");
    snapshot::assert(
        file!(),
        "validate_missing_default_manifest",
        &context.run_in(context.path(), ["manifest", "validate"]).render(),
    );
}

#[test]
fn validate_ambiguous_default_manifest() {
    let context = TestContext::new("manifest-ambiguous");
    context.write("agent.yaml", valid_manifest());
    context.write("agent.yml", valid_manifest());
    snapshot::assert(
        file!(),
        "validate_ambiguous_default_manifest",
        &context.run_in(context.path(), ["manifest", "validate"]).render(),
    );
}

#[test]
fn validate_invalid_manifest() {
    let context = TestContext::new("manifest-invalid");
    let manifest = context.write(
        "invalid.yaml",
        agentdp_test_support::manifest::invalid_absolute_repo_path(),
    );
    snapshot::assert(
        file!(),
        "validate_invalid_manifest",
        &context
            .run(vec![
                "manifest".to_owned(),
                "validate".to_owned(),
                "-f".to_owned(),
                manifest.display().to_string(),
            ])
            .render(),
    );
}

#[test]
fn validate_verbose() {
    let context = TestContext::new("manifest-verbose");
    let manifest = context.write("valid.yaml", valid_manifest());
    snapshot::assert(
        file!(),
        "validate_verbose",
        &context
            .run(vec![
                "-v".to_owned(),
                "manifest".to_owned(),
                "validate".to_owned(),
                "-f".to_owned(),
                manifest.display().to_string(),
            ])
            .render(),
    );
}
