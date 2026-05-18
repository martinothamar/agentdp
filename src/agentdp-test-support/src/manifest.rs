macro_rules! manifest {
    ($function:ident, $file:literal) => {
        #[must_use]
        pub const fn $function() -> &'static str {
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/resources/manifests/",
                $file
            ))
        }
    };
}

manifest!(standard, "standard.yaml");
manifest!(minimal, "minimal.yaml");
manifest!(no_healthchecks, "no-healthchecks.yaml");
manifest!(invalid_absolute_repo_path, "invalid-absolute-repo-path.yaml");
manifest!(network_allow_all, "network-allow-all.yaml");
manifest!(network_allow_hosts, "network-allow-hosts.yaml");
manifest!(healthcheck_code_server, "healthcheck-code-server.yaml");
manifest!(healthcheck_ssh, "healthcheck-ssh.yaml");
manifest!(healthcheck_command, "healthcheck-command.yaml");
