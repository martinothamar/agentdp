use crate::containers::core::os::CliConfig;

pub(super) const CONFIG: CliConfig = CliConfig::new("/usr/bin/podman", "/var/lib/agentdp/ca", Some("podman"));
