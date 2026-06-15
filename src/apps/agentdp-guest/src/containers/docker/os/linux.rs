use crate::containers::core::os::CliConfig;

pub(super) const CONFIG: CliConfig = CliConfig::new("/usr/bin/docker", "/var/lib/agentdp/ca", Some("docker"));
