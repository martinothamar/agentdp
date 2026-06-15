use serde::{Deserialize, Serialize};

pub const GUEST_INSTANCE_SPEC_VERSION: u16 = 1;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GuestInstanceSpec {
    pub schema_version: u16,
    pub manifest: String,
    pub instance: String,
    pub hostname: String,
    pub platform: GuestPlatform,
    pub user: GuestInstanceUser,
    pub paths: GuestInstancePaths,
}

impl GuestInstanceSpec {
    #[must_use]
    pub fn plan_id(&self) -> String {
        format!("{}/{}", self.manifest, self.instance)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GuestPlatform {
    Linux,
    Macos,
    Windows,
}

impl GuestPlatform {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Macos => "macos",
            Self::Windows => "windows",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GuestInstanceUser {
    pub name: String,
    pub home: String,
    pub code_dir: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GuestInstancePaths {
    pub spec_dir: String,
    pub instance_spec: String,
    pub manifest: String,
    pub bootstrap_plan: String,
    pub bootstrap_root: String,
    pub bootstrap_state: String,
    pub control: String,
}

#[cfg(test)]
mod tests {
    use super::{GUEST_INSTANCE_SPEC_VERSION, GuestInstancePaths, GuestInstanceSpec, GuestInstanceUser, GuestPlatform};

    #[test]
    fn instance_spec_round_trips_nested_paths() {
        let spec = GuestInstanceSpec {
            schema_version: GUEST_INSTANCE_SPEC_VERSION,
            manifest: "basic".to_owned(),
            instance: "basic-0".to_owned(),
            hostname: "basic-0".to_owned(),
            platform: GuestPlatform::Linux,
            user: GuestInstanceUser {
                name: "agent".to_owned(),
                home: "/data/home".to_owned(),
                code_dir: "/data/home/code".to_owned(),
            },
            paths: GuestInstancePaths {
                spec_dir: "/var/lib/agentdp/spec".to_owned(),
                instance_spec: "/var/lib/agentdp/spec/instance.json".to_owned(),
                manifest: "/var/lib/agentdp/spec/agent-manifest.yaml".to_owned(),
                bootstrap_plan: "/var/lib/agentdp/spec/bootstrap-plan.json".to_owned(),
                bootstrap_root: "/var/lib/agentdp/bootstrap".to_owned(),
                bootstrap_state: "/var/lib/agentdp/bootstrap-state.json".to_owned(),
                control: "/dev/virtio-ports/agentdp.control".to_owned(),
            },
        };

        let encoded = serde_json::to_string(&spec).expect("serialize spec");
        let decoded: GuestInstanceSpec = serde_json::from_str(&encoded).expect("deserialize spec");

        assert_eq!(decoded, spec);
        assert_eq!(decoded.plan_id(), "basic/basic-0");
        assert_eq!(decoded.platform.as_str(), "linux");
    }
}
