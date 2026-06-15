use agentdp_protocol::server_guest::GuestInstancePaths;

use super::ca_bundle;
use crate::provisioning::guest_os::GuestLayout;

pub(in crate::provisioning::guest_os) const AGENT_HOME: &str = "/data/home";
pub(in crate::provisioning::guest_os) const CODE_DIR: &str = "/data/home/code";
pub(in crate::provisioning::guest_os) const CUSTOM_BOOTSTRAP_PATH: &str =
    "/var/lib/agentdp/bootstrap/custom-bootstrap.sh";
pub(in crate::provisioning::guest_os) const CUSTOM_ENV_PATH: &str = "/run/agentdp/.env";
pub(in crate::provisioning::guest_os) const PERSISTENT_CUSTOM_ENV_PATH: &str = "/etc/agentdp/.env";
pub(in crate::provisioning::guest_os) const GUESTD_SYSTEM_SERVICE_PATH: &str =
    "/etc/systemd/system/guestd-system.service";
pub(in crate::provisioning) const USR_LOCAL_PREFIX: &str = "/usr/local";
pub(in crate::provisioning) const USR_LOCAL_BIN: &str = "/usr/local/bin";
pub(in crate::provisioning) const AGENTDP_LIB_DIR: &str = "/usr/local/lib/agentdp";
pub(in crate::provisioning) const AGENT_SHELL_ENV_PATH: &str = "/usr/local/lib/agentdp/env.sh";
pub(in crate::provisioning) const AGENT_ENV_PATH: &str = "/usr/local/bin/agentdp-agent-env";
pub(in crate::provisioning) const GUESTD_PATH: &str = "/usr/local/bin/guestd";
pub(in crate::provisioning) const GUESTCTL_PATH: &str = "/usr/local/bin/guestctl";

const GUEST_SPEC_DIR: &str = "/var/lib/agentdp/spec";
const GUEST_INSTANCE_SPEC_PATH: &str = "/var/lib/agentdp/spec/instance.json";
const GUEST_MANIFEST_SPEC_PATH: &str = "/var/lib/agentdp/spec/agent-manifest.yaml";
const GUEST_BOOTSTRAP_PLAN_SPEC_PATH: &str = "/var/lib/agentdp/spec/bootstrap-plan.json";
const GUEST_BOOTSTRAP_ROOT: &str = "/var/lib/agentdp/bootstrap";
const GUEST_BOOTSTRAP_STATE_PATH: &str = "/var/lib/agentdp/bootstrap-state.json";
const GUEST_CONTROL_PATH: &str = "/dev/virtio-ports/agentdp.control";

pub(super) const fn guest_layout() -> GuestLayout {
    GuestLayout {
        agent_home: AGENT_HOME,
        code_dir: CODE_DIR,
        custom_bootstrap: CUSTOM_BOOTSTRAP_PATH,
        runtime_env: CUSTOM_ENV_PATH,
        persistent_env: PERSISTENT_CUSTOM_ENV_PATH,
        ca_bundle: ca_bundle::CA_BUNDLE_PATH,
    }
}

pub(super) fn guest_instance_paths() -> GuestInstancePaths {
    GuestInstancePaths {
        spec_dir: GUEST_SPEC_DIR.to_owned(),
        instance_spec: GUEST_INSTANCE_SPEC_PATH.to_owned(),
        manifest: GUEST_MANIFEST_SPEC_PATH.to_owned(),
        bootstrap_plan: GUEST_BOOTSTRAP_PLAN_SPEC_PATH.to_owned(),
        bootstrap_root: GUEST_BOOTSTRAP_ROOT.to_owned(),
        bootstrap_state: GUEST_BOOTSTRAP_STATE_PATH.to_owned(),
        control: GUEST_CONTROL_PATH.to_owned(),
    }
}
