use crate::manifest::AgentManifest;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Serialize;
use thiserror::Error;

use super::{BootstrapPlan, SeedFile};

const BOOTSTRAP_SCRIPT_PATH: &str = "/opt/agentdp/bootstrap.sh";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudInitSeed {
    pub meta_data: String,
    pub user_data: String,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to render cloud-init meta-data: {0}")]
    MetaData(#[source] serde_yaml::Error),
    #[error("failed to render cloud-init user-data: {0}")]
    UserData(#[source] serde_yaml::Error),
}

impl CloudInitSeed {
    pub(crate) fn from_plan(
        manifest: &AgentManifest,
        bootstrap: &BootstrapPlan,
        options: &super::ProvisioningOptions,
    ) -> Result<Self, Error> {
        let hostname = options.hostname.as_deref().unwrap_or(&manifest.name);
        Ok(Self {
            meta_data: render_meta_data(&manifest.name, hostname)?,
            user_data: render_user_data(
                &bootstrap.packages,
                &bootstrap.user,
                &bootstrap.script,
                &options.seed_files,
                options.ssh_authorized_key.as_deref(),
            )?,
        })
    }
}

#[derive(Debug, Serialize)]
struct MetaData<'a> {
    #[serde(rename = "instance-id")]
    instance_id: &'a str,
    #[serde(rename = "local-hostname")]
    local_hostname: String,
}

#[derive(Debug, Serialize)]
struct UserData<'a> {
    package_update: bool,
    packages: &'a [String],
    users: Vec<UserEntry<'a>>,
    write_files: Vec<WriteFile>,
    runcmd: [[&'a str; 2]; 1],
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum UserEntry<'a> {
    Default(&'a str),
    Agent(CloudUser<'a>),
}

#[derive(Debug, Serialize)]
struct CloudUser<'a> {
    name: &'a str,
    homedir: &'a str,
    sudo: &'a str,
    shell: &'a str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    ssh_authorized_keys: Vec<&'a str>,
}

#[derive(Debug, Serialize)]
struct WriteFile {
    path: String,
    permissions: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    encoding: Option<&'static str>,
    content: String,
}

/// Renders cloud-init `NoCloud` metadata for an instance.
///
/// # Errors
///
/// Returns an error when the metadata cannot be serialized as YAML.
pub fn render_meta_data(instance_id: &str, hostname: &str) -> Result<String, Error> {
    let meta_data = MetaData {
        instance_id,
        local_hostname: hostname_from_name(hostname),
    };
    serde_yaml::to_string(&meta_data).map_err(Error::MetaData)
}

fn render_user_data(
    packages: &[String],
    user: &super::bootstrap::AgentUserPlan,
    bootstrap_script: &str,
    seed_files: &[SeedFile],
    ssh_authorized_key: Option<&str>,
) -> Result<String, Error> {
    let mut write_files = vec![WriteFile {
        path: BOOTSTRAP_SCRIPT_PATH.to_owned(),
        permissions: "0755".to_owned(),
        owner: None,
        encoding: None,
        content: bootstrap_script.to_owned(),
    }];
    write_files.extend(seed_files.iter().map(|file| WriteFile {
        path: file.path.clone(),
        permissions: file.permissions.clone(),
        owner: file.owner.clone(),
        encoding: Some("b64"),
        content: BASE64.encode(&file.contents),
    }));

    let user_data = UserData {
        package_update: true,
        packages,
        users: vec![
            UserEntry::Default("default"),
            UserEntry::Agent(CloudUser {
                name: &user.name,
                homedir: &user.home,
                sudo: "ALL=(ALL) NOPASSWD:ALL",
                shell: "/bin/bash",
                ssh_authorized_keys: ssh_authorized_key.into_iter().collect(),
            }),
        ],
        write_files,
        runcmd: [["bash", BOOTSTRAP_SCRIPT_PATH]],
    };

    let body = serde_yaml::to_string(&user_data).map_err(Error::UserData)?;
    Ok(format!("#cloud-config\n{body}"))
}

fn hostname_from_name(name: &str) -> String {
    name.bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || byte == b'-' {
                char::from(byte)
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_yaml::Value;

    use crate::provisioning::SeedFile;
    use crate::provisioning::bootstrap::AgentUserPlan;
    use crate::provisioning::cloud_init::{BOOTSTRAP_SCRIPT_PATH, render_meta_data, render_user_data};

    #[test]
    fn meta_data_is_structured_yaml() {
        let meta_data = render_meta_data("agent.example_1", "pr_0").unwrap();
        let parsed = serde_yaml::from_str::<Value>(&meta_data).unwrap();

        assert_eq!(parsed["instance-id"], Value::from("agent.example_1"));
        assert_eq!(parsed["local-hostname"], Value::from("pr-0"));
    }

    #[test]
    fn user_data_is_cloud_config_yaml() {
        let packages = vec!["git".to_owned(), "package:with:symbols".to_owned()];
        let script = "#!/usr/bin/env bash\nprintf '%s\\n' \"hello: world\"\n";
        let user_data = render_user_data(&packages, &agent_user(), script, &[], None).unwrap();

        assert!(user_data.starts_with("#cloud-config\n"));
        let parsed = parse_user_data(&user_data);
        assert_eq!(parsed["package_update"], Value::from(true));
        assert_eq!(parsed["packages"][0], Value::from("git"));
        assert_eq!(parsed["packages"][1], Value::from("package:with:symbols"));
        assert_eq!(parsed["users"][0], Value::from("default"));
        assert_eq!(parsed["users"][1]["name"], Value::from("agent"));
        assert_eq!(parsed["users"][1]["sudo"], Value::from("ALL=(ALL) NOPASSWD:ALL"));
        assert_eq!(parsed["users"][1]["shell"], Value::from("/bin/bash"));
        assert_eq!(parsed["write_files"][0]["path"], Value::from(BOOTSTRAP_SCRIPT_PATH));
        assert_eq!(parsed["write_files"][0]["permissions"], Value::from("0755"));
        assert_eq!(parsed["write_files"][0]["content"], Value::from(script));
        assert_eq!(parsed["runcmd"][0][0], Value::from("bash"));
        assert_eq!(parsed["runcmd"][0][1], Value::from(BOOTSTRAP_SCRIPT_PATH));
    }

    #[test]
    fn empty_package_list_stays_an_empty_yaml_sequence() {
        let user_data = render_user_data(&[], &agent_user(), "true\n", &[], None).unwrap();
        let parsed = parse_user_data(&user_data);

        assert_eq!(parsed["packages"].as_sequence().unwrap().len(), 0);
    }

    #[test]
    fn user_data_can_authorize_ssh_key_for_default_user() {
        let user_data = render_user_data(
            &[],
            &agent_user(),
            "true\n",
            &[],
            Some("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITest agentdp"),
        )
        .unwrap();
        let parsed = parse_user_data(&user_data);

        assert_eq!(
            parsed["users"][1]["ssh_authorized_keys"][0],
            Value::from("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITest agentdp")
        );
    }

    #[test]
    fn user_data_writes_seed_files_as_base64() {
        let seed_files = [SeedFile {
            path: "/data/home/.codex/AGENTS.md".to_owned(),
            contents: b"agent instructions\n".to_vec(),
            permissions: "0644".to_owned(),
            owner: None,
        }];
        let user_data = render_user_data(&[], &agent_user(), "true\n", &seed_files, None).unwrap();

        let parsed = parse_user_data(&user_data);
        assert_eq!(
            parsed["write_files"][1]["path"],
            Value::from("/data/home/.codex/AGENTS.md")
        );
        assert_eq!(parsed["write_files"][1]["permissions"], Value::from("0644"));
        assert!(parsed["write_files"][1].get("owner").is_none());
        assert_eq!(parsed["write_files"][1]["encoding"], Value::from("b64"));
        assert_eq!(
            parsed["write_files"][1]["content"],
            Value::from("YWdlbnQgaW5zdHJ1Y3Rpb25zCg==")
        );
    }

    fn parse_user_data(user_data: &str) -> Value {
        serde_yaml::from_str(user_data.strip_prefix("#cloud-config\n").expect("cloud-config header"))
            .expect("parse user-data")
    }

    fn agent_user() -> AgentUserPlan {
        AgentUserPlan {
            name: "agent".to_owned(),
            home: "/data/home".to_owned(),
            groups: vec!["docker".to_owned()],
        }
    }
}
