use serde::Serialize;

use crate::manifest::{NetworkMode, User};
use crate::mediated_network::MediatedNetworkProfile;
use crate::provisioning::SeedFile;
use crate::provisioning::cloud_init::Error;

#[derive(Debug, Serialize)]
struct UserData<'a> {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    bootcmd: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    package_update: Option<bool>,
    #[serde(skip_serializing_if = "string_slice_is_empty")]
    packages: &'a [String],
    users: Vec<UserEntry<'a>>,
    write_files: Vec<WriteFile>,
    #[serde(skip_serializing_if = "string_slice_is_empty")]
    runcmd: &'a [String],
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
    #[serde(skip_serializing_if = "Option::is_none")]
    uid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_group: Option<&'a str>,
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

#[derive(Debug, Clone, Copy)]
pub(super) struct PackageConfig<'a> {
    pub(super) update: bool,
    pub(super) packages: &'a [String],
}

#[must_use]
pub fn render_network_config(mode: NetworkMode, mediated_network: MediatedNetworkProfile, ipv6: bool) -> String {
    match mode {
        NetworkMode::Mediated => render_mediated_network_config(mediated_network, ipv6),
        NetworkMode::User => render_user_network_config(),
    }
}

fn render_mediated_network_config(network: MediatedNetworkProfile, ipv6: bool) -> String {
    let guest_mac = network.guest_mac;
    let guest_ipv4 = network.guest_ipv4;
    let gateway_ipv4 = network.gateway_ipv4;
    let ipv4_cidr_prefix = network.ipv4_cidr_prefix;
    if ipv6 {
        let guest_ipv6 = network.guest_ipv6;
        let gateway_ipv6 = network.gateway_ipv6;
        let ipv6_cidr_prefix = network.ipv6_cidr_prefix;
        return format!(
            "\
version: 1
config:
  - type: physical
    name: eth0
    mac_address: '{guest_mac}'
    subnets:
      - type: static
        address: {guest_ipv4}/{ipv4_cidr_prefix}
        gateway: {gateway_ipv4}
        dns_nameservers:
          - {gateway_ipv4}
      - type: static
        address: {guest_ipv6}/{ipv6_cidr_prefix}
        gateway: {gateway_ipv6}
"
        );
    }
    format!(
        "\
version: 1
config:
  - type: physical
    name: eth0
    mac_address: '{guest_mac}'
    subnets:
      - type: static
        address: {guest_ipv4}/{ipv4_cidr_prefix}
        gateway: {gateway_ipv4}
        dns_nameservers:
          - {gateway_ipv4}
"
    )
}

fn render_user_network_config() -> String {
    "\
version: 1
config:
  - type: physical
    name: eth0
    subnets:
      - type: dhcp
"
    .to_owned()
}

pub(super) fn render_user_data(
    boot_commands: &[String],
    package_config: PackageConfig<'_>,
    user: &User,
    home: &str,
    seed_files: &[SeedFile],
    run_commands: &[String],
    ssh_authorized_key: Option<&str>,
) -> Result<String, Error> {
    let write_files = seed_files
        .iter()
        .map(|file| WriteFile {
            path: file.path.clone(),
            permissions: file.permissions.clone(),
            owner: file.owner.clone(),
            encoding: Some("b64"),
            content: encode_base64(&file.contents),
        })
        .collect();

    let user_data = UserData {
        bootcmd: boot_commands.to_vec(),
        package_update: package_config.update.then_some(true),
        packages: package_config.packages,
        users: vec![
            UserEntry::Default("default"),
            UserEntry::Agent(CloudUser {
                name: &user.name,
                uid: user.linux().uid,
                primary_group: user.linux().group.as_deref(),
                homedir: home,
                sudo: "ALL=(ALL) NOPASSWD:ALL",
                shell: "/bin/bash",
                ssh_authorized_keys: ssh_authorized_key.into_iter().collect(),
            }),
        ],
        write_files,
        runcmd: run_commands,
    };

    let body = serde_yaml::to_string(&user_data).map_err(Error::UserData)?;
    Ok(format!("#cloud-config\n{body}"))
}

const fn string_slice_is_empty(values: &&[String]) -> bool {
    values.is_empty()
}

fn encode_base64(input: &[u8]) -> String {
    let mut output = vec![0u8; agentdp_base64::encoded_len(input.len())];
    let Some(written) = agentdp_base64::encode(input, &mut output) else {
        unreachable!("base64 output was pre-sized")
    };
    output.truncate(written);
    let Ok(output) = String::from_utf8(output) else {
        unreachable!("base64 output is ASCII")
    };
    output
}
