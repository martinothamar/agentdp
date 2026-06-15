#![no_main]

use agentdp_core::manifest::AgentManifest;
use agentdp_core::provisioning::cloud_init::CloudInitSeed;
use agentdp_core::provisioning::guest_os::linux::cloud_init::CloudInitOptions;
use agentdp_core::provisioning::{ProvisioningOptions, ProvisioningPlan};
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Arbitrary)]
struct Input {
    name: String,
    user: String,
    packages: Vec<String>,
    shell: Vec<String>,
    port_name: String,
    port: u16,
    cpus: u16,
    memory_gb: u16,
    storage_gb: u16,
}

fuzz_target!(|input: Input| {
    let contents = input.manifest_yaml();
    let Ok(manifest) = serde_yaml::from_str::<AgentManifest>(&contents) else {
        return;
    };
    if manifest.validate().is_err() {
        return;
    }

    let plan = ProvisioningPlan::from_manifest(&manifest, &ProvisioningOptions::default());
    let seed = CloudInitSeed::from_plan(&manifest, &plan, &CloudInitOptions::default())
        .expect("valid generated manifest should render cloud-init");

    let Some(user_data) = seed.user_data.strip_prefix("#cloud-config\n") else {
        panic!("rendered user-data must include cloud-config header");
    };
    serde_yaml::from_str::<serde_yaml::Value>(&seed.meta_data).expect("meta-data should parse as YAML");
    serde_yaml::from_str::<serde_yaml::Value>(user_data).expect("user-data should parse as YAML");
});

impl Input {
    fn manifest_yaml(&self) -> String {
        let name = identifier_or(&self.name, "fuzz");
        let mut user = identifier_or(&self.user, "agent");
        if user == "root" {
            user = "agent".to_owned();
        }
        let port_name = identifier_or(&self.port_name, "ssh");
        let port = self.port.max(1);
        let cpus = self.cpus.clamp(1, 16);
        let memory_gb = self.memory_gb.clamp(1, 64);
        let storage_gb = self.storage_gb.clamp(1, 512);

        let mut output = format!(
            r"version: 1
name: {name:?}
image:
  os: archlinux
user:
  name: {user:?}
resources:
  cpus: {cpus}
  memory: {memory_gb}G
  storage: {storage_gb}G
network:
  mode: user
  ports:
    {port_name:?}:
      guest: {port}
      protocol: tcp
bootstrap:
"
        );
        push_list(
            &mut output,
            "packages",
            self.packages.iter().map(|value| package_or(value, "git")).take(8),
        );
        push_list(
            &mut output,
            "shell",
            self.shell.iter().map(|value| shell_or(value, "true")).take(4),
        );
        output
    }
}

fn push_list(output: &mut String, name: &str, values: impl Iterator<Item = String>) {
    let values = values.collect::<Vec<_>>();
    if values.is_empty() {
        output.push_str("  ");
        output.push_str(name);
        output.push_str(": []\n");
        return;
    }

    output.push_str("  ");
    output.push_str(name);
    output.push_str(":\n");
    for value in values {
        output.push_str("    - ");
        output.push_str(&format!("{value:?}"));
        output.push('\n');
    }
}

fn identifier_or(value: &str, fallback: &str) -> String {
    sanitized_or(value, fallback, 24, |byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
    })
}

fn package_or(value: &str, fallback: &str) -> String {
    sanitized_or(value, fallback, 32, |byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-')
    })
}

fn shell_or(value: &str, fallback: &str) -> String {
    sanitized_or(value, fallback, 64, |byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'/' | b' ' | b'=' | b':' | b'-')
    })
}

fn sanitized_or(value: &str, fallback: &str, limit: usize, valid: impl Fn(u8) -> bool) -> String {
    let sanitized = value
        .bytes()
        .filter(|byte| valid(*byte))
        .take(limit)
        .map(char::from)
        .collect::<String>();
    if sanitized.trim().is_empty() {
        fallback.to_owned()
    } else {
        sanitized
    }
}
