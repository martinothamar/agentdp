use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use agentdp_core::Context;
use agentdp_core::manifest::AgentManifest;
use agentdp_core::provisioning::SeedFile;
use agentdp_core::provisioning::guest_os::{GuestLayout, GuestOsAdapter, guest_tool_seeds_for_os};
use agentdp_core::provisioning::host_input::{
    HostInputFile, HostInputFileSource, HostInputGuestPath, HostInputRequirements, MaterializationContext,
    MaterializedHostInput,
};
use agentdp_core::provisioning::secrets::SecretBindings;
use flate2::Compression;
use flate2::write::GzEncoder;
use thiserror::Error;

const GUEST_TOOL_DIR_ENV: &str = "AGENTDP_GUEST_TOOL_DIR";
const INSTALLED_GUEST_TOOL_DIR: &str = "agentdp-guest-tools";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Collected {
    pub files: Vec<SeedFile>,
    pub secrets: SecretBindings,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeSecrets {
    pub live: SecretBindings,
    pub stored_secrets: SecretBindings,
    pub files: Vec<SeedFile>,
}

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("manifest path has no parent directory: {0}")]
    MissingManifestParent(PathBuf),
    #[error("failed to read seed directory {path}: {source}")]
    ReadDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read seed file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to inspect seed file {path}: {source}")]
    Metadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to resolve current executable path: {0}")]
    CurrentExe(#[source] std::io::Error),
    #[error("host input file {label} was not found at {path}{hint}")]
    MissingHostInputFile { label: String, path: PathBuf, hint: String },
    #[cfg(not(test))]
    #[error(
        "Linux guest tool binary {name} was not found at {path}; set AGENTDP_GUEST_TOOL_DIR to a directory containing extensionless guestd and guestctl binaries"
    )]
    MissingGuestTool { name: String, path: PathBuf },
    #[error("failed to compress guest tool binary {name}: {source}")]
    CompressGuestTool {
        name: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    HostInput(#[from] agentdp_core::provisioning::host_input::Error),
}

/// Collects host-side seed files referenced by a manifest.
///
/// # Errors
///
/// Returns an error if the manifest path cannot be resolved, seed files cannot be
/// read, or host credential paths cannot be determined.
pub(crate) async fn collect(
    context: &Context,
    manifest_path: &Path,
    manifest: &AgentManifest,
) -> Result<Collected, Error> {
    let manifest_dir = manifest_path
        .parent()
        .ok_or_else(|| Error::MissingManifestParent(manifest_path.to_path_buf()))?;
    let requirements = manifest.host_input_requirements();
    let layout = GuestOsAdapter::for_os(manifest.spec.image.os).capabilities().layout;
    let mut files = Vec::new();
    let mut secrets = SecretBindings::default();
    files.extend(collect_guest_tool_seeds(context, manifest).await?);
    collect_home_seed(context, &manifest_dir.join("data/home"), layout.agent_home, &mut files).await?;
    collect_ca_seed(context, manifest_dir, manifest, layout, &mut files).await?;
    collect_host_input_files(
        context,
        &requirements,
        layout,
        MaterializationContext::default(),
        &mut files,
        &mut secrets,
    )
    .await?;
    collect_custom_bootstrap(context, manifest_dir, layout, &mut files).await?;
    let custom_env = if requirements.has_mediated_env_secret_inputs() {
        Some(
            materialize_required_custom_env(
                context,
                manifest_dir,
                &requirements,
                MaterializationContext::default(),
                "custom bootstrap env",
            )
            .await?,
        )
    } else {
        materialize_custom_env(
            context,
            manifest_dir,
            &requirements,
            MaterializationContext::default(),
            "custom bootstrap env",
        )
        .await?
    };
    if let Some(materialized) = custom_env {
        secrets.extend(materialized.secrets);
        files.push(SeedFile {
            path: layout.persistent_env.to_owned(),
            contents: materialized.contents,
            permissions: "0644".to_owned(),
            owner: Some("root:root".to_owned()),
        });
    }
    Ok(Collected {
        files: dedupe_by_path(files),
        secrets,
    })
}

pub(crate) async fn collect_guest_tool_seeds(
    context: &Context,
    manifest: &AgentManifest,
) -> Result<Vec<SeedFile>, Error> {
    let mut files = Vec::with_capacity(guest_tool_seeds_for_os(manifest.spec.image.os).len());
    collect_guest_tool_binaries(context, manifest, &mut files).await?;
    Ok(files)
}

async fn collect_ca_seed(
    context: &Context,
    manifest_dir: &Path,
    manifest: &AgentManifest,
    layout: GuestLayout,
    files: &mut Vec<SeedFile>,
) -> Result<(), Error> {
    let Some(source) = manifest.spec.network.ca.source_path() else {
        return Ok(());
    };
    let path = manifest_dir.join(source);
    context
        .logger()
        .verbose_with(|| format!("collecting CA bundle from {}", path.display()));
    files.push(SeedFile {
        path: layout.ca_bundle.to_owned(),
        contents: read_file(&path).await?,
        permissions: "0644".to_owned(),
        owner: Some("root:root".to_owned()),
    });
    Ok(())
}

pub(crate) async fn collect_runtime_secrets(
    context: &Context,
    manifest_path: &Path,
    manifest: &AgentManifest,
    existing_secrets: &SecretBindings,
) -> Result<RuntimeSecrets, Error> {
    let manifest_dir = manifest_path
        .parent()
        .ok_or_else(|| Error::MissingManifestParent(manifest_path.to_path_buf()))?;
    let requirements = manifest.host_input_requirements();
    let layout = GuestOsAdapter::for_os(manifest.spec.image.os).capabilities().layout;
    let materialization = MaterializationContext::new(existing_secrets);
    let mut files = Vec::new();
    let mut live = SecretBindings::default();
    collect_secret_host_input_files(context, &requirements, layout, materialization, &mut files, &mut live).await?;
    if requirements.has_mediated_env_secret_inputs() {
        let materialized = materialize_required_custom_env(
            context,
            manifest_dir,
            &requirements,
            materialization,
            "runtime custom env",
        )
        .await?;
        live.extend(materialized.secrets);
    }
    let stored_secrets = live.redacted();
    Ok(RuntimeSecrets {
        live,
        stored_secrets,
        files: dedupe_by_path(files),
    })
}

pub(crate) async fn collect_runtime_host_files(
    context: &Context,
    manifest_path: &Path,
    manifest: &AgentManifest,
    existing_secrets: &SecretBindings,
    secret_files: &[SeedFile],
) -> Result<Vec<SeedFile>, Error> {
    let manifest_dir = manifest_path
        .parent()
        .ok_or_else(|| Error::MissingManifestParent(manifest_path.to_path_buf()))?;
    let requirements = manifest.host_input_requirements();
    let layout = GuestOsAdapter::for_os(manifest.spec.image.os).capabilities().layout;
    let mut files = Vec::new();
    let mut ignored_secrets = SecretBindings::default();
    collect_home_seed(context, &manifest_dir.join("data/home"), layout.agent_home, &mut files).await?;
    collect_non_secret_host_input_files(
        context,
        &requirements,
        layout,
        MaterializationContext::new(existing_secrets),
        &mut files,
        &mut ignored_secrets,
    )
    .await?;
    files.extend_from_slice(secret_files);
    Ok(dedupe_by_path(files))
}

async fn collect_secret_host_input_files(
    context: &Context,
    requirements: &HostInputRequirements,
    layout: GuestLayout,
    materialization: MaterializationContext<'_>,
    files: &mut Vec<SeedFile>,
    secrets: &mut SecretBindings,
) -> Result<(), Error> {
    for requirement in requirements.files().iter().filter(|file| file.produces_secrets()) {
        collect_host_input_file(context, requirement, layout, materialization, files, secrets).await?;
    }
    Ok(())
}

async fn collect_non_secret_host_input_files(
    context: &Context,
    requirements: &HostInputRequirements,
    layout: GuestLayout,
    materialization: MaterializationContext<'_>,
    files: &mut Vec<SeedFile>,
    secrets: &mut SecretBindings,
) -> Result<(), Error> {
    for requirement in requirements.files().iter().filter(|file| !file.produces_secrets()) {
        collect_host_input_file(context, requirement, layout, materialization, files, secrets).await?;
    }
    Ok(())
}

async fn collect_host_input_files(
    context: &Context,
    requirements: &HostInputRequirements,
    layout: GuestLayout,
    materialization: MaterializationContext<'_>,
    files: &mut Vec<SeedFile>,
    secrets: &mut SecretBindings,
) -> Result<(), Error> {
    for requirement in requirements.files() {
        collect_host_input_file(context, requirement, layout, materialization, files, secrets).await?;
    }
    Ok(())
}

async fn collect_host_input_file(
    context: &Context,
    requirement: &HostInputFile,
    layout: GuestLayout,
    materialization: MaterializationContext<'_>,
    files: &mut Vec<SeedFile>,
    secrets: &mut SecretBindings,
) -> Result<(), Error> {
    let path = resolve_host_input_file_source(requirement.source());
    collect_host_input_file_from_path(context, requirement, layout, materialization, &path, files, secrets).await
}

async fn collect_host_input_file_from_path(
    context: &Context,
    requirement: &HostInputFile,
    layout: GuestLayout,
    materialization: MaterializationContext<'_>,
    path: &Path,
    files: &mut Vec<SeedFile>,
    secrets: &mut SecretBindings,
) -> Result<(), Error> {
    let materialized = materialize_host_input_file(context, requirement, materialization, path).await?;
    secrets.extend(materialized.secrets);
    files.push(SeedFile {
        path: resolve_host_input_guest_path(requirement.guest_path(), layout),
        contents: materialized.contents,
        permissions: requirement.permissions().to_owned(),
        owner: None,
    });
    Ok(())
}

async fn materialize_host_input_file(
    context: &Context,
    requirement: &HostInputFile,
    materialization: MaterializationContext<'_>,
    path: &Path,
) -> Result<agentdp_core::provisioning::host_input::MaterializedHostInput, Error> {
    if !tokio::fs::try_exists(path).await.map_err(|source| Error::ReadFile {
        path: path.to_path_buf(),
        source,
    })? {
        return Err(Error::MissingHostInputFile {
            label: requirement.label().to_owned(),
            path: path.to_path_buf(),
            hint: missing_host_input_file_hint(requirement.source()),
        });
    }
    context
        .logger()
        .verbose_with(|| format!("collecting {} from {}", requirement.label(), path.display()));
    requirement
        .materialize(&read_file(path).await?, materialization)
        .map_err(Error::HostInput)
}

async fn collect_guest_tool_binaries(
    context: &Context,
    manifest: &AgentManifest,
    files: &mut Vec<SeedFile>,
) -> Result<(), Error> {
    let tool_dir = guest_tool_dir()?;
    context
        .logger()
        .verbose_with(|| format!("collecting guest tool binaries from {}", tool_dir.display()));
    let mut contents = Vec::new();
    for tool in guest_tool_seeds_for_os(manifest.spec.image.os) {
        read_guest_tool_contents(&tool_dir, tool.name, &mut contents).await?;
        let contents = if tool.compress {
            gzip_guest_tool(tool.name, &contents)?
        } else {
            std::mem::take(&mut contents)
        };
        files.push(SeedFile {
            path: tool.guest_path.to_owned(),
            contents,
            permissions: tool.permissions.to_owned(),
            owner: Some("root:root".to_owned()),
        });
    }
    Ok(())
}

fn gzip_guest_tool(name: &str, contents: &[u8]) -> Result<Vec<u8>, Error> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(contents).map_err(|source| Error::CompressGuestTool {
        name: name.to_owned(),
        source,
    })?;
    encoder.finish().map_err(|source| Error::CompressGuestTool {
        name: name.to_owned(),
        source,
    })
}

async fn collect_home_seed(
    context: &Context,
    source: &Path,
    guest_home: &str,
    files: &mut Vec<SeedFile>,
) -> Result<(), Error> {
    if !tokio::fs::try_exists(source)
        .await
        .map_err(|error| Error::ReadDirectory {
            path: source.to_path_buf(),
            source: error,
        })?
    {
        return Ok(());
    }
    context
        .logger()
        .verbose_with(|| format!("collecting agent home seed files from {}", source.display()));
    collect_home_seed_dir(source, source, guest_home, files).await
}

async fn collect_home_seed_dir(
    root: &Path,
    directory: &Path,
    guest_home: &str,
    files: &mut Vec<SeedFile>,
) -> Result<(), Error> {
    let mut pending = vec![directory.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries = tokio::fs::read_dir(&directory)
            .await
            .map_err(|source| Error::ReadDirectory {
                path: directory.clone(),
                source,
            })?;
        while let Some(entry) = entries.next_entry().await.map_err(|source| Error::ReadDirectory {
            path: directory.clone(),
            source,
        })? {
            let path = entry.path();
            let metadata = entry.metadata().await.map_err(|source| Error::Metadata {
                path: path.clone(),
                source,
            })?;
            if metadata.is_dir() {
                pending.push(path);
                continue;
            }
            if !metadata.is_file() || is_env_file(&path) {
                continue;
            }
            let relative = path.strip_prefix(root).unwrap_or(&path);
            files.push(SeedFile {
                path: format!("{guest_home}/{}", relative_path_text(relative)),
                contents: read_file(&path).await?,
                permissions: permissions(&metadata),
                owner: None,
            });
        }
    }
    Ok(())
}

async fn collect_custom_bootstrap(
    context: &Context,
    manifest_dir: &Path,
    layout: GuestLayout,
    files: &mut Vec<SeedFile>,
) -> Result<(), Error> {
    let path = manifest_dir.join("bootstrap.sh");
    if !tokio::fs::try_exists(&path).await.map_err(|source| Error::ReadFile {
        path: path.clone(),
        source,
    })? {
        return Ok(());
    }
    context
        .logger()
        .verbose_with(|| format!("collecting custom bootstrap script from {}", path.display()));
    files.push(SeedFile {
        path: layout.custom_bootstrap.to_owned(),
        contents: read_file(&path).await?,
        permissions: "0755".to_owned(),
        owner: Some("root:root".to_owned()),
    });
    Ok(())
}

async fn materialize_custom_env(
    context: &Context,
    manifest_dir: &Path,
    requirements: &HostInputRequirements,
    materialization: MaterializationContext<'_>,
    label: &str,
) -> Result<Option<MaterializedHostInput>, Error> {
    let path = manifest_dir.join(".env");
    if !tokio::fs::try_exists(&path).await.map_err(|source| Error::ReadFile {
        path: path.clone(),
        source,
    })? {
        return Ok(None);
    }
    context
        .logger()
        .verbose_with(|| format!("collecting {label} from {}", path.display()));
    let contents = read_file(&path).await?;
    let materialized = requirements.materialize_custom_env(&contents, materialization)?;
    Ok(Some(materialized))
}

async fn materialize_required_custom_env(
    context: &Context,
    manifest_dir: &Path,
    requirements: &HostInputRequirements,
    materialization: MaterializationContext<'_>,
    label: &str,
) -> Result<MaterializedHostInput, Error> {
    materialize_custom_env(context, manifest_dir, requirements, materialization, label)
        .await?
        .ok_or_else(|| Error::MissingHostInputFile {
            label: label.to_owned(),
            path: manifest_dir.join(".env"),
            hint: String::new(),
        })
}

async fn read_file(path: &Path) -> Result<Vec<u8>, Error> {
    tokio::fs::read(path).await.map_err(|source| Error::ReadFile {
        path: path.to_path_buf(),
        source,
    })
}

fn relative_path_text(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn is_env_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    name == ".env" || name.starts_with(".env.")
}

fn resolve_host_input_file_source(source: &HostInputFileSource) -> PathBuf {
    match source {
        HostInputFileSource::HomeRelative {
            path_env,
            home_env,
            home_relative_path,
            default_home_relative_path,
        } => {
            if let Some(path) = path_env
                .as_deref()
                .and_then(|name| std::env::var_os(name).filter(|path| !path.is_empty()))
            {
                return PathBuf::from(path);
            }
            if let Some(home) = home_env
                .as_deref()
                .and_then(|name| std::env::var_os(name).filter(|path| !path.is_empty()))
            {
                return PathBuf::from(home).join(home_relative_path);
            }
            host_home_dir().join(default_home_relative_path)
        }
    }
}

fn missing_host_input_file_hint(source: &HostInputFileSource) -> String {
    match source {
        HostInputFileSource::HomeRelative { path_env, .. } => path_env
            .as_ref()
            .map(|name| format!("; create the source file or set {name}"))
            .unwrap_or_default(),
    }
}

fn resolve_host_input_guest_path(guest_path: &HostInputGuestPath, layout: GuestLayout) -> String {
    match guest_path {
        HostInputGuestPath::AgentHomeRelative(path) => {
            format!("{}/{}", layout.agent_home.trim_end_matches('/'), path)
        }
    }
}

fn host_home_dir() -> PathBuf {
    #[cfg(target_os = "windows")]
    {
        if let Some(path) = std::env::var_os("USERPROFILE").filter(|path| !path.is_empty()) {
            return PathBuf::from(path);
        }
    }
    std::env::var_os("HOME")
        .filter(|path| !path.is_empty())
        .map_or_else(|| PathBuf::from("."), PathBuf::from)
}

#[cfg(unix)]
fn permissions(metadata: &std::fs::Metadata) -> String {
    use std::os::unix::fs::PermissionsExt as _;
    if metadata.permissions().mode() & 0o111 == 0 {
        "0644".to_owned()
    } else {
        "0755".to_owned()
    }
}

#[cfg(not(unix))]
fn permissions(_metadata: &std::fs::Metadata) -> String {
    "0644".to_owned()
}

fn guest_tool_dir() -> Result<PathBuf, Error> {
    if let Some(path) = std::env::var_os(GUEST_TOOL_DIR_ENV).filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    let current_exe = std::env::current_exe().map_err(Error::CurrentExe)?;
    let current_dir = current_exe.parent().unwrap_or_else(|| Path::new("."));
    let sibling_dir = if current_dir.file_name().and_then(|name| name.to_str()) == Some("deps") {
        current_dir.parent().unwrap_or(current_dir)
    } else {
        current_dir
    };
    Ok(sibling_dir.join(INSTALLED_GUEST_TOOL_DIR))
}

fn guest_tool_path(tool_dir: &Path, name: &str) -> PathBuf {
    tool_dir.join(name)
}

#[cfg(test)]
#[allow(clippy::unnecessary_wraps, clippy::unused_async)]
async fn read_guest_tool_contents(_tool_dir: &Path, _name: &str, contents: &mut Vec<u8>) -> Result<(), Error> {
    contents.clear();
    contents.extend_from_slice(b"#!/bin/sh\nexit 0\n");
    Ok(())
}

#[cfg(not(test))]
async fn read_guest_tool_contents(tool_dir: &Path, name: &str, contents: &mut Vec<u8>) -> Result<(), Error> {
    use tokio::io::AsyncReadExt as _;

    let path = guest_tool_path(tool_dir, name);
    contents.clear();
    match tokio::fs::File::open(&path).await {
        Ok(mut file) => file
            .read_to_end(contents)
            .await
            .map(|_read| ())
            .map_err(|source| Error::ReadFile { path, source }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Err(Error::MissingGuestTool {
            name: name.to_owned(),
            path,
        }),
        Err(source) => Err(Error::ReadFile { path, source }),
    }
}

fn dedupe_by_path(files: Vec<SeedFile>) -> Vec<SeedFile> {
    let mut by_path = BTreeMap::new();
    for file in files {
        by_path.insert(file.path.clone(), file);
    }
    by_path.into_values().collect()
}

#[cfg(test)]
mod tests {
    use agentdp_platform::time;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{
        MaterializationContext, SecretBindings, collect, collect_host_input_file_from_path, collect_runtime_host_files,
        collect_runtime_secrets, guest_tool_path, materialize_custom_env,
    };
    use agentdp_core::Context;
    use agentdp_core::manifest::AgentManifest;
    use agentdp_core::provisioning::guest_os::GuestOsAdapter;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn collects_manifest_local_home_seed_files() {
        let temp = TestTempDir::create("qemu-host-seed");
        let manifest_path = temp.write("agent.yaml", agentdp_test_support::manifest::minimal());
        temp.write("data/home/.codex/AGENTS.md", "agent instructions\n");
        temp.write("data/home/.env", "secret=do-not-copy-as-home\n");
        let manifest = manifest(&manifest_path);

        let collected = collect(&Context::quiet(), &manifest_path, &manifest).await.unwrap();
        let files = collected.files;

        let paths = files.iter().map(|file| file.path.as_str()).collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "/data/home/.codex/AGENTS.md",
                "/run/agentdp/bin/guestctl.gz",
                "/usr/local/bin/guestd"
            ]
        );
    }

    #[tokio::test]
    async fn runtime_host_files_include_manifest_local_home_seed_files() {
        let temp = TestTempDir::create("qemu-host-seed-runtime-home");
        let manifest_path = temp.write("agent.yaml", agentdp_test_support::manifest::minimal());
        temp.write("data/home/.codex/AGENTS.md", "agent instructions\n");
        temp.write("data/home/.env", "secret=do-not-copy-as-home\n");
        let manifest = manifest(&manifest_path);

        let files = collect_runtime_host_files(
            &Context::quiet(),
            &manifest_path,
            &manifest,
            &SecretBindings::default(),
            &[],
        )
        .await
        .unwrap();

        let paths = files.iter().map(|file| file.path.as_str()).collect::<Vec<_>>();
        assert_eq!(paths, vec!["/data/home/.codex/AGENTS.md"]);
    }

    #[tokio::test]
    async fn runtime_secrets_do_not_depend_on_home_seed() {
        let temp = TestTempDir::create("qemu-host-seed-runtime-secrets");
        let manifest_path = temp.write("agent.yaml", agentdp_test_support::manifest::minimal());
        temp.write("data/home", "not a directory\n");
        let manifest = manifest(&manifest_path);

        let collected =
            collect_runtime_secrets(&Context::quiet(), &manifest_path, &manifest, &SecretBindings::default())
                .await
                .unwrap();

        assert!(collected.live.is_empty());
        assert!(collected.stored_secrets.is_empty());
        assert!(collected.files.is_empty());
    }

    #[tokio::test]
    async fn collects_user_network_ca_source() {
        let temp = TestTempDir::create("qemu-host-seed-ca-source");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: ca-source
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: user
      ca:
        source: data/ca/corp.pem
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets: []
    plugins: {}
",
        );
        temp.write("data/ca/corp.pem", "CORP CA\n");
        let manifest = manifest(&manifest_path);

        let collected = collect(&Context::quiet(), &manifest_path, &manifest).await.unwrap();
        let ca = collected
            .files
            .iter()
            .find(|file| file.path == "/var/lib/agentdp/ca/ca-bundle.pem")
            .unwrap();

        assert_eq!(ca.contents, b"CORP CA\n");
        assert_eq!(ca.permissions, "0644");
        assert_eq!(ca.owner.as_deref(), Some("root:root"));
    }

    #[tokio::test]
    async fn collects_custom_bootstrap_and_env_as_temporary_root_seed() {
        let temp = TestTempDir::create("qemu-host-seed-custom-bootstrap");
        let manifest_yaml = agentdp_test_support::manifest::standard().replacen(
            "        auth: mediated",
            "        auth: mediated\n        auth_source: env",
            1,
        );
        let manifest_path = temp.write("agent.yaml", &manifest_yaml);
        temp.write(".env", "GITHUB_PAT=opaque\nOPENAI_API_KEY=codex-secret\n");
        temp.write("bootstrap.sh", "gh auth login\n");
        let manifest = manifest(&manifest_path);

        let collected = collect(&Context::quiet(), &manifest_path, &manifest).await.unwrap();
        assert!(!collected.secrets.is_empty());
        let files = collected.files;

        let paths = files.iter().map(|file| file.path.as_str()).collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "/etc/agentdp/.env",
                "/run/agentdp/bin/guestctl.gz",
                "/usr/local/bin/guestd",
                "/var/lib/agentdp/bootstrap/custom-bootstrap.sh"
            ]
        );
        let env_file = files.iter().find(|file| file.path == "/etc/agentdp/.env").unwrap();
        assert_eq!(env_file.permissions, "0644");
        assert_eq!(env_file.owner.as_deref(), Some("root:root"));
        let env_contents = String::from_utf8(env_file.contents.clone()).unwrap();
        assert!(env_contents.starts_with("GITHUB_PAT=AGENTDP_SECRET_GITHUB_PAT_"));
        assert!(!env_contents.contains("opaque"));
        let bootstrap = files
            .iter()
            .find(|file| file.path == "/var/lib/agentdp/bootstrap/custom-bootstrap.sh")
            .unwrap();
        assert_eq!(bootstrap.permissions, "0755");
        assert_eq!(bootstrap.owner.as_deref(), Some("root:root"));

        let placeholder = env_value(&env_contents, "GITHUB_PAT");
        assert_secret(
            &collected.secrets,
            placeholder,
            "GITHUB_PAT",
            "opaque",
            "api.github.com",
        );
        assert!(!secret(&collected.secrets, placeholder).allows_host("example.com"));
    }

    #[tokio::test]
    async fn mediated_env_secret_scopes_follow_owning_plugin() {
        let temp = TestTempDir::create("qemu-host-seed-scoped-secrets");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: mediated-auth
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      codex:
        auth: mediated
        auth_source: env
      github:
        auth: mediated
    secrets: []
",
        );
        temp.write(
            ".env",
            "GITHUB_TOKEN=github-secret\nOPENAI_API_KEY=openai-secret\nBOOTSTRAP_FLAG=keep-me\n",
        );
        let manifest = manifest(&manifest_path);

        let collected = collect(&Context::quiet(), &manifest_path, &manifest).await.unwrap();
        let env_file = collected
            .files
            .iter()
            .find(|file| file.path == "/etc/agentdp/.env")
            .unwrap();

        let env = String::from_utf8(env_file.contents.clone()).unwrap();
        assert!(env.contains("BOOTSTRAP_FLAG=keep-me\n"));
        assert_secret(
            &collected.secrets,
            env_value(&env, "GITHUB_TOKEN"),
            "GITHUB_TOKEN",
            "github-secret",
            "api.github.com",
        );
        assert!(!secret(&collected.secrets, env_value(&env, "GITHUB_TOKEN")).allows_host("api.openai.com"));
        assert_secret(
            &collected.secrets,
            env_value(&env, "OPENAI_API_KEY"),
            "OPENAI_API_KEY",
            "openai-secret",
            "api.openai.com",
        );
        assert!(!secret(&collected.secrets, env_value(&env, "OPENAI_API_KEY")).allows_host("api.github.com"));
    }

    #[tokio::test]
    async fn mediated_env_requires_declared_variable() {
        let temp = TestTempDir::create("qemu-host-seed-missing-mediated-env");
        let manifest_path = write_mediated_manifest(&temp, "mediated-auth", GITHUB_MEDIATED_PLUGIN);
        temp.write(".env", "OTHER=value\n");
        let manifest = manifest(&manifest_path);

        let error = collect_runtime_secrets(&Context::quiet(), &manifest_path, &manifest, &SecretBindings::default())
            .await
            .unwrap_err();

        assert!(error.to_string().contains("GITHUB_TOKEN"));
    }

    #[tokio::test]
    async fn runtime_strict_requires_env_source_for_env_mediated_auth() {
        let temp = TestTempDir::create("qemu-host-seed-strict-missing-env");
        let manifest_path = write_mediated_manifest(&temp, "mediated-auth", GITHUB_MEDIATED_PLUGIN);
        let manifest = manifest(&manifest_path);

        let error = collect_runtime_secrets(&Context::quiet(), &manifest_path, &manifest, &SecretBindings::default())
            .await
            .unwrap_err();

        assert!(error.to_string().contains("runtime custom env"));
    }

    #[tokio::test]
    async fn collect_requires_env_source_for_env_mediated_auth() {
        let temp = TestTempDir::create("qemu-host-seed-collect-missing-env");
        let manifest_path = write_mediated_manifest(&temp, "mediated-auth", GITHUB_MEDIATED_PLUGIN);
        let manifest = manifest(&manifest_path);

        let error = collect(&Context::quiet(), &manifest_path, &manifest).await.unwrap_err();

        assert!(error.to_string().contains("custom bootstrap env"));
    }

    #[tokio::test]
    async fn top_level_secrets_placeholderize_custom_env_names() {
        let temp = TestTempDir::create("qemu-host-seed-top-level-secrets");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: custom-secrets
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    secrets:
      - env: STUDIO_DEV_API_KEY
        from_env: ALTINN_DEV_KEY
        allow_hosts:
          - dev.altinn.studio
      - env: STUDIO_PROD_API_KEY
        allow_hosts:
          - altinn.studio
    plugins: {}
",
        );
        temp.write(
            ".env",
            "ALTINN_DEV_KEY=dev-secret\nSTUDIO_PROD_API_KEY=prod-secret\nBOOTSTRAP_FLAG=keep-me\n",
        );
        let manifest = manifest(&manifest_path);

        let collected = collect(&Context::quiet(), &manifest_path, &manifest).await.unwrap();
        let env_file = collected
            .files
            .iter()
            .find(|file| file.path == "/etc/agentdp/.env")
            .unwrap();
        let env = String::from_utf8(env_file.contents.clone()).unwrap();

        assert!(!env.contains("ALTINN_DEV_KEY"));
        assert!(!env.contains("dev-secret"));
        assert!(!env.contains("prod-secret"));
        assert!(env.contains("STUDIO_DEV_API_KEY=AGENTDP_SECRET_STUDIO_DEV_API_KEY_"));
        assert!(env.contains("STUDIO_PROD_API_KEY=AGENTDP_SECRET_STUDIO_PROD_API_KEY_"));
        assert!(env.contains("BOOTSTRAP_FLAG=keep-me\n"));

        assert_secret(
            &collected.secrets,
            env_value(&env, "STUDIO_DEV_API_KEY"),
            "STUDIO_DEV_API_KEY",
            "dev-secret",
            "dev.altinn.studio",
        );
        assert!(
            !secret(&collected.secrets, env_value(&env, "STUDIO_DEV_API_KEY")).allows_host("staging.altinn.studio")
        );
        assert_secret(
            &collected.secrets,
            env_value(&env, "STUDIO_PROD_API_KEY"),
            "STUDIO_PROD_API_KEY",
            "prod-secret",
            "altinn.studio",
        );
    }

    #[tokio::test]
    async fn mixed_auth_copies_only_copy_owned_env_names() {
        let temp = TestTempDir::create("qemu-host-seed-mixed-auth");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: mixed-auth
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      codex:
        auth: mediated
        auth_source: env
      github:
        auth: copy-from-host
    secrets: []
",
        );
        temp.write(
            ".env",
            "GITHUB_TOKEN=github-secret\nOPENAI_API_KEY=openai-secret\nBOOTSTRAP_FLAG=keep-me\n",
        );
        let manifest = manifest(&manifest_path);

        let collected = collect(&Context::quiet(), &manifest_path, &manifest).await.unwrap();
        let env_file = collected
            .files
            .iter()
            .find(|file| file.path == "/etc/agentdp/.env")
            .unwrap();
        let env = String::from_utf8(env_file.contents.clone()).unwrap();

        assert!(env.contains("GITHUB_TOKEN=github-secret\n"));
        assert!(env.contains("BOOTSTRAP_FLAG=keep-me\n"));
        assert!(!env.contains("openai-secret"));
        assert_secret(
            &collected.secrets,
            env_value(&env, "OPENAI_API_KEY"),
            "OPENAI_API_KEY",
            "openai-secret",
            "api.openai.com",
        );
    }

    #[tokio::test]
    async fn mediated_env_rewrites_export_assignments() {
        let temp = TestTempDir::create("qemu-host-seed-export-env");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: export-auth
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      github:
        auth: mediated
    secrets: []
",
        );
        temp.write(
            ".env",
            "export GITHUB_PAT=github-secret\nexport BOOTSTRAP_FLAG=keep-me\n",
        );
        let manifest = manifest(&manifest_path);

        let collected = collect(&Context::quiet(), &manifest_path, &manifest).await.unwrap();
        let env_file = collected
            .files
            .iter()
            .find(|file| file.path == "/etc/agentdp/.env")
            .unwrap();
        let env = String::from_utf8(env_file.contents.clone()).unwrap();

        assert!(!env.contains("github-secret"));
        assert!(env.contains("export GITHUB_PAT=AGENTDP_SECRET_GITHUB_PAT_"));
        assert!(env.contains("export BOOTSTRAP_FLAG=keep-me\n"));
        assert_secret(
            &collected.secrets,
            export_env_value(&env, "GITHUB_PAT"),
            "GITHUB_PAT",
            "github-secret",
            "api.github.com",
        );
    }

    #[tokio::test]
    async fn copy_from_host_auth_seeds_custom_env_without_placeholders() {
        let temp = TestTempDir::create("qemu-host-seed-copy-auth");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: copy-auth
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: user
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      codex:
        auth: copy-from-host
      github:
        auth: copy-from-host
    secrets: []
",
        );
        temp.write(".env", "GITHUB_PAT=opaque\nOPENAI_API_KEY=also-opaque\n");
        let manifest = manifest(&manifest_path);

        let collected = collect(&Context::quiet(), &manifest_path, &manifest).await.unwrap();

        assert!(collected.secrets.is_empty());
        let env_file = collected
            .files
            .iter()
            .find(|file| file.path == "/etc/agentdp/.env")
            .unwrap();
        assert_eq!(
            String::from_utf8(env_file.contents.clone()).unwrap(),
            "GITHUB_PAT=opaque\nOPENAI_API_KEY=also-opaque\n"
        );
    }

    #[tokio::test]
    async fn copy_from_host_codex_auth_seeds_host_auth_file() {
        let temp = TestTempDir::create("qemu-host-seed-codex-auth");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: copy-auth
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: user
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      codex:
        auth: copy-from-host
    secrets: []
",
        );
        let manifest = manifest(&manifest_path);
        let mut files = Vec::new();
        let mut secrets = SecretBindings::default();
        let auth = temp.write("host-codex/auth.json", "{\"tokens\":\"opaque\"}\n");
        let requirement = first_auth_file(&manifest);

        collect_host_input_file_from_path(
            &Context::quiet(),
            &requirement,
            GuestOsAdapter::for_os(manifest.spec.image.os).capabilities().layout,
            MaterializationContext::default(),
            &auth,
            &mut files,
            &mut secrets,
        )
        .await
        .unwrap();

        assert!(secrets.is_empty());
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "/data/home/.codex/auth.json");
        assert_eq!(files[0].permissions, "0600");
        assert_eq!(files[0].owner, None);
        assert_eq!(files[0].contents, b"{\"tokens\":\"opaque\"}\n");
    }

    #[tokio::test]
    async fn mediated_codex_auth_seeds_placeholder_auth_file() {
        let temp = TestTempDir::create("qemu-host-seed-mediated-codex-auth");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: mediated-codex-auth
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      codex:
        auth: mediated
    secrets: []
",
        );
        let manifest = manifest(&manifest_path);
        let mut files = Vec::new();
        let mut secrets = SecretBindings::default();
        let auth = temp.write(
            "host-codex/auth.json",
            r#"{"tokens":{"access_token":"access-secret","refresh_token":"refresh-secret","id_token":"id-secret"},"profile":"keep-me"}"#,
        );
        let requirement = first_auth_file(&manifest);

        collect_host_input_file_from_path(
            &Context::quiet(),
            &requirement,
            GuestOsAdapter::for_os(manifest.spec.image.os).capabilities().layout,
            MaterializationContext::default(),
            &auth,
            &mut files,
            &mut secrets,
        )
        .await
        .unwrap();

        let auth_json = String::from_utf8(files[0].contents.clone()).unwrap();
        assert!(auth_json.contains("\"profile\": \"keep-me\""));
        assert!(!auth_json.contains("access-secret"));
        assert!(!auth_json.contains("refresh-secret"));
        assert!(!auth_json.contains("id-secret"));
        assert!(auth_json.contains("AGENTDP_SECRET_CODEX_AUTH_TOKENS_ACCESS_TOKEN_"));
        assert!(auth_json.contains("AGENTDP_SECRET_CODEX_AUTH_TOKENS_REFRESH_TOKEN_"));
        assert!(auth_json.contains("\"id_token\": \"eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0."));
        assert_secret(
            &secrets,
            &json_string_field(&auth_json, "access_token"),
            "CODEX_AUTH_TOKENS_ACCESS_TOKEN",
            "access-secret",
            "api.openai.com",
        );
        assert_secret(
            &secrets,
            &json_string_field(&auth_json, "refresh_token"),
            "CODEX_AUTH_TOKENS_REFRESH_TOKEN",
            "refresh-secret",
            "api.openai.com",
        );
        let id_token_placeholder = json_string_field(&auth_json, "id_token");
        assert_secret(
            &secrets,
            &id_token_placeholder,
            "CODEX_AUTH_TOKENS_ID_TOKEN",
            "id-secret",
            "api.openai.com",
        );
        assert!(!secret(&secrets, &id_token_placeholder).allows_host("example.com"));
    }

    #[tokio::test]
    async fn copy_from_host_claude_auth_seeds_host_auth_file() {
        let temp = TestTempDir::create("qemu-host-seed-claude-auth");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: copy-claude-auth
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: user
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      claude:
        auth: copy-from-host
    secrets: []
",
        );
        let manifest = manifest(&manifest_path);
        let mut files = Vec::new();
        let mut secrets = SecretBindings::default();
        let auth = temp.write(
            "host-claude/.credentials.json",
            "{\"claudeAiOauth\":{\"accessToken\":\"opaque\"}}\n",
        );
        let requirement = first_auth_file(&manifest);

        collect_host_input_file_from_path(
            &Context::quiet(),
            &requirement,
            GuestOsAdapter::for_os(manifest.spec.image.os).capabilities().layout,
            MaterializationContext::default(),
            &auth,
            &mut files,
            &mut secrets,
        )
        .await
        .unwrap();

        assert!(secrets.is_empty());
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "/data/home/.claude/.credentials.json");
        assert_eq!(files[0].permissions, "0600");
        assert_eq!(files[0].owner, None);
        assert_eq!(files[0].contents, b"{\"claudeAiOauth\":{\"accessToken\":\"opaque\"}}\n");
    }

    #[tokio::test]
    async fn copy_from_host_claude_auth_seeds_custom_env_without_placeholders() {
        let temp = TestTempDir::create("qemu-host-seed-copy-claude-env");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: copy-claude-env
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: user
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      claude:
        auth: copy-from-host
    secrets: []
",
        );
        temp.write(
            ".env",
            "ANTHROPIC_API_KEY=opaque\nCLAUDE_CODE_OAUTH_TOKEN=also-opaque\n",
        );
        let manifest = manifest(&manifest_path);
        let requirements = manifest.host_input_requirements();

        // The claude copy-from-host auth file is resolved from the host home and exercised by the
        // file-level tests above; here we assert the custom-env copy path seeds both Anthropic auth
        // env vars verbatim, with no placeholderization or mediated secrets.
        let materialized = materialize_custom_env(
            &Context::quiet(),
            manifest_path.parent().unwrap(),
            &requirements,
            MaterializationContext::default(),
            "custom bootstrap env",
        )
        .await
        .unwrap()
        .expect("custom env materialized from .env");

        assert!(materialized.secrets.is_empty());
        assert_eq!(
            String::from_utf8(materialized.contents).unwrap(),
            "ANTHROPIC_API_KEY=opaque\nCLAUDE_CODE_OAUTH_TOKEN=also-opaque\n"
        );
    }

    #[tokio::test]
    async fn mediated_claude_auth_seeds_placeholder_auth_file() {
        let temp = TestTempDir::create("qemu-host-seed-mediated-claude-auth");
        let manifest_path = temp.write(
            "agent.yaml",
            r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: mediated-claude-auth
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {}
    plugins:
      claude:
        auth: mediated
    secrets: []
",
        );
        let manifest = manifest(&manifest_path);
        let mut files = Vec::new();
        let mut secrets = SecretBindings::default();
        let auth = temp.write(
            "host-claude/.credentials.json",
            r#"{"claudeAiOauth":{"accessToken":"access-secret","refreshToken":"refresh-secret","expiresAt":123,"scopes":["user:inference"],"subscriptionType":"max"}}"#,
        );
        let requirement = first_auth_file(&manifest);

        collect_host_input_file_from_path(
            &Context::quiet(),
            &requirement,
            GuestOsAdapter::for_os(manifest.spec.image.os).capabilities().layout,
            MaterializationContext::default(),
            &auth,
            &mut files,
            &mut secrets,
        )
        .await
        .unwrap();

        assert_eq!(files[0].path, "/data/home/.claude/.credentials.json");
        let auth_json = String::from_utf8(files[0].contents.clone()).unwrap();
        assert!(auth_json.contains("\"subscriptionType\": \"max\""));
        assert!(auth_json.contains("\"expiresAt\": 123"));
        assert!(!auth_json.contains("access-secret"));
        assert!(!auth_json.contains("refresh-secret"));
        assert!(auth_json.contains("AGENTDP_SECRET_CLAUDE_AUTH_CLAUDEAIOAUTH_ACCESSTOKEN_"));
        assert!(auth_json.contains("AGENTDP_SECRET_CLAUDE_AUTH_CLAUDEAIOAUTH_REFRESHTOKEN_"));
        let access_token_placeholder = json_string_at(&auth_json, "/claudeAiOauth/accessToken");
        assert_secret(
            &secrets,
            &access_token_placeholder,
            "CLAUDE_AUTH_CLAUDEAIOAUTH_ACCESSTOKEN",
            "access-secret",
            "api.anthropic.com",
        );
        assert_secret(
            &secrets,
            &json_string_at(&auth_json, "/claudeAiOauth/refreshToken"),
            "CLAUDE_AUTH_CLAUDEAIOAUTH_REFRESHTOKEN",
            "refresh-secret",
            "console.anthropic.com",
        );
        let access_token = secret(&secrets, &access_token_placeholder);
        assert!(access_token.allows_host("claude.ai"));
        assert!(!access_token.allows_host("example.com"));
    }

    #[test]
    fn guest_tool_paths_use_linux_extensionless_names() {
        assert_eq!(
            guest_tool_path(&PathBuf::from("tools"), "guestd"),
            PathBuf::from("tools").join("guestd")
        );
    }

    fn first_auth_file(manifest: &AgentManifest) -> agentdp_core::provisioning::host_input::HostInputFile {
        manifest
            .spec
            .plugins
            .host_input_requirements()
            .files()
            .first()
            .unwrap()
            .clone()
    }

    fn env_value<'a>(contents: &'a str, name: &str) -> &'a str {
        contents
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}=")))
            .unwrap()
    }

    fn export_env_value<'a>(contents: &'a str, name: &str) -> &'a str {
        contents
            .lines()
            .find_map(|line| line.strip_prefix(&format!("export {name}=")))
            .unwrap()
    }

    fn json_string_field(contents: &str, field: &str) -> String {
        json_string_at(contents, &format!("/tokens/{field}"))
    }

    fn json_string_at(contents: &str, pointer: &str) -> String {
        let json = serde_json::from_str::<serde_json::Value>(contents).unwrap();
        json.pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .to_owned()
    }

    fn assert_secret(
        secrets: &SecretBindings,
        placeholder: &str,
        expected_name: &str,
        expected_value: &str,
        allowed_host: &str,
    ) {
        let binding = secret(secrets, placeholder);
        assert_eq!(binding.name, expected_name);
        assert_eq!(binding.value(), Some(expected_value));
        assert!(binding.allows_host(allowed_host));
    }

    fn secret<'a>(
        secrets: &'a SecretBindings,
        placeholder: &str,
    ) -> &'a agentdp_core::provisioning::secrets::SecretBinding {
        secrets
            .iter()
            .find(|binding| binding.placeholder == placeholder)
            .unwrap()
    }

    fn manifest(path: &PathBuf) -> AgentManifest {
        serde_yaml::from_str(&fs::read_to_string(path).unwrap()).unwrap()
    }

    const GITHUB_MEDIATED_PLUGIN: &str = "      github:\n        auth: mediated";

    fn write_mediated_manifest(temp: &TestTempDir, name: &str, plugins: &str) -> PathBuf {
        let plugins = if plugins.trim() == "{}" {
            "    plugins: {}".to_owned()
        } else {
            format!("    plugins:\n{plugins}")
        };
        temp.write(
            "agent.yaml",
            &format!(
                r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: {name}
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 10G
    network:
      mode: mediated
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap: {{}}
{plugins}
    secrets: []
"
            ),
        )
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn create(name: &str) -> Self {
            let timestamp = time::unix_nanos();
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("agentdp-{name}-{}-{timestamp}-{id}", std::process::id()));
            fs::create_dir(&path).unwrap();
            Self { path }
        }

        fn write(&self, relative: &str, contents: &str) -> PathBuf {
            let path = self.path.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, contents).unwrap();
            path
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _result = fs::remove_dir_all(&self.path);
        }
    }
}
