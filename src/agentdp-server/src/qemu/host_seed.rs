use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use agentdp_core::Context;
use agentdp_core::manifest::AgentManifest;
use agentdp_core::manifest::plugins::AuthMode;
use agentdp_core::provisioning::{AGENT_HOME, SeedFile};
use thiserror::Error;

const CUSTOM_BOOTSTRAP_PATH: &str = "/run/agentdp/bootstrap.sh";
const CUSTOM_ENV_PATH: &str = "/run/agentdp/.env";

#[derive(Debug, Error)]
pub(super) enum Error {
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
    #[error("failed to resolve host home directory")]
    HostHome,
}

pub(super) fn collect(
    context: &Context,
    manifest_path: &Path,
    manifest: &AgentManifest,
) -> Result<Vec<SeedFile>, Error> {
    let manifest_dir = manifest_path
        .parent()
        .ok_or_else(|| Error::MissingManifestParent(manifest_path.to_path_buf()))?;
    let mut files = Vec::new();
    collect_home_seed(context, &manifest_dir.join("data/home"), &mut files)?;
    collect_custom_bootstrap(context, manifest_dir, &mut files)?;
    collect_custom_env(context, manifest_dir, &mut files)?;
    collect_codex_auth(context, manifest, &mut files)?;
    collect_github_auth(context, manifest, &mut files)?;
    Ok(dedupe_by_path(files))
}

fn collect_home_seed(context: &Context, source: &Path, files: &mut Vec<SeedFile>) -> Result<(), Error> {
    if !source.exists() {
        return Ok(());
    }
    context
        .logger()
        .verbose_with(|| format!("collecting agent home seed files from {}", source.display()));
    collect_home_seed_dir(source, source, files)
}

fn collect_home_seed_dir(root: &Path, directory: &Path, files: &mut Vec<SeedFile>) -> Result<(), Error> {
    for entry in fs::read_dir(directory).map_err(|source| Error::ReadDirectory {
        path: directory.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| Error::ReadDirectory {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let metadata = entry.metadata().map_err(|source| Error::Metadata {
            path: path.clone(),
            source,
        })?;
        if metadata.is_dir() {
            collect_home_seed_dir(root, &path, files)?;
            continue;
        }
        if !metadata.is_file() || is_env_file(&path) {
            continue;
        }
        let relative = path.strip_prefix(root).unwrap_or(&path);
        files.push(SeedFile {
            path: format!("{AGENT_HOME}/{}", relative_path_text(relative)),
            contents: read_file(&path)?,
            permissions: permissions(&metadata),
            owner: None,
        });
    }
    Ok(())
}

fn collect_custom_bootstrap(context: &Context, manifest_dir: &Path, files: &mut Vec<SeedFile>) -> Result<(), Error> {
    let path = manifest_dir.join("bootstrap.sh");
    if !path.exists() {
        return Ok(());
    }
    context
        .logger()
        .verbose_with(|| format!("collecting custom bootstrap script from {}", path.display()));
    files.push(SeedFile {
        path: CUSTOM_BOOTSTRAP_PATH.to_owned(),
        contents: read_file(&path)?,
        permissions: "0700".to_owned(),
        owner: Some("root:root".to_owned()),
    });
    Ok(())
}

fn collect_custom_env(context: &Context, manifest_dir: &Path, files: &mut Vec<SeedFile>) -> Result<(), Error> {
    let path = manifest_dir.join(".env");
    if !path.exists() {
        return Ok(());
    }
    context
        .logger()
        .verbose_with(|| format!("collecting temporary custom bootstrap env from {}", path.display()));
    files.push(SeedFile {
        path: CUSTOM_ENV_PATH.to_owned(),
        contents: read_file(&path)?,
        permissions: "0600".to_owned(),
        owner: Some("root:root".to_owned()),
    });
    Ok(())
}

fn collect_codex_auth(context: &Context, manifest: &AgentManifest, files: &mut Vec<SeedFile>) -> Result<(), Error> {
    let Some(codex) = &manifest.plugins.codex else {
        return Ok(());
    };
    if codex.auth != AuthMode::CopyFromHost {
        return Ok(());
    }
    let path = host_home()?.join(".codex/auth.json");
    if !path.exists() {
        context
            .logger()
            .verbose_with(|| format!("codex copy-from-host requested but {} does not exist", path.display()));
        return Ok(());
    }
    files.push(SeedFile {
        path: format!("{AGENT_HOME}/.codex/auth.json"),
        contents: read_file(&path)?,
        permissions: "0600".to_owned(),
        owner: None,
    });
    Ok(())
}

fn collect_github_auth(context: &Context, manifest: &AgentManifest, files: &mut Vec<SeedFile>) -> Result<(), Error> {
    let Some(github) = &manifest.plugins.github else {
        return Ok(());
    };
    match github.auth {
        AuthMode::CopyFromHost => collect_github_hosts(context, files),
        AuthMode::Mediated => Ok(()),
    }
}

fn collect_github_hosts(context: &Context, files: &mut Vec<SeedFile>) -> Result<(), Error> {
    let path = host_config_home()?.join("gh/hosts.yml");
    if !path.exists() {
        context
            .logger()
            .verbose_with(|| format!("github copy-from-host requested but {} does not exist", path.display()));
        return Ok(());
    }
    files.push(SeedFile {
        path: format!("{AGENT_HOME}/.config/gh/hosts.yml"),
        contents: read_file(&path)?,
        permissions: "0600".to_owned(),
        owner: None,
    });
    Ok(())
}

fn read_file(path: &Path) -> Result<Vec<u8>, Error> {
    fs::read(path).map_err(|source| Error::ReadFile {
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

#[cfg(unix)]
fn permissions(metadata: &fs::Metadata) -> String {
    use std::os::unix::fs::PermissionsExt as _;
    if metadata.permissions().mode() & 0o111 == 0 {
        "0644".to_owned()
    } else {
        "0755".to_owned()
    }
}

#[cfg(not(unix))]
fn permissions(_metadata: &fs::Metadata) -> String {
    "0644".to_owned()
}

fn host_home() -> Result<PathBuf, Error> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .ok_or(Error::HostHome)
}

fn host_config_home() -> Result<PathBuf, Error> {
    if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(path));
    }
    Ok(host_home()?.join(".config"))
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
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use agentdp_core::Context;
    use agentdp_core::manifest::AgentManifest;

    use super::collect;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn collects_manifest_local_home_seed_files() {
        let temp = TestTempDir::create("qemu-host-seed");
        let manifest_path = temp.write("agent.yaml", agentdp_test_support::manifest::minimal());
        temp.write("data/home/.codex/AGENTS.md", "agent instructions\n");
        temp.write("data/home/.env", "secret=do-not-copy-as-home\n");
        let manifest = serde_yaml::from_str::<AgentManifest>(agentdp_test_support::manifest::minimal()).unwrap();

        let files = collect(&Context::quiet(), &manifest_path, &manifest).unwrap();

        let paths = files.iter().map(|file| file.path.as_str()).collect::<Vec<_>>();
        assert_eq!(paths, vec!["/data/home/.codex/AGENTS.md"]);
    }

    #[test]
    fn collects_custom_bootstrap_and_env_as_temporary_root_seed() {
        let temp = TestTempDir::create("qemu-host-seed-custom-bootstrap");
        let manifest_yaml = agentdp_test_support::manifest::minimal();
        let manifest_path = temp.write("agent.yaml", manifest_yaml);
        temp.write(".env", "GITHUB_PAT=opaque\n");
        temp.write("bootstrap.sh", "gh auth login\n");
        let manifest = serde_yaml::from_str::<AgentManifest>(manifest_yaml).unwrap();

        let files = collect(&Context::quiet(), &manifest_path, &manifest).unwrap();

        let paths = files.iter().map(|file| file.path.as_str()).collect::<Vec<_>>();
        assert_eq!(paths, vec!["/run/agentdp/.env", "/run/agentdp/bootstrap.sh"]);
        assert_eq!(files[0].permissions, "0600");
        assert_eq!(files[0].owner.as_deref(), Some("root:root"));
        assert_eq!(files[1].permissions, "0700");
        assert_eq!(files[1].owner.as_deref(), Some("root:root"));
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn create(name: &str) -> Self {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
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
