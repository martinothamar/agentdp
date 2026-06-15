use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

use crate::{Error, Result};

pub(crate) const CA_CONTEXT_NAME: &str = "agentdp_ca_bundle";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BuildRequest {
    pub(crate) args: Vec<OsString>,
    pub(crate) positionals: Vec<OsString>,
    pub(crate) dockerfile_path: PathBuf,
    pub(crate) passthrough: bool,
    pub(crate) passthrough_reason: Option<String>,
}

impl BuildRequest {
    pub(crate) fn parse(args: &[OsString], policy: BuildParsePolicy) -> Option<Self> {
        if has_help_arg(args) {
            return None;
        }

        let mut parsed = ParsedBuildArgs::parse(args, policy)?;
        if let Some(reason) = parsed.passthrough_reason {
            return Some(Self {
                args: Vec::new(),
                positionals: Vec::new(),
                dockerfile_path: PathBuf::new(),
                passthrough: true,
                passthrough_reason: Some(reason),
            });
        }
        if parsed.positionals.is_empty() {
            parsed.positionals.push(
                policy
                    .missing_context
                    .resolve(&parsed.dockerfile, parsed.explicit_dockerfile),
            );
        }
        let context = parsed.positionals[parsed.positionals.len() - 1].clone();
        let passthrough =
            parsed.dockerfile == OsStr::new("-") || context == OsStr::new("-") || is_url_context(&context);
        let passthrough_reason = if passthrough {
            Some("unsupported stdin or remote build input".to_owned())
        } else {
            None
        };
        let dockerfile_path = if passthrough || parsed.explicit_dockerfile {
            PathBuf::from(&parsed.dockerfile)
        } else {
            policy.default_build_file.resolve(&context)
        };

        Some(Self {
            args: parsed.args,
            positionals: parsed.positionals,
            dockerfile_path,
            passthrough,
            passthrough_reason,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct BuildParsePolicy {
    default_build_file: DefaultBuildFile,
    missing_context: MissingBuildContext,
    value_options: &'static [&'static str],
    flag_options: &'static [&'static str],
}

impl BuildParsePolicy {
    pub(crate) const fn new(
        default_build_file: DefaultBuildFile,
        missing_context: MissingBuildContext,
        value_options: &'static [&'static str],
        flag_options: &'static [&'static str],
    ) -> Self {
        Self {
            default_build_file,
            missing_context,
            value_options,
            flag_options,
        }
    }

    fn option_arity(self, value: &str) -> BuildOptionArity {
        let option = value.split_once('=').map_or(value, |(option, _)| option);
        if self.value_options.contains(&option) {
            BuildOptionArity::Value
        } else if self.flag_options.contains(&option) {
            BuildOptionArity::Flag
        } else {
            BuildOptionArity::Unknown
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BuildOptionArity {
    Value,
    Flag,
    Unknown,
}

#[derive(Clone, Copy)]
pub(crate) enum MissingBuildContext {
    CurrentDirectory,
    ExplicitDockerfileParent,
}

impl MissingBuildContext {
    fn resolve(self, dockerfile: &OsStr, explicit_dockerfile: bool) -> OsString {
        if matches!(self, Self::ExplicitDockerfileParent) && explicit_dockerfile && dockerfile != OsStr::new("-") {
            let dockerfile = Path::new(dockerfile);
            dockerfile
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .map_or_else(|| OsString::from("."), |parent| parent.as_os_str().to_os_string())
        } else {
            OsString::from(".")
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DefaultBuildFile {
    Dockerfile,
    ContainerfileThenDockerfile,
}

impl DefaultBuildFile {
    fn resolve(self, context: &OsStr) -> PathBuf {
        let context = PathBuf::from(context);
        match self {
            Self::Dockerfile => context.join("Dockerfile"),
            Self::ContainerfileThenDockerfile => {
                let containerfile = context.join("Containerfile");
                if containerfile.exists() {
                    containerfile
                } else {
                    context.join("Dockerfile")
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct PreparedBuild {
    temp_dir: PathBuf,
    pub(crate) dockerfile: PathBuf,
}

impl PreparedBuild {
    pub(crate) fn cleanup(&self, log_context: &str) {
        cleanup_temp_dir(&self.temp_dir, log_context);
    }
}

#[derive(Debug)]
pub(crate) struct PreparedCompose {
    temp_dir: PathBuf,
    pub(crate) compose_file: PathBuf,
}

impl PreparedCompose {
    pub(crate) fn cleanup(&self, log_context: &str) {
        cleanup_temp_dir(&self.temp_dir, log_context);
    }
}

pub(crate) fn prepare_build(request: &BuildRequest) -> Result<PreparedBuild> {
    let dockerfile = std::fs::read_to_string(&request.dockerfile_path)?;
    let injected = super::build_ca::inject_ca(&dockerfile, &named_context_copy_instruction());
    let temp_dir = create_temp_dir()?;
    let temp_dockerfile = temp_dir.join("Dockerfile.agentdp");
    if let Err(error) = std::fs::write(&temp_dockerfile, injected) {
        let prepared = PreparedBuild {
            temp_dir,
            dockerfile: temp_dockerfile,
        };
        prepared.cleanup("container build");
        return Err(error.into());
    }
    Ok(PreparedBuild {
        temp_dir,
        dockerfile: temp_dockerfile,
    })
}

pub(crate) fn build_context_arg(ca_dir: &Path) -> OsString {
    OsString::from(format!("{CA_CONTEXT_NAME}={}", ca_dir.display()))
}

pub(crate) fn prepare_compose(mut config: Value, ca_dir: &Path) -> Result<PreparedCompose> {
    let temp_dir = create_temp_dir()?;
    if let Err(error) = inject_compose_builds(&mut config, &temp_dir, ca_dir) {
        let prepared = PreparedCompose {
            compose_file: temp_dir.join("compose.yaml"),
            temp_dir,
        };
        prepared.cleanup("Compose build");
        return Err(error);
    }

    let compose_file = temp_dir.join("compose.yaml");
    if let Err(error) = std::fs::write(&compose_file, compose_file_contents(&config)?) {
        let prepared = PreparedCompose { temp_dir, compose_file };
        prepared.cleanup("Compose build");
        return Err(error.into());
    }

    Ok(PreparedCompose { temp_dir, compose_file })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ComposeCommand {
    Build,
    Up,
    Run,
    Create,
}

impl ComposeCommand {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "build" => Some(Self::Build),
            "up" => Some(Self::Up),
            "run" => Some(Self::Run),
            "create" => Some(Self::Create),
            _ => None,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Up => "up",
            Self::Run => "run",
            Self::Create => "create",
        }
    }
}

pub(crate) fn compose_build_capable_command_index(args: &[OsString], mut index: usize) -> Option<usize> {
    while index < args.len() {
        let arg = args[index].to_str()?;
        match arg {
            "build" | "up" | "run" | "create" => return Some(index),
            "help" | "-h" | "--help" => return None,
            value if value.starts_with('-') => {
                index += 1;
                if compose_parent_option_needs_value(value) && !value.contains('=') {
                    index += 1;
                }
            }
            _ => return None,
        }
    }
    None
}

pub(crate) fn has_help_arg(args: &[OsString]) -> bool {
    args.iter()
        .filter_map(|arg| arg.to_str())
        .any(|arg| matches!(arg, "-h" | "--help"))
}

pub(crate) fn create_temp_dir() -> Result<PathBuf> {
    let mut path = std::env::temp_dir();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Message(format!("system clock before Unix epoch: {error}")))?;
    path.push(format!(
        "agentdp-container-build-{}-{}",
        std::process::id(),
        now.as_nanos()
    ));
    std::fs::create_dir(&path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(path)
}

fn cleanup_temp_dir(temp_dir: &Path, log_context: &str) {
    if let Err(error) = std::fs::remove_dir_all(temp_dir) {
        eprintln!(
            "agentdp container cli: failed to remove temporary {log_context} directory {}: {error}",
            temp_dir.display()
        );
    }
}

fn compose_parent_option_needs_value(value: &str) -> bool {
    COMPOSE_PARENT_OPTIONS_WITH_VALUES.contains(&value)
}

const COMPOSE_PARENT_OPTIONS_WITH_VALUES: &[&str] = &[
    "-f",
    "--file",
    "-p",
    "--project-name",
    "--profile",
    "--env-file",
    "--project-directory",
    "--progress",
    "--parallel",
    "--ansi",
];

pub(crate) const DOCKER_BUILD_OPTIONS_WITH_VALUES: &[&str] = &[
    "-t",
    "--tag",
    "-o",
    "--output",
    "-m",
    "--memory",
    "--progress",
    "--cache-from",
    "--cache-to",
    "--build-arg",
    "--build-arg-file",
    "--target",
    "--platform",
    "--network",
    "--add-host",
    "--label",
    "--secret",
    "--ssh",
    "--iidfile",
    "--metadata-file",
    "--builder",
    "--build-context",
    "--call",
    "--shm-size",
    "--ulimit",
    "--allow",
    "--annotation",
    "--attest",
    "--provenance",
    "--sbom",
    "--policy",
    "--buildkitd-flags",
    "--cgroup-parent",
    "--isolation",
    "--no-cache-filter",
];

pub(crate) const DOCKER_BUILD_FLAG_OPTIONS: &[&str] = &[
    "-q",
    "--check",
    "--compress",
    "--force-rm",
    "--load",
    "--no-cache",
    "--pull",
    "--push",
    "--quiet",
    "--rm",
];

pub(crate) const PODMAN_BUILD_OPTIONS_WITH_VALUES: &[&str] = &[
    "-t",
    "--tag",
    "-o",
    "--output",
    "-m",
    "--memory",
    "--progress",
    "--cache-from",
    "--cache-to",
    "--build-arg",
    "--build-arg-file",
    "--target",
    "--platform",
    "--network",
    "--add-host",
    "--label",
    "--secret",
    "--ssh",
    "--iidfile",
    "--metadata-file",
    "--builder",
    "--build-context",
    "--call",
    "--shm-size",
    "--ulimit",
    "--allow",
    "--annotation",
    "--attest",
    "--provenance",
    "--sbom",
    "--policy",
    "--buildkitd-flags",
    "--cache-ttl",
    "--cgroup-parent",
    "--isolation",
    "--no-cache-filter",
    "-c",
    "-e",
    "-v",
    "--arch",
    "--authfile",
    "--cap-add",
    "--cap-drop",
    "--cert-dir",
    "--cgroupns",
    "--cpp-flag",
    "--cpu-period",
    "--cpu-quota",
    "--cpu-shares",
    "--cpuset-cpus",
    "--cpuset-mems",
    "--creds",
    "--cw",
    "--decryption-key",
    "--device",
    "--dns",
    "--dns-option",
    "--dns-search",
    "--env",
    "--format",
    "--from",
    "--group-add",
    "--hooks-dir",
    "--ignorefile",
    "--inherit-annotations",
    "--ipc",
    "--jobs",
    "--layer-label",
    "--logfile",
    "--logsplit",
    "--manifest",
    "--max-pull-procs",
    "--memory-swap",
    "--name",
    "--net",
    "--os",
    "--os-feature",
    "--os-version",
    "--pid",
    "--pull",
    "--retry",
    "--retry-delay",
    "--runtime",
    "--runtime-flag",
    "--sbom-image-output",
    "--sbom-image-purl-output",
    "--sbom-merge-strategy",
    "--sbom-output",
    "--sbom-purl-output",
    "--sbom-scanner-command",
    "--sbom-scanner-image",
    "--security-opt",
    "--sign-by",
    "--source-date-epoch",
    "--timestamp",
    "--unsetannotation",
    "--unsetenv",
    "--unsetlabel",
    "--userns",
    "--userns-gid-map",
    "--userns-gid-map-group",
    "--userns-uid-map",
    "--userns-uid-map-user",
    "--uts",
    "--variant",
    "--volume",
];

pub(crate) const PODMAN_BUILD_FLAG_OPTIONS: &[&str] = &[
    "-D",
    "-q",
    "--all-platforms",
    "--compat-volumes",
    "--compress",
    "--created-annotation",
    "--disable-compression",
    "--disable-content-trust",
    "--force-rm",
    "--http-proxy",
    "--identity-label",
    "--inherit-labels",
    "--layers",
    "--no-cache",
    "--no-hostname",
    "--no-hosts",
    "--omit-history",
    "--quiet",
    "--rewrite-timestamp",
    "--rm",
    "--skip-unused-stages",
    "--squash",
    "--squash-all",
    "--stdin",
    "--tls-verify",
];

#[derive(Debug, PartialEq, Eq)]
struct ParsedBuildArgs {
    args: Vec<OsString>,
    positionals: Vec<OsString>,
    dockerfile: OsString,
    explicit_dockerfile: bool,
    passthrough_reason: Option<String>,
}

impl ParsedBuildArgs {
    fn parse(args: &[OsString], policy: BuildParsePolicy) -> Option<Self> {
        let mut parsed = Self {
            args: Vec::new(),
            positionals: Vec::new(),
            dockerfile: OsString::from("Dockerfile"),
            explicit_dockerfile: false,
            passthrough_reason: None,
        };
        let mut index = 0usize;
        while let Some(arg) = args.get(index) {
            match arg.to_str() {
                Some("-f" | "--file") => {
                    if let Some(value) = args.get(index + 1) {
                        parsed.dockerfile.clone_from(value);
                        parsed.explicit_dockerfile = true;
                        index += 2;
                    } else {
                        return None;
                    }
                }
                Some(value) if value.starts_with("-f=") => {
                    let dockerfile = &value["-f=".len()..];
                    if dockerfile.is_empty() {
                        return None;
                    }
                    parsed.dockerfile = OsString::from(dockerfile);
                    parsed.explicit_dockerfile = true;
                    index += 1;
                }
                Some(value) if value.starts_with("-f") && value.len() > 2 => {
                    parsed.dockerfile = OsString::from(&value[2..]);
                    parsed.explicit_dockerfile = true;
                    index += 1;
                }
                Some(value) if value.starts_with("--file=") => {
                    let dockerfile = &value["--file=".len()..];
                    if dockerfile.is_empty() {
                        return None;
                    }
                    parsed.dockerfile = OsString::from(dockerfile);
                    parsed.explicit_dockerfile = true;
                    index += 1;
                }
                Some("--") => {
                    parsed.positionals.extend(args[index + 1..].iter().cloned());
                    break;
                }
                Some(value) if value.starts_with('-') => {
                    parsed.args.push(arg.clone());
                    if value.contains('=') {
                        index += 1;
                        continue;
                    }
                    match policy.option_arity(value) {
                        BuildOptionArity::Value => {
                            if let Some(value) = args.get(index + 1) {
                                parsed.args.push(value.clone());
                                index += 2;
                            } else {
                                return None;
                            }
                        }
                        BuildOptionArity::Unknown if value.starts_with("--") => {
                            if args.get(index + 1).is_some_and(|next| !looks_like_option(next)) {
                                return Some(Self::passthrough(
                                    args,
                                    format!("unsupported split build option {value}"),
                                ));
                            }
                            index += 1;
                        }
                        BuildOptionArity::Flag | BuildOptionArity::Unknown => {
                            index += 1;
                        }
                    }
                }
                _ => {
                    parsed.positionals.push(arg.clone());
                    index += 1;
                }
            }
        }
        Some(parsed)
    }

    fn passthrough(args: &[OsString], reason: String) -> Self {
        Self {
            args: args.to_vec(),
            positionals: Vec::new(),
            dockerfile: OsString::from("Dockerfile"),
            explicit_dockerfile: false,
            passthrough_reason: Some(reason),
        }
    }
}

fn looks_like_option(value: &OsStr) -> bool {
    value
        .to_str()
        .is_some_and(|value| value.starts_with('-') && value != "-")
}

fn is_url_context(value: &OsStr) -> bool {
    value
        .to_str()
        .and_then(|value| value.split_once("://"))
        .is_some_and(|(scheme, _)| is_url_scheme(scheme))
}

fn named_context_copy_instruction() -> String {
    format!(
        "COPY --from={CA_CONTEXT_NAME} ca-bundle.pem {}",
        super::build_ca::CA_CONTAINER_PATH
    )
}

fn compose_file_contents(config: &Value) -> Result<String> {
    serde_json::to_string_pretty(config)
        .map_err(|error| Error::Message(format!("failed to serialize Compose config: {error}")))
}

fn inject_compose_builds(config: &mut Value, temp_dir: &Path, ca_dir: &Path) -> Result<()> {
    let services = config
        .get_mut("services")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| Error::Message("Compose config did not contain services".to_owned()))?;
    let mut injected = false;
    for (index, (name, service)) in services.iter_mut().enumerate() {
        let Some(build) = service.get_mut("build").and_then(Value::as_object_mut) else {
            continue;
        };
        let Some(context) = build.get("context").and_then(Value::as_str) else {
            continue;
        };
        if is_url_text(context) {
            continue;
        }
        let dockerfile = build.get("dockerfile").and_then(Value::as_str).unwrap_or("Dockerfile");
        if dockerfile == "-" || is_url_text(dockerfile) {
            continue;
        }

        let dockerfile_path = resolve_compose_dockerfile_path(Path::new(context), dockerfile);
        let dockerfile = std::fs::read_to_string(&dockerfile_path)?;
        let injected_dockerfile = super::build_ca::inject_ca(&dockerfile, &named_context_copy_instruction());
        let temp_dockerfile = temp_dir.join(format!("{index}-{}.Dockerfile", sanitize_file_name(name)));
        std::fs::write(&temp_dockerfile, injected_dockerfile)?;
        build.insert(
            "dockerfile".to_owned(),
            Value::String(temp_dockerfile.display().to_string()),
        );
        insert_compose_ca_context(build, ca_dir);
        injected = true;
    }
    if injected {
        Ok(())
    } else {
        Err(Error::Message(
            "Compose config did not contain any local Dockerfile build contexts".to_owned(),
        ))
    }
}

fn resolve_compose_dockerfile_path(context: &Path, dockerfile: &str) -> PathBuf {
    let dockerfile = Path::new(dockerfile);
    if dockerfile.is_absolute() {
        dockerfile.to_path_buf()
    } else {
        context.join(dockerfile)
    }
}

fn insert_compose_ca_context(build: &mut Map<String, Value>, ca_dir: &Path) {
    let ca_dir = ca_dir.display().to_string();
    match build.get_mut("additional_contexts") {
        Some(Value::Object(contexts)) => {
            contexts.insert(CA_CONTEXT_NAME.to_owned(), Value::String(ca_dir));
        }
        Some(Value::Array(contexts)) => {
            contexts.retain(|context| {
                context
                    .as_str()
                    .is_none_or(|context| !context.starts_with(&format!("{CA_CONTEXT_NAME}=")))
            });
            contexts.push(Value::String(format!("{CA_CONTEXT_NAME}={ca_dir}")));
        }
        _ => {
            let mut contexts = Map::new();
            contexts.insert(CA_CONTEXT_NAME.to_owned(), Value::String(ca_dir));
            build.insert("additional_contexts".to_owned(), Value::Object(contexts));
        }
    }
}

fn is_url_text(value: &str) -> bool {
    value.split_once("://").is_some_and(|(scheme, _)| is_url_scheme(scheme))
}

fn is_url_scheme(scheme: &str) -> bool {
    let mut chars = scheme.chars();
    chars.next().is_some_and(|char| char.is_ascii_alphabetic())
        && chars.all(|char| char.is_ascii_alphanumeric() || matches!(char, '+' | '.' | '-'))
}

fn sanitize_file_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|char| {
            if char.is_ascii_alphanumeric() || matches!(char, '.' | '-' | '_') {
                char
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "service".to_owned()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::*;

    #[test]
    fn parse_build_rewrites_requested_dockerfile() {
        let request = BuildRequest::parse(
            &os_args([
                "--progress=plain",
                "-t",
                "image:tag",
                "-f",
                "docker/app.Dockerfile",
                ".",
            ]),
            docker_policy(),
        )
        .expect("request");

        assert_eq!(request.args, os_args(["--progress=plain", "-t", "image:tag"]));
        assert_eq!(request.positionals, os_args(["."]));
        assert_eq!(request.dockerfile_path, PathBuf::from("docker/app.Dockerfile"));
        assert!(!request.passthrough);
    }

    #[test]
    fn parse_combined_short_dockerfile_flag_rewrites_requested_file() {
        for args in [
            os_args(["-fDockerfile.custom", "."]),
            os_args(["-f=Dockerfile.custom", "."]),
        ] {
            let request = BuildRequest::parse(&args, docker_policy()).expect("request");

            assert!(request.args.is_empty());
            assert_eq!(request.positionals, os_args(["."]));
            assert_eq!(request.dockerfile_path, PathBuf::from("Dockerfile.custom"));
        }
    }

    #[test]
    fn parse_default_dockerfile_uses_build_context() {
        let request = BuildRequest::parse(&os_args(["context-dir"]), docker_policy()).expect("request");

        assert_eq!(request.positionals, os_args(["context-dir"]));
        assert_eq!(request.dockerfile_path, PathBuf::from("context-dir/Dockerfile"));
    }

    #[test]
    fn parse_missing_context_materializes_current_directory_context() {
        let request = BuildRequest::parse(&[], docker_policy()).expect("request");

        assert_eq!(request.positionals, os_args(["."]));
        assert_eq!(request.dockerfile_path, PathBuf::from("./Dockerfile"));
    }

    #[test]
    fn parse_explicit_dockerfile_is_relative_to_cwd() {
        let request =
            BuildRequest::parse(&os_args(["-f", "Dockerfile", "context-dir"]), docker_policy()).expect("request");

        assert_eq!(request.positionals, os_args(["context-dir"]));
        assert_eq!(request.dockerfile_path, PathBuf::from("Dockerfile"));
    }

    #[test]
    fn parse_split_value_build_flags_keeps_values_out_of_positionals() {
        let request = BuildRequest::parse(
            &os_args([
                "--call",
                "check",
                "--shm-size",
                "1g",
                "--ulimit",
                "nofile=1024:1024",
                "--allow",
                "network.host",
                "--annotation",
                "index:org.opencontainers.image.title=agentdp",
                "--attest",
                "type=sbom",
                "--provenance",
                "false",
                "--sbom",
                "false",
                "--policy",
                "filename=policy.json",
                "-o",
                "type=docker",
                ".",
            ]),
            docker_policy(),
        )
        .expect("request");

        assert!(!request.passthrough, "{:?}", request.passthrough_reason);
        assert_eq!(
            request.args,
            os_args([
                "--call",
                "check",
                "--shm-size",
                "1g",
                "--ulimit",
                "nofile=1024:1024",
                "--allow",
                "network.host",
                "--annotation",
                "index:org.opencontainers.image.title=agentdp",
                "--attest",
                "type=sbom",
                "--provenance",
                "false",
                "--sbom",
                "false",
                "--policy",
                "filename=policy.json",
                "-o",
                "type=docker",
            ])
        );
        assert_eq!(request.positionals, os_args(["."]));
    }

    #[test]
    fn parse_help_passes_through() {
        assert!(BuildRequest::parse(&os_args(["--help"]), docker_policy()).is_none());
    }

    #[test]
    fn parse_remote_context_passes_through() {
        let request =
            BuildRequest::parse(&os_args(["https://example.invalid/repo.git"]), docker_policy()).expect("request");

        assert!(request.passthrough);
        assert_eq!(
            request.passthrough_reason.as_deref(),
            Some("unsupported stdin or remote build input")
        );
    }

    #[test]
    fn parse_unknown_split_long_option_passes_through() {
        let request =
            BuildRequest::parse(&os_args(["--future-option", "value", "."]), docker_policy()).expect("request");

        assert!(request.passthrough);
        assert_eq!(
            request.passthrough_reason.as_deref(),
            Some("unsupported split build option --future-option")
        );
    }

    #[test]
    fn parse_unknown_inline_long_option_is_safe() {
        let request = BuildRequest::parse(&os_args(["--future-option=value", "."]), docker_policy()).expect("request");

        assert_eq!(request.args, os_args(["--future-option=value"]));
        assert_eq!(request.positionals, os_args(["."]));
        assert!(!request.passthrough);
    }

    #[test]
    fn parse_known_flag_options_do_not_consume_context() {
        let request = BuildRequest::parse(&os_args(["--no-cache", "--pull", "."]), docker_policy()).expect("request");

        assert_eq!(request.args, os_args(["--no-cache", "--pull"]));
        assert_eq!(request.positionals, os_args(["."]));
    }

    #[test]
    fn parse_incomplete_value_options_passes_through() {
        for args in [
            os_args(["-f"]),
            os_args(["--file"]),
            os_args(["-f="]),
            os_args(["--file="]),
            os_args(["--tag"]),
        ] {
            assert!(BuildRequest::parse(&args, docker_policy()).is_none(), "{args:?}");
        }
    }

    #[test]
    fn parse_containerfile_default_prefers_existing_containerfile() {
        let temp_dir = create_temp_dir().expect("temp dir");
        let context = temp_dir.join("context");
        std::fs::create_dir(&context).expect("context");
        std::fs::write(context.join("Containerfile"), "FROM alpine\nRUN true\n").expect("containerfile");

        let request = BuildRequest::parse(&[OsString::from(context.as_os_str())], podman_policy()).expect("request");

        assert_eq!(request.dockerfile_path, context.join("Containerfile"));
        std::fs::remove_dir_all(temp_dir).expect("cleanup");
    }

    #[test]
    fn parse_missing_context_can_use_explicit_dockerfile_parent() {
        let request = BuildRequest::parse(&os_args(["-f", "subdir/Containerfile"]), podman_policy()).expect("request");

        assert_eq!(request.positionals, os_args(["subdir"]));
        assert_eq!(request.dockerfile_path, PathBuf::from("subdir/Containerfile"));
    }

    #[test]
    fn parse_podman_value_options_keeps_values_out_of_positionals() {
        let request = BuildRequest::parse(
            &os_args([
                "--authfile",
                "auth.json",
                "--build-arg-file",
                "args.env",
                "--cert-dir",
                "certs",
                "--creds",
                "user:pass",
                "--hooks-dir",
                "hooks",
                "--ignorefile",
                ".containerignore",
                "--ipc",
                "host",
                "--memory-swap",
                "2g",
                "--security-opt",
                "label=disable",
                "--uts",
                "host",
                "-t",
                "image:tag",
                ".",
            ]),
            podman_policy(),
        )
        .expect("request");

        assert!(!request.passthrough, "{:?}", request.passthrough_reason);
        assert_eq!(
            request.args,
            os_args([
                "--authfile",
                "auth.json",
                "--build-arg-file",
                "args.env",
                "--cert-dir",
                "certs",
                "--creds",
                "user:pass",
                "--hooks-dir",
                "hooks",
                "--ignorefile",
                ".containerignore",
                "--ipc",
                "host",
                "--memory-swap",
                "2g",
                "--security-opt",
                "label=disable",
                "--uts",
                "host",
                "-t",
                "image:tag",
            ])
        );
        assert_eq!(request.positionals, os_args(["."]));
    }

    #[test]
    fn parse_podman_flag_options_do_not_consume_context() {
        let request = BuildRequest::parse(
            &os_args(["--tls-verify", "--no-cache", "--squash", "."]),
            podman_policy(),
        )
        .expect("request");

        assert_eq!(request.args, os_args(["--tls-verify", "--no-cache", "--squash"]));
        assert_eq!(request.positionals, os_args(["."]));
    }

    #[test]
    fn inject_compose_builds_rewrites_local_dockerfile_builds() {
        let temp_dir = create_temp_dir().expect("temp dir");
        let context = temp_dir.join("context");
        std::fs::create_dir(&context).expect("context");
        std::fs::write(
            context.join("Dockerfile.fakes"),
            "FROM golang:trixie\nRUN go mod download\n",
        )
        .expect("dockerfile");
        let config = serde_json::json!({
            "services": {
                "fakes": {
                    "build": {
                        "context": context,
                        "dockerfile": "Dockerfile.fakes"
                    }
                }
            }
        });

        let prepared = prepare_compose(config, Path::new("/var/lib/agentdp/ca")).expect("inject");

        let config = std::fs::read_to_string(&prepared.compose_file).expect("read compose");
        let config = serde_json::from_str::<Value>(&config).expect("parse compose");
        let build = config["services"]["fakes"]["build"].as_object().expect("build object");
        let dockerfile = build["dockerfile"].as_str().expect("dockerfile");
        let dockerfile = std::fs::read_to_string(dockerfile).expect("read injected dockerfile");
        assert!(dockerfile.contains(super::super::build_ca::INJECTION_MARKER));
        assert!(dockerfile.contains("COPY --from=agentdp_ca_bundle ca-bundle.pem /tmp/agentdp-ca-bundle.crt"));
        assert_eq!(
            build["additional_contexts"]["agentdp_ca_bundle"],
            Value::String("/var/lib/agentdp/ca".to_owned())
        );
        prepared.cleanup("test");
        std::fs::remove_dir_all(temp_dir).expect("cleanup");
    }

    fn os_args<const N: usize>(values: [&str; N]) -> Vec<OsString> {
        values.into_iter().map(OsString::from).collect()
    }

    fn docker_policy() -> BuildParsePolicy {
        BuildParsePolicy::new(
            DefaultBuildFile::Dockerfile,
            MissingBuildContext::CurrentDirectory,
            DOCKER_BUILD_OPTIONS_WITH_VALUES,
            DOCKER_BUILD_FLAG_OPTIONS,
        )
    }

    fn podman_policy() -> BuildParsePolicy {
        BuildParsePolicy::new(
            DefaultBuildFile::ContainerfileThenDockerfile,
            MissingBuildContext::ExplicitDockerfileParent,
            PODMAN_BUILD_OPTIONS_WITH_VALUES,
            PODMAN_BUILD_FLAG_OPTIONS,
        )
    }
}
