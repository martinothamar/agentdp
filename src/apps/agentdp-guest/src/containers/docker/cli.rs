use std::ffi::{OsStr, OsString};
use std::path::Path;

use crate::Result;

use super::super::core::build::{
    BuildParsePolicy, DOCKER_BUILD_FLAG_OPTIONS, DOCKER_BUILD_OPTIONS_WITH_VALUES, DefaultBuildFile,
    MissingBuildContext,
};
use super::super::core::cli::{self as container_cli, BuildPlan, ComposePlan};

pub(crate) fn invoked_as_docker() -> bool {
    container_cli::invoked_as(super::os::CONFIG)
}

pub(crate) fn run_from_env() -> Result<()> {
    let code = run(std::env::args_os().skip(1))?;
    std::process::exit(code);
}

pub(crate) fn run(args: impl IntoIterator<Item = OsString>) -> Result<i32> {
    run_with_real_docker(args, super::os::CONFIG.real_cli_path())
}

fn run_with_real_docker(args: impl IntoIterator<Item = OsString>, real_docker: &Path) -> Result<i32> {
    let args = args.into_iter().collect::<Vec<_>>();
    if let Some(plan) = parse_compose_plan(&args) {
        return container_cli::run_compose("docker", real_docker, super::os::CONFIG.ca_dir(), &plan, &args);
    }

    let buildkit_disabled = std::env::var_os("DOCKER_BUILDKIT").as_deref() == Some(OsStr::new("0"));
    let Some(plan) = parse_build_plan(&args, buildkit_disabled) else {
        return container_cli::pass_through(real_docker, &args);
    };
    container_cli::run_build("docker", real_docker, super::os::CONFIG.ca_dir(), &plan, &args)
}

fn parse_compose_plan(args: &[OsString]) -> Option<ComposePlan> {
    ComposePlan::parse(args, docker_command_start)
}

fn parse_build_plan(args: &[OsString], buildkit_disabled: bool) -> Option<BuildPlan> {
    let command = BuildCommand::parse(args, buildkit_disabled)?;
    BuildPlan::from_rest(
        args,
        command.rest_start,
        BuildParsePolicy::new(
            DefaultBuildFile::Dockerfile,
            MissingBuildContext::CurrentDirectory,
            DOCKER_BUILD_OPTIONS_WITH_VALUES,
            DOCKER_BUILD_FLAG_OPTIONS,
        ),
    )
}

struct BuildCommand {
    rest_start: usize,
}

impl BuildCommand {
    fn parse(args: &[OsString], buildkit_disabled: bool) -> Option<Self> {
        let command_start = docker_command_start(args)?;
        let command = args.get(command_start)?.to_str()?;
        let next = args.get(command_start + 1).and_then(|arg| arg.to_str());
        let rest_start = match (command, next) {
            ("build", _) if !buildkit_disabled => command_start + 1,
            ("image" | "builder", Some("build")) if !buildkit_disabled => command_start + 2,
            ("buildx", _) => container_cli::buildx_build_rest_start(args, command_start + 1)?,
            _ => return None,
        };
        Some(Self { rest_start })
    }
}

fn docker_command_start(args: &[OsString]) -> Option<usize> {
    container_cli::command_start(args, docker_global_option_needs_value)
}

fn docker_global_option_needs_value(value: &str) -> bool {
    matches!(
        value,
        "-c" | "--config"
            | "-H"
            | "--host"
            | "-l"
            | "--log-level"
            | "--context"
            | "--tlscacert"
            | "--tlscert"
            | "--tlskey"
    )
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::PathBuf;

    use super::super::super::core::build;
    use super::super::super::core::build::ComposeCommand;
    use super::*;

    #[test]
    fn parse_plain_build_rewrites_dockerfile_and_preserves_build_options() {
        let args = os_args([
            "build",
            "--progress=plain",
            "-t",
            "image:tag",
            "-f",
            "docker/app.Dockerfile",
            ".",
        ]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert_eq!(plan.subcommand, os_args(["build"]));
        assert_eq!(plan.request.args, os_args(["--progress=plain", "-t", "image:tag"]));
        assert_eq!(plan.request.positionals, os_args(["."]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("docker/app.Dockerfile"));
        assert!(!plan.request.passthrough);
    }

    #[test]
    fn parse_buildx_build_rewrites_dockerfile() {
        let args = os_args(["buildx", "build", "--load", "-f", "Dockerfile", "."]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert_eq!(plan.subcommand, os_args(["buildx", "build"]));
        assert_eq!(plan.request.args, os_args(["--load"]));
        assert_eq!(plan.request.positionals, os_args(["."]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("Dockerfile"));
    }

    #[test]
    fn parse_compose_build_preserves_docker_compose_and_build_args() {
        let args = os_args([
            "--context",
            "default",
            "compose",
            "-f",
            "docker-compose.yml",
            "--progress",
            "plain",
            "build",
            "--no-cache",
            "fakes",
        ]);

        let plan = parse_compose_plan(&args).expect("plan");

        assert_eq!(plan.engine_args, os_args(["--context", "default"]));
        assert_eq!(
            plan.compose_args,
            os_args(["-f", "docker-compose.yml", "--progress", "plain"])
        );
        assert_eq!(plan.command, ComposeCommand::Build);
        assert_eq!(plan.command_args, os_args(["--no-cache", "fakes"]));
    }

    #[test]
    fn parse_compose_up_build_preserves_args() {
        let args = os_args(["compose", "-f", "docker-compose.yml", "up", "-d", "--build", "fakes"]);

        let plan = parse_compose_plan(&args).expect("plan");

        assert!(plan.engine_args.is_empty());
        assert_eq!(plan.compose_args, os_args(["-f", "docker-compose.yml"]));
        assert_eq!(plan.command, ComposeCommand::Up);
        assert_eq!(plan.command_args, os_args(["-d", "--build", "fakes"]));
    }

    #[test]
    fn parse_compose_no_build_passes_through() {
        assert!(parse_compose_plan(&os_args(["compose", "up", "--no-build"])).is_none());
    }

    #[test]
    fn parse_combined_short_dockerfile_flag_rewrites_requested_file() {
        for args in [
            os_args(["build", "-fDockerfile.custom", "."]),
            os_args(["build", "-f=Dockerfile.custom", "."]),
            os_args(["buildx", "build", "-fDockerfile.custom", "."]),
        ] {
            let plan = parse_build_plan(&args, false).expect("plan");

            assert!(plan.request.args.is_empty());
            assert_eq!(plan.request.positionals, os_args(["."]));
            assert_eq!(plan.request.dockerfile_path, PathBuf::from("Dockerfile.custom"));
        }
    }

    #[test]
    fn parse_default_dockerfile_uses_build_context() {
        let args = os_args(["build", "context-dir"]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert_eq!(plan.request.positionals, os_args(["context-dir"]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("context-dir/Dockerfile"));
    }

    #[test]
    fn parse_no_context_materializes_current_directory_context() {
        let args = os_args(["build"]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert_eq!(plan.request.positionals, os_args(["."]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("./Dockerfile"));
    }

    #[test]
    fn parse_explicit_dockerfile_is_relative_to_cwd() {
        let args = os_args(["build", "-f", "Dockerfile", "context-dir"]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert_eq!(plan.request.positionals, os_args(["context-dir"]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("Dockerfile"));
    }

    #[test]
    fn parse_build_with_docker_global_options_rewrites_dockerfile() {
        let args = os_args([
            "--context",
            "default",
            "-H",
            "unix:///run/docker.sock",
            "build",
            "-f",
            "Dockerfile",
            ".",
        ]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert_eq!(
            plan.subcommand,
            os_args(["--context", "default", "-H", "unix:///run/docker.sock", "build"])
        );
        assert_eq!(plan.request.positionals, os_args(["."]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("Dockerfile"));
    }

    #[test]
    fn parse_build_with_short_log_level_global_option_rewrites_dockerfile() {
        let args = os_args(["-l", "debug", "build", "."]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert_eq!(plan.subcommand, os_args(["-l", "debug", "build"]));
        assert_eq!(plan.request.positionals, os_args(["."]));
    }

    #[test]
    fn parse_build_command_aliases_rewrite_dockerfile() {
        for (args, expected_subcommand) in [
            (os_args(["image", "build", "."]), os_args(["image", "build"])),
            (os_args(["builder", "build", "."]), os_args(["builder", "build"])),
            (os_args(["buildx", "b", "."]), os_args(["buildx", "b"])),
        ] {
            let plan = parse_build_plan(&args, false).expect("plan");

            assert_eq!(plan.subcommand, expected_subcommand);
            assert_eq!(plan.request.positionals, os_args(["."]));
            assert_eq!(plan.request.dockerfile_path, PathBuf::from("./Dockerfile"));
        }
    }

    #[test]
    fn parse_docker_boolean_build_option_does_not_consume_context() {
        let args = os_args(["build", "--pull", "."]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert_eq!(plan.request.args, os_args(["--pull"]));
        assert_eq!(plan.request.positionals, os_args(["."]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("./Dockerfile"));
    }

    #[test]
    fn parse_buildx_parent_options_before_build_rewrite_dockerfile() {
        let args = os_args(["buildx", "--builder", "default", "-D", "build", "."]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert_eq!(
            plan.subcommand,
            os_args(["buildx", "--builder", "default", "-D", "build"])
        );
        assert_eq!(plan.request.positionals, os_args(["."]));
    }

    #[test]
    fn parse_split_value_build_flags_keeps_values_out_of_positionals() {
        let args = os_args([
            "buildx",
            "build",
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
        ]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert_eq!(
            plan.request.args,
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
        assert_eq!(plan.request.positionals, os_args(["."]));
    }

    #[test]
    fn parse_build_help_passes_through() {
        assert!(parse_build_plan(&os_args(["build", "--help"]), false).is_none());
        assert!(parse_build_plan(&os_args(["buildx", "build", "--help"]), false).is_none());
        assert!(parse_build_plan(&os_args(["buildx", "--help"]), false).is_none());
    }

    #[test]
    fn parse_remote_context_passes_through() {
        let args = os_args(["build", "https://example.invalid/repo.git"]);

        let plan = parse_build_plan(&args, false).expect("plan");

        assert!(plan.request.passthrough);
    }

    #[test]
    fn parse_plain_build_with_buildkit_disabled_is_passthrough() {
        let args = os_args(["build", "."]);

        assert!(parse_build_plan(&args, true).is_none());
    }

    #[test]
    fn parse_incomplete_value_options_passes_through() {
        for args in [
            os_args(["build", "-f"]),
            os_args(["build", "--file"]),
            os_args(["build", "-f="]),
            os_args(["build", "--file="]),
            os_args(["build", "--tag"]),
        ] {
            assert!(parse_build_plan(&args, false).is_none(), "{args:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn injection_prepare_failure_falls_back_to_real_docker_unchanged() {
        let temp_dir = build::create_temp_dir().expect("temp dir");
        let fake_docker = temp_dir.join("docker");
        let args_log = temp_dir.join("args.log");
        std::fs::write(
            &fake_docker,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"{}\"\nexit 42\n",
                args_log.display()
            ),
        )
        .expect("write fake docker");
        std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755))
            .expect("fake docker permissions");

        let code =
            run_with_real_docker(os_args(["build", "-f", "Missing.Dockerfile", "."]), &fake_docker).expect("run shim");

        assert_eq!(code, 42);
        assert_eq!(
            std::fs::read_to_string(&args_log).expect("args log"),
            "build\n-f\nMissing.Dockerfile\n.\n"
        );
        std::fs::remove_dir_all(temp_dir).expect("cleanup");
    }

    fn os_args<const N: usize>(values: [&str; N]) -> Vec<OsString> {
        values.into_iter().map(OsString::from).collect()
    }
}
