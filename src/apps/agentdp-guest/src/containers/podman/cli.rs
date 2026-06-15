use std::ffi::OsString;
use std::path::Path;

use crate::Result;

use super::super::core::build::{
    BuildParsePolicy, DefaultBuildFile, MissingBuildContext, PODMAN_BUILD_FLAG_OPTIONS,
    PODMAN_BUILD_OPTIONS_WITH_VALUES,
};
use super::super::core::cli::{self as container_cli, BuildPlan, ComposePlan};

pub(crate) fn invoked_as_podman() -> bool {
    container_cli::invoked_as(super::os::CONFIG)
}

pub(crate) fn run_from_env() -> Result<()> {
    let code = run(std::env::args_os().skip(1))?;
    std::process::exit(code);
}

pub(crate) fn run(args: impl IntoIterator<Item = OsString>) -> Result<i32> {
    run_with_real_podman(args, super::os::CONFIG.real_cli_path())
}

fn run_with_real_podman(args: impl IntoIterator<Item = OsString>, real_podman: &Path) -> Result<i32> {
    let args = args.into_iter().collect::<Vec<_>>();
    if let Some(plan) = parse_compose_plan(&args) {
        return container_cli::run_compose("podman", real_podman, super::os::CONFIG.ca_dir(), &plan, &args);
    }

    let Some(plan) = parse_build_plan(&args) else {
        return container_cli::pass_through(real_podman, &args);
    };
    container_cli::run_build("podman", real_podman, super::os::CONFIG.ca_dir(), &plan, &args)
}

fn parse_compose_plan(args: &[OsString]) -> Option<ComposePlan> {
    ComposePlan::parse(args, podman_command_start)
}

fn parse_build_plan(args: &[OsString]) -> Option<BuildPlan> {
    let command = BuildCommand::parse(args)?;
    BuildPlan::from_rest(
        args,
        command.rest_start,
        BuildParsePolicy::new(
            DefaultBuildFile::ContainerfileThenDockerfile,
            MissingBuildContext::ExplicitDockerfileParent,
            PODMAN_BUILD_OPTIONS_WITH_VALUES,
            PODMAN_BUILD_FLAG_OPTIONS,
        ),
    )
}

struct BuildCommand {
    rest_start: usize,
}

impl BuildCommand {
    fn parse(args: &[OsString]) -> Option<Self> {
        let command_start = podman_command_start(args)?;
        let command = args.get(command_start)?.to_str()?;
        let next = args.get(command_start + 1).and_then(|arg| arg.to_str());
        let rest_start = match (command, next) {
            ("build", _) => command_start + 1,
            ("image", Some("build")) => command_start + 2,
            ("buildx", _) => container_cli::buildx_build_rest_start(args, command_start + 1)?,
            _ => return None,
        };
        Some(Self { rest_start })
    }
}

fn podman_command_start(args: &[OsString]) -> Option<usize> {
    container_cli::command_start(args, podman_global_option_needs_value)
}

fn podman_global_option_needs_value(value: &str) -> bool {
    matches!(
        value,
        "--connection"
            | "--cgroup-manager"
            | "--config"
            | "--conmon"
            | "--events-backend"
            | "--hooks-dir"
            | "--identity"
            | "--imagestore"
            | "--log-level"
            | "--module"
            | "--namespace"
            | "--network-cmd-path"
            | "--network-config-dir"
            | "--root"
            | "--runroot"
            | "--runtime"
            | "--runtime-flag"
            | "--ssh"
            | "--storage-driver"
            | "--storage-opt"
            | "--syslog"
            | "--tmpdir"
            | "--url"
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
            "Containerfile",
            ".",
        ]);

        let plan = parse_build_plan(&args).expect("plan");

        assert_eq!(plan.subcommand, os_args(["build"]));
        assert_eq!(plan.request.args, os_args(["--progress=plain", "-t", "image:tag"]));
        assert_eq!(plan.request.positionals, os_args(["."]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("Containerfile"));
        assert!(!plan.request.passthrough);
    }

    #[test]
    fn parse_image_build_rewrites_dockerfile() {
        let args = os_args(["image", "build", "-f", "Containerfile", "."]);

        let plan = parse_build_plan(&args).expect("plan");

        assert_eq!(plan.subcommand, os_args(["image", "build"]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("Containerfile"));
    }

    #[test]
    fn parse_buildx_build_rewrites_dockerfile() {
        let args = os_args(["buildx", "build", "--load", "-f", "Containerfile", "."]);

        let plan = parse_build_plan(&args).expect("plan");

        assert_eq!(plan.subcommand, os_args(["buildx", "build"]));
        assert_eq!(plan.request.args, os_args(["--load"]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("Containerfile"));
    }

    #[test]
    fn parse_default_build_prefers_existing_containerfile() {
        let temp_dir = build::create_temp_dir().expect("temp dir");
        let context = temp_dir.join("context");
        std::fs::create_dir(&context).expect("context");
        std::fs::write(context.join("Containerfile"), "FROM alpine\nRUN true\n").expect("containerfile");

        let plan = parse_build_plan(&[OsString::from("build"), OsString::from(context.as_os_str())]).expect("plan");

        assert_eq!(plan.request.dockerfile_path, context.join("Containerfile"));
        std::fs::remove_dir_all(temp_dir).expect("cleanup");
    }

    #[test]
    fn parse_no_context_materializes_current_directory_context() {
        let args = os_args(["build"]);

        let plan = parse_build_plan(&args).expect("plan");

        assert_eq!(plan.request.positionals, os_args(["."]));
    }

    #[test]
    fn parse_explicit_containerfile_without_context_uses_containerfile_parent_as_context() {
        let args = os_args(["build", "-f", "subdir/Containerfile"]);

        let plan = parse_build_plan(&args).expect("plan");

        assert_eq!(plan.request.positionals, os_args(["subdir"]));
        assert_eq!(plan.request.dockerfile_path, PathBuf::from("subdir/Containerfile"));
    }

    #[test]
    fn parse_build_with_podman_global_options_rewrites_dockerfile() {
        let args = os_args(["--url", "unix:///run/user/1000/podman/podman.sock", "build", "."]);

        let plan = parse_build_plan(&args).expect("plan");

        assert_eq!(
            plan.subcommand,
            os_args(["--url", "unix:///run/user/1000/podman/podman.sock", "build"])
        );
        assert_eq!(plan.request.positionals, os_args(["."]));
    }

    #[test]
    fn parse_podman_pull_option_keeps_value_out_of_positionals() {
        let args = os_args(["build", "--pull", "newer", "."]);

        let plan = parse_build_plan(&args).expect("plan");

        assert_eq!(plan.request.args, os_args(["--pull", "newer"]));
        assert_eq!(plan.request.positionals, os_args(["."]));
    }

    #[test]
    fn parse_podman_tls_verify_bare_option_does_not_consume_context() {
        let args = os_args(["build", "--tls-verify", "."]);

        let plan = parse_build_plan(&args).expect("plan");

        assert_eq!(plan.request.args, os_args(["--tls-verify"]));
        assert_eq!(plan.request.positionals, os_args(["."]));
    }

    #[cfg(unix)]
    #[test]
    fn injection_prepare_failure_falls_back_to_real_podman_unchanged() {
        let temp_dir = build::create_temp_dir().expect("temp dir");
        let fake_podman = temp_dir.join("podman");
        let args_log = temp_dir.join("args.log");
        std::fs::write(
            &fake_podman,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"{}\"\nexit 42\n",
                args_log.display()
            ),
        )
        .expect("write fake podman");
        std::fs::set_permissions(&fake_podman, std::fs::Permissions::from_mode(0o755))
            .expect("fake podman permissions");

        let code = run_with_real_podman(os_args(["build", "-f", "Missing.Containerfile", "."]), &fake_podman)
            .expect("run shim");

        assert_eq!(code, 42);
        assert_eq!(
            std::fs::read_to_string(&args_log).expect("args log"),
            "build\n-f\nMissing.Containerfile\n.\n"
        );
        std::fs::remove_dir_all(temp_dir).expect("cleanup");
    }

    #[test]
    fn parse_compose_build_preserves_parent_and_command_args() {
        let args = os_args([
            "--url",
            "unix:///run/user/1000/podman/podman.sock",
            "compose",
            "-p",
            "project",
            "-f",
            "compose.yml",
            "--profile",
            "dev",
            "build",
            "--no-cache",
            "fakes",
        ]);

        let plan = parse_compose_plan(&args).expect("plan");

        assert_eq!(
            plan.engine_args,
            os_args(["--url", "unix:///run/user/1000/podman/podman.sock"])
        );
        assert_eq!(
            plan.compose_args,
            os_args(["-p", "project", "-f", "compose.yml", "--profile", "dev"])
        );
        assert_eq!(plan.command, ComposeCommand::Build);
        assert_eq!(plan.command_args, os_args(["--no-cache", "fakes"]));
    }

    #[test]
    fn parse_compose_up_build_rewrites_builds() {
        let args = os_args(["compose", "-f", "compose.yml", "up", "-d", "--build"]);

        let plan = parse_compose_plan(&args).expect("plan");

        assert!(plan.engine_args.is_empty());
        assert_eq!(plan.compose_args, os_args(["-f", "compose.yml"]));
        assert_eq!(plan.command, ComposeCommand::Up);
        assert_eq!(plan.command_args, os_args(["-d", "--build"]));
    }

    fn os_args<const N: usize>(values: [&str; N]) -> Vec<OsString> {
        values.into_iter().map(OsString::from).collect()
    }
}
