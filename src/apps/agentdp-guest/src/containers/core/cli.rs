use std::ffi::OsString;
use std::path::Path;
use std::process::{Command, ExitStatus};

use serde_json::Value;

use crate::{Error, Result};

use super::build::{self, BuildParsePolicy, BuildRequest, ComposeCommand};
use super::os::CliConfig;

pub(crate) fn invoked_as(config: CliConfig) -> bool {
    std::env::args_os()
        .next()
        .as_deref()
        .map(Path::new)
        .and_then(Path::file_name)
        .is_some_and(|name| config.is_shim_executable_name(name))
}

pub(crate) fn run_build(
    engine_name: &str,
    real_cli: &Path,
    ca_dir: &Path,
    plan: &BuildPlan,
    original_args: &[OsString],
) -> Result<i32> {
    if plan.request.passthrough {
        if let Some(reason) = &plan.request.passthrough_reason {
            eprintln!(
                "agentdp {engine_name} cli: skipping CA bundle build injection; running {engine_name} unchanged: {reason}"
            );
        }
        return pass_through(real_cli, original_args);
    }

    let prepared = match build::prepare_build(&plan.request) {
        Ok(prepared) => prepared,
        Err(error) => {
            eprintln!(
                "agentdp {engine_name} cli: could not inject CA bundle; running {engine_name} without build CA injection: {error}"
            );
            return pass_through(real_cli, original_args);
        }
    };

    let status = Command::new(real_cli)
        .args(&plan.subcommand)
        .args(&plan.request.args)
        .arg("--build-context")
        .arg(build::build_context_arg(ca_dir))
        .arg("-f")
        .arg(&prepared.dockerfile)
        .args(&plan.request.positionals)
        .status();
    prepared.cleanup(&format!("{engine_name} build"));
    Ok(exit_code(status?))
}

pub(crate) fn run_compose(
    engine_name: &str,
    real_cli: &Path,
    ca_dir: &Path,
    plan: &ComposePlan,
    original_args: &[OsString],
) -> Result<i32> {
    let prepared = match prepare_compose(engine_name, real_cli, ca_dir, plan) {
        Ok(prepared) => prepared,
        Err(error) => {
            eprintln!(
                "agentdp {engine_name} cli: could not inject CA bundle into Compose command; running {engine_name} without build CA injection: {error}"
            );
            return pass_through(real_cli, original_args);
        }
    };

    let status = Command::new(real_cli)
        .args(&plan.engine_args)
        .arg("compose")
        .args(plan.replay_compose_args())
        .arg("-f")
        .arg(&prepared.compose_file)
        .arg(plan.command.as_str())
        .args(&plan.command_args)
        .status();
    prepared.cleanup(&format!("{engine_name} Compose build"));
    Ok(exit_code(status?))
}

pub(crate) fn pass_through(real_cli: &Path, args: &[OsString]) -> Result<i32> {
    Ok(exit_code(Command::new(real_cli).args(args).status()?))
}

pub(crate) fn command_start(args: &[OsString], option_needs_value: fn(&str) -> bool) -> Option<usize> {
    let mut index = 0usize;
    while index < args.len() {
        let arg = args[index].to_str()?;
        if arg == "--" {
            return None;
        }
        if !arg.starts_with('-') || arg == "-" {
            return Some(index);
        }
        index += 1;
        if option_needs_value(arg) && !arg.contains('=') {
            index += 1;
        }
    }
    None
}

pub(crate) fn buildx_build_rest_start(args: &[OsString], mut index: usize) -> Option<usize> {
    while index < args.len() {
        let arg = args[index].to_str()?;
        match arg {
            "build" | "b" => return Some(index + 1),
            "-h" | "--help" => return None,
            value if value.starts_with('-') => {
                index += 1;
                if buildx_parent_option_needs_value(value) && !value.contains('=') {
                    index += 1;
                }
            }
            _ => return None,
        }
    }
    None
}

fn prepare_compose(
    engine_name: &str,
    real_cli: &Path,
    ca_dir: &Path,
    plan: &ComposePlan,
) -> Result<build::PreparedCompose> {
    let output = Command::new(real_cli)
        .args(&plan.engine_args)
        .arg("compose")
        .args(&plan.compose_args)
        .arg("config")
        .arg("--format")
        .arg("json")
        .output()?;
    let config = if output.status.success() {
        match serde_json::from_slice::<Value>(&output.stdout) {
            Ok(config) => config,
            Err(json_error) => load_compose_config_yaml(engine_name, real_cli, plan, Some(json_error))?,
        }
    } else {
        load_compose_config_yaml(engine_name, real_cli, plan, None)?
    };
    build::prepare_compose(config, ca_dir)
}

fn load_compose_config_yaml(
    engine_name: &str,
    real_cli: &Path,
    plan: &ComposePlan,
    json_error: Option<serde_json::Error>,
) -> Result<Value> {
    let yaml_output = Command::new(real_cli)
        .args(&plan.engine_args)
        .arg("compose")
        .args(&plan.compose_args)
        .arg("config")
        .output()?;
    if !yaml_output.status.success() {
        return Err(Error::Message(format!(
            "{engine_name} compose config failed: yaml stderr: {}",
            String::from_utf8_lossy(&yaml_output.stderr).trim()
        )));
    }
    serde_yaml::from_slice::<Value>(&yaml_output.stdout).map_err(|yaml_error| {
        let json_detail = json_error.map_or_else(String::new, |error| format!("json error: {error}; "));
        Error::Message(format!(
            "{engine_name} compose config YAML output was invalid: {json_detail}yaml error: {yaml_error}"
        ))
    })
}

fn buildx_parent_option_needs_value(value: &str) -> bool {
    matches!(value, "--builder" | "--config")
}

fn exit_code(status: ExitStatus) -> i32 {
    status.code().unwrap_or(1)
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BuildPlan {
    pub(crate) subcommand: Vec<OsString>,
    pub(crate) request: BuildRequest,
}

impl BuildPlan {
    pub(crate) fn from_rest(args: &[OsString], rest_start: usize, policy: BuildParsePolicy) -> Option<Self> {
        let subcommand = args[..rest_start].to_vec();
        let request = BuildRequest::parse(&args[rest_start..], policy)?;
        Some(Self { subcommand, request })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ComposePlan {
    pub(crate) engine_args: Vec<OsString>,
    pub(crate) compose_args: Vec<OsString>,
    pub(crate) command: ComposeCommand,
    pub(crate) command_args: Vec<OsString>,
}

impl ComposePlan {
    pub(crate) fn parse(args: &[OsString], engine_command_start: fn(&[OsString]) -> Option<usize>) -> Option<Self> {
        let compose_index = engine_command_start(args)?;
        if args.get(compose_index)?.to_str()? != "compose" {
            return None;
        }
        let command_index = build::compose_build_capable_command_index(args, compose_index + 1)?;
        let command = ComposeCommand::parse(args.get(command_index)?.to_str()?)?;
        let command_args = args[command_index + 1..].to_vec();
        if build::has_help_arg(&command_args) || command_args.iter().any(|arg| arg == "--no-build") {
            return None;
        }
        Some(Self {
            engine_args: args[..compose_index].to_vec(),
            compose_args: args[compose_index + 1..command_index].to_vec(),
            command,
            command_args,
        })
    }

    pub(crate) fn replay_compose_args(&self) -> Vec<OsString> {
        compose_args_without_files(&self.compose_args)
    }
}

fn compose_args_without_files(args: &[OsString]) -> Vec<OsString> {
    let mut filtered = Vec::new();
    let mut index = 0usize;
    while let Some(arg) = args.get(index) {
        match arg.to_str() {
            Some("-f" | "--file") => {
                index += 2;
            }
            Some(value)
                if value.starts_with("--file=")
                    || value.starts_with("-f=")
                    || (value.starts_with("-f") && value.len() > 2) =>
            {
                index += 1;
            }
            _ => {
                filtered.push(arg.clone());
                index += 1;
            }
        }
    }
    filtered
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::*;

    #[test]
    fn compose_replay_args_drop_original_files_and_preserve_other_parent_args() {
        let args = os_args([
            "--context",
            "default",
            "compose",
            "-p",
            "project",
            "-f",
            "compose.yaml",
            "--profile",
            "dev",
            "--env-file",
            ".env.test",
            "--progress",
            "plain",
            "up",
            "--build",
        ]);
        let plan = ComposePlan::parse(&args, |_| Some(2)).expect("plan");

        assert_eq!(
            plan.replay_compose_args(),
            os_args([
                "-p",
                "project",
                "--profile",
                "dev",
                "--env-file",
                ".env.test",
                "--progress",
                "plain",
            ])
        );
    }

    #[test]
    fn compose_replay_args_drop_inline_file_options() {
        let args = os_args(["compose", "--file=compose.yaml", "-f=extra.yaml", "-fdev.yaml", "build"]);
        let plan = ComposePlan::parse(&args, |_| Some(0)).expect("plan");

        assert!(plan.replay_compose_args().is_empty());
    }

    fn os_args<const N: usize>(values: [&str; N]) -> Vec<OsString> {
        values.into_iter().map(OsString::from).collect()
    }
}
