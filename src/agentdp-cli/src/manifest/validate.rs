use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::Context;
use agentdp_core::manifest::{load_manifest, resolve_manifest_path};
use clap::Args;

#[derive(Debug, Args)]
pub struct Command {
    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,
}

pub fn run(command: &Command, context: &Context) -> ExitCode {
    let cwd = match env::current_dir() {
        Ok(cwd) => cwd,
        Err(error) => {
            eprintln!("failed to read current directory: {error}");
            return ExitCode::FAILURE;
        }
    };

    let path = match resolve_manifest_path(context, command.file.as_deref(), &cwd) {
        Ok(path) => path,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };

    match load_manifest(context, &path) {
        Ok(manifest) => {
            println!("manifest ok: {}", path.display());
            println!("name: {}", manifest.name);
            println!("image.os: {:?}", manifest.image.os);
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
