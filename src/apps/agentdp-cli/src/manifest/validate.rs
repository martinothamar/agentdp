use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::{Context, manifest::LoadedAgentManifest};
use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct Command {
    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,
}

pub(crate) async fn run(command: &Command, context: &Context) -> ExitCode {
    match LoadedAgentManifest::load_from_current_dir(context, command.file.as_deref()).await {
        Ok(manifest) => {
            println!("manifest ok: {}", manifest.source_path().display());
            println!("name: {}", manifest.value().name());
            println!("image.os: {:?}", manifest.value().spec.image.os);
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
