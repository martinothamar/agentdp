#![allow(clippy::expect_used, clippy::missing_panics_doc, clippy::panic)]

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

pub fn assert(cargo_manifest_dir: &str, source_file: &str, name: &str, actual: &str) {
    let topic = Path::new(source_file)
        .file_stem()
        .expect("snapshot source file stem")
        .to_string_lossy();
    assert_topic(cargo_manifest_dir, &topic, name, actual);
}

pub fn assert_topic(cargo_manifest_dir: &str, topic: &str, name: &str, actual: &str) {
    let path = snapshots_dir(cargo_manifest_dir).join(format!("{topic}__{name}.snap"));

    if should_update_snapshots() {
        write_snapshot(&path, actual);
        return;
    }

    let expected = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(source) => {
            panic!(
                "failed to read snapshot {}: {source}\nrun `make test` to update snapshots",
                path.display()
            );
        }
    };

    assert!(
        expected == actual,
        "snapshot mismatch: {}\n\n--- expected\n{}\n--- actual\n{}",
        path.display(),
        expected,
        actual
    );
}

#[must_use]
pub fn render_command(status: i32, stdout: &str, stderr: &str) -> String {
    let mut output = format!("status: {status}\n");
    push_section(&mut output, "stdout", stdout);
    push_section(&mut output, "stderr", stderr);
    output
}

#[must_use]
pub fn render_io(stdout: &str, stderr: &str) -> String {
    let mut output = String::new();
    push_section(&mut output, "stdout", stdout);
    push_section(&mut output, "stderr", stderr);
    output
}

fn snapshots_dir(cargo_manifest_dir: &str) -> PathBuf {
    Path::new(cargo_manifest_dir).join("tests/snapshots")
}

fn should_update_snapshots() -> bool {
    matches!(
        env::var("AGENTDP_UPDATE_SNAPSHOTS").as_deref(),
        Ok("always" | "1" | "true")
    )
}

fn write_snapshot(path: &Path, actual: &str) {
    let parent = path.parent().expect("snapshot parent directory");
    fs::create_dir_all(parent).expect("create snapshot directory");
    fs::write(path, actual).expect("write snapshot");
}

fn push_section(output: &mut String, name: &str, contents: &str) {
    output.push_str("--- ");
    output.push_str(name);
    output.push('\n');
    output.push_str(contents);
    if !contents.ends_with('\n') {
        output.push('\n');
    }
}
