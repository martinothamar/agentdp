use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Output};

use super::snapshot;
use super::tempdir::TestTempDir;

#[derive(Debug)]
pub struct CommandSnapshot {
    status: i32,
    stdout: String,
    stderr: String,
}

impl CommandSnapshot {
    #[must_use]
    pub fn render(&self) -> String {
        snapshot::render_command(self.status, &self.stdout, &self.stderr)
    }

    #[must_use]
    pub fn stdout(&self) -> &str {
        &self.stdout
    }

    #[must_use]
    pub fn stderr(&self) -> &str {
        &self.stderr
    }

    #[must_use]
    pub fn socket_permission_denied(&self) -> bool {
        self.stdout.contains("Operation not permitted") || self.stderr.contains("Operation not permitted")
    }
}

pub struct TestContext {
    repo_root: PathBuf,
    temp: TestTempDir,
}

impl TestContext {
    pub fn new(name: &str) -> Self {
        let repo_root = repo_root();
        let temp_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/.tmp");
        let temp = TestTempDir::create(&temp_root, name);
        Self { repo_root, temp }
    }

    pub fn path(&self) -> &Path {
        self.temp.path()
    }

    pub fn write(&self, path: impl AsRef<Path>, contents: &str) -> PathBuf {
        let path = self.temp.path().join(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create test file parent");
        }
        fs::write(&path, contents).expect("write test file");
        path
    }

    pub fn run<I, S>(&self, args: I) -> CommandSnapshot
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.run_in(&self.repo_root, args)
    }

    pub fn run_in<I, S>(&self, current_dir: &Path, args: I) -> CommandSnapshot
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = agentctl_command();
        command.current_dir(current_dir).args(args);
        self.run_command(command, "run agentctl", &[])
    }

    pub fn run_install_in_temp_home(&self) -> CommandSnapshot {
        let runtime = self.temp.path().join("runtime");
        let mut command = self.agentctl_in_temp_home(&runtime);
        command.args(["self", "install"]);
        self.run_command(command, "run agentctl self install", &[])
    }

    pub fn installed_agentctl_path(&self) -> PathBuf {
        if cfg!(target_os = "windows") {
            self.temp
                .path()
                .join("local_app_data")
                .join("agentdp")
                .join("bin")
                .join("agentctl.exe")
        } else {
            self.temp
                .path()
                .join("home")
                .join(".local")
                .join("bin")
                .join("agentctl")
        }
    }

    pub fn run_installed<I, S>(&self, args: I) -> CommandSnapshot
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = ProcessCommand::new(self.installed_agentctl_path());
        command.current_dir(&self.repo_root).args(args);
        self.run_command(command, "run installed agentctl", &[])
    }

    pub fn agentctl_in_temp_home(&self, runtime: &Path) -> ProcessCommand {
        let mut command = agentctl_command();
        command.current_dir(&self.repo_root);
        self.add_temp_home_env(&mut command, runtime);
        command
    }

    pub fn run_command(
        &self,
        mut command: ProcessCommand,
        expectation: &str,
        replacements: &[(&Path, &str)],
    ) -> CommandSnapshot {
        let output = command.output().expect(expectation);
        self.snapshot_with_replacements(&output, replacements)
    }

    pub fn write_fake_ssh_keygen(&self) -> PathBuf {
        self.write_fake_tool("ssh-keygen", fake_ssh_keygen_script())
    }

    pub fn write_fake_ssh(&self) -> PathBuf {
        self.write_fake_tool("ssh", fake_ssh_script())
    }

    fn add_temp_home_env(&self, command: &mut ProcessCommand, runtime: &Path) {
        command
            .env("HOME", self.temp.path().join("home"))
            .env("USERPROFILE", self.temp.path().join("home"))
            .env("XDG_DATA_HOME", self.temp.path().join("data"))
            .env("XDG_CONFIG_HOME", self.temp.path().join("config"))
            .env("XDG_CACHE_HOME", self.temp.path().join("cache"))
            .env("XDG_STATE_HOME", self.temp.path().join("state"))
            .env("XDG_RUNTIME_DIR", runtime)
            .env("LOCALAPPDATA", self.temp.path().join("local_app_data"))
            .env("APPDATA", self.temp.path().join("app_data"));
    }

    fn write_fake_tool(&self, name: &str, contents: &str) -> PathBuf {
        let executable = executable_script_name(name);
        let path = self.write(Path::new(&executable), contents);
        agentdp_core::platform::set_executable(&path).expect("make fake tool executable");
        path
    }

    fn snapshot_with_replacements(&self, output: &Output, replacements: &[(&Path, &str)]) -> CommandSnapshot {
        CommandSnapshot {
            status: output.status.code().unwrap_or(-1),
            stdout: self.normalize(&output.stdout, replacements),
            stderr: self.normalize(&output.stderr, replacements),
        }
    }

    fn normalize(&self, bytes: &[u8], replacements: &[(&Path, &str)]) -> String {
        let mut text = String::from_utf8_lossy(bytes).replace("\r\n", "\n");
        for (path, replacement) in replacements {
            text = replace_path(&text, path, replacement);
        }
        let text = replace_path(&text, self.temp.path(), "$TMP");
        let text = replace_path(&text, &self.repo_root, "$REPO");
        normalize_snapshot_paths(&normalize_help_binary_names(&text))
    }
}

#[cfg(unix)]
const fn fake_ssh_keygen_script() -> &'static str {
    "#!/bin/sh\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"-f\" ]; then\n    shift\n    printf 'private key\\n' > \"$1\"\n    printf 'ssh-ed25519 AAAATEST agentdp\\n' > \"$1.pub\"\n    exit 0\n  fi\n  shift\ndone\nexit 1\n"
}

#[cfg(unix)]
const fn fake_ssh_script() -> &'static str {
    "#!/bin/sh\nlast=\nfor arg do\n  last=$arg\ndone\ncase \"$last\" in\n  *'cloud-init status'*) printf 'status: done\\n'; exit 0 ;;\n  *hello*) printf 'hello from guest\\n'; exit 0 ;;\n  *fail*) printf 'guest failure\\n' >&2; exit 7 ;;\n  *) printf 'fake ssh: %s\\n' \"$last\"; exit 0 ;;\nesac\n"
}

#[cfg(windows)]
const fn fake_ssh_keygen_script() -> &'static str {
    "@echo off\r\necho private key> \"%~8\"\r\necho ssh-ed25519 AAAATEST agentdp> \"%~8.pub\"\r\nexit /b 0\r\n"
}

#[cfg(windows)]
const fn fake_ssh_script() -> &'static str {
    "@echo off\r\necho hello from guest\r\n"
}

#[cfg(windows)]
fn executable_script_name(name: &str) -> String {
    format!("{name}.cmd")
}

#[cfg(unix)]
fn executable_script_name(name: &str) -> String {
    name.to_owned()
}

fn agentctl_command() -> ProcessCommand {
    ProcessCommand::new(env!("CARGO_BIN_EXE_agentctl"))
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("repo root")
        .to_path_buf()
}

fn replace_path(text: &str, path: &Path, replacement: &str) -> String {
    let path = path.display().to_string();
    let normalized = path.replace('\\', "/");
    let extended = format!(r"\\?\{path}");
    let extended_normalized = format!("//?/{normalized}");
    text.replace(&extended, replacement)
        .replace(&extended_normalized, replacement)
        .replace(&path, replacement)
        .replace(&normalized, replacement)
}

fn normalize_help_binary_names(text: &str) -> String {
    text.replace("Usage: agentctl.exe", "Usage: agentctl")
}

fn normalize_snapshot_paths(text: &str) -> String {
    let text = text
        .replace('\\', "/")
        .replace("$TMP/local_app_data/agentdp/bin", "$TMP/home/.local/bin")
        .replace("$REPO/target/debug/agentctl.exe", "$REPO/target/debug/agentctl")
        .replace(
            "$REPO/target/debug/agentdp-server.exe",
            "$REPO/target/debug/agentdp-server",
        )
        .replace("$TMP/home/.local/bin/agentctl.exe", "$TMP/home/.local/bin/agentctl")
        .replace(
            "$TMP/home/.local/bin/agentdp-server.exe",
            "$TMP/home/.local/bin/agentdp-server",
        );
    collapse_repo_temp_paths(&text)
}

fn collapse_repo_temp_paths(text: &str) -> String {
    const PREFIX: &str = "$REPO/src/agentdp-cli/tests/.tmp/";
    let mut output = String::with_capacity(text.len());
    let mut remaining = text;

    while let Some(index) = remaining.find(PREFIX) {
        output.push_str(&remaining[..index]);
        let after_prefix = &remaining[index + PREFIX.len()..];
        output.push_str("$TMP");
        if let Some(end) = after_prefix.find(['/', ';', ' ', '\n', '\r']) {
            remaining = &after_prefix[end..];
        } else {
            remaining = "";
        }
    }

    output.push_str(remaining);
    output
}
