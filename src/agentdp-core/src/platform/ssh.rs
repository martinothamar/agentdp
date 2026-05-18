use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::Context;

pub const SSH_KEYGEN_PATH_ENV: &str = "AGENTDP_SSH_KEYGEN_PATH";
pub const SSH_PATH_ENV: &str = "AGENTDP_SSH_PATH";
pub const SSH_BINARY: &str = "ssh";

const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPair {
    pub private_key: PathBuf,
    pub public_key: PathBuf,
    pub public_key_contents: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionInfo {
    pub user: String,
    pub host: String,
    pub port: u16,
    pub private_key: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandPrivilege {
    User,
    Root,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshKeygen {
    binary: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshClient {
    binary: PathBuf,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("ssh-keygen was not found; install ssh-keygen or set {SSH_KEYGEN_PATH_ENV}")]
    MissingSshKeygen,
    #[error("ssh was not found; install ssh or set {SSH_PATH_ENV}")]
    MissingSsh,
    #[error("failed to create SSH key directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to remove stale SSH key {path}: {source}")]
    RemoveStaleKey {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to run ssh-keygen: {0}")]
    RunKeygen(#[source] std::io::Error),
    #[error("ssh-keygen failed: {stderr}")]
    KeygenFailed { stderr: String },
    #[error("failed to read generated SSH public key {path}: {source}")]
    ReadPublicKey {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("generated SSH public key {path} was empty")]
    EmptyPublicKey { path: PathBuf },
    #[error("failed to run ssh: {0}")]
    RunSsh(#[source] std::io::Error),
    #[error("guest command timed out after {timeout_seconds}s")]
    CommandTimedOut { timeout_seconds: u64 },
}

impl Error {
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::CommandTimedOut { .. })
    }
}

impl SshKeygen {
    #[must_use]
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self { binary: binary.into() }
    }

    /// Resolves the configured or PATH-discovered `ssh-keygen` executable.
    ///
    /// # Errors
    ///
    /// Returns an error when `AGENTDP_SSH_KEYGEN_PATH` is unset and
    /// `ssh-keygen` cannot be found on `PATH`.
    pub fn resolve() -> Result<Self, Error> {
        if let Some(path) = std::env::var_os(SSH_KEYGEN_PATH_ENV).filter(|value| !value.is_empty()) {
            return Ok(Self::new(path));
        }
        let binary = super::host::find_binary("ssh-keygen").ok_or(Error::MissingSshKeygen)?;
        Ok(Self::new(binary))
    }

    /// Generates an ed25519 key pair in `work_dir`.
    ///
    /// # Errors
    ///
    /// Returns an error when the key directory cannot be prepared,
    /// `ssh-keygen` cannot be started or exits unsuccessfully, or the generated
    /// public key cannot be read.
    pub fn generate_key_pair(&self, context: &Context, work_dir: &Path) -> Result<KeyPair, Error> {
        let ssh_dir = work_dir.join("ssh");
        fs::create_dir_all(&ssh_dir).map_err(|source| Error::CreateDirectory {
            path: ssh_dir.clone(),
            source,
        })?;
        let private_key = ssh_dir.join("agentdp_ed25519");
        let public_key = private_key.with_extension("pub");
        remove_stale_key(&private_key)?;
        remove_stale_key(&public_key)?;

        context
            .logger()
            .verbose_with(|| format!("generating instance SSH key {}", private_key.display()));
        let output = Command::new(&self.binary)
            .arg("-t")
            .arg("ed25519")
            .arg("-N")
            .arg("")
            .arg("-C")
            .arg("agentdp")
            .arg("-f")
            .arg(&private_key)
            .arg("-q")
            .output()
            .map_err(Error::RunKeygen)?;
        if !output.status.success() {
            return Err(Error::KeygenFailed {
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }

        let public_key_contents = fs::read_to_string(&public_key).map_err(|source| Error::ReadPublicKey {
            path: public_key.clone(),
            source,
        })?;
        let public_key_contents = public_key_contents.trim().to_owned();
        if public_key_contents.is_empty() {
            return Err(Error::EmptyPublicKey { path: public_key });
        }

        Ok(KeyPair {
            private_key,
            public_key,
            public_key_contents,
        })
    }
}

impl SshClient {
    #[must_use]
    fn new(binary: impl Into<PathBuf>) -> Self {
        Self { binary: binary.into() }
    }

    /// Resolves the configured or PATH-discovered `ssh` executable.
    ///
    /// # Errors
    ///
    /// Returns an error when `AGENTDP_SSH_PATH` is unset and `ssh` cannot be
    /// found on `PATH`.
    pub fn resolve() -> Result<Self, Error> {
        if let Some(path) = std::env::var_os(SSH_PATH_ENV).filter(|value| !value.is_empty()) {
            return Ok(Self::new(path));
        }
        let binary = super::host::find_binary(SSH_BINARY).ok_or(Error::MissingSsh)?;
        Ok(Self::new(binary))
    }

    /// Runs a command through SSH and waits for completion or timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when `ssh` cannot be started, process polling fails, or
    /// the timeout elapses before the command exits.
    pub fn run_command_with_timeout(
        &self,
        context: &Context,
        connection: &ConnectionInfo,
        command: &str,
        timeout: Duration,
        privilege: CommandPrivilege,
    ) -> Result<CommandOutput, Error> {
        context.logger().verbose_with(|| {
            format!(
                "running guest command over SSH on {}@{}:{}: {command}",
                connection.user, connection.host, connection.port
            )
        });
        let output = run_with_timeout(
            Command::new(&self.binary).args(command_args(connection, command, privilege)),
            timeout,
        )?;
        let status = output.status.code().unwrap_or(1);
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        Ok(CommandOutput { status, stdout, stderr })
    }
}

#[must_use]
fn command_args(connection: &ConnectionInfo, command: &str, privilege: CommandPrivilege) -> Vec<String> {
    let mut args = base_args(connection);
    args.push(format!("{}@{}", connection.user, connection.host));
    match privilege {
        CommandPrivilege::User => args.push(format!("sh -lc {}", shell_single_quote(command))),
        CommandPrivilege::Root => args.push(format!("sudo -n sh -lc {}", shell_single_quote(command))),
    }
    args
}

#[must_use]
pub fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|arg| shell_single_quote(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

#[must_use]
pub fn interactive_shell_args(connection: &ConnectionInfo, remote_command: &str) -> Vec<String> {
    let mut args = base_args(connection);
    args.push("-t".to_owned());
    args.push(format!("{}@{}", connection.user, connection.host));
    args.push(remote_command.to_owned());
    args
}

fn base_args(connection: &ConnectionInfo) -> Vec<String> {
    vec![
        "-i".to_owned(),
        path_text(&connection.private_key),
        "-p".to_owned(),
        connection.port.to_string(),
        "-o".to_owned(),
        "BatchMode=yes".to_owned(),
        "-o".to_owned(),
        "ConnectTimeout=5".to_owned(),
        "-o".to_owned(),
        "StrictHostKeyChecking=no".to_owned(),
        "-o".to_owned(),
        "UserKnownHostsFile=/dev/null".to_owned(),
        "-o".to_owned(),
        "LogLevel=ERROR".to_owned(),
    ]
}

fn remove_stale_key(path: &Path) -> Result<(), Error> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::RemoveStaleKey {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn path_text(path: &Path) -> String {
    path.display().to_string()
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn run_with_timeout(command: &mut Command, timeout: Duration) -> Result<Output, Error> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(Error::RunSsh)?;
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait().map_err(Error::RunSsh)?.is_some() {
            return child.wait_with_output().map_err(Error::RunSsh);
        }
        if Instant::now() >= deadline {
            let _result = child.kill();
            let _result = child.wait();
            return Err(Error::CommandTimedOut {
                timeout_seconds: timeout.as_secs(),
            });
        }
        thread::sleep(WAIT_POLL_INTERVAL);
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::platform::ssh::{CommandPrivilege, ConnectionInfo, command_args, interactive_shell_args, shell_join};

    #[test]
    fn builds_privileged_non_interactive_command_args() {
        let connection = connection();

        assert_eq!(
            command_args(&connection, "docker ps", CommandPrivilege::Root),
            [
                "-i",
                "/instances/pr-0/generated/qemu/ssh/agentdp_ed25519",
                "-p",
                "2222",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                "arch@127.0.0.1",
                "sudo -n sh -lc 'docker ps'",
            ]
        );
    }

    #[test]
    fn quotes_guest_commands_for_remote_shell() {
        let connection = connection();

        assert_eq!(
            command_args(&connection, "printf '%s\\n' hello", CommandPrivilege::Root)
                .last()
                .unwrap(),
            "sudo -n sh -lc 'printf '\"'\"'%s\\n'\"'\"' hello'"
        );
    }

    #[test]
    fn builds_user_non_interactive_command_args() {
        let connection = connection();

        assert_eq!(
            command_args(&connection, "id -un", CommandPrivilege::User)
                .last()
                .unwrap(),
            "sh -lc 'id -un'"
        );
    }

    #[test]
    fn builds_interactive_shell_args() {
        let connection = connection();

        assert_eq!(
            interactive_shell_args(
                &connection,
                "cd /data/home/code 2>/dev/null || cd; exec ${SHELL:-/bin/sh} -l",
            ),
            [
                "-i",
                "/instances/pr-0/generated/qemu/ssh/agentdp_ed25519",
                "-p",
                "2222",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                "-t",
                "arch@127.0.0.1",
                "cd /data/home/code 2>/dev/null || cd; exec ${SHELL:-/bin/sh} -l",
            ]
        );
    }

    #[test]
    fn shell_join_quotes_command_arguments() {
        assert_eq!(
            shell_join(&[
                "printf".to_owned(),
                "%s\n".to_owned(),
                "hello world".to_owned(),
                "it's ok".to_owned(),
            ]),
            "'printf' '%s\n' 'hello world' 'it'\"'\"'s ok'"
        );
    }

    fn connection() -> ConnectionInfo {
        ConnectionInfo {
            user: "arch".to_owned(),
            host: "127.0.0.1".to_owned(),
            port: 2222,
            private_key: PathBuf::from("/instances/pr-0/generated/qemu/ssh/agentdp_ed25519"),
        }
    }
}
