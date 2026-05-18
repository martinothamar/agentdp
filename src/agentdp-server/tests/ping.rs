#![allow(clippy::expect_used, clippy::needless_pass_by_value, clippy::panic)]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

use agentdp_core::platform::{self, LocalSocketError};
use agentdp_protocol::{self as protocol, Request, RequestKind, Response};

#[test]
fn responds_to_ping_over_jsonl_socket() {
    if !matches!(
        platform::host_target(),
        platform::HostTarget::Linux | platform::HostTarget::Wsl2 | platform::HostTarget::Windows
    ) {
        return;
    }

    let temp = TestTempDir::create("p");
    let socket = temp.path().join("s");
    let mut child = ChildGuard::spawn(&socket);

    let Some(response) = wait_for_ping(&socket) else {
        return;
    };
    assert!(response.ok);
    assert_eq!(response.id, "cmd_test");

    child.stop();
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn spawn(socket: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_agentdp-server"))
            .arg("--socket")
            .arg(socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn agentdp-server");
        Self { child }
    }

    fn stop(&mut self) {
        let _result = self.child.kill();
        let _result = self.child.wait();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    fn create(name: &str) -> Self {
        let root = std::env::temp_dir().join("agentdp-server-tests");
        fs::create_dir_all(&root).expect("create test temp root");
        let path = root.join(format!("{name}-{}", std::process::id()));
        let _result = fs::remove_dir_all(&path);
        fs::create_dir(&path).expect("create test temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        let _result = fs::remove_dir_all(&self.path);
    }
}

fn wait_for_ping(socket: &Path) -> Option<Response> {
    let mut last_error = PingError::Other("agentdp-server was not pinged".to_owned());
    for _attempt in 0..40 {
        match ping(socket) {
            Ok(response) => return Some(response),
            Err(PingError::PermissionDenied) => return None,
            Err(error) => {
                last_error = error;
                thread::sleep(Duration::from_millis(50));
            }
        }
    }

    panic!("agentdp-server ping: {last_error}");
}

#[derive(Debug)]
enum PingError {
    PermissionDenied,
    Other(String),
}

impl std::fmt::Display for PingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PermissionDenied => formatter.write_str("permission denied"),
            Self::Other(error) => formatter.write_str(error),
        }
    }
}

fn ping(socket: &Path) -> Result<Response, PingError> {
    let mut stream = platform::connect_local_socket(socket).map_err(map_socket_error)?;
    let request = Request::new("cmd_test", RequestKind::ServerPing);
    stream
        .write_all(
            protocol::encode_line(&request)
                .map_err(|error| PingError::Other(error.to_string()))?
                .as_bytes(),
        )
        .map_err(map_io_error)?;
    stream.flush().map_err(map_io_error)?;

    let mut line = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut line).map_err(map_io_error)?;
    let message = protocol::decode_server_message(&line).map_err(|error| PingError::Other(error.to_string()))?;
    message
        .response
        .ok_or_else(|| PingError::Other("ping response omitted response body".to_owned()))
}

fn map_socket_error(error: LocalSocketError) -> PingError {
    match error {
        LocalSocketError::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            PingError::PermissionDenied
        }
        other => PingError::Other(other.to_string()),
    }
}

fn map_io_error(error: std::io::Error) -> PingError {
    if error.kind() == std::io::ErrorKind::PermissionDenied {
        PingError::PermissionDenied
    } else {
        PingError::Other(error.to_string())
    }
}
