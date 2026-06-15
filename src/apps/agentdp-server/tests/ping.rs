#![allow(clippy::expect_used, clippy::needless_pass_by_value, clippy::panic)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use agentdp_platform::{self as platform, socket::LocalSocketError};
use agentdp_protocol::client_server::{self as protocol, Request, RequestKind, Response, ServerMessage};
use agentdp_protocol::jsonl::{self, JsonLineReader, ReadJsonLine};

#[tokio::test(flavor = "current_thread")]
async fn responds_to_ping_over_jsonl_socket() {
    if !matches!(
        platform::host::host_target().await,
        platform::host::HostTarget::Linux | platform::host::HostTarget::Wsl2 | platform::host::HostTarget::Windows
    ) {
        return;
    }

    let temp = TestTempDir::create("p");
    let socket = temp.path().join("s");
    let mut child = ChildGuard::spawn(&socket);

    let Some(response) = wait_for_ping(&socket).await else {
        return;
    };
    assert!(response.is_ok());
    assert_eq!(response.id(), "cmd_test");

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

async fn wait_for_ping(socket: &Path) -> Option<Response> {
    let mut last_error = PingError::Other("agentdp-server was not pinged".to_owned());
    for _attempt in 0..40 {
        match ping(socket).await {
            Ok(response) => return Some(response),
            Err(PingError::PermissionDenied) => return None,
            Err(error) => {
                last_error = error;
                tokio::time::sleep(Duration::from_millis(50)).await;
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

async fn ping(socket: &Path) -> Result<Response, PingError> {
    let mut stream = platform::socket::connect_local_socket(socket)
        .await
        .map_err(map_socket_error)?;
    let request = Request::new("cmd_test", RequestKind::ServerPing);
    let mut frame = Vec::new();
    jsonl::encode_into(&request, &mut frame).map_err(|error| PingError::Other(error.to_string()))?;
    stream.write_all(&frame).await.map_err(map_io_error)?;
    stream.flush().await.map_err(map_io_error)?;

    let mut reader = JsonLineReader::default();
    frame.clear();
    let read = tokio::time::timeout(
        Duration::from_secs(2),
        jsonl::read::<protocol::ServerMessage, _>(&mut reader, &mut stream, &mut frame),
    )
    .await;
    let message = match read {
        Ok(Ok(ReadJsonLine::Value(message))) => message,
        Ok(Ok(ReadJsonLine::Eof)) => return Err(PingError::Other("server closed before response".to_owned())),
        Err(_elapsed) => return Err(PingError::Other("server response timed out".to_owned())),
        Ok(Err(agentdp_protocol::Error::Read(error))) => return Err(map_io_error(error)),
        Ok(Err(error)) => return Err(PingError::Other(error.to_string())),
    };
    match message {
        ServerMessage::Response(response) => Ok(response),
        ServerMessage::Event(_) => Err(PingError::Other("ping returned event instead of response".to_owned())),
    }
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
