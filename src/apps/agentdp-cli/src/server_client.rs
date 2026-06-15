use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agentdp_core::{Context, layout::AgentdpLayout};
use agentdp_platform::{self as platform, socket::LocalSocketError};
use agentdp_protocol::client_server::{
    self as protocol, Event, EventKind, EventLevel, PingResult, Request, RequestKind, ServerMessage, ShutdownResult,
};
use agentdp_protocol::jsonl::{self, JsonLineReader, ReadJsonLine};
use serde::de::DeserializeOwned;
use thiserror::Error;

const SERVER_PATH_ENV: &str = "AGENTDP_SERVER_PATH";
const START_RETRY_COUNT: usize = 40;
const START_RETRY_DELAY: Duration = Duration::from_millis(50);
const RESPONSE_TIMEOUT_ENV: &str = "AGENTDP_SERVER_RESPONSE_TIMEOUT_MS";
const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_mins(2);
const CONTROL_RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);
const SERVER_STOP_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("local socket error: {0}")]
    Socket(#[from] LocalSocketError),
    #[error("I/O error while talking to agentdp-server: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] protocol::Error),
    #[error("agentdp-server returned error {code}: {message}")]
    Server { code: String, message: String },
    #[error("agentdp-server returned an invalid response: {0}")]
    InvalidResponse(String),
    #[error("agentdp-server binary was not found; set {SERVER_PATH_ENV} or put agentdp-server beside agentctl/on PATH")]
    ServerNotFound,
    #[error("failed to spawn agentdp-server: {0}")]
    Spawn(#[from] platform::process::DetachedSpawnError),
    #[error("failed to terminate agentdp-server: {0}")]
    Terminate(#[from] platform::process::TerminateProcessError),
    #[error("failed to inspect agentdp-server process: {0}")]
    ProcessStatus(#[from] platform::process::ProcessStatusError),
    #[error("agentdp-server did not stop after termination request")]
    ServerStillRunning,
    #[error("agentdp-server did not respond within {timeout_ms}ms")]
    ServerResponseTimedOut { timeout_ms: u128 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Ping {
    pub socket: PathBuf,
    pub pid: u32,
    pub version: Option<String>,
    pub executable: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Stop {
    NotRunning,
    Stopped(Ping),
}

/// Ensures agentdp-server is running and responding to ping.
///
/// # Errors
///
/// Returns an error when an existing server cannot be contacted and a new
/// server cannot be started or reached.
pub(crate) async fn ensure_running(context: &Context, layout: &AgentdpLayout) -> Result<Ping, Error> {
    match ping(layout).await {
        Ok(ping) => return Ok(ping),
        Err(Error::ServerResponseTimedOut { .. }) if cleanup_unowned_server_socket(context, layout).await? => {}
        Err(error) if should_start_after_ping_error(&error) => {
            context
                .logger()
                .verbose_with(|| format!("agentdp-server ping failed before start: {error}"));
        }
        Err(error) => return Err(error),
    }

    start(context, layout).await?;
    for _attempt in 0..START_RETRY_COUNT {
        match ping(layout).await {
            Ok(ping) => return Ok(ping),
            Err(error) if should_start_after_ping_error(&error) => {
                tokio::time::sleep(START_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }

    ping(layout).await
}

pub(crate) async fn request<T: DeserializeOwned>(
    context: &Context,
    layout: &AgentdpLayout,
    kind: RequestKind,
    on_event: Option<&mut (dyn FnMut(Event) + Send)>,
) -> Result<T, Error> {
    request_with_response_timeout(context, layout, kind, on_event, response_timeout()).await
}

pub(crate) async fn request_with_response_timeout<T: DeserializeOwned>(
    context: &Context,
    layout: &AgentdpLayout,
    kind: RequestKind,
    on_event: Option<&mut (dyn FnMut(Event) + Send)>,
    timeout: Duration,
) -> Result<T, Error> {
    let _ping = ensure_running(context, layout).await?;
    let request = protocol::request(kind);
    send_with_timeout(layout, &request, on_event, timeout).await
}

pub(crate) async fn watch_agent(
    context: &Context,
    layout: &AgentdpLayout,
    params: protocol::AgentWatchParams,
    mut on_event: impl FnMut(Event),
) -> Result<(), Error> {
    let _ping = ensure_running(context, layout).await?;
    let request = protocol::request(RequestKind::AgentWatch(params));
    let mut stream = platform::socket::connect_local_socket(&layout.socket_path()).await?;
    let mut frame = Vec::new();
    jsonl::encode_into(&request, &mut frame)?;
    stream.write_all(&frame).await?;
    stream.flush().await?;
    let mut reader = JsonLineReader::default();
    frame.clear();

    loop {
        let Some(message) = read_response_message_without_timeout(&mut stream, &mut reader, &mut frame).await? else {
            return Err(Error::InvalidResponse(
                "server closed watch connection before response".to_owned(),
            ));
        };
        match message {
            ServerMessage::Event(event) => {
                if event.id != request.id {
                    return Err(Error::InvalidResponse(format!(
                        "expected event id {}, got {}",
                        request.id, event.id
                    )));
                }
                on_event(event);
            }
            ServerMessage::Response(response) => {
                let _ignored: serde_json::Value = decode_response(&request, response)?;
                return Ok(());
            }
        }
    }
}

pub(crate) async fn stop_if_running(context: &Context, layout: &AgentdpLayout) -> Result<Stop, Error> {
    let ping = match ping(layout).await {
        Ok(ping) => ping,
        Err(Error::Socket(LocalSocketError::Unsupported)) => return Ok(Stop::NotRunning),
        Err(Error::ServerResponseTimedOut { .. }) if cleanup_unowned_server_socket(context, layout).await? => {
            return Ok(Stop::NotRunning);
        }
        Err(Error::ServerResponseTimedOut { .. }) => return stop_unresponsive_lock_owner(context, layout).await,
        Err(error) if should_start_after_ping_error(&error) => return Ok(Stop::NotRunning),
        Err(error) => return Err(error),
    };

    context.logger().verbose_with(|| {
        format!(
            "stopping running agentdp-server pid {} before refreshing installed binary",
            ping.pid
        )
    });
    stop_running(context, layout, &ping).await?;
    Ok(Stop::Stopped(ping))
}

async fn stop_unresponsive_lock_owner(context: &Context, layout: &AgentdpLayout) -> Result<Stop, Error> {
    let socket = layout.socket_path();
    let lock = socket.with_extension("lock");
    let Some(pid) = live_lock_owner_pid(&lock).await else {
        return Ok(Stop::NotRunning);
    };

    context.logger().verbose_with(|| {
        format!(
            "terminating unresponsive agentdp-server pid {pid} recorded by {}",
            lock.display()
        )
    });
    platform::process::terminate_process(pid).await?;
    if !platform::process::wait_for_process_exit(pid, SERVER_STOP_TIMEOUT).await? {
        return Err(Error::ServerStillRunning);
    }
    remove_socket_and_lock(&socket).await?;
    Ok(Stop::Stopped(Ping {
        socket,
        pid,
        version: None,
        executable: None,
    }))
}

pub(crate) async fn start_server_from(
    context: &Context,
    layout: &AgentdpLayout,
    server: &std::path::Path,
) -> Result<Ping, Error> {
    start_from(context, layout, server).await?;
    wait_for_ping(layout).await
}

async fn ping(layout: &AgentdpLayout) -> Result<Ping, Error> {
    ping_with_timeout(layout, CONTROL_RESPONSE_TIMEOUT).await
}

async fn ping_with_timeout(layout: &AgentdpLayout, response_timeout: Duration) -> Result<Ping, Error> {
    let socket = layout.socket_path();
    let request = protocol::request(RequestKind::ServerPing);
    let response: PingResult = send_with_timeout(layout, &request, None, response_timeout).await?;
    if response.service != "agentdp-server" {
        return Err(Error::InvalidResponse(
            "server.ping response omitted service marker".to_owned(),
        ));
    }

    Ok(Ping {
        socket,
        pid: response.pid,
        version: response.version,
        executable: response.executable,
    })
}

async fn send_with_timeout<T: DeserializeOwned>(
    layout: &AgentdpLayout,
    request: &Request,
    mut on_event: Option<&mut (dyn FnMut(Event) + Send)>,
    response_timeout: Duration,
) -> Result<T, Error> {
    let mut stream = platform::socket::connect_local_socket(&layout.socket_path()).await?;
    let mut frame = Vec::new();
    jsonl::encode_into(&request, &mut frame)?;
    stream.write_all(&frame).await?;
    stream.flush().await?;
    let mut reader = JsonLineReader::default();
    frame.clear();

    loop {
        let Some(message) = read_response_message(&mut stream, &mut reader, &mut frame, response_timeout).await? else {
            return Err(Error::InvalidResponse(
                "server closed connection before response".to_owned(),
            ));
        };
        match message {
            ServerMessage::Event(event) => {
                if event.id != request.id {
                    return Err(Error::InvalidResponse(format!(
                        "expected event id {}, got {}",
                        request.id, event.id
                    )));
                }
                if let Some(on_event) = &mut on_event {
                    on_event(event);
                }
            }
            ServerMessage::Response(response) => {
                return decode_response(request, response);
            }
        }
    }
}

async fn read_response_message(
    stream: &mut platform::socket::AsyncLocalSocket,
    reader: &mut JsonLineReader,
    frame: &mut Vec<u8>,
    timeout: Duration,
) -> Result<Option<protocol::ServerMessage>, Error> {
    let read = tokio::time::timeout(
        timeout,
        jsonl::read::<protocol::ServerMessage, _>(reader, stream, frame),
    )
    .await;
    match read {
        Ok(Ok(ReadJsonLine::Value(message))) => Ok(Some(message)),
        Ok(Ok(ReadJsonLine::Eof)) => Ok(None),
        Err(_elapsed) => Err(Error::ServerResponseTimedOut {
            timeout_ms: timeout.as_millis(),
        }),
        Ok(Err(protocol::Error::Read(error))) => {
            if matches!(
                error.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            ) {
                Err(Error::ServerResponseTimedOut {
                    timeout_ms: timeout.as_millis(),
                })
            } else {
                Err(Error::Io(error))
            }
        }
        Ok(Err(error)) => Err(Error::Protocol(error)),
    }
}

async fn read_response_message_without_timeout(
    stream: &mut platform::socket::AsyncLocalSocket,
    reader: &mut JsonLineReader,
    frame: &mut Vec<u8>,
) -> Result<Option<protocol::ServerMessage>, Error> {
    match jsonl::read::<protocol::ServerMessage, _>(reader, stream, frame).await {
        Ok(ReadJsonLine::Value(message)) => Ok(Some(message)),
        Ok(ReadJsonLine::Eof) => Ok(None),
        Err(protocol::Error::Read(error)) => {
            if matches!(
                error.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            ) {
                Err(Error::ServerResponseTimedOut {
                    timeout_ms: response_timeout().as_millis(),
                })
            } else {
                Err(Error::Io(error))
            }
        }
        Err(error) => Err(Error::Protocol(error)),
    }
}

fn decode_response<T: DeserializeOwned>(request: &Request, response: protocol::Response) -> Result<T, Error> {
    if response.id() != request.id {
        return Err(Error::InvalidResponse(format!(
            "expected response id {}, got {}",
            request.id,
            response.id()
        )));
    }
    if response.is_error() {
        let error = response
            .into_error()
            .ok_or_else(|| Error::InvalidResponse("error response omitted error body".to_owned()))?;
        return Err(Error::Server {
            code: error.code,
            message: error.message,
        });
    }

    response.result().map_err(Error::Protocol)
}

pub(crate) fn log_event(context: &Context, event: Event) {
    match event.event {
        EventKind::Diagnostic { level, message } => match level {
            EventLevel::Info => context.logger().info(message),
            EventLevel::Warn => context.logger().warn(message),
            EventLevel::Error => context.logger().error(message),
            EventLevel::Verbose => context.logger().verbose(message),
        },
        EventKind::SessionOutput { chunk, .. } => context.logger().info(chunk),
        EventKind::AgentDocumentChanged { .. } | EventKind::AgentEvent { .. } => {}
    }
}

async fn start(context: &Context, layout: &AgentdpLayout) -> Result<(), Error> {
    let server = resolve_server_binary().await?;
    start_from(context, layout, &server).await
}

async fn start_from(context: &Context, layout: &AgentdpLayout, server: &std::path::Path) -> Result<(), Error> {
    context
        .logger()
        .verbose_with(|| format!("starting agentdp-server from {}", server.display()));
    platform::process::spawn_detached(
        server,
        &[OsString::from("--socket"), layout.socket_path().into_os_string()],
    )
    .await?;
    Ok(())
}

async fn stop_running(context: &Context, layout: &AgentdpLayout, running: &Ping) -> Result<(), Error> {
    match shutdown(layout).await {
        Ok(()) => {}
        Err(Error::ServerResponseTimedOut { .. }) => {
            context.logger().verbose_with(|| {
                format!(
                    "timed out waiting for agentdp-server pid {} shutdown response; checking whether it exited",
                    running.pid
                )
            });
        }
        Err(Error::Server { code, .. }) if code == "unknown_method" => {
            context
                .logger()
                .verbose("running agentdp-server does not support server.shutdown; terminating by pid");
            platform::process::terminate_process(running.pid).await?;
        }
        Err(error) if should_start_after_ping_error(&error) => return Ok(()),
        Err(error) => return Err(error),
    }

    if platform::process::wait_for_process_exit(running.pid, SERVER_STOP_TIMEOUT).await? {
        remove_server_socket_and_lock(layout).await?;
        return Ok(());
    }

    context.logger().verbose_with(|| {
        format!(
            "agentdp-server pid {} did not exit after shutdown request; terminating",
            running.pid
        )
    });
    platform::process::terminate_process(running.pid).await?;
    if !platform::process::wait_for_process_exit(running.pid, SERVER_STOP_TIMEOUT).await? {
        return Err(Error::ServerStillRunning);
    }
    remove_server_socket_and_lock(layout).await?;
    Ok(())
}

async fn shutdown(layout: &AgentdpLayout) -> Result<(), Error> {
    let request = protocol::request(RequestKind::ServerShutdown);
    let response: ShutdownResult = send_with_timeout(layout, &request, None, CONTROL_RESPONSE_TIMEOUT).await?;
    if response.shutdown {
        Ok(())
    } else {
        Err(Error::InvalidResponse(
            "server.shutdown response omitted shutdown marker".to_owned(),
        ))
    }
}

async fn wait_for_ping(layout: &AgentdpLayout) -> Result<Ping, Error> {
    for _attempt in 0..START_RETRY_COUNT {
        match ping(layout).await {
            Ok(ping) => return Ok(ping),
            Err(error) if should_start_after_ping_error(&error) => tokio::time::sleep(START_RETRY_DELAY).await,
            Err(error) => return Err(error),
        }
    }
    ping(layout).await
}

const fn should_start_after_ping_error(error: &Error) -> bool {
    matches!(error, Error::Socket(LocalSocketError::Io(_)) | Error::Io(_))
}

fn response_timeout() -> Duration {
    std::env::var(RESPONSE_TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map_or(DEFAULT_RESPONSE_TIMEOUT, Duration::from_millis)
}

async fn cleanup_unowned_server_socket(context: &Context, layout: &AgentdpLayout) -> Result<bool, Error> {
    let socket = layout.socket_path();
    let lock = socket.with_extension("lock");
    if live_lock_owner(&lock).await {
        return Ok(false);
    }

    context.logger().verbose_with(|| {
        format!(
            "removing unresponsive agentdp-server socket without a live lock owner: {}",
            socket.display()
        )
    });
    remove_socket_and_lock(&socket).await?;
    Ok(true)
}

async fn live_lock_owner(lock: &Path) -> bool {
    live_lock_owner_pid(lock).await.is_some()
}

async fn live_lock_owner_pid(lock: &Path) -> Option<u32> {
    let Ok(contents) = tokio::fs::read_to_string(lock).await else {
        return None;
    };
    let pid = lock_owner_pid_from_contents(&contents)?;
    if matches!(
        platform::process::process_status(pid).await,
        Ok(platform::process::ProcessStatus::Running)
    ) {
        Some(pid)
    } else {
        None
    }
}

fn lock_owner_pid_from_contents(contents: &str) -> Option<u32> {
    contents.lines().find_map(lock_owner_pid_from_line)
}

fn lock_owner_pid_from_line(line: &str) -> Option<u32> {
    line.strip_prefix("pid=")?.trim().parse().ok()
}

async fn remove_file_if_exists(path: &Path) -> Result<(), Error> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::Io(error)),
    }
}

async fn remove_server_socket_and_lock(layout: &AgentdpLayout) -> Result<(), Error> {
    remove_socket_and_lock(&layout.socket_path()).await
}

async fn remove_socket_and_lock(socket: &Path) -> Result<(), Error> {
    remove_file_if_exists(socket).await?;
    remove_file_if_exists(&socket.with_extension("lock")).await
}

async fn resolve_server_binary() -> Result<PathBuf, Error> {
    if let Some(path) = std::env::var_os(SERVER_PATH_ENV).filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(path));
    }

    if let Ok(current_exe) = std::env::current_exe()
        && let Some(directory) = current_exe.parent()
    {
        let sibling = directory.join(format!("agentdp-server{}", std::env::consts::EXE_SUFFIX));
        if tokio::fs::metadata(&sibling)
            .await
            .is_ok_and(|metadata| metadata.is_file())
        {
            return Ok(sibling);
        }
    }

    platform::host::find_binary(&format!("agentdp-server{}", std::env::consts::EXE_SUFFIX))
        .await
        .ok_or(Error::ServerNotFound)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn send_times_out_when_server_accepts_without_response() -> Result<(), Box<dyn std::error::Error>> {
        let temp = ShortTempDir::create()?;
        let layout = AgentdpLayout::from_root(temp.path.join("agentdp"));
        let listener = platform::socket::bind_local_socket(&layout.socket_path()).await?;
        let server = tokio::spawn(async move {
            let Ok(_stream) = listener.accept().await else {
                return;
            };
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let request = protocol::request(RequestKind::ServerPing);
        let result: Result<PingResult, Error> =
            send_with_timeout(&layout, &request, None, Duration::from_millis(50)).await;

        assert!(matches!(result, Err(Error::ServerResponseTimedOut { timeout_ms: 50 })));
        let _result = server.await;
        Ok(())
    }

    struct ShortTempDir {
        path: PathBuf,
    }

    impl ShortTempDir {
        fn create() -> Result<Self, Box<dyn std::error::Error>> {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let leaf = if cfg!(target_os = "windows") {
                format!("adp{:x}{timestamp:x}", std::process::id())
            } else {
                format!("agentdp-server-client-{:x}-{timestamp:x}", std::process::id())
            };
            let path = std::env::temp_dir().join(leaf);
            fs::create_dir(&path)?;
            Ok(Self { path })
        }
    }

    impl Drop for ShortTempDir {
        fn drop(&mut self) {
            let _result = fs::remove_dir_all(&self.path);
        }
    }
}
