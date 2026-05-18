use std::ffi::OsString;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::platform::{self, LocalSocketError, PlatformPaths};
use agentdp_protocol::{
    self as protocol, Event, EventLevel, PingResult, Request, RequestKind, ServerMessageType, ShutdownResult,
};
use serde::de::DeserializeOwned;
use thiserror::Error;

const SERVER_PATH_ENV: &str = "AGENTDP_SERVER_PATH";
const START_RETRY_COUNT: usize = 40;
const START_RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Debug, Error)]
pub enum Error {
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
    Spawn(#[from] platform::DetachedSpawnError),
    #[error("failed to terminate agentdp-server: {0}")]
    Terminate(#[from] platform::TerminateProcessError),
    #[error("agentdp-server did not stop after termination request")]
    ServerStillRunning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ping {
    pub socket: PathBuf,
    pub pid: u32,
    pub version: Option<String>,
    pub executable: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stop {
    NotRunning,
    Stopped(Ping),
}

/// Ensures agentdp-server is running and responding to ping.
///
/// # Errors
///
/// Returns an error when an existing server cannot be contacted and a new
/// server cannot be started or reached.
pub fn ensure_running(context: &Context, paths: &PlatformPaths) -> Result<Ping, Error> {
    match ping(paths) {
        Ok(ping) => return Ok(ping),
        Err(error) if should_start_after_ping_error(&error) => {
            context
                .logger()
                .verbose_with(|| format!("agentdp-server ping failed before start: {error}"));
        }
        Err(error) => return Err(error),
    }

    start(context, paths)?;
    for _attempt in 0..START_RETRY_COUNT {
        match ping(paths) {
            Ok(ping) => return Ok(ping),
            Err(error) if should_start_after_ping_error(&error) => {
                thread::sleep(START_RETRY_DELAY);
            }
            Err(error) => return Err(error),
        }
    }

    ping(paths)
}

pub fn request<T: DeserializeOwned>(
    context: &Context,
    paths: &PlatformPaths,
    kind: RequestKind,
    on_event: Option<&mut dyn FnMut(Event)>,
) -> Result<T, Error> {
    let _ping = ensure_running(context, paths)?;
    let request = protocol::request(kind);
    send(paths, &request, on_event)
}

pub fn stop_if_running(context: &Context, paths: &PlatformPaths) -> Result<Stop, Error> {
    let ping = match ping(paths) {
        Ok(ping) => ping,
        Err(Error::Socket(LocalSocketError::Unsupported)) => return Ok(Stop::NotRunning),
        Err(error) if should_start_after_ping_error(&error) => return Ok(Stop::NotRunning),
        Err(error) => return Err(error),
    };

    context.logger().verbose_with(|| {
        format!(
            "stopping running agentdp-server pid {} before refreshing installed binary",
            ping.pid
        )
    });
    stop_running(context, paths, &ping)?;
    Ok(Stop::Stopped(ping))
}

pub fn start_server_from(context: &Context, paths: &PlatformPaths, server: &std::path::Path) -> Result<Ping, Error> {
    start_from(context, paths, server)?;
    wait_for_ping(paths)
}

fn ping(paths: &PlatformPaths) -> Result<Ping, Error> {
    let socket = paths.socket_path();
    let request = protocol::request(RequestKind::ServerPing);
    let response: PingResult = send(paths, &request, None)?;
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

fn send<T: DeserializeOwned>(
    paths: &PlatformPaths,
    request: &Request,
    mut on_event: Option<&mut dyn FnMut(Event)>,
) -> Result<T, Error> {
    let mut stream = platform::connect_local_socket(&paths.socket_path())?;
    stream.write_all(protocol::encode_line(&request)?.as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Err(Error::InvalidResponse(
                "server closed connection before response".to_owned(),
            ));
        }

        let message = protocol::decode_server_message(&line)?;
        match message.message_type {
            ServerMessageType::Event => {
                let event = message
                    .event
                    .ok_or_else(|| Error::InvalidResponse("event message omitted event body".to_owned()))?;
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
            ServerMessageType::Response => {
                let response = message
                    .response
                    .ok_or_else(|| Error::InvalidResponse("response message omitted response body".to_owned()))?;
                return decode_response(request, response);
            }
        }
    }
}

fn decode_response<T: DeserializeOwned>(request: &Request, response: protocol::Response) -> Result<T, Error> {
    if response.id != request.id {
        return Err(Error::InvalidResponse(format!(
            "expected response id {}, got {}",
            request.id, response.id
        )));
    }
    if !response.ok {
        let error = response
            .error
            .ok_or_else(|| Error::InvalidResponse("error response omitted error body".to_owned()))?;
        return Err(Error::Server {
            code: error.code,
            message: error.message,
        });
    }

    response.result().map_err(Error::Protocol)
}

pub fn log_event(context: &Context, event: Event) {
    match event.level {
        EventLevel::Info => context.logger().info(event.message),
        EventLevel::Warn => context.logger().warn(event.message),
        EventLevel::Error => context.logger().error(event.message),
        EventLevel::Verbose => context.logger().verbose(event.message),
    }
}

fn start(context: &Context, paths: &PlatformPaths) -> Result<(), Error> {
    let server = resolve_server_binary()?;
    start_from(context, paths, &server)
}

fn start_from(context: &Context, paths: &PlatformPaths, server: &std::path::Path) -> Result<(), Error> {
    context
        .logger()
        .verbose_with(|| format!("starting agentdp-server from {}", server.display()));
    platform::spawn_detached(
        server,
        &[OsString::from("--socket"), paths.socket_path().into_os_string()],
    )?;
    Ok(())
}

fn stop_running(context: &Context, paths: &PlatformPaths, running: &Ping) -> Result<(), Error> {
    match shutdown(paths) {
        Ok(()) => {}
        Err(Error::Server { code, .. }) if code == "unknown_method" => {
            context
                .logger()
                .verbose("running agentdp-server does not support server.shutdown; terminating by pid");
            platform::terminate_process(running.pid)?;
        }
        Err(error) if should_start_after_ping_error(&error) => return Ok(()),
        Err(error) => return Err(error),
    }

    for _attempt in 0..START_RETRY_COUNT {
        match ping(paths) {
            Ok(_) => thread::sleep(START_RETRY_DELAY),
            Err(error) if should_start_after_ping_error(&error) => return Ok(()),
            Err(error) => return Err(error),
        }
    }
    Err(Error::ServerStillRunning)
}

fn shutdown(paths: &PlatformPaths) -> Result<(), Error> {
    let request = protocol::request(RequestKind::ServerShutdown);
    let response: ShutdownResult = send(paths, &request, None)?;
    if response.shutdown {
        Ok(())
    } else {
        Err(Error::InvalidResponse(
            "server.shutdown response omitted shutdown marker".to_owned(),
        ))
    }
}

fn wait_for_ping(paths: &PlatformPaths) -> Result<Ping, Error> {
    for _attempt in 0..START_RETRY_COUNT {
        match ping(paths) {
            Ok(ping) => return Ok(ping),
            Err(error) if should_start_after_ping_error(&error) => thread::sleep(START_RETRY_DELAY),
            Err(error) => return Err(error),
        }
    }
    ping(paths)
}

const fn should_start_after_ping_error(error: &Error) -> bool {
    matches!(error, Error::Socket(LocalSocketError::Io(_)) | Error::Io(_))
}

fn resolve_server_binary() -> Result<PathBuf, Error> {
    if let Some(path) = std::env::var_os(SERVER_PATH_ENV).filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(path));
    }

    if let Ok(current_exe) = std::env::current_exe()
        && let Some(directory) = current_exe.parent()
    {
        let sibling = directory.join(format!("agentdp-server{}", std::env::consts::EXE_SUFFIX));
        if sibling.is_file() {
            return Ok(sibling);
        }
    }

    platform::find_binary(&format!("agentdp-server{}", std::env::consts::EXE_SUFFIX)).ok_or(Error::ServerNotFound)
}
