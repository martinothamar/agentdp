use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::platform;
use agentdp_core::platform::PlatformPaths;
use agentdp_protocol::{
    self as protocol, DoctorCheckResult, PingResult, Request, RequestKind, Response, ServerDoctorResult, ServerMessage,
    ShutdownResult,
};
use thiserror::Error;

use crate::progress::Progress;
use crate::{instance, runtime};

#[derive(Debug, Error)]
pub enum Error {
    #[error("agentdp-server is already running with pid {pid}: {path}")]
    AlreadyRunning { path: PathBuf, pid: u32 },
    #[error("failed to read agentdp-server lock {path}: {source}")]
    ReadLock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write agentdp-server lock {path}: {source}")]
    WriteLock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to inspect agentdp-server lock owner pid {pid}: {source}")]
    ProcessStatus {
        pid: u32,
        #[source]
        source: platform::ProcessStatusError,
    },
    #[error("local socket error: {0}")]
    Socket(#[from] platform::LocalSocketError),
    #[error("I/O error while handling local server connection: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] protocol::Error),
}

/// Starts a local agentdp-server loop on the provided socket path.
///
/// # Errors
///
/// Returns an error when the socket cannot be bound or a connection cannot be
/// accepted.
pub fn serve(context: &Context, socket_path: &Path) -> Result<(), Error> {
    context
        .logger()
        .verbose_with(|| format!("binding agentdp-server socket {}", socket_path.display()));
    let _lock = acquire_server_lock(socket_path)?;
    let listener = platform::bind_local_socket(socket_path)?;
    listener.set_nonblocking(true)?;
    let (shutdown_tx, shutdown_rx) = mpsc::channel();

    loop {
        if shutdown_rx.try_recv().is_ok() {
            break;
        }

        match listener.accept() {
            Ok(mut stream) => {
                let context = context.clone();
                let shutdown_tx = shutdown_tx.clone();
                thread::spawn(move || match handle_connection(&context, &mut stream) {
                    Ok(ConnectionAction::Continue) => {}
                    Ok(ConnectionAction::Shutdown) => {
                        let _result = shutdown_tx.send(());
                    }
                    Err(error) => {
                        context
                            .logger()
                            .warn(format!("failed to handle agentdp-server connection: {error}"));
                    }
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => return Err(error.into()),
        }
    }
    context.logger().verbose("agentdp-server shutdown requested");
    Ok(())
}

#[derive(Debug)]
struct ServerLock {
    path: PathBuf,
    pid: u32,
}

impl Drop for ServerLock {
    fn drop(&mut self) {
        let Ok(contents) = fs::read_to_string(&self.path) else {
            return;
        };
        if lock_owner_pid_from_contents(&contents) == Some(self.pid) {
            let _result = fs::remove_file(&self.path);
        }
    }
}

fn acquire_server_lock(socket_path: &Path) -> Result<ServerLock, Error> {
    let lock_path = server_lock_path(socket_path);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(|source| Error::WriteLock {
            path: lock_path.clone(),
            source,
        })?;
    }
    let pid = std::process::id();

    loop {
        match OpenOptions::new().write(true).create_new(true).open(&lock_path) {
            Ok(mut file) => {
                writeln!(file, "pid={pid}").map_err(|source| Error::WriteLock {
                    path: lock_path.clone(),
                    source,
                })?;
                return Ok(ServerLock { path: lock_path, pid });
            }
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Some(owner) = server_lock_owner(&lock_path)? {
                    match platform::process_status(owner)
                        .map_err(|source| Error::ProcessStatus { pid: owner, source })?
                    {
                        platform::ProcessStatus::Running => {
                            return Err(Error::AlreadyRunning {
                                path: lock_path,
                                pid: owner,
                            });
                        }
                        platform::ProcessStatus::NotFound => {
                            let _result = fs::remove_file(&lock_path);
                        }
                    }
                } else {
                    let _result = fs::remove_file(&lock_path);
                }
            }
            Err(source) => {
                return Err(Error::WriteLock {
                    path: lock_path,
                    source,
                });
            }
        }
    }
}

fn server_lock_path(socket_path: &Path) -> PathBuf {
    socket_path.with_extension("lock")
}

fn server_lock_owner(path: &Path) -> Result<Option<u32>, Error> {
    let contents = fs::read_to_string(path).map_err(|source| Error::ReadLock {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(lock_owner_pid_from_contents(&contents))
}

fn lock_owner_pid_from_contents(contents: &str) -> Option<u32> {
    contents
        .lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|pid| pid.parse::<u32>().ok())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionAction {
    Continue,
    Shutdown,
}

fn handle_connection(context: &Context, stream: &mut platform::LocalSocket) -> Result<ConnectionAction, Error> {
    let mut line = String::new();
    {
        let mut reader = BufReader::new(&mut *stream);
        reader.read_line(&mut line)?;
    }

    let (response, action) = match protocol::decode_request(&line) {
        Ok(request) => {
            let mut progress = ConnectionProgress {
                stream,
                request_id: request.id.clone(),
            };
            handle_request(context, &request, &mut progress)
        }
        Err(error) => (protocol::invalid_request(error.to_string()), ConnectionAction::Continue),
    };
    stream.write_all(protocol::encode_line(&ServerMessage::response(response))?.as_bytes())?;
    stream.flush()?;
    Ok(action)
}

struct ConnectionProgress<'a> {
    stream: &'a mut platform::LocalSocket,
    request_id: String,
}

impl Progress for ConnectionProgress<'_> {
    fn info(&mut self, message: String) {
        let event = ServerMessage::event(protocol::Event::info(self.request_id.clone(), message));
        if let Ok(line) = protocol::encode_line(&event) {
            let _result = self.stream.write_all(line.as_bytes());
            let _result = self.stream.flush();
        }
    }
}

fn handle_request(context: &Context, request: &Request, progress: &mut dyn Progress) -> (Response, ConnectionAction) {
    match &request.kind {
        RequestKind::ServerPing => (ping_response(request), ConnectionAction::Continue),
        RequestKind::ServerShutdown => (shutdown_response(request), ConnectionAction::Shutdown),
        RequestKind::ServerDoctor(params) => (
            server_doctor_response(context, request, params),
            ConnectionAction::Continue,
        ),
        RequestKind::ProvisioningPlan(params) => (
            provisioning_plan_response(context, request, params),
            ConnectionAction::Continue,
        ),
        RequestKind::InstanceCreate(params) => (
            instance_create_response(context, request, params),
            ConnectionAction::Continue,
        ),
        RequestKind::InstanceStatus(params) => (
            instance_status_response(context, request, params),
            ConnectionAction::Continue,
        ),
        RequestKind::InstanceLogs(params) => (
            instance_logs_response(context, request, params),
            ConnectionAction::Continue,
        ),
        RequestKind::InstanceExec(params) => (
            instance_exec_response(context, request, params),
            ConnectionAction::Continue,
        ),
        RequestKind::InstancePs(params) => (
            instance_ps_response(context, request, params),
            ConnectionAction::Continue,
        ),
        RequestKind::InstanceShell(params) => (
            instance_shell_response(context, request, params),
            ConnectionAction::Continue,
        ),
        RequestKind::InstanceUp(params) => (
            instance_up_response(context, request, params, progress),
            ConnectionAction::Continue,
        ),
        RequestKind::InstanceDown(params) => (
            instance_down_response(context, request, params),
            ConnectionAction::Continue,
        ),
        RequestKind::InstanceRm(params) => (
            instance_rm_response(context, request, params),
            ConnectionAction::Continue,
        ),
    }
}

fn instance_exec_response(
    context: &Context,
    request: &Request,
    params: &agentdp_protocol::InstanceExecParams,
) -> Response {
    handle_params(
        context,
        request,
        params,
        "instance_exec_failed",
        |context, params, paths| {
            let instance = instance::Instance::load_existing(
                context,
                &agentdp_protocol::InstanceRef {
                    manifest: params.manifest.clone(),
                    instance: params.instance.clone(),
                },
                paths,
            )?;
            instance.exec(context, params)
        },
    )
}

fn instance_ps_response(context: &Context, request: &Request, params: &agentdp_protocol::InstancePsParams) -> Response {
    handle_params(context, request, params, "instance_ps_failed", instance::ps::ps)
}

fn instance_shell_response(context: &Context, request: &Request, params: &agentdp_protocol::InstanceRef) -> Response {
    handle_params(
        context,
        request,
        params,
        "instance_shell_failed",
        |context, params, paths| {
            let instance = instance::Instance::load_existing(context, params, paths)?;
            instance.shell()
        },
    )
}

fn ping_response(request: &Request) -> Response {
    request.respond_with_success(PingResult {
        service: "agentdp-server".to_owned(),
        pid: std::process::id(),
        version: Some(env!("CARGO_PKG_VERSION").to_owned()),
        executable: Some(
            std::env::current_exe()
                .map_or_else(|error| format!("<unknown: {error}>"), |path| path.display().to_string()),
        ),
    })
}

fn shutdown_response(request: &Request) -> Response {
    request.respond_with_success(ShutdownResult {
        shutdown: true,
        pid: std::process::id(),
    })
}

fn server_doctor_response(
    context: &Context,
    request: &Request,
    params: &agentdp_protocol::ServerDoctorParams,
) -> Response {
    let mut report = agentdp_core::doctor::DoctorReport::new(None);
    runtime::Backend::from_kind(params.backend).check_prerequisites(context, &mut report);
    request.respond_with_success(ServerDoctorResult {
        backend: params.backend,
        checks: report
            .checks
            .into_iter()
            .map(|check| DoctorCheckResult {
                name: check.name,
                status: check.status.label().to_owned(),
                message: check.message,
            })
            .collect(),
    })
}

fn provisioning_plan_response(
    context: &Context,
    request: &Request,
    params: &agentdp_protocol::ProvisioningPlanParams,
) -> Response {
    handle_params(
        context,
        request,
        params,
        "provisioning_plan_failed",
        instance::provisioning::plan,
    )
}

fn instance_create_response(
    context: &Context,
    request: &Request,
    params: &agentdp_protocol::InstanceCreateParams,
) -> Response {
    handle_params(
        context,
        request,
        params,
        "instance_create_failed",
        |context, params, paths| {
            let instance = instance::Instance::create_new(context, params, paths)?;
            Ok::<_, instance::Error>(instance.create_result())
        },
    )
}

fn instance_status_response(context: &Context, request: &Request, params: &agentdp_protocol::InstanceRef) -> Response {
    handle_params(
        context,
        request,
        params,
        "instance_status_failed",
        |context, params, paths| {
            let instance = instance::Instance::load_existing(context, params, paths)?;
            Ok::<_, instance::Error>(instance.status())
        },
    )
}

fn instance_logs_response(
    context: &Context,
    request: &Request,
    params: &agentdp_protocol::InstanceLogsParams,
) -> Response {
    handle_params(
        context,
        request,
        params,
        "instance_logs_failed",
        |context, params, paths| {
            let instance = instance::Instance::load_existing(
                context,
                &agentdp_protocol::InstanceRef {
                    manifest: params.manifest.clone(),
                    instance: params.instance.clone(),
                },
                paths,
            )?;
            instance.logs(params)
        },
    )
}

fn instance_up_response(
    context: &Context,
    request: &Request,
    params: &agentdp_protocol::InstanceRef,
    progress: &mut dyn Progress,
) -> Response {
    handle_params(
        context,
        request,
        params,
        "instance_up_failed",
        |context, params, paths| {
            let mut instance = instance::Instance::load_existing(context, params, paths)?;
            instance.up(context, progress)
        },
    )
}

fn instance_down_response(context: &Context, request: &Request, params: &agentdp_protocol::InstanceRef) -> Response {
    handle_params(
        context,
        request,
        params,
        "instance_down_failed",
        |context, params, paths| {
            let mut instance = instance::Instance::load_existing(context, params, paths)?;
            instance.down(context)
        },
    )
}

fn instance_rm_response(context: &Context, request: &Request, params: &agentdp_protocol::InstanceRef) -> Response {
    handle_params(
        context,
        request,
        params,
        "instance_rm_failed",
        |context, params, paths| {
            let instance = instance::Instance::load_existing(context, params, paths)?;
            instance.rm()
        },
    )
}

fn handle_params<P, R, E>(
    context: &Context,
    request: &Request,
    params: &P,
    error_code: &'static str,
    handler: impl FnOnce(&Context, &P, &PlatformPaths) -> Result<R, E>,
) -> Response
where
    R: serde::Serialize,
    E: std::fmt::Display,
{
    let paths = match context.paths() {
        Ok(paths) => paths,
        Err(error) => return request.respond_with_failure("platform_paths", error.to_string()),
    };

    match handler(context, params, paths) {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure(error_code, error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{ConnectionAction, Error, acquire_server_lock, handle_request, server_lock_path};
    use agentdp_core::Context;
    use agentdp_protocol::{self as protocol, Request, RequestKind};

    use crate::progress::NoopProgress;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn ping_request_returns_success() {
        let (response, action) = handle_request(
            &Context::quiet(),
            &Request::new("cmd_1", RequestKind::ServerPing),
            &mut NoopProgress,
        );
        assert!(response.ok);
        assert_eq!(response.id, "cmd_1");
        assert_eq!(action, ConnectionAction::Continue);
    }

    #[test]
    fn invalid_request_returns_stable_error() {
        let mut line = r#"{"id":"cmd_1","method":"unknown.method"}"#.to_owned();
        line.push('\n');
        let response = match protocol::decode_request(&line) {
            Ok(request) => handle_request(&Context::quiet(), &request, &mut NoopProgress).0,
            Err(error) => protocol::invalid_request(error.to_string()),
        };
        let error = response.error.expect("error body");
        assert!(!response.ok);
        assert_eq!(error.code, "invalid_request");
    }

    #[test]
    fn shutdown_request_returns_success_and_shutdown_action() {
        let (response, action) = handle_request(
            &Context::quiet(),
            &Request::new("cmd_1", RequestKind::ServerShutdown),
            &mut NoopProgress,
        );
        assert!(response.ok);
        assert_eq!(action, ConnectionAction::Shutdown);
    }

    #[test]
    fn server_lock_rejects_running_owner() {
        let temp = TestTempDir::create("server-lock-running");
        let socket = temp.path.join("agentdp-server.sock");
        let lock = server_lock_path(&socket);
        fs::write(&lock, format!("pid={}\n", std::process::id())).unwrap();

        let error = acquire_server_lock(&socket).unwrap_err();

        assert!(matches!(error, Error::AlreadyRunning { .. }));
    }

    #[test]
    fn server_lock_replaces_stale_owner() {
        let temp = TestTempDir::create("server-lock-stale");
        let socket = temp.path.join("agentdp-server.sock");
        let lock = server_lock_path(&socket);
        fs::write(&lock, "pid=999999\n").unwrap();

        let guard = acquire_server_lock(&socket).unwrap();

        assert_eq!(guard.pid, std::process::id());
        assert_eq!(fs::read_to_string(&lock).unwrap(), format!("pid={}\n", guard.pid));
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn create(name: &str) -> Self {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("agentdp-{name}-{}-{timestamp}-{id}", std::process::id()));
            fs::create_dir(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _result = fs::remove_dir_all(&self.path);
        }
    }
}
