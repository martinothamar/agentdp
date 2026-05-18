use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::{Duration, Instant};

use agentdp_core::Context;
use agentdp_core::manifest::{AgentManifest, Healthcheck};
use agentdp_protocol::{HealthcheckResult, ReadinessResult, ServiceResult};
use thiserror::Error;

use super::state::{InstanceState, PortProtocolState};
use super::{Error as InstanceError, Instance};
use crate::progress::Progress;

const DEFAULT_TIMEOUT: Duration = Duration::from_mins(5);
const CONNECT_TIMEOUT: Duration = Duration::from_millis(250);
const COMMAND_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Error)]
pub enum Error {
    #[error("healthcheck {name} target {target} is invalid")]
    InvalidTcpTarget { name: String, target: String },
    #[error("healthcheck {name} target {target} could not be resolved")]
    ResolveTcpTarget { name: String, target: String },
    #[error("healthcheck {name} timed out after {timeout_seconds}s waiting for {target}")]
    Timeout {
        name: String,
        target: String,
        timeout_seconds: u64,
    },
    #[error("healthcheck {name} command cannot run: {source}")]
    CommandSetup {
        name: String,
        #[source]
        source: GuestCommandError,
    },
    #[error(
        "healthcheck {name} timed out after {timeout_seconds}s waiting for command `{command}`; last error: {last_error}"
    )]
    CommandTimeout {
        name: String,
        command: String,
        timeout_seconds: u64,
        last_error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GuestCommandOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuestCommandError {
    message: String,
    retryable: bool,
}

impl GuestCommandError {
    #[must_use]
    fn new(message: impl Into<String>, retryable: bool) -> Self {
        Self {
            message: message.into(),
            retryable,
        }
    }

    #[must_use]
    const fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl std::fmt::Display for GuestCommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for GuestCommandError {}

impl Instance {
    pub(super) fn wait_ready(
        &self,
        context: &Context,
        progress: &mut dyn Progress,
    ) -> Result<ReadinessResult, InstanceError> {
        wait(
            context,
            &self.manifest,
            &self.state,
            progress,
            |context, state, command, timeout| {
                self.backend()
                    .run_readiness_command(context, state, command, timeout)
                    .map(|output| GuestCommandOutput {
                        status: output.status,
                        stdout: output.stdout,
                        stderr: output.stderr,
                    })
                    .map_err(|error| GuestCommandError::new(error.to_string(), error.is_retryable()))
            },
        )
        .map_err(InstanceError::Readiness)
    }
}

fn wait(
    context: &Context,
    manifest: &AgentManifest,
    state: &InstanceState,
    progress: &mut dyn Progress,
    mut run_command: impl FnMut(&Context, &InstanceState, &str, Duration) -> Result<GuestCommandOutput, GuestCommandError>,
) -> Result<ReadinessResult, Error> {
    let mut healthchecks = Vec::new();
    for healthcheck in &manifest.bootstrap.healthchecks {
        progress.info(format!("healthcheck {} running", healthcheck.name));
        let result = run_healthcheck(context, healthcheck, state, &mut run_command)?;
        progress.info(format!("healthcheck {} {}", result.name, result.status));
        healthchecks.push(result);
    }

    Ok(ReadinessResult {
        ready: true,
        services: services(state),
        healthchecks,
    })
}

fn run_healthcheck(
    context: &Context,
    healthcheck: &Healthcheck,
    state: &InstanceState,
    run_command: &mut impl FnMut(&Context, &InstanceState, &str, Duration) -> Result<GuestCommandOutput, GuestCommandError>,
) -> Result<HealthcheckResult, Error> {
    if let Some(target) = &healthcheck.tcp {
        return wait_for_tcp(context, healthcheck, target, state);
    }
    if let Some(command) = &healthcheck.command {
        return wait_for_command(context, healthcheck, command, state, run_command);
    }

    Ok(HealthcheckResult {
        name: healthcheck.name.clone(),
        kind: "command".to_owned(),
        status: "skipped".to_owned(),
        reason: Some("command healthchecks require guest command execution".to_owned()),
        command: None,
        exit_status: None,
        target: None,
        host: None,
        elapsed_ms: 0,
    })
}

fn wait_for_command(
    context: &Context,
    healthcheck: &Healthcheck,
    command: &str,
    state: &InstanceState,
    mut run_command: impl FnMut(&Context, &InstanceState, &str, Duration) -> Result<GuestCommandOutput, GuestCommandError>,
) -> Result<HealthcheckResult, Error> {
    let timeout = healthcheck
        .timeout
        .as_deref()
        .map_or(Ok(DEFAULT_TIMEOUT), parse_duration)
        .unwrap_or(DEFAULT_TIMEOUT);
    context.logger().verbose_with(|| {
        format!(
            "waiting up to {}s for command healthcheck {}",
            timeout.as_secs(),
            healthcheck.name
        )
    });

    let started = Instant::now();
    let deadline = started + timeout;
    let mut last_error;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::CommandTimeout {
                name: healthcheck.name.clone(),
                command: command.to_owned(),
                timeout_seconds: timeout.as_secs(),
                last_error: "command did not run before the deadline".to_owned(),
            });
        }
        let attempt_timeout = remaining.min(COMMAND_ATTEMPT_TIMEOUT);
        match run_command(context, state, command, attempt_timeout) {
            Ok(output) if output.status == 0 => {
                return Ok(HealthcheckResult {
                    name: healthcheck.name.clone(),
                    kind: "command".to_owned(),
                    status: "passed".to_owned(),
                    reason: None,
                    command: Some(command.to_owned()),
                    exit_status: Some(output.status),
                    target: None,
                    host: None,
                    elapsed_ms: started.elapsed().as_millis(),
                });
            }
            Ok(output) => {
                last_error = format!(
                    "guest command exited with status {}; stdout: {}; stderr: {}",
                    output.status,
                    output_tail(&output.stdout),
                    output_tail(&output.stderr)
                );
                context
                    .logger()
                    .verbose_with(|| format!("command healthcheck {} is not ready: {last_error}", healthcheck.name));
            }
            Err(error) if error.is_retryable() => {
                context
                    .logger()
                    .verbose_with(|| format!("command healthcheck {} is not ready: {error}", healthcheck.name));
                last_error = error.to_string();
            }
            Err(error) => {
                return Err(Error::CommandSetup {
                    name: healthcheck.name.clone(),
                    source: error,
                });
            }
        }

        if Instant::now() >= deadline {
            return Err(Error::CommandTimeout {
                name: healthcheck.name.clone(),
                command: command.to_owned(),
                timeout_seconds: timeout.as_secs(),
                last_error,
            });
        }
        thread::sleep(RETRY_DELAY);
    }
}

fn output_tail(output: &str) -> String {
    const LIMIT: usize = 500;

    let trimmed = output.trim();
    if trimmed.is_empty() {
        return "<empty>".to_owned();
    }
    let chars = trimmed.chars().collect::<Vec<_>>();
    if chars.len() <= LIMIT {
        return trimmed.to_owned();
    }
    chars[chars.len() - LIMIT..].iter().collect()
}

fn wait_for_tcp(
    context: &Context,
    healthcheck: &Healthcheck,
    target: &str,
    state: &InstanceState,
) -> Result<HealthcheckResult, Error> {
    let endpoint = host_endpoint(&healthcheck.name, target, state)?;
    let timeout = healthcheck
        .timeout
        .as_deref()
        .map_or(Ok(DEFAULT_TIMEOUT), parse_duration)
        .unwrap_or(DEFAULT_TIMEOUT);
    context.logger().verbose_with(|| {
        format!(
            "waiting up to {}s for healthcheck {} at {}",
            timeout.as_secs(),
            healthcheck.name,
            endpoint.display
        )
    });

    let started = Instant::now();
    let deadline = started + timeout;
    let probe = probe_for(healthcheck, &endpoint);
    loop {
        if endpoint_ready(&endpoint.connect, probe) {
            return Ok(HealthcheckResult {
                name: healthcheck.name.clone(),
                kind: probe.kind().to_owned(),
                status: "passed".to_owned(),
                reason: None,
                command: None,
                exit_status: None,
                target: Some(target.to_owned()),
                host: Some(endpoint.display),
                elapsed_ms: started.elapsed().as_millis(),
            });
        }

        if Instant::now() >= deadline {
            return Err(Error::Timeout {
                name: healthcheck.name.clone(),
                target: endpoint.display,
                timeout_seconds: timeout.as_secs(),
            });
        }
        thread::sleep(RETRY_DELAY);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Probe {
    TcpConnect,
    HttpGet,
}

impl Probe {
    const fn kind(self) -> &'static str {
        match self {
            Self::TcpConnect => "tcp",
            Self::HttpGet => "http",
        }
    }
}

fn probe_for(healthcheck: &Healthcheck, endpoint: &HostEndpoint) -> Probe {
    if healthcheck.name == "code-server" || endpoint.service.as_deref() == Some("code-server") {
        Probe::HttpGet
    } else {
        Probe::TcpConnect
    }
}

fn endpoint_ready(target: &str, probe: Probe) -> bool {
    let Ok(addresses) = target.to_socket_addrs() else {
        return false;
    };
    for address in addresses {
        let Ok(mut stream) = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) else {
            continue;
        };
        match probe {
            Probe::TcpConnect => return true,
            Probe::HttpGet => {
                if stream.set_read_timeout(Some(CONNECT_TIMEOUT)).is_err()
                    || stream.set_write_timeout(Some(CONNECT_TIMEOUT)).is_err()
                {
                    continue;
                }
                if stream
                    .write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                    .is_err()
                {
                    continue;
                }
                let mut response = [0; 5];
                if stream.read_exact(&mut response).is_ok() && response == *b"HTTP/" {
                    return true;
                }
            }
        }
    }
    false
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HostEndpoint {
    connect: String,
    display: String,
    service: Option<String>,
}

fn host_endpoint(name: &str, target: &str, state: &InstanceState) -> Result<HostEndpoint, Error> {
    let Some((_host, port)) = target.rsplit_once(':') else {
        return Err(Error::InvalidTcpTarget {
            name: name.to_owned(),
            target: target.to_owned(),
        });
    };
    let port = port.parse::<u16>().map_err(|_| Error::InvalidTcpTarget {
        name: name.to_owned(),
        target: target.to_owned(),
    })?;

    for (service, mapped) in &state.network.ports {
        if mapped.guest == port && mapped.protocol == PortProtocolState::Tcp {
            let endpoint = format!("127.0.0.1:{}", mapped.host);
            return Ok(HostEndpoint {
                connect: endpoint.clone(),
                display: endpoint,
                service: Some(service.clone()),
            });
        }
    }

    if target
        .to_socket_addrs()
        .map_or(true, |mut addresses| addresses.next().is_none())
    {
        return Err(Error::ResolveTcpTarget {
            name: name.to_owned(),
            target: target.to_owned(),
        });
    }
    Ok(HostEndpoint {
        connect: target.to_owned(),
        display: target.to_owned(),
        service: None,
    })
}

fn services(state: &InstanceState) -> std::collections::BTreeMap<String, ServiceResult> {
    let mut services = std::collections::BTreeMap::new();
    if let Some(port) = state.network.ports.get("code-server") {
        services.insert(
            "code-server".to_owned(),
            ServiceResult {
                url: Some(format!("http://127.0.0.1:{}", port.host)),
                host_port: port.host,
                guest_port: port.guest,
            },
        );
    }
    services
}

fn parse_duration(value: &str) -> Result<Duration, ()> {
    let digit_count = value.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return Err(());
    }
    let number = value[..digit_count].parse::<u64>().map_err(|_| ())?;
    match &value[digit_count..] {
        "s" => Ok(Duration::from_secs(number)),
        "m" => Ok(Duration::from_secs(number * 60)),
        "h" => Ok(Duration::from_secs(number * 60 * 60)),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use super::{GuestCommandError, GuestCommandOutput, host_endpoint, wait, wait_for_command};
    use crate::instance::state::{
        InstanceState, InstanceStatus, ManifestState, NetworkModeState, NetworkState, PortMappingState,
        PortProtocolState,
    };
    use crate::progress::NoopProgress;
    use crate::qemu::runtime::{ImageState, State as QemuState};
    use crate::runtime::BackendState;
    use agentdp_core::Context;
    use agentdp_core::manifest::AgentManifest;
    use agentdp_test_support::manifest;

    #[test]
    fn maps_guest_tcp_healthcheck_to_host_forward() {
        let state = instance_state(24090);

        let endpoint = host_endpoint("code-server", "127.0.0.1:4090", &state).unwrap();

        assert_eq!(endpoint.display, "127.0.0.1:24090");
        assert_eq!(endpoint.service.as_deref(), Some("code-server"));
    }

    #[test]
    fn waits_for_mapped_tcp_healthcheck() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host_port = listener.local_addr().unwrap().port();
        let state = instance_state_with_port("ssh", 22, host_port);
        let manifest = manifest_with_ssh_healthcheck();

        let result = wait(
            &Context::quiet(),
            &manifest,
            &state,
            &mut NoopProgress,
            unsupported_guest_command,
        )
        .unwrap();

        assert!(result.ready);
        assert_eq!(result.healthchecks[0].kind, "tcp");
        assert_eq!(result.healthchecks[0].status, "passed");
    }

    #[test]
    fn code_server_healthcheck_waits_for_http_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host_port = listener.local_addr().unwrap().port();
        let state = instance_state(host_port);
        let manifest = manifest_with_code_server_healthcheck();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 128];
            let _ = stream.read(&mut request);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
        });

        let result = wait(
            &Context::quiet(),
            &manifest,
            &state,
            &mut NoopProgress,
            unsupported_guest_command,
        )
        .unwrap();
        handle.join().unwrap();

        assert!(result.ready);
        assert_eq!(result.healthchecks[0].kind, "http");
        assert_eq!(result.healthchecks[0].status, "passed");
        assert_eq!(
            result.services["code-server"].url.as_deref(),
            Some(format!("http://127.0.0.1:{host_port}").as_str())
        );
    }

    #[test]
    fn command_healthcheck_retries_until_guest_command_passes() {
        let state = instance_state_with_port("ssh", 22, 2222);
        let manifest = manifest_with_command_healthcheck();
        let healthcheck = &manifest.bootstrap.healthchecks[0];
        let mut attempts = 0;

        let result = wait_for_command(
            &Context::quiet(),
            healthcheck,
            healthcheck.command.as_deref().unwrap(),
            &state,
            |_context, _state, _command, _timeout| {
                attempts += 1;
                if attempts == 1 {
                    return Ok(GuestCommandOutput {
                        status: 1,
                        stdout: String::new(),
                        stderr: "not ready".to_owned(),
                    });
                }
                Ok(GuestCommandOutput {
                    status: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            },
        )
        .unwrap();

        assert_eq!(attempts, 2);
        assert_eq!(result.kind, "command");
        assert_eq!(result.status, "passed");
        assert_eq!(result.command.as_deref(), Some("docker ps"));
    }

    fn manifest_with_code_server_healthcheck() -> AgentManifest {
        serde_yaml::from_str(manifest::healthcheck_code_server()).unwrap()
    }

    fn manifest_with_ssh_healthcheck() -> AgentManifest {
        serde_yaml::from_str(manifest::healthcheck_ssh()).unwrap()
    }

    fn manifest_with_command_healthcheck() -> AgentManifest {
        serde_yaml::from_str(manifest::healthcheck_command()).unwrap()
    }

    fn instance_state(code_server_host_port: u16) -> InstanceState {
        instance_state_with_port("code-server", 4090, code_server_host_port)
    }

    fn instance_state_with_port(name: &str, guest: u16, host: u16) -> InstanceState {
        InstanceState {
            version: 1,
            manifest_name: "altinn-studio".to_owned(),
            instance: "pr-0".to_owned(),
            status: InstanceStatus::Running,
            manifest: ManifestState {
                source: "/agent.yaml".to_owned(),
                copy: "/instance/manifest.yaml".to_owned(),
            },
            network: NetworkState {
                mode: NetworkModeState::User,
                ports: BTreeMap::from([(
                    name.to_owned(),
                    PortMappingState {
                        guest,
                        host,
                        protocol: PortProtocolState::Tcp,
                    },
                )]),
            },
            guest_access: None,
            readiness: None,
            backend: BackendState::Qemu(QemuState {
                image: ImageState {
                    os: "archlinux".to_owned(),
                    architecture: "x86_64".to_owned(),
                    variant: "cloud".to_owned(),
                    source_url: "https://example.invalid/image.qcow2".to_owned(),
                    cache_key: "image.qcow2".to_owned(),
                    cache_path: "/cache/image.qcow2".to_owned(),
                    download_path: "/cache/image.qcow2.part".to_owned(),
                    format: "qcow2".to_owned(),
                },
                disk: "/instance/disk.qcow2".to_owned(),
                work_dir: "/instance/generated/qemu".to_owned(),
                seed_media: "/instance/generated/qemu/seed.img".to_owned(),
                seed_meta_data: "/instance/generated/qemu/seed/meta-data".to_owned(),
                seed_user_data: "/instance/generated/qemu/seed/user-data".to_owned(),
                bootstrap_script: "/instance/generated/qemu/scripts/bootstrap.sh".to_owned(),
                monitor_socket: "/run/agentdp/monitor.sock".to_owned(),
                qmp_socket: "/run/agentdp/qmp.sock".to_owned(),
                pid_file: "/run/agentdp/qemu.pid".to_owned(),
                serial_log: "/instance/logs/serial.log".to_owned(),
                qemu_log: "/instance/logs/qemu.log".to_owned(),
                pid: Some(4242),
                last_start_unix_seconds: Some(1),
            }),
        }
    }

    fn unsupported_guest_command(
        _context: &Context,
        _state: &InstanceState,
        _command: &str,
        _timeout: std::time::Duration,
    ) -> Result<GuestCommandOutput, GuestCommandError> {
        Err(GuestCommandError::new("unexpected guest command", false))
    }
}
