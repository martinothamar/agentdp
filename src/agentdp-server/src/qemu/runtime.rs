use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agentdp_core::Context;
use agentdp_core::manifest::AgentManifest;
use agentdp_core::platform::ssh::{CommandOutput, SshKeygen};
use agentdp_core::platform::{self, PlatformPaths, ProcessStatus};
use agentdp_protocol::{
    BackendCreateResult, BackendProvisioningResult, BackendRuntimeResult, BackendStatusResult, HostCommandResult,
    ImageResult, LogFile, ProcessResult, ProvisioningPlanResult, QemuCreateResult, QemuRuntimeResult, QemuStatusResult,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::backend::ssh;
use crate::instance::state::{GuestAccessState, InstanceFiles, InstanceState, InstanceStatus, NetworkState};
use crate::progress::Progress;
use crate::runtime;

use super::provisioning::{self, PreparedProvisioning};
use super::{command, disk, image, system};

const CLOUD_INIT_WAIT_TIMEOUT: Duration = Duration::from_mins(45);
const CLOUD_INIT_POLL_DELAY: Duration = Duration::from_secs(5);
const CLOUD_INIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const CLOUD_INIT_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const MONITOR_POWERDOWN_WAIT: Duration = Duration::from_secs(30);
const MONITOR_QUIT_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
}

#[derive(Debug, Error)]
enum ErrorKind {
    #[error("{0}")]
    Disk(disk::Error),
    #[error("{0}")]
    Image(image::Error),
    #[error("{0}")]
    System(system::Error),
    #[error("{0}")]
    Ssh(ssh::Error),
    #[error("{0}")]
    Provisioning(provisioning::Error),
    #[error("cloud-init did not finish after {timeout_seconds}s; last output: {last_output}")]
    CloudInitTimeout { timeout_seconds: u64, last_output: String },
    #[error("cloud-init failed: {output}")]
    CloudInitFailed { output: String },
    #[error("{0}")]
    Terminate(platform::TerminateProcessError),
    #[error("{0}")]
    ProcessStatus(platform::ProcessStatusError),
    #[error("instance {name} is running but QEMU runtime state has no pid")]
    MissingRunningPid { name: String },
    #[error("QEMU process {pid} did not exit after termination")]
    ProcessStillRunning { pid: u32 },
}

impl Error {
    const fn new(kind: ErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        match &self.kind {
            ErrorKind::Ssh(error) => error.is_retryable(),
            ErrorKind::Disk(_)
            | ErrorKind::Image(_)
            | ErrorKind::System(_)
            | ErrorKind::Provisioning(_)
            | ErrorKind::CloudInitTimeout { .. }
            | ErrorKind::CloudInitFailed { .. }
            | ErrorKind::Terminate(_)
            | ErrorKind::ProcessStatus(_)
            | ErrorKind::MissingRunningPid { .. }
            | ErrorKind::ProcessStillRunning { .. } => false,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.kind, formatter)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.kind)
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self::new(kind)
    }
}

impl From<disk::Error> for Error {
    fn from(source: disk::Error) -> Self {
        ErrorKind::Disk(source).into()
    }
}

impl From<image::Error> for Error {
    fn from(source: image::Error) -> Self {
        ErrorKind::Image(source).into()
    }
}

impl From<system::Error> for Error {
    fn from(source: system::Error) -> Self {
        ErrorKind::System(source).into()
    }
}

impl From<ssh::Error> for Error {
    fn from(source: ssh::Error) -> Self {
        ErrorKind::Ssh(source).into()
    }
}

impl From<provisioning::Error> for Error {
    fn from(source: provisioning::Error) -> Self {
        ErrorKind::Provisioning(source).into()
    }
}

impl From<platform::TerminateProcessError> for Error {
    fn from(source: platform::TerminateProcessError) -> Self {
        ErrorKind::Terminate(source).into()
    }
}

impl From<platform::ProcessStatusError> for Error {
    fn from(source: platform::ProcessStatusError) -> Self {
        ErrorKind::ProcessStatus(source).into()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct State {
    pub image: ImageState,
    pub disk: String,
    pub work_dir: String,
    pub seed_media: String,
    pub seed_meta_data: String,
    pub seed_user_data: String,
    pub bootstrap_script: String,
    pub monitor_socket: String,
    pub qmp_socket: String,
    pub pid_file: String,
    pub serial_log: String,
    pub qemu_log: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_start_unix_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ImageState {
    pub os: String,
    pub architecture: String,
    pub variant: String,
    pub source_url: String,
    pub cache_key: String,
    pub cache_path: String,
    pub download_path: String,
    pub format: String,
}

fn create_result_from_state(state: &State) -> QemuCreateResult {
    QemuCreateResult {
        image: image_result_from_state(&state.image),
        disk: state.disk.clone(),
        work_dir: state.work_dir.clone(),
        seed_media: state.seed_media.clone(),
        seed_meta_data: state.seed_meta_data.clone(),
        seed_user_data: state.seed_user_data.clone(),
        bootstrap_script: state.bootstrap_script.clone(),
        monitor_socket: state.monitor_socket.clone(),
        qmp_socket: state.qmp_socket.clone(),
        pid_file: state.pid_file.clone(),
        serial_log: state.serial_log.clone(),
        qemu_log: state.qemu_log.clone(),
    }
}

pub fn create_details(state: &State) -> BackendCreateResult {
    BackendCreateResult::Qemu(create_result_from_state(state))
}

#[must_use]
pub fn clone_state(
    source: &State,
    files: &InstanceFiles,
    paths: &PlatformPaths,
    manifest_name: &str,
    instance: &str,
) -> State {
    let work_dir = files.instance_dir.join("generated").join("qemu");
    let qemu_runtime_dir = runtime_dir(paths, manifest_name, instance);
    State {
        image: source.image.clone(),
        disk: path_text(&disk_path(files)),
        work_dir: path_text(&work_dir),
        seed_media: path_text(&work_dir.join("seed.img")),
        seed_meta_data: path_text(&work_dir.join("seed").join("meta-data")),
        seed_user_data: path_text(&work_dir.join("seed").join("user-data")),
        bootstrap_script: path_text(&work_dir.join("scripts").join("bootstrap.sh")),
        monitor_socket: path_text(&qemu_runtime_dir.join("monitor.sock")),
        qmp_socket: path_text(&qemu_runtime_dir.join("qmp.sock")),
        pid_file: path_text(&qemu_runtime_dir.join("qemu.pid")),
        serial_log: path_text(&files.logs_dir.join("serial.log")),
        qemu_log: path_text(&files.logs_dir.join("qemu.log")),
        pid: None,
        last_start_unix_seconds: None,
    }
}

pub fn plan(
    context: &Context,
    manifest_path: PathBuf,
    manifest: AgentManifest,
    instance: String,
    paths: &PlatformPaths,
) -> Result<ProvisioningPlanResult, Error> {
    let output = provisioning::plan(context, manifest_path, manifest, instance, paths)?;
    Ok(ProvisioningPlanResult {
        manifest: output.manifest,
        name: output.name,
        instance: output.instance,
        image: output.image,
        backend: BackendProvisioningResult::Qemu(output.qemu),
        work_dir: output.work_dir,
        seed: output.seed,
        guest_access: output.guest_access,
    })
}

fn runtime_result_from_state(state: &State) -> QemuRuntimeResult {
    QemuRuntimeResult {
        monitor_socket: state.monitor_socket.clone(),
        qmp_socket: state.qmp_socket.clone(),
        pid_file: state.pid_file.clone(),
        serial_log: state.serial_log.clone(),
        qemu_log: state.qemu_log.clone(),
    }
}

fn status_result_from_state(state: &State) -> QemuStatusResult {
    QemuStatusResult {
        disk: state.disk.clone(),
        seed_media: state.seed_media.clone(),
        pid_file: state.pid_file.clone(),
        monitor_socket: state.monitor_socket.clone(),
        qmp_socket: state.qmp_socket.clone(),
        serial_log: state.serial_log.clone(),
        qemu_log: state.qemu_log.clone(),
    }
}

fn image_result_from_state(state: &ImageState) -> ImageResult {
    ImageResult {
        cache_path: state.cache_path.clone(),
        download_path: state.download_path.clone(),
        source_url: state.source_url.clone(),
        format: state.format.clone(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QemuCreateBackend {
    qemu_img: disk::QemuImg,
    ssh_keygen: SshKeygen,
}

impl QemuCreateBackend {
    pub fn resolve() -> Result<Self, Error> {
        Ok(Self {
            qemu_img: disk::QemuImg::resolve()?,
            ssh_keygen: SshKeygen::resolve().map_err(ssh::Error::from)?,
        })
    }

    #[cfg(test)]
    #[must_use]
    pub fn new(qemu_img: impl Into<PathBuf>, ssh_keygen: SshKeygen) -> Self {
        Self {
            qemu_img: disk::QemuImg::new(qemu_img),
            ssh_keygen,
        }
    }

    pub fn create(&self, context: &Context, input: runtime::CreateInput<'_>) -> Result<runtime::CreateOutput, Error> {
        let prepared = self.prepare_manifest(
            context,
            input.manifest_path,
            input.manifest,
            input.instance,
            input.paths,
        )?;
        let disk = disk_path(input.files);
        self.create_disk(context, &prepared, &disk)?;
        let qemu_state = build_state(
            &prepared,
            input.files,
            &runtime_dir(input.paths, &prepared.manifest.name, &prepared.instance),
        );
        let details = create_details(&qemu_state);

        Ok(runtime::CreateOutput {
            state: runtime::BackendState::Qemu(qemu_state),
            guest_access: Some(GuestAccessState {
                user: prepared.guest_access.user,
                private_key: path_text(&prepared.guest_access.private_key),
                public_key: path_text(&prepared.guest_access.public_key),
            }),
            details,
        })
    }

    fn prepare_manifest(
        &self,
        context: &Context,
        manifest_path: PathBuf,
        manifest: AgentManifest,
        instance: String,
        paths: &PlatformPaths,
    ) -> Result<PreparedProvisioning, Error> {
        Ok(provisioning::prepare_manifest_with_keygen(
            context,
            manifest_path,
            manifest,
            instance,
            paths,
            &self.ssh_keygen,
        )?)
    }

    fn create_disk(&self, context: &Context, prepared: &PreparedProvisioning, disk_path: &Path) -> Result<(), Error> {
        image::ensure_cached(context, &prepared.image_cache)?;
        self.qemu_img.create_overlay(
            context,
            &disk::DiskCreateSpec {
                disk: disk_path.to_path_buf(),
                backing_image: prepared.image_cache.image_path.clone(),
                backing_format: prepared.qemu_image.format.to_owned(),
                size: prepared.manifest.resources.storage.clone(),
            },
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QemuRuntimeBackend {
    qemu_system: system::QemuSystem,
}

impl QemuRuntimeBackend {
    pub fn resolve() -> Result<Self, Error> {
        Ok(Self {
            qemu_system: system::QemuSystem::resolve()?,
        })
    }

    #[cfg(test)]
    #[must_use]
    pub fn new(qemu_system: impl Into<PathBuf>) -> Self {
        Self {
            qemu_system: system::QemuSystem::new(qemu_system),
        }
    }

    pub fn start(
        &self,
        context: &Context,
        manifest: &AgentManifest,
        manifest_name: &str,
        instance: &str,
        network: &NetworkState,
        qemu_state: &mut State,
    ) -> Result<runtime::StartOutput, Error> {
        let spec = command::spec_from_state(manifest, manifest_name, instance, network, qemu_state);
        let pid = self.qemu_system.start(context, &spec)?;
        qemu_state.pid = Some(pid);
        qemu_state.last_start_unix_seconds = Some(current_unix_seconds());
        Ok(runtime::StartOutput {
            process: ProcessResult {
                status: "running".to_owned(),
                pid: Some(pid),
                message: None,
            },
            details: BackendRuntimeResult::Qemu(runtime_result_from_state(qemu_state)),
        })
    }
}

pub fn ensure_absent(files: &InstanceFiles) -> Result<(), Error> {
    let disk = disk_path(files);
    if disk.exists() {
        return Err(disk::Error::DiskExists(disk).into());
    }
    Ok(())
}

pub fn run_readiness_command(
    context: &Context,
    state: &InstanceState,
    command: &str,
    timeout: std::time::Duration,
) -> Result<CommandOutput, Error> {
    ssh::run_command_with_timeout(context, state, command, timeout).map_err(Error::from)
}

pub fn wait_provisioned(context: &Context, state: &InstanceState, progress: &mut dyn Progress) -> Result<(), Error> {
    context.logger().verbose(format!(
        "waiting up to {}s for QEMU cloud-init provisioning to finish",
        CLOUD_INIT_WAIT_TIMEOUT.as_secs()
    ));

    let started = Instant::now();
    let deadline = started + CLOUD_INIT_WAIT_TIMEOUT;
    let mut last_output = "cloud-init status has not run yet".to_owned();
    let mut last_status = String::new();
    let mut last_progress = Instant::now();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ErrorKind::CloudInitTimeout {
                timeout_seconds: CLOUD_INIT_WAIT_TIMEOUT.as_secs(),
                last_output,
            }
            .into());
        }

        match ssh::run_command_with_timeout(
            context,
            state,
            "cloud-init status --long",
            remaining.min(CLOUD_INIT_COMMAND_TIMEOUT),
        ) {
            Ok(output) if ssh_output_is_retryable(&output) => {
                last_output = command_output_summary(&output);
                if last_progress.elapsed() >= CLOUD_INIT_PROGRESS_INTERVAL {
                    progress.info(format!(
                        "waiting for SSH/cloud-init after {}s",
                        started.elapsed().as_secs()
                    ));
                    last_progress = Instant::now();
                }
                context
                    .logger()
                    .verbose_with(|| format!("QEMU cloud-init is not reachable yet: {last_output}"));
            }
            Ok(output) => {
                last_output = command_output_summary(&output);
                let status = cloud_init_status(&output).unwrap_or_else(|| "unknown".to_owned());
                if status != last_status {
                    progress.info(format!("cloud-init status: {status}"));
                    last_status.clone_from(&status);
                    last_progress = Instant::now();
                } else if last_progress.elapsed() >= CLOUD_INIT_PROGRESS_INTERVAL {
                    progress.info(format!(
                        "cloud-init still {status} after {}s",
                        started.elapsed().as_secs()
                    ));
                    last_progress = Instant::now();
                }

                if status == "done" {
                    context.logger().verbose_with(|| {
                        format!(
                            "QEMU cloud-init provisioning finished after {}ms",
                            started.elapsed().as_millis()
                        )
                    });
                    return Ok(());
                }

                if status == "error" || status == "degraded" || (output.status != 0 && status != "running") {
                    let diagnostics = cloud_init_diagnostics(context, state);
                    return Err(ErrorKind::CloudInitFailed {
                        output: format!("{last_output}\n\n{diagnostics}"),
                    }
                    .into());
                }
            }
            Err(error) if error.is_retryable() => {
                last_output = error.to_string();
                if last_progress.elapsed() >= CLOUD_INIT_PROGRESS_INTERVAL {
                    progress.info(format!(
                        "waiting for cloud-init over SSH after {}s",
                        started.elapsed().as_secs()
                    ));
                    last_progress = Instant::now();
                }
                context
                    .logger()
                    .verbose_with(|| format!("QEMU cloud-init is not reachable yet: {last_output}"));
            }
            Err(error) => return Err(error.into()),
        }

        std::thread::sleep(remaining.min(CLOUD_INIT_POLL_DELAY));
    }
}

pub fn down_with_process_control(
    context: &Context,
    input: runtime::DownInput<'_>,
    state: &mut State,
    mut process_status: impl FnMut(u32) -> Result<ProcessStatus, platform::ProcessStatusError>,
    mut terminate: impl FnMut(u32) -> Result<(), platform::TerminateProcessError>,
    mut wait_for_exit: impl FnMut(u32) -> Result<bool, platform::ProcessStatusError>,
) -> Result<runtime::DownOutput, Error> {
    match input.status {
        InstanceStatus::Running => {
            let pid = state.pid.ok_or_else(|| ErrorKind::MissingRunningPid {
                name: input.name.to_owned(),
            })?;
            let mut terminated_pid = None;
            let process_result;
            match process_status(pid)? {
                ProcessStatus::Running => {
                    context
                        .logger()
                        .verbose_with(|| format!("requesting QEMU guest shutdown for {}", input.name));
                    if request_qmp_command(context, state, "system_powerdown")
                        && wait_for_process_exit_with_status(&mut process_status, pid, MONITOR_POWERDOWN_WAIT)?
                    {
                        terminated_pid = Some(pid);
                        process_result = "powered-off";
                    } else if request_qmp_command(context, state, "quit")
                        && wait_for_process_exit_with_status(&mut process_status, pid, MONITOR_QUIT_WAIT)?
                    {
                        terminated_pid = Some(pid);
                        process_result = "quit";
                    } else {
                        context
                            .logger()
                            .verbose_with(|| format!("terminating QEMU pid {pid} for {}", input.name));
                        match terminate(pid) {
                            Ok(()) => {
                                if !wait_for_exit(pid)? {
                                    return Err(ErrorKind::ProcessStillRunning { pid }.into());
                                }
                                terminated_pid = Some(pid);
                                process_result = "terminated";
                            }
                            Err(error) => match process_status(pid)? {
                                ProcessStatus::NotFound => {
                                    context.logger().warn(format!(
                                        "QEMU pid {pid} for {} exited before termination completed",
                                        input.name
                                    ));
                                    process_result = "missing";
                                }
                                ProcessStatus::Running => return Err(error.into()),
                            },
                        }
                    }
                }
                ProcessStatus::NotFound => {
                    context.logger().warn(format!(
                        "runtime state for {} recorded stale QEMU pid {pid}",
                        input.name
                    ));
                    process_result = "missing";
                }
            }
            cleanup_runtime_files(state)?;
            state.pid = None;
            Ok(runtime::DownOutput {
                process_status: process_result,
                terminated_pid,
            })
        }
        InstanceStatus::Stopped => {
            cleanup_runtime_files(state)?;
            Ok(runtime::DownOutput {
                process_status: "already-stopped",
                terminated_pid: None,
            })
        }
        InstanceStatus::Created => Ok(runtime::DownOutput {
            process_status: "not-started",
            terminated_pid: None,
        }),
    }
}

fn request_qmp_command(context: &Context, state: &State, command: &str) -> bool {
    let qmp_socket = PathBuf::from(&state.qmp_socket);
    match platform::connect_local_socket(&qmp_socket) {
        Ok(socket) => {
            let mut reader = BufReader::new(socket);
            if let Err(error) = read_qmp_greeting(&mut reader)
                .and_then(|()| qmp_execute(&mut reader, "qmp_capabilities"))
                .and_then(|()| qmp_execute(&mut reader, command))
            {
                context.logger().warn(format!(
                    "failed to execute QMP command {command} on {}: {error}",
                    qmp_socket.display()
                ));
                return false;
            }
            context
                .logger()
                .verbose_with(|| format!("executed QMP command {command} on {}", qmp_socket.display()));
            true
        }
        Err(error) => {
            context.logger().warn(format!(
                "failed to connect to QEMU QMP socket {}: {error}",
                qmp_socket.display()
            ));
            false
        }
    }
}

fn read_qmp_greeting(reader: &mut BufReader<platform::LocalSocket>) -> std::io::Result<()> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if line.contains("\"QMP\"") {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "QMP greeting did not contain QMP marker",
        ))
    }
}

fn qmp_execute(reader: &mut BufReader<platform::LocalSocket>, command: &str) -> std::io::Result<()> {
    writeln!(reader.get_mut(), r#"{{"execute":"{command}"}}"#)?;
    reader.get_mut().flush()?;
    read_qmp_command_response(reader)
}

fn read_qmp_command_response(reader: &mut BufReader<platform::LocalSocket>) -> std::io::Result<()> {
    for _ in 0..8 {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Ok(());
        }
        if line.contains("\"return\"") {
            return Ok(());
        }
        if line.contains("\"error\"") {
            return Err(std::io::Error::other(format!("QMP returned error: {}", line.trim())));
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "QMP command response was not received",
    ))
}

fn wait_for_process_exit_with_status(
    process_status: &mut impl FnMut(u32) -> Result<ProcessStatus, platform::ProcessStatusError>,
    pid: u32,
    timeout: Duration,
) -> Result<bool, Error> {
    let deadline = Instant::now() + timeout;
    loop {
        if process_status(pid)? == ProcessStatus::NotFound {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

pub fn status_with_process_status(
    instance_status: InstanceStatus,
    state: &State,
    mut process_status: impl FnMut(u32) -> Result<ProcessStatus, platform::ProcessStatusError>,
) -> runtime::StatusOutput {
    let process = inspect_process(instance_status, state, &mut process_status);
    runtime::StatusOutput {
        stale: process.stale,
        process: process.report,
        details: BackendStatusResult::Qemu(status_result_from_state(state)),
    }
}

pub const fn runtime_summary(state: &State) -> runtime::RuntimeSummary {
    runtime::RuntimeSummary { pid: state.pid }
}

pub fn runtime_details(state: &State) -> BackendRuntimeResult {
    BackendRuntimeResult::Qemu(runtime_result_from_state(state))
}

#[must_use]
pub fn log_path(state: &State, file: LogFile) -> PathBuf {
    match file {
        LogFile::Serial => PathBuf::from(&state.serial_log),
        LogFile::Qemu => PathBuf::from(&state.qemu_log),
    }
}

pub fn shell_command(state: &InstanceState) -> Result<HostCommandResult, Error> {
    ssh::interactive_shell_command(state).map_err(Error::from)
}

pub fn run_user_command(
    context: &Context,
    state: &InstanceState,
    command: &[String],
    timeout: std::time::Duration,
) -> Result<CommandOutput, Error> {
    ssh::run_user_command_with_timeout(context, state, command, timeout).map_err(Error::from)
}

pub fn cleanup_runtime_files(state: &State) -> Result<(), Error> {
    system::cleanup_runtime_files(
        &PathBuf::from(&state.pid_file),
        &PathBuf::from(&state.monitor_socket),
        &PathBuf::from(&state.qmp_socket),
    )
    .map_err(Error::from)
}

#[must_use]
fn runtime_dir(paths: &PlatformPaths, manifest_name: &str, instance: &str) -> PathBuf {
    paths
        .runtime
        .join("instances")
        .join(manifest_name)
        .join(instance)
        .join("qemu")
}

fn build_state(prepared: &PreparedProvisioning, files: &InstanceFiles, qemu_runtime_dir: &Path) -> State {
    State {
        image: ImageState {
            os: prepared.provisioning_plan.image.os_name().to_owned(),
            architecture: prepared.provisioning_plan.image.architecture_name().to_owned(),
            variant: prepared.provisioning_plan.image.variant_name().to_owned(),
            source_url: prepared.qemu_image.url.to_owned(),
            cache_key: prepared.qemu_image.cache_key.to_owned(),
            cache_path: path_text(&prepared.image_cache.image_path),
            download_path: path_text(&prepared.image_cache.download_path),
            format: prepared.qemu_image.format.to_owned(),
        },
        disk: path_text(&disk_path(files)),
        work_dir: path_text(&prepared.seed.work_dir),
        seed_media: path_text(&prepared.seed.seed_media),
        seed_meta_data: path_text(&prepared.seed.meta_data),
        seed_user_data: path_text(&prepared.seed.user_data),
        bootstrap_script: path_text(&prepared.seed.bootstrap_script),
        monitor_socket: path_text(&qemu_runtime_dir.join("monitor.sock")),
        qmp_socket: path_text(&qemu_runtime_dir.join("qmp.sock")),
        pid_file: path_text(&qemu_runtime_dir.join("qemu.pid")),
        serial_log: path_text(&files.logs_dir.join("serial.log")),
        qemu_log: path_text(&files.logs_dir.join("qemu.log")),
        pid: None,
        last_start_unix_seconds: None,
    }
}

fn disk_path(files: &InstanceFiles) -> PathBuf {
    files.instance_dir.join("disk.qcow2")
}

struct ProcessInspection {
    stale: bool,
    report: ProcessResult,
}

fn inspect_process(
    instance_status: InstanceStatus,
    state: &State,
    process_status: &mut impl FnMut(u32) -> Result<ProcessStatus, platform::ProcessStatusError>,
) -> ProcessInspection {
    let Some(pid) = state.pid else {
        if instance_status == InstanceStatus::Running {
            return ProcessInspection {
                stale: true,
                report: ProcessResult {
                    status: "missing".to_owned(),
                    pid: None,
                    message: Some("runtime status is running but no QEMU pid is recorded".to_owned()),
                },
            };
        }
        return ProcessInspection {
            stale: false,
            report: ProcessResult {
                status: "not-recorded".to_owned(),
                pid: None,
                message: None,
            },
        };
    };

    if instance_status != InstanceStatus::Running {
        return ProcessInspection {
            stale: false,
            report: ProcessResult {
                status: "recorded".to_owned(),
                pid: Some(pid),
                message: Some(format!(
                    "runtime status is {instance_status} but QEMU pid is still recorded"
                )),
            },
        };
    }

    match process_status(pid) {
        Ok(ProcessStatus::Running) => ProcessInspection {
            stale: false,
            report: ProcessResult {
                status: "running".to_owned(),
                pid: Some(pid),
                message: None,
            },
        },
        Ok(ProcessStatus::NotFound) => ProcessInspection {
            stale: true,
            report: ProcessResult {
                status: "missing".to_owned(),
                pid: Some(pid),
                message: Some(format!("runtime status is running but QEMU pid {pid} is not running")),
            },
        },
        Err(error) => ProcessInspection {
            stale: false,
            report: ProcessResult {
                status: "unknown".to_owned(),
                pid: Some(pid),
                message: Some(error.to_string()),
            },
        },
    }
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn path_text(path: &Path) -> String {
    path.display().to_string()
}

fn command_output_summary(output: &CommandOutput) -> String {
    format!(
        "status {}; stdout: {}; stderr: {}",
        output.status,
        output_tail(&output.stdout),
        output_tail(&output.stderr)
    )
}

fn ssh_output_is_retryable(output: &CommandOutput) -> bool {
    output.status == 255 && ssh_error_is_retryable(&output.stderr)
}

fn ssh_error_is_retryable(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    [
        "banner exchange",
        "connection timed out",
        "connection refused",
        "connection reset",
        "connection closed",
        "no route to host",
        "host is down",
    ]
    .iter()
    .any(|message| stderr.contains(message))
}

fn output_tail(output: &str) -> String {
    const LIMIT: usize = 4000;

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

fn cloud_init_status(output: &CommandOutput) -> Option<String> {
    output
        .stdout
        .lines()
        .chain(output.stderr.lines())
        .map(str::trim)
        .find_map(|line| line.strip_prefix("status:"))
        .map(str::trim)
        .filter(|status| !status.is_empty())
        .map(str::to_owned)
}

fn cloud_init_diagnostics(context: &Context, state: &InstanceState) -> String {
    let command = [
        "printf '%s\\n' '--- cloud-init status --long ---'",
        "cloud-init status --long || true",
        "printf '\\n%s\\n' '--- /var/log/cloud-init.log errors ---'",
        "grep -Ei 'traceback|error|warning|failed|exception|oserror|keyerror' /var/log/cloud-init.log | tail -n 80 || true",
        "printf '\\n%s\\n' '--- /var/log/cloud-init-output.log tail ---'",
        "tail -n 80 /var/log/cloud-init-output.log || true",
    ]
    .join("; ");

    match ssh::run_command_with_timeout(context, state, &command, Duration::from_secs(15)) {
        Ok(output) => command_output_summary(&output),
        Err(error) => format!("failed to collect cloud-init diagnostics: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use agentdp_core::platform::ssh::CommandOutput;

    use super::ssh_output_is_retryable;

    #[test]
    fn ssh_banner_timeout_is_retryable_during_cloud_init_wait() {
        let output = CommandOutput {
            status: 255,
            stdout: String::new(),
            stderr: "Connection timed out during banner exchange".to_owned(),
        };

        assert!(ssh_output_is_retryable(&output));
    }

    #[test]
    fn cloud_init_command_failure_is_not_ssh_retryable() {
        let output = CommandOutput {
            status: 1,
            stdout: "status: error".to_owned(),
            stderr: String::new(),
        };

        assert!(!ssh_output_is_retryable(&output));
    }
}
