use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::agent::{
    AgentInstancePhase, BackendState, GuestAccessState, NetworkState, PortMappingState, PortProtocolState,
    ProcessStatus as AgentProcessStatus,
};
use agentdp_core::manifest::{AgentManifest, NetworkMode};
use agentdp_core::mediated_network::MediatedNetworkProfile;
use agentdp_core::provisioning::guest_os::GuestOsAdapter;
use agentdp_core::provisioning::image::{ImageCatalog, ImageRequest};
use agentdp_core::provisioning::secrets::SecretBindings;
use agentdp_platform::ssh::SshKeygen;
use agentdp_platform::time;
use agentdp_platform::{self as platform, process::ProcessStatus};
use agentdp_protocol::client_server::LogFile;
use agentdp_qemu::{command, disk, image, qmp, system};

use crate::agent::{AGENT_BASE_INSTANCE, AgentBaseFiles, AgentBaseKey, AgentInstanceFiles, AgentManifestContext};
use crate::agent::{AgentName, InstanceName};
use crate::backend::{self, BackendBaseImageIdentity, CreateBaseInput, CreateBaseOutput};
use crate::host::collect_runtime_host_inputs;
use crate::services::InstanceNetwork;

use super::control;
use super::error::{Error, ErrorKind};
use super::network::{
    cleanup_instance_network_for_state, ensure_instance_network_attached, instance_network_is_attached,
    start_instance_network, terminate_started_qemu, update_instance_network_secrets, wait_instance_network_ready,
};
use super::provisioning::{self, PreparedBaseProvisioning, PreparedProvisioning};
use super::{ImageState, MediatedCaState, State};

const AGENT_BASE_SCHEMA: &str = "agentdp-qemu-agent-base-v5-unique-cloud-init-instance-id";

#[cfg(test)]
pub(super) use super::network::egress_policy;
#[cfg(test)]
use super::network::instance_network_status as instance_network_status_for_state;

const MONITOR_POWERDOWN_WAIT: Duration = Duration::from_secs(30);
const MONITOR_QUIT_WAIT: Duration = Duration::from_secs(5);
pub(super) const TERMINATE_WAIT: Duration = Duration::from_secs(30);

pub(super) async fn create_instance(
    context: &Context,
    input: backend::CreateInstanceInput<'_>,
    qemu_img: &disk::QemuImg,
    ssh_keygen: &SshKeygen,
) -> Result<backend::CreateInstanceOutput, Error> {
    let prepared = provisioning::prepare_create_with_keygen(
        context,
        provisioning::PrepareCreateInput {
            manifest: input.manifest,
            instance: input.instance,
            provisioning_plan: input.provisioning_plan,
            rendered_bootstrap: input.rendered_bootstrap,
            image_cache_dir: input.image_cache_dir,
            work_dir: &input.files.instance_dir,
        },
        ssh_keygen,
    )
    .await?;
    let disk = disk_path(input.files);
    image::ensure_cached(context, &prepared.image_cache).await?;
    qemu_img
        .create_overlay(
            context,
            &disk::DiskCreateSpec {
                disk,
                backing_image: input.agent_base.disk.clone(),
                backing_format: "qcow2".to_owned(),
                size: prepared.manifest.spec.resources.storage.clone(),
            },
        )
        .await?;
    let qemu_state = build_state(&prepared, input.files);

    Ok(backend::CreateInstanceOutput {
        state: BackendState::Qemu(qemu_state),
        guest_access: Some(GuestAccessState {
            user: prepared.guest_access.user,
            private_key: path_text(&prepared.guest_access.private_key),
            public_key: path_text(&prepared.guest_access.public_key),
        }),
    })
}

pub(super) fn base_image_identity(manifest: &AgentManifest) -> Result<BackendBaseImageIdentity, Error> {
    let (catalog, qemu_image) = qemu_image_for_manifest(manifest)?;
    Ok(BackendBaseImageIdentity {
        base_key_schema: AGENT_BASE_SCHEMA,
        os: catalog.os_name(),
        architecture: catalog.architecture_name(),
        variant: catalog.variant_name(),
        cache_key: qemu_image.cache_key,
        url: qemu_image.url,
        format: qemu_image.format,
    })
}

pub(super) async fn create_base(
    context: &Context,
    input: CreateBaseInput<'_>,
    qemu_img: &disk::QemuImg,
) -> Result<CreateBaseOutput, Error> {
    let storage = input.manifest.value().spec.resources.storage.clone();
    let prepared = provisioning::prepare_base(
        context,
        provisioning::PrepareCreateInput {
            manifest: input.manifest,
            instance: input.instance.to_string(),
            provisioning_plan: input.provisioning_plan,
            rendered_bootstrap: input.rendered_bootstrap,
            image_cache_dir: input.image_cache_dir,
            work_dir: &input.files.base_dir,
        },
    )
    .await?;
    image::ensure_cached(context, &prepared.image_cache).await?;
    let exists = tokio::fs::try_exists(&input.files.disk)
        .await
        .map_err(|source| disk::Error::CreateDirectory {
            path: input.files.disk.clone(),
            source,
        })?;
    if !exists {
        qemu_img
            .create_overlay(
                context,
                &disk::DiskCreateSpec {
                    disk: input.files.disk.clone(),
                    backing_image: prepared.image_cache.image_path.clone(),
                    backing_format: prepared.qemu_image.format.to_owned(),
                    size: storage,
                },
            )
            .await?;
    }
    let state = build_agent_base_state(&prepared, input.files);
    Ok(CreateBaseOutput {
        state: BackendState::Qemu(state),
        image_cache_key: prepared.qemu_image.cache_key.to_owned(),
    })
}

pub(super) async fn start_base(
    context: &Context,
    qemu_system: &system::QemuSystem,
    manifest: &AgentManifestContext,
    agent: &str,
    instance: &str,
    network: &NetworkState,
    state: &mut State,
) -> Result<backend::StartOutput, Error> {
    let spec = spec_from_state(manifest.value(), agent, instance, network, state)?;
    let pid = qemu_system.start(context, &spec).await?;
    state.pid = Some(pid);
    state.last_start_unix_seconds = Some(time::unix_seconds());
    Ok(backend::StartOutput {
        process: AgentProcessStatus {
            status: "running".to_owned(),
            pid: Some(pid),
            message: None,
        },
        host_ports: BTreeMap::new(),
    })
}

pub(super) async fn stop_base(
    context: &Context,
    agent: &AgentName,
    instance: &InstanceName,
    state: &mut State,
) -> Result<backend::StopOutput, Error> {
    let output = if let Some(pid) = state.pid {
        let label = format!("agent base {agent}/{instance}");
        stop_recorded_qemu_process(context, &label, state, pid).await?
    } else {
        backend::StopOutput {
            process_status: "already-stopped",
            terminated_pid: None,
        }
    };
    state.pid = None;
    cleanup_qemu_runtime_files(state).await?;
    Ok(output)
}

pub(super) async fn stop_base_runtime(
    context: &Context,
    agent: &AgentName,
    key: &AgentBaseKey,
    files: &AgentBaseFiles,
) -> Result<backend::StopOutput, Error> {
    let pid = match read_qemu_pid(&files.run_dir.join("qemu.pid")).await {
        Some(pid) => Some(pid),
        None => find_agent_base_qemu_pid(files).await,
    };
    let mut state = agent_base_state_from_files(files, pid);
    let instance = InstanceName::new(AGENT_BASE_INSTANCE);
    context
        .logger()
        .verbose_with(|| format!("stopping agent base runtime {agent}/{key}"));
    stop_base(context, agent, &instance, &mut state).await
}

fn qemu_image_for_manifest(
    manifest: &AgentManifest,
) -> Result<(agentdp_core::provisioning::image::CatalogImage, image::QemuImage), Error> {
    let catalog = ImageCatalog::resolve(ImageRequest {
        os: manifest.spec.image.os,
    });
    let qemu_image = image::resolve_image(catalog).ok_or(ErrorKind::UnsupportedImage {
        os: catalog.os_name(),
        architecture: catalog.architecture_name(),
        variant: catalog.variant_name(),
    })?;
    Ok((catalog, qemu_image))
}

pub(super) async fn start_instance(
    qemu_system: &system::QemuSystem,
    input: StartInstanceInput<'_>,
    qemu_state: &mut State,
) -> Result<backend::StartOutput, Error> {
    let StartInstanceInput {
        context,
        instance_network,
        manifest,
        agent,
        instance,
        network,
    } = input;
    let agent_name = AgentName::new(agent);
    let instance_name = InstanceName::new(instance);
    let spec = spec_from_state(manifest.value(), agent, instance, network, qemu_state)?;
    clear_stored_runtime_secrets_if_unconfigured(manifest.value(), qemu_state);
    let host_inputs = collect_runtime_host_inputs(
        context,
        manifest.source_path(),
        manifest.value(),
        &qemu_state.mediated_secrets,
    )
    .await?;
    qemu_state.mediated_secrets = host_inputs.stored_secrets.clone();
    let network_task = start_instance_network(
        context,
        instance_network,
        agent,
        instance,
        network,
        qemu_state,
        host_inputs.runtime_secrets,
    )
    .await?;
    let pid = match qemu_system.start(context, &spec).await {
        Ok(pid) => pid,
        Err(error) => {
            if network_task.is_some()
                && let Err(cleanup_error) =
                    cleanup_instance_network_for_state(instance_network, &agent_name, &instance_name, qemu_state).await
            {
                context.logger().warn(format!(
                    "failed to clean instance network socket after QEMU startup failure: {cleanup_error}"
                ));
            }
            return Err(error.into());
        }
    };
    qemu_state.pid = Some(pid);
    qemu_state.last_start_unix_seconds = Some(time::unix_seconds());
    let mut host_ports = BTreeMap::new();
    if let Some(task) = network_task {
        match wait_instance_network_ready(context, task, agent, instance).await {
            Ok(status) => {
                host_ports = host_port_mappings(&status);
            }
            Err(error) => {
                let _cleanup =
                    cleanup_instance_network_for_state(instance_network, &agent_name, &instance_name, qemu_state).await;
                terminate_started_qemu(context, instance, pid).await?;
                cleanup_runtime_files(instance_network, &agent_name, &instance_name, qemu_state).await?;
                qemu_state.pid = None;
                qemu_state.last_start_unix_seconds = None;
                return Err(error);
            }
        }
    }
    Ok(backend::StartOutput {
        process: AgentProcessStatus {
            status: "running".to_owned(),
            pid: Some(pid),
            message: None,
        },
        host_ports,
    })
}

pub(super) struct StartInstanceInput<'a> {
    pub context: &'a Context,
    pub instance_network: &'a InstanceNetwork,
    pub manifest: &'a AgentManifestContext,
    pub agent: &'a str,
    pub instance: &'a str,
    pub network: &'a NetworkState,
}

pub(super) struct RuntimeInput<'a> {
    pub context: &'a Context,
    pub instance_network: &'a InstanceNetwork,
    pub instance_status: AgentInstancePhase,
    pub agent: &'a str,
    pub instance: &'a str,
    pub network: &'a NetworkState,
    pub manifest: &'a AgentManifestContext,
}

pub(super) async fn stop_instance(
    context: &Context,
    instance_network: &InstanceNetwork,
    input: backend::StopInstanceInput<'_>,
    state: &mut State,
) -> Result<backend::StopOutput, Error> {
    let pid = match state.pid {
        Some(pid) => Some(pid),
        None => read_qemu_pid(Path::new(&state.pid_file)).await,
    };
    if let Some(pid) = pid {
        let output = stop_recorded_qemu_process(context, input.name, state, pid).await?;
        cleanup_runtime_files(instance_network, input.agent, input.instance, state).await?;
        state.pid = None;
        return Ok(output);
    }
    cleanup_runtime_files(instance_network, input.agent, input.instance, state).await?;
    let process_status = match input.status {
        AgentInstancePhase::Materialized => "not-started",
        AgentInstancePhase::Starting | AgentInstancePhase::Running | AgentInstancePhase::Stopping => "missing",
        AgentInstancePhase::Stopped | AgentInstancePhase::Deleting | AgentInstancePhase::Deleted => "already-stopped",
        AgentInstancePhase::Failed => "failed-not-running",
    };
    Ok(backend::StopOutput {
        process_status,
        terminated_pid: None,
    })
}

async fn stop_recorded_qemu_process(
    context: &Context,
    label: &str,
    state: &State,
    pid: u32,
) -> Result<backend::StopOutput, Error> {
    match platform::process::process_status(pid).await? {
        ProcessStatus::Running => stop_running_qemu_process(context, label, state, pid).await,
        ProcessStatus::NotFound => {
            context
                .logger()
                .warn(format!("runtime state for {label} recorded stale QEMU pid {pid}"));
            Ok(backend::StopOutput {
                process_status: "missing",
                terminated_pid: None,
            })
        }
    }
}

async fn stop_running_qemu_process(
    context: &Context,
    label: &str,
    state: &State,
    pid: u32,
) -> Result<backend::StopOutput, Error> {
    context
        .logger()
        .verbose_with(|| format!("requesting QEMU guest shutdown for {label}"));
    if request_qmp_command(context, state, "system_powerdown").await
        && platform::process::wait_for_process_exit(pid, MONITOR_POWERDOWN_WAIT).await?
    {
        return Ok(qemu_process_stopped("powered-off", pid));
    }
    if request_qmp_command(context, state, "quit").await
        && platform::process::wait_for_process_exit(pid, MONITOR_QUIT_WAIT).await?
    {
        return Ok(qemu_process_stopped("quit", pid));
    }

    context
        .logger()
        .verbose_with(|| format!("terminating QEMU pid {pid} for {label}"));
    match platform::process::terminate_process(pid).await {
        Ok(()) => {
            if !platform::process::wait_for_process_exit(pid, TERMINATE_WAIT).await? {
                return Err(ErrorKind::ProcessStillRunning { pid }.into());
            }
            Ok(qemu_process_stopped("terminated", pid))
        }
        Err(error) => match platform::process::process_status(pid).await? {
            ProcessStatus::NotFound => {
                context.logger().warn(format!(
                    "QEMU pid {pid} for {label} exited before termination completed"
                ));
                Ok(backend::StopOutput {
                    process_status: "missing",
                    terminated_pid: None,
                })
            }
            ProcessStatus::Running => Err(error.into()),
        },
    }
}

const fn qemu_process_stopped(process_status: &'static str, pid: u32) -> backend::StopOutput {
    backend::StopOutput {
        process_status,
        terminated_pid: Some(pid),
    }
}

async fn request_qmp_command(context: &Context, state: &State, command: &'static str) -> bool {
    let qmp_socket = PathBuf::from(&state.qmp_socket);
    match qmp::execute(&qmp_socket, command).await {
        Ok(()) => {
            context
                .logger()
                .verbose_with(|| format!("executed QMP command {command} on {}", qmp_socket.display()));
            true
        }
        Err(error) => {
            context.logger().warn(format!(
                "failed to execute QMP command {command} on {}: {error}",
                qmp_socket.display()
            ));
            false
        }
    }
}

async fn qemu_runtime_files_exist(state: &State) -> bool {
    let guest_control_socket = guest_control_socket_path(state);
    for path in [
        Path::new(&state.pid_file),
        Path::new(&state.monitor_socket),
        Path::new(&state.qmp_socket),
        guest_control_socket.as_path(),
    ] {
        if path.as_os_str().is_empty() {
            continue;
        }
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            return true;
        }
    }
    if let Some(network) = &state.instance_network
        && tokio::fs::try_exists(&network.stream_socket).await.unwrap_or(false)
    {
        return true;
    }
    false
}

#[cfg(test)]
pub(super) fn instance_network_status(
    instance_network: &InstanceNetwork,
    agent: &AgentName,
    instance: &InstanceName,
    state: &State,
) -> Option<agentdp_core::agent::AgentInstanceNetworkStatus> {
    instance_network_status_for_state(instance_network, agent, instance, state)
}

pub(super) async fn ensure_attached(input: RuntimeInput<'_>, state: &State) -> Result<(), Error> {
    if input.instance_status != AgentInstancePhase::Running {
        return Ok(());
    }
    let inspection = inspect_process(input.instance_status, state).await;
    if inspection.stale {
        return Err(ErrorKind::StaleRunningRuntime {
            message: inspection
                .report
                .message
                .unwrap_or_else(|| "runtime status is stale".to_owned()),
        }
        .into());
    }
    let agent = AgentName::new(input.agent);
    let instance = InstanceName::new(input.instance);
    if !instance_network_is_attached(input.instance_network, &agent, &instance, state).await {
        let host_inputs = collect_runtime_host_inputs(
            input.context,
            input.manifest.source_path(),
            input.manifest.value(),
            &state.mediated_secrets,
        )
        .await?;
        ensure_instance_network_attached(
            input.context,
            input.instance_network,
            input.agent,
            input.instance,
            input.network,
            state,
            host_inputs.runtime_secrets,
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn reconcile(input: RuntimeInput<'_>, state: &mut State) -> Result<backend::ReconcileOutput, Error> {
    let inspection = inspect_process(input.instance_status, state).await;
    let mut backend_changed = false;
    let mut mark_stopped = false;
    let mut reported_stale = inspection.stale;
    let mut process = inspection.report;
    let agent = AgentName::new(input.agent);
    let instance = InstanceName::new(input.instance);
    if input.instance_status == AgentInstancePhase::Running
        && !inspection.stale
        && !instance_network_is_attached(input.instance_network, &agent, &instance, state).await
    {
        clear_live_runtime_secrets_if_unconfigured(&input, state)?;
        let host_inputs = collect_runtime_host_inputs(
            input.context,
            input.manifest.source_path(),
            input.manifest.value(),
            &state.mediated_secrets,
        )
        .await?;
        state.mediated_secrets = host_inputs.stored_secrets.clone();
        ensure_instance_network_attached(
            input.context,
            input.instance_network,
            input.agent,
            input.instance,
            input.network,
            state,
            host_inputs.runtime_secrets,
        )
        .await?;
    }

    match input.instance_status {
        AgentInstancePhase::Running if inspection.stale => {
            cleanup_runtime_files(input.instance_network, &agent, &instance, state).await?;
            state.pid = None;
            backend_changed = true;
            mark_stopped = true;
        }
        phase if !phase_allows_runtime(phase) && state.pid.is_some() => {
            cleanup_runtime_files(input.instance_network, &agent, &instance, state).await?;
            state.pid = None;
            backend_changed = true;
            process = stopped_process_observation();
            reported_stale = false;
        }
        phase if !phase_allows_runtime(phase) && qemu_runtime_files_exist(state).await => {
            cleanup_runtime_files(input.instance_network, &agent, &instance, state).await?;
            backend_changed = true;
            process = stopped_process_observation();
            reported_stale = false;
        }
        _ => {}
    }

    Ok(backend::ReconcileOutput {
        stale: reported_stale,
        mark_stopped,
        backend_changed,
        process,
        host_ports: BTreeMap::new(),
    })
}

pub(super) async fn reconcile_host_inputs(
    input: RuntimeInput<'_>,
    state: &mut State,
) -> Result<backend::ReconcileHostInputsOutput, Error> {
    if input.instance_status != AgentInstancePhase::Running {
        return Ok(backend::ReconcileHostInputsOutput::default());
    }
    clear_live_runtime_secrets_if_unconfigured(&input, state)?;
    let collected = collect_runtime_host_inputs(
        input.context,
        input.manifest.source_path(),
        input.manifest.value(),
        &state.mediated_secrets,
    )
    .await?;

    update_instance_network_secrets(
        input.instance_network,
        input.agent,
        input.instance,
        &collected.runtime_secrets,
    )?;
    let mut guest_files_updated = 0;
    let mut guest_file_failures = 0;
    let control_socket = guest_control_socket_path(state);
    for file in &collected.files {
        let path = user_home_relative_seed_path(input.manifest.value(), &file.path)?;
        match control::write_user_file(
            input.context,
            &control_socket,
            &path,
            &file.contents,
            file.permissions.as_str(),
        )
        .await
        {
            Ok(true) => {
                guest_files_updated += 1;
            }
            Ok(false) => {}
            Err(error) => {
                guest_file_failures += 1;
                input
                    .context
                    .logger()
                    .warn(format!("failed to write guest file {}: {error}", file.path));
            }
        }
    }
    state.mediated_secrets = collected.stored_secrets;
    Ok(backend::ReconcileHostInputsOutput {
        guest_files_updated,
        guest_file_failures,
    })
}

fn user_home_relative_seed_path(manifest: &AgentManifest, path: &str) -> Result<String, Error> {
    let layout = GuestOsAdapter::for_os(manifest.spec.image.os).capabilities().layout;
    let prefix = format!("{}/", layout.agent_home.trim_end_matches('/'));
    path.strip_prefix(&prefix)
        .filter(|relative| !relative.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            ErrorKind::GuestControlMessage {
                code: "guest_file_path".to_owned(),
                message: format!("runtime host input file {path} is not under {}", layout.agent_home),
            }
            .into()
        })
}

fn clear_live_runtime_secrets_if_unconfigured(input: &RuntimeInput<'_>, state: &mut State) -> Result<(), Error> {
    if !should_clear_runtime_secrets(input.manifest.value(), state) {
        return Ok(());
    }
    update_instance_network_secrets(
        input.instance_network,
        input.agent,
        input.instance,
        &SecretBindings::default(),
    )?;
    state.mediated_secrets = SecretBindings::default();
    Ok(())
}

fn clear_stored_runtime_secrets_if_unconfigured(manifest: &AgentManifest, state: &mut State) -> bool {
    if !should_clear_runtime_secrets(manifest, state) {
        return false;
    }
    state.mediated_secrets = SecretBindings::default();
    true
}

fn should_clear_runtime_secrets(manifest: &AgentManifest, state: &State) -> bool {
    !manifest.host_input_requirements().has_mediated_secret_inputs() && !state.mediated_secrets.is_empty()
}

#[must_use]
pub(super) fn log_path(state: &State, file: LogFile) -> PathBuf {
    match file {
        LogFile::Serial => PathBuf::from(&state.serial_log),
        LogFile::Qemu => PathBuf::from(&state.qemu_log),
        LogFile::Events => unreachable!("instance event log path is owned by AgentInstance"),
    }
}

pub(super) async fn cleanup_runtime_files(
    instance_network: &InstanceNetwork,
    agent: &AgentName,
    instance: &InstanceName,
    state: &State,
) -> Result<(), Error> {
    cleanup_instance_network_for_state(instance_network, agent, instance, state).await?;
    cleanup_qemu_runtime_files(state).await
}

async fn cleanup_qemu_runtime_files(state: &State) -> Result<(), Error> {
    system::cleanup_runtime_files(
        &PathBuf::from(&state.pid_file),
        &PathBuf::from(&state.monitor_socket),
        &PathBuf::from(&state.qmp_socket),
        &guest_control_socket_path(state),
    )
    .await
    .map_err(Error::from)?;
    Ok(())
}

#[must_use]
fn build_state(prepared: &PreparedProvisioning, files: &AgentInstanceFiles) -> State {
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
        seed_network_config: path_text(&prepared.seed.network_config),
        seed_user_data: path_text(&prepared.seed.user_data),
        monitor_socket: path_text(&files.run_dir.join("monitor.sock")),
        qmp_socket: path_text(&files.run_dir.join("qmp.sock")),
        guest_control_socket: path_text(&files.run_dir.join("guest-control.sock")),
        pid_file: path_text(&files.run_dir.join("qemu.pid")),
        serial_log: path_text(&files.logs_dir.join("serial.log")),
        qemu_log: path_text(&files.logs_dir.join("qemu.log")),
        instance_network: (prepared.manifest.spec.network.mode == NetworkMode::Mediated)
            .then(|| instance_network_state(&files.run_dir)),
        mediated_secrets: prepared.mediated_secrets.redacted(),
        mediated_ca: prepared.mediated_ca.clone(),
        pid: None,
        last_start_unix_seconds: None,
    }
}

#[must_use]
fn build_agent_base_state(prepared: &PreparedBaseProvisioning, files: &AgentBaseFiles) -> State {
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
        disk: path_text(&files.disk),
        work_dir: path_text(&prepared.seed.work_dir),
        seed_media: path_text(&prepared.seed.seed_media),
        seed_meta_data: path_text(&prepared.seed.meta_data),
        seed_network_config: path_text(&prepared.seed.network_config),
        seed_user_data: path_text(&prepared.seed.user_data),
        monitor_socket: path_text(&files.run_dir.join("monitor.sock")),
        qmp_socket: path_text(&files.run_dir.join("qmp.sock")),
        guest_control_socket: path_text(&files.run_dir.join("guest-control.sock")),
        pid_file: path_text(&files.run_dir.join("qemu.pid")),
        serial_log: path_text(&files.logs_dir.join("serial.log")),
        qemu_log: path_text(&files.logs_dir.join("qemu.log")),
        instance_network: None,
        mediated_secrets: SecretBindings::default(),
        mediated_ca: MediatedCaState::default(),
        pid: None,
        last_start_unix_seconds: None,
    }
}

fn agent_base_state_from_files(files: &AgentBaseFiles, pid: Option<u32>) -> State {
    State {
        image: ImageState {
            os: String::new(),
            architecture: String::new(),
            variant: String::new(),
            source_url: String::new(),
            cache_key: String::new(),
            cache_path: String::new(),
            download_path: String::new(),
            format: String::new(),
        },
        disk: path_text(&files.disk),
        work_dir: path_text(&files.seed_dir),
        seed_media: path_text(&files.seed_media),
        seed_meta_data: path_text(&files.seed_dir.join("meta-data")),
        seed_network_config: path_text(&files.seed_dir.join("network-config")),
        seed_user_data: path_text(&files.seed_dir.join("user-data")),
        monitor_socket: path_text(&files.run_dir.join("monitor.sock")),
        qmp_socket: path_text(&files.run_dir.join("qmp.sock")),
        guest_control_socket: path_text(&files.run_dir.join("guest-control.sock")),
        pid_file: path_text(&files.run_dir.join("qemu.pid")),
        serial_log: path_text(&files.logs_dir.join("serial.log")),
        qemu_log: path_text(&files.logs_dir.join("qemu.log")),
        instance_network: None,
        mediated_secrets: SecretBindings::default(),
        mediated_ca: MediatedCaState::default(),
        pid,
        last_start_unix_seconds: None,
    }
}

async fn read_qemu_pid(path: &Path) -> Option<u32> {
    let contents = tokio::fs::read_to_string(path).await.ok()?;
    contents.trim().parse().ok()
}

async fn find_agent_base_qemu_pid(files: &AgentBaseFiles) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        let disk = path_text(&files.disk);
        let mut entries = tokio::fs::read_dir("/proc").await.ok()?;
        while let Some(entry) = entries.next_entry().await.ok()? {
            let Some(pid) = entry.file_name().to_str().and_then(|name| name.parse::<u32>().ok()) else {
                continue;
            };
            let cmdline = tokio::fs::read(entry.path().join("cmdline")).await;
            if cmdline
                .as_deref()
                .is_ok_and(|cmdline| agent_base_cmdline_matches(cmdline, &disk))
            {
                return Some(pid);
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _files = files;
        None
    }
}

fn agent_base_cmdline_matches(cmdline: &[u8], disk: &str) -> bool {
    let mut args = cmdline
        .split(|byte| *byte == 0)
        .filter(|arg| !arg.is_empty())
        .filter_map(|arg| std::str::from_utf8(arg).ok());
    let Some(program) = args.next() else {
        return false;
    };
    program.contains("qemu-system") && args.any(|arg| arg.contains(&format!("file={disk}")))
}

fn spec_from_state(
    manifest: &AgentManifest,
    agent: &str,
    instance: &str,
    network: &NetworkState,
    state: &State,
) -> Result<command::CommandSpec, Error> {
    Ok(command::CommandSpec {
        name: format!("agentdp-{agent}-{instance}"),
        accelerator: command::Accelerator::local_default(),
        cpus: manifest.spec.resources.cpus,
        memory: manifest.spec.resources.memory.clone(),
        disk: PathBuf::from(&state.disk),
        seed_media: PathBuf::from(&state.seed_media),
        monitor_socket: PathBuf::from(&state.monitor_socket),
        qmp_socket: PathBuf::from(&state.qmp_socket),
        guest_control_socket: guest_control_socket_path(state),
        pid_file: PathBuf::from(&state.pid_file),
        serial_log: PathBuf::from(&state.serial_log),
        qemu_log: PathBuf::from(&state.qemu_log),
        network: command_network_backend(network, state)?,
        daemonize: !cfg!(target_os = "windows"),
    })
}

fn command_network_backend(network: &NetworkState, state: &State) -> Result<command::NetworkBackend, Error> {
    match state.instance_network.as_ref() {
        Some(instance_network) => Ok(command::NetworkBackend::Stream {
            socket: PathBuf::from(&instance_network.stream_socket),
            mac: agentdp_core::mediated_network::DEFAULT_PROFILE.guest_mac.to_string(),
        }),
        None => Ok(command::NetworkBackend::User {
            ports: user_port_forwards(network)?,
        }),
    }
}

fn guest_control_socket_path(state: &State) -> PathBuf {
    PathBuf::from(&state.guest_control_socket)
}

fn instance_network_state(qemu_runtime_dir: &Path) -> agentdp_core::agent::QemuInstanceNetworkState {
    let profile = agentdp_core::mediated_network::DEFAULT_PROFILE;
    agentdp_core::agent::QemuInstanceNetworkState {
        addresses: instance_network_addresses(profile),
        stream_socket: agentdp_qemu::net::stream_socket_path(qemu_runtime_dir)
            .display()
            .to_string(),
    }
}

pub(super) const fn instance_network_addresses(profile: MediatedNetworkProfile) -> agentdp_network::InstanceAddresses {
    agentdp_network::InstanceAddresses {
        gateway: ipv4_address(profile.gateway_ipv4),
        address: ipv4_address(profile.guest_ipv4),
        cidr_prefix: profile.ipv4_cidr_prefix,
    }
}

pub(super) const fn instance_network_mac(profile: MediatedNetworkProfile) -> agentdp_network::InstanceMacAddresses {
    agentdp_network::InstanceMacAddresses {
        gateway: agentdp_network::MacAddress::new(profile.gateway_mac.octets()),
        guest: agentdp_network::MacAddress::new(profile.guest_mac.octets()),
    }
}

const fn ipv4_address(address: std::net::Ipv4Addr) -> agentdp_network::Ipv4AddressText {
    agentdp_network::Ipv4AddressText::from_std(address)
}

fn user_port_forwards(network: &NetworkState) -> Result<BTreeMap<String, command::PortForward>, Error> {
    network
        .ports
        .iter()
        .map(|(name, port)| {
            let host = port
                .host
                .ok_or_else(|| ErrorKind::MissingUserNetworkHostPort { name: name.clone() })?;
            Ok((
                name.clone(),
                command::PortForward {
                    guest: port.guest,
                    host,
                    protocol: match port.protocol {
                        PortProtocolState::Tcp => command::PortProtocol::Tcp,
                        PortProtocolState::Udp => command::PortProtocol::Udp,
                    },
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()
}

fn disk_path(files: &AgentInstanceFiles) -> PathBuf {
    files.disk.clone()
}

struct ProcessInspection {
    stale: bool,
    report: AgentProcessStatus,
}

async fn inspect_process(instance_status: AgentInstancePhase, state: &State) -> ProcessInspection {
    let Some(pid) = state.pid else {
        if instance_status == AgentInstancePhase::Running {
            return ProcessInspection {
                stale: true,
                report: AgentProcessStatus {
                    status: "missing".to_owned(),
                    pid: None,
                    message: Some("runtime status is running but no QEMU pid is recorded".to_owned()),
                },
            };
        }
        return ProcessInspection {
            stale: false,
            report: AgentProcessStatus {
                status: "not-recorded".to_owned(),
                pid: None,
                message: None,
            },
        };
    };

    if instance_status != AgentInstancePhase::Running {
        return ProcessInspection {
            stale: !phase_allows_runtime(instance_status),
            report: AgentProcessStatus {
                status: "recorded".to_owned(),
                pid: Some(pid),
                message: Some(format!(
                    "runtime status is {instance_status} but QEMU pid is still recorded"
                )),
            },
        };
    }

    match platform::process::process_status(pid).await {
        Ok(ProcessStatus::Running) => ProcessInspection {
            stale: false,
            report: AgentProcessStatus {
                status: "running".to_owned(),
                pid: Some(pid),
                message: None,
            },
        },
        Ok(ProcessStatus::NotFound) => ProcessInspection {
            stale: true,
            report: AgentProcessStatus {
                status: "missing".to_owned(),
                pid: Some(pid),
                message: Some(format!("runtime status is running but QEMU pid {pid} is not running")),
            },
        },
        Err(error) => ProcessInspection {
            stale: false,
            report: AgentProcessStatus {
                status: "unknown".to_owned(),
                pid: Some(pid),
                message: Some(error.to_string()),
            },
        },
    }
}

fn stopped_process_observation() -> AgentProcessStatus {
    AgentProcessStatus {
        status: "not-recorded".to_owned(),
        pid: None,
        message: None,
    }
}

const fn phase_allows_runtime(phase: AgentInstancePhase) -> bool {
    matches!(
        phase,
        AgentInstancePhase::Starting
            | AgentInstancePhase::Running
            | AgentInstancePhase::Stopping
            | AgentInstancePhase::Deleting
    )
}

fn host_port_mappings(status: &agentdp_network::InstanceNetworkStatus) -> BTreeMap<String, PortMappingState> {
    status
        .host_ports
        .iter()
        .map(|port| {
            (
                port.name.clone(),
                PortMappingState {
                    guest: port.guest,
                    host: Some(port.host),
                    protocol: host_port_protocol_state(port.protocol),
                },
            )
        })
        .collect()
}

const fn host_port_protocol_state(protocol: agentdp_network::HostPortProtocol) -> PortProtocolState {
    match protocol {
        agentdp_network::HostPortProtocol::Tcp => PortProtocolState::Tcp,
        agentdp_network::HostPortProtocol::Udp => PortProtocolState::Udp,
    }
}

fn path_text(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
