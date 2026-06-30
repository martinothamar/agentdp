mod handle;
mod output;
mod runner;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::agent::{AgentInstanceNetworkEvent, AgentInstanceNetworkStatus, PortMappingState, PortProtocolState};
use agentdp_ds::{local, sync};
use agentdp_network::{
    GuestFrameTransport, HostPortProtocol, InstanceNetworkError, InstanceNetworkSpec, InstanceNetworkState,
    InstanceNetworkStatus as NetworkRuntimeStatus, NetworkCommand, NetworkExit, ProductionWake, RuntimeSecrets,
};
use tokio::task::JoinHandle as TaskJoinHandle;
use tokio::time::Instant;

pub(crate) use handle::InstanceNetworkHandle;
use runner::{CommandInbox, join_network_thread, spawn_network_thread};

pub(crate) const FRAME_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const RECONNECT_DELAY: Duration = Duration::from_secs(1);
pub(crate) const TRAFFIC_TIMEOUT: Duration = Duration::from_mins(4);
const STOP_TIMEOUT: Duration = Duration::from_secs(2);
const NETWORK_EVENT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug)]
pub(crate) struct InstanceNetwork {
    events: Rc<RefCell<local::spsc::Sender<AgentInstanceNetworkEvent>>>,
    runtime_state: RefCell<Option<InstanceNetworkRuntime>>,
}

#[derive(Debug)]
struct InstanceNetworkRuntime {
    handle: InstanceNetworkHandle,
    commands: sync::spsc::Sender<NetworkCommand>,
    wake: ProductionWake,
    thread: std::thread::JoinHandle<NetworkExit>,
    output_task: TaskJoinHandle<()>,
}

#[derive(Debug, Clone)]
pub(crate) struct InstanceNetworkObservation {
    pub status: NetworkRuntimeStatus,
    pub event_drops: u64,
}

impl InstanceNetwork {
    #[must_use]
    pub(crate) fn new(events: local::spsc::Sender<AgentInstanceNetworkEvent>) -> Self {
        Self {
            events: Rc::new(RefCell::new(events)),
            runtime_state: RefCell::new(None),
        }
    }

    pub(crate) fn observation(&self) -> Option<InstanceNetworkObservation> {
        let runtime = self.runtime_state.borrow();
        let network = runtime.as_ref()?;
        let observation = network.handle.observation();
        drop(runtime);
        Some(observation)
    }

    pub(crate) fn is_running(&self) -> bool {
        self.runtime_state
            .borrow()
            .as_ref()
            .is_some_and(InstanceNetworkRuntime::is_running)
    }

    pub(crate) fn start<T>(
        &self,
        context: &Context,
        spec: InstanceNetworkSpec,
        transport: T,
    ) -> Result<InstanceNetworkHandle, InstanceNetworkError>
    where
        T: GuestFrameTransport + Send + 'static,
    {
        let label = spec.label.clone();
        let transport_description = transport.describe();
        context.logger().verbose_with(|| {
            format!("starting instance network for {label}; connecting for guest frames on {transport_description}")
        });

        if let Some(network) = self.existing_network()? {
            return Ok(network);
        }

        let (commands, command_source) = sync::spsc::bounded(spec.config.limits.command_inbox_capacity);
        let (sink, observation_rx, output_drain) = output::network_outputs(
            NetworkRuntimeStatus::starting(&spec.config.limits),
            NETWORK_EVENT_CHANNEL_CAPACITY,
            Rc::clone(&self.events),
        );
        let (wake, thread) =
            spawn_network_thread(label.clone(), spec, transport, sink, CommandInbox::new(command_source))?;
        let output_task = tokio::task::spawn_local(output::drain_network_outputs(output_drain));

        let handle = InstanceNetworkHandle::new(label.clone(), observation_rx);
        *self.runtime_state.borrow_mut() = Some(InstanceNetworkRuntime {
            handle: handle.clone(),
            commands,
            wake,
            thread,
            output_task,
        });

        context
            .logger()
            .verbose_with(|| format!("instance network started for {label}"));
        Ok(handle)
    }

    pub(crate) async fn stop(&self) -> Result<(), InstanceNetworkError> {
        let Some(network) = self.runtime_state.borrow_mut().take() else {
            return Ok(());
        };
        network.stop().await
    }

    pub(crate) async fn cleanup(&self) -> Result<(), InstanceNetworkError> {
        self.stop().await
    }

    pub(crate) fn update_secrets(&self, secrets: RuntimeSecrets) -> Result<bool, InstanceNetworkError> {
        let mut runtime = self.runtime_state.borrow_mut();
        let Some(network) = runtime.as_mut() else {
            return Ok(false);
        };
        if !network.is_running() {
            return Ok(false);
        }
        network.update_secrets(secrets)?;
        Ok(true)
    }

    fn existing_network(&self) -> Result<Option<InstanceNetworkHandle>, InstanceNetworkError> {
        let mut runtime = self.runtime_state.borrow_mut();
        let Some(network) = runtime.as_mut() else {
            return Ok(None);
        };
        if network.is_running() {
            return Ok(Some(network.handle.clone()));
        }
        let Some(network) = runtime.take() else {
            return Ok(None);
        };
        if let InstanceNetworkState::Failed { error } = network.handle.status().state {
            return Err(InstanceNetworkError::TaskFailed {
                label: network.handle.label().to_owned(),
                message: error,
            });
        }
        Ok(None)
    }
}

impl InstanceNetworkObservation {
    pub(crate) fn status(&self) -> AgentInstanceNetworkStatus {
        let ready = self.status.state.is_ready();
        let (state, transport, generation) = match &self.status.state {
            InstanceNetworkState::Starting => ("starting", None, None),
            InstanceNetworkState::Connecting { transport } => ("connecting", Some(transport.clone()), None),
            InstanceNetworkState::Connected { generation } => ("connected", None, Some(*generation)),
            InstanceNetworkState::TrafficObserved { generation } => ("traffic-observed", None, Some(*generation)),
            InstanceNetworkState::Backoff { generation, .. } => ("backoff", None, Some(*generation)),
            InstanceNetworkState::Stopping => ("stopping", None, None),
            InstanceNetworkState::Stopped => ("stopped", None, None),
            InstanceNetworkState::Failed { .. } => ("failed", None, None),
        };
        let telemetry = &self.status.telemetry;
        AgentInstanceNetworkStatus {
            state: state.to_owned(),
            ready,
            host_ports: self
                .status
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
                .collect(),
            transport,
            generation,
            started_unix_seconds: telemetry.started_unix_seconds,
            last_state_change_unix_seconds: telemetry.last_state_change_unix_seconds,
            last_transport_connect_unix_seconds: telemetry.last_transport_connect_unix_seconds,
            last_guest_frame_unix_seconds: telemetry.last_guest_frame_unix_seconds,
            last_host_frame_unix_seconds: telemetry.last_host_frame_unix_seconds,
            guest_frames_received: telemetry.guest_frames_received,
            guest_bytes_received: telemetry.guest_bytes_received,
            host_frames_sent: telemetry.host_frames_sent,
            host_bytes_sent: telemetry.host_bytes_sent,
            session_disconnects: telemetry.session_disconnects,
            connect_errors: telemetry.connect_errors,
            egress_errors: telemetry.egress_errors,
            network_event_drops: self.event_drops,
            last_error: telemetry.error_events.latest().map(|event| event.message.clone()),
        }
    }
}

const fn host_port_protocol_state(protocol: HostPortProtocol) -> PortProtocolState {
    match protocol {
        HostPortProtocol::Tcp => PortProtocolState::Tcp,
        HostPortProtocol::Udp => PortProtocolState::Udp,
    }
}

impl InstanceNetworkRuntime {
    fn is_running(&self) -> bool {
        !self.thread.is_finished() && !self.handle.status().is_terminal()
    }

    async fn stop(mut self) -> Result<(), InstanceNetworkError> {
        self.send_stop().await;
        let result = join_network_thread(&self.handle.label, self.thread, STOP_TIMEOUT).await;
        let _joined = self.output_task.await;
        result
    }

    async fn send_stop(&mut self) {
        let deadline = Instant::now() + STOP_TIMEOUT;
        loop {
            match self.commands.try_send(NetworkCommand::Stop) {
                Ok(()) => {
                    let _woken = self.wake.wake();
                    return;
                }
                Err(sync::spsc::TrySendError::Full(_command)) => {
                    if Instant::now() >= deadline {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(sync::spsc::TrySendError::Disconnected(_command)) => return,
            }
        }
    }

    fn update_secrets(&mut self, secrets: RuntimeSecrets) -> Result<(), InstanceNetworkError> {
        self.commands
            .try_send(NetworkCommand::UpdateSecrets(secrets))
            .map_err(|error| match error {
                sync::spsc::TrySendError::Full(_) => InstanceNetworkError::TaskFailed {
                    label: self.handle.label().to_owned(),
                    message: "network command queue is full while updating mediated secrets".to_owned(),
                },
                sync::spsc::TrySendError::Disconnected(_) => InstanceNetworkError::TaskFailed {
                    label: self.handle.label().to_owned(),
                    message: "network command queue is closed while updating mediated secrets".to_owned(),
                },
            })?;
        self.wake.wake().map_err(|error| InstanceNetworkError::TaskFailed {
            label: self.handle.label().to_owned(),
            message: format!("failed to wake network after updating mediated secrets: {error}"),
        })
    }
}
