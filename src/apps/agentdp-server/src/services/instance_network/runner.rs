use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::Duration;

use agentdp_ds::sync::spsc;
use agentdp_network::{
    EventLoop, GuestFrameTransport, InstanceNetworkError, InstanceNetworkSpec, NetworkCommand, NetworkCommandSource,
    NetworkEventSink, NetworkExit, ProductionWake,
};
use tokio::time::Instant;

pub(super) struct CommandInbox {
    receiver: spsc::Receiver<NetworkCommand>,
}

impl CommandInbox {
    pub(super) const fn new(receiver: spsc::Receiver<NetworkCommand>) -> Self {
        Self { receiver }
    }
}

impl NetworkCommandSource for CommandInbox {
    fn try_recv(&mut self) -> Option<NetworkCommand> {
        match self.receiver.try_recv() {
            Ok(command) => Some(command),
            Err(spsc::TryRecvError::Empty) => None,
            Err(spsc::TryRecvError::Disconnected) => Some(NetworkCommand::Stop),
        }
    }
}

pub(super) fn spawn_network_thread<T, O, C>(
    label: String,
    spec: InstanceNetworkSpec,
    transport: T,
    outputs: O,
    commands: C,
) -> Result<(ProductionWake, ThreadJoinHandle<NetworkExit>), InstanceNetworkError>
where
    T: GuestFrameTransport + Send + 'static,
    O: NetworkEventSink + Send + 'static,
    C: NetworkCommandSource,
{
    let thread_name = format!("agentdp-network-{label}");
    let (started_tx, started_rx) = startup_channel();
    let thread = std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let event_loop = match EventLoop::new(spec, transport, outputs, commands) {
                Ok(event_loop) => event_loop,
                Err(error) => {
                    started_tx.send(Err(error.clone()));
                    return NetworkExit::Failed(error);
                }
            };
            let wake = event_loop.wake_handle();
            started_tx.send(Ok(wake));
            event_loop.run()
        })
        .map_err(|error| InstanceNetworkError::TaskFailed {
            label: label.clone(),
            message: format!("failed to spawn instance network thread: {error}"),
        })?;
    match started_rx.recv() {
        Some(Ok(wake)) => Ok((wake, thread)),
        Some(Err(error)) => {
            let _joined = thread.join();
            Err(error)
        }
        None => Err(join_startup_failure(label, thread)),
    }
}

struct StartupSender {
    shared: Arc<StartupShared>,
    sent: bool,
}

struct StartupReceiver {
    shared: Arc<StartupShared>,
}

struct StartupShared {
    state: Mutex<StartupState>,
    ready: Condvar,
}

enum StartupState {
    Pending,
    Ready(Option<Result<ProductionWake, InstanceNetworkError>>),
    Closed,
}

fn startup_channel() -> (StartupSender, StartupReceiver) {
    let shared = Arc::new(StartupShared {
        state: Mutex::new(StartupState::Pending),
        ready: Condvar::new(),
    });
    (
        StartupSender {
            shared: Arc::clone(&shared),
            sent: false,
        },
        StartupReceiver { shared },
    )
}

impl StartupSender {
    fn send(mut self, result: Result<ProductionWake, InstanceNetworkError>) {
        let mut state = lock_startup_state(&self.shared.state);
        if matches!(*state, StartupState::Pending) {
            *state = StartupState::Ready(Some(result));
        }
        self.sent = true;
        drop(state);
        self.shared.ready.notify_one();
    }
}

impl Drop for StartupSender {
    fn drop(&mut self) {
        if self.sent {
            return;
        }
        let mut state = lock_startup_state(&self.shared.state);
        if matches!(*state, StartupState::Pending) {
            *state = StartupState::Closed;
            drop(state);
            self.shared.ready.notify_one();
        }
    }
}

impl StartupReceiver {
    fn recv(self) -> Option<Result<ProductionWake, InstanceNetworkError>> {
        let mut state = lock_startup_state(&self.shared.state);
        loop {
            match &mut *state {
                StartupState::Pending => state = wait_startup_state(&self.shared, state),
                StartupState::Ready(result) => return result.take(),
                StartupState::Closed => return None,
            }
        }
    }
}

fn lock_startup_state(mutex: &Mutex<StartupState>) -> MutexGuard<'_, StartupState> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn wait_startup_state<'a>(
    shared: &'a StartupShared,
    state: MutexGuard<'a, StartupState>,
) -> MutexGuard<'a, StartupState> {
    shared
        .ready
        .wait(state)
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn join_startup_failure(label: String, thread: ThreadJoinHandle<NetworkExit>) -> InstanceNetworkError {
    match thread.join() {
        Ok(NetworkExit::Failed(error)) => error,
        Ok(NetworkExit::Stopped) => InstanceNetworkError::TaskFailed {
            label,
            message: "instance network thread stopped during startup".to_owned(),
        },
        Err(_panic) => InstanceNetworkError::TaskFailed {
            label,
            message: "instance network thread panicked during startup".to_owned(),
        },
    }
}

pub(super) async fn join_network_thread(
    label: &str,
    thread: ThreadJoinHandle<NetworkExit>,
    timeout: Duration,
) -> Result<(), InstanceNetworkError> {
    let deadline = Instant::now() + timeout;
    loop {
        if thread.is_finished() {
            return match thread.join() {
                Ok(NetworkExit::Stopped) => Ok(()),
                Ok(NetworkExit::Failed(error)) => Err(error),
                Err(_panic) => Err(InstanceNetworkError::TaskFailed {
                    label: label.to_owned(),
                    message: "instance network thread panicked".to_owned(),
                }),
            };
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(InstanceNetworkError::StopTimeout {
                label: label.to_owned(),
                timeout,
            });
        }
        tokio::time::sleep((deadline - now).min(Duration::from_millis(10))).await;
    }
}
