use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::rc::Rc;

use agentdp_core::Context;
use agentdp_ds::local::{oneshot, spsc};
use agentdp_protocol::client_server::BackendKind;
use tokio::task::JoinHandle;

use crate::backend;
use crate::host::tailscale::TailscaleService;

use super::{Agent, AgentError as Error, AgentManifestContext, AgentName, AgentdpLayout};

pub(crate) struct AgentRegistry {
    inner: Rc<AgentRegistryInner>,
    task: Rc<JoinHandle<()>>,
}

impl fmt::Debug for AgentRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentRegistry")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl AgentRegistry {
    pub(crate) async fn load(
        context: Context,
        layout: AgentdpLayout,
        tailscale: Rc<TailscaleService>,
    ) -> Result<Self, Error> {
        let mut state = RegistryState::default();
        let backend = backend::resolve_for_kind(BackendKind::Qemu);
        for agent in layout.deployed_agents().await? {
            state.agents.insert(
                agent.clone(),
                Agent::spawn(
                    context.clone(),
                    agent,
                    layout.clone(),
                    backend.clone(),
                    Rc::clone(&tailscale),
                ),
            );
        }
        Ok(Self::from_state(context, layout, backend, tailscale, state))
    }

    fn from_state(
        context: Context,
        layout: AgentdpLayout,
        backend: backend::BackendRef,
        tailscale: Rc<TailscaleService>,
        state: RegistryState,
    ) -> Self {
        let (command_tx, command_rx) = spsc::bounded(REGISTRY_COMMAND_CAPACITY);
        let inner = Rc::new(AgentRegistryInner {
            context,
            layout,
            backend,
            tailscale,
            commands: RefCell::new(command_tx),
        });
        let task = spawn_registry_loop(Rc::clone(&inner), state, command_rx);
        Self {
            inner,
            task: Rc::new(task),
        }
    }

    pub(crate) async fn stop(&self) {
        let (respond, receive) = oneshot::channel();
        if self.submit_command(RegistryCommand::Stop { respond }).is_ok() {
            let _result = receive.await;
        }
    }

    pub(crate) async fn get(&self, agent: &AgentName) -> Option<Agent> {
        let (respond, receive) = oneshot::channel();
        if self
            .submit_command(RegistryCommand::Get {
                agent: agent.clone(),
                respond,
            })
            .is_err()
        {
            return None;
        }
        receive.await.ok().flatten()
    }

    pub(crate) async fn list(&self, agent: Option<&AgentName>) -> Vec<Agent> {
        let (respond, receive) = oneshot::channel();
        if self
            .submit_command(RegistryCommand::List {
                agent: agent.cloned(),
                respond,
            })
            .is_err()
        {
            return Vec::new();
        }
        receive.await.unwrap_or_default()
    }

    pub(crate) async fn agent_for_manifest(
        &self,
        context: &Context,
        manifest: &Path,
    ) -> Result<(Agent, AgentManifestContext), Error> {
        let manifest = AgentManifestContext::load(context, &self.inner.layout, manifest).await?;
        let (respond, receive) = oneshot::channel();
        self.submit_command(RegistryCommand::EnsureAgent {
            manifest: Box::new(manifest.clone()),
            respond,
        })?;
        let agent = self.receive_command_response(receive).await?;
        Ok((agent, manifest))
    }

    fn submit_command(&self, command: RegistryCommand) -> Result<(), Error> {
        if self.task.is_finished() {
            return Err(Error::InstanceUnavailable {
                name: "agent registry".to_owned(),
            });
        }
        self.inner
            .commands
            .borrow_mut()
            .try_send(command)
            .map_err(|error| match error {
                spsc::TrySendError::Full(_) => Error::OperationInProgress {
                    name: "agent registry".to_owned(),
                    operation: "registry command queue",
                },
                spsc::TrySendError::Disconnected(_) => Error::InstanceUnavailable {
                    name: "agent registry".to_owned(),
                },
            })
    }

    async fn receive_command_response<T>(&self, receive: oneshot::Receiver<Result<T, Error>>) -> Result<T, Error> {
        receive.await.map_err(|_| Error::InstanceUnavailable {
            name: "agent registry".to_owned(),
        })?
    }
}

impl Drop for AgentRegistry {
    fn drop(&mut self) {
        if Rc::strong_count(&self.task) == 1 {
            self.task.abort();
        }
    }
}

const REGISTRY_COMMAND_CAPACITY: usize = 1024;

struct AgentRegistryInner {
    context: Context,
    layout: AgentdpLayout,
    backend: backend::BackendRef,
    tailscale: Rc<TailscaleService>,
    commands: RefCell<spsc::Sender<RegistryCommand>>,
}

impl fmt::Debug for AgentRegistryInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentRegistryInner")
            .field("layout", &self.layout)
            .finish_non_exhaustive()
    }
}

fn spawn_registry_loop(
    inner: Rc<AgentRegistryInner>,
    mut state: RegistryState,
    mut commands: spsc::Receiver<RegistryCommand>,
) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        loop {
            match commands.recv().await {
                Ok(RegistryCommand::Stop { respond }) => {
                    state.agents.clear();
                    respond.try_send(());
                    break;
                }
                Ok(RegistryCommand::EnsureAgent { manifest, respond }) => {
                    let result = inner.ensure_agent(&mut state, &manifest);
                    respond.try_send(result);
                }
                Ok(RegistryCommand::Get { agent, respond }) => {
                    if state.agents.get(&agent).is_some_and(Agent::is_finished) {
                        state.agents.remove(&agent);
                    }
                    let agent = state.agents.get(&agent).cloned();
                    respond.try_send(agent);
                }
                Ok(RegistryCommand::List { agent, respond }) => {
                    state.agents.retain(|_, candidate| !candidate.is_finished());
                    let agents = state
                        .agents
                        .values()
                        .filter(|candidate| agent.as_ref().is_none_or(|agent| candidate.agent() == agent))
                        .cloned()
                        .collect();
                    respond.try_send(agents);
                }
                Err(spsc::TryRecvError::Empty) => {}
                Err(spsc::TryRecvError::Disconnected) => break,
            }
        }
    })
}

impl AgentRegistryInner {
    fn ensure_agent(&self, state: &mut RegistryState, manifest: &AgentManifestContext) -> Result<Agent, Error> {
        let name = manifest.agent().clone();
        if let Some(agent) = state.agents.get(&name) {
            if agent.is_finished() {
                state.agents.remove(&name);
            } else {
                return Ok(agent.clone());
            }
        }
        let agent = Agent::spawn(
            self.context.clone(),
            name.clone(),
            self.layout.clone(),
            backend::ensure_manifest_supported(manifest.value(), self.backend.clone())?,
            Rc::clone(&self.tailscale),
        );
        state.agents.insert(name, agent.clone());
        Ok(agent)
    }
}

pub(crate) enum RegistryCommand {
    Stop {
        respond: oneshot::Sender<()>,
    },
    EnsureAgent {
        manifest: Box<AgentManifestContext>,
        respond: oneshot::Sender<Result<Agent, Error>>,
    },
    Get {
        agent: AgentName,
        respond: oneshot::Sender<Option<Agent>>,
    },
    List {
        agent: Option<AgentName>,
        respond: oneshot::Sender<Vec<Agent>>,
    },
}

#[derive(Default)]
struct RegistryState {
    agents: BTreeMap<AgentName, Agent>,
}

impl fmt::Debug for RegistryState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistryState")
            .field("agents", &self.agents.keys().collect::<Vec<_>>())
            .finish()
    }
}
