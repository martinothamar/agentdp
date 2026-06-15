use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use agentdp_core::{
    Context,
    agent::{
        AgentBasePhase, AgentDocument, AgentEvent, AgentEventEnvelope, AgentEventSource,
        AgentInstanceBootstrapStepStatus, AgentInstanceBootstrapWorkPhase, AgentInstanceBootstrapWorkStatus,
        AgentInstanceEvent, AgentInstanceId, AgentInstancePhase, AgentInstanceStatus, AgentStatusPhase,
        AgentWaitResult, BootstrapEvent,
    },
    layout::AgentdpLayout,
    manifest::LoadedAgentManifest,
};
use agentdp_protocol::client_server::{
    AgentSelector, AgentWaitCondition, AgentWaitParams, Event, EventKind, EventLevel, RequestKind,
};
use agentdp_protocol::server_guest::{BootstrapLifecycleStatus, BootstrapStepStatus};
use clap::{Args, ValueEnum};

use crate::server_client;

#[derive(Debug, Args)]
pub(crate) struct Command {
    #[arg(short, long, value_name = "PATH")]
    file: Option<PathBuf>,

    #[arg(long, value_name = "GENERATION")]
    generation: Option<u64>,

    #[arg(long = "for", value_enum, default_value_t = Condition::Ready)]
    condition: Condition,

    #[arg(long, value_name = "SECONDS")]
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Condition {
    Accepted,
    Observed,
    Ready,
    Paused,
    Stopped,
    Deleted,
}

impl From<Condition> for AgentWaitCondition {
    fn from(value: Condition) -> Self {
        match value {
            Condition::Accepted => Self::Accepted,
            Condition::Observed => Self::Observed,
            Condition::Ready => Self::Ready,
            Condition::Paused => Self::Paused,
            Condition::Stopped => Self::Stopped,
            Condition::Deleted => Self::Deleted,
        }
    }
}

pub(crate) async fn run(command: &Command, context: &Context) -> ExitCode {
    match try_run(command, context).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn try_run(command: &Command, context: &Context) -> Result<(), Error> {
    let manifest = LoadedAgentManifest::load_from_current_dir(context, command.file.as_deref()).await?;
    let layout = AgentdpLayout::resolve().map_err(Error::AgentdpLayout)?;
    let generation = match command.generation {
        Some(generation) => generation,
        None => current_generation(context, &layout, manifest.agent_name()).await?,
    };
    let mut progress = WaitProgress::default();
    let mut on_event = |event| progress.print_event(event);
    let result = wait_for(
        context,
        &layout,
        manifest.agent_name().to_owned(),
        generation,
        command.condition.into(),
        command.timeout_seconds,
        Some(&mut on_event),
    )
    .await
    .map_err(Error::Server)?;
    print_result(&result);
    Ok(())
}

async fn current_generation(context: &Context, layout: &AgentdpLayout, agent: &str) -> Result<u64, Error> {
    let result: AgentDocument = server_client::request(
        context,
        layout,
        RequestKind::AgentStatus(AgentSelector {
            agent: agent.to_owned(),
        }),
        None,
    )
    .await
    .map_err(Error::Server)?;
    Ok(result.generation())
}

pub(crate) async fn wait_for(
    context: &Context,
    layout: &AgentdpLayout,
    agent: String,
    generation: u64,
    condition: AgentWaitCondition,
    timeout_seconds: Option<u64>,
    on_event: Option<&mut (dyn FnMut(Event) + Send)>,
) -> Result<AgentWaitResult, server_client::Error> {
    server_client::request(
        context,
        layout,
        RequestKind::AgentWait(AgentWaitParams {
            agent,
            generation,
            condition,
            timeout_seconds,
        }),
        on_event,
    )
    .await
}

pub(crate) const fn mutation_condition(result: &AgentDocument) -> AgentWaitCondition {
    match result.status.phase {
        AgentStatusPhase::Running if result.status.replicas.desired > 0 => AgentWaitCondition::Ready,
        AgentStatusPhase::Running | AgentStatusPhase::Paused => AgentWaitCondition::Stopped,
        AgentStatusPhase::Deleting | AgentStatusPhase::Deleted => AgentWaitCondition::Deleted,
    }
}

#[derive(Default)]
pub(crate) struct WaitProgress {
    base_phase: Option<AgentBasePhase>,
    instance_phases: BTreeMap<AgentInstanceId, AgentInstancePhase>,
    bootstrap_steps: BTreeMap<AgentInstanceId, String>,
    ready_instances: BTreeMap<AgentInstanceId, bool>,
}

impl WaitProgress {
    pub(crate) fn print_event(&mut self, event: Event) {
        match event.event {
            EventKind::Diagnostic { level, message } if level != EventLevel::Verbose => {
                println!("{}", progress_message(level, &message));
            }
            EventKind::AgentDocumentChanged { document } => {
                if let Ok(document) = serde_json::from_value::<AgentDocument>(document) {
                    self.print_document(&document);
                }
            }
            EventKind::AgentEvent { item } => {
                if let Ok(event) = serde_json::from_value::<AgentEventEnvelope>(item) {
                    self.print_agent_event(event);
                }
            }
            EventKind::Diagnostic { .. } | EventKind::SessionOutput { .. } => {}
        }
    }

    fn print_agent_event(&mut self, envelope: AgentEventEnvelope) {
        let source = envelope.source;
        match envelope.event {
            AgentEvent::AgentBaseStarted { .. } => self.print_base_phase(AgentBasePhase::Building),
            AgentEvent::AgentBaseReady { .. } => self.print_base_phase(AgentBasePhase::Ready),
            AgentEvent::AgentBaseFailed { error, .. } => {
                self.base_phase = Some(AgentBasePhase::Failed);
                println!("base: failed: {error}");
            }
            AgentEvent::Diagnostic { level, message } if level != agentdp_core::agent::EventLevel::Verbose => {
                println!("{}", progress_message(event_level(level), &message));
            }
            AgentEvent::DesiredStateAccepted { generation } => {
                println!("accepted desired generation {generation}");
            }
            AgentEvent::ScaleAccepted { generation, replicas } => {
                println!("accepted scale to {replicas} replicas for generation {generation}");
            }
            AgentEvent::DeleteAccepted { generation } => {
                println!("accepted delete for generation {generation}");
            }
            AgentEvent::InstanceDeleted { instance_id } => {
                self.print_instance_phase(instance_id, AgentInstancePhase::Deleted);
            }
            AgentEvent::InstanceCreated { instance_id } => {
                self.print_instance_phase(instance_id, AgentInstancePhase::Materialized);
            }
            AgentEvent::BootstrapEvent { event } => self.print_bootstrap_event(&source, event),
            AgentEvent::InstanceEvent { event } => {
                let AgentEventSource::Instance { id } = source else {
                    return;
                };
                self.print_instance_event(id, event.event);
            }
            AgentEvent::DocumentChanged { .. } | AgentEvent::Diagnostic { .. } => {}
        }
    }

    fn print_document(&mut self, document: &AgentDocument) {
        self.print_base_phase(document.status.agent_base.phase);
        for (instance_id, status) in &document.status.instances {
            self.print_instance_phase(*instance_id, status.phase);
            self.print_transition_work(*instance_id, status.work.transition.as_ref());
            self.print_bootstrap_work(*instance_id, status.work.bootstrap.as_ref());
            self.print_ready(*instance_id, status);
        }
    }

    fn print_base_phase(&mut self, phase: AgentBasePhase) {
        if self.base_phase == Some(phase) || phase == AgentBasePhase::Missing {
            return;
        }
        self.base_phase = Some(phase);
        println!("base: {}", base_phase_label(phase));
    }

    fn print_instance_phase(&mut self, instance_id: AgentInstanceId, phase: AgentInstancePhase) {
        if self.instance_phases.insert(instance_id, phase) == Some(phase) {
            return;
        }
        println!("{instance_id}: {}", instance_phase_label(phase));
    }

    fn print_transition_work(
        &mut self,
        instance_id: AgentInstanceId,
        transition: Option<&agentdp_core::agent::AgentInstanceTransitionWorkStatus>,
    ) {
        let Some(transition) = transition else {
            return;
        };
        if let Some(phase) = transition_started_phase(transition.kind) {
            self.print_instance_phase(instance_id, phase);
        } else if let Some(message) = transition_started_message(transition.kind) {
            println!("{instance_id}: {message}");
        }
    }

    fn print_instance_event(&mut self, instance_id: AgentInstanceId, event: AgentInstanceEvent) {
        match event {
            AgentInstanceEvent::BootstrapFinished { result } => {
                self.bootstrap_steps.remove(&instance_id);
                if let agentdp_core::agent::OperationResult::Failed { error } = result {
                    println!("{instance_id}: bootstrap failed: {error}");
                }
            }
            AgentInstanceEvent::BootstrapStarted
            | AgentInstanceEvent::SpecApplied { .. }
            | AgentInstanceEvent::SessionStarted { .. }
            | AgentInstanceEvent::SessionOutput { .. }
            | AgentInstanceEvent::SessionFinished { .. }
            | AgentInstanceEvent::NetworkEvent(_)
            | AgentInstanceEvent::DocumentChanged { .. } => {}
            AgentInstanceEvent::TransitionStarted { transition } => {
                if let Some(phase) = transition_started_phase(transition) {
                    self.print_instance_phase(instance_id, phase);
                } else if let Some(message) = transition_started_message(transition) {
                    println!("{instance_id}: {message}");
                }
            }
            AgentInstanceEvent::TransitionFinished { transition, result } => {
                if let Some(phase) = transition_finished_phase(transition, &result) {
                    self.print_instance_phase(instance_id, phase);
                } else if let Some(message) = transition_finished_message(transition, result) {
                    println!("{instance_id}: {message}");
                }
            }
            AgentInstanceEvent::Diagnostic { level, message } => {
                if level != agentdp_core::agent::EventLevel::Verbose {
                    println!("{instance_id}: {}", progress_message(event_level(level), &message));
                }
            }
        }
    }

    fn print_bootstrap_event(&mut self, source: &AgentEventSource, event: BootstrapEvent) {
        match source {
            AgentEventSource::AgentBase => match event {
                BootstrapEvent::Diagnostic { level, message } => {
                    if level != agentdp_core::agent::EventLevel::Verbose {
                        println!("base: {}", progress_message(event_level(level), &message));
                    }
                }
                BootstrapEvent::StepStarted { step } => {
                    println!("base: bootstrap {}", bootstrap_step_message(&step));
                }
                BootstrapEvent::StepFinished {
                    step,
                    status,
                    duration_ms,
                    ..
                } => {
                    println!(
                        "base: bootstrap {step} {} in {duration_ms}ms",
                        bootstrap_step_status_label(status)
                    );
                }
                BootstrapEvent::StepFailed {
                    step,
                    duration_ms,
                    message,
                    ..
                } => {
                    println!("base: bootstrap {step} failed in {duration_ms}ms: {message}");
                }
            },
            AgentEventSource::Instance { id } => match event {
                BootstrapEvent::Diagnostic { level, message } => {
                    if level != agentdp_core::agent::EventLevel::Verbose {
                        println!("{id}: {}", progress_message(event_level(level), &message));
                    }
                }
                BootstrapEvent::StepStarted { step } => self.print_bootstrap_step(*id, Some(&step)),
                BootstrapEvent::StepFinished {
                    step,
                    status,
                    duration_ms,
                    ..
                } => {
                    self.bootstrap_steps.remove(id);
                    println!(
                        "{id}: bootstrap {step} {} in {duration_ms}ms",
                        bootstrap_step_status_label(status)
                    );
                }
                BootstrapEvent::StepFailed {
                    step,
                    duration_ms,
                    message,
                    ..
                } => {
                    self.bootstrap_steps.remove(id);
                    println!("{id}: bootstrap {step} failed in {duration_ms}ms: {message}");
                }
            },
            AgentEventSource::Controller => {}
        }
    }

    fn print_bootstrap_step(&mut self, instance_id: AgentInstanceId, step: Option<&AgentInstanceBootstrapStepStatus>) {
        let Some(step) = step else {
            self.bootstrap_steps.remove(&instance_id);
            return;
        };
        self.print_bootstrap_message(instance_id, bootstrap_step_message(step));
    }

    fn print_bootstrap_work(
        &mut self,
        instance_id: AgentInstanceId,
        bootstrap: Option<&AgentInstanceBootstrapWorkStatus>,
    ) {
        let Some(bootstrap) = bootstrap else {
            self.bootstrap_steps.remove(&instance_id);
            return;
        };
        let message = match &bootstrap.current_step {
            Some(step) => bootstrap_step_message(step),
            None if bootstrap.phase == AgentInstanceBootstrapWorkPhase::Running => return,
            None => bootstrap_phase_message(bootstrap),
        };
        self.print_bootstrap_message(instance_id, message);
    }

    fn print_bootstrap_message(&mut self, instance_id: AgentInstanceId, message: String) {
        if self.bootstrap_steps.get(&instance_id) == Some(&message) {
            return;
        }
        println!("{instance_id}: bootstrap {message}");
        self.bootstrap_steps.insert(instance_id, message);
    }

    fn print_ready(&mut self, instance_id: AgentInstanceId, status: &AgentInstanceStatus) {
        let ready = status.readiness.as_ref().is_some_and(|readiness| readiness.ready);
        if self.ready_instances.insert(instance_id, ready) == Some(ready) || !ready {
            return;
        }
        println!("{instance_id}: ready{}", ready_services(status));
    }
}

pub(crate) fn progress_message(level: EventLevel, message: &str) -> String {
    match level {
        EventLevel::Info | EventLevel::Verbose => message.to_owned(),
        EventLevel::Warn => format!("warning: {message}"),
        EventLevel::Error => format!("error: {message}"),
    }
}

const fn event_level(level: agentdp_core::agent::EventLevel) -> EventLevel {
    match level {
        agentdp_core::agent::EventLevel::Info => EventLevel::Info,
        agentdp_core::agent::EventLevel::Warn => EventLevel::Warn,
        agentdp_core::agent::EventLevel::Error => EventLevel::Error,
        agentdp_core::agent::EventLevel::Verbose => EventLevel::Verbose,
    }
}

const fn base_phase_label(phase: AgentBasePhase) -> &'static str {
    match phase {
        AgentBasePhase::Missing => "missing",
        AgentBasePhase::Building => "building agent base",
        AgentBasePhase::Ready => "agent base ready",
        AgentBasePhase::Failed => "failed",
    }
}

const fn instance_phase_label(phase: AgentInstancePhase) -> &'static str {
    match phase {
        AgentInstancePhase::Materialized => "instance materialized",
        AgentInstancePhase::Starting => "starting runtime",
        AgentInstancePhase::Running => "runtime running",
        AgentInstancePhase::Stopping => "stopping runtime",
        AgentInstancePhase::Stopped => "runtime stopped",
        AgentInstancePhase::Deleting => "deleting runtime",
        AgentInstancePhase::Deleted => "deleted",
        AgentInstancePhase::Failed => "failed",
    }
}

const fn transition_started_message(
    transition: agentdp_core::agent::AgentInstanceTransitionKind,
) -> Option<&'static str> {
    match transition {
        agentdp_core::agent::AgentInstanceTransitionKind::Materialize => Some("materializing runtime"),
        agentdp_core::agent::AgentInstanceTransitionKind::Start => Some("starting runtime"),
        agentdp_core::agent::AgentInstanceTransitionKind::Stop => Some("stopping runtime"),
        agentdp_core::agent::AgentInstanceTransitionKind::Delete => Some("deleting runtime"),
        agentdp_core::agent::AgentInstanceTransitionKind::Reconcile => None,
    }
}

const fn transition_started_phase(
    transition: agentdp_core::agent::AgentInstanceTransitionKind,
) -> Option<AgentInstancePhase> {
    match transition {
        agentdp_core::agent::AgentInstanceTransitionKind::Materialize => Some(AgentInstancePhase::Materialized),
        agentdp_core::agent::AgentInstanceTransitionKind::Start => Some(AgentInstancePhase::Starting),
        agentdp_core::agent::AgentInstanceTransitionKind::Stop => Some(AgentInstancePhase::Stopping),
        agentdp_core::agent::AgentInstanceTransitionKind::Delete => Some(AgentInstancePhase::Deleting),
        agentdp_core::agent::AgentInstanceTransitionKind::Reconcile => None,
    }
}

const fn transition_finished_phase(
    transition: agentdp_core::agent::AgentInstanceTransitionKind,
    result: &agentdp_core::agent::OperationResult,
) -> Option<AgentInstancePhase> {
    if !matches!(result, agentdp_core::agent::OperationResult::Succeeded) {
        return None;
    }
    match transition {
        agentdp_core::agent::AgentInstanceTransitionKind::Materialize => Some(AgentInstancePhase::Materialized),
        agentdp_core::agent::AgentInstanceTransitionKind::Start => Some(AgentInstancePhase::Running),
        agentdp_core::agent::AgentInstanceTransitionKind::Stop => Some(AgentInstancePhase::Stopped),
        agentdp_core::agent::AgentInstanceTransitionKind::Delete => Some(AgentInstancePhase::Deleted),
        agentdp_core::agent::AgentInstanceTransitionKind::Reconcile => None,
    }
}

fn transition_finished_message(
    transition: agentdp_core::agent::AgentInstanceTransitionKind,
    result: agentdp_core::agent::OperationResult,
) -> Option<String> {
    let success = matches!(result, agentdp_core::agent::OperationResult::Succeeded);
    let message = match (transition, success) {
        (agentdp_core::agent::AgentInstanceTransitionKind::Materialize, true) => "runtime materialized".to_owned(),
        (agentdp_core::agent::AgentInstanceTransitionKind::Start, true) => "runtime running".to_owned(),
        (agentdp_core::agent::AgentInstanceTransitionKind::Stop, true) => "runtime stopped".to_owned(),
        (agentdp_core::agent::AgentInstanceTransitionKind::Delete, true) => "runtime deleted".to_owned(),
        (agentdp_core::agent::AgentInstanceTransitionKind::Reconcile, true) => return None,
        (_, false) => {
            let agentdp_core::agent::OperationResult::Failed { error } = result else {
                return None;
            };
            format!("{} failed: {error}", transition.as_str())
        }
    };
    Some(message)
}

fn bootstrap_phase_message(bootstrap: &AgentInstanceBootstrapWorkStatus) -> String {
    match bootstrap.phase {
        AgentInstanceBootstrapWorkPhase::Running => "running".to_owned(),
        AgentInstanceBootstrapWorkPhase::BackingOff => "backoff before retry".to_owned(),
        AgentInstanceBootstrapWorkPhase::Failed => bootstrap
            .last_error
            .as_ref()
            .map_or_else(|| "failed".to_owned(), |error| format!("failed: {error}")),
    }
}

fn bootstrap_step_message(step: &AgentInstanceBootstrapStepStatus) -> String {
    let mut message = step.step.clone();
    if let Some(status) = &step.status {
        message.push(' ');
        message.push_str(bootstrap_lifecycle_label(*status));
    }
    message
}

const fn bootstrap_lifecycle_label(status: BootstrapLifecycleStatus) -> &'static str {
    match status {
        BootstrapLifecycleStatus::Pending => "pending",
        BootstrapLifecycleStatus::Running => "running",
        BootstrapLifecycleStatus::Passed => "passed",
        BootstrapLifecycleStatus::Failed => "failed",
    }
}

const fn bootstrap_step_status_label(status: BootstrapStepStatus) -> &'static str {
    match status {
        BootstrapStepStatus::Passed => "completed",
        BootstrapStepStatus::Failed => "failed",
    }
}

fn ready_services(status: &AgentInstanceStatus) -> String {
    let Some(readiness) = &status.readiness else {
        return String::new();
    };
    if readiness.result.services.is_empty() {
        return String::new();
    }
    readiness
        .result
        .services
        .iter()
        .map(|(name, service)| format!("{name}={}", service.host_port))
        .fold(String::new(), |mut output, service| {
            output.push(' ');
            output.push_str(&service);
            output
        })
}

pub(crate) fn print_result(result: &AgentWaitResult) {
    println!("wait {}", result.agent);
    println!("generation: {}", result.generation);
    println!("condition: {:?}", result.condition);
    println!("status: {:?}", result.status);
    println!("observed generation: {}", result.document.observed_generation());
    println!(
        "ready replicas: {}/{}",
        result.document.status.replicas.ready, result.document.status.replicas.desired
    );
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("{0}")]
    AgentManifest(#[from] agentdp_core::manifest::Error),
    #[error("{0}")]
    AgentdpLayout(agentdp_core::layout::Error),
    #[error("{0}")]
    Server(server_client::Error),
}
