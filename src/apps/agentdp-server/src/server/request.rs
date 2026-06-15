use agentdp_core::Context;
use agentdp_core::agent::{AgentDocument, AgentWaitResult, AgentWaitStatusResult};
use agentdp_ds::local::{oneshot, spsc};
use agentdp_protocol::client_server::{
    AgentApplyParams, AgentInstanceExecParams, AgentInstanceListParams, AgentInstanceListResult,
    AgentInstanceLogsParams, AgentInstanceSelector, AgentScaleParams, AgentSelector, AgentWaitParams, AgentWatchParams,
    DoctorCheckResult, EventLevel, PingResult, Request, RequestKind, Response, ServerDoctorResult, ShutdownResult,
};
use thiserror::Error;

use crate::agent::{
    Agent, AgentCommand, AgentContextError, AgentError, AgentInstanceId, AgentInstanceSessionOutput, AgentName,
    AgentRegistry, AgentStreamItem, IdentityError, wait_condition_result, wait_status,
};

use super::{ConnectionAction, ConnectionEvents};

const REQUEST_SESSION_OUTPUT_CAPACITY: usize = 1024;
const REQUEST_AGENT_STREAM_CAPACITY: usize = 1024;
const WAIT_STREAM_HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug, Error)]
enum RequestError {
    #[error("{0}")]
    Agent(#[from] AgentError),
    #[error("{0}")]
    Context(#[from] AgentContextError),
    #[error("{0}")]
    Identity(#[from] IdentityError),
}

pub(super) async fn handle(
    context: &Context,
    agents: &AgentRegistry,
    request: &Request,
    mut events: ConnectionEvents,
) -> (Response, ConnectionAction) {
    match &request.kind {
        RequestKind::ServerPing => (ping_response(request), ConnectionAction::Continue),
        RequestKind::ServerShutdown => (shutdown_response(request), ConnectionAction::Shutdown),
        RequestKind::AgentApply(params) => (
            agent_apply_response(context, agents, request, params).await,
            ConnectionAction::Continue,
        ),
        RequestKind::AgentScale(params) => (
            agent_scale_response(agents, request, params).await,
            ConnectionAction::Continue,
        ),
        RequestKind::AgentDelete(params) => (
            agent_delete_response(agents, request, params).await,
            ConnectionAction::Continue,
        ),
        RequestKind::AgentStatus(params) => (
            agent_status_response(agents, request, params).await,
            ConnectionAction::Continue,
        ),
        RequestKind::AgentWait(params) => (
            agent_wait_response(agents, request, params, &mut events).await,
            ConnectionAction::Continue,
        ),
        RequestKind::AgentWatch(params) => (
            agent_watch_response(agents, request, params, &mut events).await,
            ConnectionAction::Continue,
        ),
        RequestKind::AgentInstanceStatus(params) => (
            instance_status_response(agents, request, params).await,
            ConnectionAction::Continue,
        ),
        RequestKind::AgentInstanceExec(params) => (
            instance_exec_response(context, agents, request, params, &mut events).await,
            ConnectionAction::Continue,
        ),
        RequestKind::ServerDoctor(params) => (
            server_doctor_response(context, agents, request, params).await,
            ConnectionAction::Continue,
        ),
        RequestKind::AgentInstanceLogs(params) => (
            instance_logs_response(agents, request, params).await,
            ConnectionAction::Continue,
        ),
        RequestKind::AgentInstanceList(params) => (
            instance_ps_response(context, agents, request, params).await,
            ConnectionAction::Continue,
        ),
        RequestKind::AgentInstanceShell(params) => (
            instance_shell_response(context, agents, request, params).await,
            ConnectionAction::Continue,
        ),
    }
}

pub(super) const fn continues_after_disconnect(kind: &RequestKind) -> bool {
    matches!(
        kind,
        RequestKind::AgentApply(_) | RequestKind::AgentScale(_) | RequestKind::AgentDelete(_)
    )
}

async fn instance_exec_response(
    context: &Context,
    agents: &AgentRegistry,
    request: &Request,
    params: &AgentInstanceExecParams,
    events: &mut ConnectionEvents,
) -> Response {
    let result = async {
        let (agent, instance) = agent_instance(
            agents,
            &AgentInstanceSelector {
                agent: params.agent.clone(),
                instance_id: params.instance_id,
            },
        )
        .await?;
        let (respond, receive) = oneshot::channel();
        let (output_tx, mut output_rx) = spsc::bounded(REQUEST_SESSION_OUTPUT_CAPACITY);
        agent.send(AgentCommand::InstanceExec {
            context: context.clone(),
            instance,
            params: params.clone(),
            output: output_tx,
            respond,
        })?;
        receive_session_result(agent.agent(), receive, &mut output_rx, events)
            .await
            .map_err(RequestError::from)
    }
    .await;
    match result {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure("instance_exec_failed", error.to_string()),
    }
}

async fn instance_ps_response(
    context: &Context,
    agents: &AgentRegistry,
    request: &Request,
    params: &AgentInstanceListParams,
) -> Response {
    match instance_ps(context, agents, params).await {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure("agent_instance_list_failed", error.to_string()),
    }
}

async fn instance_shell_response(
    context: &Context,
    agents: &AgentRegistry,
    request: &Request,
    params: &AgentInstanceSelector,
) -> Response {
    let result = async {
        let (agent, instance) = agent_instance(agents, params).await?;
        let (respond, receive) = oneshot::channel();
        agent.send(AgentCommand::InstanceShell {
            context: context.clone(),
            instance,
            respond,
        })?;
        receive_agent_response(agent.agent(), receive)
            .await
            .map_err(RequestError::from)
    }
    .await;
    match result {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure("instance_shell_failed", error.to_string()),
    }
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

async fn server_doctor_response(
    context: &Context,
    _agents: &AgentRegistry,
    request: &Request,
    params: &agentdp_protocol::client_server::ServerDoctorParams,
) -> Response {
    let mut report = agentdp_core::doctor::DoctorReport::new();
    crate::backend::resolve_for_kind(params.backend)
        .check_prerequisites(context, &mut report)
        .await;
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

async fn agent_apply_response(
    context: &Context,
    agents: &AgentRegistry,
    request: &Request,
    params: &AgentApplyParams,
) -> Response {
    let result = async {
        let (agent, manifest) = agents.agent_for_manifest(context, &params.manifest).await?;
        let (respond, receive) = oneshot::channel();
        agent.send(AgentCommand::Apply {
            manifest: Box::new(manifest),
            respond,
        })?;
        receive_agent_response(agent.agent(), receive)
            .await
            .map_err(RequestError::from)
    }
    .await;
    match result {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure("agent_apply_failed", error.to_string()),
    }
}

async fn agent_scale_response(agents: &AgentRegistry, request: &Request, params: &AgentScaleParams) -> Response {
    let result = async {
        let agent = AgentName::parse(&params.agent)?;
        let agent = get_agent(agents, &agent).await?;
        let (respond, receive) = oneshot::channel();
        agent.send(AgentCommand::Scale {
            replicas: params.replicas,
            respond,
        })?;
        receive_agent_response(agent.agent(), receive)
            .await
            .map_err(RequestError::from)
    }
    .await;
    match result {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure("agent_scale_failed", error.to_string()),
    }
}

async fn agent_delete_response(agents: &AgentRegistry, request: &Request, params: &AgentSelector) -> Response {
    let result = async {
        let agent = AgentName::parse(&params.agent)?;
        let agent = get_agent(agents, &agent).await?;
        let (respond, receive) = oneshot::channel();
        agent.send(AgentCommand::Delete { respond })?;
        receive_agent_response(agent.agent(), receive)
            .await
            .map_err(RequestError::from)
    }
    .await;
    match result {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure("agent_delete_failed", error.to_string()),
    }
}

async fn agent_status_response(agents: &AgentRegistry, request: &Request, params: &AgentSelector) -> Response {
    let result: Result<AgentDocument, RequestError> = async {
        let agent = AgentName::parse(&params.agent)?;
        let agent = get_agent(agents, &agent).await?;
        let (respond, receive) = oneshot::channel();
        agent.send(AgentCommand::Document { respond })?;
        receive_agent_response(agent.agent(), receive)
            .await
            .map_err(RequestError::from)
    }
    .await;
    match result {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure("agent_status_failed", error.to_string()),
    }
}

async fn agent_wait_response(
    agents: &AgentRegistry,
    request: &Request,
    params: &AgentWaitParams,
    events: &mut ConnectionEvents,
) -> Response {
    let result = async {
        let agent = AgentName::parse(&params.agent)?;
        let agent = get_agent(agents, &agent).await?;
        wait_on_agent(&agent, params, events).await.map_err(RequestError::from)
    }
    .await;
    match result {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure("agent_wait_failed", error.to_string()),
    }
}

async fn agent_watch_response(
    agents: &AgentRegistry,
    request: &Request,
    params: &AgentWatchParams,
    events: &mut ConnectionEvents,
) -> Response {
    let result: Result<(), RequestError> = async {
        let agent = AgentName::parse(&params.agent)?;
        let agent = get_agent(agents, &agent).await?;
        let mut stream = open_agent_stream(&agent, None).await?;
        loop {
            emit_agent_stream_item(events, recv_agent_stream_item(&agent, &mut stream).await?);
        }
    }
    .await;
    match result {
        Ok(()) => std::future::pending().await,
        Err(error) => request.respond_with_failure("agent_watch_failed", error.to_string()),
    }
}

async fn instance_status_response(
    agents: &AgentRegistry,
    request: &Request,
    params: &AgentInstanceSelector,
) -> Response {
    let result = async {
        let (agent, instance) = agent_instance(agents, params).await?;
        let (respond, receive) = oneshot::channel();
        agent.send(AgentCommand::InstanceStatus { instance, respond })?;
        receive_agent_response(agent.agent(), receive)
            .await
            .map_err(RequestError::from)
    }
    .await;
    match result {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure("instance_status_failed", error.to_string()),
    }
}

async fn instance_logs_response(
    agents: &AgentRegistry,
    request: &Request,
    params: &AgentInstanceLogsParams,
) -> Response {
    let result = async {
        let (agent, instance) = agent_instance(
            agents,
            &AgentInstanceSelector {
                agent: params.agent.clone(),
                instance_id: params.instance_id,
            },
        )
        .await?;
        let (respond, receive) = oneshot::channel();
        agent.send(AgentCommand::InstanceLogs {
            instance,
            params: params.clone(),
            respond,
        })?;
        receive_agent_response(agent.agent(), receive)
            .await
            .map_err(RequestError::from)
    }
    .await;
    match result {
        Ok(result) => request.respond_with_success(result),
        Err(error) => request.respond_with_failure("instance_logs_failed", error.to_string()),
    }
}

async fn agent_instance(
    agents: &AgentRegistry,
    params: &AgentInstanceSelector,
) -> Result<(Agent, AgentInstanceId), RequestError> {
    let agent = AgentName::parse(&params.agent)?;
    let instance = AgentInstanceId::new(params.instance_id);
    let agent = agents.get(&agent).await.ok_or_else(|| AgentError::InstanceNotFound {
        name: format!("{agent}/{instance}"),
    })?;
    Ok((agent, instance))
}

async fn get_agent(agents: &AgentRegistry, agent: &AgentName) -> Result<Agent, RequestError> {
    agents.get(agent).await.ok_or_else(|| {
        AgentError::InstanceNotFound {
            name: agent.to_string(),
        }
        .into()
    })
}

async fn receive_agent_response<T>(
    agent: &AgentName,
    receive: oneshot::Receiver<Result<T, AgentError>>,
) -> Result<T, AgentError> {
    receive.await.map_err(|_| AgentError::InstanceUnavailable {
        name: agent.to_string(),
    })?
}

async fn receive_session_result<T>(
    agent: &AgentName,
    receive: oneshot::Receiver<Result<T, AgentError>>,
    output: &mut spsc::Receiver<AgentInstanceSessionOutput>,
    events: &mut ConnectionEvents,
) -> Result<T, AgentError> {
    let mut output_open = true;
    tokio::pin!(receive);
    loop {
        tokio::select! {
            result = &mut receive => {
                drain_session_output(output, events);
                return result.map_err(|_| AgentError::InstanceUnavailable {
                    name: agent.to_string(),
                })?;
            }
            event = output.recv(), if output_open => {
                match event {
                    Ok(event) => emit_session_output(events, event),
                    Err(spsc::TryRecvError::Disconnected) => {
                        output_open = false;
                    }
                    Err(spsc::TryRecvError::Empty) => {}
                }
            }
        }
    }
}

fn drain_session_output(receiver: &mut spsc::Receiver<AgentInstanceSessionOutput>, events: &mut ConnectionEvents) {
    receiver.drain(|event| emit_session_output(events, event));
}

fn emit_session_output(events: &mut ConnectionEvents, event: AgentInstanceSessionOutput) {
    match event {
        AgentInstanceSessionOutput::Stdout(chunk) => events.stdout(chunk),
        AgentInstanceSessionOutput::Stderr(chunk) => events.stderr(chunk),
    }
}

async fn instance_ps(
    _context: &Context,
    agents: &AgentRegistry,
    params: &AgentInstanceListParams,
) -> Result<AgentInstanceListResult, RequestError> {
    let agent = params.agent.as_deref().map(AgentName::parse).transpose()?;
    let agents = agents.list(agent.as_ref()).await;
    let mut instances = Vec::new();
    for agent in agents {
        let (respond, receive) = oneshot::channel();
        if agent.send(AgentCommand::ListItems { respond }).is_ok() {
            instances.extend(receive.await.unwrap_or_default());
        }
    }
    instances.sort_by(|left, right| {
        left.agent
            .cmp(&right.agent)
            .then_with(|| left.instance_id.cmp(&right.instance_id))
    });
    Ok(AgentInstanceListResult { instances })
}

async fn wait_on_agent(
    agent: &Agent,
    params: &AgentWaitParams,
    events: &mut ConnectionEvents,
) -> Result<AgentWaitResult, AgentError> {
    let started = tokio::time::Instant::now();
    let (respond, receive) = oneshot::channel();
    agent.send(AgentCommand::Document { respond })?;
    let current_document = receive_agent_response(agent.agent(), receive).await?;
    let current_status = wait_status(&current_document, params.generation, params.condition);
    if current_status != AgentWaitStatusResult::Pending {
        return Ok(wait_result(params, current_status, current_document));
    }
    let mut stream = match open_agent_stream(agent, Some(params.generation)).await {
        Ok(stream) => stream,
        Err(error) => return wait_on_finished_agent_or_error(agent, params, error, Some(current_document)),
    };
    let mut last_document = Some(current_document);
    let heartbeat = tokio::time::sleep(WAIT_STREAM_HEARTBEAT_INTERVAL);
    tokio::pin!(heartbeat);
    loop {
        let item = if let Some(timeout) = params.timeout_seconds {
            let remaining = std::time::Duration::from_secs(timeout).saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return wait_timeout_result(agent, params, last_document);
            }
            tokio::select! {
                item = recv_agent_stream_item(agent, &mut stream) => {
                    match item {
                        Ok(item) => item,
                        Err(error) => return wait_on_finished_agent_or_error(
                            agent,
                            params,
                            error,
                            last_document,
                        ),
                    }
                },
                () = tokio::time::sleep(remaining) => {
                    return wait_timeout_result(agent, params, last_document);
                },
                () = &mut heartbeat => {
                    events.diagnostic(EventLevel::Verbose, wait_heartbeat_message(params));
                    heartbeat.as_mut().reset(tokio::time::Instant::now() + WAIT_STREAM_HEARTBEAT_INTERVAL);
                    continue;
                }
            }
        } else {
            tokio::select! {
                item = recv_agent_stream_item(agent, &mut stream) => {
                    match item {
                        Ok(item) => item,
                        Err(error) => return wait_on_finished_agent_or_error(
                            agent,
                            params,
                            error,
                            last_document,
                        ),
                    }
                },
                () = &mut heartbeat => {
                    events.diagnostic(EventLevel::Verbose, wait_heartbeat_message(params));
                    heartbeat.as_mut().reset(tokio::time::Instant::now() + WAIT_STREAM_HEARTBEAT_INTERVAL);
                    continue;
                }
            }
        };
        let Some(document) = emit_agent_stream_item(events, item) else {
            continue;
        };
        let mut status = wait_status(&document, params.generation, params.condition);
        if status == AgentWaitStatusResult::Pending
            && params
                .timeout_seconds
                .is_some_and(|timeout| started.elapsed() >= std::time::Duration::from_secs(timeout))
        {
            status = AgentWaitStatusResult::Timeout;
        }
        if status != AgentWaitStatusResult::Pending {
            return Ok(wait_result(params, status, document));
        }
        last_document = Some(document);
    }
}

fn wait_heartbeat_message(params: &AgentWaitParams) -> String {
    format!(
        "waiting for {} generation {} to become {:?}",
        params.agent, params.generation, params.condition
    )
}

fn wait_on_finished_agent_or_error(
    agent: &Agent,
    params: &AgentWaitParams,
    error: AgentError,
    last_document: Option<AgentDocument>,
) -> Result<AgentWaitResult, AgentError> {
    if agent.is_finished()
        && let Some(document) = last_document
    {
        let status = wait_status(&document, params.generation, params.condition);
        if status != AgentWaitStatusResult::Pending {
            return Ok(wait_result(params, status, document));
        }
    }
    Err(error)
}

fn wait_timeout_result(
    agent: &Agent,
    params: &AgentWaitParams,
    last_document: Option<AgentDocument>,
) -> Result<AgentWaitResult, AgentError> {
    let Some(document) = last_document else {
        return Err(AgentError::InstanceUnavailable {
            name: agent.agent().to_string(),
        });
    };
    Ok(wait_result(params, AgentWaitStatusResult::Timeout, document))
}

fn wait_result(params: &AgentWaitParams, status: AgentWaitStatusResult, document: AgentDocument) -> AgentWaitResult {
    AgentWaitResult {
        agent: document.agent().clone(),
        generation: params.generation,
        condition: wait_condition_result(params.condition),
        status,
        document,
    }
}

async fn open_agent_stream(
    agent: &Agent,
    replay_from_generation: Option<u64>,
) -> Result<spsc::Receiver<AgentStreamItem>, AgentError> {
    let (items, receiver) = spsc::bounded(REQUEST_AGENT_STREAM_CAPACITY);
    let (respond, receive) = oneshot::channel();
    agent.send(AgentCommand::OpenStream {
        replay_from_generation,
        items,
        respond,
    })?;
    receive_agent_response(agent.agent(), receive).await?;
    Ok(receiver)
}

async fn recv_agent_stream_item(
    agent: &Agent,
    stream: &mut spsc::Receiver<AgentStreamItem>,
) -> Result<AgentStreamItem, AgentError> {
    stream.recv().await.map_err(|_| AgentError::InstanceUnavailable {
        name: agent.agent().to_string(),
    })
}

fn emit_agent_stream_item(events: &mut ConnectionEvents, item: AgentStreamItem) -> Option<AgentDocument> {
    match item {
        AgentStreamItem::Document(document) => {
            events.agent_document(&document);
            Some(*document)
        }
        AgentStreamItem::Event(event) => {
            events.agent_event(&event);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use agentdp_protocol::client_server::{
        self as protocol, AgentApplyParams, AgentScaleParams, AgentSelector, Request, RequestKind,
    };

    use super::{continues_after_disconnect, ping_response, shutdown_response};

    #[test]
    fn ping_request_returns_success() {
        let response = ping_response(&Request::new("cmd_1", RequestKind::ServerPing));
        assert!(response.is_ok());
        assert_eq!(response.id(), "cmd_1");
    }

    #[test]
    fn invalid_request_returns_stable_error() {
        let mut line = r#"{"id":"cmd_1","method":"unknown.method"}"#.to_owned();
        line.push('\n');
        let error = protocol::decode_request(&line).expect_err("unknown request method should be invalid");
        let response = protocol::invalid_request(error.to_string());
        let error = response.error().expect("error body");
        assert!(response.is_error());
        assert_eq!(error.code, "invalid_request");
    }

    #[test]
    fn shutdown_request_returns_success() {
        let response = shutdown_response(&Request::new("cmd_1", RequestKind::ServerShutdown));
        assert!(response.is_ok());
    }

    #[test]
    fn desired_state_requests_continue_after_client_disconnect() {
        assert!(continues_after_disconnect(&RequestKind::AgentApply(AgentApplyParams {
            manifest: "/tmp/agent.yaml".into(),
        })));
        assert!(continues_after_disconnect(&RequestKind::AgentScale(AgentScaleParams {
            agent: "altinn-studio".to_owned(),
            replicas: 0,
        })));
        assert!(continues_after_disconnect(&RequestKind::AgentDelete(AgentSelector {
            agent: "altinn-studio".to_owned(),
        })));
        assert!(!continues_after_disconnect(&RequestKind::AgentInstanceStatus(
            agentdp_protocol::client_server::AgentInstanceSelector {
                agent: "altinn-studio".to_owned(),
                instance_id: 0,
            },
        )));
    }
}
