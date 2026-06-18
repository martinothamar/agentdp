use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use agentdp_core::{Context, control_plane};
use agentdp_ds::local::oneshot;
use agentdp_platform::{self as platform};
use agentdp_protocol::client_server::{
    AgentInstanceListParams, AgentInstanceListResult, AgentInstanceLogsParams, LogFile,
};
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::agent::{Agent, AgentCommand, AgentError, AgentInstanceId, AgentName, AgentRegistry, AgentdpLayout};
use crate::host::tailscale::{TailscaleServeDesired, TailscaleService};

const MAX_REQUEST_BYTES: usize = 64 * 1024;
const EVENT_STREAM_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Config(#[from] control_plane::Error),
    #[error("invalid web bind address {address}:{port}: {source}")]
    BindAddress {
        address: String,
        port: u16,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("failed to bind web control plane at {address}: {source}")]
    Bind {
        address: SocketAddr,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug)]
pub(crate) struct WebControlPlane {
    task: Option<JoinHandle<()>>,
}

impl WebControlPlane {
    pub(crate) async fn new(
        context: &Context,
        agents: Rc<AgentRegistry>,
        layout: AgentdpLayout,
        tailscale: Rc<TailscaleService>,
    ) -> Self {
        match spawn_control_plane(context, agents, layout, tailscale).await {
            Ok(task) => Self { task },
            Err(error) => {
                context
                    .logger()
                    .warn(format!("failed to start web control plane: {error}"));
                Self { task: None }
            }
        }
    }

    pub(crate) async fn stop(&mut self) {
        let task = self.task.take();
        if let Some(task) = task {
            task.abort();
            let _result = task.await;
        }
    }
}

impl Drop for WebControlPlane {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn spawn_control_plane(
    context: &Context,
    agents: Rc<AgentRegistry>,
    layout: AgentdpLayout,
    tailscale: Rc<TailscaleService>,
) -> Result<Option<JoinHandle<()>>, Error> {
    let config = control_plane::load_or_default(&layout.config_dir()).await?;
    if !config.web.enabled {
        context.logger().verbose("web control plane disabled by config");
        return Ok(None);
    }
    let address = format!("{}:{}", config.web.bind_address, config.web.port)
        .parse::<SocketAddr>()
        .map_err(|source| Error::BindAddress {
            address: config.web.bind_address.clone(),
            port: config.web.port,
            source,
        })?;
    let listener = TcpListener::bind(address)
        .await
        .map_err(|source| Error::Bind { address, source })?;
    let local_addr = listener.local_addr().unwrap_or(address);
    context
        .logger()
        .verbose_with(|| format!("web control plane listening at http://{local_addr}"));
    if config.tailscale.expose_web {
        apply_tailscale_serve(context, &tailscale, config.web.port).await;
    }
    let context = context.clone();
    let agents = Rc::clone(&agents);
    let config = Rc::new(config);
    Ok(Some(tokio::task::spawn_local(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    if let Err(error) = platform::net::configure_tcp_stream(&stream) {
                        context.logger().warn(format!(
                            "failed to configure web control-plane TCP stream from {peer}: {error}"
                        ));
                        continue;
                    }
                    let context = context.clone();
                    let agents = Rc::clone(&agents);
                    let config = Rc::clone(&config);
                    tokio::task::spawn_local(async move {
                        if let Err(error) =
                            Box::pin(handle_connection(&context, agents.as_ref(), &config, stream)).await
                        {
                            context.logger().warn(format!(
                                "failed to handle web control-plane request from {peer}: {error}"
                            ));
                        }
                    });
                }
                Err(error) => {
                    context
                        .logger()
                        .warn(format!("failed to accept web control-plane connection: {error}"));
                }
            }
        }
    })))
}

async fn apply_tailscale_serve(context: &Context, tailscale: &TailscaleService, port: u16) {
    match tailscale
        .reconcile(context, TailscaleServeDesired::ControlPlane { port })
        .await
    {
        Ok(_) => context
            .logger()
            .verbose_with(|| format!("configured Tailscale Serve root HTTPS route to http://127.0.0.1:{port}")),
        Err(error) => {
            context.logger().warn(format!(
                "failed to configure Tailscale Serve for web control plane: {error}"
            ));
        }
    }
}

async fn handle_connection(
    context: &Context,
    agents: &AgentRegistry,
    config: &control_plane::ServerConfig,
    mut stream: TcpStream,
) -> std::io::Result<()> {
    let mut buffer = vec![0; MAX_REQUEST_BYTES];
    let size = stream.read(&mut buffer).await?;
    let request = match Request::parse(&buffer[..size]) {
        Ok(request) => request,
        Err(error) => {
            let response = Response::text(400, &format!("bad request: {error}"));
            stream.write_all(&response.to_bytes()).await?;
            stream.flush().await?;
            return Ok(());
        }
    };
    if request.method == "GET" && request.path == "/api/events" {
        if let Err(response) = authorize_request(config, &request) {
            stream.write_all(&response.to_bytes()).await?;
            stream.flush().await?;
            return Ok(());
        }
        return Box::pin(stream_instance_events(agents, stream)).await;
    }
    let response = Box::pin(handle_request(context, agents, config, &request)).await;
    stream.write_all(&response.to_bytes()).await?;
    stream.flush().await
}

async fn handle_request(
    _context: &Context,
    agents: &AgentRegistry,
    config: &control_plane::ServerConfig,
    request: &Request,
) -> Response {
    if let Err(response) = authorize_request(config, request) {
        return response;
    }
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => Response::html(200, INDEX_HTML),
        ("GET", "/api/instances") => match list_instances(agents, &AgentInstanceListParams { agent: None }).await {
            Ok(result) => Response::json(200, &result),
            Err(error) => Response::text(500, &error.to_string()),
        },
        ("GET", "/api/events") => Response::text(405, "event stream requires a streaming connection"),
        ("GET", "/api/routes") => Response::json(200, &RouteRegistry::default()),
        _ => Box::pin(handle_instance_route(agents, request)).await,
    }
}

async fn stream_instance_events(agents: &AgentRegistry, mut stream: TcpStream) -> std::io::Result<()> {
    stream.write_all(event_stream_headers().as_bytes()).await?;
    stream.flush().await?;
    send_instance_event(agents, &mut stream).await?;
    loop {
        tokio::time::sleep(EVENT_STREAM_INTERVAL).await;
        send_instance_event(agents, &mut stream).await?;
    }
}

async fn send_instance_event(agents: &AgentRegistry, stream: &mut TcpStream) -> std::io::Result<()> {
    let event = match list_instances(agents, &AgentInstanceListParams { agent: None }).await {
        Ok(result) => sse_event("instances", &result),
        Err(error) => sse_event("error", &error.to_string()),
    }
    .unwrap_or_else(|error| format!("event: error\ndata: \"failed to serialize event: {error}\"\n\n").into_bytes());
    stream.write_all(&event).await?;
    stream.flush().await
}

fn authorize_request(config: &control_plane::ServerConfig, request: &Request) -> Result<(), Response> {
    if let Some(origin) = request.headers.get("origin")
        && !config.web.allowed_origins.iter().any(|allowed| allowed == origin)
    {
        return Err(Response::text(403, "origin is not allowed"));
    }
    let Some(token) = &config.web.auth_token else {
        return Ok(());
    };
    if request.path == "/" {
        return Ok(());
    }
    let expected = format!("Bearer {token}");
    match request.headers.get("authorization") {
        Some(value) if value == &expected => Ok(()),
        _ => Err(Response::text(401, "missing or invalid authorization")),
    }
}

async fn handle_instance_route(agents: &AgentRegistry, request: &Request) -> Response {
    let Some(route) = InstanceRoute::parse(&request.path) else {
        return Response::text(404, "not found");
    };
    match (request.method.as_str(), route.action.as_str()) {
        ("GET", "status") => match route_status(agents, &route).await {
            Ok(result) => Response::json(200, &result),
            Err(error) => Response::text(404, &error.to_string()),
        },
        ("GET", "logs") => {
            let Ok(instance_id) = AgentInstanceId::parse(&route.instance) else {
                return Response::text(404, "invalid instance id");
            };
            let params = AgentInstanceLogsParams {
                agent: route.agent.clone(),
                instance_id: instance_id.as_u32(),
                file: LogFile::Serial,
                lines: 200,
            };
            match route_logs(agents, &route, &params).await {
                Ok(result) => Response::json(200, &result),
                Err(error) => Response::text(500, &error.to_string()),
            }
        }
        _ => Response::text(404, "not found"),
    }
}

async fn list_instances(
    agents: &AgentRegistry,
    params: &AgentInstanceListParams,
) -> Result<AgentInstanceListResult, AgentError> {
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

async fn route_status(
    agents: &AgentRegistry,
    route: &InstanceRoute,
) -> Result<agentdp_core::agent::AgentInstanceDocument, AgentError> {
    let (agent, instance) = route_agent(agents, route).await?;
    let (respond, receive) = oneshot::channel();
    agent.send(AgentCommand::InstanceStatus { instance, respond })?;
    receive_agent_error(agent.agent(), receive).await
}

async fn route_logs(
    agents: &AgentRegistry,
    route: &InstanceRoute,
    params: &AgentInstanceLogsParams,
) -> Result<agentdp_protocol::client_server::AgentInstanceLogsResult, AgentError> {
    let (agent, instance) = route_agent(agents, route).await?;
    let (respond, receive) = oneshot::channel();
    agent.send(AgentCommand::InstanceLogs {
        instance,
        params: params.clone(),
        respond,
    })?;
    receive_agent_error(agent.agent(), receive).await
}

async fn receive_agent_error<T>(
    agent: &AgentName,
    receive: oneshot::Receiver<Result<T, AgentError>>,
) -> Result<T, AgentError> {
    receive.await.map_err(|_| AgentError::InstanceUnavailable {
        name: agent.to_string(),
    })?
}

async fn route_agent(agents: &AgentRegistry, route: &InstanceRoute) -> Result<(Agent, AgentInstanceId), AgentError> {
    let agent = AgentName::parse(&route.agent)?;
    let instance = AgentInstanceId::parse(&route.instance)?;
    let agent = agents.get(&agent).await.ok_or_else(|| AgentError::InstanceNotFound {
        name: format!("{agent}/{instance}"),
    })?;
    Ok((agent, instance))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
}

impl Request {
    fn parse(bytes: &[u8]) -> Result<Self, &'static str> {
        let text = std::str::from_utf8(bytes).map_err(|_| "request is not UTF-8")?;
        let line = text.lines().next().ok_or("request line is missing")?;
        let mut parts = line.split_whitespace();
        let method = parts.next().ok_or("method is missing")?;
        let target = parts.next().ok_or("target is missing")?;
        let _version = parts.next().ok_or("version is missing")?;
        let headers = text
            .lines()
            .skip(1)
            .take_while(|line| !line.is_empty())
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            })
            .collect();
        Ok(Self {
            method: method.to_owned(),
            path: target.split('?').next().unwrap_or(target).to_owned(),
            headers,
        })
    }
}

struct Response {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl Response {
    fn html(status: u16, body: &str) -> Self {
        Self {
            status,
            content_type: "text/html; charset=utf-8",
            body: body.as_bytes().to_vec(),
        }
    }

    fn text(status: u16, body: &str) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.as_bytes().to_vec(),
        }
    }

    fn json(status: u16, value: &impl serde::Serialize) -> Self {
        match serde_json::to_vec_pretty(value) {
            Ok(body) => Self {
                status,
                content_type: "application/json",
                body,
            },
            Err(error) => Self::text(500, &format!("failed to serialize response: {error}")),
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let reason = match self.status {
            401 => "Unauthorized",
            403 => "Forbidden",
            400 => "Bad Request",
            405 => "Method Not Allowed",
            404 => "Not Found",
            500 => "Internal Server Error",
            _ => "OK",
        };
        let mut response = format!(
            "HTTP/1.1 {} {reason}\r\ncontent-type: {}\r\ncontent-length: {}\r\ncache-control: no-store\r\nx-content-type-options: nosniff\r\n\r\n",
            self.status,
            self.content_type,
            self.body.len()
        )
        .into_bytes();
        response.extend_from_slice(&self.body);
        response
    }
}

const fn event_stream_headers() -> &'static str {
    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-store\r\nconnection: keep-alive\r\nx-content-type-options: nosniff\r\n\r\n"
}

fn sse_event(event: &str, value: &impl serde::Serialize) -> Result<Vec<u8>, serde_json::Error> {
    let data = serde_json::to_string(value)?;
    Ok(format!("event: {event}\ndata: {data}\n\n").into_bytes())
}

struct InstanceRoute {
    agent: String,
    instance: String,
    action: String,
}

impl InstanceRoute {
    fn parse(path: &str) -> Option<Self> {
        let parts = path.strip_prefix("/api/instances/")?.split('/').collect::<Vec<_>>();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return None;
        }
        Some(Self {
            agent: parts[0].to_owned(),
            instance: parts[1].to_owned(),
            action: parts[2].to_owned(),
        })
    }
}

#[derive(Debug, Default, serde::Serialize)]
struct RouteRegistry {
    routes: BTreeMap<String, String>,
}

const INDEX_HTML: &str = include_str!("web/index.html");

#[cfg(test)]
mod tests {
    use agentdp_core::control_plane::ServerConfig;

    use super::{InstanceRoute, Request, authorize_request, event_stream_headers, sse_event};

    #[test]
    fn request_parser_reads_method_and_path() {
        let request = Request::parse(b"GET /api/instances HTTP/1.1\r\nhost: localhost\r\n\r\n").unwrap();

        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/api/instances");
        assert_eq!(request.headers.get("host").map(String::as_str), Some("localhost"));
    }

    #[test]
    fn instance_route_parses_agent_instance_and_action() {
        let route = InstanceRoute::parse("/api/instances/altinn-studio/0/status").unwrap();

        assert_eq!(route.agent, "altinn-studio");
        assert_eq!(route.instance, "0");
        assert_eq!(route.action, "status");
    }

    #[test]
    fn authorization_rejects_disallowed_origins() {
        let config = ServerConfig::default();
        let request = Request::parse(b"GET /api/instances HTTP/1.1\r\norigin: http://evil.test\r\n\r\n").unwrap();

        let response = authorize_request(&config, &request).unwrap_err();

        assert_eq!(response.status, 403);
    }

    #[test]
    fn authorization_accepts_configured_bearer_token() {
        let mut config = ServerConfig::default();
        config.web.auth_token = Some("secret".to_owned());
        let request = Request::parse(b"GET /api/instances HTTP/1.1\r\nauthorization: Bearer secret\r\n\r\n").unwrap();

        assert!(authorize_request(&config, &request).is_ok());
    }

    #[test]
    fn event_stream_headers_keep_connection_open() {
        let headers = event_stream_headers();

        assert!(headers.contains("content-type: text/event-stream"));
        assert!(headers.contains("connection: keep-alive"));
        assert!(!headers.contains("content-length"));
    }

    #[test]
    fn sse_event_renders_named_json_payload() {
        let event = sse_event("instances", &["dev-0"]).unwrap();

        assert_eq!(
            String::from_utf8(event).unwrap(),
            "event: instances\ndata: [\"dev-0\"]\n\n"
        );
    }
}
