use std::collections::BTreeSet;
use std::path::Path;

use agentdp_core::Context;
use agentdp_core::agent::{
    AgentInstanceDocument, NetworkState, PortProtocolState, TailscaleServeRouteState, TailscaleServeState,
};
use agentdp_core::control_plane;
use agentdp_core::manifest::AgentManifest;
use agentdp_core::manifest::plugins::tailscale_serve::TailscaleServe;
use agentdp_platform as platform;
use serde_json::Value;
use thiserror::Error;

const SERVICE_TOKEN: &str = concat!("{", "service", "}");
const INSTANCE_TOKEN: &str = concat!("{", "instance", "}");
const AGENT_TOKEN: &str = concat!("{", "agent", "}");
const DEFAULT_HOST_TEMPLATE: &str = concat!("{", "service", "}-", "{", "instance", "}-", "{", "agent", "}");
const DEFAULT_PATH_TEMPLATE: &str = concat!(
    "/agents/",
    "{",
    "agent",
    "}",
    "/instances/",
    "{",
    "instance",
    "}",
    "/services/",
    "{",
    "service",
    "}"
);

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    ControlPlaneConfig(#[from] control_plane::Error),
    #[error("plugins.tailscale_serve requires Tailscale to be enabled in {}", path.display())]
    Disabled { path: std::path::PathBuf },
    #[error("plugins.tailscale_serve requires the tailscale CLI on PATH")]
    MissingCli,
    #[error("failed to run tailscale {args}: {source}")]
    CommandIo {
        args: String,
        #[source]
        source: std::io::Error,
    },
    #[error("tailscale {args} failed with status {status}: {stderr}")]
    CommandFailed {
        args: String,
        status: String,
        stderr: String,
    },
    #[error("plugins.tailscale_serve requires tailscale to be authenticated")]
    NotAuthenticated,
    #[error("plugins.tailscale_serve requires Tailscale HTTPS/MagicDNS readiness")]
    HttpsUnavailable,
    #[error("route service {service} is not an assigned instance port")]
    UnknownService { service: String },
    #[error("route for service {service} requires tcp; got {protocol}")]
    UnsupportedProtocol { service: String, protocol: String },
    #[error("route service {service} has no bound host port")]
    MissingHostPort { service: String },
    #[error("generated Tailscale host `{host}` is not DNS-safe")]
    InvalidHost { host: String },
    #[error("duplicate Tailscale route host `{host}`")]
    DuplicateHost { host: String },
    #[error("duplicate Tailscale route path `{path}`")]
    DuplicatePath { path: String },
    #[error("duplicate Tailscale HTTPS port `{port}`")]
    DuplicatePort { port: u16 },
}

pub(crate) async fn apply(
    context: &Context,
    config_dir: &Path,
    manifest: &AgentManifest,
    state: &mut AgentInstanceDocument,
) -> Result<(), Error> {
    let Some(plugin) = &manifest.spec.plugins.tailscale_serve else {
        return remove(context, state).await;
    };
    let config = control_plane::load_or_default(config_dir).await?;
    let detection = detect().await?;
    ensure_ready(config_dir, &config, &detection)?;
    let root_host = config
        .tailscale
        .root_host
        .clone()
        .or(detection.dns_name)
        .ok_or(Error::HttpsUnavailable)?;
    remove(context, state).await?;
    let routes = resolved_routes(plugin, manifest, state, &root_host)?;
    for route in &routes {
        match route.mode.as_str() {
            "direct" => serve_port(context, route.https_port.unwrap_or(443), &route.target).await?,
            "proxy" => serve_path(context, &route.path, &local_web_target(&config)).await?,
            _ => {}
        }
    }
    state.status.tailscale_serve = Some(TailscaleServeState { routes });
    Ok(())
}

pub(crate) async fn remove(context: &Context, state: &mut AgentInstanceDocument) -> Result<(), Error> {
    let Some(existing) = state.status.tailscale_serve.take() else {
        return Ok(());
    };
    for route in existing.routes {
        match route.mode.as_str() {
            "direct" => {
                if let Some(port) = route.https_port {
                    remove_port(context, port).await?;
                } else {
                    remove_path(context, &route.path).await?;
                }
            }
            "proxy" => remove_path(context, &route.path).await?,
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn needs_reconcile(manifest: &AgentManifest, state: &AgentInstanceDocument) -> bool {
    let desired = manifest.spec.plugins.tailscale_serve.as_ref();
    match (desired, state.status.tailscale_serve.as_ref()) {
        (None, None) => false,
        (None, Some(_)) | (Some(_), None) => true,
        (Some(plugin), Some(existing)) => routes_changed(plugin, state, existing),
    }
}

fn routes_changed(plugin: &TailscaleServe, state: &AgentInstanceDocument, existing: &TailscaleServeState) -> bool {
    if plugin.routes.len() != existing.routes.len() {
        return true;
    }
    plugin.routes.iter().any(|route| {
        let Some(port) = state.status.network.ports.get(&route.service) else {
            return true;
        };
        let Some(host_port) = port.host else {
            return true;
        };
        let mode = route.mode.to_string();
        let expected_target = format!("http://127.0.0.1:{host_port}");
        let expected_host = match route.mode {
            agentdp_core::manifest::plugins::tailscale_serve::RouteMode::Direct => None,
            agentdp_core::manifest::plugins::tailscale_serve::RouteMode::Proxy => Some(render_host(
                route.host_template.as_deref(),
                &route.service,
                state.metadata.name.as_str(),
                state.metadata.agent.as_str(),
            )),
        };
        let expected_path = match route.mode {
            agentdp_core::manifest::plugins::tailscale_serve::RouteMode::Direct => "/".to_owned(),
            agentdp_core::manifest::plugins::tailscale_serve::RouteMode::Proxy => render_path(
                route.path.as_deref(),
                &route.service,
                state.metadata.name.as_str(),
                state.metadata.agent.as_str(),
            ),
        };
        !existing.routes.iter().any(|existing| {
            existing.service == route.service
                && existing.mode == mode
                && existing.path == expected_path
                && existing.target == expected_target
                && match route.mode {
                    agentdp_core::manifest::plugins::tailscale_serve::RouteMode::Direct => {
                        existing.https_port == Some(host_port)
                    }
                    agentdp_core::manifest::plugins::tailscale_serve::RouteMode::Proxy => {
                        existing.https_port.is_none() && expected_host.as_deref() == Some(existing.host.as_str())
                    }
                }
        })
    })
}

fn ensure_ready(config_dir: &Path, config: &control_plane::ServerConfig, detection: &Detection) -> Result<(), Error> {
    let path = control_plane::config_path(config_dir);
    if !config.tailscale.enabled {
        return Err(Error::Disabled { path });
    }
    if !detection.installed {
        return Err(Error::MissingCli);
    }
    if !detection.authenticated {
        return Err(Error::NotAuthenticated);
    }
    if !config.tailscale.https_available && config.tailscale.root_host.is_none() && detection.dns_name.is_none() {
        return Err(Error::HttpsUnavailable);
    }
    Ok(())
}

fn resolved_routes(
    plugin: &TailscaleServe,
    manifest: &AgentManifest,
    state: &AgentInstanceDocument,
    root_host: &str,
) -> Result<Vec<TailscaleServeRouteState>, Error> {
    resolved_routes_for_network(
        plugin,
        manifest.name(),
        state.metadata.name.as_str(),
        &state.status.network,
        root_host,
    )
}

fn resolved_routes_for_network(
    plugin: &TailscaleServe,
    agent: &str,
    instance: &str,
    network: &NetworkState,
    root_host: &str,
) -> Result<Vec<TailscaleServeRouteState>, Error> {
    let mut paths = BTreeSet::new();
    let mut hosts = BTreeSet::new();
    let mut ports = BTreeSet::new();
    let mut routes = Vec::new();
    for route in &plugin.routes {
        let port = network.ports.get(&route.service).ok_or_else(|| Error::UnknownService {
            service: route.service.clone(),
        })?;
        if port.protocol != PortProtocolState::Tcp {
            return Err(Error::UnsupportedProtocol {
                service: route.service.clone(),
                protocol: port_protocol_name(port.protocol).to_owned(),
            });
        }
        let host_port = port.host.ok_or_else(|| Error::MissingHostPort {
            service: route.service.clone(),
        })?;
        let (host, https_port, path, url) = match route.mode {
            agentdp_core::manifest::plugins::tailscale_serve::RouteMode::Direct => {
                if !ports.insert(host_port) {
                    return Err(Error::DuplicatePort { port: host_port });
                }
                (
                    root_host.to_owned(),
                    Some(host_port),
                    "/".to_owned(),
                    format!("https://{root_host}:{host_port}"),
                )
            }
            agentdp_core::manifest::plugins::tailscale_serve::RouteMode::Proxy => {
                let host = render_host(route.host_template.as_deref(), &route.service, instance, agent);
                validate_host(&host)?;
                if !hosts.insert(host.clone()) {
                    return Err(Error::DuplicateHost { host });
                }
                let path = render_path(route.path.as_deref(), &route.service, instance, agent);
                if !paths.insert(path.clone()) {
                    return Err(Error::DuplicatePath { path });
                }
                (host, None, path.clone(), format!("https://{root_host}{path}"))
            }
        };
        let target = format!("http://127.0.0.1:{host_port}");
        routes.push(TailscaleServeRouteState {
            service: route.service.clone(),
            mode: route.mode.to_string(),
            host,
            https_port,
            path,
            url,
            target,
            status: "applied".to_owned(),
        });
    }
    Ok(routes)
}

fn render_host(template: Option<&str>, service: &str, instance: &str, agent: &str) -> String {
    template
        .unwrap_or(DEFAULT_HOST_TEMPLATE)
        .replace(SERVICE_TOKEN, &dns_fragment(service))
        .replace(INSTANCE_TOKEN, &dns_fragment(instance))
        .replace(AGENT_TOKEN, &dns_fragment(agent))
}

fn render_path(template: Option<&str>, service: &str, instance: &str, agent: &str) -> String {
    template
        .map_or_else(|| DEFAULT_PATH_TEMPLATE.to_owned(), ToOwned::to_owned)
        .replace(SERVICE_TOKEN, &dns_fragment(service))
        .replace(INSTANCE_TOKEN, &dns_fragment(instance))
        .replace(AGENT_TOKEN, &dns_fragment(agent))
}

fn validate_host(host: &str) -> Result<(), Error> {
    let valid = !host.is_empty()
        && !host.starts_with('-')
        && !host.ends_with('-')
        && host
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidHost { host: host.to_owned() })
    }
}

fn dns_fragment(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' => char::from(byte.to_ascii_lowercase()),
            b'a'..=b'z' | b'0'..=b'9' | b'-' => char::from(byte),
            _ => '-',
        })
        .collect()
}

fn local_web_target(config: &control_plane::ServerConfig) -> String {
    format!("http://127.0.0.1:{}", config.web.port)
}

const fn port_protocol_name(protocol: PortProtocolState) -> &'static str {
    match protocol {
        PortProtocolState::Tcp => "tcp",
        PortProtocolState::Udp => "udp",
    }
}

async fn serve_path(context: &Context, path: &str, target: &str) -> Result<(), Error> {
    run_tailscale(
        context,
        &[
            "serve",
            "--bg",
            "--yes",
            "--https=443",
            &format!("--set-path={path}"),
            target,
        ],
    )
    .await
}

async fn serve_port(context: &Context, port: u16, target: &str) -> Result<(), Error> {
    run_tailscale(context, &["serve", "--bg", "--yes", &format!("--https={port}"), target]).await
}

async fn remove_path(context: &Context, path: &str) -> Result<(), Error> {
    run_tailscale(context, &["serve", "--https=443", &format!("--set-path={path}"), "off"]).await
}

async fn remove_port(context: &Context, port: u16) -> Result<(), Error> {
    run_tailscale(context, &["serve", &format!("--https={port}"), "off"]).await
}

async fn detect() -> Result<Detection, Error> {
    let Ok(version) = tailscale_command(&["version"]).await else {
        return Ok(Detection::default());
    };
    if !version.status.success() {
        return Ok(Detection::default());
    }
    let status = run_tailscale_output(&["status", "--json"]).await?;
    Ok(parse_status(&status.stdout))
}

async fn run_tailscale(context: &Context, args: &[&str]) -> Result<(), Error> {
    let output = run_tailscale_output(args).await?;
    if output.status.success() {
        context
            .logger()
            .verbose_with(|| format!("tailscale {} completed", args.join(" ")));
        Ok(())
    } else {
        Err(Error::CommandFailed {
            args: args.join(" "),
            status: output.status.to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        })
    }
}

async fn run_tailscale_output(args: &[&str]) -> Result<std::process::Output, Error> {
    tailscale_command(args).await.map_err(|source| Error::CommandIo {
        args: args.join(" "),
        source,
    })
}

async fn tailscale_command(args: &[&str]) -> std::io::Result<std::process::Output> {
    let mut command = tokio::process::Command::new(tailscale_binary());
    command.args(args);
    platform::command::hide_child_window(&mut command).output().await
}

fn tailscale_binary() -> String {
    std::env::var("AGENTDP_TAILSCALE_PATH").unwrap_or_else(|_| "tailscale".to_owned())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Detection {
    installed: bool,
    authenticated: bool,
    dns_name: Option<String>,
}

fn parse_status(stdout: &[u8]) -> Detection {
    let Ok(value) = serde_json::from_slice::<Value>(stdout) else {
        return Detection {
            installed: true,
            ..Detection::default()
        };
    };
    let self_status = value.get("Self");
    let dns_name = self_status
        .and_then(|status| status.get("DNSName"))
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .map(|name| name.trim_end_matches('.').to_owned());
    Detection {
        installed: true,
        authenticated: self_status
            .and_then(|status| status.get("Online"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || self_status.and_then(|status| status.get("ID")).is_some(),
        dns_name,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use agentdp_core::agent::{
        NetworkAllowState, NetworkIpv6State, NetworkModeState, NetworkState, PortMappingState, PortProtocolState,
    };
    use agentdp_core::manifest::plugins::tailscale_serve::{Route, RouteMode, TailscaleServe};

    use super::{dns_fragment, parse_status, render_host, resolved_routes_for_network, validate_host};

    #[test]
    fn parses_tailscale_status_dns_name() {
        let detection = parse_status(br#"{"Self":{"Online":true,"DNSName":"dev.tail.ts.net."}}"#);

        assert!(detection.installed);
        assert!(detection.authenticated);
        assert_eq!(detection.dns_name.as_deref(), Some("dev.tail.ts.net"));
    }

    #[test]
    fn renders_default_dns_safe_host() {
        assert_eq!(
            render_host(None, "code_server", "pr_0", "altinn.studio"),
            "code-server-pr-0-altinn-studio"
        );
    }

    #[test]
    fn rejects_invalid_dns_hosts() {
        assert!(validate_host("-bad").is_err());
        assert!(validate_host("bad_underscore").is_err());
        assert_eq!(dns_fragment("Code_Server"), "code-server");
    }

    #[test]
    fn resolves_direct_routes_from_assigned_instance_ports() {
        let routes = resolved_routes_for_network(
            &TailscaleServe {
                routes: vec![Route {
                    service: "code_server".to_owned(),
                    host_template: None,
                    path: None,
                    mode: RouteMode::Direct,
                }],
            },
            "altinn-studio",
            "pr_0",
            &network([("code_server", 4090, 31090, PortProtocolState::Tcp)]),
            "dev.tail.ts.net",
        )
        .unwrap();

        assert_eq!(routes.len(), 1);
        let route = &routes[0];
        assert_eq!(route.host, "dev.tail.ts.net");
        assert_eq!(route.https_port, Some(31090));
        assert_eq!(route.path, "/");
        assert_eq!(route.url, "https://dev.tail.ts.net:31090");
        assert_eq!(route.target, "http://127.0.0.1:31090");
    }

    #[test]
    fn resolves_proxy_routes_under_control_plane_path() {
        let routes = resolved_routes_for_network(
            &TailscaleServe {
                routes: vec![Route {
                    service: "code_server".to_owned(),
                    host_template: None,
                    path: None,
                    mode: RouteMode::Proxy,
                }],
            },
            "altinn-studio",
            "pr_0",
            &network([("code_server", 4090, 31090, PortProtocolState::Tcp)]),
            "dev.tail.ts.net",
        )
        .unwrap();

        assert_eq!(routes.len(), 1);
        let route = &routes[0];
        assert_eq!(route.host, "code-server-pr-0-altinn-studio");
        assert_eq!(route.https_port, None);
        assert_eq!(route.path, "/agents/altinn-studio/instances/pr-0/services/code-server");
        assert_eq!(
            route.url,
            "https://dev.tail.ts.net/agents/altinn-studio/instances/pr-0/services/code-server"
        );
        assert_eq!(route.target, "http://127.0.0.1:31090");
    }

    #[test]
    fn rejects_udp_routes() {
        let error = resolved_routes_for_network(
            &TailscaleServe {
                routes: vec![Route {
                    service: "dns".to_owned(),
                    host_template: None,
                    path: None,
                    mode: RouteMode::Direct,
                }],
            },
            "agent",
            "dev",
            &network([("dns", 53, 5300, PortProtocolState::Udp)]),
            "dev.tail.ts.net",
        )
        .unwrap_err();

        assert!(error.to_string().contains("requires tcp"));
    }

    fn network<const N: usize>(ports: [(&str, u16, u16, PortProtocolState); N]) -> NetworkState {
        NetworkState {
            mode: NetworkModeState::User,
            allow: NetworkAllowState::default(),
            ipv6: NetworkIpv6State::default(),
            ports: ports
                .into_iter()
                .map(|(name, guest, host, protocol)| {
                    (
                        name.to_owned(),
                        PortMappingState {
                            guest,
                            host: Some(host),
                            protocol,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            runtime: None,
        }
    }
}
