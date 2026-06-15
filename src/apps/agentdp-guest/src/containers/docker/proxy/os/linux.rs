use std::path::{Path, PathBuf};

use agentdp_platform::socket::{AsyncLocalSocket, AsyncLocalSocketListener};

use crate::{Error, Result};

use super::super::{CaConfig, Config, PreparedRequest, copy_response, prepare_request, read_request_head};

const DOCKER_SOCKET_PATH: &str = "/run/docker.sock";
const UPSTREAM_SOCKET_PATH: &str = "/run/agentdp/docker/docker.sock";
pub(super) async fn run(config: Config) -> Result<()> {
    let ca = CaConfig {
        pem: tokio::fs::read_to_string(&config.ca).await?,
        host_path: config.ca.to_string_lossy().into_owned(),
    };
    let listener = match socket_activation_listener()? {
        Some(listener) => listener,
        None => bind_listener(&config.listen).await?,
    };
    eprintln!(
        "guestd docker proxy: listening on {} and forwarding to {}",
        config.listen.display(),
        config.upstream.display()
    );

    loop {
        let client = listener.accept().await?;
        let upstream = config.upstream.clone();
        let ca = ca.clone();
        tokio::spawn(async move {
            if let Err(error) = Box::pin(handle_connection(client, upstream, &ca)).await {
                eprintln!("guestd docker proxy: connection failed: {error}");
            }
        });
    }
}

pub(super) fn default_listen_path() -> PathBuf {
    PathBuf::from(DOCKER_SOCKET_PATH)
}

pub(super) fn default_upstream_path() -> PathBuf {
    PathBuf::from(UPSTREAM_SOCKET_PATH)
}

pub(super) async fn bind_listener(path: &Path) -> Result<AsyncLocalSocketListener> {
    if path == Path::new(DOCKER_SOCKET_PATH) {
        return Err(Error::Message(format!(
            "{DOCKER_SOCKET_PATH} must be provided by socket activation; refusing direct bind"
        )));
    }
    let listener = agentdp_platform::socket::bind_local_socket(path)
        .await
        .map_err(|error| {
            Error::Message(format!(
                "failed to bind Docker proxy listener {}: {error}",
                path.display()
            ))
        })?;
    agentdp_platform::fs::set_file_mode(path, 0o660).await?;
    Ok(listener)
}

fn socket_activation_listener() -> Result<Option<AsyncLocalSocketListener>> {
    let mut listen_fds = agentdp_platform::socket_activation::ListenFds::from_env();
    if listen_fds.is_empty() {
        return Ok(None);
    }
    if listen_fds.len() != 1 {
        return Err(Error::Message(format!(
            "guestd docker proxy expected one socket activation fd, got {}",
            listen_fds.len()
        )));
    }
    let Some(listener) = listen_fds.take_local_socket_listener(0)? else {
        return Ok(None);
    };
    Ok(Some(listener))
}

async fn handle_connection(mut client: AsyncLocalSocket, upstream_path: PathBuf, ca: &CaConfig) -> Result<()> {
    let mut pending = Vec::new();
    loop {
        let Some(request) = read_request_head(&mut client, &mut pending).await? else {
            return Ok(());
        };
        let prepared = prepare_request(&mut client, request, ca).await?;
        match prepared {
            PreparedRequest::Forward {
                bytes,
                trailing,
                tunnel,
            } => {
                pending = trailing;
                let mut upstream = agentdp_platform::socket::connect_local_socket(&upstream_path)
                    .await
                    .map_err(|error| {
                        Error::Message(format!(
                            "failed to connect Docker proxy upstream {}: {error}",
                            upstream_path.display()
                        ))
                    })?;
                upstream.write_all(&bytes).await?;
                if tunnel {
                    let _ = Box::pin(agentdp_platform::socket::copy_bidirectional_local_socket(
                        client, upstream,
                    ))
                    .await?;
                    return Ok(());
                }
                copy_response(&mut upstream, &mut client).await?;
            }
            PreparedRequest::Response { bytes, close_client } => {
                client.write_all(&bytes).await?;
                if close_client {
                    return Ok(());
                }
            }
        }
    }
}
