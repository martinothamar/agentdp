use agentdp_platform as platform;
use agentdp_protocol::Error as ProtocolError;
use agentdp_qemu::{disk, image, net::stream, system};

use crate::host::{HostCredentialError, HostSeedError, HostSshError};

use super::provisioning;

#[derive(Debug)]
pub(crate) struct Error {
    kind: ErrorKind,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ErrorKind {
    #[error("{0}")]
    Disk(disk::Error),
    #[error("{0}")]
    Image(image::Error),
    #[error("{0}")]
    System(system::Error),
    #[error("{0}")]
    Stream(stream::Error),
    #[error("{0}")]
    Ssh(HostSshError),
    #[error("{0}")]
    Provisioning(provisioning::Error),
    #[error("{0}")]
    HostSeed(HostSeedError),
    #[error("{0}")]
    HostCredential(HostCredentialError),
    #[error("{0}")]
    BootstrapGraph(agentdp_core::provisioning::bootstrap::BootstrapGraphError),
    #[error("QEMU does not support image {os} {architecture} {variant}")]
    UnsupportedImage {
        os: &'static str,
        architecture: &'static str,
        variant: &'static str,
    },
    #[error("guest bootstrap did not finish after {timeout_seconds}s; last event: {last_event}")]
    GuestBootstrapTimeout { timeout_seconds: u64, last_event: String },
    #[error("guest bootstrap step {step} failed: {message}; stdout tail: {stdout_tail}; stderr tail: {stderr_tail}")]
    GuestBootstrapFailed {
        step: String,
        message: String,
        stdout_tail: String,
        stderr_tail: String,
    },
    #[error("guest control channel sent invalid message {message}: {source}")]
    GuestControlDecode {
        message: String,
        #[source]
        source: ProtocolError,
    },
    #[error("guest control channel reported {code}: {message}")]
    GuestControlMessage { code: String, message: String },
    #[error("{0}")]
    Terminate(platform::process::TerminateProcessError),
    #[error("{0}")]
    ProcessStatus(platform::process::ProcessStatusError),
    #[error("{message}")]
    StaleRunningRuntime { message: String },
    #[error("QEMU process {pid} did not exit after termination")]
    ProcessStillRunning { pid: u32 },
    #[error("failed to establish mediated QEMU stream for {instance}: {message}")]
    InstanceNetworkConnect { instance: String, message: String },
    #[error("mediated network is not running for {instance}")]
    InstanceNetworkNotRunning { instance: String },
    #[error(
        "mediated CA state is incomplete: cert_pem configured={cert_configured}, key_path configured={key_path_configured}"
    )]
    IncompleteMediatedCaState {
        cert_configured: bool,
        key_path_configured: bool,
    },
    #[error("failed to read mediated CA private key {path}: {source}")]
    ReadMediatedCaKey {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("user networking port `{name}` has no explicit host port")]
    MissingUserNetworkHostPort { name: String },
}

impl Error {
    const fn new(kind: ErrorKind) -> Self {
        Self { kind }
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

impl From<stream::Error> for Error {
    fn from(source: stream::Error) -> Self {
        ErrorKind::Stream(source).into()
    }
}

impl From<HostSshError> for Error {
    fn from(source: HostSshError) -> Self {
        ErrorKind::Ssh(source).into()
    }
}

impl From<provisioning::Error> for Error {
    fn from(source: provisioning::Error) -> Self {
        ErrorKind::Provisioning(source).into()
    }
}

impl From<HostSeedError> for Error {
    fn from(source: HostSeedError) -> Self {
        ErrorKind::HostSeed(source).into()
    }
}

impl From<HostCredentialError> for Error {
    fn from(source: HostCredentialError) -> Self {
        ErrorKind::HostCredential(source).into()
    }
}

impl From<agentdp_core::provisioning::bootstrap::BootstrapGraphError> for Error {
    fn from(source: agentdp_core::provisioning::bootstrap::BootstrapGraphError) -> Self {
        ErrorKind::BootstrapGraph(source).into()
    }
}

impl From<platform::process::TerminateProcessError> for Error {
    fn from(source: platform::process::TerminateProcessError) -> Self {
        ErrorKind::Terminate(source).into()
    }
}

impl From<platform::process::ProcessStatusError> for Error {
    fn from(source: platform::process::ProcessStatusError) -> Self {
        ErrorKind::ProcessStatus(source).into()
    }
}
