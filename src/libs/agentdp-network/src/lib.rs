#![forbid(unsafe_code)]
// agentdp-network is library code. Keep user I/O at app boundaries.
#![deny(clippy::dbg_macro)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]
#![allow(clippy::future_not_send, reason = "public handle methods are async")]

mod application;
mod buffers;
mod clock;
mod command;
mod connectors;
mod drive;
mod egress;
mod event_loop;
mod events;
mod gateway;
mod guest;
mod ingress;
mod network;
mod policy;
mod reactor;
mod runtime;
#[cfg(any(test, feature = "simulation"))]
pub mod test_support;
mod timer;
mod tls;

pub use buffers::FrameBuf;
pub use command::{NetworkCommand, NetworkCommandSource};
pub use event_loop::{EventLoop, NetworkExit};
pub use events::{
    NetworkAddresses, NetworkDnsEvent, NetworkEgressEvent, NetworkEgressProtocol, NetworkEvent, NetworkEventEnvelope,
    NetworkEventSink, NetworkEventText, NetworkHostPortEvent, NetworkLifecycleEvent, NetworkReactorEvent,
    NetworkStateEvent, NetworkTelemetryEvent, NetworkTelemetrySnapshot, NetworkTransportEvent,
};
pub use guest::{
    ConnectStatus, FrameRead, FrameWrite, GuestFrameSession, GuestFrameTransport, GuestIoSource, TransportError,
};
pub use network::{
    HostPortProtocol, HostPortSpec, InstanceAddresses, InstanceMacAddresses, InstanceNetworkConfig,
    InstanceNetworkError, InstanceNetworkSpec, InstanceNetworkState, InstanceNetworkStatus, Ipv4AddressText,
    MacAddress,
};
pub use policy::{Authority, EgressPolicy, Error, NetworkPolicy, RuntimeSecret, RuntimeSecrets, SecretScope};
pub use reactor::ProductionWake;
pub use tls::TlsInterceptConfig;
