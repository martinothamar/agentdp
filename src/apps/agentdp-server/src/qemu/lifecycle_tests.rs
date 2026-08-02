use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::agent::{AgentManifestContext, AgentName, AgentdpLayout, InstanceName};
use crate::qemu::{ImageState, MediatedCaState, State};
use crate::services::InstanceNetwork;
use agentdp_core::Context;
use agentdp_core::agent::{
    AgentInstancePhase, NetworkAllowState, NetworkIpv6State, NetworkModeState, NetworkState, PortMappingState,
    PortProtocolState, QemuInstanceNetworkState,
};
use agentdp_core::provisioning::secrets::SecretBinding;
use agentdp_network::{Authority, HostPortProtocol, HostPortSpec, InstanceNetworkConfig, InstanceNetworkSpec};
use agentdp_platform::socket::{self, AsyncLocalSocket};
use agentdp_platform::time;
use agentdp_qemu::net::QemuStreamTransport;
use smoltcp::phy::ChecksumCapabilities;
use smoltcp::wire::{
    ArpOperation, ArpPacket, ArpRepr, ETHERNET_HEADER_LEN, EthernetAddress, EthernetFrame, EthernetProtocol,
    IPV4_HEADER_LEN, IpAddress, IpProtocol, Ipv4Address, Ipv4Packet, Ipv4Repr, TcpControl, TcpPacket, TcpRepr,
    TcpSeqNumber,
};

use super::{
    agent_base_cmdline_matches, cleanup_runtime_files, egress_policy, instance_network_addresses, instance_network_mac,
};

#[test]
fn mediated_egress_policy_respects_network_allow_all() {
    let policy = egress_policy(&NetworkState {
        mode: NetworkModeState::Mediated,
        allow: NetworkAllowState::All,
        ipv6: NetworkIpv6State::default(),
        ports: BTreeMap::new(),
        runtime: None,
    });

    assert!(
        policy
            .check_destination(std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2)))
            .is_ok()
    );
}

#[test]
fn agent_base_process_match_requires_qemu_and_base_disk_path() {
    let disk = "/home/martin/.agentdp/agents/example/bases/sha256-test/disk.qcow2";
    let matching = format!(
        "/usr/bin/qemu-system-x86_64\0-name\0agentdp-example-agent-base\0-drive\0if=virtio,format=qcow2,file={disk}\0"
    );
    let wrong_disk = "/usr/bin/qemu-system-x86_64\0-drive\0if=virtio,format=qcow2,file=/tmp/other.qcow2\0";
    let wrong_program = format!("/usr/bin/qemu-img\0info\0{disk}\0");

    assert!(agent_base_cmdline_matches(matching.as_bytes(), disk));
    assert!(!agent_base_cmdline_matches(wrong_disk.as_bytes(), disk));
    assert!(!agent_base_cmdline_matches(wrong_program.as_bytes(), disk));
}

#[test]
fn mediated_egress_policy_restricts_host_allowlist() {
    let policy = egress_policy(&NetworkState {
        mode: NetworkModeState::Mediated,
        allow: NetworkAllowState::Hosts(vec!["api.github.com".to_owned()]),
        ipv6: NetworkIpv6State::default(),
        ports: BTreeMap::new(),
        runtime: None,
    });
    let destination = std::net::IpAddr::V4(std::net::Ipv4Addr::new(140, 82, 112, 6));
    let github = Authority::new("api.github.com");
    let example = Authority::new("example.com");

    assert!(policy.check_destination_authority(destination, Some(&github)).is_ok());
    assert!(policy.check_destination_authority(destination, None).is_err());
    assert!(policy.check_destination_authority(destination, Some(&example)).is_err());
}

#[tokio::test(flavor = "local")]
async fn instance_network_stream_answers_arp() {
    let network_runtime = test_instance_network();
    let network = test_network();
    let listener = start_test_qemu_server(&network.stream_socket).await;
    let handle = start_test_service_instance_network(&network_runtime, &network);
    let mut frames = accept_test_qemu_connection(&listener).await;

    frames.write_frame(&arp_request()).await.unwrap();
    handle.wait_ready(Duration::from_secs(2)).await.unwrap();
    let arp_response = read_test_frame(&mut frames).await;
    let ethernet = EthernetFrame::new_checked(arp_response.as_slice()).unwrap();
    assert_eq!(ethernet.ethertype(), EthernetProtocol::Arp);
    let arp = ArpPacket::new_checked(ethernet.payload()).unwrap();
    let ArpRepr::EthernetIpv4 {
        operation,
        source_protocol_addr,
        target_protocol_addr,
        ..
    } = ArpRepr::parse(&arp).unwrap()
    else {
        panic!("expected ethernet IPv4 ARP response");
    };
    assert_eq!(operation, ArpOperation::Reply);
    assert_eq!(source_protocol_addr, Ipv4Address::new(10, 73, 0, 1));
    assert_eq!(target_protocol_addr, Ipv4Address::new(10, 73, 0, 10));

    network_runtime.stop().await.unwrap();
}

#[tokio::test(flavor = "local")]
async fn instance_network_stream_reconnects_to_qemu_server() {
    let network_runtime = test_instance_network();
    let network = test_network();
    let listener = start_test_qemu_server(&network.stream_socket).await;
    let handle = start_test_service_instance_network(&network_runtime, &network);
    let mut first = accept_test_qemu_connection(&listener).await;
    first.write_frame(&arp_request()).await.unwrap();
    handle.wait_ready(Duration::from_secs(2)).await.unwrap();
    let _arp_response = read_test_frame(&mut first).await;
    drop(first);

    let mut second = accept_test_qemu_connection(&listener).await;
    second.write_frame(&arp_request()).await.unwrap();
    let arp_response = read_test_frame(&mut second).await;
    let ethernet = EthernetFrame::new_checked(arp_response.as_slice()).unwrap();

    assert_eq!(ethernet.ethertype(), EthernetProtocol::Arp);
    network_runtime.stop().await.unwrap();
}

#[tokio::test(flavor = "local")]
async fn cleanup_runtime_files_stops_instance_network_and_removes_runtime_dir() {
    let network_runtime = test_instance_network();
    let qemu_dir = test_runtime_dir();
    tokio::fs::create_dir_all(&qemu_dir).await.unwrap();
    let network = test_network_in_dir(&qemu_dir);
    let listener = start_test_qemu_server(&network.stream_socket).await;
    let task = start_test_service_instance_network(&network_runtime, &network);
    let mut frames = accept_test_qemu_connection(&listener).await;

    frames.write_frame(&arp_request()).await.unwrap();
    task.wait_ready(Duration::from_secs(2)).await.unwrap();
    let _arp_response = read_test_frame(&mut frames).await;
    drop(frames);
    drop(listener);

    let state = test_state(&qemu_dir, network.clone());
    let (agent, instance) = test_instance_names();
    cleanup_runtime_files(&network_runtime, &agent, &instance, &state)
        .await
        .unwrap();

    assert!(!tokio::fs::try_exists(&network.stream_socket).await.unwrap());
    assert!(!tokio::fs::try_exists(&qemu_dir).await.unwrap());
}

#[tokio::test(flavor = "local")]
async fn stop_instance_uses_pid_file_when_in_memory_pid_is_missing() {
    let network_runtime = test_instance_network();
    let qemu_dir = test_runtime_dir();
    tokio::fs::create_dir_all(&qemu_dir).await.unwrap();
    let network = test_network_in_dir(&qemu_dir);
    let mut state = test_state(&qemu_dir, network);
    tokio::fs::write(&state.pid_file, u32::MAX.to_string()).await.unwrap();
    let (agent, instance) = test_instance_names();

    let mut control = None;
    let output = super::stop_instance(
        &Context::quiet(),
        &network_runtime,
        crate::backend::StopInstanceInput {
            name: "test-instance",
            agent: &agent,
            instance: &instance,
            status: AgentInstancePhase::Failed,
        },
        &mut state,
        &mut control,
    )
    .await
    .unwrap();

    assert_eq!(output.process_status, "missing");
    assert!(!tokio::fs::try_exists(&qemu_dir).await.unwrap());
}

#[tokio::test(flavor = "local")]
async fn reconcile_reattaches_instance_network_without_marking_instance_stale() {
    let network_runtime = test_instance_network();
    let qemu_dir = test_runtime_dir();
    tokio::fs::create_dir_all(&qemu_dir).await.unwrap();
    let network = test_network_in_dir(&qemu_dir);
    let mut state = test_state(&qemu_dir, network);
    state.pid = Some(std::process::id());
    let instance_network = NetworkState {
        mode: NetworkModeState::Mediated,
        allow: NetworkAllowState::All,
        ipv6: NetworkIpv6State::default(),
        ports: BTreeMap::new(),
        runtime: None,
    };
    let context = Context::quiet();
    let manifest = test_loaded_manifest().await;

    let output = super::reconcile(
        super::RuntimeInput {
            context: &context,
            instance_network: &network_runtime,
            instance_status: AgentInstancePhase::Running,
            agent: "test-manifest",
            instance: "test-instance",
            network: &instance_network,
            manifest: &manifest,
        },
        &mut state,
    )
    .await
    .unwrap();

    assert!(!output.stale);
    assert!(!output.backend_changed);
    assert!(!output.mark_stopped);

    let (agent, instance) = test_instance_names();
    cleanup_runtime_files(&network_runtime, &agent, &instance, &state)
        .await
        .unwrap();
}

#[tokio::test(flavor = "local")]
async fn unconfigured_runtime_secrets_are_cleared_before_runtime_host_input_collection() {
    let qemu_dir = test_runtime_dir();
    let mut state = test_state(&qemu_dir, test_network_in_dir(&qemu_dir));
    state.mediated_secrets.insert(
        SecretBinding::new_with_placeholder(
            "CODEX_AUTH_TOKEN",
            Some("AGENTDP_SECRET_CODEX_AUTH_TOKEN_TEST".to_owned()),
            "stored-token",
            &["chatgpt.com".to_owned()],
        )
        .expect("test secret binding"),
    );
    let manifest = test_loaded_manifest().await;

    assert!(super::clear_stored_runtime_secrets_if_unconfigured(
        manifest.value(),
        &mut state
    ));

    assert!(state.mediated_secrets.is_empty());
}

#[tokio::test(flavor = "local")]
async fn runtime_secret_refresh_fails_when_mediated_network_is_not_running() {
    let network_runtime = test_instance_network();
    let qemu_dir = test_runtime_dir();
    let mut state = test_state(&qemu_dir, test_network_in_dir(&qemu_dir));
    state.mediated_secrets.insert(
        SecretBinding::new_with_placeholder(
            "CODEX_AUTH_TOKEN",
            Some("AGENTDP_SECRET_CODEX_AUTH_TOKEN_TEST".to_owned()),
            "stored-token",
            &["chatgpt.com".to_owned()],
        )
        .expect("test secret binding"),
    );
    let instance_network = NetworkState {
        mode: NetworkModeState::Mediated,
        allow: NetworkAllowState::All,
        ipv6: NetworkIpv6State::default(),
        ports: BTreeMap::new(),
        runtime: None,
    };
    let context = Context::quiet();
    let manifest = test_loaded_manifest().await;

    let error = super::reconcile_runtime_secrets(
        super::RuntimeInput {
            context: &context,
            instance_network: &network_runtime,
            instance_status: AgentInstancePhase::Running,
            agent: "test-manifest",
            instance: "test-instance",
            network: &instance_network,
            manifest: &manifest,
        },
        &mut state,
    )
    .await
    .expect_err("missing mediated network must be retried");

    assert!(error.to_string().contains("network is not running"));
}

#[tokio::test(flavor = "local")]
async fn instance_network_status_reports_live_attach_progress() {
    let network_runtime = test_instance_network();
    let qemu_dir = test_runtime_dir();
    tokio::fs::create_dir_all(&qemu_dir).await.unwrap();
    let network = test_network_in_dir(&qemu_dir);
    let mut state = test_state(&qemu_dir, network.clone());
    state.pid = Some(std::process::id());
    state.qemu_log = qemu_dir.join("qemu.log").display().to_string();
    let instance_network = NetworkState {
        mode: NetworkModeState::Mediated,
        allow: NetworkAllowState::All,
        ipv6: NetworkIpv6State::default(),
        ports: BTreeMap::new(),
        runtime: None,
    };
    let context = Context::quiet();
    let manifest = test_loaded_manifest().await;

    let output = super::reconcile(
        super::RuntimeInput {
            context: &context,
            instance_network: &network_runtime,
            instance_status: AgentInstancePhase::Running,
            agent: "test-manifest",
            instance: "test-instance",
            network: &instance_network,
            manifest: &manifest,
        },
        &mut state,
    )
    .await
    .unwrap();
    assert!(!output.stale);

    let status = wait_for_mediated_status(&network_runtime, &state, |status| {
        matches!(status.state.as_str(), "connecting" | "backoff") && !status.ready
    })
    .await;
    assert!(status.transport.is_some() || status.generation == Some(0), "{status:?}");

    let listener = start_test_qemu_server(&network.stream_socket).await;
    let mut frames = accept_test_qemu_connection(&listener).await;
    let status = wait_for_mediated_status(&network_runtime, &state, |status| {
        status.state == "connected" && !status.ready
    })
    .await;
    assert_eq!(status.generation, Some(1));

    frames.write_frame(&arp_request()).await.unwrap();
    let _arp_response = read_test_frame(&mut frames).await;
    wait_for_mediated_ready(&network_runtime, &state).await;
    let (agent, instance) = test_instance_names();
    let status = super::instance_network_status(&network_runtime, &agent, &instance, &state).unwrap();

    assert!(status.ready);
    assert_eq!(status.state, "traffic-observed");
    assert_eq!(status.guest_frames_received, 1);
    assert_eq!(status.connect_errors, 0);
    assert_eq!(status.last_error, None);
    assert!(status.last_transport_connect_unix_seconds.is_some());
    assert!(status.last_guest_frame_unix_seconds.is_some());

    drop(frames);
    drop(listener);
    cleanup_runtime_files(&network_runtime, &agent, &instance, &state)
        .await
        .unwrap();
}

#[tokio::test(flavor = "local")]
async fn reconcile_connects_existing_qemu_socket_and_republishes_ports() {
    let network_runtime = test_instance_network();
    let qemu_dir = test_runtime_dir();
    tokio::fs::create_dir_all(&qemu_dir).await.unwrap();
    let network = test_network_in_dir(&qemu_dir);
    let listener = start_test_qemu_server(&network.stream_socket).await;
    let host_port = unused_host_port();
    let mut state = test_state(&qemu_dir, network.clone());
    state.pid = Some(std::process::id());
    let instance_network = NetworkState {
        mode: NetworkModeState::Mediated,
        allow: NetworkAllowState::All,
        ipv6: NetworkIpv6State::default(),
        ports: BTreeMap::from([(
            "web".to_owned(),
            PortMappingState {
                guest: HOST_PORT_GUEST_PORT,
                host: Some(host_port),
                protocol: PortProtocolState::Tcp,
            },
        )]),
        runtime: None,
    };
    let context = Context::quiet();
    let manifest = test_loaded_manifest().await;

    let output = super::reconcile(
        super::RuntimeInput {
            context: &context,
            instance_network: &network_runtime,
            instance_status: AgentInstancePhase::Running,
            agent: "test-manifest",
            instance: "test-instance",
            network: &instance_network,
            manifest: &manifest,
        },
        &mut state,
    )
    .await
    .unwrap();
    let mut frames = accept_test_qemu_connection(&listener).await;
    let client_task = tokio::spawn(async move {
        let mut client = agentdp_platform::net::connect_tcp_stream((Ipv4Addr::LOCALHOST, host_port))
            .await
            .unwrap();
        let mut banner = [0; 12];
        tokio::io::AsyncReadExt::read_exact(&mut client, &mut banner)
            .await
            .unwrap();
        banner
    });

    let connection = accept_host_port_tcp_connection(&mut frames).await;
    connection.send_guest_payload(&mut frames, b"SSH-2.0-test", 0).await;
    let banner = tokio::time::timeout(Duration::from_secs(2), client_task)
        .await
        .expect("host-port TCP client did not receive guest banner")
        .unwrap();

    assert!(!output.stale);
    assert_eq!(&banner, b"SSH-2.0-test");
    drop(frames);
    drop(listener);
    let (agent, instance) = test_instance_names();
    cleanup_runtime_files(&network_runtime, &agent, &instance, &state)
        .await
        .unwrap();
}

#[tokio::test(flavor = "local")]
async fn host_port_bind_failure_fails_instance_network_startup() {
    let occupied = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let host = occupied.local_addr().unwrap().port();
    let mut ports = BTreeMap::new();
    ports.insert(
        "web".to_owned(),
        PortMappingState {
            guest: 8080,
            host: Some(host),
            protocol: PortProtocolState::Tcp,
        },
    );
    let qemu_network = test_network();
    let mut config = InstanceNetworkConfig::new(
        qemu_network.addresses,
        instance_network_mac(agentdp_core::mediated_network::DEFAULT_PROFILE),
        agentdp_network::EgressPolicy::default_deny_private(),
    );
    config.host_ports = ports
        .into_iter()
        .map(|(name, port)| HostPortSpec {
            name,
            protocol: match port.protocol {
                PortProtocolState::Tcp => HostPortProtocol::Tcp,
                PortProtocolState::Udp => HostPortProtocol::Udp,
            },
            guest: port.guest,
            host: port
                .host
                .unwrap_or_else(|| panic!("test host port fixture must be explicit")),
        })
        .collect();
    let network_runtime = test_instance_network();
    let transport = QemuStreamTransport::connect(&qemu_network.stream_socket);
    let handle = network_runtime
        .start(
            &Context::quiet(),
            InstanceNetworkSpec {
                label: "test-instance".to_owned(),
                config,
                reconnect_delay: Duration::from_millis(10),
                write_timeout: Duration::from_secs(1),
            },
            transport,
        )
        .unwrap();

    let Err(error) = handle.wait_ready(Duration::from_secs(2)).await else {
        panic!("expected host port bind failure");
    };
    assert!(
        error
            .to_string()
            .contains(&format!("failed to bind TCP host port web on 127.0.0.1:{host}"))
    );

    let observation = network_runtime.observation().unwrap();
    let agentdp_network::InstanceNetworkState::Failed { error } = &observation.status.state else {
        panic!("expected failed instance network status");
    };
    assert!(
        error.contains(&format!("failed to bind TCP host port web on 127.0.0.1:{host}")),
        "{error}"
    );

    let cleanup_error = network_runtime.cleanup().await.unwrap_err();
    assert!(
        cleanup_error.to_string().contains("failed to bind TCP host port web"),
        "{cleanup_error}"
    );
}

async fn read_test_frame(frames: &mut FrameStream) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(2), frames.read_frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn start_test_qemu_server(path: &str) -> FrameListener {
    FrameListener::bind(path).await
}

async fn accept_test_qemu_connection(listener: &FrameListener) -> FrameStream {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(frames) = listener.accept().await {
                return frames;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("agentdp instance network did not connect to test QEMU server")
}

struct FrameListener {
    listener: socket::AsyncLocalSocketListener,
}

impl FrameListener {
    async fn bind(path: &str) -> Self {
        let path = Path::new(path);
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            let _removed = tokio::fs::remove_file(path).await;
        }
        let listener = socket::bind_local_socket(path).await.unwrap();
        Self { listener }
    }

    async fn accept(&self) -> std::io::Result<FrameStream> {
        self.listener.accept().await.map(FrameStream::new)
    }
}

struct FrameStream {
    stream: AsyncLocalSocket,
}

impl FrameStream {
    const fn new(stream: AsyncLocalSocket) -> Self {
        Self { stream }
    }

    async fn read_frame(&mut self) -> Result<Option<Vec<u8>>, std::io::Error> {
        let mut length = [0_u8; 4];
        match self.stream.read_exact(&mut length).await {
            Ok(()) => {}
            Err(source) if source.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(source) => return Err(source),
        }
        let mut frame = vec![0; u32::from_be_bytes(length) as usize];
        self.stream.read_exact(&mut frame).await?;
        Ok(Some(frame))
    }

    async fn write_frame(&mut self, frame: &[u8]) -> Result<(), std::io::Error> {
        let length = u32::try_from(frame.len()).unwrap_or(u32::MAX);
        self.stream.write_all(&length.to_be_bytes()).await?;
        self.stream.write_all(frame).await?;
        self.stream.flush().await
    }
}

async fn wait_for_mediated_status(
    network_runtime: &InstanceNetwork,
    state: &State,
    matches: impl Fn(&agentdp_core::agent::AgentInstanceNetworkStatus) -> bool + Send + Sync,
) -> agentdp_core::agent::AgentInstanceNetworkStatus {
    let (agent, instance) = test_instance_names();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(status) = super::instance_network_status(network_runtime, &agent, &instance, state)
                && matches(&status)
            {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("instance network did not reach expected status")
}

async fn wait_for_mediated_ready(network_runtime: &InstanceNetwork, state: &State) {
    wait_for_mediated_status(network_runtime, state, |status| status.ready).await;
}

fn start_test_service_instance_network(
    network_runtime: &InstanceNetwork,
    network: &QemuInstanceNetworkState,
) -> crate::services::InstanceNetworkHandle {
    let transport = QemuStreamTransport::connect(&network.stream_socket);
    let mut config = InstanceNetworkConfig::new(
        network.addresses,
        instance_network_mac(agentdp_core::mediated_network::DEFAULT_PROFILE),
        agentdp_network::EgressPolicy::default_deny_private(),
    );
    config.limits.status_publish_interval = Duration::from_millis(10);
    network_runtime
        .start(
            &Context::quiet(),
            InstanceNetworkSpec {
                label: "test-instance".to_owned(),
                config,
                reconnect_delay: Duration::from_millis(10),
                write_timeout: Duration::from_secs(1),
            },
            transport,
        )
        .unwrap()
}

fn test_instance_names() -> (AgentName, InstanceName) {
    (test_agent_name(), test_instance_name())
}

fn test_agent_name() -> AgentName {
    AgentName::new("test-manifest")
}

fn test_instance_name() -> InstanceName {
    InstanceName::new("test-instance")
}

fn test_instance_network() -> InstanceNetwork {
    let (events, _receiver) = agentdp_ds::local::spsc::bounded(128);
    InstanceNetwork::new(events)
}

struct HostPortTcpConnection {
    gateway_port: u16,
    gateway_seq: u32,
    server_seq: u32,
}

impl HostPortTcpConnection {
    async fn send_guest_payload(&self, frames: &mut FrameStream, payload: &[u8], host_payload_len: usize) {
        frames
            .write_frame(&tcp_payload_frame(
                gateway_ipv4(),
                self.gateway_port,
                HOST_PORT_GUEST_PORT,
                self.server_seq + 1,
                self.guest_ack(host_payload_len),
                payload,
            ))
            .await
            .unwrap();
    }

    fn guest_ack(&self, host_payload_len: usize) -> u32 {
        self.gateway_seq + 1 + u32::try_from(host_payload_len).unwrap_or(u32::MAX)
    }
}

const GUEST_IP: Ipv4Address = ipv4(agentdp_core::mediated_network::DEFAULT_PROFILE.guest_ipv4);
const GATEWAY_IP: Ipv4Address = ipv4(agentdp_core::mediated_network::DEFAULT_PROFILE.gateway_ipv4);
const GUEST_MAC: EthernetAddress = EthernetAddress(agentdp_core::mediated_network::DEFAULT_PROFILE.guest_mac.octets());
const GATEWAY_MAC: EthernetAddress =
    EthernetAddress(agentdp_core::mediated_network::DEFAULT_PROFILE.gateway_mac.octets());
const HOST_PORT_GUEST_PORT: u16 = 22;
const HOST_PORT_SERVER_SEQ: u32 = 10_000;

const fn ipv4(address: Ipv4Addr) -> Ipv4Address {
    let [a, b, c, d] = address.octets();
    Ipv4Address::new(a, b, c, d)
}

async fn accept_host_port_tcp_connection(frames: &mut FrameStream) -> HostPortTcpConnection {
    let (gateway_port, gateway_seq) = wait_for_host_port_tcp_syn(frames).await;
    frames
        .write_frame(&tcp_frame(
            gateway_ipv4(),
            gateway_port,
            HOST_PORT_GUEST_PORT,
            TcpControl::Syn,
            HOST_PORT_SERVER_SEQ,
            Some(gateway_seq + 1),
            &[],
        ))
        .await
        .unwrap();
    wait_for_tcp_ack(frames, gateway_port).await;
    HostPortTcpConnection {
        gateway_port,
        gateway_seq,
        server_seq: HOST_PORT_SERVER_SEQ,
    }
}

async fn wait_for_host_port_tcp_syn(frames: &mut FrameStream) -> (u16, u32) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = read_test_frame(frames).await;
            if is_arp_request_for_guest(&frame) {
                frames.write_frame(&arp_reply()).await.unwrap();
                continue;
            }
            if let Some(segment) = ipv4_tcp_segment(&frame)
                && segment.dst_port == HOST_PORT_GUEST_PORT
                && segment.control == TcpControl::Syn
            {
                return (segment.src_port, seq_to_u32(segment.seq_number));
            }
        }
    })
    .await
    .expect("host-port TCP SYN did not reach fake QEMU guest")
}

async fn wait_for_tcp_ack(frames: &mut FrameStream, gateway_port: u16) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = read_test_frame(frames).await;
            if let Some(segment) = ipv4_tcp_segment(&frame)
                && segment.dst_port == HOST_PORT_GUEST_PORT
                && segment.src_port == gateway_port
                && segment.control == TcpControl::None
            {
                return;
            }
        }
    })
    .await
    .expect("host-port TCP handshake ACK did not reach fake QEMU guest");
}

fn test_network() -> QemuInstanceNetworkState {
    static NEXT_NETWORK_ID: AtomicU64 = AtomicU64::new(0);

    let nonce = time::unix_nanos();
    let id = NEXT_NETWORK_ID.fetch_add(1, Ordering::Relaxed);
    let socket = std::env::temp_dir().join(format!(
        "agentdp-mediated-network-test-{}-{id}-{nonce}.sock",
        std::process::id()
    ));
    test_network_with_socket(&socket)
}

fn test_network_in_dir(qemu_dir: &Path) -> QemuInstanceNetworkState {
    test_network_with_socket(&qemu_dir.join("qemu.sock"))
}

fn test_network_with_socket(socket: &Path) -> QemuInstanceNetworkState {
    QemuInstanceNetworkState {
        addresses: instance_network_addresses(agentdp_core::mediated_network::DEFAULT_PROFILE),
        stream_socket: socket.display().to_string(),
    }
}

async fn test_loaded_manifest() -> AgentManifestContext {
    const MANIFEST: &str = r"
apiVersion: agentdp.dev/v1alpha1
kind: Agent
metadata:
  name: test-manifest
spec:
  phase: Running
  replicas: 1
  template:
    image:
      os: archlinux
    user:
      name: agent
    resources:
      cpus: 1
      memory: 1G
      storage: 4G
    network:
      mode: mediated
      allow: all
      ports:
        ssh:
          guest: 22
          protocol: tcp
    bootstrap:
      healthchecks: []
    secrets: []
    plugins: {}
";
    let root = test_runtime_dir().join("manifest-context");
    tokio::fs::create_dir_all(&root).await.unwrap();
    let manifest_path = root.join("agent.yaml");
    tokio::fs::write(&manifest_path, MANIFEST).await.unwrap();
    let layout = AgentdpLayout::from_root(root.join("agentdp"));
    AgentManifestContext::load(&Context::quiet(), &layout, &manifest_path)
        .await
        .expect("manifest context")
}

fn test_runtime_dir() -> PathBuf {
    let nonce = time::unix_nanos();
    std::env::temp_dir()
        .join("adp")
        .join(format!("{:x}{nonce:x}", std::process::id()))
        .join("qemu")
}

fn test_state(qemu_dir: &Path, network: QemuInstanceNetworkState) -> State {
    State {
        image: ImageState {
            os: "test".to_owned(),
            architecture: "x86_64".to_owned(),
            variant: "cloud".to_owned(),
            source_url: String::new(),
            cache_key: String::new(),
            cache_path: String::new(),
            download_path: String::new(),
            format: String::new(),
        },
        disk: String::new(),
        work_dir: String::new(),
        seed_media: String::new(),
        seed_meta_data: String::new(),
        seed_network_config: String::new(),
        seed_user_data: String::new(),
        monitor_socket: qemu_dir.join("monitor.sock").display().to_string(),
        qmp_socket: qemu_dir.join("qmp.sock").display().to_string(),
        guest_control_socket: qemu_dir.join("guest-control.sock").display().to_string(),
        pid_file: qemu_dir.join("qemu.pid").display().to_string(),
        serial_log: String::new(),
        qemu_log: String::new(),
        instance_network: Some(network),
        mediated_secrets: agentdp_core::provisioning::secrets::SecretBindings::default(),
        mediated_ca: MediatedCaState::default(),
        pid: None,
        last_start_unix_seconds: None,
    }
}

fn unused_host_port() -> u16 {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.local_addr().unwrap().port()
}

fn is_arp_request_for_guest(frame: &[u8]) -> bool {
    let Ok(ethernet) = EthernetFrame::new_checked(frame) else {
        return false;
    };
    if ethernet.ethertype() != EthernetProtocol::Arp {
        return false;
    }
    let Ok(arp) = ArpPacket::new_checked(ethernet.payload()) else {
        return false;
    };
    let Ok(ArpRepr::EthernetIpv4 {
        operation,
        target_protocol_addr,
        ..
    }) = ArpRepr::parse(&arp)
    else {
        return false;
    };
    operation == ArpOperation::Request && target_protocol_addr == GUEST_IP
}

fn arp_request() -> Vec<u8> {
    let request = ArpRepr::EthernetIpv4 {
        operation: ArpOperation::Request,
        source_hardware_addr: GUEST_MAC,
        source_protocol_addr: GUEST_IP,
        target_hardware_addr: EthernetAddress([0, 0, 0, 0, 0, 0]),
        target_protocol_addr: GATEWAY_IP,
    };
    ethernet_frame(
        GUEST_MAC,
        EthernetAddress::BROADCAST,
        EthernetProtocol::Arp,
        request.buffer_len(),
        |payload| request.emit(&mut ArpPacket::new_unchecked(payload)),
    )
}

fn arp_reply() -> Vec<u8> {
    let reply = ArpRepr::EthernetIpv4 {
        operation: ArpOperation::Reply,
        source_hardware_addr: GUEST_MAC,
        source_protocol_addr: GUEST_IP,
        target_hardware_addr: GATEWAY_MAC,
        target_protocol_addr: GATEWAY_IP,
    };
    ethernet_frame(
        GUEST_MAC,
        GATEWAY_MAC,
        EthernetProtocol::Arp,
        reply.buffer_len(),
        |payload| {
            reply.emit(&mut ArpPacket::new_unchecked(payload));
        },
    )
}

fn ipv4_tcp_segment(frame: &[u8]) -> Option<TcpRepr<'_>> {
    let ethernet = EthernetFrame::new_checked(frame).ok()?;
    if ethernet.ethertype() != EthernetProtocol::Ipv4 {
        return None;
    }
    let ipv4 = Ipv4Packet::new_checked(ethernet.payload()).ok()?;
    if ipv4.next_header() != IpProtocol::Tcp {
        return None;
    }
    let tcp = TcpPacket::new_checked(ipv4.payload()).ok()?;
    TcpRepr::parse(
        &tcp,
        &IpAddress::Ipv4(ipv4.src_addr()),
        &IpAddress::Ipv4(ipv4.dst_addr()),
        &ChecksumCapabilities::ignored(),
    )
    .ok()
}

fn tcp_payload_frame(
    destination: Ipv4Address,
    destination_port: u16,
    source_port: u16,
    sequence: u32,
    acknowledgement: u32,
    payload: &[u8],
) -> Vec<u8> {
    tcp_frame(
        destination,
        destination_port,
        source_port,
        TcpControl::Psh,
        sequence,
        Some(acknowledgement),
        payload,
    )
}

fn tcp_frame(
    destination: Ipv4Address,
    destination_port: u16,
    source_port: u16,
    control: TcpControl,
    sequence: u32,
    acknowledgement: Option<u32>,
    payload: &[u8],
) -> Vec<u8> {
    let tcp = TcpRepr {
        src_port: source_port,
        dst_port: destination_port,
        control,
        seq_number: seq_from_u32(sequence),
        ack_number: acknowledgement.map(seq_from_u32),
        window_len: u16::MAX,
        window_scale: None,
        max_seg_size: matches!(control, TcpControl::Syn).then_some(1460),
        sack_permitted: false,
        sack_ranges: [None, None, None],
        timestamp: None,
        payload,
    };
    let ipv4 = Ipv4Repr {
        src_addr: GUEST_IP,
        dst_addr: destination,
        next_header: IpProtocol::Tcp,
        payload_len: tcp.buffer_len(),
        hop_limit: 64,
    };
    ethernet_frame(
        GUEST_MAC,
        GATEWAY_MAC,
        EthernetProtocol::Ipv4,
        ipv4.buffer_len() + ipv4.payload_len,
        |ethernet_payload| {
            let (ip_header, ip_payload) = ethernet_payload.split_at_mut(IPV4_HEADER_LEN);
            ipv4.emit(
                &mut Ipv4Packet::new_unchecked(ip_header),
                &ChecksumCapabilities::default(),
            );
            tcp.emit(
                &mut TcpPacket::new_unchecked(&mut ip_payload[..tcp.buffer_len()]),
                &IpAddress::Ipv4(GUEST_IP),
                &IpAddress::Ipv4(destination),
                &ChecksumCapabilities::default(),
            );
        },
    )
}

const fn seq_to_u32(sequence: TcpSeqNumber) -> u32 {
    sequence.0.cast_unsigned()
}

const fn seq_from_u32(sequence: u32) -> TcpSeqNumber {
    TcpSeqNumber(sequence.cast_signed())
}

const fn gateway_ipv4() -> Ipv4Address {
    GATEWAY_IP
}

fn ethernet_frame(
    source: EthernetAddress,
    destination: EthernetAddress,
    protocol: EthernetProtocol,
    payload_len: usize,
    emit_payload: impl FnOnce(&mut [u8]),
) -> Vec<u8> {
    let mut bytes = vec![0_u8; ETHERNET_HEADER_LEN + payload_len];
    let mut frame = EthernetFrame::new_unchecked(bytes.as_mut_slice());
    frame.set_src_addr(source);
    frame.set_dst_addr(destination);
    frame.set_ethertype(protocol);
    emit_payload(frame.payload_mut());
    bytes
}
