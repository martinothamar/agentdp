use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

mod device;
mod dns;
mod packet;

use device::FrameDevice;
use dns::{DnsAttribution, dns_attribution_capacity};
use packet::{TcpSyn, UdpDatagram, resolve_gateway_destination, udp_frame, udp_response_frame};

use crate::application::{self, ApplicationProtocol};
use crate::buffers::{BufferPool, ByteBuf, FrameBuf};
use crate::clock::NetworkClock;
use crate::drive::DriveTurn;
use crate::egress::tcp::TcpProxies;
use crate::ingress::TcpConnections;
use crate::ingress::UdpPeers;
use crate::network::{
    ApplicationPolicy, BlockReason, EgressDecision, EgressUdpSend, HostConnectionId, IngressTcpOutput, IngressUdpSend,
    InstanceMacAddresses, InstanceNetworkConfig, TcpEgressPolicy, TcpEgressRoute, UdpProxyKey,
};
use crate::policy::{Authority, NetworkPolicy};
use crate::reactor::ReactorBackend;
use crate::tls::TlsIntercept;
use smoltcp::iface::{Config as SmoltcpConfig, Interface, PollResult, SocketSet};
use smoltcp::wire::IpAddress;
use smoltcp::wire::{HardwareAddress, IpCidr};

pub(crate) struct Gateway<C: NetworkClock> {
    iface: Interface,
    sockets: SocketSet<'static>,
    device: FrameDevice,
    buffers: BufferPool,
    dns: DnsAttribution,
    clock: C,
    config: GatewayConfig,
}

pub(crate) struct GatewayOutputs<'a> {
    egress_udp_sends: &'a mut Vec<EgressUdpSend>,
    ingress_udp_sends: &'a mut Vec<IngressUdpSend>,
    guest_frames: &'a mut Vec<FrameBuf>,
}

impl<'a> GatewayOutputs<'a> {
    pub(crate) const fn new(
        egress_udp_sends: &'a mut Vec<EgressUdpSend>,
        ingress_udp_sends: &'a mut Vec<IngressUdpSend>,
        guest_frames: &'a mut Vec<FrameBuf>,
    ) -> Self {
        Self {
            egress_udp_sends,
            ingress_udp_sends,
            guest_frames,
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "gateway returns owned pooled outputs on capacity exhaustion without boxing or copying"
    )]
    fn push_egress_udp(&mut self, send: EgressUdpSend, drive: &mut DriveTurn<'_>) -> Result<(), EgressUdpSend> {
        drive.push_component_output(self.egress_udp_sends, send)
    }

    #[allow(
        clippy::result_large_err,
        reason = "gateway returns owned pooled outputs on capacity exhaustion without boxing or copying"
    )]
    fn push_ingress_udp(&mut self, send: IngressUdpSend, drive: &mut DriveTurn<'_>) -> Result<(), IngressUdpSend> {
        drive.push_component_output(self.ingress_udp_sends, send)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UdpFrameWrite {
    Queued,
    Blocked,
    Dropped,
}

#[derive(Debug)]
pub(crate) enum GuestFrameIngest {
    Queued,
    Blocked(FrameBuf),
    Dropped,
}

impl<C: NetworkClock> Gateway<C> {
    #[must_use]
    pub(crate) fn new(config: &InstanceNetworkConfig, buffers: BufferPool, clock: C) -> Self {
        let mut device = FrameDevice::new(config.mtu, buffers.clone(), config.limits.frame_device_queue_capacity);
        let mut iface = Interface::new(
            SmoltcpConfig::new(HardwareAddress::Ethernet(config.mac.gateway.smoltcp())),
            &mut device,
            clock.smoltcp_now(),
        );
        iface.update_ip_addrs(|addrs| {
            let _ = addrs.push(IpCidr::Ipv4(smoltcp::wire::Ipv4Cidr::new(
                config.network.gateway.0,
                config.network.cidr_prefix,
            )));
        });
        let _route = iface.routes_mut().add_default_ipv4_route(config.network.gateway.0);
        iface.set_any_ip(true);

        let sockets = SocketSet::new(vec![]);
        Self {
            iface,
            sockets,
            device,
            buffers,
            dns: DnsAttribution::new(dns_attribution_capacity(&config.limits)),
            clock,
            config: GatewayConfig {
                policy: config.policy.clone(),
                mac: config.mac,
                gateway: config.network.gateway.std(),
                guest: config.network.address.std(),
                dns_upstream: config.dns_upstream,
                tls: config.tls.clone().map(TlsIntercept::new),
                idle_poll_delay: config.limits.idle_poll_delay,
            },
        }
    }

    pub(crate) fn record_dns_resolution(&mut self, host: &str, addresses: Vec<IpAddr>, ttl: Duration) {
        self.dns.record(host, addresses, ttl, &self.clock);
    }

    /// Returns smoltcp's advisory wait time before the gateway should be polled again.
    #[must_use]
    pub(crate) fn next_poll_delay(&mut self) -> Duration {
        self.iface
            .poll_delay(self.clock.smoltcp_now(), &self.sockets)
            .map_or(self.config.idle_poll_delay, Into::into)
    }

    /// Advances smoltcp timers/socket state and returns frames produced by the gateway.
    pub(crate) fn poll(&mut self, guest_frames: &mut Vec<FrameBuf>, drive: &mut DriveTurn<'_>) {
        self.drain_transmitted_frames(guest_frames, drive);
        if !self.has_poll_work() {
            return;
        }
        let now = self.clock.smoltcp_now();
        let poll_delay_before_poll = self.iface.poll_delay(now, &self.sockets).map(Duration::from);
        let received_frames_before_poll = self.device.received_frame_count();
        let transmitted_frames_before_poll = self.device.transmitted_frame_count();
        let Some(poll_result) = drive.poll_gateway(|| {
            self.device.allow_one_receive();
            self.iface.poll(now, &mut self.device, &mut self.sockets)
        }) else {
            return;
        };
        if poll_result == PollResult::SocketStateChanged
            || self.iface.poll_delay(now, &self.sockets).map(Duration::from) != poll_delay_before_poll
            || self.device.received_frame_count() < received_frames_before_poll
            || self.device.transmitted_frame_count() > transmitted_frames_before_poll
        {
            drive.gateway_poll_progress();
        }
        self.drain_transmitted_frames(guest_frames, drive);
    }

    fn has_poll_work(&mut self) -> bool {
        if self.device.has_received_frames() || self.device.has_transmitted_frames() {
            return true;
        }
        self.iface
            .poll_delay(self.clock.smoltcp_now(), &self.sockets)
            .is_some_and(|delay| Duration::from(delay).is_zero())
    }

    fn drain_transmitted_frames(&mut self, guest_frames: &mut Vec<FrameBuf>, drive: &mut DriveTurn<'_>) {
        let mut frame_count = 0_usize;
        while let Some(frame_len) = self.device.next_transmitted_frame_len() {
            let reservation = match drive.enqueue_guest_frame(guest_frames.len(), guest_frames.capacity(), frame_len) {
                crate::drive::DriveGuestFrameEnqueue::Reserved(reservation) => reservation,
                crate::drive::DriveGuestFrameEnqueue::Blocked => break,
            };
            let Some(frame) = self.device.pop_transmitted_frame() else {
                break;
            };
            reservation.push_vec(guest_frames, frame);
            frame_count = frame_count.saturating_add(1);
        }
        if self.device.has_transmitted_frames() {
            drive.wait_for_guest_send_capacity();
        }
        drive.gateway_frames_transmitted(frame_count);
    }

    pub(crate) const fn tcp_sockets(&self) -> &SocketSet<'static> {
        &self.sockets
    }

    pub(crate) const fn tcp_sockets_mut(&mut self) -> &mut SocketSet<'static> {
        &mut self.sockets
    }

    pub(crate) fn ingress_tcp_guest_send_blocked(&self, tcp: &TcpConnections, output: &mut Vec<HostConnectionId>) {
        tcp.guest_send_blocked_connections(&self.sockets, output);
    }

    pub(crate) fn relay_ingress_tcp_guest_bytes(
        &mut self,
        tcp: &mut TcpConnections,
        outputs: &mut Vec<IngressTcpOutput>,
        guest_frames: &mut Vec<FrameBuf>,
        drive: &mut DriveTurn<'_>,
    ) {
        tcp.relay_guest_bytes(&mut self.sockets, outputs, &self.buffers, drive);
        self.poll(guest_frames, drive);
    }

    pub(crate) fn tcp_egress_route(&mut self, dst: SocketAddr) -> (SocketAddr, TcpEgressRoute) {
        let route = tcp_egress_route(dst, &mut self.dns, &mut self.config, &self.clock);
        let dst = resolve_gateway_destination(dst, self.config.gateway);
        (dst, route)
    }

    pub(crate) fn ingest_guest_frame<R: ReactorBackend>(
        &mut self,
        tcp: &mut TcpProxies<R>,
        udp: &UdpPeers,
        frame: FrameBuf,
        outputs: &mut GatewayOutputs<'_>,
        drive: &mut DriveTurn<'_>,
    ) -> GuestFrameIngest {
        if let Some(datagram) = UdpDatagram::from_frame(frame.as_slice(), &self.buffers) {
            match udp.host_port_response(datagram.dst, IpAddr::V4(self.config.gateway), datagram.payload) {
                Ok(send) => {
                    if outputs.push_ingress_udp(send, drive).is_err() {
                        return GuestFrameIngest::Blocked(frame);
                    }
                }
                Err(payload) => {
                    if !self.handle_guest_udp(datagram.src, datagram.dst, payload.as_slice(), outputs, drive) {
                        return GuestFrameIngest::Blocked(frame);
                    }
                }
            }
            return GuestFrameIngest::Queued;
        }
        if let Some(syn) = TcpSyn::from_frame(frame.as_slice())
            && self.allows_tcp_syn(&syn)
            && !tcp.has_connection(syn.src, syn.dst)
        {
            if !self.device.can_receive_frame() {
                self.poll(outputs.guest_frames, drive);
            }
            if !self.device.can_receive_frame() {
                drive.wait_for_local_buffer_capacity();
                return GuestFrameIngest::Blocked(frame);
            }
            if !tcp.listen(syn.src, syn.dst, &mut self.sockets) {
                return GuestFrameIngest::Dropped;
            }
        }

        if !self.device.can_receive_frame() {
            self.poll(outputs.guest_frames, drive);
        }
        if !self.device.can_receive_frame() {
            drive.wait_for_local_buffer_capacity();
            return GuestFrameIngest::Blocked(frame);
        }
        let queued = self.device.receive_frame(frame);
        debug_assert!(queued);
        self.poll(outputs.guest_frames, drive);
        GuestFrameIngest::Queued
    }

    pub(crate) fn write_udp_response(
        &mut self,
        proxy: UdpProxyKey,
        bytes: &ByteBuf,
        is_dns: bool,
        guest_frames: &mut Vec<FrameBuf>,
        drive: &mut DriveTurn<'_>,
    ) -> UdpFrameWrite {
        let Some(frame) = udp_response_frame(proxy, bytes.as_slice(), self.config.mac, &self.buffers) else {
            drive.wait_for_local_buffer_capacity();
            return UdpFrameWrite::Blocked;
        };
        if drive.push_guest_frame(guest_frames, frame).is_err() {
            return UdpFrameWrite::Blocked;
        }
        if is_dns {
            self.dns.record_response(bytes.as_slice(), &self.clock);
        }
        self.poll(guest_frames, drive);
        UdpFrameWrite::Queued
    }

    pub(crate) fn write_ingress_tcp(
        &mut self,
        tcp: &mut TcpConnections,
        connection: HostConnectionId,
        bytes: ByteBuf,
        guest_frames: &mut Vec<FrameBuf>,
        drive: &mut DriveTurn<'_>,
    ) {
        tcp.write_peer_bytes(connection, bytes, &mut self.sockets, drive);
        self.poll(guest_frames, drive);
    }

    pub(crate) fn accept_ingress_tcp(
        &mut self,
        tcp: &mut TcpConnections,
        port: u16,
        connection: HostConnectionId,
        ingress_tcp_outputs: &mut Vec<IngressTcpOutput>,
        guest_frames: &mut Vec<FrameBuf>,
        drive: &mut DriveTurn<'_>,
    ) -> bool {
        if !tcp.connect(
            connection,
            self.config.guest,
            port,
            |mut socket, guest, port, local_port| {
                if socket
                    .connect(self.iface.context(), (IpAddress::Ipv4(guest), port), local_port)
                    .is_err()
                {
                    return None;
                }
                Some(self.sockets.add(socket))
            },
        ) {
            return drive
                .push_component_output(ingress_tcp_outputs, IngressTcpOutput::Close { connection })
                .is_ok();
        }
        self.poll(guest_frames, drive);
        true
    }

    pub(crate) fn close_ingress_tcp(
        &mut self,
        tcp: &mut TcpConnections,
        connection: HostConnectionId,
        guest_frames: &mut Vec<FrameBuf>,
        drive: &mut DriveTurn<'_>,
    ) {
        tcp.close(connection, &mut self.sockets);
        self.poll(guest_frames, drive);
    }

    pub(crate) fn ingest_ingress_udp_datagram(
        &mut self,
        udp: &mut UdpPeers,
        port: u16,
        peer: SocketAddr,
        bytes: &ByteBuf,
        guest_frames: &mut Vec<FrameBuf>,
        drive: &mut DriveTurn<'_>,
    ) -> UdpFrameWrite {
        if !udp.can_open_host_port(port) {
            return UdpFrameWrite::Dropped;
        }
        let Some(frame) = udp_frame(
            SocketAddr::new(IpAddr::V4(self.config.gateway), port),
            SocketAddr::new(IpAddr::V4(self.config.guest), port),
            bytes.as_slice(),
            self.config.mac.gateway,
            self.config.mac.guest,
            &self.buffers,
        ) else {
            drive.wait_for_local_buffer_capacity();
            return UdpFrameWrite::Blocked;
        };
        if drive.push_guest_frame(guest_frames, frame).is_err() {
            return UdpFrameWrite::Blocked;
        }
        let opened = udp.open_host_port(port, peer);
        debug_assert!(opened);
        if !opened {
            return UdpFrameWrite::Dropped;
        }
        self.poll(guest_frames, drive);
        UdpFrameWrite::Queued
    }

    fn handle_guest_udp(
        &mut self,
        src: SocketAddr,
        dst: SocketAddr,
        payload: &[u8],
        outputs: &mut GatewayOutputs<'_>,
        drive: &mut DriveTurn<'_>,
    ) -> bool {
        let protocol = application::classify_udp_datagram(payload);
        let is_dns = self.is_dns_udp(dst) || matches!(protocol, ApplicationProtocol::Dns);
        let bytes = match prepare_udp_payload(&self.config.policy, protocol, payload, &self.buffers) {
            Ok(bytes) => bytes,
            Err(_error) => return true,
        };
        let guest_dst = dst;
        let dst = udp_host_destination(dst, &self.config);
        if self.config.policy.egress.check_destination(dst.ip()).is_err() {
            return true;
        }
        if !is_dns
            && !self
                .dns
                .has_allowed_authority_for_ip(dst.ip(), &self.config.policy.egress, &self.clock)
        {
            return true;
        }
        let proxy = UdpProxyKey {
            guest_src: src,
            guest_dst,
            host_dst: dst,
        };
        if outputs
            .push_egress_udp(EgressUdpSend { proxy, bytes, is_dns }, drive)
            .is_err()
        {
            return false;
        }
        if is_dns {
            self.dns.record_query(payload);
        }
        true
    }

    fn is_dns_udp(&self, dst: SocketAddr) -> bool {
        dst.ip() == IpAddr::V4(self.config.gateway) && dst.port() == 53
    }

    fn allows_tcp_syn(&mut self, syn: &TcpSyn) -> bool {
        if syn.dst.ip() == IpAddr::V4(self.config.gateway) && syn.dst.port() == 53 {
            return true;
        }
        self.config.policy.egress.check_destination(syn.dst.ip()).is_ok()
            && self
                .dns
                .has_allowed_authority_for_ip(syn.dst.ip(), &self.config.policy.egress, &self.clock)
    }
}

fn udp_host_destination(dst: SocketAddr, config: &GatewayConfig) -> SocketAddr {
    if dst.ip() == IpAddr::V4(config.gateway) && dst.port() == 53 {
        return config.dns_upstream;
    }
    resolve_gateway_destination(dst, config.gateway)
}

fn prepare_udp_payload(
    policy: &NetworkPolicy,
    protocol: ApplicationProtocol,
    payload: &[u8],
    buffers: &BufferPool,
) -> std::io::Result<ByteBuf> {
    if matches!(protocol, ApplicationProtocol::Quic | ApplicationProtocol::Http3) && !policy.secrets.is_empty() {
        return Err(std::io::Error::other(
            "QUIC/HTTP3 UDP egress blocked while secrets are configured",
        ));
    }
    if matches!(protocol, ApplicationProtocol::Dns) {
        let mut output = buffers
            .try_byte_with_capacity(payload.len())
            .map_err(std::io::Error::other)?;
        output.extend_from_slice(payload);
        return Ok(output);
    }
    let mut output = buffers
        .try_byte_with_capacity(payload.len())
        .map_err(std::io::Error::other)?;
    application::process(payload, output.as_mut_vec())?;
    Ok(output)
}

struct GatewayConfig {
    policy: NetworkPolicy,
    mac: InstanceMacAddresses,
    gateway: Ipv4Addr,
    guest: Ipv4Addr,
    dns_upstream: SocketAddr,
    tls: Option<TlsIntercept>,
    idle_poll_delay: Duration,
}

fn tcp_egress_route(
    dst: SocketAddr,
    dns: &mut DnsAttribution,
    config: &mut GatewayConfig,
    clock: &impl NetworkClock,
) -> TcpEgressRoute {
    if dst.ip() == IpAddr::V4(config.gateway) && dst.port() == 53 {
        return TcpEgressRoute::Dns {
            upstream: config.dns_upstream,
        };
    }
    if let Some(tls) = &mut config.tls
        && tls.intercepts_port(dst.port())
    {
        let upstream = resolve_gateway_destination(dst, config.gateway);
        let authorities = dns
            .hosts_for_ip(dst.ip(), clock)
            .into_iter()
            .map(Authority::new)
            .collect();
        return tls
            .tls_egress_policy(upstream, authorities, &config.policy, clock)
            .map_or_else(
                |_error| {
                    TcpEgressRoute::Plain(TcpEgressPolicy {
                        decision: EgressDecision {
                            application: ApplicationPolicy::Block {
                                reason: BlockReason::TlsInterceptUnavailable,
                            },
                        },
                        reject_secret_placeholders: !config.policy.secrets.is_empty(),
                    })
                },
                TcpEgressRoute::Tls,
            );
    }
    TcpEgressRoute::Plain(TcpEgressPolicy {
        decision: EgressDecision {
            application: ApplicationPolicy::Raw,
        },
        reject_secret_placeholders: !config.policy.secrets.is_empty(),
    })
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use smoltcp::wire::{
        ETHERNET_HEADER_LEN, EthernetFrame, EthernetProtocol, IPV4_HEADER_LEN, IpAddress, IpProtocol, Ipv4Packet,
        Ipv4Repr, TCP_HEADER_LEN, TcpControl, TcpPacket, TcpRepr, TcpSeqNumber,
    };

    use crate::RuntimeSecret;
    use crate::buffers::{BufferPool, FrameBuf};
    use crate::clock::SystemClock;
    use crate::drive::{DriveBudget, DriveReport, DriveTurn};
    use crate::ingress::{TcpConnections, UdpPeers};
    use crate::network::{
        ApplicationPolicy, EgressDecision, EgressUdpSend, HostConnectionId, IngressTcpOutput, IngressUdpSend,
        InstanceAddresses, InstanceMacAddresses, InstanceNetworkConfig, MacAddress, NetworkLimits,
    };
    use crate::policy::{EgressPolicy, NetworkPolicy, RuntimeSecrets};
    use crate::test_support::unit::{dns_a_response, dns_query};

    use super::dns::DnsAttribution;
    use super::packet::{UdpDatagram, resolve_gateway_destination, udp_frame, udp_response_frame};
    use super::{
        Gateway, GatewayConfig, GatewayOutputs, GuestFrameIngest, TcpEgressPolicy, TcpEgressRoute, UdpFrameWrite,
        UdpProxyKey, prepare_udp_payload, tcp_egress_route, udp_host_destination,
    };
    use crate::application::ApplicationProtocol;
    use crate::egress::tcp::{TcpProxies, tcp_listen_endpoint};
    use crate::reactor::MioReactor;

    const TEST_MAC: InstanceMacAddresses = InstanceMacAddresses {
        gateway: MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]),
        guest: MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x02]),
    };

    const TEST_ADDRESSES: InstanceAddresses = InstanceAddresses {
        gateway: crate::network::Ipv4AddressText(smoltcp::wire::Ipv4Address::new(10, 73, 0, 1)),
        address: crate::network::Ipv4AddressText(smoltcp::wire::Ipv4Address::new(10, 73, 0, 10)),
        cidr_prefix: 24,
    };

    fn test_buffers() -> BufferPool {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        buffers
    }

    fn test_guest_frames(limits: &NetworkLimits) -> Vec<FrameBuf> {
        Vec::with_capacity(limits.component_output_batch_capacity)
    }

    fn test_egress_udp_sends(limits: &NetworkLimits) -> Vec<EgressUdpSend> {
        Vec::with_capacity(limits.component_output_batch_capacity)
    }

    fn test_ingress_udp_sends(limits: &NetworkLimits) -> Vec<IngressUdpSend> {
        Vec::with_capacity(limits.component_output_batch_capacity)
    }

    fn test_ingress_tcp_outputs(limits: &NetworkLimits) -> Vec<IngressTcpOutput> {
        Vec::with_capacity(limits.component_output_batch_capacity)
    }

    fn test_bytes(buffers: &BufferPool, bytes: &[u8]) -> crate::buffers::ByteBuf {
        let mut output = buffers
            .try_byte_with_capacity(bytes.len())
            .expect("prewarmed byte buffer");
        output.extend_from_slice(bytes);
        output
    }

    #[test]
    fn dns_attribution_expires_hosts_and_honors_authority_policy() {
        let mut dns = DnsAttribution::new(8);
        let clock = SystemClock;
        let addr = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));
        let policy = EgressPolicy::allow_all().with_allowed_authority("allowed.test");

        assert!(!dns.has_allowed_authority_for_ip(addr, &policy, &clock));
        dns.record("Allowed.TEST.", vec![addr], Duration::from_mins(1), &clock);

        assert!(dns.has_allowed_authority_for_ip(addr, &policy, &clock));
        assert!(!dns.has_allowed_authority_for_ip(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 8)), &policy, &clock));
    }

    #[test]
    fn udp_frame_roundtrips_to_datagram_and_response() -> Result<(), Box<dyn std::error::Error>> {
        let buffers = test_buffers();
        let frame = udp_frame(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 10)), 40_000),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 1)), 53),
            b"query",
            TEST_MAC.guest,
            TEST_MAC.gateway,
            &buffers,
        );
        let frame = frame.ok_or("expected UDP frame")?;
        let datagram = UdpDatagram::from_frame(frame.as_slice(), &buffers).ok_or("expected UDP datagram")?;

        assert_eq!(
            datagram.src,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 10)), 40_000)
        );
        assert_eq!(
            datagram.dst,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 1)), 53)
        );
        assert_eq!(datagram.payload.as_slice(), b"query");

        let response = udp_response_frame(
            UdpProxyKey {
                guest_src: datagram.src,
                guest_dst: datagram.dst,
                host_dst: datagram.dst,
            },
            b"response",
            TEST_MAC,
            &buffers,
        );
        let response = response.ok_or("expected UDP response frame")?;
        assert!(UdpDatagram::from_frame(response.as_slice(), &buffers).is_some());

        Ok(())
    }

    #[test]
    fn udp_host_port_response_routes_to_registered_peer() -> Result<(), Box<dyn std::error::Error>> {
        let buffers = test_buffers();
        let frame = udp_frame(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 10)), 40_000),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 1)), 8080),
            b"payload",
            TEST_MAC.guest,
            TEST_MAC.gateway,
            &buffers,
        );
        let frame = frame.ok_or("expected UDP frame")?;
        let datagram = UdpDatagram::from_frame(frame.as_slice(), &buffers).ok_or("expected UDP datagram")?;
        let mut peers = UdpPeers::new(1);
        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999);

        assert!(
            peers
                .host_port_response(datagram.dst, IpAddr::V4(Ipv4Addr::new(10, 73, 0, 1)), datagram.payload)
                .is_err()
        );

        let datagram = UdpDatagram::from_frame(frame.as_slice(), &buffers).ok_or("expected UDP datagram")?;
        assert!(peers.open_host_port(8080, peer));
        let send =
            match peers.host_port_response(datagram.dst, IpAddr::V4(Ipv4Addr::new(10, 73, 0, 1)), datagram.payload) {
                Ok(send) => send,
                Err(_datagram) => return Err("expected host UDP operation".into()),
            };

        assert_eq!(send.port, 8080);
        assert_eq!(send.peer, peer);
        assert_eq!(send.bytes.as_slice(), b"payload");
        Ok(())
    }

    #[test]
    fn udp_payload_blocks_quic_when_secrets_are_configured() {
        let buffers = test_buffers();
        let mut secrets = RuntimeSecrets::new();
        secrets.insert(RuntimeSecret::new(
            "AGENTDP_SECRET_TOKEN",
            "value",
            ["allowed.test".to_owned()],
        ));
        let policy = NetworkPolicy::new(EgressPolicy::allow_all()).with_secrets(secrets);

        assert!(prepare_udp_payload(&policy, ApplicationProtocol::Quic, &[0xc0], &buffers).is_err());
        let dns = prepare_udp_payload(&policy, ApplicationProtocol::Dns, b"dns", &buffers);
        assert!(dns.is_ok());
    }

    #[test]
    fn gateway_destinations_and_ephemeral_ports_are_normalized() {
        let gateway = Ipv4Addr::new(10, 73, 0, 1);
        let dns = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53);
        let config = GatewayConfig {
            policy: NetworkPolicy::new(EgressPolicy::allow_all()),
            mac: TEST_MAC,
            gateway,
            guest: Ipv4Addr::new(10, 73, 0, 10),
            dns_upstream: dns,
            tls: None,
            idle_poll_delay: Duration::from_secs(1),
        };

        assert_eq!(
            resolve_gateway_destination(SocketAddr::new(IpAddr::V4(gateway), 443), gateway),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443)
        );
        assert_eq!(
            udp_host_destination(SocketAddr::new(IpAddr::V4(gateway), 53), &config),
            dns
        );
        assert!(tcp_listen_endpoint(SocketAddr::new(IpAddr::V4(gateway), 80)).is_some());
        assert!(tcp_listen_endpoint(SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST), 80)).is_none());
    }

    #[test]
    fn tcp_egress_route_uses_dns_and_plain_defaults() {
        let mut dns = DnsAttribution::new(8);
        let clock = SystemClock;
        let mut config = GatewayConfig {
            policy: NetworkPolicy::new(EgressPolicy::allow_all()),
            mac: TEST_MAC,
            gateway: Ipv4Addr::new(10, 73, 0, 1),
            guest: Ipv4Addr::new(10, 73, 0, 10),
            dns_upstream: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53),
            tls: None,
            idle_poll_delay: Duration::from_secs(1),
        };

        assert!(matches!(
            tcp_egress_route(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 1)), 53),
                &mut dns,
                &mut config,
                &clock,
            ),
            TcpEgressRoute::Dns { .. }
        ));
        assert!(matches!(
            tcp_egress_route(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)), 80),
                &mut dns,
                &mut config,
                &clock,
            ),
            TcpEgressRoute::Plain(TcpEgressPolicy {
                decision: EgressDecision {
                    application: ApplicationPolicy::Raw
                },
                reject_secret_placeholders: false,
            })
        ));
    }

    #[test]
    fn gateway_handles_ingress_udp_datagram() {
        let buffers = test_buffers();
        let config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let bytes = test_bytes(&buffers, b"ping");
        let mut guest_frames = test_guest_frames(&config.limits);

        handle_ingress_udp_datagram(
            &mut gateway,
            &mut guest_frames,
            8080,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999),
            &bytes,
        );

        assert_eq!(guest_frames.len(), 1);
    }

    #[test]
    fn gateway_ingress_tcp_connect_does_not_finish_host_write_before_guest_fin() {
        let buffers = test_buffers();
        let config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        let mut gateway = Gateway::new(&config, buffers, SystemClock);
        let mut tcp = TcpConnections::new(
            config.limits.ingress_tcp_connection_limit,
            config.limits.tcp_socket_buffer_capacity,
        );
        let mut outputs = test_ingress_tcp_outputs(&config.limits);
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let connection = HostConnectionId(22);

        gateway.accept_ingress_tcp(&mut tcp, 22, connection, &mut outputs, &mut guest_frames, &mut drive);
        gateway.relay_ingress_tcp_guest_bytes(&mut tcp, &mut outputs, &mut guest_frames, &mut drive);

        assert!(
            !outputs.iter().any(
                |output| matches!(output, IngressTcpOutput::FinishWrite { connection: output } if *output == connection)
            ),
            "guest-to-host half-close must wait for guest FIN, not pre-established TCP state"
        );
    }

    #[test]
    fn gateway_reports_udp_response_frame_backpressure() {
        let limits = NetworkLimits {
            frame_buffer_pool_capacity: 0,
            ..NetworkLimits::default()
        };
        let buffers = BufferPool::new(limits.clone());
        buffers.prewarm_instance_network();
        let resolved = Ipv4Addr::new(203, 0, 113, 7);
        let mut config = InstanceNetworkConfig::new(
            TEST_ADDRESSES,
            TEST_MAC,
            EgressPolicy::allow_all().with_allowed_authority("allowed.test"),
        );
        config.limits = limits;
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let query = dns_query(0x1201, "allowed.test", 1);
        gateway.dns.record_query(&query);
        let response = dns_a_response(0x1201, "allowed.test", resolved, 60);
        let bytes = test_bytes(&buffers, &response);
        let proxy = UdpProxyKey {
            guest_src: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 10)), 40_000),
            guest_dst: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53),
            host_dst: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 53),
        };
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        let result = gateway.write_udp_response(proxy, &bytes, true, &mut guest_frames, &mut drive);

        assert_eq!(result, UdpFrameWrite::Blocked);
        assert!(guest_frames.is_empty());
        assert!(report.wait().contains(crate::drive::DriveWait::LOCAL_BUFFER_CAPACITY));
        assert!(
            !gateway
                .dns
                .has_allowed_authority_for_ip(IpAddr::V4(resolved), &config.policy.egress, &SystemClock,)
        );
    }

    #[test]
    fn gateway_udp_response_budget_block_does_not_commit_dns_attribution() {
        let limits = NetworkLimits {
            drive_byte_budget: 4,
            ..NetworkLimits::default()
        };
        let buffers = BufferPool::new(limits.clone());
        buffers.prewarm_instance_network();
        let resolved = Ipv4Addr::new(203, 0, 113, 7);
        let mut config = InstanceNetworkConfig::new(
            TEST_ADDRESSES,
            TEST_MAC,
            EgressPolicy::allow_all().with_allowed_authority("allowed.test"),
        );
        config.limits = limits;
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let query = dns_query(0x1201, "allowed.test", 1);
        gateway.dns.record_query(&query);
        let response = dns_a_response(0x1201, "allowed.test", resolved, 60);
        let bytes = test_bytes(&buffers, &response);
        let proxy = UdpProxyKey {
            guest_src: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 10)), 40_000),
            guest_dst: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53),
            host_dst: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 53),
        };
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        let result = gateway.write_udp_response(proxy, &bytes, true, &mut guest_frames, &mut drive);

        assert_eq!(result, UdpFrameWrite::Blocked);
        assert!(guest_frames.is_empty());
        assert!(report.budget_exhausted());
        assert!(
            !gateway
                .dns
                .has_allowed_authority_for_ip(IpAddr::V4(resolved), &config.policy.egress, &SystemClock,)
        );
    }

    #[test]
    fn gateway_udp_response_frame_write_reports_progress() {
        let buffers = test_buffers();
        let config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let bytes = test_bytes(&buffers, b"response");
        let proxy = UdpProxyKey {
            guest_src: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 10)), 40_000),
            guest_dst: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53),
            host_dst: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 53),
        };
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        let result = gateway.write_udp_response(proxy, &bytes, false, &mut guest_frames, &mut drive);

        assert_eq!(result, UdpFrameWrite::Queued);
        assert_eq!(guest_frames.len(), 1);
        assert!(report.made_progress());
    }

    #[test]
    fn blocked_ingress_udp_frame_does_not_commit_peer_mapping() {
        let limits = NetworkLimits {
            frame_buffer_pool_capacity: 0,
            ..NetworkLimits::default()
        };
        let buffers = BufferPool::new(limits.clone());
        buffers.prewarm_instance_network();
        let mut config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        config.limits = limits;
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let mut udp = UdpPeers::new(config.limits.ingress_udp_peer_limit);
        let bytes = test_bytes(&buffers, b"ingress");
        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999);
        let port = 8080;
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        let result = gateway.ingest_ingress_udp_datagram(&mut udp, port, peer, &bytes, &mut guest_frames, &mut drive);

        assert_eq!(result, UdpFrameWrite::Blocked);
        assert!(!report.made_progress());
        assert!(guest_frames.is_empty());
        let response = test_bytes(&buffers, b"response");
        assert!(
            udp.host_port_response(
                SocketAddr::new(IpAddr::V4(TEST_ADDRESSES.gateway.std()), port),
                IpAddr::V4(TEST_ADDRESSES.gateway.std()),
                response,
            )
            .is_err()
        );
    }

    #[test]
    fn blocked_ingress_udp_frame_budget_does_not_commit_peer_mapping() {
        let limits = NetworkLimits {
            drive_byte_budget: 4,
            ..NetworkLimits::default()
        };
        let buffers = BufferPool::new(limits.clone());
        buffers.prewarm_instance_network();
        let mut config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        config.limits = limits;
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let mut udp = UdpPeers::new(config.limits.ingress_udp_peer_limit);
        let bytes = test_bytes(&buffers, b"ingress");
        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999);
        let port = 8080;
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        let result = gateway.ingest_ingress_udp_datagram(&mut udp, port, peer, &bytes, &mut guest_frames, &mut drive);

        assert_eq!(result, UdpFrameWrite::Blocked);
        assert!(report.budget_exhausted());
        assert!(guest_frames.is_empty());
        let response = test_bytes(&buffers, b"response");
        assert!(
            udp.host_port_response(
                SocketAddr::new(IpAddr::V4(TEST_ADDRESSES.gateway.std()), port),
                IpAddr::V4(TEST_ADDRESSES.gateway.std()),
                response,
            )
            .is_err()
        );
    }

    #[test]
    fn ingress_udp_peer_capacity_is_checked_before_frame_capacity() {
        let limits = NetworkLimits {
            frame_buffer_pool_capacity: 0,
            ingress_udp_peer_limit: 0,
            ..NetworkLimits::default()
        };
        let buffers = BufferPool::new(limits.clone());
        buffers.prewarm_instance_network();
        let mut config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        config.limits = limits;
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let mut udp = UdpPeers::new(config.limits.ingress_udp_peer_limit);
        let bytes = test_bytes(&buffers, b"ingress");
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        let result = gateway.ingest_ingress_udp_datagram(
            &mut udp,
            8080,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999),
            &bytes,
            &mut guest_frames,
            &mut drive,
        );

        assert_eq!(result, UdpFrameWrite::Dropped);
        assert!(!report.wait().contains(crate::drive::DriveWait::LOCAL_BUFFER_CAPACITY));
        assert!(guest_frames.is_empty());
    }

    #[test]
    fn gateway_drops_new_ingress_udp_peers_after_limit() {
        let buffers = test_buffers();
        let mut config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        config.limits = NetworkLimits {
            ingress_udp_peer_limit: 0,
            ..NetworkLimits::default()
        };
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let bytes = test_bytes(&buffers, b"ping");
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut udp = UdpPeers::new(config.limits.ingress_udp_peer_limit);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        let result = gateway.ingest_ingress_udp_datagram(
            &mut udp,
            8080,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999),
            &bytes,
            &mut guest_frames,
            &mut drive,
        );

        assert_eq!(result, UdpFrameWrite::Dropped);
        assert!(guest_frames.is_empty());
    }

    #[test]
    fn gateway_drops_allowed_tcp_syn_when_listener_capacity_is_exhausted() {
        let buffers = test_buffers();
        let mut config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        config.limits = NetworkLimits {
            tcp_proxy_limit: 0,
            ..NetworkLimits::default()
        };
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let mut tcp = test_tcp(&config.limits, &buffers);
        let udp = UdpPeers::new(config.limits.ingress_udp_peer_limit);
        let mut egress_udp_sends = test_egress_udp_sends(&config.limits);
        let mut ingress_udp_sends = test_ingress_udp_sends(&config.limits);
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut outputs = GatewayOutputs::new(&mut egress_udp_sends, &mut ingress_udp_sends, &mut guest_frames);
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let src_addr = Ipv4Addr::new(10, 73, 0, 10);
        let src = SocketAddr::new(IpAddr::V4(src_addr), 40_001);
        let dst_addr = Ipv4Addr::new(203, 0, 113, 7);
        let dst = SocketAddr::new(IpAddr::V4(dst_addr), 443);

        gateway.ingest_guest_frame(
            &mut tcp,
            &udp,
            tcp_syn_frame(src_addr, src.port(), dst_addr, dst.port(), &buffers),
            &mut outputs,
            &mut drive,
        );

        assert!(!tcp.has_connection(src, dst));
        assert!(egress_udp_sends.is_empty());
        assert!(ingress_udp_sends.is_empty());
        assert!(guest_frames.is_empty());
    }

    #[test]
    fn gateway_returns_blocked_guest_tcp_frame_without_opening_proxy() {
        let limits = NetworkLimits {
            frame_device_queue_capacity: 0,
            ..NetworkLimits::default()
        };
        let buffers = BufferPool::new(limits.clone());
        buffers.prewarm_instance_network();
        let mut config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        config.limits = limits;
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let mut tcp = test_tcp(&config.limits, &buffers);
        let udp = UdpPeers::new(config.limits.ingress_udp_peer_limit);
        let mut egress_udp_sends = test_egress_udp_sends(&config.limits);
        let mut ingress_udp_sends = test_ingress_udp_sends(&config.limits);
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut outputs = GatewayOutputs::new(&mut egress_udp_sends, &mut ingress_udp_sends, &mut guest_frames);
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let src_addr = Ipv4Addr::new(10, 73, 0, 10);
        let src = SocketAddr::new(IpAddr::V4(src_addr), 40_001);
        let dst_addr = Ipv4Addr::new(203, 0, 113, 7);
        let dst = SocketAddr::new(IpAddr::V4(dst_addr), 443);
        let frame = tcp_syn_frame(src_addr, src.port(), dst_addr, dst.port(), &buffers);
        let frame_len = frame.len();

        let result = gateway.ingest_guest_frame(&mut tcp, &udp, frame, &mut outputs, &mut drive);

        let GuestFrameIngest::Blocked(frame) = result else {
            panic!("expected blocked guest frame");
        };
        assert_eq!(frame.len(), frame_len);
        assert!(!tcp.has_connection(src, dst));
        assert!(egress_udp_sends.is_empty());
        assert!(ingress_udp_sends.is_empty());
        assert!(guest_frames.is_empty());
        assert!(report.wait().contains(crate::drive::DriveWait::LOCAL_BUFFER_CAPACITY));
    }

    #[test]
    fn gateway_polls_existing_guest_frame_before_blocking_next_frame() {
        let limits = NetworkLimits {
            frame_device_queue_capacity: 1,
            ..NetworkLimits::default()
        };
        let buffers = BufferPool::new(limits.clone());
        buffers.prewarm_instance_network();
        let mut config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        config.limits = limits;
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let mut tcp = test_tcp(&config.limits, &buffers);
        let udp = UdpPeers::new(config.limits.ingress_udp_peer_limit);
        let mut egress_udp_sends = test_egress_udp_sends(&config.limits);
        let mut ingress_udp_sends = test_ingress_udp_sends(&config.limits);
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut outputs = GatewayOutputs::new(&mut egress_udp_sends, &mut ingress_udp_sends, &mut guest_frames);
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let stale_src = Ipv4Addr::new(10, 73, 0, 10);
        let stale_dst = Ipv4Addr::new(203, 0, 113, 7);
        let next_src = Ipv4Addr::new(10, 73, 0, 10);
        let next_dst = Ipv4Addr::new(203, 0, 113, 8);
        let next = SocketAddr::new(IpAddr::V4(next_src), 40_002);
        let next_dst_socket = SocketAddr::new(IpAddr::V4(next_dst), 443);

        assert!(
            gateway
                .device
                .receive_frame(tcp_syn_frame(stale_src, 40_001, stale_dst, 443, &buffers))
        );

        let result = gateway.ingest_guest_frame(
            &mut tcp,
            &udp,
            tcp_syn_frame(next_src, next.port(), next_dst, next_dst_socket.port(), &buffers),
            &mut outputs,
            &mut drive,
        );

        assert!(matches!(result, GuestFrameIngest::Queued));
        assert!(tcp.has_connection(next, next_dst_socket));
        assert!(!report.wait().contains(crate::drive::DriveWait::LOCAL_BUFFER_CAPACITY));
    }

    #[test]
    fn gateway_poll_reports_progress_when_rx_frame_is_consumed_without_socket_change() {
        let buffers = test_buffers();
        let config = InstanceNetworkConfig::new(TEST_ADDRESSES, TEST_MAC, EgressPolicy::allow_all());
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let mut frame = buffers.try_frame().expect("prewarmed frame");
        frame.as_mut_vec().extend_from_slice(b"invalid");
        assert!(gateway.device.receive_frame(frame));
        let mut guest_frames = test_guest_frames(&config.limits);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        gateway.poll(&mut guest_frames, &mut drive);

        assert!(report.made_progress());
        assert!(guest_frames.is_empty());
    }

    #[test]
    fn gateway_dns_attribution_allows_followup_udp_to_resolved_ip() -> Result<(), Box<dyn std::error::Error>> {
        let buffers = test_buffers();
        let resolved = Ipv4Addr::new(203, 0, 113, 7);
        let mut config = InstanceNetworkConfig::new(
            TEST_ADDRESSES,
            TEST_MAC,
            EgressPolicy::allow_all().with_allowed_authority("allowed.test"),
        );
        config.dns_upstream = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5300);
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let mut egress_udp_sends = test_egress_udp_sends(&config.limits);
        let mut ingress_udp_sends = test_ingress_udp_sends(&config.limits);
        let mut guest_frames = test_guest_frames(&config.limits);
        let query = dns_query(0x1201, "allowed.test", 1);

        handle_guest_frame(
            &mut gateway,
            &mut egress_udp_sends,
            &mut ingress_udp_sends,
            &mut guest_frames,
            udp_frame(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 10)), 40_001),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 1)), 53),
                &query,
                TEST_MAC.guest,
                TEST_MAC.gateway,
                &buffers,
            )
            .expect("prewarmed UDP frame"),
        );

        let proxy = match egress_udp_sends.pop() {
            Some(EgressUdpSend { proxy, bytes, is_dns }) => {
                assert!(is_dns);
                assert_eq!(proxy.host_dst, config.dns_upstream);
                assert_eq!(bytes.as_slice(), query.as_slice());
                proxy
            }
            _ => return Err("expected DNS UDP datagram operation".into()),
        };
        assert!(egress_udp_sends.is_empty());
        assert!(ingress_udp_sends.is_empty());
        assert!(guest_frames.is_empty());

        let response = dns_a_response(0x1201, "allowed.test", resolved, 60);
        let response_bytes = test_bytes(&buffers, &response);
        let mut budget = DriveBudget::event_loop(&config.limits);
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        gateway.write_udp_response(proxy, &response_bytes, true, &mut guest_frames, &mut drive);
        assert!(egress_udp_sends.is_empty());
        assert!(ingress_udp_sends.is_empty());
        assert_eq!(guest_frames.len(), 1);
        guest_frames.clear();

        handle_guest_frame(
            &mut gateway,
            &mut egress_udp_sends,
            &mut ingress_udp_sends,
            &mut guest_frames,
            udp_frame(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 10)), 40_002),
                SocketAddr::new(IpAddr::V4(resolved), 443),
                b"payload",
                TEST_MAC.guest,
                TEST_MAC.gateway,
                &buffers,
            )
            .expect("prewarmed UDP frame"),
        );
        match egress_udp_sends.pop() {
            Some(EgressUdpSend { proxy, bytes, is_dns }) => {
                assert!(!is_dns);
                assert_eq!(proxy.host_dst, SocketAddr::new(IpAddr::V4(resolved), 443));
                assert_eq!(bytes.as_slice(), b"payload");
            }
            _ => return Err("expected attributed UDP datagram operation".into()),
        }
        assert!(egress_udp_sends.is_empty());
        assert!(ingress_udp_sends.is_empty());
        assert!(guest_frames.is_empty());
        Ok(())
    }

    #[test]
    fn gateway_blocks_restricted_udp_without_dns_attribution() {
        let buffers = test_buffers();
        let config = InstanceNetworkConfig::new(
            TEST_ADDRESSES,
            TEST_MAC,
            EgressPolicy::allow_all().with_allowed_authority("allowed.test"),
        );
        let mut gateway = Gateway::new(&config, buffers.clone(), SystemClock);
        let mut egress_udp_sends = test_egress_udp_sends(&config.limits);
        let mut ingress_udp_sends = test_ingress_udp_sends(&config.limits);
        let mut guest_frames = test_guest_frames(&config.limits);

        handle_guest_frame(
            &mut gateway,
            &mut egress_udp_sends,
            &mut ingress_udp_sends,
            &mut guest_frames,
            udp_frame(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 73, 0, 10)), 40_001),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), 443),
                b"payload",
                TEST_MAC.guest,
                TEST_MAC.gateway,
                &buffers,
            )
            .expect("prewarmed UDP frame"),
        );

        assert!(egress_udp_sends.is_empty());
        assert!(ingress_udp_sends.is_empty());
        assert!(guest_frames.is_empty());
    }

    fn handle_guest_frame(
        gateway: &mut Gateway<SystemClock>,
        egress_udp_sends: &mut Vec<EgressUdpSend>,
        ingress_udp_sends: &mut Vec<IngressUdpSend>,
        guest_frames: &mut Vec<FrameBuf>,
        frame: crate::FrameBuf,
    ) {
        let buffers = test_buffers();
        let mut tcp = test_tcp(&NetworkLimits::default(), &buffers);
        let udp = UdpPeers::new(NetworkLimits::default().ingress_udp_peer_limit);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut report = DriveReport::new();
        let mut outputs = GatewayOutputs::new(egress_udp_sends, ingress_udp_sends, guest_frames);
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        gateway.ingest_guest_frame(&mut tcp, &udp, frame, &mut outputs, &mut drive);
    }

    fn handle_ingress_udp_datagram(
        gateway: &mut Gateway<SystemClock>,
        guest_frames: &mut Vec<FrameBuf>,
        port: u16,
        peer: SocketAddr,
        bytes: &crate::buffers::ByteBuf,
    ) {
        let mut udp = UdpPeers::new(NetworkLimits::default().ingress_udp_peer_limit);
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let _result = gateway.ingest_ingress_udp_datagram(&mut udp, port, peer, bytes, guest_frames, &mut drive);
    }

    fn test_tcp(limits: &NetworkLimits, buffers: &BufferPool) -> TcpProxies<MioReactor> {
        TcpProxies::new(limits, buffers)
    }

    fn tcp_syn_frame(
        src_addr: Ipv4Addr,
        src_port: u16,
        dst_addr: Ipv4Addr,
        dst_port: u16,
        buffers: &BufferPool,
    ) -> FrameBuf {
        let tcp = TcpRepr {
            src_port,
            dst_port,
            control: TcpControl::Syn,
            seq_number: TcpSeqNumber(0x1234_5678),
            ack_number: None,
            window_len: 64_240,
            window_scale: None,
            max_seg_size: None,
            sack_permitted: false,
            sack_ranges: [None, None, None],
            payload: &[],
            timestamp: None,
        };
        let ipv4 = Ipv4Repr {
            src_addr,
            dst_addr,
            next_header: IpProtocol::Tcp,
            payload_len: tcp.buffer_len(),
            hop_limit: 64,
        };
        let frame_len = ETHERNET_HEADER_LEN + ipv4.buffer_len() + tcp.buffer_len();
        let mut bytes = buffers.try_frame_with_capacity(frame_len).expect("prewarmed frame");
        bytes.resize_zeroed(frame_len);
        let mut ethernet = EthernetFrame::new_unchecked(bytes.as_mut_vec());
        ethernet.set_src_addr(TEST_MAC.guest.smoltcp());
        ethernet.set_dst_addr(TEST_MAC.gateway.smoltcp());
        ethernet.set_ethertype(EthernetProtocol::Ipv4);
        let (ip_header, tcp_packet) = ethernet.payload_mut().split_at_mut(IPV4_HEADER_LEN);
        let checksum = smoltcp::phy::ChecksumCapabilities::default();
        ipv4.emit(&mut Ipv4Packet::new_unchecked(ip_header), &checksum);
        tcp.emit(
            &mut TcpPacket::new_unchecked(&mut tcp_packet[..TCP_HEADER_LEN]),
            &IpAddress::Ipv4(src_addr),
            &IpAddress::Ipv4(dst_addr),
            &checksum,
        );
        bytes
    }
}
