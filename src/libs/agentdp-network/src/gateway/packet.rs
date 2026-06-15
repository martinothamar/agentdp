use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::buffers::{BufferPool, ByteBuf, FrameBuf};
use crate::network::{InstanceMacAddresses, MacAddress, UdpProxyKey};
use smoltcp::phy::ChecksumCapabilities;
use smoltcp::wire::{
    ETHERNET_HEADER_LEN, EthernetFrame, EthernetProtocol, IPV4_HEADER_LEN, IpAddress, IpProtocol, Ipv4Packet, Ipv4Repr,
    TcpPacket, UdpPacket, UdpRepr,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TcpSyn {
    pub(super) src: SocketAddr,
    pub(super) dst: SocketAddr,
}

impl TcpSyn {
    pub(super) fn from_frame(frame: &[u8]) -> Option<Self> {
        let eth = EthernetFrame::new_checked(frame).ok()?;
        if eth.ethertype() != EthernetProtocol::Ipv4 {
            return None;
        }
        let ip = Ipv4Packet::new_checked(eth.payload()).ok()?;
        if ip.next_header() != IpProtocol::Tcp {
            return None;
        }
        let tcp = TcpPacket::new_checked(ip.payload()).ok()?;
        if !tcp.syn() || tcp.ack() {
            return None;
        }
        Some(Self {
            src: SocketAddr::new(IpAddr::V4(ip.src_addr()), tcp.src_port()),
            dst: SocketAddr::new(IpAddr::V4(ip.dst_addr()), tcp.dst_port()),
        })
    }
}

#[derive(Debug)]
pub(super) struct UdpDatagram {
    pub(super) src: SocketAddr,
    pub(super) dst: SocketAddr,
    pub(super) payload: ByteBuf,
}

impl UdpDatagram {
    pub(super) fn from_frame(frame: &[u8], buffers: &BufferPool) -> Option<Self> {
        let eth = EthernetFrame::new_checked(frame).ok()?;
        if eth.ethertype() != EthernetProtocol::Ipv4 {
            return None;
        }
        let ip = Ipv4Packet::new_checked(eth.payload()).ok()?;
        if ip.next_header() != IpProtocol::Udp {
            return None;
        }
        let udp = UdpPacket::new_checked(ip.payload()).ok()?;
        let mut payload = buffers.try_byte_with_capacity(udp.payload().len()).ok()?;
        payload.extend_from_slice(udp.payload());
        Some(Self {
            src: SocketAddr::new(IpAddr::V4(ip.src_addr()), udp.src_port()),
            dst: SocketAddr::new(IpAddr::V4(ip.dst_addr()), udp.dst_port()),
            payload,
        })
    }
}

pub(super) fn udp_response_frame(
    proxy: UdpProxyKey,
    payload: &[u8],
    mac: InstanceMacAddresses,
    buffers: &BufferPool,
) -> Option<FrameBuf> {
    udp_frame(
        proxy.guest_dst,
        proxy.guest_src,
        payload,
        mac.gateway,
        mac.guest,
        buffers,
    )
}

pub(super) fn udp_frame(
    src: SocketAddr,
    dst: SocketAddr,
    payload: &[u8],
    source_mac: MacAddress,
    destination_mac: MacAddress,
    buffers: &BufferPool,
) -> Option<FrameBuf> {
    let (IpAddr::V4(src_addr), IpAddr::V4(dst_addr)) = (src.ip(), dst.ip()) else {
        return buffers.try_frame().ok();
    };
    let udp = UdpRepr {
        src_port: src.port(),
        dst_port: dst.port(),
    };
    let ipv4 = Ipv4Repr {
        src_addr,
        dst_addr,
        next_header: IpProtocol::Udp,
        payload_len: udp.header_len() + payload.len(),
        hop_limit: 64,
    };
    let frame_len = ETHERNET_HEADER_LEN + ipv4.buffer_len() + ipv4.payload_len;
    let mut bytes = buffers.try_frame_with_capacity(frame_len).ok()?;
    bytes.resize_zeroed(frame_len);
    let mut ethernet = EthernetFrame::new_unchecked(bytes.as_mut_vec());
    ethernet.set_src_addr(source_mac.smoltcp());
    ethernet.set_dst_addr(destination_mac.smoltcp());
    ethernet.set_ethertype(EthernetProtocol::Ipv4);
    let (ip_header, udp_packet) = ethernet.payload_mut().split_at_mut(IPV4_HEADER_LEN);
    let checksum = ChecksumCapabilities::default();
    ipv4.emit(&mut Ipv4Packet::new_unchecked(ip_header), &checksum);
    udp.emit(
        &mut UdpPacket::new_unchecked(udp_packet),
        &IpAddress::Ipv4(src_addr),
        &IpAddress::Ipv4(dst_addr),
        payload.len(),
        |udp_payload| udp_payload.copy_from_slice(payload),
        &checksum,
    );
    Some(bytes)
}

pub(super) fn resolve_gateway_destination(dst: SocketAddr, gateway: Ipv4Addr) -> SocketAddr {
    match dst.ip() {
        IpAddr::V4(address) if address == gateway => SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), dst.port()),
        _ => dst,
    }
}
