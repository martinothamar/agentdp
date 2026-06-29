use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use smoltcp::iface::{Config as InterfaceConfig, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{ChecksumCapabilities, Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::{tcp, udp};
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, IpEndpoint, Ipv4Address};

use super::packets::{GATEWAY_IP, GUEST_IP, GUEST_MAC};
use super::{Error, GuestLink, Result, SteppedNetwork};

const MTU_WITH_ETHERNET_HEADER: usize = 1514;
const TCP_BUFFER_BYTES: usize = 2 * 1024 * 1024;
const UDP_PACKET_SLOTS: usize = 64;
const UDP_BUFFER_BYTES: usize = 64 * 1024;
const DEFAULT_STEP: Duration = Duration::from_millis(1);
const DEFAULT_MAX_STEPS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpHandle(SocketHandle);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpHandle {
    handle: SocketHandle,
    remote: SocketAddr,
}

pub struct SmolTcpGuest {
    link: GuestLink,
    device: GuestDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    now: SmolInstant,
    next_port: u16,
    tcp_buffer_bytes: usize,
}

impl SmolTcpGuest {
    #[must_use]
    pub const fn tcp_buffer_bytes() -> usize {
        TCP_BUFFER_BYTES
    }

    #[must_use]
    pub fn pending_to_network_frames(&self) -> usize {
        self.link.pending_to_network_frames()
    }

    #[must_use]
    pub fn pending_from_network_frames(&self) -> usize {
        self.link.pending_from_network_frames()
    }

    #[must_use]
    pub fn progress_marker(&self) -> usize {
        self.link.progress_marker()
    }

    /// # Errors
    ///
    /// Returns an error when the guest interface cannot install the configured route.
    pub fn new(link: GuestLink) -> Result<Self> {
        Self::with_tcp_buffer_bytes(link, TCP_BUFFER_BYTES)
    }

    /// # Errors
    ///
    /// Returns an error when the guest interface cannot install the configured route.
    pub fn with_tcp_buffer_bytes(link: GuestLink, tcp_buffer_bytes: usize) -> Result<Self> {
        let mut device = GuestDevice::default();
        let mut iface = Interface::new(
            InterfaceConfig::new(HardwareAddress::Ethernet(EthernetAddress(GUEST_MAC))),
            &mut device,
            SmolInstant::ZERO,
        );
        iface.update_ip_addrs(|addresses| {
            let _inserted = addresses.push(IpCidr::new(IpAddress::Ipv4(Ipv4Address::from_octets(GUEST_IP)), 24));
        });
        iface
            .routes_mut()
            .add_default_ipv4_route(Ipv4Address::from_octets(GATEWAY_IP))
            .map_err(|error| Error::new(format!("add guest default route: {error:?}")))?;

        Ok(Self {
            link,
            device,
            iface,
            sockets: SocketSet::new(Vec::new()),
            now: SmolInstant::ZERO,
            next_port: 49_152,
            tcp_buffer_bytes,
        })
    }

    /// # Errors
    ///
    /// Returns an error when the UDP socket cannot be bound.
    pub fn open_udp(&mut self, remote: SocketAddr) -> Result<UdpHandle> {
        let rx_meta = vec![udp::PacketMetadata::EMPTY; UDP_PACKET_SLOTS];
        let tx_meta = vec![udp::PacketMetadata::EMPTY; UDP_PACKET_SLOTS];
        let rx_buffer = udp::PacketBuffer::new(rx_meta, vec![0; UDP_BUFFER_BYTES]);
        let tx_buffer = udp::PacketBuffer::new(tx_meta, vec![0; UDP_BUFFER_BYTES]);
        let mut socket = udp::Socket::new(rx_buffer, tx_buffer);
        socket
            .bind(self.take_port())
            .map_err(|error| Error::new(format!("bind guest UDP socket: {error:?}")))?;
        Ok(UdpHandle {
            handle: self.sockets.add(socket),
            remote,
        })
    }

    /// # Errors
    ///
    /// Returns an error when the datagram cannot be sent.
    pub fn send_udp<N>(&mut self, running: &mut N, udp: UdpHandle, payload: &[u8]) -> Result<()>
    where
        N: SteppedNetwork,
    {
        let endpoint = udp_endpoint(udp.remote)?;
        self.drive_until(running, "guest UDP send", |guest| {
            guest.sockets.get::<udp::Socket>(udp.handle).can_send()
        })?;
        self.sockets
            .get_mut::<udp::Socket>(udp.handle)
            .send_slice(payload, endpoint)
            .map_err(|error| Error::new(format!("guest UDP send: {error:?}")))?;
        self.pump(running)
    }

    /// # Errors
    ///
    /// Returns an error when no datagram is received before the drive budget is exhausted.
    pub fn recv_udp<N>(&mut self, running: &mut N, udp: UdpHandle, label: &str) -> Result<Vec<u8>>
    where
        N: SteppedNetwork,
    {
        for _step in 0..DEFAULT_MAX_STEPS {
            if let Some(bytes) = self.try_recv_udp(udp)? {
                return Ok(bytes);
            }
            self.pump(running)?;
        }
        Err(Error::new(format!(
            "{label}: exhausted after {DEFAULT_MAX_STEPS} guest drive steps waiting for UDP datagram"
        )))
    }

    /// # Errors
    ///
    /// Returns an error when smoltcp rejects a pending receive operation.
    pub fn try_recv_udp(&mut self, udp: UdpHandle) -> Result<Option<Vec<u8>>> {
        let socket = self.sockets.get_mut::<udp::Socket>(udp.handle);
        if !socket.can_recv() {
            return Ok(None);
        }
        let (bytes, _meta) = socket
            .recv()
            .map_err(|error| Error::new(format!("guest UDP receive: {error:?}")))?;
        Ok(Some(bytes.to_vec()))
    }

    pub fn close_udp(&mut self, udp: UdpHandle) {
        self.sockets.remove(udp.handle);
    }

    /// # Errors
    ///
    /// Returns an error when the TCP socket cannot be opened or the handshake does not complete within the drive budget.
    pub fn connect<N>(&mut self, running: &mut N, dst: SocketAddr) -> Result<TcpHandle>
    where
        N: SteppedNetwork,
    {
        let IpAddr::V4(dst_ip) = dst.ip() else {
            return Err(Error::new(format!(
                "smoltcp guest only supports IPv4 destinations: {dst}"
            )));
        };
        let rx = tcp::SocketBuffer::new(vec![0; self.tcp_buffer_bytes]);
        let tx = tcp::SocketBuffer::new(vec![0; self.tcp_buffer_bytes]);
        let socket = tcp::Socket::new(rx, tx);
        let handle = self.sockets.add(socket);
        let local_port = self.take_port();
        self.sockets
            .get_mut::<tcp::Socket>(handle)
            .connect(
                self.iface.context(),
                (IpAddress::Ipv4(Ipv4Address::from_octets(dst_ip.octets())), dst.port()),
                local_port,
            )
            .map_err(|error| Error::new(format!("guest TCP connect to {dst}: {error:?}")))?;

        self.drive_until(running, "guest TCP connect", |guest| {
            guest.sockets.get::<tcp::Socket>(handle).may_send()
        })?;
        Ok(TcpHandle(handle))
    }

    /// # Errors
    ///
    /// Returns an error when the socket closes before all bytes are accepted or the drive budget is exhausted.
    pub fn write_all<N>(&mut self, running: &mut N, handle: TcpHandle, mut bytes: &[u8]) -> Result<()>
    where
        N: SteppedNetwork,
    {
        while !bytes.is_empty() {
            let written = {
                let socket = self.sockets.get_mut::<tcp::Socket>(handle.0);
                if !socket.may_send() {
                    return Err(Error::new("guest TCP socket closed while writing"));
                }
                if socket.can_send() {
                    socket
                        .send_slice(bytes)
                        .map_err(|error| Error::new(format!("guest TCP send: {error:?}")))?
                } else {
                    0
                }
            };
            bytes = &bytes[written..];
            self.pump(running)?;
        }
        Ok(())
    }

    /// # Errors
    ///
    /// Returns an error when the socket closes or the drive budget is exhausted before `predicate` accepts the buffer.
    pub fn read_until<N>(
        &mut self,
        running: &mut N,
        handle: TcpHandle,
        label: &str,
        mut predicate: impl FnMut(&[u8]) -> bool,
    ) -> Result<Vec<u8>>
    where
        N: SteppedNetwork,
    {
        let mut output = Vec::new();
        for _step in 0..DEFAULT_MAX_STEPS {
            self.read_available(handle, &mut output)?;
            if predicate(&output) {
                return Ok(output);
            }
            let socket = self.sockets.get::<tcp::Socket>(handle.0);
            if !socket.may_recv() && !socket.can_recv() {
                return Err(Error::new(format!(
                    "{label}: guest TCP socket closed before expected bytes; received {output:02x?}"
                )));
            }
            self.pump(running)?;
        }
        Err(Error::new(format!(
            "{label}: exhausted after {DEFAULT_MAX_STEPS} guest drive steps; received {output:02x?}; status={:?}; pending_reactor_ready={}; debug={}",
            running.status(),
            running.pending_reactor_ready(),
            running.debug_snapshot(),
        )))
    }

    /// # Errors
    ///
    /// Returns an error when smoltcp rejects a pending receive operation.
    pub fn read_available_bytes(&mut self, handle: TcpHandle) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        self.read_available(handle, &mut output)?;
        Ok(output)
    }

    #[must_use]
    pub fn tcp_may_recv(&self, handle: TcpHandle) -> bool {
        let socket = self.sockets.get::<tcp::Socket>(handle.0);
        socket.may_recv() || socket.can_recv()
    }

    /// # Errors
    ///
    /// Returns an error when a final guest/network pump fails.
    pub fn close<N>(&mut self, running: &mut N, handle: TcpHandle) -> Result<()>
    where
        N: SteppedNetwork,
    {
        self.sockets.get_mut::<tcp::Socket>(handle.0).close();
        self.pump(running)
    }

    /// # Errors
    ///
    /// Returns an error when a final guest/network pump fails.
    pub fn abort_tcp<N>(&mut self, running: &mut N, handle: TcpHandle) -> Result<()>
    where
        N: SteppedNetwork,
    {
        self.sockets.get_mut::<tcp::Socket>(handle.0).abort();
        self.pump(running)
    }

    pub fn remove_tcp(&mut self, handle: TcpHandle) {
        self.sockets.remove(handle.0);
    }

    /// # Errors
    ///
    /// Returns an error when a guest/network pump fails while draining protocol cleanup frames.
    pub fn drain<N>(&mut self, running: &mut N, steps: usize) -> Result<()>
    where
        N: SteppedNetwork,
    {
        for _step in 0..steps {
            self.pump(running)?;
        }
        Ok(())
    }

    /// # Errors
    ///
    /// Returns an error when the socket does not close before the drive budget is exhausted.
    pub fn wait_closed<N>(&mut self, running: &mut N, handle: TcpHandle, label: &str) -> Result<()>
    where
        N: SteppedNetwork,
    {
        for _step in 0..DEFAULT_MAX_STEPS {
            let socket = self.sockets.get::<tcp::Socket>(handle.0);
            if !socket.may_recv() && !socket.can_recv() {
                return Ok(());
            }
            self.pump(running)?;
        }
        Err(Error::new(format!(
            "{label}: socket did not close after {DEFAULT_MAX_STEPS} guest drive steps"
        )))
    }

    /// # Errors
    ///
    /// Returns an error when the guest link rejects a transmitted frame.
    pub fn pump<N>(&mut self, running: &mut N) -> Result<()>
    where
        N: SteppedNetwork,
    {
        self.pump_with_step(running, DEFAULT_STEP)
    }

    /// # Errors
    ///
    /// Returns an error when the guest link rejects a transmitted frame.
    pub fn pump_with_step<N>(&mut self, running: &mut N, step: Duration) -> Result<()>
    where
        N: SteppedNetwork,
    {
        let _delivered = self.link.deliver_due(running.simulated_time());
        while let Some(frame) = self.link.try_recv_from_network() {
            self.device.rx.push_back(frame);
        }
        let _poll = self.iface.poll(self.now, &mut self.device, &mut self.sockets);
        while let Some(frame) = self.device.tx.pop_front() {
            self.link.send_to_network(frame)?;
        }
        let _delivered = self.link.deliver_due(running.simulated_time());
        running.step();
        running.advance_time(step);
        let _delivered = self.link.deliver_due(running.simulated_time());
        self.now += smoltcp::time::Duration::from_micros(u64::try_from(step.as_micros()).unwrap_or(u64::MAX));
        Ok(())
    }

    fn drive_until<N>(&mut self, running: &mut N, label: &str, mut predicate: impl FnMut(&Self) -> bool) -> Result<()>
    where
        N: SteppedNetwork,
    {
        for _step in 0..DEFAULT_MAX_STEPS {
            if predicate(self) {
                return Ok(());
            }
            self.pump(running)?;
        }
        Err(Error::new(format!(
            "{label}: exhausted after {DEFAULT_MAX_STEPS} guest drive steps"
        )))
    }

    fn read_available(&mut self, handle: TcpHandle, output: &mut Vec<u8>) -> Result<()> {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle.0);
        let mut buffer = [0_u8; 4096];
        while socket.can_recv() {
            let read_len = self.tcp_buffer_bytes.min(socket.recv_queue()).min(buffer.len());
            let read = socket
                .recv_slice(&mut buffer[..read_len])
                .map_err(|error| Error::new(format!("guest TCP receive: {error:?}")))?;
            output.extend_from_slice(&buffer[..read]);
        }
        Ok(())
    }

    const fn take_port(&mut self) -> u16 {
        let port = self.next_port;
        self.next_port = self.next_port.saturating_add(1);
        port
    }
}

fn udp_endpoint(remote: SocketAddr) -> Result<IpEndpoint> {
    let IpAddr::V4(remote_ip) = remote.ip() else {
        return Err(Error::new(format!(
            "smoltcp guest only supports IPv4 destinations: {remote}"
        )));
    };
    Ok(IpEndpoint::new(
        IpAddress::Ipv4(Ipv4Address::from_octets(remote_ip.octets())),
        remote.port(),
    ))
}

#[derive(Debug, Default)]
struct GuestDevice {
    rx: VecDeque<Vec<u8>>,
    tx: VecDeque<Vec<u8>>,
}

impl Device for GuestDevice {
    type RxToken<'a> = GuestRxToken;
    type TxToken<'a> = GuestTxToken<'a>;

    fn receive(&mut self, _timestamp: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let frame = self.rx.pop_front()?;
        Some((GuestRxToken { frame }, GuestTxToken { tx: &mut self.tx }))
    }

    fn transmit(&mut self, _timestamp: SmolInstant) -> Option<Self::TxToken<'_>> {
        Some(GuestTxToken { tx: &mut self.tx })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut capabilities = DeviceCapabilities::default();
        capabilities.medium = Medium::Ethernet;
        capabilities.max_transmission_unit = MTU_WITH_ETHERNET_HEADER;
        capabilities.checksum = ChecksumCapabilities::default();
        capabilities
    }
}

struct GuestRxToken {
    frame: Vec<u8>,
}

impl RxToken for GuestRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.frame)
    }
}

struct GuestTxToken<'a> {
    tx: &'a mut VecDeque<Vec<u8>>,
}

impl TxToken for GuestTxToken<'_> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut frame = vec![0; len];
        let result = f(&mut frame);
        self.tx.push_back(frame);
        result
    }
}
