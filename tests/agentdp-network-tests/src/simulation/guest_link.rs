use std::cell::{RefCell, RefMut};
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use std::time::Duration;

use agentdp_network::{
    ConnectStatus, FrameBuf, FrameRead, FrameWrite, GuestFrameSession, GuestFrameTransport, GuestIoSource,
    TransportError,
};
use agentdp_platform::socket::{LocalWake, LocalWakeReader};
use agentdp_rand::Seed;

use super::packet_scheduler::{PacketScheduler, PacketSchedulerConfig, SubmitFault, SubmitResult};
use super::{Error, Result};

#[derive(Debug, Clone, Copy)]
pub struct GuestLinkConfig {
    pub queue_capacity: usize,
    pub mtu: usize,
}

impl GuestLinkConfig {
    /// # Errors
    ///
    /// Returns an error when the guest link wake source cannot be created.
    pub fn open(self, seed: Seed) -> Result<GuestLink> {
        GuestLink::open(seed, self)
    }
}

impl Default for GuestLinkConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 256,
            mtu: 1514,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GuestLink {
    inner: Rc<RefCell<GuestLinkState>>,
}

impl GuestLink {
    /// # Errors
    ///
    /// Returns an error when the guest link wake source cannot be created.
    pub fn open(seed: Seed, config: GuestLinkConfig) -> Result<Self> {
        let wake = LocalWake::new().map_err(|error| Error::from_display("create guest link wake source", error))?;
        Ok(Self {
            inner: Rc::new(RefCell::new(GuestLinkState {
                seed,
                config,
                scheduler: PacketScheduler::new(
                    seed.derive("packet-scheduler"),
                    PacketSchedulerConfig {
                        capacity: config.queue_capacity,
                        mtu: config.mtu,
                    },
                ),
                now: Duration::ZERO,
                trace: Vec::new(),
                next_trace_order: 0,
                faults: VecDeque::new(),
                connected: false,
                closed: false,
                wake,
            })),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the frame exceeds the configured MTU or the queue is full.
    pub fn send_to_network(&self, frame: impl Into<Vec<u8>>) -> Result<()> {
        let frame = frame.into();
        let mut state = self.lock();
        state.push_guest_frame(frame)
    }

    #[must_use]
    pub fn try_recv_from_network(&self) -> Option<Vec<u8>> {
        let mut state = self.lock();
        let now = state.now;
        let packet = state.scheduler.pop_ready(LinkDirection::NetworkToGuest, now)?;
        state.trace_packet_event(
            LinkDirection::NetworkToGuest,
            LinkTraceEventKind::GuestRead,
            packet.sequence,
            packet.bytes.len(),
        );
        drop(state);
        Some(packet.bytes)
    }

    pub fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        state.trace_event(LinkDirection::GuestToNetwork, LinkTraceEventKind::Closed, 0);
        state.wake();
    }

    pub fn push_fault(&self, fault: LinkFault) {
        self.lock().faults.push_back(fault);
    }

    pub fn set_path_delay(&self, direction: LinkDirection, delay: Duration) {
        self.lock().scheduler.set_delay(direction, delay);
    }

    pub fn duplicate_next(&self, direction: LinkDirection) {
        self.lock().scheduler.duplicate_next(direction);
    }

    pub fn reorder_next(&self, direction: LinkDirection) {
        self.lock().scheduler.reorder_next(direction);
    }

    pub fn set_path_enabled(&self, direction: LinkDirection, enabled: bool) {
        self.lock().scheduler.set_enabled(direction, enabled);
    }

    #[must_use]
    pub(super) fn deliver_due(&self, now: Duration) -> usize {
        let mut state = self.lock();
        state.now = now;
        let delivered = state.scheduler.deliver_due(now).delivered;
        if state.scheduler.ready_len(LinkDirection::GuestToNetwork) > 0 {
            state.wake();
        }
        delivered
    }

    #[must_use]
    pub fn pending_to_network_frames(&self) -> usize {
        self.lock().scheduler.queued_len(LinkDirection::GuestToNetwork)
    }

    #[must_use]
    pub fn pending_from_network_frames(&self) -> usize {
        self.lock().scheduler.queued_len(LinkDirection::NetworkToGuest)
    }

    #[must_use]
    pub fn trace(&self) -> Vec<LinkTraceEvent> {
        let mut trace = {
            let state = self.lock();
            let mut trace = state.trace.clone();
            trace.extend_from_slice(state.scheduler.trace());
            trace
        };
        trace.sort_by_key(|event| (event.at, event.event.trace_rank(), event.order));
        trace
    }

    #[must_use]
    pub fn progress_marker(&self) -> usize {
        let state = self.lock();
        u64_to_usize(state.next_trace_order.saturating_add(state.scheduler.progress_marker()))
    }

    fn lock(&self) -> RefMut<'_, GuestLinkState> {
        self.inner.borrow_mut()
    }
}

impl GuestFrameTransport for GuestLink {
    type Session = GuestLinkSession;

    fn try_connect(&mut self) -> std::result::Result<ConnectStatus<Self::Session>, TransportError> {
        let mut state = self.lock();
        if state.take_fault(LinkFault::PendingConnect) {
            state.trace_event(LinkDirection::GuestToNetwork, LinkTraceEventKind::PendingConnect, 0);
            drop(state);
            return Ok(ConnectStatus::Pending);
        }
        if state.take_fault(LinkFault::FailConnect) {
            state.trace_event(LinkDirection::GuestToNetwork, LinkTraceEventKind::ConnectFailed, 0);
            drop(state);
            return Err(TransportError::operation(
                "connect guest link",
                "injected connect failure",
            ));
        }
        if state.connected {
            drop(state);
            return Err(TransportError::operation(
                "connect guest link",
                "session already connected",
            ));
        }
        let wake_reader = state.take_wake_reader()?;
        state.connected = true;
        state.trace_event(LinkDirection::GuestToNetwork, LinkTraceEventKind::Connected, 0);
        drop(state);
        Ok(ConnectStatus::Connected(GuestLinkSession {
            link: self.clone(),
            wake_reader,
        }))
    }

    fn cleanup(self) -> std::result::Result<(), TransportError> {
        self.close();
        Ok(())
    }

    fn describe(&self) -> String {
        let state = self.lock();
        let seed = state.seed;
        drop(state);
        format!("simulation guest link {seed}")
    }
}

pub struct GuestLinkSession {
    link: GuestLink,
    wake_reader: LocalWakeReader,
}

impl GuestFrameSession for GuestLinkSession {
    fn io_source(&mut self) -> GuestIoSource<'_> {
        self.wake_reader.io_source().into()
    }

    fn read_frame_into(&mut self, frame: &mut FrameBuf) -> std::result::Result<FrameRead, TransportError> {
        let mut state = self.link.lock();
        if state.take_fault(LinkFault::BlockNextRead) {
            state.trace_event(LinkDirection::GuestToNetwork, LinkTraceEventKind::ReadBlocked, 0);
            state.wake();
            drop(state);
            return Ok(FrameRead::Blocked);
        }
        let now = state.now;
        let Some(packet) = state.scheduler.pop_ready(LinkDirection::GuestToNetwork, now) else {
            let read = if state.closed {
                FrameRead::Closed
            } else {
                FrameRead::Blocked
            };
            drop(state);
            return Ok(read);
        };
        frame.as_mut_vec().clear();
        frame.as_mut_vec().extend_from_slice(&packet.bytes);
        state.trace_packet_event(
            LinkDirection::GuestToNetwork,
            LinkTraceEventKind::NetworkRead,
            packet.sequence,
            packet.bytes.len(),
        );
        if state.scheduler.ready_len(LinkDirection::GuestToNetwork) > 0 {
            state.wake();
        }
        drop(state);
        Ok(FrameRead::Frame)
    }

    fn write_frame(&mut self, frame: &[u8]) -> std::result::Result<FrameWrite, TransportError> {
        let mut state = self.link.lock();
        if state.take_fault(LinkFault::BlockNextWrite) {
            state.trace_event(
                LinkDirection::NetworkToGuest,
                LinkTraceEventKind::WriteBlocked,
                frame.len(),
            );
            drop(state);
            return Ok(FrameWrite::Blocked);
        }
        if state.scheduler.queued_len(LinkDirection::NetworkToGuest) >= state.config.queue_capacity {
            state.trace_event(
                LinkDirection::NetworkToGuest,
                LinkTraceEventKind::WriteBlocked,
                frame.len(),
            );
            drop(state);
            return Ok(FrameWrite::Blocked);
        }
        let fault = state
            .take_fault(LinkFault::DropNextNetworkFrame)
            .then_some(SubmitFault::Drop);
        let now = state.now;
        match state
            .scheduler
            .submit(LinkDirection::NetworkToGuest, now, frame.to_vec(), fault)
        {
            SubmitResult::Accepted | SubmitResult::Dropped => {}
            SubmitResult::CapacityDropped => {
                drop(state);
                return Err(TransportError::operation(
                    "write guest link frame",
                    "network-to-guest queue is full",
                ));
            }
            SubmitResult::MtuExceeded => {
                drop(state);
                return Err(TransportError::operation(
                    "write guest link frame",
                    format!("network frame length {} exceeds MTU", frame.len()),
                ));
            }
        }
        drop(state);
        Ok(FrameWrite::Flushed)
    }

    fn shutdown_write(&mut self) -> std::result::Result<(), TransportError> {
        let mut state = self.link.lock();
        if state.take_fault(LinkFault::FailShutdownWrite) {
            state.trace_event(LinkDirection::NetworkToGuest, LinkTraceEventKind::ShutdownFailed, 0);
            drop(state);
            return Err(TransportError::operation(
                "shutdown guest link write",
                "injected shutdown failure",
            ));
        }
        state.trace_event(LinkDirection::NetworkToGuest, LinkTraceEventKind::Shutdown, 0);
        drop(state);
        Ok(())
    }
}

#[derive(Debug)]
struct GuestLinkState {
    seed: Seed,
    config: GuestLinkConfig,
    scheduler: PacketScheduler,
    now: Duration,
    trace: Vec<LinkTraceEvent>,
    next_trace_order: u64,
    faults: VecDeque<LinkFault>,
    connected: bool,
    closed: bool,
    wake: LocalWake,
}

impl GuestLinkState {
    fn trace_event(&mut self, direction: LinkDirection, event: LinkTraceEventKind, bytes: usize) {
        self.trace_packet_event(direction, event, 0, bytes);
    }

    fn trace_packet_event(&mut self, direction: LinkDirection, event: LinkTraceEventKind, sequence: u64, bytes: usize) {
        let order = self.take_trace_order();
        self.trace.push(LinkTraceEvent::packet(
            direction, event, sequence, bytes, self.now, order,
        ));
    }

    fn push_guest_frame(&mut self, frame: Vec<u8>) -> Result<()> {
        if frame.len() > self.config.mtu {
            self.trace_event(
                LinkDirection::GuestToNetwork,
                LinkTraceEventKind::MtuExceeded,
                frame.len(),
            );
            return Err(Error::new(format!(
                "guest frame length {} exceeds MTU {}",
                frame.len(),
                self.config.mtu
            )));
        }
        let fault = self
            .take_fault(LinkFault::DropNextGuestFrame)
            .then_some(SubmitFault::Drop);
        match self
            .scheduler
            .submit(LinkDirection::GuestToNetwork, self.now, frame, fault)
        {
            SubmitResult::Accepted => {
                if self.scheduler.ready_len(LinkDirection::GuestToNetwork) > 0 {
                    self.wake();
                }
                Ok(())
            }
            SubmitResult::Dropped => Ok(()),
            SubmitResult::CapacityDropped => Err(Error::new("guest-to-network queue is full")),
            SubmitResult::MtuExceeded => unreachable!("guest MTU is checked before scheduler submit"),
        }
    }

    fn take_fault(&mut self, fault: LinkFault) -> bool {
        if self.faults.front().copied() == Some(fault) {
            let _fault = self.faults.pop_front();
            true
        } else {
            false
        }
    }

    fn take_wake_reader(&mut self) -> std::result::Result<LocalWakeReader, TransportError> {
        self.wake
            .take_reader()
            .ok_or_else(|| TransportError::operation("connect guest link", "wake reader already registered"))
    }

    fn wake(&mut self) {
        let _sent = self.wake.wake();
    }

    const fn take_trace_order(&mut self) -> u64 {
        let order = self.next_trace_order;
        self.next_trace_order = self.next_trace_order.saturating_add(1);
        order
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkFault {
    PendingConnect,
    FailConnect,
    BlockNextRead,
    BlockNextWrite,
    DropNextGuestFrame,
    DropNextNetworkFrame,
    FailShutdownWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkDirection {
    GuestToNetwork,
    NetworkToGuest,
}

impl Display for LinkDirection {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GuestToNetwork => formatter.write_str("guest-to-network"),
            Self::NetworkToGuest => formatter.write_str("network-to-guest"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkTraceEvent {
    pub direction: LinkDirection,
    pub event: LinkTraceEventKind,
    pub sequence: u64,
    pub bytes: usize,
    pub at: Duration,
    pub order: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkTraceEventKind {
    Connected,
    PendingConnect,
    ConnectFailed,
    Submitted,
    Scheduled,
    Delivered,
    Consumed,
    GuestRead,
    NetworkRead,
    ReadBlocked,
    WriteBlocked,
    Dropped,
    Duplicated,
    Reordered,
    CapacityDropped,
    DisabledPathDropped,
    MtuExceeded,
    Closed,
    Shutdown,
    ShutdownFailed,
}

impl LinkTraceEventKind {
    const fn trace_rank(self) -> u8 {
        match self {
            Self::Connected | Self::PendingConnect | Self::ConnectFailed => 0,
            Self::Submitted => 10,
            Self::Duplicated => 11,
            Self::Dropped | Self::CapacityDropped | Self::DisabledPathDropped | Self::MtuExceeded => 12,
            Self::Scheduled => 20,
            Self::Reordered => 29,
            Self::Delivered => 30,
            Self::Consumed => 40,
            Self::GuestRead | Self::NetworkRead => 41,
            Self::ReadBlocked | Self::WriteBlocked => 50,
            Self::Closed | Self::Shutdown | Self::ShutdownFailed => 90,
        }
    }
}

impl LinkTraceEvent {
    #[must_use]
    pub const fn packet(
        direction: LinkDirection,
        event: LinkTraceEventKind,
        sequence: u64,
        bytes: usize,
        at: Duration,
        order: u64,
    ) -> Self {
        Self {
            direction,
            event,
            sequence,
            bytes,
            at,
            order,
        }
    }
}

fn u64_to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}
