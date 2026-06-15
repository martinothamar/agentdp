use std::collections::VecDeque;
use std::time::Duration;

use agentdp_rand::Seed;

use super::guest_link::{LinkDirection, LinkTraceEvent, LinkTraceEventKind};

#[derive(Debug)]
pub(super) struct PacketScheduler {
    guest_to_network: DirectedPath,
    network_to_guest: DirectedPath,
    next_sequence: u64,
    next_trace_order: u64,
    trace: Vec<LinkTraceEvent>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PacketSchedulerConfig {
    pub(super) capacity: usize,
    pub(super) mtu: usize,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum SubmitFault {
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SubmitResult {
    Accepted,
    Dropped,
    CapacityDropped,
    MtuExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DeliveryReport {
    pub(super) delivered: usize,
}

#[derive(Debug)]
struct DirectedPath {
    capacity: usize,
    mtu: usize,
    enabled: bool,
    delay: Duration,
    duplicate_next: bool,
    reorder_next: bool,
    ready: VecDeque<ReadyPacket>,
    pending: VecDeque<ScheduledPacket>,
}

#[derive(Debug)]
pub(super) struct ReadyPacket {
    pub(super) sequence: u64,
    pub(super) bytes: Vec<u8>,
}

#[derive(Debug)]
struct ScheduledPacket {
    sequence: u64,
    bytes: Vec<u8>,
    ready_at: Duration,
}

impl PacketScheduler {
    #[must_use]
    pub(super) const fn new(_seed: Seed, config: PacketSchedulerConfig) -> Self {
        Self {
            guest_to_network: DirectedPath::new(LinkDirection::GuestToNetwork, config),
            network_to_guest: DirectedPath::new(LinkDirection::NetworkToGuest, config),
            next_sequence: 1,
            next_trace_order: 0,
            trace: Vec::new(),
        }
    }

    pub(super) const fn set_delay(&mut self, direction: LinkDirection, delay: Duration) {
        self.path_mut(direction).delay = delay;
    }

    pub(super) const fn duplicate_next(&mut self, direction: LinkDirection) {
        self.path_mut(direction).duplicate_next = true;
    }

    pub(super) const fn reorder_next(&mut self, direction: LinkDirection) {
        self.path_mut(direction).reorder_next = true;
    }

    pub(super) const fn set_enabled(&mut self, direction: LinkDirection, enabled: bool) {
        self.path_mut(direction).enabled = enabled;
    }

    pub(super) fn submit(
        &mut self,
        direction: LinkDirection,
        now: Duration,
        bytes: Vec<u8>,
        fault: Option<SubmitFault>,
    ) -> SubmitResult {
        if bytes.len() > self.path(direction).mtu {
            self.trace_packet(direction, LinkTraceEventKind::MtuExceeded, 0, bytes.len(), now);
            return SubmitResult::MtuExceeded;
        }

        if matches!(fault, Some(SubmitFault::Drop)) {
            self.trace_packet(direction, LinkTraceEventKind::Dropped, 0, bytes.len(), now);
            return SubmitResult::Dropped;
        }

        if !self.path(direction).enabled {
            self.trace_packet(direction, LinkTraceEventKind::DisabledPathDropped, 0, bytes.len(), now);
            return SubmitResult::Dropped;
        }

        if self.path(direction).queued_len() >= self.path(direction).capacity {
            self.trace_packet(direction, LinkTraceEventKind::CapacityDropped, 0, bytes.len(), now);
            return SubmitResult::CapacityDropped;
        }

        let duplicate = self.path_mut(direction).take_duplicate_next().then(|| bytes.clone());
        let sequence = self.take_sequence();
        self.trace_packet(direction, LinkTraceEventKind::Submitted, sequence, bytes.len(), now);
        self.schedule_packet(direction, now, sequence, bytes);

        if let Some(duplicate) = duplicate {
            let duplicate_sequence = self.take_sequence();
            self.trace_packet(
                direction,
                LinkTraceEventKind::Duplicated,
                duplicate_sequence,
                duplicate.len(),
                now,
            );
            if self.path(direction).queued_len() >= self.path(direction).capacity {
                self.trace_packet(
                    direction,
                    LinkTraceEventKind::CapacityDropped,
                    duplicate_sequence,
                    duplicate.len(),
                    now,
                );
            } else {
                self.schedule_packet(direction, now, duplicate_sequence, duplicate);
            }
        }

        SubmitResult::Accepted
    }

    pub(super) fn deliver_due(&mut self, now: Duration) -> DeliveryReport {
        let delivered = self.deliver_direction(LinkDirection::GuestToNetwork, now)
            + self.deliver_direction(LinkDirection::NetworkToGuest, now);
        DeliveryReport { delivered }
    }

    pub(super) fn pop_ready(&mut self, direction: LinkDirection, now: Duration) -> Option<ReadyPacket> {
        let packet = self.path_mut(direction).ready.pop_front()?;
        self.trace_packet(
            direction,
            LinkTraceEventKind::Consumed,
            packet.sequence,
            packet.bytes.len(),
            now,
        );
        Some(packet)
    }

    #[must_use]
    pub(super) fn ready_len(&self, direction: LinkDirection) -> usize {
        self.path(direction).ready.len()
    }

    #[must_use]
    pub(super) fn queued_len(&self, direction: LinkDirection) -> usize {
        self.path(direction).queued_len()
    }

    #[must_use]
    pub(super) fn trace(&self) -> &[LinkTraceEvent] {
        &self.trace
    }

    #[must_use]
    pub(super) const fn progress_marker(&self) -> u64 {
        self.next_trace_order
    }

    fn schedule_packet(&mut self, direction: LinkDirection, now: Duration, sequence: u64, bytes: Vec<u8>) {
        let delay = self.path(direction).delay;
        let ready_at = now.saturating_add(delay);
        self.trace_packet(
            direction,
            LinkTraceEventKind::Scheduled,
            sequence,
            bytes.len(),
            ready_at,
        );
        let path = self.path_mut(direction);
        let insert_at = path
            .pending
            .iter()
            .position(|packet| (ready_at, sequence) < (packet.ready_at, packet.sequence))
            .unwrap_or(path.pending.len());
        path.pending.insert(
            insert_at,
            ScheduledPacket {
                sequence,
                bytes,
                ready_at,
            },
        );
    }

    fn deliver_direction(&mut self, direction: LinkDirection, now: Duration) -> usize {
        let Some(first) = self.pop_due_packet(direction, now) else {
            return 0;
        };

        let mut delivered = 1;
        let reorder = self
            .path(direction)
            .pending
            .front()
            .is_some_and(|packet| packet.ready_at <= now)
            && self.path_mut(direction).take_reorder_next();
        if reorder {
            let Some(second) = self.pop_due_packet(direction, now) else {
                self.push_delivered_packet(direction, now, first);
                return delivered;
            };
            self.trace_packet(
                direction,
                LinkTraceEventKind::Reordered,
                first.sequence,
                first.bytes.len(),
                now,
            );
            self.trace_packet(
                direction,
                LinkTraceEventKind::Reordered,
                second.sequence,
                second.bytes.len(),
                now,
            );
            self.push_delivered_packet(direction, now, second);
            self.push_delivered_packet(direction, now, first);
            delivered += 1;
        } else {
            self.push_delivered_packet(direction, now, first);
        }

        while let Some(packet) = self.pop_due_packet(direction, now) {
            self.push_delivered_packet(direction, now, packet);
            delivered += 1;
        }
        delivered
    }

    fn pop_due_packet(&mut self, direction: LinkDirection, now: Duration) -> Option<ScheduledPacket> {
        self.path(direction)
            .pending
            .front()
            .is_some_and(|packet| packet.ready_at <= now)
            .then(|| self.path_mut(direction).pending.pop_front())
            .flatten()
    }

    fn push_delivered_packet(&mut self, direction: LinkDirection, now: Duration, packet: ScheduledPacket) {
        self.trace_packet(
            direction,
            LinkTraceEventKind::Delivered,
            packet.sequence,
            packet.bytes.len(),
            now,
        );
        self.path_mut(direction).ready.push_back(ReadyPacket {
            sequence: packet.sequence,
            bytes: packet.bytes,
        });
    }

    const fn take_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        sequence
    }

    fn trace_packet(
        &mut self,
        direction: LinkDirection,
        event: LinkTraceEventKind,
        sequence: u64,
        bytes: usize,
        at: Duration,
    ) {
        let order = self.take_trace_order();
        self.trace
            .push(LinkTraceEvent::packet(direction, event, sequence, bytes, at, order));
    }

    const fn take_trace_order(&mut self) -> u64 {
        let order = self.next_trace_order;
        self.next_trace_order = self.next_trace_order.saturating_add(1);
        order
    }

    const fn path(&self, direction: LinkDirection) -> &DirectedPath {
        match direction {
            LinkDirection::GuestToNetwork => &self.guest_to_network,
            LinkDirection::NetworkToGuest => &self.network_to_guest,
        }
    }

    const fn path_mut(&mut self, direction: LinkDirection) -> &mut DirectedPath {
        match direction {
            LinkDirection::GuestToNetwork => &mut self.guest_to_network,
            LinkDirection::NetworkToGuest => &mut self.network_to_guest,
        }
    }
}

impl DirectedPath {
    const fn new(_direction: LinkDirection, config: PacketSchedulerConfig) -> Self {
        Self {
            capacity: config.capacity,
            mtu: config.mtu,
            enabled: true,
            delay: Duration::ZERO,
            duplicate_next: false,
            reorder_next: false,
            ready: VecDeque::new(),
            pending: VecDeque::new(),
        }
    }

    fn queued_len(&self) -> usize {
        self.ready.len() + self.pending.len()
    }

    const fn take_duplicate_next(&mut self) -> bool {
        let duplicate = self.duplicate_next;
        self.duplicate_next = false;
        duplicate
    }

    const fn take_reorder_next(&mut self) -> bool {
        let reorder = self.reorder_next;
        self.reorder_next = false;
        reorder
    }
}
