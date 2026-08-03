use std::collections::VecDeque;
use std::io::{self, Read};
use std::net::SocketAddr;

use crate::buffers::{BufferPool, ByteBuf, FrameBuf, WriteQueue};
use crate::network::NetworkLimits;
use crate::reactor::ReactorUdpSocket;
use crate::readiness::IoSlotState;
use smoltcp::socket::tcp;

/// Bounded dataplane work reports three separate facts:
///
/// - what changed during this turn,
/// - what external or resource condition prevents more work on the path that stopped,
/// - whether local buffer pressure left a local continuation eligible.
///
/// Keeping those facts separate prevents common event-loop bugs where local
/// backpressure is mistaken for reactor unreadiness, or one blocked direction
/// suppresses progress that could drain buffers in the opposite direction.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DriveReport {
    progress: DriveProgress,
    wait: DriveWait,
    local_buffer_continuation: bool,
    budget_exhausted: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DriveProgress {
    pub(crate) bytes_read: usize,
    pub(crate) bytes_written: usize,
    pub(crate) guest_bytes_enqueued: usize,
    pub(crate) guest_bytes_dequeued: usize,
    pub(crate) events_emitted: usize,
    pub(crate) state_changes: usize,
}

/// Turn-local wait reasons recorded by typed drive operations.
///
/// These bits are not global readiness ownership. Reactor readiness is owned by
/// `IoSlotState` and is cleared by typed IO helpers only after an attempted
/// operation returns `WouldBlock`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DriveWait(u16);

pub(crate) struct DriveTurn<'a> {
    budget: &'a mut DriveBudget,
    report: &'a mut DriveReport,
}

pub(crate) enum DriveStreamRead {
    Bytes(ByteBuf),
    Closed,
    NotReady,
    WouldBlock,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DriveStreamWrite {
    Drained,
    Pending { blocked_on: DriveStreamWriteBlock },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DriveStreamWriteBlock {
    Budget,
    ReactorWrite,
}

pub(crate) enum DriveSmoltcpTcpRecv {
    Bytes(ByteBuf),
    Empty,
    Blocked,
}

pub(crate) enum DriveDatagramRecv {
    Bytes(ByteBuf),
    NotReady,
    WouldBlock,
    Blocked,
    Budget,
}

pub(crate) enum DriveDatagramRecvFrom {
    Bytes { bytes: ByteBuf, peer: SocketAddr },
    NotReady,
    WouldBlock,
    Blocked,
    Budget,
}

pub(crate) enum DriveDatagramSend {
    Sent,
    NotReady,
    WouldBlock,
    Budget,
}

#[must_use]
pub(crate) enum DriveGuestFrameEnqueue {
    Reserved(GuestFrameReservation),
    Blocked,
}

#[must_use]
pub(crate) struct GuestFrameReservation {
    _private: (),
}

impl GuestFrameReservation {
    pub(crate) fn push_vec(self, guest_frames: &mut Vec<FrameBuf>, frame: FrameBuf) {
        let Self { _private: () } = self;
        debug_assert!(guest_frames.len() < guest_frames.capacity());
        guest_frames.push(frame);
    }

    pub(crate) fn push_queue(self, guest_frames: &mut VecDeque<FrameBuf>, frame: FrameBuf) {
        let Self { _private: () } = self;
        guest_frames.push_back(frame);
    }
}

pub(crate) enum DriveGuestFrameWrite {
    Flushed,
    WouldBlock,
    Budget,
}

pub(crate) enum DriveGuestFrameRead {
    Frame(FrameBuf),
    Closed,
    WouldBlock,
    Blocked,
}

pub(crate) enum DriveGuestFrameReadStatus {
    Frame,
    Blocked,
    Closed,
}

pub(crate) enum DriveProtocolOp<T> {
    Progress { bytes: usize, value: T },
    NoProgress { value: T },
}

pub(crate) enum DriveTransportOp<T> {
    Progress { bytes: usize, value: T },
    WouldBlock { value: T },
    NoProgress { value: T },
}

pub(crate) enum DriveProtocolPoll<T> {
    Complete(T),
    Budget,
}

pub(crate) enum DriveTransportPoll<T> {
    Complete(T),
    Pending,
    Budget,
}

pub(crate) enum DriveApply<T, E> {
    Applied(T),
    Deferred,
    Failed(E),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DriveProtocolOutput {
    Bytes,
    Empty,
}

impl DriveReport {
    pub(crate) const fn new() -> Self {
        Self {
            progress: DriveProgress::new(),
            wait: DriveWait::NONE,
            local_buffer_continuation: false,
            budget_exhausted: false,
        }
    }

    #[cfg(any(test, feature = "simulation"))]
    #[allow(dead_code)]
    pub(crate) const fn wait(&self) -> DriveWait {
        self.wait
    }

    pub(crate) const fn has_local_buffer_continuation(&self) -> bool {
        self.local_buffer_continuation
    }

    pub(crate) const fn blocked_on(&self, wait: DriveWait) -> bool {
        self.wait.contains(wait)
    }

    pub(crate) const fn budget_exhausted(&self) -> bool {
        self.budget_exhausted
    }

    pub(crate) const fn made_progress(&self) -> bool {
        self.progress.made_progress()
    }

    pub(crate) const fn progress(&self) -> DriveProgress {
        self.progress
    }

    const fn bytes_read(&mut self, bytes: usize) {
        self.progress.bytes_read = self.progress.bytes_read.saturating_add(bytes);
    }

    const fn bytes_written(&mut self, bytes: usize) {
        self.progress.bytes_written = self.progress.bytes_written.saturating_add(bytes);
    }

    const fn guest_bytes_enqueued(&mut self, bytes: usize) {
        self.progress.guest_bytes_enqueued = self.progress.guest_bytes_enqueued.saturating_add(bytes);
    }

    const fn guest_bytes_dequeued(&mut self, bytes: usize) {
        self.progress.guest_bytes_dequeued = self.progress.guest_bytes_dequeued.saturating_add(bytes);
    }

    const fn event_emitted(&mut self) {
        self.progress.events_emitted = self.progress.events_emitted.saturating_add(1);
    }

    const fn state_changed(&mut self) {
        self.progress.state_changes = self.progress.state_changes.saturating_add(1);
    }

    const fn mark_budget_exhausted(&mut self) {
        self.budget_exhausted = true;
    }
}

impl DriveProgress {
    pub(crate) const fn new() -> Self {
        Self {
            bytes_read: 0,
            bytes_written: 0,
            guest_bytes_enqueued: 0,
            guest_bytes_dequeued: 0,
            events_emitted: 0,
            state_changes: 0,
        }
    }

    pub(crate) const fn made_progress(&self) -> bool {
        self.bytes_read > 0
            || self.bytes_written > 0
            || self.guest_bytes_enqueued > 0
            || self.guest_bytes_dequeued > 0
            || self.events_emitted > 0
            || self.state_changes > 0
    }
}

impl<'a> DriveTurn<'a> {
    pub(crate) const fn new(budget: &'a mut DriveBudget, report: &'a mut DriveReport) -> Self {
        Self { budget, report }
    }

    const fn can_continue_or_exhausted(&mut self) -> bool {
        self.budget.can_continue_or_exhausted(self.report)
    }

    pub(crate) const fn can_start_operation(&mut self) -> bool {
        self.can_continue_or_exhausted()
    }

    const fn step_or_exhausted(&mut self) -> bool {
        self.budget.step_or_exhausted(self.report)
    }

    const fn can_emit_event_or_exhausted(&mut self) -> bool {
        self.budget.can_emit_event_or_exhausted(self.report)
    }

    fn event_or_exhausted(&mut self, bytes: usize) -> bool {
        self.budget.event_or_exhausted(bytes, self.report)
    }

    const fn emit_event(&mut self) -> bool {
        if !self.budget.event_step_or_exhausted(self.report) {
            return false;
        }
        if !self.budget.event_only_or_exhausted(self.report) {
            return false;
        }
        self.report.event_emitted();
        true
    }

    pub(crate) fn push_event<T>(&mut self, events: &mut Vec<T>, event: T) -> Result<(), T> {
        if !self.emit_event() {
            return Err(event);
        }
        events.push(event);
        Ok(())
    }

    const fn remaining_bytes(&self) -> usize {
        self.budget.remaining_bytes()
    }

    fn spend_bytes(&mut self, bytes: usize) {
        self.budget.consume_bytes_or_exhausted(bytes, self.report);
    }

    pub(crate) const fn progress(&self) -> DriveProgress {
        self.report.progress()
    }

    #[cfg(any(test, feature = "simulation"))]
    #[allow(dead_code)]
    pub(crate) const fn wait(&self) -> DriveWait {
        self.report.wait()
    }

    pub(crate) const fn budget_is_exhausted(&self) -> bool {
        self.report.budget_exhausted()
    }

    /// Check whether a whole-item IO operation can be prepared before doing
    /// setup that has external side effects, such as creating/registering a
    /// socket. The actual IO helper still consumes the budget.
    pub(crate) const fn can_prepare_whole_item_operation(&mut self, len: usize) -> bool {
        if !self.can_continue_or_exhausted() {
            return false;
        }
        if len > self.remaining_bytes() {
            self.report.mark_budget_exhausted();
            return false;
        }
        true
    }

    pub(crate) const fn wait_for_reactor_read(&mut self) {
        self.block_on(DriveWait::REACTOR_READ);
    }

    pub(crate) const fn wait_for_reactor_write(&mut self) {
        self.block_on(DriveWait::REACTOR_WRITE);
    }

    pub(crate) const fn wait_for_reactor_read_write(&mut self) {
        self.block_on(DriveWait::REACTOR_READ_WRITE);
    }

    pub(crate) const fn wait_for_guest_recv(&mut self) {
        self.block_on(DriveWait::GUEST_RECV);
    }

    pub(crate) const fn wait_for_guest_send_capacity(&mut self) {
        self.block_on(DriveWait::GUEST_SEND_CAPACITY);
    }

    pub(crate) const fn wait_for_local_buffer_capacity(&mut self) {
        self.block_on(DriveWait::LOCAL_BUFFER_CAPACITY);
    }

    pub(crate) const fn wait_for_connection_slot(&mut self) {
        self.block_on(DriveWait::CONNECTION_SLOT);
    }

    pub(crate) const fn wait_for_local_buffer_for_protocol_output(&mut self) {
        self.wait_for_local_buffer_capacity_with_continuation();
    }

    const fn wait_for_local_buffer_capacity_with_continuation(&mut self) {
        self.block_on(DriveWait::LOCAL_BUFFER_CAPACITY);
        self.report.local_buffer_continuation = true;
    }

    const fn block_on(&mut self, wait: DriveWait) {
        self.report.wait.merge(wait);
    }

    pub(crate) fn apply_state_change<T>(&mut self, apply: impl FnOnce() -> T) -> Option<T> {
        if !self.step_or_exhausted() {
            return None;
        }
        let value = apply();
        self.report.state_changed();
        Some(value)
    }

    pub(crate) fn try_apply_state_change<T, E>(&mut self, apply: impl FnOnce() -> Result<T, E>) -> DriveApply<T, E> {
        if !self.step_or_exhausted() {
            return DriveApply::Deferred;
        }
        match apply() {
            Ok(value) => {
                self.report.state_changed();
                DriveApply::Applied(value)
            }
            Err(error) => DriveApply::Failed(error),
        }
    }

    pub(crate) fn apply_component_output<T>(&mut self, item: T, apply: impl FnOnce(T)) -> Result<(), T> {
        if !self.step_or_exhausted() {
            return Err(item);
        }
        apply(item);
        self.report.state_changed();
        Ok(())
    }

    pub(crate) fn push_component_output<T>(&mut self, queue: &mut Vec<T>, item: T) -> Result<(), T> {
        if queue.len() >= queue.capacity() {
            self.block_on(DriveWait::LOCAL_BUFFER_CAPACITY);
            return Err(item);
        }
        if !self.step_or_exhausted() {
            return Err(item);
        }
        queue.push(item);
        self.report.state_changed();
        Ok(())
    }

    pub(crate) fn push_component_output_after_progress<T>(&mut self, queue: &mut Vec<T>, item: T) -> Result<(), T> {
        if queue.len() >= queue.capacity() {
            self.block_on(DriveWait::LOCAL_BUFFER_CAPACITY);
            return Err(item);
        }
        queue.push(item);
        self.report.state_changed();
        Ok(())
    }

    fn protocol_chunk(&mut self, available_bytes: usize) -> Option<usize> {
        if !self.step_or_exhausted() {
            return None;
        }
        let chunk = available_bytes.min(self.remaining_bytes());
        if chunk == 0 {
            self.report.mark_budget_exhausted();
            return None;
        }
        Some(chunk)
    }

    pub(crate) fn drive_protocol_op<T, E>(
        &mut self,
        available_bytes: usize,
        operation: impl FnOnce(usize) -> Result<DriveProtocolOp<T>, E>,
    ) -> Result<DriveProtocolPoll<T>, E> {
        let Some(chunk) = self.protocol_chunk(available_bytes) else {
            return Ok(DriveProtocolPoll::Budget);
        };
        match operation(chunk)? {
            DriveProtocolOp::Progress { bytes, value } => {
                debug_assert!(bytes <= chunk);
                if bytes > 0 {
                    self.spend_bytes(bytes);
                    self.report.state_changed();
                }
                Ok(DriveProtocolPoll::Complete(value))
            }
            DriveProtocolOp::NoProgress { value } => Ok(DriveProtocolPoll::Complete(value)),
        }
    }

    pub(crate) fn take_protocol_output(
        &mut self,
        output: &mut ByteBuf,
        buffers: &BufferPool,
        max_len: usize,
    ) -> DriveProtocolPoll<Result<Option<ByteBuf>, crate::buffers::PoolExhausted>> {
        if output.is_empty() {
            return DriveProtocolPoll::Complete(Ok(None));
        }
        let Some(len) = self.protocol_chunk(output.len().min(max_len)) else {
            return DriveProtocolPoll::Budget;
        };
        let mut bytes = match buffers.try_byte_with_capacity(len) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.block_on(DriveWait::LOCAL_BUFFER_CAPACITY);
                return DriveProtocolPoll::Complete(Err(error));
            }
        };
        bytes.extend_from_slice(&output.as_slice()[..len]);
        output.as_mut_vec().drain(..len);
        self.spend_bytes(len);
        self.report.state_changed();
        DriveProtocolPoll::Complete(Ok(Some(bytes)))
    }

    pub(crate) fn queue_protocol_output(
        &mut self,
        queue: &mut WriteQueue,
        output: &mut ByteBuf,
        output_offset: &mut usize,
        buffers: &BufferPool,
        max_len: usize,
    ) -> DriveProtocolPoll<Result<DriveProtocolOutput, crate::buffers::PoolExhausted>> {
        if *output_offset >= output.len() {
            output.as_mut_vec().clear();
            *output_offset = 0;
            return DriveProtocolPoll::Complete(Ok(DriveProtocolOutput::Empty));
        }
        let available = output.len().saturating_sub(*output_offset).min(max_len);
        let Some(len) = self.protocol_chunk(available) else {
            return DriveProtocolPoll::Budget;
        };
        let mut bytes = match buffers.try_byte_with_capacity(len) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.block_on(DriveWait::LOCAL_BUFFER_CAPACITY);
                return DriveProtocolPoll::Complete(Err(error));
            }
        };
        let end = *output_offset + len;
        bytes.extend_from_slice(&output.as_slice()[*output_offset..end]);
        queue.push(bytes);
        *output_offset = end;
        if *output_offset >= output.len() {
            output.as_mut_vec().clear();
            *output_offset = 0;
        }
        self.spend_bytes(len);
        self.report.state_changed();
        DriveProtocolPoll::Complete(Ok(DriveProtocolOutput::Bytes))
    }

    pub(crate) fn read_stream_ready(
        &mut self,
        io: &mut IoSlotState,
        buffers: &BufferPool,
        stream: &mut impl Read,
    ) -> io::Result<DriveStreamRead> {
        if !io.can_read() {
            self.wait_for_reactor_read();
            return Ok(DriveStreamRead::NotReady);
        }
        match self.read_stream_unchecked(buffers, stream)? {
            DriveStreamRead::WouldBlock => {
                io.clear_read_after_would_block();
                Ok(DriveStreamRead::WouldBlock)
            }
            read => Ok(read),
        }
    }

    fn read_stream_unchecked(&mut self, buffers: &BufferPool, stream: &mut impl Read) -> io::Result<DriveStreamRead> {
        if self.remaining_bytes() == 0 {
            self.report.mark_budget_exhausted();
            return Ok(DriveStreamRead::Blocked);
        }
        let Ok(mut bytes) = buffers.try_tcp_byte() else {
            self.wait_for_local_buffer_capacity_with_continuation();
            return Ok(DriveStreamRead::Blocked);
        };
        if !self.step_or_exhausted() {
            return Ok(DriveStreamRead::Blocked);
        }
        bytes.resize_zeroed(buffers.tcp_byte_capacity().min(self.remaining_bytes()));
        match stream.read(bytes.as_mut_slice()) {
            Ok(0) => Ok(DriveStreamRead::Closed),
            Ok(len) => {
                bytes.truncate(len);
                self.spend_bytes(len);
                self.report.bytes_read(len);
                Ok(DriveStreamRead::Bytes(bytes))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.block_on(DriveWait::REACTOR_READ);
                Ok(DriveStreamRead::WouldBlock)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn write_stream_queue_ready(
        &mut self,
        io: &mut IoSlotState,
        queue: &mut WriteQueue,
        stream: &mut impl io::Write,
    ) -> io::Result<DriveStreamWrite> {
        if queue.is_empty() {
            return Ok(DriveStreamWrite::Drained);
        }
        if !io.can_write() {
            self.wait_for_reactor_write();
            return Ok(DriveStreamWrite::Pending {
                blocked_on: DriveStreamWriteBlock::ReactorWrite,
            });
        }
        let write = self.write_stream_queue_unchecked(queue, stream)?;
        if matches!(
            write,
            DriveStreamWrite::Pending {
                blocked_on: DriveStreamWriteBlock::ReactorWrite
            }
        ) {
            io.clear_write_after_would_block();
        }
        Ok(write)
    }

    fn write_stream_queue_unchecked(
        &mut self,
        queue: &mut WriteQueue,
        stream: &mut impl io::Write,
    ) -> io::Result<DriveStreamWrite> {
        let mut bytes_written = 0_usize;
        let mut blocked_on = None;
        while let Some(remaining) = queue.front_slice() {
            let write_len = remaining.len().min(self.remaining_bytes());
            if write_len == 0 {
                self.report.mark_budget_exhausted();
                blocked_on = Some(DriveStreamWriteBlock::Budget);
                break;
            }
            if !self.step_or_exhausted() {
                blocked_on = Some(DriveStreamWriteBlock::Budget);
                break;
            }
            match stream.write(&remaining[..write_len]) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(written) => {
                    bytes_written = bytes_written.saturating_add(written);
                    self.spend_bytes(written);
                    self.report.bytes_written(written);
                    queue.advance_front(written);
                    if !self.budget.can_continue() {
                        blocked_on = Some(DriveStreamWriteBlock::Budget);
                        break;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    blocked_on = Some(DriveStreamWriteBlock::ReactorWrite);
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        if queue.is_empty() {
            return Ok(DriveStreamWrite::Drained);
        }
        let blocked_on = blocked_on.unwrap_or(DriveStreamWriteBlock::ReactorWrite);
        if blocked_on == DriveStreamWriteBlock::ReactorWrite {
            self.block_on(DriveWait::REACTOR_WRITE);
        }
        Ok(DriveStreamWrite::Pending { blocked_on })
    }

    pub(crate) fn transport_read_ready<T>(
        &mut self,
        io: &mut IoSlotState,
        available_bytes: usize,
        op: impl FnOnce(usize) -> io::Result<DriveTransportOp<T>>,
    ) -> io::Result<DriveTransportPoll<T>> {
        if !io.can_read() {
            self.wait_for_reactor_read();
            return Ok(DriveTransportPoll::Pending);
        }
        self.drive_transport_op(io, available_bytes, DriveWait::REACTOR_READ, op, |io| {
            io.clear_read_after_would_block();
        })
    }

    pub(crate) fn transport_write_ready<T>(
        &mut self,
        io: &mut IoSlotState,
        available_bytes: usize,
        op: impl FnOnce(usize) -> io::Result<DriveTransportOp<T>>,
    ) -> io::Result<DriveTransportPoll<T>> {
        if !io.can_write() {
            self.wait_for_reactor_write();
            return Ok(DriveTransportPoll::Pending);
        }
        self.drive_transport_op(io, available_bytes, DriveWait::REACTOR_WRITE, op, |io| {
            io.clear_write_after_would_block();
        })
    }

    fn drive_transport_op<T>(
        &mut self,
        io: &mut IoSlotState,
        available_bytes: usize,
        wait: DriveWait,
        op: impl FnOnce(usize) -> io::Result<DriveTransportOp<T>>,
        clear_blocked: impl FnOnce(&mut IoSlotState),
    ) -> io::Result<DriveTransportPoll<T>> {
        let Some(chunk) = self.protocol_chunk(available_bytes) else {
            return Ok(DriveTransportPoll::Budget);
        };
        match op(chunk)? {
            DriveTransportOp::Progress { bytes, value } => {
                self.spend_bytes(bytes);
                if wait.contains(DriveWait::REACTOR_READ) {
                    self.report.bytes_read(bytes);
                } else {
                    self.report.bytes_written(bytes);
                }
                Ok(DriveTransportPoll::Complete(value))
            }
            DriveTransportOp::WouldBlock { value } => {
                self.block_on(wait);
                clear_blocked(io);
                Ok(DriveTransportPoll::Complete(value))
            }
            DriveTransportOp::NoProgress { value } => Ok(DriveTransportPoll::Complete(value)),
        }
    }

    pub(crate) fn recv_smoltcp_tcp(
        &mut self,
        buffers: &BufferPool,
        socket: &mut tcp::Socket<'_>,
    ) -> DriveSmoltcpTcpRecv {
        if !socket.can_recv() {
            return DriveSmoltcpTcpRecv::Empty;
        }
        if !self.can_continue_or_exhausted() {
            return DriveSmoltcpTcpRecv::Blocked;
        }
        let Ok(mut bytes) = buffers.try_tcp_byte() else {
            self.wait_for_local_buffer_capacity_with_continuation();
            return DriveSmoltcpTcpRecv::Blocked;
        };
        if !self.step_or_exhausted() {
            return DriveSmoltcpTcpRecv::Blocked;
        }
        bytes.resize_zeroed(buffers.tcp_byte_capacity().min(self.remaining_bytes()));
        match socket.recv_slice(bytes.as_mut_slice()) {
            Ok(0) | Err(_) => DriveSmoltcpTcpRecv::Empty,
            Ok(len) => {
                bytes.truncate(len);
                self.spend_bytes(len);
                self.report.guest_bytes_dequeued(len);
                DriveSmoltcpTcpRecv::Bytes(bytes)
            }
        }
    }

    pub(crate) fn send_smoltcp_tcp_queue(&mut self, queue: &mut WriteQueue, socket: &mut tcp::Socket<'_>) {
        loop {
            if !socket.can_send() {
                break;
            }
            let Some(remaining) = queue.front_slice() else {
                break;
            };
            let write_len = remaining.len().min(self.remaining_bytes());
            if write_len == 0 {
                self.report.mark_budget_exhausted();
                break;
            }
            if !self.step_or_exhausted() {
                break;
            }
            match socket.send_slice(&remaining[..write_len]) {
                Ok(0) | Err(_) => break,
                Ok(written) => {
                    self.spend_bytes(written);
                    self.report.guest_bytes_enqueued(written);
                    queue.advance_front(written);
                    if !self.budget.can_continue() {
                        break;
                    }
                }
            }
        }
        if !queue.is_empty() {
            if !self.budget_is_exhausted() {
                self.block_on(DriveWait::GUEST_SEND_CAPACITY);
            }
        } else if !socket.can_send() {
            self.block_on(DriveWait::GUEST_SEND_CAPACITY);
        }
    }

    pub(crate) fn recv_datagram_ready(
        &mut self,
        io: &mut IoSlotState,
        buffers: &BufferPool,
        socket: &impl ReactorUdpSocket,
        capacity: usize,
    ) -> io::Result<DriveDatagramRecv> {
        if !io.can_read() {
            self.wait_for_reactor_read();
            return Ok(DriveDatagramRecv::NotReady);
        }
        match self.recv_datagram_unchecked(buffers, socket, capacity)? {
            DriveDatagramRecv::WouldBlock => {
                io.clear_read_after_would_block();
                Ok(DriveDatagramRecv::WouldBlock)
            }
            recv => Ok(recv),
        }
    }

    fn recv_datagram_unchecked(
        &mut self,
        buffers: &BufferPool,
        socket: &impl ReactorUdpSocket,
        capacity: usize,
    ) -> io::Result<DriveDatagramRecv> {
        // UDP receive is whole-datagram work from the scheduler's point of
        // view. If the configured receive buffer cannot fit in this turn's
        // byte budget, do not call into the socket and risk hidden truncation
        // or budget overrun.
        if !self.start_whole_item_operation(capacity) {
            return Ok(DriveDatagramRecv::Budget);
        }
        let Ok(mut bytes) = buffers.try_byte_with_capacity(capacity) else {
            self.wait_for_local_buffer_capacity_with_continuation();
            return Ok(DriveDatagramRecv::Blocked);
        };
        bytes.resize_zeroed(capacity);
        match socket.recv(bytes.as_mut_slice()) {
            Ok(len) => {
                bytes.truncate(len);
                self.spend_bytes(len);
                self.report.bytes_read(len);
                Ok(DriveDatagramRecv::Bytes(bytes))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.block_on(DriveWait::REACTOR_READ);
                Ok(DriveDatagramRecv::WouldBlock)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn recv_datagram_from_ready(
        &mut self,
        io: &mut IoSlotState,
        buffers: &BufferPool,
        socket: &impl ReactorUdpSocket,
        capacity: usize,
    ) -> io::Result<DriveDatagramRecvFrom> {
        if !io.can_read() {
            self.wait_for_reactor_read();
            return Ok(DriveDatagramRecvFrom::NotReady);
        }
        match self.recv_datagram_from_unchecked(buffers, socket, capacity)? {
            DriveDatagramRecvFrom::WouldBlock => {
                io.clear_read_after_would_block();
                Ok(DriveDatagramRecvFrom::WouldBlock)
            }
            recv => Ok(recv),
        }
    }

    fn recv_datagram_from_unchecked(
        &mut self,
        buffers: &BufferPool,
        socket: &impl ReactorUdpSocket,
        capacity: usize,
    ) -> io::Result<DriveDatagramRecvFrom> {
        // See `recv_datagram`: readiness is only consumed after this whole
        // receive operation is admitted by the drive budget.
        if !self.start_whole_item_operation(capacity) {
            return Ok(DriveDatagramRecvFrom::Budget);
        }
        let Ok(mut bytes) = buffers.try_byte_with_capacity(capacity) else {
            self.wait_for_local_buffer_capacity_with_continuation();
            return Ok(DriveDatagramRecvFrom::Blocked);
        };
        bytes.resize_zeroed(capacity);
        match socket.recv_from(bytes.as_mut_slice()) {
            Ok((len, peer)) => {
                bytes.truncate(len);
                self.spend_bytes(len);
                self.report.bytes_read(len);
                Ok(DriveDatagramRecvFrom::Bytes { bytes, peer })
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.block_on(DriveWait::REACTOR_READ);
                Ok(DriveDatagramRecvFrom::WouldBlock)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn send_datagram_ready(
        &mut self,
        io: &mut IoSlotState,
        socket: &impl ReactorUdpSocket,
        bytes: &[u8],
    ) -> io::Result<DriveDatagramSend> {
        if !io.can_write() {
            self.wait_for_reactor_write();
            return Ok(DriveDatagramSend::NotReady);
        }
        match self.send_datagram_unchecked(socket, bytes)? {
            DriveDatagramSend::WouldBlock => {
                io.clear_write_after_would_block();
                Ok(DriveDatagramSend::WouldBlock)
            }
            send => Ok(send),
        }
    }

    fn send_datagram_unchecked(
        &mut self,
        socket: &impl ReactorUdpSocket,
        bytes: &[u8],
    ) -> io::Result<DriveDatagramSend> {
        if !self.start_whole_item_operation(bytes.len()) {
            return Ok(DriveDatagramSend::Budget);
        }
        match socket.send(bytes) {
            Ok(sent) if sent == bytes.len() => {
                self.spend_bytes(sent);
                self.report.bytes_written(sent);
                Ok(DriveDatagramSend::Sent)
            }
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.block_on(DriveWait::REACTOR_WRITE);
                Ok(DriveDatagramSend::WouldBlock)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn send_datagram_to_ready(
        &mut self,
        io: &mut IoSlotState,
        socket: &impl ReactorUdpSocket,
        bytes: &[u8],
        target: SocketAddr,
    ) -> io::Result<DriveDatagramSend> {
        if !io.can_write() {
            self.wait_for_reactor_write();
            return Ok(DriveDatagramSend::NotReady);
        }
        match self.send_datagram_to_unchecked(socket, bytes, target)? {
            DriveDatagramSend::WouldBlock => {
                io.clear_write_after_would_block();
                Ok(DriveDatagramSend::WouldBlock)
            }
            send => Ok(send),
        }
    }

    fn send_datagram_to_unchecked(
        &mut self,
        socket: &impl ReactorUdpSocket,
        bytes: &[u8],
        target: SocketAddr,
    ) -> io::Result<DriveDatagramSend> {
        if !self.start_whole_item_operation(bytes.len()) {
            return Ok(DriveDatagramSend::Budget);
        }
        match socket.send_to(bytes, target) {
            Ok(sent) if sent == bytes.len() => {
                self.spend_bytes(sent);
                self.report.bytes_written(sent);
                Ok(DriveDatagramSend::Sent)
            }
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.block_on(DriveWait::REACTOR_WRITE);
                Ok(DriveDatagramSend::WouldBlock)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn enqueue_guest_frame(
        &mut self,
        queued_frames: usize,
        queue_capacity: usize,
        frame_len: usize,
    ) -> DriveGuestFrameEnqueue {
        if queued_frames >= queue_capacity {
            self.block_on(DriveWait::GUEST_SEND_CAPACITY);
            return DriveGuestFrameEnqueue::Blocked;
        }
        if !self.start_whole_item_operation(frame_len) {
            return DriveGuestFrameEnqueue::Blocked;
        }
        self.spend_bytes(frame_len);
        self.report.guest_bytes_enqueued(frame_len);
        DriveGuestFrameEnqueue::Reserved(GuestFrameReservation { _private: () })
    }

    pub(crate) fn write_guest_frame<E>(
        &mut self,
        frame_len: usize,
        write: impl FnOnce() -> Result<bool, E>,
    ) -> Result<DriveGuestFrameWrite, E> {
        if !self.can_emit_event_or_exhausted() || !self.start_whole_item_operation(frame_len) {
            return Ok(DriveGuestFrameWrite::Budget);
        }
        if write()? {
            if !self.event_or_exhausted(frame_len) {
                return Ok(DriveGuestFrameWrite::Budget);
            }
            self.report.guest_bytes_dequeued(frame_len);
            Ok(DriveGuestFrameWrite::Flushed)
        } else {
            self.block_on(DriveWait::GUEST_SEND_CAPACITY);
            Ok(DriveGuestFrameWrite::WouldBlock)
        }
    }

    pub(crate) fn read_guest_frame<E>(
        &mut self,
        buffers: &BufferPool,
        read: impl FnOnce(&mut FrameBuf) -> Result<DriveGuestFrameReadStatus, E>,
    ) -> Result<DriveGuestFrameRead, E> {
        if !self.can_emit_event_or_exhausted()
            || !self.start_whole_item_operation(buffers.limits().frame_buffer_capacity)
        {
            return Ok(DriveGuestFrameRead::Blocked);
        }
        let Ok(mut frame) = buffers.try_guest_frame() else {
            self.wait_for_local_buffer_capacity_with_continuation();
            return Ok(DriveGuestFrameRead::Blocked);
        };
        match read(&mut frame)? {
            DriveGuestFrameReadStatus::Frame => {
                let len = frame.len();
                if !self.event_or_exhausted(len) {
                    return Ok(DriveGuestFrameRead::Blocked);
                }
                self.report.bytes_read(len);
                self.report.event_emitted();
                Ok(DriveGuestFrameRead::Frame(frame))
            }
            DriveGuestFrameReadStatus::Blocked => {
                self.block_on(DriveWait::GUEST_RECV);
                Ok(DriveGuestFrameRead::WouldBlock)
            }
            DriveGuestFrameReadStatus::Closed => {
                if !self.event_or_exhausted(0) {
                    return Ok(DriveGuestFrameRead::Blocked);
                }
                self.report.event_emitted();
                Ok(DriveGuestFrameRead::Closed)
            }
        }
    }

    const fn start_whole_item_operation(&mut self, len: usize) -> bool {
        if len > self.remaining_bytes() {
            self.report.mark_budget_exhausted();
            return false;
        }
        self.step_or_exhausted()
    }

    pub(crate) fn poll_gateway<T>(&mut self, poll_once: impl FnOnce() -> T) -> Option<T> {
        if !self.step_or_exhausted() {
            return None;
        }
        Some(poll_once())
    }

    pub(crate) const fn gateway_poll_progress(&mut self) {
        self.report.state_changed();
    }

    pub(crate) const fn gateway_frames_transmitted(&mut self, frame_count: usize) {
        if frame_count > 0 {
            self.report.state_changed();
        }
    }

    pub(crate) fn push_guest_frame(
        &mut self,
        guest_frames: &mut Vec<FrameBuf>,
        frame: FrameBuf,
    ) -> Result<(), FrameBuf> {
        let frame_len = frame.len();
        let reservation = match self.enqueue_guest_frame(guest_frames.len(), guest_frames.capacity(), frame_len) {
            DriveGuestFrameEnqueue::Reserved(reservation) => reservation,
            DriveGuestFrameEnqueue::Blocked => return Err(frame),
        };
        reservation.push_vec(guest_frames, frame);
        Ok(())
    }
}

impl DriveWait {
    const REACTOR_READ_BITS: u16 = 1 << 0;
    const REACTOR_WRITE_BITS: u16 = 1 << 1;
    const GUEST_RECV_BITS: u16 = 1 << 2;
    const GUEST_SEND_CAPACITY_BITS: u16 = 1 << 3;
    const LOCAL_BUFFER_CAPACITY_BITS: u16 = 1 << 4;
    const CONNECTION_SLOT_BITS: u16 = 1 << 5;

    pub(crate) const NONE: Self = Self(0);
    pub(crate) const REACTOR_READ: Self = Self(Self::REACTOR_READ_BITS);
    pub(crate) const REACTOR_WRITE: Self = Self(Self::REACTOR_WRITE_BITS);
    pub(crate) const REACTOR_READ_WRITE: Self = Self(Self::REACTOR_READ_BITS | Self::REACTOR_WRITE_BITS);
    pub(crate) const GUEST_RECV: Self = Self(Self::GUEST_RECV_BITS);
    pub(crate) const GUEST_SEND_CAPACITY: Self = Self(Self::GUEST_SEND_CAPACITY_BITS);
    pub(crate) const LOCAL_BUFFER_CAPACITY: Self = Self(Self::LOCAL_BUFFER_CAPACITY_BITS);
    pub(crate) const CONNECTION_SLOT: Self = Self(Self::CONNECTION_SLOT_BITS);

    pub(crate) const fn contains(self, wait: Self) -> bool {
        self.0 & wait.0 != 0
    }

    const fn merge(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DriveBudget {
    events: usize,
    steps: usize,
    bytes: usize,
}

impl DriveBudget {
    #[must_use]
    pub(crate) const fn event_loop(limits: &NetworkLimits) -> Self {
        Self {
            events: limits.drive_event_budget,
            steps: limits.drive_step_budget,
            bytes: limits.drive_byte_budget,
        }
    }

    #[must_use]
    pub(crate) const fn can_continue(&self) -> bool {
        self.events > 0 && self.steps > 0 && self.bytes > 0
    }

    const fn step(&mut self) -> bool {
        if self.steps == 0 {
            return false;
        }
        self.steps -= 1;
        true
    }

    const fn can_continue_or_exhausted(&self, report: &mut DriveReport) -> bool {
        if self.can_continue() {
            true
        } else {
            report.mark_budget_exhausted();
            false
        }
    }

    const fn step_or_exhausted(&mut self, report: &mut DriveReport) -> bool {
        if self.can_continue() && self.step() {
            true
        } else {
            report.mark_budget_exhausted();
            false
        }
    }

    const fn event_step_or_exhausted(&mut self, report: &mut DriveReport) -> bool {
        if self.events > 0 && self.steps > 0 && self.step() {
            true
        } else {
            report.mark_budget_exhausted();
            false
        }
    }

    const fn can_emit_event_or_exhausted(&self, report: &mut DriveReport) -> bool {
        if self.events > 0 && self.bytes > 0 {
            true
        } else {
            report.mark_budget_exhausted();
            false
        }
    }

    fn event(&mut self, bytes: usize) -> bool {
        if self.events == 0 || self.bytes == 0 {
            return false;
        }
        self.events -= 1;
        self.bytes = self.bytes.saturating_sub(bytes.max(1));
        true
    }

    fn event_or_exhausted(&mut self, bytes: usize, report: &mut DriveReport) -> bool {
        if self.event(bytes) {
            true
        } else {
            report.mark_budget_exhausted();
            false
        }
    }

    const fn event_only_or_exhausted(&mut self, report: &mut DriveReport) -> bool {
        if self.events > 0 {
            self.events -= 1;
            true
        } else {
            report.mark_budget_exhausted();
            false
        }
    }

    pub(crate) const fn remaining_bytes(&self) -> usize {
        self.bytes
    }

    fn consume_bytes_or_exhausted(&mut self, bytes: usize, report: &mut DriveReport) {
        self.bytes = self.bytes.saturating_sub(bytes.max(1));
        if !self.can_continue() {
            report.mark_budget_exhausted();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::net::SocketAddr;

    use crate::buffers::{BufferPool, WriteQueue};
    use crate::network::NetworkLimits;
    use crate::reactor::ReactorUdpSocket;

    use super::{
        DriveApply, DriveBudget, DriveDatagramRecv, DriveDatagramRecvFrom, DriveDatagramSend, DriveGuestFrameEnqueue,
        DriveGuestFrameRead, DriveGuestFrameWrite, DriveReport, DriveStreamRead, DriveStreamWrite,
        DriveStreamWriteBlock, DriveTurn, DriveWait,
    };

    #[test]
    fn budget_stops_after_event_byte_or_step_capacity_is_exhausted() {
        let mut budget = DriveBudget {
            events: 2,
            steps: 2,
            bytes: 8,
        };

        assert!(budget.can_continue());
        assert!(budget.step());
        assert!(budget.event(4));
        assert!(budget.can_continue());
        assert!(budget.step());
        assert!(budget.event(4));
        assert!(!budget.can_continue());
        assert!(!budget.step());
        assert!(!budget.event(1));
    }

    #[test]
    fn report_keeps_progress_waits_and_allowed_work_separate() {
        let mut report = DriveReport::new();
        let mut budget = DriveBudget {
            events: 4,
            steps: 4,
            bytes: 16,
        };
        {
            let mut drive = DriveTurn::new(&mut budget, &mut report);
            let mut events = Vec::new();
            assert!(drive.push_event(&mut events, ()).is_ok());
            drive.wait_for_local_buffer_for_protocol_output();
        }

        assert!(report.made_progress());
        assert!(report.wait().contains(DriveWait::LOCAL_BUFFER_CAPACITY));
        assert!(report.has_local_buffer_continuation());

        assert!(report.wait().contains(DriveWait::LOCAL_BUFFER_CAPACITY));
    }

    #[test]
    fn write_stream_queue_stops_at_drive_byte_budget() {
        let mut queue = WriteQueue::new();
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        queue.push(byte_buf(&buffers, b"abcdef"));
        queue.push(byte_buf(&buffers, b"ghij"));
        let mut writer = Vec::new();
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 6,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();

        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let write = drive
            .write_stream_queue_unchecked(&mut queue, &mut writer)
            .expect("flush should succeed");

        assert_eq!(
            write,
            DriveStreamWrite::Pending {
                blocked_on: DriveStreamWriteBlock::Budget,
            }
        );
        assert_eq!(writer, b"abcdef");
        assert_eq!(queue.pending_bytes(), 4);
        assert_eq!(report.progress().bytes_written, 6);
        assert!(report.budget_exhausted());
    }

    #[test]
    fn write_stream_queue_stops_at_drive_step_budget() {
        let mut queue = WriteQueue::new();
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        queue.push(byte_buf(&buffers, b"abcdef"));
        let mut writer = OneByteWriter::default();
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_step_budget: 1,
            drive_byte_budget: 16,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();

        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let write = drive
            .write_stream_queue_unchecked(&mut queue, &mut writer)
            .expect("flush should succeed");

        assert_eq!(
            write,
            DriveStreamWrite::Pending {
                blocked_on: DriveStreamWriteBlock::Budget,
            }
        );
        assert_eq!(writer.bytes, b"a");
        assert_eq!(queue.pending_bytes(), 5);
        assert_eq!(report.progress().bytes_written, 1);
        assert!(report.budget_exhausted());
    }

    #[test]
    fn write_stream_queue_reports_reactor_write_block() {
        let mut queue = WriteQueue::new();
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        queue.push(byte_buf(&buffers, b"abcdef"));
        let mut writer = WouldBlockWriter;
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut report = DriveReport::new();

        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let write = drive
            .write_stream_queue_unchecked(&mut queue, &mut writer)
            .expect("flush should not fail");

        assert_eq!(
            write,
            DriveStreamWrite::Pending {
                blocked_on: DriveStreamWriteBlock::ReactorWrite,
            }
        );
        assert_eq!(queue.pending_bytes(), 6);
        assert!(!report.made_progress());
        assert!(!report.budget_exhausted());
        assert!(report.wait().contains(DriveWait::REACTOR_WRITE));
    }

    #[test]
    fn read_stream_buffer_exhaustion_marks_local_buffer_continuation_without_reading() {
        let buffers = BufferPool::new(NetworkLimits::default());
        let mut reader = RecordingReader::default();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut report = DriveReport::new();

        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let read = drive
            .read_stream_unchecked(&buffers, &mut reader)
            .expect("buffer exhaustion should not fail");

        assert!(matches!(read, DriveStreamRead::Blocked));
        assert_eq!(reader.read_calls.get(), 0);
        assert!(report.wait().contains(DriveWait::LOCAL_BUFFER_CAPACITY));
        assert!(report.has_local_buffer_continuation());
    }

    #[test]
    fn send_datagram_to_requires_whole_datagram_byte_budget_before_side_effect() {
        let socket = RecordingUdpSocket::default();
        let target = SocketAddr::from(([127, 0, 0, 1], 4000));
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 2,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();

        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let send = drive
            .send_datagram_to_unchecked(&socket, b"hello", target)
            .expect("budget block should not fail");

        assert!(matches!(send, DriveDatagramSend::Budget));
        assert_eq!(socket.send_calls.get(), 0);
        assert!(report.budget_exhausted());
        assert!(!report.made_progress());
    }

    #[test]
    fn recv_datagram_requires_whole_datagram_byte_budget_before_side_effect() {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        let socket = RecordingUdpSocket::default();
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 2,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();

        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let recv = drive
            .recv_datagram_unchecked(&buffers, &socket, 5)
            .expect("budget block should not fail");

        assert!(matches!(recv, DriveDatagramRecv::Budget));
        assert_eq!(socket.recv_calls.get(), 0);
        assert!(report.budget_exhausted());
        assert!(!report.made_progress());
    }

    #[test]
    fn recv_datagram_buffer_exhaustion_marks_local_buffer_continuation_without_reading() {
        let buffers = BufferPool::new(NetworkLimits::default());
        let socket = RecordingUdpSocket::default();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut report = DriveReport::new();

        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let recv = drive
            .recv_datagram_unchecked(&buffers, &socket, 5)
            .expect("buffer exhaustion should not fail");

        assert!(matches!(recv, DriveDatagramRecv::Blocked));
        assert_eq!(socket.recv_calls.get(), 0);
        assert!(report.wait().contains(DriveWait::LOCAL_BUFFER_CAPACITY));
        assert!(report.has_local_buffer_continuation());
    }

    #[test]
    fn recv_datagram_from_requires_whole_datagram_byte_budget_before_side_effect() {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        let socket = RecordingUdpSocket::default();
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 2,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();

        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let recv = drive
            .recv_datagram_from_unchecked(&buffers, &socket, 5)
            .expect("budget block should not fail");

        assert!(matches!(recv, DriveDatagramRecvFrom::Budget));
        assert_eq!(socket.recv_calls.get(), 0);
        assert!(report.budget_exhausted());
        assert!(!report.made_progress());
    }

    #[test]
    fn recv_datagram_from_buffer_exhaustion_marks_local_buffer_continuation_without_reading() {
        let buffers = BufferPool::new(NetworkLimits::default());
        let socket = RecordingUdpSocket::default();
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut report = DriveReport::new();

        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let recv = drive
            .recv_datagram_from_unchecked(&buffers, &socket, 5)
            .expect("buffer exhaustion should not fail");

        assert!(matches!(recv, DriveDatagramRecvFrom::Blocked));
        assert_eq!(socket.recv_calls.get(), 0);
        assert!(report.wait().contains(DriveWait::LOCAL_BUFFER_CAPACITY));
        assert!(report.has_local_buffer_continuation());
    }

    #[test]
    fn guest_frame_enqueue_requires_whole_frame_byte_budget() {
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 4,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        let result = drive.enqueue_guest_frame(0, 8, 5);

        assert!(matches!(result, DriveGuestFrameEnqueue::Blocked));
        assert!(report.budget_exhausted());
        assert!(!report.made_progress());
    }

    #[test]
    fn guest_frame_write_requires_whole_frame_byte_budget_before_side_effect() {
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 4,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let mut called = false;

        let result = drive
            .write_guest_frame(5, || -> Result<bool, std::convert::Infallible> {
                called = true;
                Ok(true)
            })
            .expect("infallible write should not fail");

        assert!(matches!(result, DriveGuestFrameWrite::Budget));
        assert!(!called);
        assert!(report.budget_exhausted());
        assert!(!report.made_progress());
    }

    #[test]
    fn guest_frame_write_requires_event_budget_before_side_effect() {
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_event_budget: 0,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let mut called = false;

        let result = drive
            .write_guest_frame(5, || -> Result<bool, std::convert::Infallible> {
                called = true;
                Ok(true)
            })
            .expect("infallible write should not fail");

        assert!(matches!(result, DriveGuestFrameWrite::Budget));
        assert!(!called);
        assert!(report.budget_exhausted());
        assert!(!report.made_progress());
    }

    #[test]
    fn guest_frame_read_requires_frame_capacity_budget_before_side_effect() {
        let buffers = BufferPool::new(NetworkLimits {
            frame_buffer_capacity: 8,
            ..NetworkLimits::default()
        });
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_byte_budget: 7,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let mut called = false;

        let result = drive
            .read_guest_frame(&buffers, |_frame| -> Result<_, std::convert::Infallible> {
                called = true;
                Ok(super::DriveGuestFrameReadStatus::Frame)
            })
            .expect("infallible read should not fail");

        assert!(matches!(result, DriveGuestFrameRead::Blocked));
        assert!(!called);
        assert!(report.budget_exhausted());
        assert!(!report.made_progress());
    }

    #[test]
    fn guest_frame_read_requires_event_budget_before_side_effect() {
        let buffers = BufferPool::new(NetworkLimits::default());
        let mut budget = DriveBudget::event_loop(&NetworkLimits {
            drive_event_budget: 0,
            ..NetworkLimits::default()
        });
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let mut called = false;

        let result = drive
            .read_guest_frame(&buffers, |_frame| -> Result<_, std::convert::Infallible> {
                called = true;
                Ok(super::DriveGuestFrameReadStatus::Frame)
            })
            .expect("infallible read should not fail");

        assert!(matches!(result, DriveGuestFrameRead::Blocked));
        assert!(!called);
        assert!(report.budget_exhausted());
        assert!(!report.made_progress());
    }

    #[test]
    fn guest_frame_read_buffer_exhaustion_marks_local_buffer_continuation_without_reading() {
        let buffers = BufferPool::new(NetworkLimits::default());
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);
        let mut called = false;

        let result = drive
            .read_guest_frame(&buffers, |_frame| -> Result<_, std::convert::Infallible> {
                called = true;
                Ok(super::DriveGuestFrameReadStatus::Frame)
            })
            .expect("infallible read should not fail");

        assert!(matches!(result, DriveGuestFrameRead::Blocked));
        assert!(!called);
        assert!(report.wait().contains(DriveWait::LOCAL_BUFFER_CAPACITY));
        assert!(report.has_local_buffer_continuation());
    }

    #[test]
    fn failed_state_change_is_not_reported_as_progress() {
        let mut budget = DriveBudget::event_loop(&NetworkLimits::default());
        let mut report = DriveReport::new();
        let mut drive = DriveTurn::new(&mut budget, &mut report);

        let result = drive.try_apply_state_change(|| -> Result<(), &'static str> { Err("reregister failed") });

        assert!(matches!(result, DriveApply::Failed("reregister failed")));
        assert!(!report.made_progress());
        assert!(!report.budget_exhausted());
    }

    fn byte_buf(buffers: &BufferPool, bytes: &[u8]) -> crate::buffers::ByteBuf {
        let mut output = buffers
            .try_byte_with_capacity(bytes.len())
            .expect("prewarmed byte buffer");
        output.extend_from_slice(bytes);
        output
    }

    #[derive(Default)]
    struct OneByteWriter {
        bytes: Vec<u8>,
    }

    impl std::io::Write for OneByteWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let Some(byte) = buf.first() else {
                return Ok(0);
            };
            self.bytes.push(*byte);
            Ok(1)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct WouldBlockWriter;

    impl std::io::Write for WouldBlockWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingReader {
        read_calls: Cell<usize>,
    }

    impl std::io::Read for RecordingReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.read_calls.set(self.read_calls.get() + 1);
            Ok(buffer.len())
        }
    }

    #[derive(Default)]
    struct RecordingUdpSocket {
        send_calls: Cell<usize>,
        recv_calls: Cell<usize>,
    }

    impl ReactorUdpSocket for RecordingUdpSocket {
        fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
            Ok(Self::default())
        }

        fn from_std(_socket: std::net::UdpSocket) -> Self {
            Self::default()
        }

        fn send(&self, bytes: &[u8]) -> std::io::Result<usize> {
            self.send_calls.set(self.send_calls.get() + 1);
            Ok(bytes.len())
        }

        fn recv(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.recv_calls.set(self.recv_calls.get() + 1);
            Ok(buffer.len())
        }

        fn send_to(&self, bytes: &[u8], _target: SocketAddr) -> std::io::Result<usize> {
            self.send_calls.set(self.send_calls.get() + 1);
            Ok(bytes.len())
        }

        fn recv_from(&self, buffer: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            self.recv_calls.set(self.recv_calls.get() + 1);
            Ok((buffer.len(), SocketAddr::from(([127, 0, 0, 1], 4000))))
        }

        fn local_addr(&self) -> std::io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
        }
    }
}
