use std::collections::VecDeque;
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::time::Instant as SmoltcpInstant;
use smoltcp::wire::ETHERNET_HEADER_LEN;

use crate::buffers::{BufferPool, FrameBuf};

#[derive(Debug)]
pub(crate) struct FrameDevice {
    mtu: usize,
    buffers: BufferPool,
    queue_capacity: usize,
    rx: VecDeque<FrameBuf>,
    tx: VecDeque<FrameBuf>,
    rx_budget: usize,
}

impl FrameDevice {
    pub(crate) fn new(mtu: usize, buffers: BufferPool, queue_capacity: usize) -> Self {
        Self {
            mtu,
            buffers,
            queue_capacity,
            rx: VecDeque::with_capacity(queue_capacity),
            tx: VecDeque::with_capacity(queue_capacity),
            rx_budget: usize::MAX,
        }
    }

    pub(crate) fn receive_frame(&mut self, frame: FrameBuf) -> bool {
        if self.rx.len() >= self.queue_capacity {
            return false;
        }
        self.rx.push_back(frame);
        true
    }

    pub(crate) fn can_receive_frame(&self) -> bool {
        self.rx.len() < self.queue_capacity
    }

    pub(crate) fn has_received_frames(&self) -> bool {
        !self.rx.is_empty()
    }

    pub(crate) fn received_frame_count(&self) -> usize {
        self.rx.len()
    }

    pub(crate) fn pop_transmitted_frame(&mut self) -> Option<FrameBuf> {
        self.tx.pop_front()
    }

    pub(crate) fn next_transmitted_frame_len(&self) -> Option<usize> {
        self.tx.front().map(FrameBuf::len)
    }

    pub(crate) fn has_transmitted_frames(&self) -> bool {
        !self.tx.is_empty()
    }

    pub(crate) fn transmitted_frame_count(&self) -> usize {
        self.tx.len()
    }

    pub(crate) const fn allow_one_receive(&mut self) {
        self.rx_budget = 1;
    }
}

pub(crate) struct FrameRxToken {
    frame: FrameBuf,
}

pub(crate) struct FrameTxToken<'a> {
    device: &'a mut FrameDevice,
    frame: FrameBuf,
}

impl Device for FrameDevice {
    type RxToken<'a> = FrameRxToken;
    type TxToken<'a> = FrameTxToken<'a>;

    fn receive(&mut self, _timestamp: SmoltcpInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.tx.len() >= self.queue_capacity {
            return None;
        }
        if self.rx_budget == 0 {
            return None;
        }
        if self.rx.is_empty() {
            return None;
        }
        let tx_frame = self
            .buffers
            .try_frame_with_capacity(self.mtu + ETHERNET_HEADER_LEN)
            .ok()?;
        let frame = self.rx.pop_front()?;
        self.rx_budget = self.rx_budget.saturating_sub(1);
        Some((
            FrameRxToken { frame },
            FrameTxToken {
                device: self,
                frame: tx_frame,
            },
        ))
    }

    fn transmit(&mut self, _timestamp: SmoltcpInstant) -> Option<Self::TxToken<'_>> {
        if self.tx.len() >= self.queue_capacity {
            return None;
        }
        let frame = self
            .buffers
            .try_frame_with_capacity(self.mtu + ETHERNET_HEADER_LEN)
            .ok()?;
        Some(FrameTxToken { device: self, frame })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ethernet;
        caps.max_transmission_unit = self.mtu + ETHERNET_HEADER_LEN;
        caps
    }
}

impl RxToken for FrameRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(self.frame.as_slice())
    }
}

impl TxToken for FrameTxToken<'_> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut frame = self.frame;
        frame.resize_zeroed(len);
        let result = f(frame.as_mut_vec());
        self.device.tx.push_back(frame);
        result
    }
}

#[cfg(test)]
pub(crate) fn smoltcp_now() -> SmoltcpInstant {
    SmoltcpInstant::from_micros(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_micros().try_into().unwrap_or(i64::MAX)),
    )
}

#[cfg(test)]
mod tests {
    use smoltcp::phy::{Device, RxToken, TxToken};
    use smoltcp::wire::ETHERNET_HEADER_LEN;

    use crate::buffers::BufferPool;
    use crate::network::NetworkLimits;

    use super::{FrameDevice, smoltcp_now};

    #[test]
    fn device_receives_and_transmits_frames() -> Result<(), Box<dyn std::error::Error>> {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        let mut device = FrameDevice::new(1500, buffers.clone(), 512);
        let mut frame = buffers.try_frame().expect("prewarmed frame");
        frame.as_mut_vec().extend_from_slice(b"frame");
        assert!(device.receive_frame(frame));

        let (rx, tx) = device.receive(smoltcp_now()).ok_or("expected queued receive frame")?;
        let received = rx.consume(<[u8]>::to_vec);
        assert_eq!(received, b"frame");

        tx.consume(4, |bytes| bytes.copy_from_slice(b"pong"));
        let transmitted = std::iter::from_fn(|| device.pop_transmitted_frame())
            .map(|frame| frame.as_slice().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(transmitted, vec![b"pong".to_vec()]);
        Ok(())
    }

    #[test]
    fn capabilities_include_ethernet_header_in_mtu() {
        let device = FrameDevice::new(1400, BufferPool::default(), 512);
        let capabilities = device.capabilities();

        assert_eq!(capabilities.max_transmission_unit, 1400 + ETHERNET_HEADER_LEN);
    }

    #[test]
    fn receive_frame_respects_queue_capacity() {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        let mut device = FrameDevice::new(1500, buffers.clone(), 1);

        let mut first = buffers.try_frame().expect("prewarmed frame");
        first.as_mut_vec().extend_from_slice(b"first");
        let mut second = buffers.try_frame().expect("prewarmed frame");
        second.as_mut_vec().extend_from_slice(b"second");

        assert!(device.receive_frame(first));
        assert!(!device.receive_frame(second));
    }

    #[test]
    fn receive_budget_limits_rx_frames_per_poll_call() {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        let mut device = FrameDevice::new(1500, buffers.clone(), 4);
        let mut first = buffers.try_frame().expect("prewarmed frame");
        first.as_mut_vec().extend_from_slice(b"first");
        let mut second = buffers.try_frame().expect("prewarmed frame");
        second.as_mut_vec().extend_from_slice(b"second");
        assert!(device.receive_frame(first));
        assert!(device.receive_frame(second));

        device.allow_one_receive();
        assert!(device.receive(smoltcp_now()).is_some());
        assert!(device.receive(smoltcp_now()).is_none());

        device.allow_one_receive();
        assert!(device.receive(smoltcp_now()).is_some());
    }

    #[test]
    fn receive_keeps_rx_frame_when_tx_buffer_allocation_fails() -> Result<(), Box<dyn std::error::Error>> {
        let buffers = BufferPool::new(NetworkLimits {
            frame_buffer_pool_capacity: 2,
            ..NetworkLimits::default()
        });
        buffers.prewarm_instance_network();
        let mut device = FrameDevice::new(1500, buffers.clone(), 1);
        let mut frame = buffers.try_frame().expect("prewarmed frame");
        frame.as_mut_vec().extend_from_slice(b"saved");
        let held = buffers.try_frame().expect("prewarmed frame");
        assert!(device.receive_frame(frame));

        assert!(device.receive(smoltcp_now()).is_none());
        drop(held);
        let (rx, _tx) = device
            .receive(smoltcp_now())
            .ok_or("expected RX frame to remain queued")?;

        assert_eq!(rx.consume(<[u8]>::to_vec), b"saved");
        Ok(())
    }

    #[test]
    fn transmit_waits_when_output_queue_is_full() {
        let buffers = BufferPool::default();
        buffers.prewarm_instance_network();
        let mut device = FrameDevice::new(1500, buffers, 1);

        let tx = device
            .transmit(smoltcp_now())
            .expect("first TX token should be available");
        tx.consume(4, |bytes| bytes.copy_from_slice(b"full"));

        assert!(device.transmit(smoltcp_now()).is_none());
    }
}
