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
}

impl FrameDevice {
    pub(crate) fn new(mtu: usize, buffers: BufferPool, queue_capacity: usize) -> Self {
        Self {
            mtu,
            buffers,
            queue_capacity,
            rx: VecDeque::with_capacity(queue_capacity),
            tx: VecDeque::with_capacity(queue_capacity),
        }
    }

    pub(crate) fn receive_frame(&mut self, frame: FrameBuf) -> bool {
        if self.rx.len() >= self.queue_capacity {
            return false;
        }
        self.rx.push_back(frame);
        true
    }

    pub(crate) fn take_transmitted_frames(&mut self) -> impl Iterator<Item = FrameBuf> + '_ {
        std::iter::from_fn(|| self.tx.pop_front())
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
        let frame = self.rx.pop_front()?;
        let tx_frame = self
            .buffers
            .try_frame_with_capacity(self.mtu + ETHERNET_HEADER_LEN)
            .ok()?;
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
        let transmitted = device
            .take_transmitted_frames()
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
