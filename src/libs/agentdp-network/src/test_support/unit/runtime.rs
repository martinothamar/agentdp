use crate::buffers::FrameBuf;
use crate::clock::SystemClock;
use crate::connectors::tcp::ProductionTcpConnector;
use crate::connectors::udp::ProductionUdpSocketFactory;
use crate::guest::{
    ConnectStatus, FrameRead, FrameWrite, GuestFrameSession, GuestFrameTransport, GuestIoSource, TransportError,
};
use crate::reactor::MioReactor;
use crate::runtime::RuntimeContext;

#[derive(Debug, Clone, Copy)]
pub(crate) struct UnusedTransport;

pub(crate) struct UnusedSession;

impl GuestFrameTransport for UnusedTransport {
    type Session = UnusedSession;

    fn try_connect(&mut self) -> Result<ConnectStatus<Self::Session>, TransportError> {
        Err(TransportError::operation(
            "unused test transport",
            "test runtime must not connect guest transport",
        ))
    }

    fn cleanup(self) -> Result<(), TransportError> {
        Ok(())
    }

    fn describe(&self) -> String {
        "unused test transport".to_owned()
    }
}

impl GuestFrameSession for UnusedSession {
    fn io_source(&mut self) -> GuestIoSource<'_> {
        unreachable!("unused test transport never creates sessions")
    }

    fn read_frame_into(&mut self, _frame: &mut FrameBuf) -> Result<FrameRead, TransportError> {
        Ok(FrameRead::Blocked)
    }

    fn write_frame(&mut self, _frame: &[u8]) -> Result<FrameWrite, TransportError> {
        Ok(FrameWrite::Blocked)
    }

    fn shutdown_write(&mut self) -> Result<(), TransportError> {
        Ok(())
    }
}

pub(crate) fn runtime_context(
    reactor: MioReactor,
) -> RuntimeContext<UnusedTransport, MioReactor, SystemClock, ProductionTcpConnector, ProductionUdpSocketFactory> {
    RuntimeContext::new(
        UnusedTransport,
        reactor,
        SystemClock,
        ProductionTcpConnector,
        ProductionUdpSocketFactory,
    )
}
