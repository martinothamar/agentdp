use std::cell::RefCell;
use std::io;
use std::rc::Rc;

use agentdp_network::test_support::simulation::{SimUdpHandler, SimUdpHandlerFn, SimUdpResponse};

pub(crate) fn udp_response_handler(
    handler: impl FnMut(&[u8]) -> io::Result<SimUdpResponse> + 'static,
) -> SimUdpHandler {
    Rc::new(RefCell::new(ResponseHandler { handler }))
}

struct ResponseHandler<F> {
    handler: F,
}

impl<F> SimUdpHandlerFn for ResponseHandler<F>
where
    F: FnMut(&[u8]) -> io::Result<SimUdpResponse>,
{
    fn handle(&mut self, bytes: &[u8]) -> io::Result<SimUdpResponse> {
        (self.handler)(bytes)
    }
}
