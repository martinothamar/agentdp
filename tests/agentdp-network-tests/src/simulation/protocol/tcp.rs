use std::cell::RefCell;
use std::io;
use std::rc::Rc;

use agentdp_network::test_support::simulation::{SimTcpHandler, SimTcpHandlerFn, SimTcpResponse};

pub(crate) fn tcp_handler(handler: impl FnMut(&[u8], &mut Vec<u8>) -> io::Result<()> + 'static) -> SimTcpHandler {
    Rc::new(RefCell::new(VecOutputHandler { handler }))
}

pub(crate) fn tcp_response_handler(
    handler: impl FnMut(&[u8]) -> io::Result<SimTcpResponse> + 'static,
) -> SimTcpHandler {
    Rc::new(RefCell::new(ResponseHandler { handler }))
}

struct VecOutputHandler<F> {
    handler: F,
}

impl<F> SimTcpHandlerFn for VecOutputHandler<F>
where
    F: FnMut(&[u8], &mut Vec<u8>) -> io::Result<()>,
{
    fn handle(&mut self, bytes: &[u8]) -> io::Result<SimTcpResponse> {
        let mut output = Vec::new();
        (self.handler)(bytes, &mut output)?;
        Ok(SimTcpResponse::bytes(output))
    }
}

struct ResponseHandler<F> {
    handler: F,
}

impl<F> SimTcpHandlerFn for ResponseHandler<F>
where
    F: FnMut(&[u8]) -> io::Result<SimTcpResponse>,
{
    fn handle(&mut self, bytes: &[u8]) -> io::Result<SimTcpResponse> {
        (self.handler)(bytes)
    }
}
