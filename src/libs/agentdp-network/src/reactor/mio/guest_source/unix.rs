use ::mio::{Interest, Registry, Token};
use std::os::fd::AsRawFd as _;

use crate::guest::{GuestIoSource, TransportError};
use crate::reactor::ReactorItemId;

pub(super) struct GuestSources;

impl GuestSources {
    pub(super) const fn new() -> Self {
        Self
    }

    #[allow(clippy::unused_self)]
    pub(super) fn register(
        &self,
        registry: &Registry,
        source: GuestIoSource<'_>,
        _item: ReactorItemId,
        token: Token,
        interest: Interest,
    ) -> Result<(), TransportError> {
        let GuestIoSource::Fd(fd) = source;

        let raw_fd = fd.as_raw_fd();
        let mut source = mio::unix::SourceFd(&raw_fd);
        registry
            .register(&mut source, token, interest)
            .map_err(|error| TransportError::operation("register guest frame session", error))
    }

    #[allow(clippy::unused_self)]
    pub(super) fn reregister(
        &self,
        registry: &Registry,
        source: GuestIoSource<'_>,
        _item: ReactorItemId,
        token: Token,
        interest: Interest,
    ) -> Result<(), TransportError> {
        let GuestIoSource::Fd(fd) = source;

        let raw_fd = fd.as_raw_fd();
        let mut source = mio::unix::SourceFd(&raw_fd);
        registry
            .reregister(&mut source, token, interest)
            .map_err(|error| TransportError::operation("reregister guest frame session", error))
    }

    #[allow(clippy::unused_self)]
    pub(super) fn deregister(
        &self,
        registry: &Registry,
        source: GuestIoSource<'_>,
        _item: ReactorItemId,
    ) -> Result<(), TransportError> {
        let GuestIoSource::Fd(fd) = source;

        let raw_fd = fd.as_raw_fd();
        let mut source = mio::unix::SourceFd(&raw_fd);
        registry
            .deregister(&mut source)
            .map_err(|error| TransportError::operation("deregister guest frame session", error))
    }
}
