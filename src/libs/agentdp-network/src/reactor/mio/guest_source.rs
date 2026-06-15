use ::mio::{Interest, Registry, Token};

use crate::guest::{GuestIoSource, TransportError};
use crate::reactor::ReactorItemId;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
use unix as sys;
#[cfg(windows)]
use windows as sys;

pub(super) struct GuestSources {
    inner: sys::GuestSources,
}

impl GuestSources {
    pub(super) const fn new() -> Self {
        Self {
            inner: sys::GuestSources::new(),
        }
    }

    pub(super) fn register(
        &self,
        registry: &Registry,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
        token: Token,
        interest: Interest,
    ) -> Result<(), TransportError> {
        self.inner.register(registry, source, item, token, interest)
    }

    pub(super) fn reregister(
        &self,
        registry: &Registry,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
        token: Token,
        interest: Interest,
    ) -> Result<(), TransportError> {
        self.inner.reregister(registry, source, item, token, interest)
    }

    pub(super) fn deregister(
        &self,
        registry: &Registry,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
    ) -> Result<(), TransportError> {
        self.inner.deregister(registry, source, item)
    }
}
