use std::cell::RefCell;
use std::collections::BTreeMap;
use std::os::windows::io::{AsRawSocket as _, BorrowedSocket, RawSocket};

use ::mio::{Interest, Registry, Token};

use crate::guest::{GuestIoSource, TransportError};
use crate::reactor::ReactorItemId;

pub(super) struct GuestSources {
    sockets: RefCell<BTreeMap<ReactorItemId, GuestSocketSource>>,
}

impl GuestSources {
    pub(super) const fn new() -> Self {
        Self {
            sockets: RefCell::new(BTreeMap::new()),
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
        match source {
            GuestIoSource::Socket(socket) => self.register_socket(registry, socket, item, token, interest),
            GuestIoSource::Handle(_handle) => Err(unsupported_guest_handle_source("register guest frame session")),
        }
    }

    pub(super) fn reregister(
        &self,
        registry: &Registry,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
        token: Token,
        interest: Interest,
    ) -> Result<(), TransportError> {
        match source {
            GuestIoSource::Socket(socket) => self.reregister_socket(registry, socket, item, token, interest),
            GuestIoSource::Handle(_handle) => Err(unsupported_guest_handle_source("reregister guest frame session")),
        }
    }

    pub(super) fn deregister(
        &self,
        registry: &Registry,
        source: GuestIoSource<'_>,
        item: ReactorItemId,
    ) -> Result<(), TransportError> {
        match source {
            GuestIoSource::Socket(socket) => self.deregister_socket(registry, socket, item),
            GuestIoSource::Handle(_handle) => Err(unsupported_guest_handle_source("deregister guest frame session")),
        }
    }

    fn register_socket(
        &self,
        registry: &Registry,
        socket: BorrowedSocket<'_>,
        item: ReactorItemId,
        token: Token,
        interest: Interest,
    ) -> Result<(), TransportError> {
        let mut source = GuestSocketSource::new(socket);
        registry
            .register(&mut source.inner, token, interest)
            .map_err(|error| TransportError::operation("register guest frame session", error))?;
        self.sockets.borrow_mut().insert(item, source);
        Ok(())
    }

    fn reregister_socket(
        &self,
        registry: &Registry,
        socket: BorrowedSocket<'_>,
        item: ReactorItemId,
        token: Token,
        interest: Interest,
    ) -> Result<(), TransportError> {
        let mut sources = self.sockets.borrow_mut();
        let source = sources.get_mut(&item).ok_or_else(|| {
            TransportError::operation("reregister guest frame session", "guest socket is not registered")
        })?;
        if !source.matches(socket) {
            return Err(TransportError::operation(
                "reregister guest frame session",
                "guest socket source changed after registration",
            ));
        }
        registry
            .reregister(&mut source.inner, token, interest)
            .map_err(|error| TransportError::operation("reregister guest frame session", error))
    }

    fn deregister_socket(
        &self,
        registry: &Registry,
        socket: BorrowedSocket<'_>,
        item: ReactorItemId,
    ) -> Result<(), TransportError> {
        let mut sources = self.sockets.borrow_mut();
        let Some(mut source) = sources.remove(&item) else {
            return Ok(());
        };
        if !source.matches(socket) {
            return Err(TransportError::operation(
                "deregister guest frame session",
                "guest socket source changed after registration",
            ));
        }
        registry
            .deregister(&mut source.inner)
            .map_err(|error| TransportError::operation("deregister guest frame session", error))
    }
}

struct GuestSocketSource {
    raw: RawSocket,
    inner: ::mio::IoSource<GuestSocket>,
}

impl GuestSocketSource {
    fn new(socket: BorrowedSocket<'_>) -> Self {
        let raw = socket.as_raw_socket();
        Self {
            raw,
            inner: ::mio::IoSource::new(GuestSocket { raw }),
        }
    }

    fn matches(&self, socket: BorrowedSocket<'_>) -> bool {
        self.raw == socket.as_raw_socket()
    }
}

struct GuestSocket {
    raw: RawSocket,
}

impl std::os::windows::io::AsRawSocket for GuestSocket {
    fn as_raw_socket(&self) -> RawSocket {
        self.raw
    }
}

fn unsupported_guest_handle_source(operation: &'static str) -> TransportError {
    TransportError::operation(
        operation,
        "Mio guest source registration for Windows handles is not implemented yet",
    )
}
