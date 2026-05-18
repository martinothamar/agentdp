#![cfg(windows)]

use std::io::{Read, Write};
use std::mem::MaybeUninit;
use std::path::Path;
use std::sync::OnceLock;

use windows_sys::Win32::Networking::WinSock::{
    AF_UNIX, FIONBIO, INVALID_SOCKET, SOCK_STREAM, SOCKADDR, SOCKADDR_UN, SOCKET, SOCKET_ERROR, WSADATA,
    WSAGetLastError, WSAStartup, accept, bind, closesocket, connect, ioctlsocket, listen, recv, send, socket,
};

const BACKLOG: i32 = 128;
const WINSOCK_VERSION_2_2: u16 = 0x0202;

static WINSOCK: OnceLock<i32> = OnceLock::new();

pub struct UnixStream {
    socket: Socket,
}

impl UnixStream {
    /// Connects to a Windows `AF_UNIX` socket.
    ///
    /// # Errors
    ///
    /// Returns an error if WinSock cannot be initialized, the socket cannot be
    /// created, the path is invalid, or the connection fails.
    pub fn connect(path: &Path) -> std::io::Result<Self> {
        startup()?;
        let socket = Socket::new()?;
        let address = SocketAddress::new(path)?;
        // SAFETY: `socket.raw` is a valid WinSock socket, and `address` points
        // to an initialized `SOCKADDR_UN` that remains alive for the call.
        let result = unsafe { connect(socket.raw, address.as_sockaddr(), address.len()) };
        if result == SOCKET_ERROR {
            return Err(last_error());
        }
        Ok(Self { socket })
    }

    fn from_raw(raw: SOCKET) -> Self {
        Self { socket: Socket { raw } }
    }
}

impl Read for UnixStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let len = i32::try_from(buffer.len()).unwrap_or(i32::MAX);
        // SAFETY: `buffer` is valid for writes of `len` bytes, and the socket
        // is owned by this stream.
        let result = unsafe { recv(self.socket.raw, buffer.as_mut_ptr(), len, 0) };
        if result == SOCKET_ERROR {
            Err(last_error())
        } else {
            Ok(result as usize)
        }
    }
}

impl Write for UnixStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let len = i32::try_from(buffer.len()).unwrap_or(i32::MAX);
        // SAFETY: `buffer` is valid for reads of `len` bytes, and the socket is
        // owned by this stream.
        let result = unsafe { send(self.socket.raw, buffer.as_ptr(), len, 0) };
        if result == SOCKET_ERROR {
            Err(last_error())
        } else {
            Ok(result as usize)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct UnixListener {
    socket: Socket,
}

impl UnixListener {
    /// Binds a Windows `AF_UNIX` listener.
    ///
    /// # Errors
    ///
    /// Returns an error if WinSock cannot be initialized, the socket cannot be
    /// created, the path is invalid, bind fails, or listen fails.
    pub fn bind(path: &Path) -> std::io::Result<Self> {
        startup()?;
        let socket = Socket::new()?;
        let address = SocketAddress::new(path)?;
        // SAFETY: `socket.raw` is a valid WinSock socket, and `address` points
        // to an initialized `SOCKADDR_UN` that remains alive for the call.
        let bind_result = unsafe { bind(socket.raw, address.as_sockaddr(), address.len()) };
        if bind_result == SOCKET_ERROR {
            return Err(last_error());
        }
        // SAFETY: `socket.raw` is a valid bound WinSock socket.
        let listen_result = unsafe { listen(socket.raw, BACKLOG) };
        if listen_result == SOCKET_ERROR {
            return Err(last_error());
        }
        Ok(Self { socket })
    }

    /// Accepts one stream.
    ///
    /// # Errors
    ///
    /// Returns an error if accepting from the listener fails.
    pub fn accept(&self) -> std::io::Result<UnixStream> {
        // SAFETY: `self.socket.raw` is a valid listening WinSock socket. We do
        // not need the peer address for this local transport.
        let raw = unsafe { accept(self.socket.raw, std::ptr::null_mut(), std::ptr::null_mut()) };
        if raw == INVALID_SOCKET {
            Err(last_error())
        } else {
            Ok(UnixStream::from_raw(raw))
        }
    }

    /// Sets nonblocking mode on the listener.
    ///
    /// # Errors
    ///
    /// Returns an error if WinSock cannot change the socket mode.
    pub fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()> {
        let mut value = u32::from(nonblocking);
        // SAFETY: `self.socket.raw` is a valid WinSock socket, and `value` is a
        // valid pointer for `ioctlsocket` to read.
        let result = unsafe { ioctlsocket(self.socket.raw, FIONBIO, &mut value) };
        if result == SOCKET_ERROR {
            Err(last_error())
        } else {
            Ok(())
        }
    }
}

struct Socket {
    raw: SOCKET,
}

impl Socket {
    fn new() -> std::io::Result<Self> {
        // SAFETY: `socket` is called with constant parameters for a stream
        // `AF_UNIX` socket.
        let raw = unsafe { socket(AF_UNIX.into(), SOCK_STREAM, 0) };
        if raw == INVALID_SOCKET {
            Err(last_error())
        } else {
            Ok(Self { raw })
        }
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // SAFETY: this type owns `raw`, and closing an already-created WinSock
        // socket is the required cleanup operation.
        unsafe {
            closesocket(self.raw);
        }
    }
}

struct SocketAddress {
    inner: SOCKADDR_UN,
}

impl SocketAddress {
    fn new(path: &Path) -> std::io::Result<Self> {
        let path = path.to_string_lossy();
        let bytes = path.as_bytes();
        if bytes.len() >= 108 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Windows AF_UNIX socket path is too long: {path}"),
            ));
        }

        let mut inner = SOCKADDR_UN {
            sun_family: AF_UNIX,
            sun_path: [0; 108],
        };
        for (destination, source) in inner.sun_path.iter_mut().zip(bytes.iter().copied()) {
            *destination = source.cast_signed();
        }
        Ok(Self { inner })
    }

    fn as_sockaddr(&self) -> *const SOCKADDR {
        std::ptr::from_ref(&self.inner).cast::<SOCKADDR>()
    }

    fn len(&self) -> i32 {
        size_of::<SOCKADDR_UN>() as i32
    }
}

fn startup() -> std::io::Result<()> {
    let result = *WINSOCK.get_or_init(|| {
        let mut data = MaybeUninit::<WSADATA>::uninit();
        // SAFETY: `data` points to valid uninitialized storage for WinSock
        // to initialize.
        unsafe { WSAStartup(WINSOCK_VERSION_2_2, data.as_mut_ptr()) }
    });
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::from_raw_os_error(result))
    }
}

fn last_error() -> std::io::Error {
    // SAFETY: `WSAGetLastError` has no preconditions.
    std::io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}
