#![allow(unsafe_code)]

use std::io::{Read, Write};
use std::mem::MaybeUninit;
use std::os::windows::io::{AsRawSocket, AsSocket, BorrowedSocket, RawSocket};
use std::path::Path;
use std::sync::OnceLock;

use windows_sys::Win32::Networking::WinSock::{
    AF_UNIX, FIONBIO, INVALID_SOCKET, SD_SEND, SOCK_STREAM, SOCKADDR, SOCKADDR_UN, SOCKET, SOCKET_ERROR, WSADATA,
    WSAEWOULDBLOCK, WSAGetLastError, WSAStartup, accept, bind, closesocket, connect, ioctlsocket, listen, recv, send,
    shutdown, socket,
};

const BACKLOG: i32 = 128;
const WINSOCK_VERSION_2_2: u16 = 0x0202;
const SOCKADDR_UN_PATH_OFFSET: usize = 2;

static WINSOCK: OnceLock<i32> = OnceLock::new();

#[derive(Debug)]
pub(crate) struct UnixStream {
    socket: Socket,
}

impl UnixStream {
    /// Connects to a Windows `AF_UNIX` socket.
    ///
    /// # Errors
    ///
    /// Returns an error if `WinSock` cannot be initialized, the socket cannot be
    /// created, the path is invalid, or the connection fails.
    pub(crate) fn connect(path: &Path) -> std::io::Result<Self> {
        startup()?;
        let socket = Socket::new()?;
        let address = SocketAddress::new(path)?;
        // SAFETY: `socket.raw` is a valid WinSock socket, and `address` points
        // to an initialized `SOCKADDR_UN` that remains alive for the call.
        let result = unsafe { connect(socket.raw, address.as_sockaddr(), address.len()) };
        if result == SOCKET_ERROR {
            return Err(last_error());
        }
        socket.set_nonblocking(true)?;
        Ok(Self { socket })
    }

    const fn from_raw(raw: SOCKET) -> Self {
        Self { socket: Socket { raw } }
    }

    pub(crate) fn read(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.socket.read(buffer)
    }

    pub(crate) fn write(&self, buffer: &[u8]) -> std::io::Result<usize> {
        self.socket.write(buffer)
    }

    pub(crate) fn shutdown_write(&self) -> std::io::Result<()> {
        // SAFETY: `self.socket.raw` is a valid owned WinSock socket.
        let result = unsafe { shutdown(self.socket.raw, SD_SEND) };
        if result == SOCKET_ERROR {
            Err(last_error())
        } else {
            Ok(())
        }
    }
}

impl AsSocket for UnixStream {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.socket.as_socket()
    }
}

impl Read for UnixStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        Self::read(self, buffer)
    }
}

impl Write for UnixStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        Self::write(self, buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct UnixListener {
    socket: Socket,
}

impl UnixListener {
    /// Binds a Windows `AF_UNIX` listener.
    ///
    /// # Errors
    ///
    /// Returns an error if `WinSock` cannot be initialized, the socket cannot be
    /// created, the path is invalid, bind fails, or listen fails.
    pub(crate) fn bind(path: &Path) -> std::io::Result<Self> {
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
        socket.set_nonblocking(true)?;
        Ok(Self { socket })
    }

    /// Accepts one stream.
    ///
    /// # Errors
    ///
    /// Returns an error if accepting from the listener fails.
    pub(crate) fn accept(&self) -> std::io::Result<UnixStream> {
        // SAFETY: `self.socket.raw` is a valid listening WinSock socket. We do
        // not need the peer address for this local transport.
        let raw = unsafe { accept(self.socket.raw, std::ptr::null_mut(), std::ptr::null_mut()) };
        if raw == INVALID_SOCKET {
            let error = last_error_code();
            if error == WSAEWOULDBLOCK {
                Err(std::io::ErrorKind::WouldBlock.into())
            } else {
                Err(std::io::Error::from_raw_os_error(error))
            }
        } else {
            let stream = UnixStream::from_raw(raw);
            stream.socket.set_nonblocking(true)?;
            Ok(stream)
        }
    }
}

#[derive(Debug)]
struct Socket {
    raw: SOCKET,
}

// WinSock stream sockets support concurrent recv/send from different threads.
unsafe impl Send for Socket {}
unsafe impl Sync for Socket {}

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

    fn set_nonblocking(&self, enabled: bool) -> std::io::Result<()> {
        let mut enabled = u32::from(enabled);
        // SAFETY: `self.raw` is a valid WinSock socket, and `enabled` points to
        // a valid `u32` flag for the duration of the call.
        let result = unsafe { ioctlsocket(self.raw, FIONBIO, std::ptr::addr_of_mut!(enabled)) };
        if result == SOCKET_ERROR {
            Err(last_error())
        } else {
            Ok(())
        }
    }

    fn read(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let len = i32::try_from(buffer.len()).unwrap_or(i32::MAX);
        // SAFETY: `buffer` is valid for writes of `len` bytes, and `raw` is a
        // valid WinSock stream socket owned by this process.
        let result = unsafe { recv(self.raw, buffer.as_mut_ptr(), len, 0) };
        if result == SOCKET_ERROR {
            Err(last_error())
        } else {
            usize::try_from(result).map_err(std::io::Error::other)
        }
    }

    fn write(&self, buffer: &[u8]) -> std::io::Result<usize> {
        let len = i32::try_from(buffer.len()).unwrap_or(i32::MAX);
        // SAFETY: `buffer` is valid for reads of `len` bytes, and `raw` is a
        // valid WinSock stream socket owned by this process.
        let result = unsafe { send(self.raw, buffer.as_ptr(), len, 0) };
        if result == SOCKET_ERROR {
            Err(last_error())
        } else {
            usize::try_from(result).map_err(std::io::Error::other)
        }
    }
}

impl AsRawSocket for Socket {
    fn as_raw_socket(&self) -> RawSocket {
        self.raw as RawSocket
    }
}

impl AsSocket for Socket {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        // SAFETY: `raw` is a valid WinSock socket owned by this `Socket`, and
        // the borrowed socket cannot outlive `self`.
        unsafe { BorrowedSocket::borrow_raw(self.as_raw_socket()) }
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
    len: i32,
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
        let len = i32::try_from(SOCKADDR_UN_PATH_OFFSET + bytes.len() + 1).map_err(std::io::Error::other)?;
        Ok(Self { inner, len })
    }

    const fn as_sockaddr(&self) -> *const SOCKADDR {
        std::ptr::from_ref(&self.inner).cast::<SOCKADDR>()
    }

    const fn len(&self) -> i32 {
        self.len
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
    std::io::Error::from_raw_os_error(last_error_code())
}

fn last_error_code() -> i32 {
    // SAFETY: `WSAGetLastError` has no preconditions.
    unsafe { WSAGetLastError() }
}

pub(crate) fn is_would_block(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::WouldBlock || error.raw_os_error() == Some(WSAEWOULDBLOCK)
}
