#![cfg_attr(windows, allow(unsafe_code))]

use std::net::{Ipv6Addr, SocketAddr};
use std::time::Duration;

/// Connects a TCP stream with agentdp's host socket defaults applied.
///
/// # Errors
///
/// Returns an error when the connection fails or a socket option cannot be applied.
pub async fn connect_tcp_stream<A: tokio::net::ToSocketAddrs>(addr: A) -> std::io::Result<tokio::net::TcpStream> {
    let stream = tokio::net::TcpStream::connect(addr).await?;
    configure_tcp_stream(&stream)?;
    Ok(stream)
}

/// Applies agentdp's host TCP socket defaults to an existing stream.
///
/// # Errors
///
/// Returns an error when the host OS rejects a socket option.
pub fn configure_tcp_stream(stream: &tokio::net::TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)
}

/// Returns whether this host appears to have usable IPv6 egress.
pub async fn has_ipv6_egress() -> bool {
    tokio::time::timeout(
        Duration::from_secs(2),
        connect_tcp_stream(SocketAddr::from((
            Ipv6Addr::new(0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111),
            443,
        ))),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

/// Prevents a socket from being inherited by child processes.
///
/// # Errors
///
/// Returns an error when the host OS rejects the socket inheritance update.
#[cfg(target_os = "windows")]
pub fn prevent_child_socket_inheritance(socket: &impl std::os::windows::io::AsSocket) -> std::io::Result<()> {
    use std::os::windows::io::AsRawSocket as _;
    use windows_sys::Win32::Foundation::{HANDLE, HANDLE_FLAG_INHERIT, SetHandleInformation};

    // SAFETY: `as_raw_socket` is borrowed from a live socket for the duration of
    // this call, and `SetHandleInformation` only updates the inherit flag.
    let result = unsafe { SetHandleInformation(socket.as_socket().as_raw_socket() as HANDLE, HANDLE_FLAG_INHERIT, 0) };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Prevents a socket from being inherited by child processes.
///
/// # Errors
///
/// Never returns an error on non-Windows hosts because socket inheritance is
/// controlled through process spawning there.
#[cfg(not(target_os = "windows"))]
pub const fn prevent_child_socket_inheritance<T>(_socket: &T) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::connect_tcp_stream;

    #[tokio::test]
    async fn connect_tcp_stream_enables_nodelay() {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { listener.accept().await.map(|_accepted| ()) });

        let stream = connect_tcp_stream(addr).await.unwrap();

        assert!(stream.nodelay().unwrap());
        server.await.unwrap().unwrap();
    }
}
