use super::super::{MioTcpListener, MioTcpStream, MioUdpSocket};

impl std::os::windows::io::AsSocket for MioTcpStream {
    fn as_socket(&self) -> std::os::windows::io::BorrowedSocket<'_> {
        std::os::windows::io::AsSocket::as_socket(&self.inner)
    }
}

impl std::os::windows::io::AsSocket for MioTcpListener {
    fn as_socket(&self) -> std::os::windows::io::BorrowedSocket<'_> {
        std::os::windows::io::AsSocket::as_socket(&self.inner)
    }
}

impl std::os::windows::io::AsSocket for MioUdpSocket {
    fn as_socket(&self) -> std::os::windows::io::BorrowedSocket<'_> {
        std::os::windows::io::AsSocket::as_socket(&self.inner)
    }
}
