use super::super::{MioTcpListener, MioTcpStream, MioUdpSocket};

impl std::os::fd::AsFd for MioTcpStream {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.inner)
    }
}

impl std::os::fd::AsFd for MioTcpListener {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.inner)
    }
}

impl std::os::fd::AsFd for MioUdpSocket {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.inner)
    }
}
