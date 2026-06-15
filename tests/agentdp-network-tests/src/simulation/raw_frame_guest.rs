use super::{DriveBudget, GuestLink, Result, Simulator, SteppedNetwork};

#[derive(Debug, Clone)]
pub struct RawFrameGuest {
    link: GuestLink,
}

impl RawFrameGuest {
    #[must_use]
    pub const fn new(link: GuestLink) -> Self {
        Self { link }
    }

    #[must_use]
    pub const fn link(&self) -> &GuestLink {
        &self.link
    }

    /// # Errors
    ///
    /// Returns an error when the frame cannot be submitted to the simulated guest link.
    pub fn send_frame(&self, frame: impl Into<Vec<u8>>) -> Result<()> {
        self.link.send_to_network(frame)
    }

    /// # Errors
    ///
    /// Returns an error when no network frame is received before the drive budget is exhausted.
    pub fn recv_frame<N>(
        &self,
        sim: &mut Simulator,
        running: &mut N,
        label: &str,
        budget: DriveBudget,
    ) -> Result<Vec<u8>>
    where
        N: SteppedNetwork,
    {
        sim.drive_until_network_frame(running, &self.link, label, budget)
    }
}
