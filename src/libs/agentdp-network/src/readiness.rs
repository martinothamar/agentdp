use crate::reactor::ReactorInterest;

/// Single-threaded readiness owner for one registered IO slot.
///
/// This crate relies on event-loop phase ordering instead of Tokio-style
/// readiness generations: reactor events are first latched into the owning slot,
/// then that owner drives IO, and only the typed `WouldBlock` result for the
/// attempted direction clears the corresponding latch. No newer readiness event
/// can interleave while a slot is being driven.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IoSlotState {
    readiness: IoReadiness,
    registered_interest: ReactorInterest,
    write_probe: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct IoReadiness(u8);

impl IoSlotState {
    pub(crate) const fn new(registered_interest: ReactorInterest) -> Self {
        Self {
            readiness: IoReadiness::empty(),
            registered_interest,
            write_probe: false,
        }
    }

    pub(crate) const fn registered(interest: ReactorInterest) -> Self {
        Self {
            readiness: IoReadiness::empty(),
            registered_interest: interest,
            write_probe: interest_writes(interest),
        }
    }

    pub(crate) const fn registered_interest(self) -> ReactorInterest {
        self.registered_interest
    }

    pub(crate) const fn watches_write(self) -> bool {
        interest_writes(self.registered_interest)
    }

    pub(crate) const fn can_read(self) -> bool {
        interest_reads(self.registered_interest) && self.readiness.readable()
    }

    pub(crate) const fn can_write(self) -> bool {
        interest_writes(self.registered_interest) && (self.readiness.writable() || self.write_probe)
    }

    pub(crate) const fn mark_reactor_ready(&mut self, readable: bool, writable: bool) {
        self.readiness.mark(readable, writable);
    }

    pub(crate) const fn clear_read_after_would_block(&mut self) {
        self.readiness.clear_read();
    }

    pub(crate) const fn clear_write_after_would_block(&mut self) {
        self.readiness.clear_write();
        self.write_probe = false;
    }

    pub(crate) const fn clear_for_drop_or_reset(&mut self) {
        self.readiness = IoReadiness::empty();
        self.registered_interest = ReactorInterest::Disabled;
        self.write_probe = false;
    }

    pub(crate) const fn set_registered_interest_after_reregister(&mut self, interest: ReactorInterest) {
        let previous_writable = interest_writes(self.registered_interest);
        let next_writable = interest_writes(interest);
        self.registered_interest = interest;
        if !next_writable {
            self.write_probe = false;
        } else if !previous_writable {
            self.write_probe = true;
        }
    }
}

impl IoReadiness {
    const READ: u8 = 1 << 0;
    const WRITE: u8 = 1 << 1;

    const fn empty() -> Self {
        Self(0)
    }

    const fn readable(self) -> bool {
        self.0 & Self::READ != 0
    }

    const fn writable(self) -> bool {
        self.0 & Self::WRITE != 0
    }

    const fn mark(&mut self, readable: bool, writable: bool) {
        if readable {
            self.0 |= Self::READ;
        }
        if writable {
            self.0 |= Self::WRITE;
        }
    }

    const fn clear_read(&mut self) {
        self.0 &= !Self::READ;
    }

    const fn clear_write(&mut self) {
        self.0 &= !Self::WRITE;
    }
}

const fn interest_reads(interest: ReactorInterest) -> bool {
    matches!(interest, ReactorInterest::Readable | ReactorInterest::ReadWrite)
}

const fn interest_writes(interest: ReactorInterest) -> bool {
    matches!(interest, ReactorInterest::Writable | ReactorInterest::ReadWrite)
}

#[cfg(test)]
mod tests {
    use crate::reactor::ReactorInterest;

    use super::IoSlotState;

    #[test]
    fn failed_reregister_does_not_change_registered_interest() {
        let slot = IoSlotState::new(ReactorInterest::Readable);

        assert_eq!(slot.registered_interest(), ReactorInterest::Readable);
        assert!(!slot.can_write());
    }

    #[test]
    fn initial_writable_registration_allows_one_write_probe() {
        let mut slot = IoSlotState::registered(ReactorInterest::ReadWrite);

        assert!(slot.can_write());
        slot.clear_write_after_would_block();
        assert!(!slot.can_write());
    }

    #[test]
    fn first_write_probe_is_available_after_enabling_writable_interest() {
        let mut slot = IoSlotState::new(ReactorInterest::Readable);

        slot.set_registered_interest_after_reregister(ReactorInterest::ReadWrite);

        assert_eq!(slot.registered_interest(), ReactorInterest::ReadWrite);
        assert!(slot.can_write());
    }

    #[test]
    fn write_would_block_disables_probe_until_reactor_reports_writable() {
        let mut slot = IoSlotState::new(ReactorInterest::Readable);
        slot.set_registered_interest_after_reregister(ReactorInterest::ReadWrite);

        slot.clear_write_after_would_block();

        assert!(!slot.can_write());
        slot.mark_reactor_ready(false, true);
        assert!(slot.can_write());
    }

    #[test]
    fn clearing_read_after_would_block_preserves_write() {
        let mut slot = IoSlotState::new(ReactorInterest::ReadWrite);
        slot.mark_reactor_ready(true, true);

        slot.clear_read_after_would_block();

        assert!(!slot.can_read());
        assert!(slot.can_write());
    }

    #[test]
    fn clearing_write_after_would_block_preserves_read() {
        let mut slot = IoSlotState::new(ReactorInterest::ReadWrite);
        slot.mark_reactor_ready(true, true);

        slot.clear_write_after_would_block();

        assert!(slot.can_read());
        assert!(!slot.can_write());
    }

    #[test]
    fn interest_masking_preserves_latched_readiness() {
        let mut slot = IoSlotState::new(ReactorInterest::ReadWrite);
        slot.mark_reactor_ready(true, true);

        slot.set_registered_interest_after_reregister(ReactorInterest::Writable);
        assert!(!slot.can_read());
        assert!(slot.can_write());

        slot.set_registered_interest_after_reregister(ReactorInterest::Readable);
        assert!(slot.can_read());
        assert!(!slot.can_write());
    }
}
