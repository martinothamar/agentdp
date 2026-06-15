use crate::network::NetworkLimits;

#[derive(Debug, Clone, Copy)]
pub(crate) struct DriveBudget {
    events: usize,
    steps: usize,
    bytes: usize,
}

impl DriveBudget {
    #[must_use]
    pub(crate) const fn event_loop(limits: &NetworkLimits) -> Self {
        Self {
            events: limits.drive_event_budget,
            steps: limits.drive_step_budget,
            bytes: limits.drive_byte_budget,
        }
    }

    #[must_use]
    pub(crate) const fn can_continue(&self) -> bool {
        self.events > 0 && self.steps > 0 && self.bytes > 0
    }

    pub(crate) const fn step(&mut self) -> bool {
        if self.steps == 0 {
            return false;
        }
        self.steps -= 1;
        true
    }

    pub(crate) fn event(&mut self, bytes: usize) -> bool {
        if self.events == 0 || self.bytes == 0 {
            return false;
        }
        self.events -= 1;
        self.bytes = self.bytes.saturating_sub(bytes.max(1));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::DriveBudget;

    #[test]
    fn budget_stops_after_event_byte_or_step_capacity_is_exhausted() {
        let mut budget = DriveBudget {
            events: 2,
            steps: 2,
            bytes: 8,
        };

        assert!(budget.can_continue());
        assert!(budget.step());
        assert!(budget.event(4));
        assert!(budget.can_continue());
        assert!(budget.step());
        assert!(budget.event(4));
        assert!(!budget.can_continue());
        assert!(!budget.step());
        assert!(!budget.event(1));
    }
}
