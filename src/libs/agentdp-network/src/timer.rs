use std::time::{Duration, Instant};

use crate::clock::NetworkClock;

pub(crate) const TIMER_QUEUE_REQUIRED_CAPACITY: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TimerId {
    ConnectRetry,
    Reconnect,
    GatewayPoll,
    UdpExpiry,
    StatusPublish,
}

impl TimerId {
    const ALL: [Self; TIMER_QUEUE_REQUIRED_CAPACITY] = [
        Self::ConnectRetry,
        Self::Reconnect,
        Self::GatewayPoll,
        Self::UdpExpiry,
        Self::StatusPublish,
    ];

    const fn index(self) -> usize {
        match self {
            Self::ConnectRetry => 0,
            Self::Reconnect => 1,
            Self::GatewayPoll => 2,
            Self::UdpExpiry => 3,
            Self::StatusPublish => 4,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("network timer queue capacity {capacity} is smaller than required timer count {required}")]
pub(crate) struct TimerQueueError {
    capacity: usize,
    required: usize,
}

#[derive(Debug)]
pub(crate) struct TimerQueue<C: NetworkClock> {
    clock: C,
    deadlines: [Option<Instant>; TIMER_QUEUE_REQUIRED_CAPACITY],
}

impl<C> TimerQueue<C>
where
    C: NetworkClock,
{
    pub(crate) fn new(capacity: usize, clock: C) -> Result<Self, TimerQueueError> {
        if capacity < TIMER_QUEUE_REQUIRED_CAPACITY {
            return Err(TimerQueueError {
                capacity,
                required: TIMER_QUEUE_REQUIRED_CAPACITY,
            });
        }
        Ok(Self {
            clock,
            deadlines: [None; TIMER_QUEUE_REQUIRED_CAPACITY],
        })
    }

    pub(crate) fn schedule_after(&mut self, timer: TimerId, delay: Duration) {
        self.schedule_at(timer, self.clock.now() + delay);
    }

    pub(crate) const fn schedule_at(&mut self, timer: TimerId, deadline: Instant) {
        self.deadlines[timer.index()] = Some(deadline);
    }

    pub(crate) const fn clear(&mut self, timer: TimerId) {
        self.deadlines[timer.index()] = None;
    }

    pub(crate) fn next_timeout(&self) -> Option<Duration> {
        let now = self.clock.now();
        self.deadlines
            .iter()
            .flatten()
            .min()
            .map(|deadline| deadline.saturating_duration_since(now))
    }

    pub(crate) fn pop_expired(&mut self, output: &mut Vec<TimerId>) {
        let now = self.clock.now();
        output.clear();
        for timer in TimerId::ALL {
            let deadline = &mut self.deadlines[timer.index()];
            if deadline.is_some_and(|deadline| deadline <= now) {
                *deadline = None;
                output.push(timer);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::clock::SystemClock;

    use super::{TIMER_QUEUE_REQUIRED_CAPACITY, TimerId, TimerQueue};

    #[test]
    fn validates_minimum_capacity() {
        assert!(TimerQueue::new(TIMER_QUEUE_REQUIRED_CAPACITY - 1, SystemClock).is_err());
        assert!(TimerQueue::new(TIMER_QUEUE_REQUIRED_CAPACITY, SystemClock).is_ok());
    }

    #[test]
    fn reports_next_timeout_and_pops_expired_timers() {
        let mut timers = TimerQueue::new(TIMER_QUEUE_REQUIRED_CAPACITY, SystemClock).expect("valid timer queue");
        let now = Instant::now();
        timers.schedule_at(
            TimerId::GatewayPoll,
            now.checked_sub(Duration::from_millis(1)).unwrap_or(now),
        );
        timers.schedule_at(TimerId::StatusPublish, now + Duration::from_secs(1));

        assert_eq!(timers.next_timeout(), Some(Duration::ZERO));

        let mut expired = Vec::new();
        timers.pop_expired(&mut expired);
        assert_eq!(expired, vec![TimerId::GatewayPoll]);
        assert!(timers.next_timeout().is_some_and(|timeout| timeout > Duration::ZERO));
    }
}
