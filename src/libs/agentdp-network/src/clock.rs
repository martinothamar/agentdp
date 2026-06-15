use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use smoltcp::time::Instant as SmoltcpInstant;

pub(crate) trait NetworkClock: Clone + 'static {
    fn now(&self) -> Instant;

    fn system_time(&self) -> SystemTime;

    fn unix_seconds(&self) -> u64 {
        self.system_time()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs())
    }

    fn smoltcp_now(&self) -> SmoltcpInstant {
        SmoltcpInstant::from_micros(duration_micros_saturating(
            self.system_time().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO),
        ))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SystemClock;

impl NetworkClock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn system_time(&self) -> SystemTime {
        SystemTime::now()
    }
}

fn duration_micros_saturating(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}
