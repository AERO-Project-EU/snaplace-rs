use std::time::{Duration, SystemTime};

use tokio::{sync::OwnedSemaphorePermit, time::Instant};

use crate::metrics::Nanoseconds;

#[derive(Debug)]
pub struct RunningSlot {
    permit: Option<OwnedSemaphorePermit>,
    /// Timestamp (monotonic) of when this `permit` was acquired.
    start_ts: Instant,
    /// Duration of ~`start_ts` since Epoch (using [`SystemTime`]; i.e., non-monotonic).
    start_ts_epoch_ns: Nanoseconds,
}

impl RunningSlot {
    #[inline]
    pub(crate) fn new(
        permit: OwnedSemaphorePermit,
        start_ts: Instant,
        start_ts_epoch_ns: Nanoseconds,
    ) -> Self {
        Self {
            permit: Some(permit),
            start_ts,
            start_ts_epoch_ns,
        }
    }

    /// Time elapsed since this slot (i.e., the associated `permit`) was acquired.
    #[inline]
    pub fn elapsed(&self) -> Duration {
        self.start_ts.elapsed()
    }

    /// Timestamp (in nanoseconds since Unix Epoch, based on the non-monotonic system clock) of
    /// when this `RunningSlot`'s `permit` was acquired.
    #[inline]
    pub fn acquired_since_epoch(&self) -> Nanoseconds {
        self.start_ts_epoch_ns
    }

    #[inline]
    pub fn release(&mut self) {
        self.permit.take();
    }
}

impl From<OwnedSemaphorePermit> for RunningSlot {
    fn from(permit: OwnedSemaphorePermit) -> Self {
        debug_assert_eq!(permit.num_permits(), 1);
        let start_ts_epoch_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as _;
        Self::new(permit, Instant::now(), start_ts_epoch_ns)
    }
}
