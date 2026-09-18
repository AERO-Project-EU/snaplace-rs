use tokio::sync::oneshot;
use tracing::{error, instrument, Level};

use crate::{
    conf::PerfConfig,
    snapman::{
        error::Result,
        perfmon::{MonitorTarget, PerformanceMonitorHandle},
        Error, PerformanceMonitor,
    },
    worker::SandboxStateRef,
};

pub type NoOp = ();

impl PerformanceMonitorHandle for NoOp {
    type Metrics = Self;

    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    async fn finish(self) -> Result<Self::Metrics> {
        Ok(())
    }
}

impl PerformanceMonitor for NoOp {
    type Handle = Self;

    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    async fn start(
        _config: &PerfConfig,
        _monitor_targets: &[MonitorTarget],
        _state: SandboxStateRef,
        ready: oneshot::Sender<Result<()>>,
    ) -> Result<Self::Handle> {
        ready.send(Ok(())).map_err(|_err| {
            const MSG: &str = "PerformanceMonitor failed to communicate readiness to Worker";
            error!(MSG);
            Error::Channel {
                msg: MSG.into(),
                source: MSG.into(),
            }
        })
    }
}
