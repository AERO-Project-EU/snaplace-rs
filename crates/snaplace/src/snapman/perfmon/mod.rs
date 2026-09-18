mod noop;
pub use noop::NoOp;

use std::{fmt::Debug, path::PathBuf};

use tokio::sync::oneshot;

use crate::{conf::PerfConfig, snapman::error::Result, worker::SandboxStateRef};

/// Identifiers used by [`PerformanceMonitor`]s to monitor sandboxes.
#[derive(Debug, Clone)]
pub enum MonitorTarget {
    Pid(u64),
    Cgroup(PathBuf),
}

pub trait PerformanceMonitorHandle: Debug + Send + Sync + 'static {
    /// Performance metrics collected by a [`PerformanceMonitor`] implementation.
    type Metrics: Clone + Debug;

    /// Consume this handle to stop the associated [`PerformanceMonitor`] and collect its
    /// `Metrics`.
    fn finish(self) -> impl Future<Output = Result<Self::Metrics>> + Send;
}

pub trait PerformanceMonitor: Debug + Send + Sync + 'static {
    /// Handle associated with a `PerformanceMonitor`.
    type Handle: PerformanceMonitorHandle;

    /// Initialize the `PerformanceMonitor` and return a handle to it.
    fn start(
        config: &PerfConfig,
        monitor_targets: &[MonitorTarget],
        state: SandboxStateRef,
        ready: oneshot::Sender<Result<()>>,
    ) -> impl Future<Output = Result<Self::Handle>> + Send;
}
