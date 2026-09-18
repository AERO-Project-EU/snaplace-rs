mod fixed;
pub use fixed::Fixed;

pub(crate) mod sd_static;
pub use sd_static::SlowdownsStatic;

use std::time::Duration;

use crate::{
    snapman::{
        error::Result, perfmon::PerformanceMonitorHandle, PerformanceMonitor, SnapshotPaths,
    },
    worker::SandboxStateRef,
    FunctionId,
};

pub trait PlacementAlgorithm: Send + Sync + 'static {
    type PerformanceMonitor: PerformanceMonitor;

    fn update_metrics(
        &mut self,
        function_id: &FunctionId,
        state: SandboxStateRef,
        metrics: &<<Self::PerformanceMonitor as PerformanceMonitor>::Handle as PerformanceMonitorHandle>::Metrics,
        issuer_duration: Duration,
    ) -> Result<()>;

    /// Returns the filesystem paths where snapshot files for the sandbox with the given
    /// [`FunctionId`] should be stored, or `None` if no sandbox snapshot should be created
    /// at all.
    fn query_path(&mut self, function_id: &FunctionId) -> Result<Option<SnapshotPaths>>;
}
