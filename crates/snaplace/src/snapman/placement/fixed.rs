use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{
    snapman::{
        error::Result,
        perfmon::{self, PerformanceMonitorHandle},
        PerformanceMonitor, PlacementAlgorithm, SnapshotPaths,
    },
    worker::SandboxStateRef,
    FunctionId,
};

#[derive(Debug, Clone)]
pub struct Fixed(Option<PathBuf>);

impl Fixed {
    pub fn new(path: Option<impl AsRef<Path>>) -> Self {
        Self(path.map(|p| p.as_ref().to_path_buf()))
    }

    #[inline]
    pub fn path(&self) -> Option<&Path> {
        self.0.as_deref()
    }
}

impl PlacementAlgorithm for Fixed {
    type PerformanceMonitor = perfmon::NoOp;

    #[inline]
    fn update_metrics(
        &mut self,
        _function_id: &FunctionId,
        _state: SandboxStateRef,
        _metrics: &<<Self::PerformanceMonitor as PerformanceMonitor>::Handle as PerformanceMonitorHandle>::Metrics,
        _issuer_duration: Duration,
    ) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn query_path(&mut self, _function_id: &FunctionId) -> Result<Option<SnapshotPaths>> {
        Ok(self.0.as_ref().map(|path| SnapshotPaths {
            state: path.to_path_buf(),
            memory: path.to_path_buf(),
        }))
    }
}
