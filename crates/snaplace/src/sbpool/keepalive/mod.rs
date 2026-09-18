pub mod eviction;
pub mod fixed;
pub mod sd_static;

pub use fixed::Fixed;

use std::time::Duration;

use crate::{metadata::FunctionMetadataStore, sbpool::pool::PoolContext, FunctionId};

pub trait Policy: eviction::Policy + Send + 'static {
    /// Decide how long should a [`Worker`] owning a [`Sandbox`] for the Function identified by
    /// the provided [`FunctionId`] be kept alive (i.e., remain _Idle_) before being shut down.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`Worker`]: crate::worker::Worker
    fn assign<FmdStore, FunctionInfo>(
        &mut self,
        pctx: PoolContext<'_, FmdStore, FunctionInfo>,
        function_id: &FunctionId,
    ) -> Option<Duration>
    where
        Self: Sized,
        FmdStore: FunctionMetadataStore<FunctionInfo>;
}
