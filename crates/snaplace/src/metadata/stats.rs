use std::sync::atomic::AtomicU64;

use serde::{Deserialize, Serialize};

/// Stats related to the whole Function.
#[derive(Debug, Default)]
pub struct FunctionStats {
    /// Total number of dispatch attempts of invocation requests for the
    /// Function that have arrived to [`SandboxPool`].
    ///
    /// In contrast to [`dispatches`](Self::dispatches), this includes both
    /// those that led to an assignment to a [`Worker`] _and_ those denied
    /// (blocked or rejected).
    ///
    /// In other words, this effectively counts the number of times
    /// [`SandboxPool::handle_dispatch()`] was called with an invocation
    /// request for this Function.
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`SandboxPool::handle_dispatch()`]: crate::sbpool::SandboxPool::handle_dispatch
    pub(crate) dispatch_attempts: AtomicU64,

    /// Number of invocation requests for the Function that have been assigned
    /// to a [`Worker`] by the [`SandboxPool`].
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    pub(crate) dispatches: AtomicU64,

    /// Number of *new* sandboxes that have been *created* to handle an invocation of the Function.
    pub(crate) sandboxes_created: AtomicU64,

    /// Number of sandbox snapshots that have been *created* and stored for future use from
    /// invocations of the Function.
    pub(crate) snapshots_created: AtomicU64, // TODO: Use or remove

    /// Number of times a sandbox has been restored from its snapshot to handle an invocation of
    /// the Function.
    pub(crate) snapshots_restored: AtomicU64,

    /// Number of [`Worker`] tasks that have been spawned to manage a sandbox handling invocations
    /// of the Function.
    pub(crate) workers_spawned: AtomicU64,
}

/// Stats related to the a single [`Sandbox`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SandboxStats {
    /// Number of invocations that have been assigned to the sandbox by the [`SandboxPool`].
    pub(crate) invocations: u64,

    /// Number of times the sandbox has been restored from a snapshot.
    pub(crate) restorations: u64,
}
