use std::collections::HashSet;

use triomphe::Arc;

use crate::{
    metadata::{registration::RegisteredFunction, FunctionInfo, FunctionStats},
    worker::WorkerId,
};

#[derive(Debug)]
pub struct StdHashMapFmd<FunctionInfo> {
    pub(crate) stats: Arc<FunctionStats>,
    pub(crate) registered_function: Arc<RegisteredFunction<FunctionInfo>>,

    /// [`Worker`]s that are currently running and handling a Function [`Request`] that the
    /// [`SandboxPool`] has assigned to them some time earlier.
    ///
    /// # Notes
    ///
    /// - If the number of _Active_ [`Worker`]s is greater than the configured `max_concurrency`,
    ///   some of them should be blocked trying to acquire [`SandboxPool`]'s internal
    ///   [`Semaphore`].
    /// - These [`Worker`]s shall be moved to the `idle_workers` list when they declare to
    ///   the [`SandboxPool`] that they are available again (through some sort of `NeedWork`
    ///   notification), when the Pool processes their status.
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Semaphore`]: tokio::sync::Semaphore
    /// [`Worker`]: crate::worker::Worker
    // TODO(ckatsak): We never shrink that, therefore peaks lead to wasted memory
    pub(crate) active_workers: HashSet<WorkerId, crate::BuildHasher>,

    /// [`Worker`]s that are currently _Idle_ due to the [keep-alive policy] in place.
    ///
    /// [keep-alive policy]: crate::sbpool::keepalive::Policy
    /// [`Worker`]: crate::worker::Worker
    // TODO(ckatsak): We never shrink that, therefore peaks lead to wasted memory
    pub(crate) idle_workers: HashSet<WorkerId, crate::BuildHasher>,

    /// [`Worker`] tasks that have been sent the shutdown signal (by [`SandboxPool`]), and are
    /// awaited to confirm their death (through the `worker_status` channel) to be reaped (without
    /// leaking their [`Sandbox`]es, be they running or merely persisted snapshots).
    ///
    /// [`Sandbox`]: crate::worker::Runtime::Sandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    // TODO(ckatsak): We never shrink that, therefore peaks lead to wasted memory
    pub(crate) dying_workers: HashSet<WorkerId, crate::BuildHasher>,
}

impl<FnInfo: FunctionInfo> StdHashMapFmd<FnInfo> {
    // TODO(ckatsak): In the future, when Function registration is dynamic, implemented as a
    // service, `trait FunctionMetadata` will probably also require a constructor method to be
    // called by that service upon receiving a registration request.
    pub fn new(registered_function: RegisteredFunction<FnInfo>) -> Self {
        Self {
            stats: Default::default(),
            registered_function: Arc::new(registered_function),
            active_workers: Default::default(),
            idle_workers: Default::default(),
            dying_workers: Default::default(),
        }
    }
}
