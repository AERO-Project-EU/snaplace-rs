use crate::FunctionId;

/// Advisory Pool-to-admission wake-up event.
///
/// Pool events are intentionally lossy hints.  They are not reservations,
/// permissions to dispatch, or authoritative state transfers.  Admission may
/// use them to make [blocked] Functions [runnable] again, but it must always
/// issue a fresh [`DispatchRequest`] before assigning work.
///
///
/// [`DispatchRequest`]: crate::admission::DispatchRequest
/// [blocked]: crate::admission::controller::FunctionDispatchState::Blocked
/// [runnable]: crate::admission::controller::FunctionDispatchState::Runnable
#[derive(Debug)]
pub(crate) enum PoolEvent {
    /// A Worker for this Function became _Idle_.
    ///
    /// This may make a request runnable by reusing an already-provisioned
    /// Worker, even when no Pool-wide capacity changed.
    WorkerIdle { function_id: FunctionId },

    /// Pool memory capacity may now be available.
    ///
    /// This may unblock Functions whose last dispatch attempt failed because
    /// provisioning needed more memory.  This is not Function-specific.
    MemoryCapAvailable,

    /// Per-Function Worker capacity may now be available.
    ///
    /// Emitted when `#_Active_(function_id) + #_Idle_(function_id)` decreases,
    /// so a Function blocked by its Worker cap may be worth retrying.
    WorkerCapAvailable { function_id: FunctionId },
}
