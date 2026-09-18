use std::fmt::Debug;

use tokio::sync::oneshot;

use crate::{sbpool::Cpu, worker::WorkerRef, FunctionId, Request};

pub(super) mod controller;
pub(crate) use controller::AdmissionController;

pub(super) mod running_slot;
pub(crate) use running_slot::RunningSlot;

/// Placement policy requested by `AdmissionController` for one dispatch attempt.
///
/// This is a request to `SandboxPool`, not a guarantee. Pool remains
/// authoritative and may still reply with `WouldBlock` or `Reject`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchMode {
    /// Reuse only an already-_Idle_ Worker for this Function.
    ///
    /// Pool must not create, restore, or otherwise provision new capacity in
    /// this mode.  This is useful when admission is retrying a Function that
    /// should only make progress if an existing Worker for the Function becomes
    /// _Idle_.
    ReuseIdleOnly,
    /// Prefer an _Idle_ Worker, but "allow" Pool to create one if needed.
    ///
    /// This is the normal first-attempt mode.  Pool still reserves the right
    /// to reject or block the request if conditions prevent assignment.
    ReuseIdleOrProvision,
}

/// Admission-to-Pool request for exactly one Worker assignment attempt.
///
/// The request does not contain the user request body. Admission keeps the
/// request in `OutstandingDispatch` until Pool replies through `respond_to`.
pub(crate) struct DispatchRequest<Req: Request> {
    /// Function whose queued head request is being dispatched.
    function_id: FunctionId,
    /// Assignment mode requested for this attempt.
    mode: DispatchMode,
    /// One-shot reply channel for Pool's placement decision.
    respond_to: oneshot::Sender<DispatchResponse<Req>>,
}

impl<Req: Request> Debug for DispatchRequest<Req> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("DispatchRequest")
            .field("function_id", &self.function_id)
            .field("mode", &self.mode)
            .finish()
    }
}

impl<Req: Request> DispatchRequest<Req> {
    #[inline]
    pub(crate) fn function_id(&self) -> &FunctionId {
        &self.function_id
    }

    #[inline]
    pub(crate) fn mode(&self) -> DispatchMode {
        self.mode
    }

    #[inline]
    pub(crate) fn reply(self, resp: DispatchResponse<Req>) -> Result<(), DispatchResponse<Req>> {
        self.respond_to.send(resp)
    }
}

/// Temporary reason why Pool could not satisfy a dispatch attempt.
///
/// A block reason is advisory scheduler state. Admission uses it to decide
/// which Pool events may make a Function worth retrying, but every retry
/// still has to ask Pool again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockReason {
    /// Pool could not assign work because this dispatch attempt was restricted
    /// to [reusing an existing _Idle_ Worker](DispatchMode::ReuseIdleOnly),
    /// and none was available.
    NoIdleWorker,
    /// Pool could not assign work because this Function is already at its
    /// configured live-Worker cap; i.e., no _Idle_ Worker existed, and
    /// `#_Active_(function_id) + #_Idle_(function_id) >= worker_cap(F)`.
    FunctionWorkerCap,
    /// Pool could not create another Worker for this Function because the
    /// memory available at the time of the attempt was insufficient.
    MemoryPressure,
    // TODO(ckatsak): (?)
    // /// Pool could not create another Worker for this Function because the
    // /// CPU resources available at the time of the attempt were insufficient.
    // NoCpu,
    // /// Pool cannot satisfy dispatch request now, because resource reclamation/eviction
    // /// work that may make progress is already underway.
    // EvictionInProgress,
    // /// Pool could not create or restore a Worker for a transient reason
    // /// unrelated to memory, CPU, or the Function's live-Worker cap.
    // ProvisioningUnavailable,
}

/// Terminal reason why Pool rejected an admission attempt.
///
/// Unlike [`BlockReason`], this is not backoff state. Admission must fail the
/// request instead of requeueing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RejectReason {
    /// Request rejected because the target Function is not registered.
    UnknownFunction,
    /// Request rejected because of an internal error.
    Internal,
    /// Request rejected because shut down was in progress and no new dispatch
    /// attempts are accepted.
    ShuttingDown,
}

/// Pool's answer to one [`DispatchRequest`].
///
/// Each response resolves exactly one outstanding admission attempt.
#[allow(private_interfaces)] // only admission needs to match reasons; Pool uses constructors
#[derive(Debug)]
pub(crate) enum DispatchResponse<Req: Request> {
    /// Pool assigned a Worker and CPU set; admission should forward the request.
    Dispatch {
        worker_ref: WorkerRef<Req>,
        cpuset: Cpu,
    },
    /// Pool cannot assign a Worker now, but the Function may become runnable later.
    WouldBlock { reason: BlockReason },
    /// Pool determined that this request cannot be admitted successfully.
    Reject { reason: RejectReason },
}

impl<Req: Request> DispatchResponse<Req> {
    #[inline]
    pub(crate) fn dispatch(worker_ref: WorkerRef<Req>, cpuset: Cpu) -> Self {
        Self::Dispatch { worker_ref, cpuset }
    }

    #[inline]
    pub(crate) fn no_idle_worker() -> Self {
        Self::WouldBlock {
            reason: BlockReason::NoIdleWorker,
        }
    }

    #[inline]
    pub(crate) fn function_worker_cap() -> Self {
        Self::WouldBlock {
            reason: BlockReason::FunctionWorkerCap,
        }
    }

    #[inline]
    pub(crate) fn oom() -> Self {
        Self::WouldBlock {
            reason: BlockReason::MemoryPressure,
        }
    }

    #[inline]
    pub(crate) fn unknown_function() -> Self {
        Self::Reject {
            reason: RejectReason::UnknownFunction,
        }
    }

    #[inline]
    pub(crate) fn shutting_down() -> Self {
        Self::Reject {
            reason: RejectReason::ShuttingDown,
        }
    }
}

enum AdmissionFailure {
    QueueFullGlobal,
    QueueFullFunction,
    DeadlinePassed,
    Rejected(RejectReason),
}
