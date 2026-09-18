use crate::{
    admission::RunningSlot, metadata::SandboxStats, sbpool::Cpu, worker::WorkerId, FunctionId,
    Request, SandboxId,
};

/// The (only) _data-plane_ message that a [`Worker`] can _receive_, an invocation request.
///
/// An incoming Function [`Request`] along with the associated (acquired) [`RunningSlot`]
/// to process it, and the [`Cpu`] allocated for handling the invocation.
/// This is sent by [`AdmissionController`], to be handled by the [`Worker`] by
/// invoking the Function in a [`Sandbox`], as decided by the [`SandboxPool`].
///
/// [`AdmissionController`]: crate::admission::AdmissionController
/// [`Request`]: crate::Request
/// [`RunningSlot`]: crate::admission::RunningSlot
/// [`Sandbox`]: crate::worker::Sandbox
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
#[derive(Debug)]
pub(super) struct Invocation<Req: Request> {
    pub(super) req: Req,
    pub(super) running_slot: RunningSlot,
    pub(super) cpuset: Cpu,
}

/// All _control-plane_ messages that a [`Worker`] can _receive_.
///
/// ## [`ShutDown`] vs [`DestroySandbox`]
///
/// - `ShutDown` means: retire the [`Worker`], but preserve reusable [`Sandbox`]
///   state when appropriate.
/// - `DestroySandbox` means: retire the [`Worker`] and destroy [`Sandbox`]
///   resources more aggressively.
///
/// [`DestroySandbox`]: Self::DestroySandbox
/// [`Sandbox`]: crate::worker::runtime::Sandbox
/// [`ShutDown`]: Self::ShutDown
/// [`Worker`]: crate::worker::Worker
#[derive(Debug)]
pub(super) enum ControlMessage {
    /// Sent by [`SandboxPool`] to retire the [`Worker`], signaling it to prepare
    /// to either cleanup or hand off to Pool (at reap time) its resources before
    /// yielding execution.
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    ShutDown,

    /// Sent by [`SandboxPool`] to instruct the [`Worker`] to prepare a [`Sandbox`]
    /// for future use (and report the result back asynchronously).
    ///
    /// The provided `cpuset` is a transient allocation to use while creating or
    /// restoring the sandbox during this control operation.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    PrepareSandbox {
        cpuset: Cpu,
        // TODO: running slot?
    },

    /// Sent by [`SandboxPool`] to instruct the [`Worker`] to shut down (hence
    /// similar to [`ControlMessage::ShutDown`]), and also destroy/deallocate
    /// its [`Sandbox`]'s snapshot (and its associated resources, like files or
    /// network-related), if any.
    ///
    /// This is originally meant to serve only the path of the `DestroySandbox`
    /// control operation RPC.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    DestroySandbox,

    /// Sent by [`SandboxPool`] to instruct the [`Worker`] to create a snapshot
    /// of the [`Sandbox`] it owns.
    ///
    /// [`Sandbox`]: crate::worker::Sandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    CreateSnapshot {
        // NOTE: This should be useless; a Worker always knows its own Sandbox's ID. We include
        // it here only defensively, so that a Worker can verify/assert it is the recipient of
        // the request (i.e., even if SandboxPool's request routing logic changes in the future),
        // thus preventing the more difficult troubleshooting of chasing random/wrong snapshots.
        sandbox_id: SandboxId,
    },
}

/// All variants of a message that a [`Worker`] can *send* to the [`SandboxPool`].
///
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
#[derive(Debug)]
pub(crate) enum OutboundMessage {
    /// Sent upon (new) [`Sandbox`] creation (not on restoration nor resumption, _only_ creation),
    /// to let [`SandboxPool`] know of the new <code>[SandboxId] <-> [WorkerId]</code> mapping.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    SandboxId {
        worker_id: WorkerId,
        sandbox_id: SandboxId,
    },

    /// Sent when finished handling a [`Request`], to let [`SandboxPool`] decide what the
    /// [`Worker`] should do next.
    ///
    /// [`Request`]: crate::Request
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    NeedWork {
        worker_id: WorkerId,
        function_id: FunctionId,
    },

    /// Sent when finished cleaning up (triggered by an earlier [`ControlMessage::ShutDown`]), to
    /// let [`SandboxPool`] know that the [`Worker`] is now ready to be reaped.
    ///
    /// [`ControlMessage::ShutDown`]: crate::worker::ControlMessage::ShutDown
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    ReapMe {
        worker_id: WorkerId,
        function_id: FunctionId,
    },

    /// Sent when sandbox preparation (triggered by an earlier [`ControlMessage::PrepareSandbox`])
    /// has been completed, to let [`SandboxPool`] decide the [`Worker`]'s next state.
    ///
    /// [`ControlMessage::PrepareSandbox`]: crate::worker::ControlMessage::PrepareSandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    SandboxPreparation(Box<PrepareSandboxResult>),

    /// Sent when snapshot creation (triggered by an earlier [`ControlMessage::CreateSnapshot`])
    /// has been completed, to let [`SandboxPool`] decide [`Worker`]'s next state.
    ///
    /// [`ControlMessage::CreateSnapshot`]: crate::worker::ControlMessage::CreateSnapshot
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    SnapshotCreation(Box<CreateSnapshotResult>),
}

/// Contained in [`OutboundMessage::SandboxPreparation`], as a response to
/// [`ControlMessage::PrepareSandbox`].
#[derive(Debug, Clone)]
pub(crate) struct PrepareSandboxResult {
    pub worker_id: WorkerId,
    pub function_id: FunctionId,
    pub result: Result<(SandboxId, bool, SandboxStats), Box<str>>,
}

/// Contained in [`OutboundMessage::SnapshotCreation`], as a response to
/// [`ControlMessage::CreateSnapshot`].
///
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
#[derive(Debug, Clone)]
pub(crate) struct CreateSnapshotResult {
    pub worker_id: WorkerId,
    pub function_id: FunctionId,
    pub sandbox_id: SandboxId,
    pub result: Result<(), Box<str>>,
}
