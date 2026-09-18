use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    metrics::{MetricsCollectorRef, Nanoseconds},
    network::{self, NetworkManagerRef},
    snapman::SnapshotManagerRef,
    worker::{OutboundMessage, Sandbox},
};

/// Type used as a unique key identifying each [`Worker`].
///
/// # Notes
///
/// If you change that, mind that for now this type is always treated as [`Copy`] throughout the
/// crate.
///
/// [`Worker`]: crate::worker::Worker
pub(crate) type WorkerId = Uuid;

/// Sender halves of channels, owned by [`Worker`]s.
///
/// [`SandboxPool`] also stores an instance of this, allowing it to properly initialize any new
/// [`Worker`]s it spawns.
///
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
#[derive(Debug)]
pub(crate) struct OutChannels<Resp, Sb, NetResource>
where
    Sb: Sandbox,
    NetResource: network::Resource,
{
    // - Only stored here to be passed to the `Worker`s
    // - Matching rx half (1) is owned by the `SandboxPool`
    pub(super) pool: mpsc::Sender<OutboundMessage>,

    // - `SnapshotManagerMessage`s from `Worker`s to `SnapshotManager`
    // - Matching rx half (1) is owned by the `SnapshotManager`
    pub(super) snapman: SnapshotManagerRef<Sb>,

    // - Passed to `Worker`s for them to return Functions' "Measurements" to the `response::Sink`
    // - Matching rx half (1) is owned by a `response::Sink`
    pub(super) response_sink: mpsc::Sender<Resp>,

    // - Passed to `Worker`s for them to request allocation of network resources
    // - Matching rx half (1) is owned by the `NetworkManager`
    pub(super) netman: NetworkManagerRef<NetResource>,

    // - Passed to `Worker`s for them to store `Timing`s
    // - Matching rx half (1) is owned by the associated `MetricsCollector`
    pub(super) timings: MetricsCollectorRef<Nanoseconds>,
}

impl<Resp, Sb, NetResource> Clone for OutChannels<Resp, Sb, NetResource>
where
    Sb: Sandbox,
    NetResource: network::Resource,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            snapman: self.snapman.clone(),
            response_sink: self.response_sink.clone(),
            netman: self.netman.clone(),
            timings: self.timings.clone(),
        }
    }
}

impl<Resp, Sb, NetResource> OutChannels<Resp, Sb, NetResource>
where
    Sb: Sandbox,
    NetResource: network::Resource,
{
    #[inline]
    pub(crate) fn new(
        to_pool: mpsc::Sender<OutboundMessage>,
        snapman: SnapshotManagerRef<Sb>,
        to_response_sink: mpsc::Sender<Resp>,
        netman: NetworkManagerRef<NetResource>,
        timings: MetricsCollectorRef<Nanoseconds>,
    ) -> Self {
        Self {
            pool: to_pool,
            snapman,
            response_sink: to_response_sink,
            netman,
            timings,
        }
    }
}

#[allow(clippy::doc_overindented_list_items)]
/// Type (internal to each [`Worker`]) that represents the state of the [`Sandbox`] owned by that
/// [`Worker`].
///
/// # States
///
/// ## Happy Path
///
/// - In the beginning, a fresh [`Worker`]'s provided [`Sandbox`] may be either:
///   * `Nonexistent`: no associated [`Sandbox`] was provided to the [`Worker`] by the
///     [`SandboxPool`], or
///   * `Snapshot`: an associated [`Sandbox`] (which only exists as a snapshot, presumably
///     persisted on disk) was provided to the [`Worker`] by the [`SandboxPool`].
/// - After preparing the [`Sandbox`], its state becomes `Running`, which means that the [`Worker`]
///   has acquired a [`RunningSlot`] and the underlying [`Sandbox`] is running, managed by a
///   [`Runtime`] implementation (e.g., [`FirecrackerContainerd`]), and tracked by us.
/// - When the [`Worker`] has finished handling the invocation, it pauses its [`Sandbox`] and its
///   state becomes `Paused`. This is when [`Worker`] releases the [`RunningSlot`] and notifies
///   [`SandboxPool`] (i.e., [`OutboundMessage::NeedWork`]) that it has reached the end of the
///   invocation.
/// - At that point, [`SandboxPool`] decides whether the [`Worker`] becomes _Idle_, remains
///   _Active_ to handle some pending control-plane operation, or starts shutting down.
/// - An _Idle_ [`Worker`] may be reactivated by the [`SandboxPool`], so it becomes _Active_ again;
///   this is when its [`Sandbox`] becomes `Running` again.
/// - When the keep-alive timer of an _Idle_ [`Worker`] (whose [`Sandbox`] is therefore `Paused`)
///   goes off, the (_Idle_) [`Worker`] receives a [`ControlMessage::ShutDown`] message by the
///   [`SandboxPool`]. The [`Worker`] shuts its [`Sandbox`] down (e.g., `UnloadVM` in the case of
///   firecracker-containerd), thus transitioning its [`Sandbox`]'s state to `Snapshot`. Therefore,
///   `Snapshot` should be the terminating state for every [`Worker`]'s **snapshotted**
///   [`Sandbox`]. Mind that this does **not** include [`Sandbox`]es which, for whatever reason,
///   have not actually created a snapshot yet: these are destroyed by the [`Worker`] itself
///   before exiting (thus their terminal state is `Nonexistent`).
///
/// ## Transitions
///
/// ### Entry states
///
/// - [`Worker`] spawn without [`Sandbox`]: `Nonexistent`
/// - [`Worker`] spawn with orphan snapshot: `Snapshot`
///
/// ### State-changing transitions
///
/// #### Normal
///
/// - `Nonexistent -> Running`: fresh [`Sandbox`] creation succeeds during preparation.
/// - `Snapshot -> Running`: snapshot load succeeds during preparation.
/// - `Paused -> Running`: resume of paused [`Sandbox`] succeeds during preparation, or after the
///                        pre-invocation snapshotting.
/// - `Running -> Paused`: invocation completes and pause succeeds; Pool prepare request pauses
///                        after preparation; failure cleanup pauses before shutdown/destroy;
///                        pre-invocation snapshotting pauses before `create_snapshot`.
/// - `Paused -> Snapshot`: idle shutdown unloads a snapshotted [`Sandbox`];
///                        prepare-failure cleanup unloads a paused snapshotted [`Sandbox`].
/// - `Paused -> Nonexistent`: destroy succeeds for a paused non-snapshotted [`Sandbox`].
/// - `Snapshot -> Nonexistent`: destroy succeeds for a snapshot-only [`Sandbox`].
/// - `Running -> Nonexistent`: destroy succeeds for a running [`Sandbox`].
///
/// #### Failure/terminal
///
/// - `Paused -> Trashed`: shutdown or destroy fails during terminal cleanup.
/// - `Snapshot -> Trashed`: destroy fails for a snapshot-only [`Sandbox`].
/// - `Running/Paused -> Trashed`: pause, shutdown or destroy fails during terminal cleanup.
///
/// #### Implementation details
///
/// - `Paused/Snapshot/Running -> Nonexistent -> Trashed` is how [`Worker::destroy_sandbox()`] is
///   implemented internally with [`SandboxState::take()`]. Conceptually, the diagram shows
///   direct transitions from the original state to `Trashed`, not `Nonexistent -> Trashed`.
/// - `Running -> Snapshot`: can conceptually occur as a (collapsed) recovery after failure: Worker
///                          pauses a running, snapshotted [`Sandbox`] and then shuts/unloads it
///                          (so `Running -> Paused -> Snapshot` is how it's actually implemented).
///
/// #### (Maybe Notable?) State-preserving ...outcomes?
///
/// - `Nonexistent -> Nonexistent`: creation/preparation fails before a [`Sandbox`] exists
///                                 (destroy on `Nonexistent` is a no-op).
/// - `Snapshot -> Snapshot`: load fails before runtime rehydration.
/// - `Paused -> Paused`: snapshot creation (success or failure).
/// - `Trashed` is terminal; normal code should never leave it except by [`Worker`] exit/reap (also
///   see [`SandboxExit`]'s [`Trashed`](SandboxExit::Trashed).
///
///
/// ## Diagram
///
///
/// ```text
///              |                             |
///              |                             |
///      +-------v-------+             +-------v-------+
///      |               |             |               |
///      |  Nonexistent  <-------------+    Snapshot   +----------------+
///      |               |             |               |                |
///      +---^---^---+---+             +---+-------^---+                |
///          |   |   |                     |       |                    |
///          |   |   |                     |       |                    |
///          |   |   |  +---------------+  |       |            +-------v-------+
///          |   |   +-->               <--+       |            |               |
///          |   |      |    Running    |          |            |    Trashed    |
///          |   +------|               +----------+------------>               |
///          |          +-----+---^-----+          |            +-------^-------+
///          |                |   |                |                    |
///          |                |   |                |                    |
///          |          +-----v---+-----+          |                    |
///          |          |               |          |                    |
///          +----------+    Paused     +----------+--------------------+
///                     |               |
///                     +---------------+
/// ```
///
///
/// ## Failures
///
/// - If preparation fails before any [`Sandbox`] exists, the state remains
///   `Nonexistent`.
/// - If preparation fails before a provided snapshot is rehydrated, the state
///   remains `Snapshot`.
/// - If cleanup after a failure can prove the [`Sandbox`] reusable, the state
///   becomes `Snapshot`.
/// - If cleanup after a failure successfully destroys the [`Sandbox`], the state
///   becomes `Nonexistent`.
/// - If cleanup after a failure cannot prove reuse or destruction, the state
///   becomes `Trashed` and the [`Worker`] exits with [`SandboxExit::Trashed`].
///   * [`SandboxPool`] must NEVER reuse a [`Sandbox`] returned as `Trashed`.
///
/// # Notes
///
/// Sadly, I cannot see how we could employ typestate without exposing the internal state (i.e.,
/// the generic type argument that represents [`Sandbox`]'s state) to the container/wrapper struct
/// (i.e., all the way up to [`Worker`]). So let's just do this with enums and runtime checks...
///
///
/// [`FirecrackerContainerd`]: crate::worker::runtime::fcctrd::FirecrackerContainerd
/// [`ControlMessage::ShutDown`]: crate::worker::ControlMessage::ShutDown
/// [`OutboundMessage::NeedWork`]: crate::worker::OutboundMessage::NeedWork
/// [`RunningSlot`]: crate::admission::RunningSlot
/// [`Runtime`]: crate::worker::runtime::Runtime
/// [`Runtime::destroy_sandbox`]: crate::worker::runtime::Runtime::destroy_sandbox
/// [`Sandbox`]: crate::worker::runtime::Sandbox
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
/// [`Worker::destroy_sandbox()`]: crate::worker::Worker::destroy_sandbox
#[derive(Debug)]
pub(crate) enum SandboxState<S> {
    /// The [`Sandbox`] is nonexistent; i.e., the [`Worker`] currently owns no
    /// [`Sandbox`].
    ///
    /// This is a possible starting state and the terminal state after successful
    /// destruction of a non-reusable [`Sandbox`].
    ///
    /// [`Worker`]: crate::worker::Worker
    Nonexistent,
    /// The [`Sandbox`] is paused (and still owned by the [`Worker`]).
    ///
    /// This is the idle/reusable live state after an invocation.  It may later be
    /// resumed, snapshotted, shut down to [`SandboxState::Snapshot`], or destroyed.
    ///
    /// [`Worker`]: crate::worker::Worker
    Paused(S),
    /// The [`Sandbox`] only exists as a snapshot.
    ///
    /// This is a possible starting state when [`SandboxPool`] assigns an orphan
    /// snapshot, and the normal terminal state for a successfully shut down
    /// snapshotted [`Sandbox`].
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    Snapshot(S),
    /// The [`Sandbox`] is up and running under the [`Runtime`], having acquired
    /// a [`RunningSlot`].
    ///
    /// [`RunningSlot`]: crate::admission::RunningSlot
    /// [`Runtime`]: crate::worker::runtime::Runtime
    Running(S),
    /// The [`Worker`] still owns a [`Sandbox`] handle, but the underlying [`Sandbox`]
    /// has been removed from the normal reusable lifecycle.
    ///
    /// This does not describe _any_ exact state: the sandbox may still be running,
    /// paused, partially destroyed, or otherwise unknown. It means cleanup or
    /// reuse was not proven.
    ///
    /// This is a terminal state. The [`Sandbox`] must never be reused, nor any
    /// normal runtime operation may ever be attempted afterwards.
    ///
    /// [`Runtime::destroy_sandbox`]: crate::worker::runtime::Runtime::destroy_sandbox
    /// [`Worker`]: crate::worker::Worker
    Trashed(S),
}

impl<S> SandboxState<S> {
    /// Take the `SandboxState` out of `self`, leaving a [`SandboxState::Nonexistent`] in its
    /// place.
    #[inline]
    pub(super) fn take(&mut self) -> SandboxState<S> {
        ::std::mem::replace(self, Self::Nonexistent)
    }
}

impl<S: Sandbox> SandboxState<S> {
    #[inline]
    pub fn id(&self) -> Option<&str> {
        use SandboxState::*;
        match self {
            Nonexistent => None,
            Paused(sb) | Snapshot(sb) | Running(sb) | Trashed(sb) => Some(sb.id()),
        }
    }
}

impl<S> From<Option<S>> for SandboxState<S> {
    /// Since the only valid starting states for [`Sandbox`] really are
    /// [`SandboxState::Nonexistent`] and [`SandboxState::Snapshot`], we map:
    /// - <code>[Some]\([Sandbox]\)</code> to <code>[SandboxState::Snapshot]\([Sandbox]\)</code>,
    ///   and
    /// - [`None`] to [`SandboxState::Nonexistent`].
    #[inline]
    fn from(sandbox: Option<S>) -> Self {
        match sandbox {
            Some(sandbox) => Self::Snapshot(sandbox),
            None => Self::Nonexistent,
        }
    }
}

impl<S> From<SandboxState<S>> for Option<S> {
    fn from(sandbox_state: SandboxState<S>) -> Self {
        use SandboxState::*;
        match sandbox_state {
            Nonexistent => None,
            Paused(sb) | Snapshot(sb) | Running(sb) | Trashed(sb) => Some(sb),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxStateRef {
    Nonexistent,
    Paused,
    Snapshot,
    Running,
    Trashed,
}

impl<S> AsRef<SandboxStateRef> for SandboxState<S> {
    fn as_ref(&self) -> &SandboxStateRef {
        match self {
            SandboxState::Nonexistent => &SandboxStateRef::Nonexistent,
            SandboxState::Paused(_) => &SandboxStateRef::Paused,
            SandboxState::Snapshot(_) => &SandboxStateRef::Snapshot,
            SandboxState::Running(_) => &SandboxStateRef::Running,
            SandboxState::Trashed(_) => &SandboxStateRef::Trashed,
        }
    }
}

/// Final result returned by a [`Worker`] task to [`SandboxPool`] when the
/// [`Worker`] is joined during reap.
///
/// This deliberately separates the Worker-side success/error status from the
/// final [`Sandbox`] disposition. Pool needs both pieces independently:
/// - `result` tells whether the Worker exited cleanly or while propagating a
///   [`worker::Error`];
/// - `sandbox` tells whether Pool receives no sandbox, a reusable snapshot, or
///   a [`Sandbox`] object that must be treated as trashed.
///
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
/// [`worker::Error`]: crate::worker::error::Error
#[derive(Debug)]
pub(crate) struct WorkerExit<S> {
    pub(crate) result: super::Result<()>,
    pub(crate) sandbox: SandboxExit<S>,
}

/// Final [`Sandbox`] disposition returned by a [`Worker`] at reap time.
///
/// This is derived from the [`Worker`]'s final [`SandboxState`]:
///
/// - [`None`](Self::None) corresponds to [`SandboxState::Nonexistent`].
/// - [`Reusable`](Self::Reusable) corresponds to [`SandboxState::Snapshot`].
///   [`SandboxPool`] may store the [`Sandbox`] as an orphan snapshot for
///   future reuse.
/// - [`Trashed`](Self::Trashed) normally corresponds to
///   [`SandboxState::Trashed`]. As a defensive fallback,
///   [`SandboxState::Paused`] and [`SandboxState::Running`] also convert to
///   `Trashed`, because a [`Worker`] exiting before reaching
///   [`Snapshot`](SandboxState::Snapshot) or
///   [`Nonexistent`](SandboxState::Nonexistent) has not proved reusability.
///
/// [`Sandbox`]: crate::worker::runtime::Sandbox
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
#[derive(Debug)]
pub(crate) enum SandboxExit<S> {
    None,
    Reusable(S),
    Trashed(S),
}

impl<S> From<SandboxState<S>> for SandboxExit<S> {
    fn from(sandbox_state: SandboxState<S>) -> Self {
        match sandbox_state {
            SandboxState::Nonexistent => SandboxExit::None,
            SandboxState::Snapshot(sb) => SandboxExit::Reusable(sb),
            SandboxState::Paused(sb) | SandboxState::Running(sb) => {
                // Defensive fallback; nowadays, normal failure cleanup should
                // have first transitioned these to `SandboxState::Trashed`.
                SandboxExit::Trashed(sb)
            }
            SandboxState::Trashed(sb) => SandboxExit::Trashed(sb),
        }
    }
}
