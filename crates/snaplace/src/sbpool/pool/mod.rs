mod control;
mod reinstate;

use std::{
    borrow::Cow, collections::HashMap, fmt::Debug, marker::PhantomData, sync::atomic::Ordering,
    time::Duration,
};

use itertools::Itertools;
use redb::Database;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::{interval_at, sleep_until, Instant, MissedTickBehavior},
};
use tracing::{debug, error, event, info, instrument, trace, warn, Level};
use triomphe::{Arc, ArcBorrow};
use ubyte::{ByteUnit, ToByteUnit};

use self::control::PendingControlRequests;
use crate::{
    admission::{DispatchMode, DispatchRequest, DispatchResponse},
    conf::{SnaplaceConfig, WorkerConfig},
    metadata::{FunctionInfo, FunctionMetadataStore},
    metrics::{MetricsCollectorRef, Nanoseconds},
    network::{self, NetworkManagerRef},
    sbpool::{
        api::PoolControlMessage,
        cpuset::CpuSetManager,
        error::{Error, Result},
        keepalive, PoolEvent, SandboxPoolHandle,
    },
    snapman::SnapshotManagerRef,
    worker::{self, Sandbox, SandboxExit, SpawnError, Worker, WorkerExit, WorkerHandle, WorkerId},
    FunctionId, Request, Response, SandboxId,
};

/// [`SandboxPool`]'s inbound channels.
#[derive(Debug)]
struct InChannels<Req: Request> {
    // - `DispatchRequest`s arrive from the `AdmissionController` to the `SandboxPool`
    // - Matching tx half (1) is owned by the `AdmissionController`
    admission: mpsc::Receiver<DispatchRequest<Req>>,

    // - `PoolControlMessage`s arrive from the control-API server, and possibly the `Orchestrator`
    // - Matching tx halves are owned by `SandboxPoolHandle` (1, owned by the `Orchestrator`),
    //   and by control plane threads/tasks (none up to many, 1 per `SandboxPoolRef`).
    control_plane: mpsc::Receiver<PoolControlMessage>,

    // - "Some TODO messages" arrive from the `SnapshotManager` to the `SandboxPool`
    // - Matching tx half (1) is owned by the `SnapshotManager`
    snapman: mpsc::Receiver<()>, // FIXME: type?!

    // - "Status changes" arrive from `Worker`s to the `SandboxPool`
    // - Matching tx halves are owned by all `Worker`s, and by us (1, to allow passing a new tx
    // half to each new `Worker` we may spawn in the future)
    workers: mpsc::Receiver<worker::OutboundMessage>,

    // NOTE(ckatsak): This Receiver never closes, because Pool itself always holds an open Sender
    // - Some sort of keepalive timeout messages arrive from "keep-alive timer tasks" to Pool
    // - Matching tx halves are owned by the "keep-alive timer tasks" (many) and by ourselves (1,
    // inside `KeepAlive`, so that we can pass it to the "keep-alive timer tasks" that we spawn)
    idle_timers: mpsc::Receiver<WorkerId>,

    quit_rx: broadcast::Receiver<()>,
}

/// Helper type grouping stuff (originally synchronization primitives mostly) stored in
/// [`SandboxPool`] with the purpose of being [`Clone`]d and passed to new [`Worker`]s spawned
/// by the [`SandboxPool`].
#[derive(Debug, Clone)]
struct WorkerAux<Resp, RtCfg, Sb: Sandbox, NetResource: network::Resource> {
    /// Sender halves of channels stored only to be passed to new [`Worker`]s.
    channels: worker::OutChannels<Resp, Sb, NetResource>,

    /// [`Worker`]s' configuration provided by the user.
    config: WorkerConfig<RtCfg>,
}

#[derive(Debug)]
struct KeepAlive<K: keepalive::Policy> {
    policy: K,
    timers: HashMap<WorkerId, JoinHandle<Result<()>>, crate::BuildHasher>,

    /// Sender half stored only to be passed to new "keep-alive timer" tasks, allowing them to
    /// notify [`SandboxPool`].
    // - Keep-alive timeout notifications are sent from "keep-alive timer" tasks to the Pool
    // - Matching rx half (1) lives in SandboxPool (inside `InChannels`)
    timeout_tx: mpsc::Sender<WorkerId>,

    /// [`Vec`] passed by the [`SandboxPool`] to its underlying [eviction policy] to be filled
    /// with [`Worker`]s recommended for eviction.
    ///
    /// ## Notes
    ///
    /// This field is internally used as a temporary buffer, to avoid reallocating [`Vec`]s
    /// when the system is under memory pressure:
    ///
    /// 1. [`SandboxPool`] only ever writes this field to [`Vec::clear`] it, right before it
    ///    passes it to the [eviction policy].
    /// 2. The [eviction policy] writes the [`Vec`] to fill it with references of the
    ///    [`Worker`]s suggested for eviction.
    /// 3. Right after that, [`SandboxPool`] reads the references to trigger the evictions.
    ///
    /// Accessing this field and its content at any other time is not expected, as the
    /// content will probably be garbage (i.e., inconsistent with the state of the system).
    ///
    /// [eviction policy]: crate::sbpool::keepalive::eviction::Policy
    eviction_victims: Vec<(FunctionId, WorkerId)>,
}

impl<K: keepalive::Policy> KeepAlive<K> {
    const TIMER_NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(60);

    /// Initial capacity of the data structure where the [eviction policy] buffers its results
    /// until the [`SandboxPool`] processes them.
    ///
    /// [`SandboxPool`]: crate::sbpool::pool::SandboxPool
    /// [eviction policy]: crate::sbpool::keepalive::eviction::Policy
    const EVICTION_VICTIMS_BUFFER_CAP: u8 = 8;

    #[instrument(
        level = Level::TRACE,
        skip(deadline, to_pool),
        fields(alarm = ?(deadline - Instant::now()))
    )]
    async fn timer_run(
        worker_id: WorkerId,
        deadline: Instant,
        to_pool: mpsc::Sender<WorkerId>,
    ) -> Result<()> {
        sleep_until(deadline).await;
        to_pool
            .send_timeout(worker_id, Self::TIMER_NOTIFICATION_TIMEOUT)
            .await
            .map_err(|_| Error::KeepAliveTimerSendTimeout(worker_id))
    }

    #[instrument(
        level = Level::TRACE, skip(self, deadline), fields(alarm = ?(deadline - Instant::now()))
    )]
    #[inline]
    async fn spawn_timer(&mut self, worker_id: WorkerId, deadline: Instant) {
        let timer = ::tokio::spawn({
            let timer_tx = self.timeout_tx.clone();
            async move { Self::timer_run(worker_id, deadline, timer_tx).await }
        });
        if let Some(old_timer) = self.timers.insert(worker_id, timer) {
            // NOTE(ckatsak): This should never happen as long as we keep track of the timers
            // correctly. If it does occur though, then we could try aborting the timer. If we make
            // it, then the system remains sound, but some Worker(s) probably remain Idle for some
            // wrong duration.
            old_timer.abort();
            error!(?worker_id, "Old timer overwritten"); // FIXME(ckatsak): panic?
        }
    }

    //#[instrument(level = Level::TRACE, skip(self))]
    #[inline]
    fn stop_timer(&mut self, worker_id: WorkerId) {
        if let Some(timer) = self.timers.remove(&worker_id) {
            // NOTE(ckatsak): This is inherently race-y; aborting the timer does not mean that:
            // - the timer has not already completed (so a notification might be waiting for us to
            // process at the event loop);
            // - the timer will not make it to completion before the abortion actually takes place
            // (it does not happen in instantly, it rather requires some time -- check tokio docs).
            // For this reason, whenever we (the Pool) receive a keepalive notification about a
            // WorkerId, we also need to check whether we are still interested for this timer (by
            // looking it up in `self.keepalive.timers`). Also see `Self::handle_keepalive_timeout`
            timer.abort();
        }
    }

    /// The code comments in the body of [`Self::stop_timer`] also apply here.
    ///
    /// # Safety
    ///
    /// `worker_id` must be associated with a timer task.
    ///
    /// Only use this right after checking with <code>[Self::timers].[contains_key]</code>.
    ///
    /// [contains_key]: std::collections::HashMap::contains_key
    #[inline]
    unsafe fn stop_timer_unchecked(&mut self, worker_id: WorkerId) {
        unsafe { self.timers.remove(&worker_id).unwrap_unchecked().abort() }
    }
}

/// [`SandboxPool`]'s memory bookkeeping.
#[derive(Clone, Copy)]
struct MemoryStats {
    /// The total amount of memory that [`SandboxPool`] is allowed to use for spawned
    /// [`Sandbox`]es.
    ///
    /// This is configurable at deployment time, and is practically a constant later on.
    total: ByteUnit,
    /// The amount of memory that is **currently in use** and tracked by [`SandboxPool`] for
    /// its "alive" [`Sandbox`]es (be they managed by either _Active_ or _Idle_ [`Worker`]s).
    inuse: ByteUnit,
    /// Memory that is currently being reclaimed from _Dying_ [`Worker`]s that are about to be
    /// reaped.
    reclaimed: ByteUnit,
    /// Reaching this amount of used memory triggers [`Worker`] evictions,
    /// aiming to reduce it down to [`eviction_thres_lo`](Self::eviction_thres_lo).
    eviction_thres_hi: ByteUnit,
    /// Target amount of used memory when triggering [`Worker`] evictions
    /// (i.e., when reaching [`eviction_thres_hi`](Self::eviction_thres_hi)).
    eviction_thres_lo: ByteUnit,
}

impl MemoryStats {
    /// Reclaim the specified amount of memory (i.e., account for it as "being reclaimed" rather
    /// than "currently in use").
    fn reclaim(&mut self, amount: ByteUnit) {
        self.inuse -= amount;
        self.reclaimed += amount;
    }
}

impl ::std::fmt::Display for MemoryStats {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(
            f,
            "Memory {{ total: {}, inuse: {}, reclaimed: {}, eviction_low: {}, eviction_high: {} }}",
            self.total, self.inuse, self.reclaimed, self.eviction_thres_lo, self.eviction_thres_hi,
        )
    }
}

impl ::std::fmt::Debug for MemoryStats {
    #[inline]
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        <Self as ::std::fmt::Display>::fmt(self, f)
    }
}

/// Memory-accounting classification for a live [`Worker`].
///
/// This records where Pool has charged the [`Worker`]'s Function memory while
/// the Worker is live.
/// On a normal reap outcome (i.e., not in case of trashed [`Sandbox`]es), this
/// tells [`SandboxPool::reap_worker`] which [`MemoryStats`] bucket to decrement.
///
/// The value tracks how Pool has currently accounted the Worker's associated
/// sandbox memory:
///
/// - [`InUse`](Self::InUse):
///   the [`Worker`]'s Function memory is currently charged into
///   [`MemoryStats::inuse`]. This is the normal case for a live [`Worker`]
///   that has been spawned for regular invocation/control handling.
///
/// - [`Reclaimed`](Self::Reclaimed):
///   the [`Worker`]'s Function memory has already been reclassified from
///   [`MemoryStats::inuse`] into [`MemoryStats::reclaimed`] as part of a
///   Pool-side transition to `_Dying_`. On normal reap outcomes, such a
///   [`Worker`] must decrement [`MemoryStats::reclaimed`], not
///   [`MemoryStats::inuse`]
///
/// - [`Uncharged`](Self::Uncharged):
///   this [`Worker`] never contributed to either memory bucket. This is used
///   for synthetic control-only [`Worker`]s (currently the dedicated
///   snapshot-destruction [`Worker`]s spawned solely to destroy an orphan
///   snapshot without rehydrating it into a live [`Sandbox`]). Reaping such a
///   [`Worker`] must not adjust [`SandboxPool`]'s memory accounting at all.
///
/// Keeping this classification explicit allows [`SandboxPool::reap_worker`] to
/// perform correct memory accounting without inferring it indirectly from
/// side effects such as cpuset ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerMemAccounting {
    /// The [`Worker`]'s Function memory is currently charged into
    /// [`MemoryStats::inuse`].
    InUse,

    /// The [`Worker`]'s Function memory has already been moved into
    /// [`MemoryStats::reclaimed`].
    Reclaimed,

    /// The [`Worker`] never contributed to [`SandboxPool`]'s memory accounting.
    Uncharged,
}

/// Helper struct to pass around information owned by the [`SandboxPool`] that may be useful
/// for decision making (e.g., [keep-alive policies], [eviction policies], etc).
///
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [keep-alive policies]: crate::sbpool::keepalive::Policy
/// [eviction policies]: crate::sbpool::keepalive::eviction::Policy
pub struct PoolContext<'p, S, FunctionInfo>
where
    S: FunctionMetadataStore<FunctionInfo>,
{
    pub(super) store: ArcBorrow<'p, S>,
    pub(super) worker_deadlines: &'p mut HashMap<WorkerId, Instant, crate::BuildHasher>,
    pub(super) victims: &'p mut Vec<(FunctionId, WorkerId)>,
    _function_info: PhantomData<fn() -> FunctionInfo>,
}

pub(crate) struct SandboxPool<Req, Resp, Store, KeepAlivePolicy, Runtime, Issuer, NetResource>
where
    Req: Request,
    KeepAlivePolicy: keepalive::Policy,
    Runtime: worker::Runtime,
    Issuer: worker::issuer::RequestIssuer<Req, Resp>,
    NetResource: network::Resource,
{
    /// See [SandboxPoolConfig::retain_snapshots].
    retain_snapshots: bool,
    /// Tracks CPU allocations to [`Sandbox`]es.
    cpuset: CpuSetManager,
    /// Tracks memory allocations to [`Sandbox`]es.
    memory: MemoryStats,

    /// Stored [`FunctionMetadata`], indexed by Function's unique ID (derived by the incoming
    /// [`Request`]).
    store: Arc<Store>,
    /// Persistent state, possibly required across boots.
    ///
    /// # Notes
    ///
    /// `SandboxPool` currently needs that to:
    /// - reinstate [`Sandbox`] snapshots _on boot_, which needs tables [`Tables::FUNCTIONS`] (to
    ///   read the [`FunctionInfo`] required to initialize [`Runtime`]s for the reinstatement) and
    ///   [`Tables::SNAPSHOTS`] (to read the actual [`Sandbox::SnapshotState`]s).
    /// - update [`Tables::SNAPSHOTS`] _upon shutting down_, either to delete it altogether or
    ///   to sync it in accordance with the [`Sandbox`] snapshots that are actually preserved.
    ///
    /// [`Runtime`]: worker::Runtime
    db: Arc<Database>,

    ///// The ("main", owning) handle of the [`NetworkManager`] is owned by `SandboxPool`.
    /////
    ///// # Notes
    /////
    ///// Under normal circumstances, the [`NetworkManager`] does not exit unless all handle and
    ///// refs to it have been dropped (so that its channels' sender halves are dropped). The owners
    ///// include us (the Pool, holding the handle and 1 ref), as well as *all* _Active_ and
    ///// _Idle_ `Worker`s (each holds 1 ref).
    //// NOTE(ckatsak): `NetworkManager` won't break from its shutdown loop until Pool is dropped.
    /// A reference to the [`NetworkManager`].
    ///
    /// For now, `SandboxPool` uses it only when destroying [`Sandbox`]es.
    ///
    /// [`NetworkManager`]: crate::network::NetworkManager
    netman: NetworkManagerRef<NetResource>,

    keepalive: KeepAlive<KeepAlivePolicy>,

    from: InChannels<Req>,

    /// To send [`PoolEvent`] updates to the [`AdmissionController`].
    ///
    /// [`AdmissionController`]: crate::admission::AdmissionController
    to_admission: mpsc::Sender<PoolEvent>,
    /// See [`AdmissionConfig::default_max_live_instances_per_func`].
    ///
    /// [`AdmissionConfig::default_max_live_instances_per_func`]:
    ///     crate::conf::AdmissionConfig::default_max_live_instances_per_func
    default_worker_cap_per_func: usize,

    /// All [`Worker`]s' (owning) handles.
    worker_handles: HashMap<WorkerId, WorkerHandle<Runtime::Sandbox, Req>, crate::BuildHasher>,
    /// All [`Worker`]s' deadlines.
    worker_deadlines: HashMap<WorkerId, Instant, crate::BuildHasher>,
    /// Reverse index of live ownership: [`SandboxId`] -> owning [`WorkerId`].
    worker_by_sandbox: HashMap<SandboxId, WorkerId, crate::BuildHasher>,
    /// All snapshotted [`Sandbox`]es for all Functions.
    snapshots: HashMap<FunctionId, Vec<Runtime::Sandbox>, crate::BuildHasher>,
    /// [`Sandbox`]es returned by failed and reaped [`Worker`]s.
    ///
    /// ## Notes
    ///
    /// For now, no [`Sandbox`]es actually end up here, and this mostly exists precautionarily.
    /// The question remains, though: what should we do in such cases? When/how should they be
    /// cleaned up? _(TODO)_
    trashed_sandboxes: Vec<Runtime::Sandbox>,
    /// Helper field grouping synchronization primitives stored in `SandboxPool` with the sole
    /// purpose of being [`Clone`]d and passed to new entities spawned by the `SandboxPool` (i.e.,
    /// [`Worker`]s and "keep-alive timer" tasks).
    worker_aux: WorkerAux<Resp, Runtime::Config, Runtime::Sandbox, NetResource>,

    /// Pending control-plane API response receivers.
    pending: PendingControlRequests,

    _workers_request_issuer: PhantomData<Issuer>,
}

impl<Req, Resp, Store, KeepAlivePolicy, Runtime, Issuer, NetResource>
    SandboxPool<Req, Resp, Store, KeepAlivePolicy, Runtime, Issuer, NetResource>
where
    Req: Request,
    Resp: Response,
    Store: FunctionMetadataStore<Runtime::FunctionInfo>,
    KeepAlivePolicy: keepalive::Policy,
    Runtime: worker::Runtime<NetResource = NetResource>,
    Issuer: worker::issuer::RequestIssuer<Req, Resp>,
    NetResource: network::Resource,
{
    const CHAN_FROM_WORKERS_CAP: usize = 512;
    const MAX_FINISHED_TIMERS: usize = 512;

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn spawn(
        glob_cfg: &SnaplaceConfig<Runtime::Config>,
        func_metadata_store: Arc<Store>,
        db: Arc<Database>,
        keepalive_policy: KeepAlivePolicy,
        netman: NetworkManagerRef<NetResource>,
        from_admission: mpsc::Receiver<DispatchRequest<Req>>,
        to_admission: mpsc::Sender<PoolEvent>,
        from_snapman: mpsc::Receiver<()>, // TODO: type?!
        snapman: SnapshotManagerRef<Runtime::Sandbox>,
        timings: MetricsCollectorRef<Nanoseconds>,
        to_response_sink: mpsc::Sender<Resp>,
        quit_rx: broadcast::Receiver<()>,
    ) -> Result<SandboxPoolHandle> {
        let config = &glob_cfg.sandbox_pool;
        let workers_config = &glob_cfg.workers;

        // Validate input config
        if !config.eviction_lo_watermark_pct.is_finite()
            || !config.eviction_hi_watermark_pct.is_finite()
            || config.eviction_lo_watermark_pct < 0.
            || config.eviction_lo_watermark_pct > config.eviction_hi_watermark_pct
            || config.eviction_hi_watermark_pct > 1.
        {
            return Err(Error::Init {
                msg: "eviction watermark percentages must be: 0.0 <= low <= high <= 1.0".into(),
                source: None,
            });
        }

        let (worker_state_tx, from_workers) = mpsc::channel(Self::CHAN_FROM_WORKERS_CAP);
        let (keepalive_timeout_tx, from_idle_timers) = mpsc::channel(Self::MAX_FINISHED_TIMERS);
        let (to_pool_ctrl, from_control_plane) = mpsc::channel(16); // TODO: chan cap FIXME(XXX)

        let cpuset = CpuSetManager::new(&config.cpuset, glob_cfg.admission.max_concurrency)
            .map_err(|err| Error::Init {
                msg: "failed to initialize CpuSetManager".into(),
                source: Some(Box::new(Error::CpuSet(err))),
            })?;
        info!(sandbox.cpus = ?cpuset.all_cpus());

        let mut pool = Self {
            retain_snapshots: config.retain_snapshots,
            cpuset,
            memory: MemoryStats {
                total: config.max_memory,
                inuse: 0.bytes(),
                reclaimed: 0.bytes(),
                eviction_thres_hi: ByteUnit::Byte(
                    (config.max_memory.as_u64() as f64 * config.eviction_hi_watermark_pct) as u64,
                ),
                eviction_thres_lo: ByteUnit::Byte(
                    (config.max_memory.as_u64() as f64 * config.eviction_lo_watermark_pct) as u64,
                ),
            },
            store: func_metadata_store,
            db,
            netman: netman.clone(),
            trashed_sandboxes: Default::default(),

            keepalive: KeepAlive {
                policy: keepalive_policy,
                timers: Default::default(),
                timeout_tx: keepalive_timeout_tx,
                eviction_victims: Vec::with_capacity(
                    KeepAlive::<KeepAlivePolicy>::EVICTION_VICTIMS_BUFFER_CAP as _,
                ),
            },

            from: InChannels {
                admission: from_admission,
                control_plane: from_control_plane,
                snapman: from_snapman,
                workers: from_workers,
                idle_timers: from_idle_timers,
                quit_rx,
            },

            to_admission,
            default_worker_cap_per_func: glob_cfg.admission.default_max_live_instances_per_func,

            snapshots: Default::default(),
            worker_handles: Default::default(),
            worker_deadlines: Default::default(),
            worker_by_sandbox: Default::default(),
            worker_aux: WorkerAux {
                channels: worker::OutChannels::new(
                    worker_state_tx,
                    snapman,
                    to_response_sink,
                    netman,
                    timings,
                ),
                config: workers_config.clone(),
            },

            pending: Default::default(),

            _workers_request_issuer: PhantomData,
        };

        // Reinstate any snapshottted sandboxes from the database.
        if config.retain_snapshots {
            pool.snapshots = pool
                .reinstate_snapshots_from_db::<crate::BuildHasher>()
                .await
                .map_err(|err| Error::Init {
                    msg: "failed to reinstate snapshots from database".into(),
                    source: Some(Box::new(err)),
                })?;
        }

        let handle = ::tokio::spawn(async move { pool.run().await });

        Ok(SandboxPoolHandle::new(to_pool_ctrl, handle))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    pub(crate) async fn run(&mut self) -> Result<()> {
        enum PoolboundPlane<Req: Request> {
            Data(DispatchRequest<Req>),
            Control(PoolControlMessage),
            AdmissionClosed,
        }

        let mut ctrl_plane_up = true;
        loop {
            ::tokio::select! {
                biased;

                quit_res = self.from.quit_rx.recv() => {
                    match quit_res {
                        Ok(()) => warn!("Received quit notification!"),
                        Err(err) => error!("Quit channel unexpectedly emitted: {err}"),
                    }
                    // ¿TODO(ckatsak): Clean up anything or notify anyone before quitting?
                    break
                }

                Some(worker_id) = self.from.idle_timers.recv() => {
                    // NOTE(ckatsak): This channel can never return None, because we ourselves keep
                    // a Sender half always open (to pass it to the new Worker tasks)! It may only
                    // return None if we ourselves close it (e.g., in Self::shutdown), in which
                    // case we are already shutting down, hence no need to handle this here.
                    // TODO(ckatsak): We just received a notification from a timer task that the
                    // KeepAlive period of its associated (idle) Worker has elapsed. We handle this
                    // by shutting down the associated Worker (but keeping its Vm).
                    if let Err(err) = self.decommission_idle_worker(worker_id, None).await {
                        error!(error = ?err, "Failed to handle keep-alive timeout: {err:#}");
                        break; // FIXME: Don't shutdown normally?!
                    }
                }

                Some(worker_msg) = self.from.workers.recv() => {
                    // NOTE(ckatsak):
                    // - This channel can never return None, because we ourselves keep a Sender
                    //   half always open (to pass it to the new Worker tasks)! It may only
                    //   return None if we ourselves close it (e.g., in Self::shutdown), in which
                    //   case we are already shutting down, hence no need to handle this here.
                    // - Workers notify us when they are done handling an invocation, so that we
                    //   remove them from the Active list and (depending on the keep-alive policy
                    //   in place) either move them to the Idle list (and spawn an associated timer
                    //   tracking the Idle time), or just shut them down (but keep their Sandbox).
                    // - Workers notify us when they are done cleaning up stuff after being sent
                    //   a ShutDown signal (e.g., when their keep-alive timer has elapsed) so
                    //   that we reap them (join their tokio task and retrieve their Sandbox).
                    // - Workers notify us upon (new) Sandbox creation, letting us know its ID for
                    //   our `SandboxId -> WorkerId` reverse mapping.
                    if let Err(err) = self.handle_worker_status(worker_msg).await {
                        error!(error = ?err, "Failed to handle change of Worker's status: {err:#}");
                        break // FIXME: Don't shutdown normally?!
                    }
                }

                snap_opt = self.from.snapman.recv() => {
                    // TODO/FIXME(ckatsak):
                    // - What does SnapshotManager sends us? Perhaps snapshot migration requests?
                    // - In case of snapshot migration, perhaps we should enter a state where we
                    // only handle requests with brand new VMs (rather than hydrating snapshots) so
                    // that we (the Pool) can safely pass ownership of most (all?) FunctionMetadata
                    // to the SnapshotManager to do the work (and take it back afterwards)? Also:
                    //   * Actor + State pattern combination?! How, in Rust?!
                    //   * Perhaps we should rethink how Pool stores its data...
                    // - Perhaps rethink all this on subsequent iterations rather than now...?
                    match snap_opt {
                        Some(_) => warn!("Received something from SnapshotManager!"),
                        None => warn!("SnapshotManager's channel just closed!"),
                    }
                }

                data_or_ctrl = async {
                    // - Rather than enforcing an ordering between data and control planes, poll
                    //   them both in a separated nested _unbiased_ `select!`, allowing tokio to
                    //   be fair and not prioritize one over the other, thus avoiding starvation.
                    //   Alternatively, we could just prioritize control plane, since we'd not
                    //   expect it to be capable of starving the data plane (while the opposite
                    //   is both easier and worse). This is probably the safest route though.
                    // - The auxiliary future is just another branch of the outer `select!`;
                    //   when it is pending, the outer loop continues polling quit, timers,
                    //   Worker-status, and snapman normally.
                    // - Poll control-plane mpsc only if the channel was not found closed earlier.
                    //   This allows serving invocations even without a functional control plane.
                    // - Looping allows us to update local `ctrl_plane_up` (later used as a branch
                    //   precondition to avoid evaluating and polling control plane in subsequent
                    //   iterations) before continuing polling the data plane for a value.
                    loop {
                        ::tokio::select! {
                            disp_opt = self.from.admission.recv() => match disp_opt {
                                Some(dreq) => break PoolboundPlane::Data(dreq),
                                None => break PoolboundPlane::AdmissionClosed,
                            },
                            ctrl_opt = self.from.control_plane.recv(), if ctrl_plane_up => {
                                match ctrl_opt {
                                    Some(ctrl_msg) => break PoolboundPlane::Control(ctrl_msg),
                                    None => ctrl_plane_up = false,
                                }
                            },
                        }
                    }
                } => match data_or_ctrl {
                    PoolboundPlane::Data(dreq) => {
                        trace!("Dispatching `{dreq:?}`...");
                        match self.handle_dispatch(dreq).await {
                            Ok(()) => {}
                            Err(err @ Error::AdmissionUnresponsive(_)) => {
                                // Not much we can do here; AdmissionController looks dead, so the
                                // system is probably dysfunctional anyway (?) Log and shutdown.
                                error!(error = ?err, "Shutting down due to fatal error: {err:#}");
                                break // TODO: Don't shutdown normally?
                            }
                            Err(err) => {
                                // NOTE(ckatsak): We should probably just log and tear the whole
                                // thing down at this point, since any of these must have occured:
                                // - cannot spawn new Workers
                                // - UUIDv4 collision
                                error!(error = ?err, "Fatal error while dispatching request: {err:#}");
                                // ¿TODO(ckatsak): Anything special before shutting down in this case?
                                break // TODO: Don't shutdown normally?
                            }
                        }
                    },
                    PoolboundPlane::Control(ctrl_msg) => {
                        if let Err(err) = self.handle_control(ctrl_msg).await {
                            error!(error = ?err, "Failure while handling control operation: {err:#}");
                        }
                    },
                    PoolboundPlane::AdmissionClosed => {
                        // Reaching this means that AdmissionController has shut down.
                        // Therefore, we should probably (gracefully?) shut down as well...?
                        // Anything special before that?
                        break
                    },
                }
            }
        }
        self.shutdown().await
    }

    /// Helper method to notify [`AdmissionController`] of a new [`PoolEvent`]
    /// on a best-effort basis.
    ///
    /// [`AdmissionController`]: crate::admission::AdmissionController
    #[instrument(level = Level::TRACE, skip(self))]
    fn emit_admission_event(&self, event: PoolEvent) {
        match self.to_admission.try_send(event) {
            Ok(()) => {}
            Err(err @ mpsc::error::TrySendError::Full(_)) => {
                trace!("Dropping advisory PoolEvent: {err:#}");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!("AdmissionController's channel is closed")
            }
        }
    }

    /// Handle an incoming [`DispatchRequest`].
    ///
    /// # Errors
    ///
    /// - [`Error::WorkerCreation`] is returned in case of failure to spawn a new [`Worker`].
    /// - [`Error::AdmissionUnresponsive`] if unable to respond to the [`AdmissionController`]
    ///   through the [`oneshot`] channel associated with the [`DispatchRequest`].
    /// - ... TODO
    ///
    /// [`AdmissionController`]: crate::admission::AdmissionController
    /// [`oneshot`]: tokio::sync::oneshot
    #[instrument(level = Level::TRACE, skip_all, fields(function.id = %dreq.function_id()))]
    async fn handle_dispatch(&mut self, dreq: DispatchRequest<Req>) -> Result<()> {
        #[inline]
        fn _deny<Req: Request>(
            dreq: DispatchRequest<Req>,
            dresp: DispatchResponse<Req>,
        ) -> Result<()> {
            dreq.reply(dresp)
                .map_err(|_| Error::AdmissionUnresponsive("failed denying request".into()))
        }
        #[cold]
        fn deny_unknown_function<Req: Request>(dreq: DispatchRequest<Req>) -> Result<()> {
            debug!("Denying invocation request for unregistered Function");
            _deny(dreq, DispatchResponse::unknown_function())
        }
        #[inline]
        fn deny_oom<Req: Request>(dreq: DispatchRequest<Req>) -> Result<()> {
            info!("Denying invocation request due to memory overload");
            _deny(dreq, DispatchResponse::oom())
        }
        #[cold]
        fn deny_no_idle<Req: Request>(dreq: DispatchRequest<Req>) -> Result<()> {
            info!("Denying invocation request for existing _Idle_ Worker only");
            _deny(dreq, DispatchResponse::no_idle_worker())
        }
        #[inline]
        fn deny_function_worker_cap<Req: Request>(dreq: DispatchRequest<Req>) -> Result<()> {
            info!("Denying invocation request as Function has reached its Worker cap");
            _deny(dreq, DispatchResponse::function_worker_cap())
        }

        let function_id = dreq.function_id();
        // NOTE: This is where Pool encounters any possibly yet-unseen Function for the first
        // time. Therefore, we may "try_get" once here, and always "get" from now on.
        // TODO: For now, FunctionIds are never cleaned up in FunctionMetadataStore
        if !self.store.function_exists(function_id) {
            return deny_unknown_function(dreq);
        }
        self.store
            .function_stats(function_id)
            .dispatch_attempts
            .fetch_add(1, Ordering::Relaxed);

        // First, attempt to find an existing Idle Worker
        let worker_id = if let Some(worker_id) = self.pick_idle_worker(function_id) {
            match self.store.remove_idle_worker(worker_id, function_id) {
                Ok(true) => Ok(worker_id),
                Ok(false) => unreachable!("Worker just found to be tracked as Idle"),
                Err(err) => Err(Error::Metadata(Box::new(err))),
            }
        } else {
            // If there is no existing Idle Worker, and admission layer specifically requested a
            // Worker reuse, deny the invocation request appropriately.
            // NOTE(ckatsak): For now, `ReuseIdleOnly` is never admitted, so this check is probably
            // useless; it might be useful in the future, when we actually expand admission logic.
            if dreq.mode() == DispatchMode::ReuseIdleOnly {
                ::std::hint::cold_path(); // TODO: remove hint when Admission actually can emit this
                return deny_no_idle(dreq);
            }

            let func = self.store.registered_function(function_id);
            let mem_requested = func.info().memory();

            // If Function's capacity to spawn new Workers has hit its limit, deny accordingly.
            let worker_cap = func
                .admission()
                .and_then(|a| a.max_live_instances)
                .unwrap_or(self.default_worker_cap_per_func);
            let num_live_workers =
                self.store.num_idle(Some(function_id)) + self.store.num_active(Some(function_id));
            if num_live_workers >= worker_cap {
                return deny_function_worker_cap(dreq);
            }

            // If there is no existing Idle Worker capable of serving this Request, can we
            // immediately spawn a new one? (When) should we start evicting existing ones?
            //
            // If there is no existing Idle Worker, and we do not have the memory
            // capacity to spawn a new Worker (to either create a new Sandbox or
            // rehydrate any of our tracked snapshots), we have three options:
            //  (1) Immediately deny the request.
            //  (2) Requeue the request to be dispatched later (though such a mechanism
            //      is not implemented yet).
            //  (3) Make room for a new Worker. This can be accomplished by shutting
            //      down one or more Idle Workers (thus orphaning one or more Sandboxes
            //      whose snapshots should already be stored for later use). If there
            //      are no Idle Workers to kill, fall back to (1) or (2)?
            // Current approach:
            // - Since the SandboxPool runs its own event loop, there is no easy way to
            //   evict a Worker and synchronously wait for it to shut down (and even if
            //   there was, it would probably introduce significant latency overheads).
            //   Therefore we do (attempt to) trigger Worker evictions, but we wait to
            //   reap the evicted Workers asynchronously, at the event loop, as usual.
            // - Meanwhile, what about the pending (current) invocation request, which
            //   cannot be handled immediately? For now, our only option is to deny it.
            // - TODO(ckatsak): When we implement "requeuing" at the Dispatcher level,
            //   requests can be returned to the Dispatcher, who can count the number
            //   of such requeuing attempts before discarding the invocation request
            //   (possibly taking into account some configurable latency SLO? perhaps
            //   us, the Pool, have additional statistics about the Function's past
            //   invocations, which might be useful to estimate the request's response
            //   time, and discard it if we are certain that the SLO is violated?).
            //   Requeuing attempts can follow an exponential (or fibonacci?) backoff.
            // - When the number of invocation requests is too large, evictions should
            //   probably be dense too, and possibly sub-optimal too. We can think of
            //   this as the equivalent of thrashing in caching, and I don't think
            //   there is any straightforward way to avoid it.

            // If we do have enough memory to spawn a new Worker, do that first.
            let new_worker_id =
                if self.memory.inuse + self.memory.reclaimed + mem_requested <= self.memory.total {
                    // Reaching here means that no Idle Worker is available, but there is free
                    // memory to spawn a new one:
                    // - first try to find a snapshotted Sandbox to pass to the new Worker;
                    // - if none is available, the new Worker will need to create a new Sandbox.
                    let sandbox = self.pick_orphan_snapshot(function_id);
                    Some(self.spawn_new_worker(function_id, sandbox)?)
                } else {
                    None
                };

            // If we have reached the configured memory eviction threshold
            // (whether we did just spawn a new Worker or not), trigger the
            // eviction policy to eventually get below the threshold again.
            if self.memory.inuse >= self.memory.eviction_thres_hi {
                self.trigger_eviction_policy().await;
            }

            // Finally, if we did spawn a new Worker, get its ID to continue
            // setting it up. Otherwise, deny and return to the event loop.
            match new_worker_id {
                Some(worker_id) => Ok(worker_id),
                None => {
                    warn!(
                        memory.stats = %self.memory,
                        memory.requested = %mem_requested,
                        "Cannot spawn new Worker while operating at memory capacity",
                    );
                    trace!(
                        active = ?self.store.iter_active().map(|(f, _)| f).counts(),
                        idle = ?self.store.iter_idle().map(|(f, _)| f).counts(),
                        dying = ?self.store.iter_dying().map(|(f, _)| f).counts(),
                        "dropping request"
                    );
                    return deny_oom(dreq); // TODO(ckatsak): Ideally requeue?
                }
            }
        }?;

        // Reaching here means that the invocation request is assigned to a Worker.

        debug_assert!(
            !self.pending.op_by_worker.contains_key(&worker_id),
            "Idle Workers should never have pending control operations"
        );

        // Stop any keep-alive timer associated with the Worker
        self.keepalive.stop_timer(worker_id);

        // The Worker -- be it existing or new -- should now be Active
        self.store
            .insert_active_worker(worker_id, function_id)
            .map_err(|err| Error::Metadata(Box::new(err)))?; // TODO: error handling + rollback?

        // Allocate a new CPU for the workload to be pinned on
        let cpuset = self.cpuset.alloc(worker_id);
        // TODO?

        let worker = self
            .worker_handles
            .get_mut(&worker_id)
            .expect("WorkerHandle must be present by now");

        // Clear any deadline associated with the Worker
        let old_deadline = self.worker_deadlines.remove(&worker.id());

        // Increase the number of invocations assigned to a Worker. TODO: probably after reply?
        self.store
            .function_stats(function_id)
            .dispatches
            .fetch_add(1, Ordering::Relaxed);

        // A new WorkerRef is sent to AdmissionController as a response
        if let Err(dresp) = dreq.reply(DispatchResponse::dispatch(worker.new_ref(), cpuset)) {
            self.cleanup_failed_dispatch(dresp, old_deadline).await?;
        }
        Ok(())
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // New Worker
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Finds and returns an appropriate [`WorkerId`] for the incoming [`DispatchRequest`]
    /// among `SandboxPool`'s stored _Idle_ [`Worker`]s (stored in [`FunctionMetadata`]).
    ///
    /// - Currently, we pick the [`Worker`] that was set to be _Idle_ **most** recently. This
    ///   favors system's load (since older _Idle_ [`Worker`]s are more likely to reach their
    ///   deadline as long as there is not any sort of invocations peak), at the cost of Function
    ///   execution times (since we are utilizing the most recently spawned [`Worker`]s, who might
    ///   be "colder" compared to the older ones)?
    /// - An alternative would be to pick the [`Worker`] that has been _Idle_ for the **longest**
    ///   time, which could possibly favor Function execution times, at the cost of comparatively
    ///   more infrequent decommissions of _Idle_ [`Worker`]s.
    ///
    /// # Returns
    ///
    /// - A [`WorkerId`] that corresponds to an _Idle_ [`Worker`], who can now be reused to
    ///   handle a [`Request`], wrapped in `Some`.
    /// - `None` when no such eligible _Idle_ [`Worker`] is currently available.
    ///
    /// [`DispatchRequest`]: crate::admission::DispatchRequest
    /// [`FunctionMetadata`]: crate::sbpool::metadata::FunctionMetadata
    /// [`Request`]: crate::Request
    /// [`Worker`]: crate::worker::Worker
    /// [`WorkerId`]: crate::worker::WorkerId
    #[inline]
    fn pick_idle_worker(&mut self, function_id: &FunctionId) -> Option<WorkerId> {
        self.store
            .iter_idle()
            .filter(|(fid, _)| fid == function_id)
            .map(|(_, wid)| {
                (
                    wid,
                    self.worker_deadlines
                        .get(&wid)
                        .expect("all Idle Workers should have an associated deadline"),
                )
            })
            .max_by(|&(_, &xd), &(_, &yd)| xd.cmp(&yd))
            .map(|(wid, _)| wid)
    }

    /// Finds an appropriate [`Runtime::Sandbox`] for the incoming [`DispatchRequest`] in
    /// `SandboxPool`'s stored [`FunctionMetadata`], removes it (taking ownership) and returns it.
    ///
    /// Currently, we pick the (snapshotted) [`Runtime::Sandbox`] that has been restored the most
    /// times.
    ///
    /// # Notes
    ///
    /// - When returned, the [`Runtime::Sandbox`] has been removed from `SandboxPool`'s list of
    ///   [`Runtime::Sandbox`]es that are currently **not** assigned to any [`Worker`].
    ///
    /// [`Runtime::Sandbox`]: crate::worker::Runtime::Sandbox
    #[inline]
    fn pick_orphan_snapshot(&mut self, function_id: &FunctionId) -> Option<Runtime::Sandbox> {
        let most_used_snapshot = self
            .snapshots
            .get(function_id)?
            .iter()
            .enumerate()
            .max_by(|(_, sbx), (_, sby)| sbx.stats().restorations.cmp(&sby.stats().restorations))
            .map(|(idx, _sandbox)| idx)?;
        self.snapshots
            .get_mut(function_id)
            .map(|sandboxes| sandboxes.remove(most_used_snapshot))
    }

    /// This is an internal helper method; you should probably use
    /// [`Self::spawn_new_worker`] instead.
    ///
    /// This helper:
    /// - spawns a new [`Worker`];
    /// - registers its [`WorkerHandle`] and [`SandboxId`]->[`WorkerId`]
    ///   mapping;
    /// - returns the [`WorkerId`] of the newly spawned [`Worker`].
    ///
    /// # Errors
    ///
    /// [`Error::WorkerCreation`] on failure to spawn a new [`Worker`].
    ///
    /// # Panics
    ///
    /// On [`WorkerId`] collision, which should never really occur, unless
    /// there is some bookkeeping bug.
    #[inline]
    fn _spawn_untracked_worker(
        &mut self,
        function_id: &FunctionId,
        sandbox: Option<Runtime::Sandbox>,
        mem_accounting: WorkerMemAccounting,
    ) -> Result<WorkerId> {
        let maybe_sandbox_id = sandbox.as_ref().map(|s| SandboxId::from(s.id()));
        let func = self.store.registered_function(function_id);

        let new_worker = match Worker::<Runtime, _, _, Issuer, _>::spawn(
            func.info(),
            &self.worker_aux.config,
            self.worker_aux.channels.clone(),
            sandbox,
            mem_accounting,
        ) {
            Ok(wh) => wh,
            Err(SpawnError { err, sandbox }) => {
                error!(error = ?err, %function_id, ?sandbox, "Failed to spawn Worker: {err:#}");
                if let Some(sb) = sandbox {
                    debug_assert!(sb.has_snapshot(), "returned Sandbox should be snapshotted");
                    self.snapshots
                        .entry(function_id.clone())
                        .or_default()
                        .push(sb);
                }
                return Err(Error::WorkerCreation(err));
            }
        };

        let new_worker_id = new_worker.id();
        if let Some(_existing_worker) = self.worker_handles.insert(new_worker_id, new_worker) {
            todo!("TODO: collision: {_existing_worker:?}")
        }

        // If Sandbox ID is already known, update the Sandbox->Worker reverse mapping
        if let Some(sandbox_id) = maybe_sandbox_id
            && let Some(_existing_wid) = self
                .worker_by_sandbox
                .insert(sandbox_id.clone(), new_worker_id)
        {
            error!(
                sandbox.id = %sandbox_id,
                worker.id.new = %new_worker_id,
                worker.id.existing = %_existing_wid,
                "BUG: reverse Sandbox ID <-> Worker ID mapping"
            );
            panic!("BUG: map collision: {sandbox_id} -> ({new_worker_id} / {_existing_wid})")
        }

        Ok(new_worker_id)
    }

    /// Spawn a new [`Worker`], either using an existing [`Sandbox`] snapshot
    /// or not, and update internal state:
    /// - new [`WorkerHandle`]
    /// - new [`SandboxId`]->[`WorkerId`] mapping
    /// - memory usage accounting
    /// - Function stats
    ///
    /// Returns the unique ID of the new [`Worker`].
    ///
    /// # Errors / Panics
    ///
    /// Same as [`Self::_spawn_untracked_worker`].
    #[inline]
    fn spawn_new_worker(
        &mut self,
        function_id: &FunctionId,
        sandbox: Option<Runtime::Sandbox>,
    ) -> Result<WorkerId> {
        let using_snapshot = sandbox.is_some();
        let worker_id =
            self._spawn_untracked_worker(function_id, sandbox, WorkerMemAccounting::InUse)?;

        self.memory.inuse += self.store.registered_function(function_id).info().memory();

        let stats = self.store.function_stats(function_id);
        stats.workers_spawned.fetch_add(1, Ordering::Relaxed);
        if using_snapshot {
            stats.snapshots_restored.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.sandboxes_created.fetch_add(1, Ordering::Relaxed);
        }

        Ok(worker_id)
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Low-level FSM helpers
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// "Low-level" FSM helper: transits [`Worker`] state: _Idle_ -> _Dying_.
    ///
    /// Performs only Pool-side bookkeeping:
    /// - updates [`Worker`] state in metadata store,
    /// - aborts [`Worker`]'s' associated timer and stops tracking its deadline,
    /// - reclassifies [`Worker`]'s memory from `inuse` to `reclaimed`.
    ///
    /// Note that it does __not__ contact the [`Worker`].
    //
    // This helper must only be called for a [`Worker`] that is currently
    // tracked as _Idle_, which implies an associated keepalive timer.
    //
    // # Safety
    //
    // - The provided [`WorkerId`] must be present as key in [`KeepAlive::timers`].
    #[inline]
    fn _idle_worker_to_dying(
        &mut self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<()> {
        // Change the Worker from Idle to Dying
        self.store
            .worker_idle_to_dying(worker_id, function_id)
            .map_err(|err| Error::Metadata(Box::new(err)))?;

        // Let admission know of Function's new Worker cap change
        self.emit_admission_event(PoolEvent::WorkerCapAvailable {
            function_id: function_id.clone(),
        });

        // Clean up the associated keep-alive timer & deadline
        //unsafe { self.keepalive.stop_timer_unchecked(worker_id) };
        // FIXME: maybe use that_vv  instead of this_^^ and avoid `unsafe` both
        // here and in method's signature? Do all callsites check `self.timers`? XXX
        self.keepalive.stop_timer(worker_id);
        let _old_deadline = self.worker_deadlines.remove(&worker_id);
        debug_assert!(_old_deadline.is_some(), "Idle Workers should have deadline"); // FIXME?

        // Reclassify its memory as "being reclaimed" rather than "in use"
        let worker = self
            .worker_handles
            .get_mut(&worker_id)
            .expect("Worker exists, since state transition just succeeded");
        let worker_mem = self.store.function_memory(worker.function_id());
        self.memory.reclaim(worker_mem);
        worker.set_memory_accounting(WorkerMemAccounting::Reclaimed);

        Ok(())
    }

    /// "Low-level" FSM helper: transits [`Worker`] state: _Active_ -> _Dying_.
    ///
    /// Performs only Pool-side bookkeeping:
    /// - updates [`Worker`] state in metadata store,
    /// - removes any stale deadline defensively,
    /// - reclassifies the [`Worker`]'s memory from `inuse` to `reclaimed`.
    ///
    /// Note that it does __not__ contact the [`Worker`].
    #[inline]
    fn _active_worker_to_dying(
        &mut self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<()> {
        // Change the Worker from Active to Dying
        self.store
            .worker_active_to_dying(worker_id, function_id)
            .map_err(|err| Error::Metadata(Box::new(err)))?;

        // Let admission know of Function's new Worker cap change
        self.emit_admission_event(PoolEvent::WorkerCapAvailable {
            function_id: function_id.clone(),
        });

        // Remove any deadline assigned to the Worker (defensive; there shouldn't be any)
        let _old_dl = self.worker_deadlines.remove(&worker_id);
        debug_assert!(_old_dl.is_none(), "Active Workers should not have deadline");

        // Reclassify its memory as "being reclaimed" rather than "in use"
        let worker_mem = self.store.function_memory(function_id);
        self.memory.reclaim(worker_mem);
        self.worker_handles
            .get_mut(&worker_id)
            .expect("Worker exists, since state transition just succeeded")
            .set_memory_accounting(WorkerMemAccounting::Reclaimed);

        Ok(())
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Keep-alive
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Builds a [`PoolContext`] and passes it, together with the `KeepAlivePolicy`, to `f`.
    ///
    /// Policy decisions need simultaneous access to disjoint parts of [`SandboxPool`]: the
    /// metadata store, keep-alive deadlines, the eviction-victim buffer, and the policy object
    /// itself.
    /// Constructing [`PoolContext`] directly at call sites often needs to borrow
    /// `self.keepalive.policy` and `self.keepalive.eviction_victims` at the same time,
    /// which makes otherwise valid field-disjoint borrows harder for the compiler to see.
    ///
    /// This is only a borrow-shaping helper: it destructures `self` first, so the borrow checker
    /// can track those fields independently.
    /// The closure is called exactly once, synchronously, and must not retain the context beyond
    /// the call.
    /// `FnOnce` helps with this: each use performs a single policy operation, and `FnOnce` is the
    /// least restrictive bound. (It still accepts closures that also implement `FnMut` or `Fn`.)
    #[inline]
    fn with_policy_context<R>(
        &mut self,
        f: impl FnOnce(&mut KeepAlivePolicy, PoolContext<'_, Store, Runtime::FunctionInfo>) -> R,
    ) -> R {
        let Self {
            store,
            worker_deadlines,
            keepalive:
                KeepAlive {
                    policy,
                    eviction_victims,
                    ..
                },
            ..
        } = self;

        let pctx = PoolContext {
            store: store.borrow_arc(),
            worker_deadlines,
            victims: eviction_victims,
            _function_info: PhantomData,
        };

        f(policy, pctx)
    }

    /// By calling this function, we:
    /// - change the state of the specified _Idle_ [`Worker`] to _Dying_;
    /// - abort its associated timer and stop tracking its deadline.
    /// - mark its memory as "being reclaimed";
    /// - notify the [`Worker`] to shutdown;
    ///
    /// # Panics
    ///
    /// If the provided [`WorkerId`] does not refer to an existing [`Worker`].
    ///
    /// # Notes
    ///
    /// If the specified [`Worker`] is not currently associated with a keep-alive timer (i.e.,
    /// is not _Idle_), this method returns `Ok(())` early (without doing any of the above).
    #[instrument(level = Level::TRACE, skip(self))]
    async fn decommission_idle_worker(
        &mut self,
        worker_id: WorkerId,
        function_id: impl Into<Option<&FunctionId>> + Debug, // TODO(ckatsak): needlessly generic?
    ) -> Result<()> {
        // NOTE(ckatsak): Before actually doing anything, verify that the timeout is still valid by
        // checking whether we are still keeping track of the related timer. If `worker_id` is not
        // contained there, it should normally mean that the timer has been aborted and the Idle
        // Worker is now back in use. This may occur because:
        // - `JoinHandle::abort()` does not happen instantly (see tokio docs);
        // - We may actually have aborted it ourselves after the `Sleep` Future has completed,
        //   but we did not have the chance process it because we (the Pool) were busy handling
        //   some other event.
        if !self.keepalive.timers.contains_key(&worker_id) {
            let stale_deadline_discarded = self.worker_deadlines.remove(&worker_id);
            warn!(
                %worker_id, ?function_id, ?stale_deadline_discarded,
                "Ignoring spurious keepalive timeout"
            );
            return Ok(());
        }
        // Valid keepalive timers refer to Idle Workers.

        let function_id = match function_id.into() {
            Some(fid) => Cow::Borrowed(fid),
            None => Cow::Owned(
                self.worker_handles
                    .get(&worker_id)
                    .expect("invalid Worker ID")
                    .function_id()
                    .clone(),
            ),
        };

        // Manipulate Worker state.
        // SAFETY:
        // - Just checked that a timer exists for the given WorkerId.
        // - Pool is the sole accessor of `KeepAlive::timers`.
        self._idle_worker_to_dying(worker_id, function_id.as_ref())?;

        // Shut the Worker down.
        self.worker_handles
            .get(&worker_id)
            .expect("Worker exists, since state transition just succeeded")
            .shutdown()
            .await
            .map_err(Error::Worker)?; // TODO: error handling?

        Ok(())
    }

    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    async fn trigger_eviction_policy(&mut self) {
        self.keepalive.eviction_victims.clear();

        let mem_to_reclaim = self.memory.inuse - self.memory.eviction_thres_lo;
        self.with_policy_context(|policy, pctx| policy.evict(pctx, mem_to_reclaim));
        debug!(num_worker_victims = self.keepalive.eviction_victims.len());

        let victims = ::std::mem::take(&mut self.keepalive.eviction_victims);
        for (fid, wid) in &victims {
            debug!(victim.function_id = ?fid, victim.worker_id = ?wid, "Evicting...");
            if let Err(err) = self.decommission_idle_worker(*wid, Some(fid)).await {
                error!(
                    error = ?err, victim.function_id = ?fid, victim.worker_id = ?wid,
                    "Failure during Worker eviction: {err:#}",
                );
            }
        }
        let _ = ::std::mem::replace(&mut self.keepalive.eviction_victims, victims);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Worker state transitions handling
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[instrument(level = Level::TRACE, skip(self))]
    async fn handle_worker_status(&mut self, msg: worker::OutboundMessage) -> Result<()> {
        use worker::OutboundMessage as Msg;
        match msg {
            Msg::SandboxId {
                worker_id,
                sandbox_id,
            } => self.update_sandbox_id(worker_id, sandbox_id),
            Msg::NeedWork {
                worker_id,
                function_id,
            } => self.on_need_work(worker_id, function_id).await,
            Msg::ReapMe {
                worker_id,
                function_id,
            } => self.reap_worker(worker_id, function_id).await,
            Msg::SandboxPreparation(resp) => self.finalize_sandbox_preparation(resp).await,
            Msg::SnapshotCreation(resp) => self.finalize_snapshotting(resp).await,
        }
    }

    fn update_sandbox_id(&mut self, worker_id: WorkerId, sandbox_id: SandboxId) -> Result<()> {
        let worker = self
            .worker_handles
            .get_mut(&worker_id)
            .expect("Worker exists; just sent message");
        debug_assert!(worker.sandbox_id().is_none(), "only sent upon creation");
        worker.set_sandbox_id(sandbox_id.clone());

        if let Some(_existing_wid) = self.worker_by_sandbox.insert(sandbox_id.clone(), worker_id) {
            #[cold]
            #[inline(never)]
            fn _collision_panic_wid_by_sb(wid: WorkerId, sid: SandboxId, old_wid: WorkerId) -> ! {
                error!(
                    sandbox.id = %sid,
                    worker.id.new = %wid,
                    worker.id.existing = %old_wid,
                    "BUG: reverse Sandbox ID <-> Worker ID mapping"
                );
                panic!("BUG: sb map collision: {sid} -> ({wid} / {old_wid})") // TODO?
            }
            _collision_panic_wid_by_sb(worker_id, sandbox_id, _existing_wid)
        }

        Ok(())
    }

    /// Consults [`Self::keepalive`] and calls [`Self::deactivate_worker`].
    #[inline]
    async fn deactivate_worker_per_keepalive(
        &mut self,
        worker_id: WorkerId,
        function_id: &FunctionId,
        ts_start: Instant,
    ) -> Result<()> {
        // Consult the KeepAlivePolicy for a new deadline
        let maybe_new_deadline = self
            .with_policy_context(|policy, pctx| policy.assign(pctx, function_id))
            .map(|idle_duration| ts_start + idle_duration);

        self.deactivate_worker(worker_id, function_id, maybe_new_deadline)
            .await
    }

    /// When provided with a new deadline for the given [`WorkerId`] (serving the given
    /// [`FunctionId`]), it marks the [`Worker`] as _Idle_ and spawns a timer that goes
    /// off when this deadline elapses.
    /// If no deadline is provided, it marks the [`Worker`] as _Dying_ and calls its
    /// [`WorkerHandle::shutdown`].
    ///
    /// # Notes
    ///
    /// - This method may fail (or even panic, but probably fails before panicking) if
    ///   called with a [`WorkerId`] that refers to a [`Worker`] that is not _Active_.
    #[instrument(level = Level::TRACE, skip_all)]
    async fn deactivate_worker(
        &mut self,
        worker_id: WorkerId,
        function_id: &FunctionId,
        maybe_new_deadline: Option<Instant>,
    ) -> Result<()> {
        if let Some(new_deadline) = maybe_new_deadline {
            // Change the Worker from Active to Idle
            self.store
                .worker_active_to_idle(worker_id, function_id)
                .map_err(|err| Error::Metadata(Box::new(err)))?; // TODO: error handling?

            // Set Worker's new deadline to when the timer goes off
            let _old_deadline = self.worker_deadlines.insert(worker_id, new_deadline);
            debug_assert!(_old_deadline.is_none(), "deadline should have been removed");
            // Spawn the keep-alive timer task
            self.keepalive.spawn_timer(worker_id, new_deadline).await;

            // Notify AdmissionController of a newly _Idle_ Worker
            self.emit_admission_event(PoolEvent::WorkerIdle {
                function_id: function_id.clone(),
            });
        } else {
            self._active_worker_to_dying(worker_id, function_id)?;

            // Shut the Worker down
            self.worker_handles
                .get_mut(&worker_id)
                .expect("Worker exists, since state transition just succeeded")
                .shutdown()
                .await
                .map_err(Error::Worker)?; // TODO: error handling?
        }
        Ok(())
    }

    #[instrument(level = Level::TRACE, skip_all)]
    async fn on_need_work(&mut self, worker_id: WorkerId, function_id: FunctionId) -> Result<()> {
        let now = Instant::now();

        //  vv  NOTE  vv
        // `NeedWork` actually sounds like the perfect time to release any CPU resources associated
        // with the invocation: the Worker must have paused its sandbox, and will be allocated new
        // CPU resources once a new Request is dispatched to that Worker. What's the problem then?
        // (1) Workers that shut down without ever sending a `NeedWork` (e.g., due to failures):
        //     when should we release _their_ CPU resources? Failed Workers might skip `NeedWork`
        //     and only send `ReapMe` (unless `Worker::run()` is flawed). Perhaps we could
        //     _attempt_ to also release them on `ReapMe`, but without the hard requirement that
        //     they are still expected to be allocated (this requirement exists on `NeedWork`).
        // (2) This does not apply for all resources (e.g., memory remains allocated until the
        //     sandbox is actually shut down or destroyed). In other words, _Idle_ Workers still
        //     occupy memory. Perhaps we could group all resources by their "scope"/"lifetime"
        //     (e.g., invocation, sandbox, ...?) and release each group at its own time? TODO
        //  ^^  NOTE  ^^

        if let Err(err) = self.cpuset.release(worker_id) {
            error!(error = ?err, ?worker_id, ?function_id, "Parking Active Worker: {err:#}");
            // Reaching here having received `NeedWork`, there _must_ be some cpuset allocated,
            // otherwise the Request should not have even been dispatched; i.e., if no
            // cpuset had been allocated, our resource tracking logic is probably wrong!
            return Err(Error::CpuSet(err));
        }

        // Only continue parking the Worker if there are no pending control ops either.
        if let Some(ctrl_op) = self.pending.op_by_worker.get(&worker_id) {
            debug!(
                ?worker_id,
                ?function_id,
                ?ctrl_op,
                "Postponing Worker deactivation due to pending control op",
            );
            return Ok(());
        }

        self.deactivate_worker_per_keepalive(worker_id, &function_id, now)
            .await
    }

    #[instrument(level = Level::DEBUG, skip(self))]
    async fn reap_worker(&mut self, worker_id: WorkerId, function_id: FunctionId) -> Result<()> {
        /// Pool-side metadata state from which this Worker is being reaped.
        ///
        /// This is not the Worker-side exit status; that comes later from
        /// `WorkerExit`. We keep this local state only for reap-time Pool cleanup that
        /// depends on how far the Worker had been registered in Pool metadata.
        enum ReapState {
            /// The Worker was still tracked as _Active_. This usually means it failed before
            /// sending `NeedWork`, so an invocation/control-op cpuset may still be assigned.
            Active,
            /// The Worker had already been moved to _Dying_. It should normally
            /// have released (invocation-scoped) CPU resources earlier, but it
            /// may still own a cpuset if Pool moved it _Active_->_Dying_ while an
            /// invocation was in flight and that invocation failed before `NeedWork`.
            Dying,
            /// The Worker was present in `worker_handles` but not yet in Store. This covers
            /// async initialization failure, after `Worker::spawn()` returned but before
            /// Pool completed metadata insertion. Such Workers should not have a cpuset yet.
            Starting,
        }

        let (worker, reap_state) = if self
            .store
            .remove_dying_worker(worker_id, &function_id)
            .map_err(|err| Error::Metadata(Box::new(err)))?
        {
            trace!("Reaping _Dying_ Worker");
            (
                self.worker_handles
                    .remove(&worker_id)
                    .expect("just checked that _Dying_ Worker exists"),
                ReapState::Dying,
            )
        } else if self
            .store
            .remove_active_worker(worker_id, &function_id)
            .map_err(|err| Error::Metadata(Box::new(err)))?
        {
            debug!("Reaping _Active_ Worker");
            // Let admission know of Function's new Worker cap change
            self.emit_admission_event(PoolEvent::WorkerCapAvailable {
                function_id: function_id.clone(),
            });
            (
                self.worker_handles
                    .remove(&worker_id)
                    .expect("just checked that _Active_ Worker exists"),
                ReapState::Active,
            )
        } else if self
            .store
            .find_idle_worker(worker_id)
            .map_err(|err| Error::Metadata(Box::new(err)))?
            .is_some()
        {
            warn!(%worker_id, %function_id, "Ignoring (spurious?) ReapMe from _Idle_ Worker");
            return Ok(());
        } else if let Some(worker) = self.worker_handles.remove(&worker_id) {
            warn!(
                %worker_id, %function_id,
                "Reaping untracked Worker; treating this as initialization failure"
            );
            (worker, ReapState::Starting)
        } else {
            warn!(%worker_id, %function_id, "Ignoring ReapMe from completely _untracked_ Worker");
            return Ok(());
        };

        // Update Worker deadlines (defensive; there should not be any)
        let _dl = self.worker_deadlines.remove(&worker_id);
        debug_assert!(_dl.is_none(), "non-Idle Worker should not have deadline");

        // Update `worker_by_sandbox`, since the Worker is done and any associated Sandbox
        // must no longer be considered owned by it.  The Sandbox may have been destroyed,
        // or be about to be handed back to us (the Pool) through `worker.reap()`.
        if let Some(sandbox_id) = worker.sandbox_id()
            && let Some(_wid) = self.worker_by_sandbox.remove(sandbox_id)
        {
            debug_assert_eq!(_wid, worker_id);
        }

        // Defensively release any cpuset (reaped _Active_ and _Dying_ Workers can still have).
        if self.cpuset.release(worker_id).is_ok() {
            debug_assert!(
                matches!(reap_state, ReapState::Active | ReapState::Dying),
                "Workers failed during initialization should not have cpuset assigned yet"
            );
            debug!(%worker_id, %function_id, "Released stale cpuset while reaping Worker");
        }

        // Cleanup or complete any pending control operations associated with the Worker's Sandbox.
        let maybe_destr_op_ctx = self
            .finalize_control_ops(worker.id(), worker.sandbox_id())
            .await;

        let worker_mem = self.store.function_memory(&function_id);
        let mem_accounting = worker.memory_accounting();

        let worker_exit_status = match worker.reap().await {
            Ok(status) => status,
            Err(err) => {
                error!(error = ?err, %worker_id, %function_id, "Failed to join Worker: {err:#}");
                // Reply to waiters, if any
                if let Some(op_ctx) = maybe_destr_op_ctx {
                    let msg = format!("failed to join Worker {worker_id}: {err:#}");
                    op_ctx.reply_destroy_internal_failure(msg);
                }
                return Err(Error::WorkerJoin(worker_id));
            }
        };
        match worker_exit_status {
            WorkerExit {
                result: Ok(()),
                sandbox: SandboxExit::Reusable(sandbox),
            } => {
                // The Worker exited successfully and handed back reusable snapshot state.
                // This corresponds to final `SandboxState::Snapshot`: either a normal shutdown
                // preserved a snapshotted Sandbox for reuse, or the Worker internally recovered
                // from a failure by quiescing/unloading the Sandbox back to snapshot-only state.
                debug_assert!(
                    sandbox.has_snapshot(),
                    "Sandbox handed off by reaped Worker should be snapshotted"
                );

                // Stash the Sandbox for reuse
                self.snapshots.entry(function_id).or_default().push(sandbox);

                // Memory accounting
                self.reap_mem_account(mem_accounting, worker_mem);

                // Reply to waiters, if any
                if let Some(op_ctx) = maybe_destr_op_ctx {
                    op_ctx.reply_destroy_success(true);
                }
            }

            WorkerExit {
                result: Ok(()),
                sandbox: SandboxExit::None,
            } => {
                // The Worker exited successfully without handing any Sandbox back; hence, its
                // final `SandboxState` was `Nonexistent`.
                // In the current codebase, the only valid cases should be:
                // (1) The Worker gracefully handled an internal failure before the Sandbox
                //     was ever created (i.e., failed with `worker::Error::PrepareSandbox`
                //     while on`SandboxState::Nonexistent`) (be it after an incoming
                //     invocation or a `PrepareSandbox` ctrl request).
                // (2) The Worker owned a non-snapshotted Sandbox, and destroyed it successfully
                //     before exit. This can occur in many paths (always non-snapshotted); e.g.:
                //     - keep-alive shutdown of an _Idle_ Worker,
                //     - `DestroySandbox` against a previously live Worker,
                //     - graceful failure handling after an invocation/forwarding error.
                // (3) The Worker hit `worker::Error::PauseSandbox`, treated the Sandbox as
                //     terminally non-reusable, and successfully destroyed it. (This may have
                //     consumed a (still) running Sandbox which might also had snapshot backing.)
                // (4) The Worker handled `DestroySandbox(remove_persisted_snapshot = true)`
                //     and successfully destroyed a snapshotted Sandbox before exiting.
                // (5) A dedicated cleanup-only Worker destroyed an orphan snapshot. Pool may
                //     spawn a fresh Worker around an already snapshotted orphan Sandbox solely to
                //     service `DestroySandbox(remove_persisted_snapshot = true)` for a snapshot
                //     that is no longer owned by any live Worker. That Worker starts as _Dying_
                //     and in `SandboxState::Snapshot`, receives `ControlMessage::DestroySandbox`,
                //     destroys the snapshot, and exits in `SandboxState::Nonexistent`.
                trace!("Reaped Worker returned no Sandbox");

                // In all above cases, no Sandbox resources remain owned by
                // the Worker, so apply normal reap-time memory accounting.
                self.reap_mem_account(mem_accounting, worker_mem);

                // Reply to waiters, if any
                if let Some(op_ctx) = maybe_destr_op_ctx {
                    op_ctx.reply_destroy_success(false);
                }
            }

            WorkerExit {
                result: Ok(()),
                sandbox: SandboxExit::Trashed(sandbox),
            } => {
                // `SandboxExit::Trashed` is reserved for failed Worker exits. A successful
                // Worker exit must have either returned reusable snapshot state or no Sandbox at
                // all. Reaching this means `Worker::run()` produced an inconsistent `WorkerExit`.
                const _MSG: &str = "Workers can never return successfully with a trashed sandbox";
                error!(%worker_id, %function_id, ?sandbox, _MSG);
                unreachable!("{_MSG}");
            }

            WorkerExit {
                result: Err(err),
                sandbox: SandboxExit::Reusable(sandbox),
            } => {
                // The Worker failed but handed back reusable snapshot state.
                // This corresponds to final `SandboxState::Snapshot` on an error path. In the
                // current codebase, this should only happen when `runtime.init()` fails after
                // Pool spawned the Worker assigned with a snapshot: Worker never entered its
                // main loop, the snapshot was never rehydrated, and hence can be returned to
                // Pool as an orphan snapshot, completely untouched.
                debug_assert!(
                    sandbox.has_snapshot(),
                    "Sandbox handed off by reaped Worker should be snapshotted"
                );
                warn!(
                    error = ?err, %worker_id, %function_id, ?sandbox,
                    "Reaped failed Worker with reusable Sandbox: {err:#}"
                );

                // Stash the Sandbox for reuse
                self.snapshots.entry(function_id).or_default().push(sandbox);

                // Pool owns the Sandbox again, and no live Worker continues to own
                // its Function memory, so apply normal reap-time memory accounting.
                self.reap_mem_account(mem_accounting, worker_mem);

                // Reply to waiters, if any
                if let Some(op_ctx) = maybe_destr_op_ctx {
                    op_ctx.reply_destroy_worker_failure(&err);
                }
            }

            WorkerExit {
                result: Err(err),
                sandbox: SandboxExit::Trashed(sandbox),
            } => {
                // The Worker failed and handed back a Sandbox that Pool must NOT reuse.
                // This normally corresponds to `SandboxState::Trashed`: Worker still owned
                // a `Sandbox` handle, but cleanup could not prove the Sandbox reusable or
                // fully destroyed. (`SandboxState::{Paused|Running}` are nowadays converted
                // to `SandboxExit::Trashed` only as a defensive fallback.)
                // Just track it as "trashed"/"leaked" for now. FIXME?
                error!(
                    error = ?err, %worker_id, %function_id, ?sandbox,
                    previous.total.trashed = %self.trashed_sandboxes.len(),
                    "Reaped failed Worker; +1 trashed Sandbox; failure: {err:#}"
                );
                self.trashed_sandboxes.push(sandbox);

                // We do NOT apply normal reap-time memory accounting in this case.
                // `SandboxExit::Trashed` means Pool is keeping this Sandbox in
                // `self.trashed_sandboxes` because it cannot safely reuse or prove cleanup of the
                // underlying runtime resources. Keep the Worker's Function memory charged exactly
                // as it was, so admission logic continues to treat that capacity as unavailable.

                // Reply to waiters, if any
                if let Some(op_ctx) = maybe_destr_op_ctx {
                    op_ctx.reply_destroy_worker_failure(&err);
                }
            }

            WorkerExit {
                result: Err(err),
                sandbox: SandboxExit::None,
            } => {
                // The Worker failed without handing any Sandbox back.
                // Nowadays, there can be two root causes for this:
                // (1) Worker w/o assigned Sandbox failed at `Runtime::init()`.
                // (2) Worker successfully destroyed its Sandbox (`Runtime::destroy_sandbox`),
                //     but then failed to deallocate its networking resource.
                warn!(error = ?err, "Reaped failed Worker with no Sandbox: {err:#}");

                if let worker::Error::Init { .. } = err {
                    // In case (1) above, Pool _may_ already have charged this Worker into
                    // memory accounting.  In this case, this should be undone here.
                    self.reap_mem_account(mem_accounting, worker_mem);
                } else {
                    // Otherwise, keep memory charged, as `Err + None` should mean cleanup/destroy
                    // consumed the Sandbox but did not prove runtime resources are gone.
                }

                // Reply to waiters, if any
                if let Some(op_ctx) = maybe_destr_op_ctx {
                    op_ctx.reply_destroy_worker_failure(&err);
                }
            }
        }

        Ok(())
    }

    /// Apply normal reap-time memory accounting for a [`Worker`] whose sandbox
    /// resources are no longer retained by Pool.
    ///
    /// This must be called only for outcomes where the Worker's sandbox is either:
    /// - absent, or
    /// - returned as reusable snapshot state.
    ///
    /// # Note
    ///
    /// This must __not__ be called for [`SandboxExit::Trashed`].
    /// Trashed sandboxes are still tracked by `self.trashed_sandboxes`, and
    /// Pool conservatively keeps their memory charged so future admission
    /// decisions do not reuse capacity that may still be occupied by leaked
    /// runtime resources.
    #[inline]
    fn _reap_mem_account(&mut self, mem_accounting: WorkerMemAccounting, worker_mem: ByteUnit) {
        //let worker_mem = self.store.function_memory(function_id);
        match mem_accounting {
            WorkerMemAccounting::InUse => self.memory.inuse -= worker_mem,
            WorkerMemAccounting::Reclaimed => self.memory.reclaimed -= worker_mem,
            WorkerMemAccounting::Uncharged => {}
        }
    }

    /// Wrapper over [`Self::_reap_mem_account`], which also emits a
    /// [`PoolEvent::MemoryCapAvailable`] when applicable.
    #[inline]
    fn reap_mem_account(&mut self, mem_accounting: WorkerMemAccounting, worker_mem: ByteUnit) {
        self._reap_mem_account(mem_accounting, worker_mem);
        if mem_accounting != WorkerMemAccounting::Uncharged {
            self.emit_admission_event(PoolEvent::MemoryCapAvailable);
        }
    }

    /// Sweeps through [`Self::worker_handles`] and attempts to reap all finished.
    ///
    /// - This is not a general reap accelerator; it is a fallback recovery mechanism
    ///   for Workers that Pool still believes are Active, but whose tasks have already
    ///   finished.
    /// - This targets _Active_ Workers that may have abrupty exited (e.g., panicked)
    ///   without signaling their death through a [`worker::OutboundMessage::ReapMe`].
    ///   It really is a last resort to re-acquire any leaked resources.
    /// - Maybe it should be called periodically (e.g., every: 1s? 5s? 10s?) by the
    ///   Pool itself, but probably with low priority. Periodic sweeping would interfere
    ///   with the happy path of `ReapMe` handling: one of them would successfully reap
    ///   the finished Worker, while the other would "ignore .. from _untracked_ Worker".
    // TODO(ckatsak): Call periodically within a new event loop branch?
    #[instrument(level = Level::TRACE, skip(self))]
    async fn reap_finished_workers(&mut self) {
        let finished = self
            .store
            .iter_active()
            //.chain(self.store.iter_dying())
            .filter_map(|(function_id, worker_id)| {
                self.worker_handles
                    .get(&worker_id)
                    .filter(|wh| wh.is_finished())
                    .map(|_| (worker_id, function_id))
            })
            .collect::<Vec<_>>();
        warn!(
            ?finished,
            "Found {} finished _Active_ Workers; trying to reap them...",
            finished.len()
        );

        for (worker_id, function_id) in finished {
            if let Err(err) = self.reap_worker(worker_id, function_id.clone()).await {
                error!(
                    ?worker_id,
                    ?function_id,
                    error = ?err,
                    "Failed to reap finished _Active_ Worker: {err:#}",
                );
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // ShutDown
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline(never)]
    #[cold]
    #[instrument(level = Level::INFO, skip_all)]
    async fn shutdown(&mut self) -> Result<()> {
        // TODO: Should we close our own sender halves here, or should we rely only on the quit
        // notification for shutting the rest of the actors down?
        // TODO(ckatsak): Workers surely don't listen on the quit channel, therefore it is our
        // responsibility to shut them down.

        // TODO(ckatsak): Shut down all dependents first (e.g., all Workers) before shutting down
        // ourselves, so that we don't have to handle the closure of all channels for our
        // dependents?

        // NOTE(ckatsak): NetworkManager won't shut down until all sender halves of its channels
        // have been closed or dropped (i.e., our Workers' and ours).
        // We could drop the sender halves we own now, but we need to keep them open to clean up
        // any network resources attached to our remaining (orphan, either before the shutting down
        // begins or just made) VMs.

        //
        // FIXME(ckatsak): Store iterators are practically unusable :( let's just collect for now..
        //

        event!(
            Level::INFO,
            "#idle" = self.store.iter_idle().count(),
            "#active" = self.store.iter_active().count(),
            "#dying" = self.store.iter_dying().count(),
            "Beginning shutting Workers down...",
        );

        // First:
        // - change all (currently) Idle Workers to Dying,
        // - shut them down,
        // - abort their associated keepalive-timer.
        let idle_worker_ids = self.store.iter_idle().collect::<Vec<_>>();
        for (function_id, worker_id) in idle_worker_ids {
            if let Err(err) = self.decommission_idle_worker(worker_id, &function_id).await {
                error!(
                    error = ?err, %worker_id, %function_id,
                    "Failed to decommission _Idle_ Worker: {err:#}",
                );
            }
        }

        // Then:
        // - change all (currently) Active Workers to Dying,
        // - shut them down.
        let active_worker_ids = self.store.iter_active().collect::<Vec<_>>();
        for (function_id, worker_id) in active_worker_ids {
            // Maybe could use `self.deactivate_worker()` here, but let's not rush into
            // coupling it with the shutdown process, as the latter can be "special".
            if let Err(err) = self._active_worker_to_dying(worker_id, &function_id) {
                error!(
                    error = ?err, %worker_id, %function_id,
                    "Failed to change _Active_ Worker to _Dying_: {err:#}"
                );
                continue;
            }
            // Shut the Worker down
            if let Err(err) = self
                .worker_handles
                .get(&worker_id)
                .expect("Worker exists, since state transition just succeeded")
                .shutdown()
                .await
            {
                error!(
                    error = ?err, %worker_id, %function_id,
                    "Failed to shut down _Active_ Worker: {err:#}"
                );
            }
        }

        // Finaly, reap all Dying Workers.
        let start = Instant::now();
        const TICK: Duration = Duration::from_millis(3000);
        let mut interval = interval_at(start + TICK, TICK);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            ::tokio::select! {
                Some(dreq) = self.from.admission.recv() => {
                    trace!(?dreq, "Rejecting due to shutdown");
                    if let Err(dresp) = dreq.reply(DispatchResponse::shutting_down()) {
                        debug!(?dresp, "Failed to reject DispatchRequest");
                    }
                }
                msg = self.from.workers.recv() => {
                    use worker::OutboundMessage as Msg;
                    trace!(?msg, "New message while shutting down");
                    match msg {
                        None => break,
                        Some(Msg::SandboxId { worker_id, sandbox_id }) => {
                            // NOTE: Receiving SandboxId means that the Worker is still Active
                            // (just created a new Sandbox), and is about to actually handle an
                            // invocation. No harm to play along; it will eventually process the
                            // Shutdown message, clean up, shut down, and get reaped.
                            info!(%worker_id, %sandbox_id, "Processing SandboxId message");
                            if let Err(err) = self.update_sandbox_id(worker_id, sandbox_id.clone()) {
                                error!(
                                    error = ?err, %worker_id, %sandbox_id,
                                    "Failed to track WorkerId<->SandboxId: {err:#}"
                                );
                            }
                        }
                        Some(Msg::NeedWork { worker_id, function_id }) => {
                            // Receiving NeedWork means that the Worker was Active and now
                            // is Dying, and must have received a ShutDown message by now.
                            info!(%worker_id, %function_id, "Received NeedWork");
                            if let Err(err) = self.cpuset.release(worker_id) {
                                error!(
                                    error = ?err, %worker_id, %function_id,
                                    "Failed to release cpuset for Worker during shutdown: {err:#}"
                                );
                            }
                        },
                        Some(Msg::ReapMe { worker_id, function_id }) => {
                            if let Err(err) =
                                self.reap_worker(worker_id, function_id.clone()).await
                            {
                                error!(
                                    ?worker_id,
                                    ?function_id,
                                    error = ?err,
                                    "Failed to reap Worker: {err:#}",
                                );
                            }
                        },
                        Some(Msg::SandboxPreparation(resp)) => {
                            // NOTE: The Worker has been _Active_ while handling the sandbox prep
                            // request, and must have been sent and be processing a Shutdown request
                            // by now. Don't route its completion handling through the regular path,
                            // since all Workers should be Dying by now. Do a minimal, best-effort
                            // handling here, respond to any subscribed waiters, and await Worker's
                            // ReapMe for correct cleanup.
                            self.finalize_sandbox_preparation_during_shutdown(resp);
                        }
                        Some(Msg::SnapshotCreation(resp)) => {
                            // NOTE: The Worker has been _Active_ while handling the snapshotting
                            // request, and must have been sent and be processing a ShutDown request
                            // by now. Let's attempt to notify any subscribed channels (best-effort)
                            // and then just wait for the subsequent ReapMe by the Worker.
                            self.finalize_snapshotting_during_shutdown(resp);
                        }
                    }
                },
                now = interval.tick() => {
                    // NOTE(ckatsak): We cannot rely only on the number of sender halves of
                    // self.from.workers, because we ourselves keep a sending half open (to be
                    // able to clone it and pass it around to new Workers). Therefore, we check
                    // the number of tracked Workers as well, and consider the shutdown to have
                    // been completed when no more workers are being tracked.
                    if self.store.iter_all().count() == 0 {
                        break
                    }
                    event!(
                        Level::INFO,
                        "#idle" = self.store.iter_idle().count(),
                        "#active" = self.store.iter_active().count(),
                        "#dying" = self.store.iter_dying().count(),
                        "Waiting ({} so far) for all Workers to shut down before exiting...",
                        ::humantime::format_duration(now - start)
                    );
                }
            }
        }
        debug!(
            "All workers must have been shut down (after {}); exiting",
            ::humantime::format_duration(Instant::now() - start)
        );

        // TODO(ckatsak): What to do with all those orphan VMs that I now own? These correspond to
        // node's resources (network and storage).
        warn!(
            "#orphans" = self.snapshots.values().map(Vec::len).sum::<usize>(),
            "Remaining orphaned/snapshotted sandboxes"
        );
        if !self.retain_snapshots {
            for (function_id, sandboxes) in ::std::mem::take(&mut self.snapshots) {
                for sandbox in sandboxes {
                    let _sandbox_id = sandbox.id().to_owned();
                    if let Err(err) = self.destroy_orphan_sandbox(&function_id, sandbox).await {
                        error!(
                            %function_id, sandbox_id = _sandbox_id, error = ?err,
                            "Failed to destroy orphan sandbox and free its resources",
                        );
                    }
                }
            }
            if let Err(err) = self.wipe_db_snapshots().await {
                error!(error = ?err, "Failed to wipe sandbox snapshots from database: {err:#}")
            }
        } else if let Err(err) = self.sync_db_snapshots().await {
            error!(error = ?err, "Failed to sync sandbox snapshots with database: {err:#}")
        }

        // TODO(ckatsak): What about trashed sandboxes?
        if !self.trashed_sandboxes.is_empty() {
            warn!(
                trashed_sandboxes = ?self.trashed_sandboxes,
                "FIXME: Clean trashed sandboxes up?!",
            );
        }

        Ok(())
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Error handling / cleaning up
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// TODO: impl?
    #[instrument(level = Level::DEBUG, skip(self))]
    async fn destroy_orphan_sandbox(
        &mut self,
        function_id: &FunctionId,
        sb: Runtime::Sandbox,
    ) -> Result<()> {
        let mut rt = Runtime::new(
            &self.worker_aux.config.runtime,
            self.store.registered_function(function_id).info(),
            Some(&sb),
        )
        .map_err(|err| Error::Runtime {
            msg: String::from("failed to instantiate runtime").into_boxed_str(),
            source: err,
        })?;
        rt.init().await.map_err(|err| Error::Runtime {
            msg: String::from("failed to initialize new runtime instance").into_boxed_str(),
            source: err,
        })?;

        let net_resource = match rt.destroy_sandbox(sb).await {
            Ok(net_resource) => net_resource,
            Err(err) => {
                let (sandbox, err) = err.into_parts();
                error!(
                    error = ?err, %function_id, ?sandbox,
                    previous.total.trashed = %self.trashed_sandboxes.len(),
                    "Runtime failed to destroy sandbox; +1 trashed; failure: {err:#}"
                );
                self.trashed_sandboxes.push(sandbox);

                return Err(Error::Runtime {
                    msg: String::from("runtime failed to destroy sandbox").into_boxed_str(),
                    source: err,
                });
            }
        };

        self.netman
            .deallocate(net_resource)
            .await
            .map_err(|err| Error::Network {
                msg: String::from("failed to queue net resource deallocation").into_boxed_str(),
                source: err,
            })?;

        Ok(())
    }

    /// Basically rolls back any changes made in [`Self::handle_dispatch`].
    ///
    /// # Returns
    ///
    /// Always [`Error::AdmissionUnresponsive`].
    ///
    /// # Implementation Notes
    ///
    /// - Free the allocated CPU, to be used for other invocations.
    /// - Remove the [`WorkerHandle`] from the _Active_ list and (¿re)-insert it to the _Idle_ list
    /// - If we intended to kill the [`Worker`], then we should mind that it owns `Rt::Sandbox`es,
    /// which we should own again and somehow(?) handle them. However, now that we chose to keep
    /// the [`Worker`] around as _Idle_, we don't need to do that.
    /// - "Resume" an associated timer exactly where we left off (or spawn anew if new [`Worker`]).
    #[instrument(level = Level::WARN, skip_all)]
    #[cold]
    #[inline(never)]
    async fn cleanup_failed_dispatch(
        &mut self,
        dresp: DispatchResponse<Req>,
        old_deadline: Option<Instant>,
    ) -> Result<()> {
        let DispatchResponse::Dispatch { worker_ref, .. } = dresp else {
            error!("SandboxPool::cleanup_failed_dispatch() called with {dresp:?}");
            unreachable!("BUG: SandboxPool::cleanup_failed_dispatch() called with {dresp:?}");
        };
        error!("Failed to send `{worker_ref:?}` to AdmissionController");

        let function_id = self
            .worker_handles
            .get(&worker_ref.id())
            .expect("must have just been inserted to SandboxPool.worker_handles & the Active list")
            .function_id()
            .clone();

        let memory_freed = self.store.function_memory(&function_id);
        self.memory.inuse -= memory_freed;
        if let Err(err) = self.cpuset.release(worker_ref.id()) {
            // We must have just allocated that!
            error!(
                error = ?err,
                "Failed to release cpuset while rolling back a failed dispatch: {err:#}",
            );
            return Err(Error::CpuSet(err));
        }

        // Calculate the new deadline
        let new_deadline = match old_deadline {
            Some(deadline) => deadline,
            None => {
                Instant::now()
                    + self
                        .with_policy_context(|policy, pctx| policy.assign(pctx, &function_id))
                        .unwrap_or(Duration::ZERO)
            }
        };

        // - Remove the `WorkerHandle` from the Active list and (¿re-)insert it to the Idle list
        // SAFETY: We must have just inserted the worker to the Active list
        self.store
            .worker_active_to_idle(worker_ref.id(), &function_id)
            .map_err(|err| Error::Metadata(Box::new(err)))?; // TODO: error handling?

        // NOTE(ckatsak): If we intended to kill the Worker, then we should mind that it owns
        // `Rt::Sandboxes`, which we should own again and somehow(?) handle them. However,
        // now that we chose to keep the Worker around as Idle, we don't need to do that.

        // - "Resume" an associated timer exactly where we left off (or spawn anew if new Worker)
        let _reverted_deadline = self.worker_deadlines.insert(worker_ref.id(), new_deadline);
        self.keepalive
            .spawn_timer(worker_ref.id(), new_deadline)
            .await;

        Err(Error::AdmissionUnresponsive(
            format!("failed to send WorkerRef (WorkerId: `{}`)", worker_ref.id()).into_boxed_str(),
        ))
    }
}

#[cfg(feature = "__toy")]
::static_assertions::assert_impl_all!(
    SandboxPool<
        crate::request::toy::ToyStringRequest,
        crate::response::toy::ToyStringResponse,
        crate::metadata::StdHashMapFmdStore<
            <crate::worker::runtime::toy::ToyRt as crate::worker::Runtime>::FunctionInfo
        >,
        crate::sbpool::keepalive::Fixed<crate::sbpool::keepalive::eviction::NoOp>,
        crate::worker::runtime::toy::ToyRt,
        crate::worker::issuer::toy::ToyIssuer,
        crate::network::Tap,
    >: Send,
);
