use std::{collections::HashMap, marker::PhantomData, path::PathBuf, time::Duration};

use compact_str::ToCompactString;
use redb::Database;
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinError, JoinHandle},
};
use tracing::{debug, error, info, instrument, trace, warn, Level};
use triomphe::{Arc, ArcBorrow};

use crate::{
    conf::{PerfConfig, SnaplaceConfig},
    metadata::db::Tables,
    metrics::{MetricsCollectorRef, Nanoseconds},
    snapman::{
        error::Result,
        perfmon::{MonitorTarget, PerformanceMonitorHandle},
        Error, PerformanceMonitor, PlacementAlgorithm,
    },
    worker::{self, runtime::SnapshotState, Sandbox, SandboxStateRef, WorkerId},
    FunctionId, FunctionMetadataStore, SandboxId,
};

#[derive(Debug, Clone)]
pub struct SnapshotPaths {
    pub state: PathBuf,
    pub memory: PathBuf,
}

#[derive(Debug)]
enum SnapshotManagerMessage<Sb: Sandbox> {
    /// Sent by a [`Worker`] to spawn & initialize a new [`PerformanceMonitor`].
    ///
    /// [`Worker`]: crate::worker::Worker
    SpawnMonitor {
        worker_id: WorkerId,
        monitor_targets: Vec<MonitorTarget>,
        start_state: SandboxStateRef,
        /// Original sender blocks on this until [`PerformanceMonitor`] has been fully initialized.
        /// Error is returned in case of failure during initialization.
        respond_to: oneshot::Sender<Result<()>>,
    },

    /// Sent by a [`Worker`] to signal the end of request handling (so the end of performance
    /// monitoring too).
    ///
    /// [`Worker`]: crate::worker::Worker
    StopMonitor {
        worker_id: WorkerId,
        function_id: FunctionId,
        /// The monitored [`Sandbox`]'s starting state.
        ///
        /// [`Sandbox`]: crate::worker::runtime::Sandbox
        start_state: SandboxStateRef,
        /// Possibly an associated duration (presumably related to [`Worker`]'s
        /// [`RequestIssuer`]) that might be useful to [`PlacementAlgorithm`].
        ///
        /// [`Worker`]: crate::worker::Worker
        /// [`RequestIssuer`]: crate::worker::RequestIssuer
        maybe_duration: Option<Duration>,
    },

    /// Sent by a [`Worker`] to query snapshot files' paths.
    ///
    /// [`Worker`]: crate::worker::Worker
    QueryPaths {
        function_id: FunctionId,
        respond_to: oneshot::Sender<Result<Option<SnapshotPaths>>>,
    },

    /// Sent by a [`Worker`] to notify of a newly created snapshot.
    ///
    /// [`Worker`]: crate::worker::Worker
    NewSnapshot {
        function_id: FunctionId,
        state: Box<Sb::SnapshotState>,
    },

    /// Sent by a [`Worker`] to notify of a newly destroyed snapshot.
    ///
    /// [`Worker`]: crate::worker::Worker
    SnapshotRemoved {
        function_id: FunctionId,
        sandbox_id: SandboxId,
    },
}

#[derive(Debug)]
pub struct SnapshotManagerHandle<Sb: Sandbox> {
    to_snapman: mpsc::Sender<SnapshotManagerMessage<Sb>>,
    handle: JoinHandle<Result<()>>,
}

#[derive(Debug)]
pub struct SnapshotManagerRef<Sb: Sandbox> {
    to_snapman: mpsc::Sender<SnapshotManagerMessage<Sb>>,
}

impl<Sb: Sandbox> Clone for SnapshotManagerRef<Sb> {
    fn clone(&self) -> Self {
        Self {
            to_snapman: self.to_snapman.clone(),
        }
    }
}

impl<Sb: Sandbox> SnapshotManagerRef<Sb> {
    #[instrument(level = Level::TRACE, skip_all, fields(monitor_targets))]
    pub async fn spawn_monitor(
        &self,
        worker_id: WorkerId,
        monitor_targets: Vec<MonitorTarget>,
        start_state: SandboxStateRef,
    ) -> Result<()> {
        let (respond_to, rx) = oneshot::channel();

        self.to_snapman
            .send(SnapshotManagerMessage::SpawnMonitor {
                worker_id,
                monitor_targets,
                start_state,
                respond_to,
            })
            .await
            .map_err(|err| {
                warn!(error = ?err, "Failed to spawn monitor through SnapshotManager: {err:#}");
                Error::Channel {
                    msg: format!("error sending '{:?}' to SnapshotManager", err.0).into_boxed_str(),
                    source: Box::new(err),
                }
            })?;

        rx.await.map_err(|err| {
            error!(error = ?err, "Failed to spawn monitor through SnapshotManager: {err:#}");
            Error::Channel {
                msg: "error receiving Monitor's initialization Result from SnapshotManager".into(),
                source: Box::new(err),
            }
        })?
    }

    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn stop_monitor(
        &self,
        worker_id: WorkerId,
        function_id: FunctionId,
        start_state: SandboxStateRef,
        maybe_duration: Option<Duration>,
    ) -> Result<()> {
        self.to_snapman
            .send(SnapshotManagerMessage::StopMonitor {
                worker_id,
                function_id,
                start_state,
                maybe_duration,
            })
            .await
            .map_err(|err| {
                warn!(error = ?err, "Failed to contact SnapshotManager to stop monitor: {err:#}");
                Error::Channel {
                    msg: format!("error sending '{:?}' to SnapshotManager", err.0).into_boxed_str(),
                    source: Box::new(err),
                }
            })
    }

    /// Query the `SnapshotManager` for the snapshot file paths.
    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn query_paths(&self, function_id: FunctionId) -> Result<Option<SnapshotPaths>> {
        let (respond_to, rx) = oneshot::channel();

        self.to_snapman
            .send(SnapshotManagerMessage::QueryPaths {
                function_id,
                respond_to,
            })
            .await
            .map_err(|err| {
                warn!(error = ?err, "Failed to query SnapshotManager for snapshot paths: {err:#}");
                Error::Channel {
                    msg: format!("error sending '{:?}' to SnapshotManager", err.0).into_boxed_str(),
                    source: Box::new(err),
                }
            })?;

        rx.await.map_err(|err| {
            error!(error = ?err, "Failed to receive snapshot paths from SnapshotManager: {err:#}");
            Error::Channel {
                msg: "error receiving paths from SnapshotManager".into(),
                source: Box::new(err),
            }
        })?
    }

    /// Notify the `SnapshotManager` of a newly created snapshot, to update the database.
    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn notify_new_snapshot(
        &self,
        function_id: FunctionId,
        state: Box<Sb::SnapshotState>,
    ) -> Result<()> {
        self.to_snapman
            .send(SnapshotManagerMessage::NewSnapshot { function_id, state })
            .await
            .map_err(|err| {
                warn!(error = ?err, "Failed to notify SnapshotManager of new snapshot: {err:#}");
                Error::Channel {
                    msg: format!("error sending '{:?}' to SnapshotManager", err.0).into_boxed_str(),
                    source: Box::new(err),
                }
            })
    }

    /// Notify the `SnapshotManager` of a destroyed snapshot, to update the database.
    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn notify_snapshot_removal(
        &self,
        function_id: FunctionId,
        sandbox_id: SandboxId,
    ) -> Result<()> {
        self.to_snapman
            .send(SnapshotManagerMessage::SnapshotRemoved {
                function_id,
                sandbox_id,
            })
            .await
            .map_err(|err| {
                warn!(error = ?err, "Failed to notify SnapshotManager of snapshot destruction: {err:#}");
                Error::Channel {
                    msg: format!("error sending '{:?}' to SnapshotManager", err.0).into_boxed_str(),
                    source: Box::new(err),
                }
            })
    }
}

impl<Sb: Sandbox> SnapshotManagerHandle<Sb> {
    #[inline]
    pub fn new_ref(&self) -> SnapshotManagerRef<Sb> {
        SnapshotManagerRef {
            to_snapman: self.to_snapman.clone(),
        }
    }

    /// TODO: impl?
    #[instrument(level = Level::DEBUG, skip_all)]
    pub async fn reap(self) -> ::std::result::Result<Result<()>, JoinError> {
        // Drop the Sender owned by `SnapshotManagerHandle`, to make sure `SnapshotManager` exits
        // its main run loop (when all other `SnapshotManagerRef`s are dropped, which presumably
        // already have by now)...
        drop(self.to_snapman);
        // ...and then just wait for `SnapshotManager::run` to complete
        self.handle.await
    }
}

#[derive(Debug)]
pub(crate) struct SnapshotManager<PA, Runtime, Store, FunctionInfo>
where
    PA: PlacementAlgorithm,
    Runtime: worker::Runtime,
    Store: FunctionMetadataStore<FunctionInfo>,
    FunctionInfo: 'static,
{
    store: Arc<Store>,

    perf_config: PerfConfig,

    perfmons: HashMap<
        WorkerId,
        <PA::PerformanceMonitor as PerformanceMonitor>::Handle,
        crate::BuildHasher,
    >,

    writer: JoinHandle<Result<()>>,
    to_writer: mpsc::Sender<SnapshotManagerMessage<Runtime::Sandbox>>,

    _to_pool: mpsc::Sender<()>, // TODO: Should SnapshotManager ever have to talk to Pool?
    _to_timings: MetricsCollectorRef<Nanoseconds>, // TODO

    rx: mpsc::Receiver<SnapshotManagerMessage<Runtime::Sandbox>>,

    // - can be used by `SnapshotManager` (internally) to create `SnapshotManagerRef`s.
    // - the corresponding `rx` is stored here in `SnapshotManager` as well.
    // - if this was a regular `Sender`, we would not be able to rely on the channel closing when
    // `Pool` shuts down (because we'd still hold a tx half), so we'd never shut down.
    _tx: mpsc::WeakSender<SnapshotManagerMessage<Runtime::Sandbox>>,

    placement: PA,

    _function_info: PhantomData<fn() -> FunctionInfo>,
}

impl<PA, Runtime, Store, FunctionInfo> SnapshotManager<PA, Runtime, Store, FunctionInfo>
where
    PA: PlacementAlgorithm,
    Runtime: worker::Runtime,
    Store: FunctionMetadataStore<FunctionInfo>,
{
    const CHANNEL_SIZE: usize = 128; // TODO(ckatsak): right-sized buffer?

    #[instrument(level = Level::DEBUG, skip_all)]
    pub fn spawn(
        config: &SnaplaceConfig<Runtime::Config>,
        store: Arc<Store>,
        db: Arc<Database>,
        placement: PA,
        _to_pool: mpsc::Sender<()>,                    // TODO
        _to_timings: MetricsCollectorRef<Nanoseconds>, // TODO
    ) -> SnapshotManagerHandle<Runtime::Sandbox> {
        let (to_writer, from_snapman) = mpsc::channel(Self::CHANNEL_SIZE);
        let writer = ::tokio::task::spawn_blocking(|| Self::db_writer(db, from_snapman));

        let (to_snapman, rx) = mpsc::channel(Self::CHANNEL_SIZE);
        let snapman = Self {
            store,
            perfmons: Default::default(),
            perf_config: config.workers.perf.clone(),

            writer,
            to_writer,

            _to_pool,
            _to_timings,
            rx,
            _tx: to_snapman.downgrade(),

            placement,

            _function_info: PhantomData,
        };
        let handle = ::tokio::spawn(async move { snapman.run().await });

        SnapshotManagerHandle { to_snapman, handle }
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    pub(crate) async fn run(mut self) -> Result<()> {
        // SnapshotManager shuts down when all references to it are dropped (i.e., when SandboxPool
        // shuts down, and Orchestrator calls `SnapshotManagerHandle::reap`).
        while let Some(msg) = self.rx.recv().await {
            trace!(?msg, "New message received");
            match msg {
                SnapshotManagerMessage::SpawnMonitor {
                    worker_id,
                    monitor_targets,
                    start_state,
                    respond_to,
                } => {
                    // Start a new PerformanceMonitor & store its handle in `self.perfmons`
                    match PA::PerformanceMonitor::start(
                        &self.perf_config,
                        &monitor_targets,
                        start_state,
                        respond_to,
                    )
                    .await
                    {
                        Ok(handle) => {
                            if let Some(old_handle) = self.perfmons.insert(worker_id, handle) {
                                // FIXME: PerformanceMonitorHandles are not cleaned up correctly
                                // for now. This can be serious. Fix it here or at the Worker?
                                warn!(
                                    ?old_handle,
                                    "Old PerformanceMonitorHandle had not been cleaned up correctly"
                                );
                            }
                        }
                        Err(err) => error!(
                            ?worker_id, ?monitor_targets, error = ?err,
                            "Failed to start PerformanceMonitor: {err:#}",
                        ),
                    }
                }
                SnapshotManagerMessage::StopMonitor {
                    worker_id,
                    function_id,
                    start_state,
                    maybe_duration,
                } => {
                    // Stop the PerformanceMonitor and update Function's metrics
                    // NOTE:
                    // - For now, failed `SpawnMonitor`s lead to spurious `StopMonitor`s, which
                    //   we ignore.
                    // - `PlacementAlgorithm`'s metrics are only updated if a duration is attached
                    //   to the `StopMonitor` message; otherwise let's consider it a failure and
                    //   avoid "learning" from its execution.
                    if let Some(handle) = self.perfmons.remove(&worker_id) {
                        match handle.finish().await {
                            Ok(metrics) => {
                                if let Some(duration) = maybe_duration {
                                    let _ = self.placement.update_metrics(
                                        &function_id,
                                        start_state,
                                        &metrics,
                                        duration,
                                    )
                                    .map_err(|err| {
                                        error!(
                                            ?worker_id, ?function_id, error = ?err,
                                            "Failed to update metrics in PlacementAlgorithm: {err:#}",
                                        );
                                    });
                                }
                            }
                            Err(err) => error!(
                                ?worker_id, ?function_id, error = ?err,
                                "Failed to finish PerformanceMonitor and collect its metrics: {err:#}",
                            ),
                        }
                    }
                    // TODO?
                }
                SnapshotManagerMessage::QueryPaths {
                    function_id,
                    respond_to,
                } => {
                    let paths_res = self.placement.query_path(&function_id);
                    let _ = respond_to.send(paths_res).map_err(|paths_res| {
                        error!(?function_id, "Failed to respond query with {paths_res:?}",);
                    });
                }
                msg @ SnapshotManagerMessage::NewSnapshot { .. }
                | msg @ SnapshotManagerMessage::SnapshotRemoved { .. } => {
                    if let Err(ref err @ mpsc::error::SendError(ref msg)) =
                        self.to_writer.send(msg).await
                    {
                        warn!(
                            ?msg,
                            "Failed to forward snapshot tables update to DB writer: {err}"
                        );
                    }
                }
            }
        }
        info!("Exiting...");

        ::std::mem::drop(self.to_writer);
        match self.writer.await {
            Ok(Ok(())) => debug!("Successfully reaped database writing thread"),
            Ok(Err(err)) => error!(error = ?err, "Reaped database writing thread failed: {err:#}"),
            Err(jerr) => error!(error = ?jerr, "Failed to join tokio task: {jerr:#}"),
        }

        Ok(())
    }
}

/// Implementation related to the database-writing (blocking) thread.
impl<PA, Runtime, Store, FunctionInfo> SnapshotManager<PA, Runtime, Store, FunctionInfo>
where
    PA: PlacementAlgorithm,
    Runtime: worker::Runtime,
    Store: FunctionMetadataStore<FunctionInfo>,
{
    fn db_writer(
        db: Arc<Database>,
        mut from_snapman: mpsc::Receiver<SnapshotManagerMessage<Runtime::Sandbox>>,
    ) -> Result<()> {
        use SnapshotManagerMessage as Msg;
        loop {
            // TODO: change to `blocking_recv_many()`
            match from_snapman.blocking_recv() {
                Some(Msg::NewSnapshot { function_id, state }) => {
                    Self::db_new_snapshot(db.borrow_arc(), function_id, &state);
                }
                Some(Msg::SnapshotRemoved {
                    function_id,
                    sandbox_id,
                }) => Self::db_rm_snapshot(db.borrow_arc(), function_id, sandbox_id),
                None => return Ok(()),
                Some(msg) => error!(?msg, "Ignoring unexpected message variant"),
            }
        }
    }

    /// Persist a newly created snapshot to the snapshot tables.
    ///
    /// This updates both:
    /// - [`Tables::SNAPS_PER_FUNC`], which indexes snapshot IDs by [`FunctionId`];
    /// - [`Tables::SNAPSHOTS`], which stores the full [`SnapshotState`].
    ///
    /// This helper is best-effort only. It logs and returns on storage errors,
    /// leaving eventual reconciliation to Pool's shutdown-time DB sync.
    fn db_new_snapshot(
        db: ArcBorrow<Database>,
        function_id: FunctionId,
        state: &<Runtime::Sandbox as Sandbox>::SnapshotState,
    ) {
        let wtxn = match db.begin_write() {
            Ok(wtxn) => wtxn,
            Err(err) => {
                error!(error = ?err, ?function_id, "Failed to open write txn: {err:#}");
                return;
            }
        };
        {
            let sid = state.id();
            match wtxn.open_multimap_table(Tables::<Runtime>::SNAPS_PER_FUNC) {
                Ok(mut tbl) => match tbl.insert(&function_id, sid.to_compact_string()) {
                    Ok(false) => {}
                    Err(err) => {
                        error!(
                            error = ?err, ?function_id, ?state,
                            "Failed to insert new SandboxID to table SNAPS_PER_FUNC: {err:#}"
                        );
                        return;
                    }
                    Ok(true) => {
                        warn!(?state, ?function_id, ?sid, "SandboxID already exists");
                        return;
                    }
                },
                Err(err) => {
                    error!(error = ?err, ?function_id, "Failed to open table SNAPS_PER_FUNC");
                    return;
                }
            };
            match wtxn.open_table(Tables::<Runtime>::SNAPSHOTS) {
                Ok(mut tbl) => match tbl.insert(sid.to_compact_string(), state) {
                    Ok(maybe_old_state) => debug!(
                        old.state = ?maybe_old_state.map(|s| s.value()), new.state = ?state
                    ),
                    Err(err) => {
                        error!(
                            error = ?err, ?function_id, ?state,
                            "Failed to insert new snapshot to table SNAPSHOTS: {err:#}"
                        );
                        return;
                    }
                },
                Err(err) => {
                    error!(error = ?err, ?function_id, "Failed to open table SNAPSHOTS");
                    return;
                }
            }
        }
        let _ = wtxn.commit().inspect_err(
            |err| warn!(error = ?err, "Failed to commit txn to snapshot tables: {err:#}"),
        );
    }

    /// Remove a destroyed snapshot from the snapshot tables.
    ///
    /// This attempts to delete the snapshot from both:
    /// - [`Tables::SNAPS_PER_FUNC`], under its owning [`FunctionId`];
    /// - [`Tables::SNAPSHOTS`], under its [`SandboxId`].
    ///
    /// Missing rows are logged but do not abort the rest of the cleanup, so this
    /// helper can still converge the DB back to a consistent state when the two
    /// tables have drifted apart.
    #[instrument(level = Level::TRACE, skip(db))]
    fn db_rm_snapshot(db: ArcBorrow<Database>, function_id: FunctionId, sandbox_id: SandboxId) {
        let wtxn = match db.begin_write() {
            Ok(wtxn) => wtxn,
            Err(err) => {
                error!(error = ?err, ?function_id, "Failed to open write txn: {err:#}");
                return;
            }
        };
        {
            match wtxn.open_multimap_table(Tables::<Runtime>::SNAPS_PER_FUNC) {
                Ok(mut tbl) => match tbl.remove(&function_id, sandbox_id.clone()) {
                    Ok(true) => {}
                    Err(err) => error!(
                        error = ?err, %function_id, %sandbox_id,
                        "Failed to remove SandboxID from table SNAPS_PER_FUNC: {err:#}"
                    ),
                    Ok(false) => warn!(
                        %function_id, %sandbox_id,
                        "Failed to remove SandboxId from table SNAPS_PER_FUNC: not found"
                    ),
                },
                Err(err) => {
                    error!(error = ?err, ?function_id, "Failed to open table SNAPS_PER_FUNC")
                }
            };
            match wtxn.open_table(Tables::<Runtime>::SNAPSHOTS) {
                Ok(mut tbl) => match tbl.remove(sandbox_id) {
                    Ok(maybe_old_state) => debug!(old.state = ?maybe_old_state.map(|s| s.value())),
                    Err(err) => {
                        error!(
                            error = ?err, %function_id,
                            "Failed to remove snapshot from table SNAPSHOTS: {err:#}"
                        );
                        return;
                    }
                },
                Err(err) => {
                    error!(error = ?err, ?function_id, "Failed to open table SNAPSHOTS");
                    return;
                }
            }
        }
        let _ = wtxn.commit().inspect_err(
            |err| warn!(error = ?err, "Failed to commit txn to snapshot tables: {err:#}"),
        );
    }
}
