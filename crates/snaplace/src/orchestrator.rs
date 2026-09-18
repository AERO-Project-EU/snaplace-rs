use std::{fmt::Debug, path::Path, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use redb::Database;
use rustix::process::{getrlimit, setrlimit};
use tokio::{
    signal::unix::{signal, Signal, SignalKind},
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::{sleep, Instant},
};
use tracing::{debug, error, info, instrument, trace, warn, Level};
use triomphe::Arc;

use crate::{
    admission::{self, AdmissionController},
    conf::{AdmissionConfig, SnaplaceConfig},
    control::{self, ControlPlaneServerHandle},
    error::Error,
    metadata::{db::Tables, FunctionInfo, FunctionMetadataStore},
    metrics::{MetricsCollector, MetricsCollectorHandle, Nanoseconds},
    network::{self, NetworkManager, NetworkManagerHandle},
    request, response,
    sbpool::{keepalive, PoolEvent, SandboxPool, SandboxPoolHandle},
    snapman::{self, SnapshotManager, SnapshotManagerHandle},
    worker,
};

#[instrument(level = Level::TRACE, skip_all, fields(sig = sig))]
async fn signal_handler(sig: &'static str, mut sig_stream: Signal, quit_tx: broadcast::Sender<()>) {
    loop {
        info!("Blocking for SIG{sig}...");
        sig_stream.recv().await;
        warn!("Just caught SIG{sig}; broadcasting quit notification...");
        match quit_tx.send(()) {
            Ok(num_subscribers) => {
                warn!("Broadcasted quit notification to {num_subscribers} subscribers...");
                break;
            }
            Err(err) => {
                error!(error = ?err, "Error broadcasting quit notification upon SIG{sig} delivery");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[derive(Debug)]
pub struct Orchestrator<Runtime>
where
    Runtime: worker::Runtime,
{
    /// Handles for the signal handling tasks.
    signal_handlers: Vec<JoinHandle<()>>,
    /// Handle for the [`request::Source`].
    request_source: JoinHandle<Result<(), request::Error>>,
    /// Handle for the [`response::Sink`].
    response_sink: JoinHandle<Result<(), response::Error>>,
    /// Handle for the [`NetworkManager`].
    netman: NetworkManagerHandle,
    /// Handle for the [`AdmissionController`].
    admission_ctl: JoinHandle<Result<admission::controller::QuickStats, ()>>,
    /// Handle for the [`SandboxPool`].
    sandbox_pool: SandboxPoolHandle, //JoinHandle<Result<(), sbpool::Error>>,
    /// Handle for the [`SnapshotManager`].
    snapman: SnapshotManagerHandle<Runtime::Sandbox>,
    /// Handle for the gRPC server task for the control-plane API.
    ctl_plane: ControlPlaneServerHandle, //JoinHandle<Result<(), Error>>,
    /// Handle for the [`MetricsCollector`] used by [`Worker`]s to store their measured
    /// [`Timing`]s.
    ///
    /// [`Worker`]: crate::worker::Worker
    timings: MetricsCollectorHandle<Nanoseconds>,

    /// The database where any persistent state is stored.
    ///
    /// `Orchestrator` initializes it before passing it to the other actors, and keeps a
    /// reference to it merely to compact the underlying database file before exiting.
    db: Arc<Database>,

    /// The `Orchestrator` receives broadcasted quit notifications to stop running and shut other
    /// components down as gracefully as possible.
    quit_rx: broadcast::Receiver<()>,
}

impl<Runtime> Orchestrator<Runtime>
where
    Runtime: worker::Runtime,
{
    /// Initialize a new `Orchestrator`, spawning all component actors and setting up signal
    /// handlers.
    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn spawn<
        ReqSource,
        RespSink,
        Issuer,
        KeepAlivePolicy,
        SnapshotPlacement,
        NetProvider,
    >(
        config: SnaplaceConfig<Runtime::Config>,
        request_source: ReqSource,
        response_sink: RespSink,
        keepalive_policy: KeepAlivePolicy,
        snapshot_placement: SnapshotPlacement,
        provider: NetProvider,
    ) -> Result<Self, Error>
    where
        ReqSource: request::Source,
        RespSink: response::Sink,
        Issuer: worker::RequestIssuer<ReqSource::Request, RespSink::Response>,
        Runtime: worker::Runtime<NetResource = NetProvider::Resource>,
        KeepAlivePolicy: keepalive::Policy,
        SnapshotPlacement: snapman::PlacementAlgorithm,
        NetProvider: network::SandboxNetworkingProvider,
    {
        info!("Spawning with: {config:?}");

        Self::set_umask();
        Self::increase_rlimit_nofile().map_err(|err| Error::OrchestratorInit {
            msg: String::from("failed to increase soft limit for # of file descriptors"),
            source: Box::new(err),
        })?;

        // broadcast channel to propagate quit notifications, which are produced by signal handlers
        // and consumed by most other actors
        let (quit_tx, quit_rx) = broadcast::channel(1);
        let signal_handlers = Self::setup_signal_handlers(quit_tx.clone()).map_err(|err| {
            Error::OrchestratorInit {
                msg: String::from("failed to setup signal handlers"),
                source: Box::new(err),
            }
        })?;

        // mpsc channel for `Request`s produced by `request::Source` & consumed by `AdmissionController`
        let (to_admission, from_request_source) = mpsc::channel(config.admission.queue_size);
        let request_source =
            Self::spawn_request_source(request_source, to_admission, quit_tx.subscribe());

        // mpsc channel for `Response`s produced by `Worker`s and consumed by `response::Sink`
        let to_response_sink = response_sink.tx();
        let response_sink = Self::spawn_response_sink(response_sink, quit_tx.subscribe());

        let (netman, netman_ref) =
            NetworkManager::spawn(provider, quit_tx.subscribe()).map_err(|err| {
                Error::OrchestratorInit {
                    msg: String::from("failed to initialize NetworkManager"),
                    source: Box::new(err),
                }
            })?;

        let timings = MetricsCollector::spawn(&config.orchestrator.timings_path);

        let (db, store) = Self::initialize_state(&config.orchestrator.db_path)
            .map(|(db, store)| (Arc::new(db), Arc::new(store)))?;

        // mpsc channel for `DispatchRequest`s produced by AdmissionController and consumed by Pool
        let (to_pool, from_admission) = mpsc::channel(2 * config.admission.max_concurrency); // FIXME: channel cap?
        let (to_admission, from_pool) = mpsc::channel(512); // FIXME: channel cap?
        let admission_ctl = Self::spawn_admission::<
            ReqSource::Request,
            RespSink::Response,
            _,
            Runtime::FunctionInfo,
        >(
            &config.admission,
            Arc::clone(&store),
            from_request_source,
            to_response_sink.clone(),
            to_pool,
            from_pool,
            quit_tx.subscribe(),
        );

        // mpsc channel for "notifications"(?) produced by `SnapshotManager` & consumed by the Pool
        let (to_pool, from_snapman) = mpsc::channel(1); // FIXME: chan cap?
        let snapman = SnapshotManager::<_, Runtime, _, _>::spawn(
            &config,
            Arc::clone(&store),
            Arc::clone(&db),
            snapshot_placement,
            to_pool,
            timings.new_ref(),
        );

        let sandbox_pool = SandboxPool::<
            ReqSource::Request,
            RespSink::Response,
            _,
            _,
            Runtime,
            Issuer,
            NetProvider::Resource,
        >::spawn(
            &config,
            Arc::clone(&store),
            Arc::clone(&db),
            keepalive_policy,
            netman_ref,
            from_admission,
            to_admission,
            from_snapman,
            snapman.new_ref(),
            timings.new_ref(),
            to_response_sink,
            quit_tx.subscribe(),
        )
        .await
        .map_err(|err| Error::OrchestratorInit {
            msg: "failed to initialize SandboxPool".into(),
            source: err.into(),
        })?;

        let ctl_plane = control::spawn::<_, Runtime>(
            &config.workers.runtime,
            config.orchestrator.address.clone(),
            sandbox_pool.new_ref(),
            store,
            Arc::clone(&db),
            quit_tx.subscribe(),
        )
        .await
        .map_err(|err| Error::OrchestratorInit {
            msg: "failed to initialize Control Plane API server".into(),
            source: Box::new(err),
        })?;

        Ok(Self {
            signal_handlers,
            request_source,
            response_sink,
            netman,
            admission_ctl,
            sandbox_pool,
            snapman,
            ctl_plane,
            timings,
            db,
            quit_rx,
        })
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn set_umask() {
        const SNAPLACE_UMASK: ::rustix::fs::RawMode = 0o002;

        let old = ::rustix::process::umask(SNAPLACE_UMASK.into());
        debug!("Set umask: {old:#06o} -> {SNAPLACE_UMASK:#06o}");
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn increase_rlimit_nofile() -> Result<(), Error> {
        use rustix::process::Resource;

        let mut rlim = getrlimit(Resource::Nofile);
        rlim.current = rlim.maximum;
        setrlimit(Resource::Nofile, rlim).map_err(|err| Error::Io {
            msg: format!("failed to configure maximum file descriptor using: {rlim:?}"),
            source: err.into(),
        })
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn setup_signal_handlers(quit_tx: broadcast::Sender<()>) -> Result<Vec<JoinHandle<()>>, Error> {
        [
            ("HUP", SignalKind::hangup()),
            ("INT", SignalKind::interrupt()),
            ("QUIT", SignalKind::quit()),
            ("TERM", SignalKind::terminate()),
            ("USR1", SignalKind::user_defined1()),
            ("USR2", SignalKind::user_defined2()),
        ]
        .into_iter()
        .map(|(sig, signum)| {
            signal(signum)
                .map_err(|err| Error::Io {
                    msg: format!("failed to setup signal handler for SIG{sig}({signum:?})"),
                    source: err,
                })
                .map(|signal| {
                    let quit_tx = quit_tx.clone();
                    ::tokio::spawn(async move { signal_handler(sig, signal, quit_tx).await })
                })
        })
        .collect()
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn spawn_request_source<ReqSource: request::Source>(
        request_source: ReqSource,
        to_admission: mpsc::Sender<ReqSource::Request>,
        quit_rx: broadcast::Receiver<()>,
    ) -> JoinHandle<::std::result::Result<(), request::Error>> {
        // NOTE(ckatsak): There is always one `request::Source` associated with the `Orchestrator`,
        // which is the owner of the only tx half of `AdmissionController`'s data channel.
        // This means that if this tx half is dropped, `AdmissionController` shuts down, so Pool
        // shuts down, and the whole system is brought down.
        // Therefore, it is the responsibility of the `request::Source` to make sure that
        // `AdmissionController`'s tx half remains open as long as required.
        ::tokio::spawn(async move { request_source.run(to_admission, quit_rx).await })
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn spawn_response_sink<RespSink: response::Sink>(
        response_sink: RespSink,
        quit_rx: broadcast::Receiver<()>,
    ) -> JoinHandle<::std::result::Result<(), response::Error>> {
        ::tokio::spawn(async move { response_sink.run(quit_rx).await })
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn spawn_admission<Req, Resp, Store, FnInfo>(
        config: &AdmissionConfig,
        store: Arc<Store>,
        from_request_source: mpsc::Receiver<Req>,
        to_response_sink: mpsc::Sender<Resp>,
        to_pool: mpsc::Sender<crate::admission::DispatchRequest<Req>>,
        from_pool: mpsc::Receiver<PoolEvent>,
        quit_rx: broadcast::Receiver<()>,
    ) -> JoinHandle<::std::result::Result<admission::controller::QuickStats, ()>>
    // TODO: return Result<Stats, Error>? FIXME(XXX)
    where
        Req: crate::Request,
        Resp: crate::Response,
        Store: FunctionMetadataStore<FnInfo>,
        FnInfo: FunctionInfo,
    {
        let mut admission_controller = AdmissionController::new(
            config,
            store,
            from_request_source,
            to_response_sink,
            to_pool,
            from_pool,
            quit_rx,
        );
        ::tokio::spawn(async move { admission_controller.run().await })
    }

    //#[allow(clippy::too_many_arguments)]
    //#[instrument(level = Level::TRACE, skip_all)]
    //async fn spawn_sandbox_pool<ReqSource, RespSink, Store, KeepAlivePolicy, Issuer, NetResource>(
    //    config: &SandboxPoolConfig,
    //    workers_config: &WorkerConfig,
    //    store: Arc<Store>,
    //    db: Arc<Database>,
    //    keepalive_policy: KeepAlivePolicy,
    //    netman_ref: NetworkManagerRef<NetResource>,
    //    from_dispatcher: mpsc::Receiver<DispatchRequest<ReqSource::Request>>,
    //    from_control_plane: mpsc::Receiver<PoolControlMessage>,
    //    from_snapman: mpsc::Receiver<()>, // FIXME: type?!
    //    snapman_ref: SnapshotManagerRef<Runtime::Sandbox>,
    //    timings: MetricsCollectorRef<Nanoseconds>,
    //    to_response_sink: mpsc::Sender<RespSink::Response>,
    //    quit_rx: broadcast::Receiver<()>,
    //) -> Result<JoinHandle<::std::result::Result<(), sbpool::Error>>, sbpool::Error>
    //where
    //    ReqSource: request::Source,
    //    RespSink: response::Sink,
    //    Store: FunctionMetadataStore<Runtime::FunctionInfo>,
    //    KeepAlivePolicy: keepalive::Policy,
    //    Issuer: worker::RequestIssuer<ReqSource::Request, RespSink::Response>,
    //    Runtime: worker::Runtime<NetResource = NetResource>,
    //    NetResource: network::Resource,
    //{
    //    let mut sandbox_pool = SandboxPool::<_, _, _, _, Runtime, Issuer, _>::new(
    //        config,
    //        workers_config,
    //        store,
    //        db,
    //        keepalive_policy,
    //        netman_ref,
    //        from_dispatcher,
    //        from_control_plane,
    //        from_snapman,
    //        snapman_ref,
    //        timings,
    //        to_response_sink,
    //        quit_rx,
    //    )
    //    .await?;
    //    Ok(::tokio::spawn(async move { sandbox_pool.run().await }))
    //}
    //
    //#[instrument(level = Level::TRACE, skip_all)]
    //async fn spawn_control_server<Store>(
    //    config_orch: &OrchestratorConfig,
    //    config_rt: &RuntimeConfig,
    //    pool_ref: SandboxPoolRef,
    //    store: Arc<Store>,
    //    db: Arc<Database>,
    //    mut quit_rx: broadcast::Receiver<()>,
    //) -> Result<JoinHandle<Result<(), Error>>, Error>
    //where
    //    Store: FunctionMetadataStore<Runtime::FunctionInfo>,
    //{
    //    let registrar = Registrar::<Runtime, _>::new(store, db, config_rt)
    //        .await
    //        .map_err(|err| Error::OrchestratorInit {
    //            msg: String::from("failed to load registered Functions from database"),
    //            source: Box::new(err),
    //        })?;
    //    let registration = FunctionRegistrationServer::new(registrar);
    //
    //    let quit = async move {
    //        match quit_rx.recv().await {
    //            Ok(()) => warn!("Received quit notification!"),
    //            Err(err) => error!(error = ?err, "Failed to receive from quit channel"),
    //        }
    //    };
    //
    //    let ctl_plane_task = ::tokio::spawn({
    //        let config_orch = config_orch.clone();
    //
    //        async move {
    //            match config_orch.address {
    //                Address::Net(addr) => {
    //                    let inc = TcpListenerStream::new(TcpListener::bind(&addr).await.map_err(
    //                        |err| Error::OrchestratorInit {
    //                            msg: format!("failed to bind TCP socket to {addr:?}"),
    //                            source: Box::new(err),
    //                        },
    //                    )?);
    //
    //                    Server::builder()
    //                        .add_service(registration)
    //                        .serve_with_incoming_shutdown(inc, quit)
    //                        .await
    //                        .map_err(Error::Tonic)
    //                }
    //                Address::Uds(path) => {
    //                    let inc =
    //                        UnixListenerStream::new(UnixListener::bind(&path).map_err(|err| {
    //                            Error::OrchestratorInit {
    //                                msg: format!("failed to bind Unix socket to {path:?}"),
    //                                source: Box::new(err),
    //                            }
    //                        })?);
    //
    //                    Server::builder()
    //                        .add_service(registration)
    //                        .serve_with_incoming_shutdown(inc, quit)
    //                        .await
    //                        .map_err(Error::Tonic)
    //                }
    //            }
    //        }
    //        .instrument(info_span!("control_plane"))
    //    });
    //    Ok(ctl_plane_task)
    //}

    /// Block until `Orchestrator` receives a quit signal, and attempt to gracefully shut down
    /// after that.
    #[instrument(level = Level::INFO, skip_all)]
    pub async fn run(mut self) -> Result<(), Error> {
        let t_start = Instant::now();
        match self.quit_rx.recv().await {
            Ok(()) => warn!("Received quit notification!"),
            Err(err) => error!(error = ?err, "Failed to receive from quit channel"),
        }
        info!(
            "Shutting down after {}",
            ::humantime::format_duration(t_start.elapsed())
        );
        self.shutdown().await
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn shutdown(mut self) -> Result<(), Error> {
        trace!("Reaping gRPC control-plane server...");
        match self.ctl_plane.reap().await {
            Ok(Ok(())) => info!("gRPC control-plane server was successfully terminated and reaped"),
            Ok(Err(err)) => error!(error = ?err, "gRPC server exited with error: {err:#}"),
            Err(join_err) => error!("gRPC server failed to run to completion: {join_err:#}"),
        }

        trace!("Reaping AdmissionController...");
        match self.admission_ctl.await {
            Ok(Ok(stats)) => {
                // TODO: Result<Stats, Error> ? FIXME(XXX)
                info!("AdmissionController was successfully terminated and reaped; {stats:#?}")
            }
            Ok(Err(disp_err)) => {
                error!(error = ?disp_err, "AdmissionController exited with error: {disp_err:?}")
            }
            Err(join_err) => {
                error!("AdmissionController failed to run to completion: {join_err:#}")
            }
        }

        trace!("Reaping Request Source...");
        match self.request_source.await {
            Ok(Ok(stats)) => {
                // TODO: stats type?
                info!("Request Source was successfully terminated and reaped; {stats:#?}")
            }
            Ok(Err(req_err)) => {
                error!(error = ?req_err, "Request Source exited with error: {req_err:#}")
            }
            Err(join_err) => {
                error!(error = ?join_err, "Request Source failed to run to completion: {join_err:#}")
            }
        }

        trace!("Reaping SandboxPool...");
        // NOTE(ckatsak): SandboxPool can only break its shutdown loop after all its Workers have
        // dropped their sending halves.
        match self.sandbox_pool.reap().await {
            Ok(Ok(stats)) => {
                info!("SandboxPool was successfully terminated and reaped; {stats:#?}")
            }
            Ok(Err(pool_err)) => {
                error!(error = ?pool_err, "SandboxPool exited with error: {pool_err:#}")
            }
            Err(join_err) => error!("SandboxPool failed to run to completion: {join_err:#}"),
        }

        trace!("Reaping NetworkManager...");
        // NOTE(ckatsak): NetworkManager can only break its loop after SandboxPool and all its
        // Workers have dropped their sending halves.
        match self.netman.reap().await {
            Ok(Ok(stats)) => info!("NetworkManager successfully terminated and reaped; {stats:#?}"),
            Ok(Err(net_err)) => {
                error!(error = ?net_err, "NetworkManager exited with error: {net_err:#}")
            }
            Err(join_err) => error!("NetworkManager failed to run to completion: {join_err:#}"),
        }

        trace!("Reaping SnapshotManager...");
        match self.snapman.reap().await {
            Ok(Ok(stats)) => {
                // TODO
                info!("SnapshotManager successfully terminated and reaped; {stats:#?}")
            }
            Ok(Err(snaperr)) => {
                // TODO
                error!(error = ?snaperr, "SnapshotManager exited with error: {snaperr:#}")
            }
            Err(join_err) => error!("SnapshotManager failed to run to completion: {join_err:#}"),
        }

        trace!("Reaping Response Sink...");
        match self.response_sink.await {
            Ok(Ok(stats)) => {
                // FIXME: stats type!
                info!("Response Sink was successfully terminated and reaped; {stats:#?}")
            }
            Ok(Err(resp_err)) => {
                error!(error = ?resp_err, "Response Sink exited with error: {resp_err:#}")
            }
            Err(join_err) => {
                error!(error = ?join_err, "Response Sink failed to run to completion: {join_err:#}")
            }
        }

        trace!("Reaping (timings') MetricsCollector...");
        match self.timings.shutdown().await {
            Ok(Ok(())) => info!("MetricsCollector successfully terminated and reaped"),
            Ok(Err(mtr_err)) => {
                error!(error = ?mtr_err, "MetricsCollector exited with error: {mtr_err:#}")
            }
            Err(join_err) => error!("MetricsCollector failed to run to completion: {join_err:#}"),
        }

        match Arc::get_mut(&mut self.db).map(Database::compact) {
            Some(Ok(true)) => info!("Successfully compacted the database"),
            Some(Ok(false)) => debug!("No further database compaction was possible"),
            Some(Err(err)) => error!(error = ?err, "Database compaction failed: {err:#}"),
            None => warn!("Failed to acquire unique, mutable access to the database to compact it"),
        }

        trace!("Aborting & reaping all signal handling tasks...");
        let mut awaited_sighandlers = self
            .signal_handlers
            .iter_mut()
            .inspect(|sht| sht.abort())
            .collect::<FuturesUnordered<_>>();
        while let Some(join_res) = awaited_sighandlers.next().await {
            match join_res {
                Ok(()) => warn!("Signal handling task was joined successfully (signal caught?)"),
                Err(join_err) if join_err.is_cancelled() => {
                    debug!("Signal handling task has been successfully cancelled")
                }
                Err(join_err) => warn!("Failed to join signal handling task: {join_err:#}"),
            }
        }

        // TODO?

        Ok(())
    }

    /// Allocates the [`FunctionMetadataStore`], and opens the database and all tables (possibly
    /// creating them (but leaving them empty), if any of them does not already exist).
    fn initialize_state(
        db_path: &Path,
    ) -> Result<(Database, impl FunctionMetadataStore<Runtime::FunctionInfo>), Error> {
        let store = {
            #[cfg(feature = "fmd-store-dash")]
            {
                crate::metadata::DashMapStore::default()
            }
            #[cfg(not(feature = "fmd-store-dash"))]
            {
                crate::metadata::StdHashMapFmdStore::default()
            }
        };

        let db = Database::create(db_path).map_err(|err| Error::OrchestratorInit {
            msg: format!("failed to open database at '{}'", db_path.display()),
            source: Box::new(err),
        })?;
        let wtxn = db.begin_write().map_err(|err| Error::OrchestratorInit {
            msg: String::from("failed to begin write transaction"),
            source: Box::new(err),
        })?;
        {
            let _tblf = wtxn
                .open_table(Tables::<Runtime>::FUNCTIONS)
                .map_err(|err| Error::OrchestratorInit {
                    msg: "failed to open/create table FUNCTIONS".into(),
                    source: Box::new(err),
                })?;
            let _tbls = wtxn
                .open_table(Tables::<Runtime>::SNAPSHOTS)
                .map_err(|err| Error::OrchestratorInit {
                    msg: "failed to open/create table SNAPSHOTS".into(),
                    source: Box::new(err),
                })?;
            let _tblspf = wtxn
                .open_multimap_table(Tables::<Runtime>::SNAPS_PER_FUNC)
                .map_err(|err| Error::OrchestratorInit {
                    msg: "failed to open/create table SNAPS_PER_FUNC".into(),
                    source: Box::new(err),
                })?;
        }
        wtxn.commit().map_err(|err| Error::OrchestratorInit {
            msg:
                "failed to commit write txn creating tables FUNCTIONS, SNAPS_PER_FUNC and SNAPSHOTS"
                    .into(),
            source: Box::new(err),
        })?;

        Ok((db, store))
    }
}
