use std::{fmt::Debug, hint::unreachable_unchecked, path::PathBuf, time::Duration};

use enum_map::EnumMap;
use tokio::{
    sync::mpsc::{self, error::SendTimeoutError},
    time::Instant,
};
use tracing::{debug, error, instrument, span, trace, warn, Instrument, Level, Span};
use uuid::Uuid;

use crate::{
    conf::{SnapshotCreationTime, WorkerConfig},
    metadata::{FunctionInfo, SandboxStats},
    metrics::{Nanoseconds, Timing},
    network,
    request::HEADER_KEY_SOURCE_TIMESTAMP_NS,
    sbpool::{Cpu, WorkerMemAccounting},
    snapman::SnapshotPaths,
    utils::backoff::FibonacciBackoff,
    worker::{
        error::SpawnError,
        issuer::{
            IssuerResponse, RequestIssuer, HEADER_KEY_HANDLER_DURATION,
            HEADER_KEY_RESPONSE_DURATION,
        },
        messages::{CreateSnapshotResult, PrepareSandboxResult},
        runtime::{DestroySandboxRuntimeError, Runtime, Sandbox},
        types::SandboxState,
        ControlMessage, Error, Invocation, OutChannels, OutboundMessage, Result, SandboxStateRef,
        WorkerHandle, WorkerId,
    },
    FunctionId, InvocationId, Request, Response, SandboxId,
};

/// Request identity retained before `Req` is moved into the [`RequestIssuer`].
///
/// Used only to construct a response when request issuing fails or times out.
#[derive(Debug)]
struct IssuerErrorResponseContext {
    /// Original invocation ID for the failure response.
    invocation_id: InvocationId,
    /// Original function ID for the failure response.
    function_id: FunctionId,
    /// Source timestamp propagated for latency accounting.
    source_timestamp_ns: Option<::compact_str::CompactString>,
}

#[derive(Debug)]
pub(crate) struct Worker<Rt, Req, Resp, Issuer, NetResource>
where
    Rt: Runtime,
    Req: Request,
    Issuer: RequestIssuer<Req, Resp>,
    NetResource: network::Resource,
{
    id: WorkerId,
    function_id: FunctionId,
    snap_creation_time: SnapshotCreationTime,
    control_rx: mpsc::Receiver<ControlMessage>,
    invocation_rx: mpsc::Receiver<Invocation<Req>>,
    to: OutChannels<Resp, Rt::Sandbox, NetResource>,

    /// As long as a `Worker` is alive, it owns __one__ instance of <code>[Rt]::[Sandbox]</code>
    /// (e.g., in the case of firecracker-containerd, a [`Vm`] and its related metadata and
    /// resources, such as a [`TapDevice`]).
    /// The <code>[Rt]::[Sandbox]</code> goes through certain [`SandboxState`]s throughout its
    /// lifecycle.
    ///
    /// When the `Worker` dies, the ownership of its <code>[Rt]::[Sandbox]</code> passes back to
    /// the [`SandboxPool`] (which had assigned this <code>[Rt]::[Sandbox]</code> to the `Worker`
    /// in the first place).
    ///
    /// [Rt]: crate::worker::Runtime
    /// [`TapDevice`]: crate::network::TapDevice
    /// [`Vm`]: ::firecracker_containerd_client::Vm
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    sandbox_state: SandboxState<Rt::Sandbox>,

    /// Buffer for [`Timing`]s related to the Function invocation that is currently handled.
    /// The buffer is cleared at the end of each invocation.
    invocation_timings: EnumMap<Timing, Nanoseconds>,

    /// The duration a [`Worker`] may wait for [`RequestIssuer::issue_request`]
    /// to return before timing out.
    issuer_timeout: Duration,

    //config: WorkerConfig<Rt::Config>,
    runtime: Rt,
    issuer: Issuer,
}

impl<Rt, Req, Resp, Issuer, NetResource> Worker<Rt, Req, Resp, Issuer, NetResource>
where
    Rt: Runtime<NetResource = NetResource>,
    Rt::Sandbox: Debug,
    Req: Request,
    Resp: Response,
    Issuer: RequestIssuer<Req, Resp>,
    NetResource: network::Resource,
{
    /// Invocation dispatch is effectively single-slot; buffering more requests per Worker would
    /// weaken the Pool-side FSM contracts.
    const INVOCATION_QUEUE_SIZE: usize = 1;
    /// Control-plane requests may queue while an invocation is in flight.
    const CONTROL_QUEUE_SIZE: usize = 4;

    #[instrument(
        level = Level::TRACE,
        skip_all,
        fields(function_id = ?function_info.id(), sandbox = ?sandbox),
    )]
    pub(crate) fn spawn(
        function_info: &Rt::FunctionInfo,
        config: &WorkerConfig<Rt::Config>,
        to_channels: OutChannels<Resp, Rt::Sandbox, NetResource>,
        sandbox: Option<Rt::Sandbox>,
        mem_accounting: WorkerMemAccounting,
    ) -> ::std::result::Result<WorkerHandle<Rt::Sandbox, Req>, SpawnError<Rt::Sandbox>> {
        let id = Uuid::new_v4();

        let (tx_control, control_rx) = mpsc::channel(Self::CONTROL_QUEUE_SIZE);
        let (tx_invocations, invocation_rx) = mpsc::channel(Self::INVOCATION_QUEUE_SIZE);

        let runtime = match Rt::new(&config.runtime, function_info, sandbox.as_ref()) {
            Ok(rt) => rt,
            Err(err) => {
                error!(error = ?err, "failed to construct new runtime instance");
                return Err(SpawnError {
                    err: Error::Init {
                        dscr: String::from("runtime construction").into_boxed_str(),
                        source: err,
                    },
                    sandbox,
                });
            }
        };
        let issuer = match Issuer::new() {
            Ok(iss) => iss,
            Err(err) => {
                error!(error = ?err, "failed to construct new request issuer");
                return Err(SpawnError {
                    err: Error::Init {
                        dscr: String::from("request issuer construction").into_boxed_str(),
                        source: err,
                    },
                    sandbox,
                });
            }
        };
        let sandbox_id = sandbox.as_ref().map(|sb| sb.id().into());
        let mut worker = Self {
            id,
            function_id: function_info.id().clone(),
            snap_creation_time: config.snapshot_creation_time,
            control_rx,
            invocation_rx,
            to: to_channels,
            sandbox_state: sandbox.into(),
            invocation_timings: EnumMap::default(),
            issuer_timeout: config.issuer_timeout,
            //config: config.clone(),
            runtime,
            issuer,
        };
        let handle = ::tokio::spawn(async move { worker.run().await });

        Ok(WorkerHandle::new(
            id,
            function_info.id().clone(),
            sandbox_id,
            mem_accounting,
            tx_invocations,
            tx_control,
            handle,
        ))
    }

    #[instrument(
        level = Level::INFO,
        skip_all,
        fields(
            worker_id = ?self.id,
            function_id = self.function_id.as_str(),
            sandbox_id = ::tracing::field::Empty,
        )
    )]
    async fn run(&mut self) -> super::WorkerExit<Rt::Sandbox> {
        if let Some(sandbox_id) = self.sandbox_state.id() {
            Span::current().record("sandbox_id", sandbox_id);
        }

        // Initialize the runtime
        match self.runtime.init().await {
            Ok(()) => {}
            Err(err) => {
                error!(error = ?err, "Failed to initialize runtime instance: {err:#}");

                // Notify Pool to reap me, so that my Sandbox, if any, is not leaked.
                self.send_reap_me().await;

                return super::WorkerExit {
                    result: Err(Error::Init {
                        dscr: String::from("runtime initialization").into_boxed_str(),
                        source: err,
                    }),
                    sandbox: self.sandbox_state.take().into(),
                };
            }
        };

        let mut exit_status = Ok(());
        loop {
            // Reaching this point (i.e., entering the loop), my (a Worker's) SandboxState can be:
            // - Snapshot: the first time we ever enter this loop, if Rt::Sandbox was provided by
            //   Pool
            // - Nonexistent: the first time we ever enter this loop, if no Rt::Sandbox was
            //   provided
            // - Paused: every time we enter the loop except the first
            assert!(
                !matches!(
                    self.sandbox_state,
                    SandboxState::Running(_) | SandboxState::Trashed(_)
                ),
                "should never be in SandboxState::{{Running|Trashed}} when entering the main loop"
            );
            ::tokio::select! {
                biased;
                // Prioritize invocations over ctrl-op messages: By the time an invocation reaches
                // this queue, Pool should have already marked the Worker as _Active_, allocated
                // its resources (cpuset), and handed out the RunningSlot for that invocation.
                // Processing control first would result in Pool observing the control completion
                // first, which would lead to wrong state transition or resource cleanup for this
                // _Active_ Worker, whose dispatched invocation may even have not started yet.

                // For now, loss of either Pool ownership or ability to dispatch
                // invocation requests is treated as fatal for the Worker.

                invoc = self.invocation_rx.recv() => {
                    let Some(invoc) = invoc else {
                        warn!("Data-plane channel is closed!");
                        break;
                    };

                    if let Err(err) = self.handle_request(invoc).await {
                        error!(error = ?err, "Failed to handle request: {err:#}");
                        if self.try_handle_failure(&err).await.is_err() {
                            // - Reaching this, we still need to contact Pool to reap
                            //   us, but we should also communicate the error.
                            // - Pool should have us classified us as _Active_ now.
                            exit_status = Err(err);
                        }
                        break;
                    }
                }

                ctrl_msg = self.control_rx.recv() => {
                    let Some(ctrl_msg) = ctrl_msg else {
                        warn!("Control-plane channel is closed!");
                        break;
                    };
                    match ctrl_msg {
                        msg @ ControlMessage::ShutDown  => {
                            trace!(?msg);
                            if let Err(err) = self.shutdown().await {
                                // On failure to shut the Sandbox down (except when
                                // "ttrpc: closed"), set the exit status to Err to make sure:
                                // (i) no uncaching is attempted,
                                // (ii) the Sandbox will not be reused in the future.
                                exit_status = Err(err);
                            }
                            break;
                        }
                        msg @ ControlMessage::DestroySandbox  => {
                            trace!(?msg);
                            if let Err(err) = self.destroy_sandbox().await {
                                // On failure to destroy the Sandbox down,
                                // set the exit status to Err to make sure:
                                // (i) no uncaching is attempted,
                                // (ii) the Sandbox will not be reused in the future.
                                exit_status = Err(err);
                            }
                            break;
                        }
                        ControlMessage::PrepareSandbox { cpuset } => {
                            if let Err(err) = self.handle_prepare_sandbox_request(cpuset).await {
                                error!(
                                    error = ?err,
                                    "Failed to prepare Sandbox upon Pool's request: {err:#}"
                                );
                                if self.try_handle_failure(&err).await.is_err() {
                                    // - Reaching this, we still need to contact Pool to
                                    //   reap us, but we should also communicate the error.
                                    // - Pool should have us classified us as _Active_ now.
                                    exit_status = Err(err);
                                }
                                break;
                            }
                        }
                        ControlMessage::CreateSnapshot { sandbox_id } => {
                            self.handle_snapshot_request(sandbox_id).await
                        }
                    }
                }
            }

            // If the Sandbox was just created, we must be missing "sandbox_id" field on
            // that first request. Reaching here means that we now have a Sandbox, so
            // let's include the associated field from now on, for subsequent requests.
            if !Span::current().has_field("sandbox_id")
                && let Some(sandbox_id) = self.sandbox_state.id()
            {
                Span::current().record("sandbox_id", sandbox_id);
            }
        }
        trace!("Exited run loop");

        // A Worker is normally _Dying_ when sending a ReapMe to SandboxPool, unless there has
        // been some error while handling the request: in this case the Worker is _Active_.
        self.send_reap_me().await;

        match exit_status {
            Ok(()) => {
                // NOTE: Reaching this point, make sure `SandboxState::Snapshot` (for snapshotted
                // sandboxes) and `SandboxState::Nonexistent` (either for non-snapshotted sandboxes
                // or on creation failures) are the only two possible sandbox states.
                self.uncache_sandbox().await;
                super::WorkerExit {
                    result: Ok(()),
                    sandbox: self.sandbox_state.take().into(),
                }
            }
            Err(err) => super::WorkerExit {
                result: Err(err),
                sandbox: self.sandbox_state.take().into(),
            },
        }
    }

    #[instrument(
        level = Level::INFO, skip_all, fields(invocation_id = req.invocation_id().as_ref()),
    )]
    async fn handle_request(
        &mut self,
        Invocation {
            req,
            mut running_slot,
            cpuset,
        }: Invocation<Req>,
    ) -> Result<()> {
        let invocation_id = InvocationId::from(req.invocation_id());

        // Clear the timings buffer, to make sure no earlier on-demand snapshot creations are
        // counted against this invocation.
        self.invocation_timings.clear();

        // If a valid timestamp from the RequestSource is present in Request's metadata, use it to
        // calc & store the duration that the Request remained in queue before acquiring the slot:
        if let Some(Ok(source_ts)) = req
            .metadata_map()
            .get(HEADER_KEY_SOURCE_TIMESTAMP_NS)
            .map(|ts| ts.parse())
        {
            self.invocation_timings[Timing::Queued] = running_slot
                .acquired_since_epoch()
                .saturating_sub(source_ts);
        }

        // Attempt to handle the request while holding the semaphore...
        let result = self.handle_with_slot(req, cpuset).await;
        // ...but always release the semaphore...
        running_slot.release();

        // Send all accumulated timings for this invocation and clear the buffer
        self.invocation_timings[Timing::RunningSlot] = running_slot.elapsed().as_nanos() as _;
        self.to
            .timings
            .store_all(
                self.function_id.clone(),
                self.sandbox_state.id(),
                invocation_id,
                self.invocation_timings,
            )
            .await;

        // ...and now check the result of the request handling attempt
        result?; // NOTE: We do not send `NeedWork` when request handling has failed

        // `NeedWork` marks the end of the invocation only. Pool decides whether this leads
        // to idling, continued Active state for control work, or shutdown.
        self.send_need_work().await;

        Ok(())
    }

    async fn handle_with_slot(&mut self, req: Req, cpuset: Cpu) -> Result<()> {
        let start_state = *self.sandbox_state.as_ref();

        // Prepare the sandbox (create, load or just resume it, depending on my state)
        self.prepare_sandbox(cpuset).await.map_err(|err| {
            error!(error = ?err, "Failed to prepare the sandbox: {err:#}");
            Error::PrepareSandbox(Box::new(err))
        })?; // FIXME: error handling?

        // If we just created the Sandbox, we'll be missing "sandbox_id" field from the grandparent
        // span. This is taken care of for subsequent requests in methods higher up the call stack,
        // but not for this request. So let's create a span here to include "sandbox_id" for the
        // case of the first Request of Sandbox-creating Worker.
        let sandbox_creator_span = if let SandboxStateRef::Nonexistent = start_state {
            if let Some(sandbox_id) = self.sandbox_state.id() {
                span!(Level::INFO, "sandbox_creator", ?sandbox_id)
            } else {
                Span::none()
            }
        } else {
            Span::none()
        };

        // Wrapping the rest of the logic in a separate async method just to allow optionally
        // instrumenting it with the above (optional) span without impairing readability.
        self._handle_with_sandbox(req, start_state)
            .instrument(sandbox_creator_span)
            .await
    }

    async fn _handle_with_sandbox(&mut self, req: Req, start_state: SandboxStateRef) -> Result<()> {
        // When `SnapshotCreationTime::OnCreationBeforeInvoc`, newly created sandboxes have to
        // do the `Running -> Paused -> <create_snapshot> -> Running` dance before handling
        // their first invocation. This obviously adds latency right on the critical path.
        // NOTE: Errors during Sandbox state transitions lead to failure of the invocation
        // altogether, and bubble up to the main event loop. However, a failure in creating
        // the snapshot itself is non-fatal.
        if start_state == SandboxStateRef::Nonexistent
            && self.snap_creation_time == SnapshotCreationTime::OnCreationBeforeInvoc
        {
            let SandboxState::Running(ref sandbox) = self.sandbox_state else {
                error!(sandbox_state = ?self.sandbox_state, "BUG: Entering _handle_with_sandbox");
                unreachable!("Entering _handle_with_sandbox while not in SandboxState::Running")
            };
            assert!(
                !sandbox.has_snapshot(),
                "Newly created Sandbox should not already have snapshot"
            );

            // Pause the sandbox
            self.pause_sandbox()
                .await
                .inspect_err(|err| error!(error = ?err, "Failed to pause the sandbox: {err:#}"))?;
            // TODO: error handling?
            let SandboxState::Running(sandbox) = self.sandbox_state.take() else {
                error!(sandbox_state = ?self.sandbox_state, "BUG: Entering _handle_with_sandbox");
                unreachable!("Entering _handle_with_sandbox while not in SandboxState::Running")
            };
            // NOTE: Running -> Paused
            self.sandbox_state = SandboxState::Paused(sandbox);

            // Create the snapshot
            if let Err(err) = self.create_snapshot().await {
                error!(error = ?err, "Failed to create snapshot: {err:#}");
            }

            // Resume the sandbox --- no need to CPU-pin again, as nothing should have changed
            self.resume_sandbox(None).await.inspect_err(
                |err| error!(error = ?err, "Failed to resume Sandbox after snapshot: {err:#}"),
            )?;
            let SandboxState::Paused(sandbox) = self.sandbox_state.take() else {
                unreachable!("Just transitioned to SandboxState::Paused for snapshotting")
            };
            // NOTE: Paused -> Running
            self.sandbox_state = SandboxState::Running(sandbox);
        }

        // Spawn PerformanceMonitor and wait until it's ready
        let perfmon_started = self
            .spawn_performance_monitor(start_state)
            .await
            .inspect_err(|err| error!(error = ?err, "Failed to spawn PerformanceMonitor: {err:#}"))
            .is_ok();

        let err_resp_ctx = IssuerErrorResponseContext {
            invocation_id: InvocationId::from(req.invocation_id()),
            function_id: FunctionId::from(req.function_id()),
            source_timestamp_ns: req
                .metadata_map()
                .get(HEADER_KEY_SOURCE_TIMESTAMP_NS)
                .map(Into::into),
        };

        // Attempt to issue the Function request using RequestIssuer
        let issue_result = self.issue_request(req, start_state).await;

        // Prepare a response for ResponseSink, regardless of the result
        let (maybe_resp, issuer_dur, mut maybe_err) = match issue_result {
            Ok((resp, issuer_dur)) => (Some(resp), Some(issuer_dur), None),
            Err(err @ Error::HandleFunctionRequest(_)) => {
                let resp = self
                    .make_issuer_error_response(err_resp_ctx, &err, "request issuer failed")
                    .inspect_err(
                        |err| error!(error = ?err, "Failed to construct error response: {err:#}"),
                    )
                    .ok();
                (resp, None, Some(err))
            }
            Err(err @ Error::IssuerTimeout(_)) => {
                let resp = self
                    .make_issuer_error_response(err_resp_ctx, &err, "request issuer timed out")
                    .inspect_err(
                        |err| error!(error = ?err, "Failed to construct error response: {err:#}"),
                    )
                    .ok();
                (resp, None, Some(err))
            }
            Err(err) => {
                error!("Only Error::{{HandleFunctionRequest,IssuerTimeout}} can be returned; got {err:?}");
                return Err(err);
            }
        };

        // Forward the response to ResponseSink
        if let Some(resp) = maybe_resp
            && let Err(err) = self.forward_response(resp).await
        {
            error!(error = ?err, "Failed to forward Function's response to Sink: {err:#}");
            if maybe_err.is_none() {
                maybe_err = Some(Error::ForwardToSink(Box::new(err)));
            }
        }

        // Stop the PerformanceMonitor, which might also update SnapshotManager's metrics.
        if perfmon_started {
            let _res_todo = self
                .to
                .snapman
                .stop_monitor(self.id, self.function_id.clone(), start_state, issuer_dur)
                .await
                .inspect_err(|err| {
                    error!(error = ?err, "Failure while stopping PerformanceMonitor: {err:#}");
                });
        }

        if let Some(err) = maybe_err {
            return Err(err);
        }

        // Pause the sandbox
        debug_assert!(matches!(self.sandbox_state, SandboxState::Running(_)));
        self.pause_sandbox().await.map_err(|err| {
            error!(error = ?err, "Failed to pause the sandbox: {err:#}");
            err
        })?; // FIXME: error handling?
        let SandboxState::Running(sandbox) = self.sandbox_state.take() else {
            unreachable!()
        };
        // NOTE: Running -> Paused
        self.sandbox_state = SandboxState::Paused(sandbox);

        if start_state == SandboxStateRef::Nonexistent
            && self.snap_creation_time == SnapshotCreationTime::OnCreationAfterInvoc
            && let Err(err) = self.create_snapshot().await
        {
            // A Worker that failed to snapshot its sandbox should be able to remain
            // _Idle_ and handle future requests, and die when its keepalive window ends.
            // Therefore, we do not bubble the error up here; we assume everything is fine,
            // prepared to handle a possible failure of `self.runtime.shutdown_sandbox()`
            // during `Worker::shutdown()`.
            error!(error = ?err, "Failed to create snapshot for the new sandbox: {err:#}");
        }

        Ok(())
    }

    #[cold]
    fn make_issuer_error_response(
        &self,
        ctx: IssuerErrorResponseContext,
        err: &Error,
        message: &'static str,
    ) -> Result<Resp> {
        let mut metadata = ::tonic::metadata::MetadataMap::new();

        if let Some(source_ts) = ctx.source_timestamp_ns.as_deref()
            && let Ok(value) = source_ts.parse()
        {
            metadata.insert(HEADER_KEY_SOURCE_TIMESTAMP_NS, value);
        }

        let status_code = match &err {
            Error::HandleFunctionRequest(_) => ::tonic::Code::Unavailable,
            Error::IssuerTimeout(_) => ::tonic::Code::DeadlineExceeded,
            _ => unreachable!(
                "Only Error::{{HandleFunctionRequest,IssuerTimeout}} can be returned; got {err:?}"
            ),
        };

        Resp::try_from_parts(
            &ctx.invocation_id,
            &ctx.function_id,
            status_code,
            metadata,
            ::prost::bytes::Bytes::from_static(message.as_bytes()),
        )
        .map_err(|err| {
            error!(
                error = ?err,
                ?status_code,
                "Failed to construct issuer error response: {err:#}"
            );
            Error::HandleFunctionRequest(Box::new(err))
        })
    }

    /// Forward the generated [`Response`] to the [`Sink`].
    ///
    /// # Notes
    ///
    /// This is called while holding the Semaphore!
    ///
    /// [`Response`]: crate::response::Response
    /// [`Sink`]: crate::response::Sink
    #[instrument(level = Level::TRACE, skip_all)]
    async fn forward_response(&self, mut resp: Resp) -> Result<()> {
        const MAX_FWD_ATTEMPTS: u64 = 10;

        let mut backoff = FibonacciBackoff::new(Duration::from_millis(25));

        let mut attempts = 0;
        while let Err(err) = self
            .to
            .response_sink
            .send_timeout(resp, backoff.next().expect("never-ending iterator"))
            .await
        {
            attempts += 1;
            debug!(attempts, "failed to forward response to Sink: {err}");
            if attempts >= MAX_FWD_ATTEMPTS {
                return Err(Error::Channel(
                    "forwarding response to Sink over mpsc".into(),
                    format!("Reached maximum number of attempts ({MAX_FWD_ATTEMPTS})").into(),
                ));
            }
            resp = match err {
                SendTimeoutError::Timeout(resp) => resp,
                SendTimeoutError::Closed(_) => {
                    error!("ResponseSink's channel looks closed");
                    // Sink is down; everything is pointless now, burn it to the ground
                    return Err(Error::Channel(
                        "forwarding response to Sink over mpsc".into(),
                        err.to_string().into(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Prepare a sandbox, either by:
    /// - creating it if we are a fresh [`Worker`] with no snapshot provided by [`SandboxPool`];
    /// - loading it from the snapshot provided by [`SandboxPool`];
    /// - resuming it if we were _Idle_ and just became _Active_ by [`SandboxPool`].
    ///
    /// After a successful call, sandbox's state should be [`SandboxState::Running`].
    ///
    /// # Panics
    ///
    /// In case of bugs in internal logic (probably concerning FSM transitions).
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`SandboxState`]: crate::worker::SandboxState
    #[inline]
    async fn prepare_sandbox(&mut self, cpuset: Cpu) -> Result<()> {
        // Let's not `SandboxState::take` yet, so that method calls can access it via`&mut self`:
        let sandbox = match self.sandbox_state {
            SandboxState::Nonexistent => {
                // I am a fresh Worker with no sandbox (nor sandbox snapshot)
                self.create_sandbox(cpuset).await?
            }
            SandboxState::Snapshot(_) => {
                // I am a fresh Worker assigned with a Sandbox snapshot
                self.load_sandbox(cpuset).await?;
                let SandboxState::Running(sandbox) = self.sandbox_state.take() else {
                    unsafe { unreachable_unchecked() }
                };
                sandbox
            }
            SandboxState::Paused(_) => {
                // I am an Idle Worker assigned with a paused Sandbox
                self.resume_sandbox(Some(cpuset)).await?;
                let SandboxState::Paused(sandbox) = self.sandbox_state.take() else {
                    unsafe { unreachable_unchecked() }
                };
                sandbox
            }
            ref s @ SandboxState::Running(_) | ref s @ SandboxState::Trashed(_) => {
                let s = s.as_ref();
                error!("BUG: should not prepare sandbox while in SandboxState::{s:?}");
                unreachable!("BUG: should not prepare sandbox while in SandboxState::{s:?}");
            }
        };
        self.sandbox_state = SandboxState::Running(sandbox);

        // TODO: Pin the sandbox to cores through the runtime at this point?

        Ok(())
    }

    /// # Panics
    ///
    /// This method assumes <code>let [SandboxState::Running]\(_\) = [Self::sandbox_state]</code>
    /// and panics otherwise (i.e., if [`Worker`]'s owned [`Sandbox`] is not already `Running`).
    #[inline]
    async fn spawn_performance_monitor(&self, start_state: SandboxStateRef) -> Result<()> {
        let SandboxState::Running(ref sandbox) = self.sandbox_state else {
            error!(sandbox_state = ?self.sandbox_state, "BUG: Spawning PerformanceMonitor");
            unreachable!("Spawning PerformanceMonitor while not in SandboxState::Running")
        };

        self.to
            .snapman
            .spawn_monitor(self.id, sandbox.monitor_targets(), start_state)
            .await
            .map_err(|err| Error::SnapshotManager {
                msg: "failed to initialize PerformanceMonitor".into(),
                err,
            })
    }

    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    async fn send_sandbox_id(&self, sandbox_id: SandboxId) {
        self.retry_send_to_pool(OutboundMessage::SandboxId {
            worker_id: self.id,
            sandbox_id,
        })
        .await
    }

    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    async fn send_need_work(&self) {
        self.retry_send_to_pool(OutboundMessage::NeedWork {
            worker_id: self.id,
            function_id: self.function_id.clone(),
        })
        .await
    }

    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    async fn send_reap_me(&self) {
        self.retry_send_to_pool(OutboundMessage::ReapMe {
            worker_id: self.id,
            function_id: self.function_id.clone(),
        })
        .await
    }

    /// # Panics
    ///
    /// If <code>[Self::sandbox_state].[id()].[is_none()]</code> (i.e., when
    /// <code>matches!([Self::sandbox_state], [SandboxState::Nonexistent])</code>).
    ///
    /// [id()]: crate::worker::types::SandboxState::id
    /// [is_none()]: std::option::Option::is_none
    #[instrument(level = Level::TRACE, skip_all)]
    async fn send_snapshot_result(
        &self,
        result: ::std::result::Result<(), Error>,
        sandbox_id: SandboxId,
    ) {
        self.retry_send_to_pool(OutboundMessage::SnapshotCreation(Box::new(
            CreateSnapshotResult {
                worker_id: self.id,
                function_id: self.function_id.clone(),
                sandbox_id,
                result: result.map_err(|err| err.to_string().into_boxed_str()),
            },
        )))
        .await
    }

    #[instrument(level = Level::TRACE, skip_all)]
    async fn send_sandbox_prep_result(
        &self,
        result: ::std::result::Result<(SandboxId, bool, SandboxStats), Box<str>>,
    ) {
        self.retry_send_to_pool(OutboundMessage::SandboxPreparation(Box::new(
            PrepareSandboxResult {
                worker_id: self.id,
                function_id: self.function_id.clone(),
                result,
            },
        )))
        .await
    }

    async fn retry_send_to_pool(&self, mut msg: OutboundMessage) {
        const TIMEOUT_DURATION: Duration = Duration::from_millis(1000);

        while let Err(err) = self.to.pool.send_timeout(msg, TIMEOUT_DURATION).await {
            error!(error = ?err, "Failed to contact SandboxPool: {err:#}");
            msg = match err {
                SendTimeoutError::Closed(msg) | SendTimeoutError::Timeout(msg) => msg,
            };
            debug!(?msg, "Retrying to contact SandboxPool...");
        }
    }

    /// # Notes
    ///
    /// I am a fresh Worker with no snapshotted sandbox.
    #[instrument(level = Level::TRACE, skip_all)]
    async fn create_sandbox(&mut self, cpuset: Cpu) -> Result<Rt::Sandbox> {
        let SandboxState::Nonexistent = self.sandbox_state else {
            error!(sandbox_state = ?self.sandbox_state, "BUG: Creating new sandbox");
            unreachable!("Creating new sandbox while not in SandboxState::Nonexistent")
        };

        let setup_net_start = Instant::now();
        let net_rsc = match self.to.netman.allocate().await {
            Ok(rcv) => match rcv.await {
                Ok(Ok(net_rsc)) => Ok(net_rsc),
                Ok(Err(err)) => Err(Error::Network(Box::new(err))),
                Err(err) => Err(Error::Channel(
                    "receiving from NetworkManager over oneshot".into(),
                    Box::new(err),
                )),
            },
            Err(err) => Err(Error::Channel(
                "sending to NetworkManager over mpsc".into(),
                Box::new(err),
            )),
        }?;
        self.invocation_timings[Timing::SetupResources] +=
            setup_net_start.elapsed().as_nanos() as Nanoseconds;

        let create_sandbox_start = Instant::now();
        let mut sandbox = self
            .runtime
            .create_sandbox(&self.function_id, net_rsc, &mut self.invocation_timings)
            .await
            .map_err(Error::CreateSandbox)?;
        let create_sandbox_dur = create_sandbox_start.elapsed().as_nanos() as _;
        // If Runtime has not been co-operative wrt timings, better track _something_ than nothing:
        if self.invocation_timings[Timing::CreateSandbox] == 0 {
            self.invocation_timings[Timing::CreateSandbox] = create_sandbox_dur;
        }

        // FIXME: in case of failure setting up the resources, make sure the sandbox is cleaned up:
        let setup_cpu_start = Instant::now();
        self.runtime
            .setup_cpuset(&mut sandbox, cpuset)
            .await
            .map_err(Error::SetupCpuset)?;
        self.invocation_timings[Timing::SetupResources] +=
            setup_cpu_start.elapsed().as_nanos() as Nanoseconds;

        // Notify SandboxPool of our newly created Sandbox's ID.
        self.send_sandbox_id(sandbox.id().into()).await;

        Ok(sandbox)
    }

    /// # Notes
    ///
    /// - Always called by a fresh Worker assigned with a snapshotted [`Sandbox`].
    /// - Assumes initially in [`SandboxState::Snapshot`].
    /// - On success, always in [`SandboxState::Running`].
    ///
    /// # Panics
    ///
    /// This method assumes <code>let [SandboxState::Snapshot]\(_\) = [Self::sandbox_state]</code>
    /// and panics otherwise (i.e., if `Self`'s owned [`Sandbox`] is not in `Snapshot` state).
    #[instrument(level = Level::TRACE, skip_all)]
    async fn load_sandbox(&mut self, cpuset: Cpu) -> Result<()> {
        let SandboxState::Snapshot(ref mut sandbox) = self.sandbox_state else {
            error!(sandbox_state = ?self.sandbox_state, "BUG: Loading sandbox snapshot");
            unreachable!("Loading sandbox while not in SandboxState::Snapshot")
        };

        let load_sandbox_start = Instant::now();
        self.runtime
            .load_sandbox(sandbox)
            .await
            .map_err(Error::LoadSandbox)?;
        self.invocation_timings[Timing::LoadSandbox] = load_sandbox_start.elapsed().as_nanos() as _;

        sandbox.stats_mut().restorations += 1;

        // From this point on, the Sandbox is no longer snapshot-only. Even if subsequent
        // calls (e.g., CPU setup) fail, failure handling must treat it as a live Running
        // Sandbox rather than returning it as an untouched reusable snapshot.
        let SandboxState::Snapshot(sandbox) = self.sandbox_state.take() else {
            unreachable!("already checked that we are on SandboxState::Snapshot");
        };
        // NOTE: Snapshot -> Running
        self.sandbox_state = SandboxState::Running(sandbox);

        let SandboxState::Running(ref mut sandbox) = self.sandbox_state else {
            unreachable!("just transitioned Snapshot -> Running");
        };

        let setup_cpu_start = Instant::now();
        // If this fails, `try_handle_failure(Error::PrepareSandbox(_))` sees `Running`
        // and attempts to quiesce the Sandbox before returning or destroying it.
        self.runtime
            .setup_cpuset(sandbox, cpuset)
            .await
            .map_err(Error::SetupCpuset)?;
        self.invocation_timings[Timing::SetupResources] = setup_cpu_start.elapsed().as_nanos() as _;

        Ok(())
    }

    /// # Panics
    ///
    /// This method assumes <code>let [SandboxState::Running]\(_\) = [Self::sandbox_state]</code>
    /// and panics otherwise (i.e., if [`Worker`]'s owned [`Sandbox`] is not already `Running`).
    #[instrument(level = Level::TRACE, skip_all)]
    async fn pause_sandbox(&mut self) -> Result<()> {
        let SandboxState::Running(ref mut sandbox) = self.sandbox_state else {
            error!(sandbox_state = ?self.sandbox_state, "BUG: Pausing sandbox");
            unreachable!("Pausing sandbox while not in SandboxState::Running")
        };

        self.runtime
            .pause_sandbox(sandbox)
            .await
            .map_err(Error::PauseSandbox)
    }

    /// # Notes
    ///
    /// I am an Idle Worker handling a paused sandbox.
    ///
    /// # Panics
    ///
    /// This method assumes <code>let [SandboxState::Paused]\(_\) = [Self::sandbox_state]</code>
    /// and panics otherwise (i.e., if [`Worker`]'s owned [`Sandbox`] is not already `Paused`).
    #[instrument(level = Level::TRACE, skip_all)]
    async fn resume_sandbox(&mut self, cpuset: Option<Cpu>) -> Result<()> {
        let SandboxState::Paused(ref mut sandbox) = self.sandbox_state else {
            error!(sandbox_state = ?self.sandbox_state, "BUG: Resuming sandbox");
            unreachable!("Resuming sandbox while not in SandboxState::Paused")
        };

        let resume_sandbox_start = Instant::now();
        self.runtime
            .resume_sandbox(sandbox)
            .await
            .map_err(Error::ResumeSandbox)?;
        self.invocation_timings[Timing::ResumeSandbox] +=
            resume_sandbox_start.elapsed().as_nanos() as Nanoseconds;

        if let Some(cpuset) = cpuset {
            let setup_cpu_start = Instant::now();
            // If this fails, `try_handle_failure(Error::PrepareSandbox(_))` sees `Paused`
            // and attempts to unload or destroy the Sandbox according to snapshot state.
            self.runtime
                .setup_cpuset(sandbox, cpuset)
                .await
                .map_err(Error::SetupCpuset)?;
            self.invocation_timings[Timing::SetupResources] +=
                setup_cpu_start.elapsed().as_nanos() as Nanoseconds;
        }

        Ok(())
    }

    #[instrument(level = Level::TRACE, skip_all)]
    async fn rt_create_snapshot(
        &mut self,
        state_file_path: PathBuf,
        memory_file_path: PathBuf,
    ) -> Result<()> {
        let SandboxState::Paused(ref mut sandbox) = self.sandbox_state else {
            error!(sandbox_state = ?self.sandbox_state, "BUG: Creating sandbox snapshot");
            unreachable!("Creating sandbox snapshot while not in SandboxState::Paused")
        };

        self.runtime
            .create_snapshot(sandbox, state_file_path, memory_file_path)
            .await
            .map_err(Error::CreateSnapshot)
    }

    /// # Panics
    ///
    /// This method assumes <code>let [SandboxState::Paused]\(_\) == [Self::sandbox_state]</code>
    /// and panics otherwise (i.e., if [`Worker`]'s owned [`Sandbox`] is not already `Paused`).
    ///
    /// # Errors
    ///
    /// - In case of a [`SnapshotManager`]-related failure:
    ///   * of the [`PlacementAlgorithm`] to make a decision, propagated by the [`SnapshotManager`];
    ///   * contacting the [`SnapshotManager`] to query snapshot file paths.
    /// - In case of the underlying [`Runtime`]'s failure to create a snapshot of the [`Sandbox`].
    ///
    /// ## Note
    ///
    /// 1. A [`Worker`] that failed to snapshot its [`Sandbox`] should be able to remain
    ///    _Idle_ and handle future requests, and die when its keep-alive window ends.
    ///    Therefore, returning an error here does not _have to_ be fatal; we can assume
    ///    everything is fine, and be prepared to handle a potential failure of
    ///    <code>[Self::runtime].[shutdown_sandbox()]</code> in the near future, during
    ///    [`Self::shutdown`].
    /// 2. Failures notifying [`SnapshotManager`] of newly created snapshots are logged
    ///    but otherwise not handled (e.g., neither is this method interrupted, nor are
    ///    such errors returned).
    ///
    /// [`PlacementAlgorithm`]: crate::snapman::placement::PlacementAlgorithm
    /// [`SnapshotManager`]: crate::snapman::SnapshotManager
    /// [shutdown_sandbox()]: crate::worker::runtime::Runtime::shutdown_sandbox
    #[instrument(level = Level::TRACE, skip_all)]
    async fn create_snapshot(&mut self) -> Result<()> {
        let SandboxState::Paused(ref sandbox) = self.sandbox_state else {
            error!(sandbox_state = ?self.sandbox_state, "BUG: Creating sandbox snapshot");
            unreachable!("Creating sandbox snapshot while not in SandboxState::Paused")
        };

        let create_snap_start = Instant::now();

        // Query SnapshotManager for snapshot file paths
        let SnapshotPaths {
            mut state,
            mut memory,
        } = match self.to.snapman.query_paths(self.function_id.clone()).await {
            Ok(Some(paths)) => paths,
            Ok(None) => {
                trace!("SnapshotManager indicated to skip sandbox snapshot creation");
                return Ok(());
            }
            Err(err) => {
                error!(error = ?err, "Failed to query SnapshotManager for paths: {err:#}");
                return Err(Error::SnapshotManager {
                    msg: "failed to query paths".into(),
                    err,
                });
            }
        };

        state.push(format!("{}.state", sandbox.id()));
        memory.push(format!("{}.memory", sandbox.id()));

        if let Err(err) = self.rt_create_snapshot(state, memory).await {
            error!(error = ?err, "Failed to create snapshot for the new sandbox: {err:#}");
            return Err(err);
        } else {
            if let Err(err) = self.snapman_notify_creation().await {
                error!(error = ?err, "Failed to notify SnapMan of new snapshot: {err:#}")
            }

            self.invocation_timings[Timing::CreateSnapshot] =
                create_snap_start.elapsed().as_nanos() as _;
        }

        Ok(())
    }

    /// Notify [`SnapshotManager`] that the currently paused [`Sandbox`] has
    /// just created a snapshot.
    ///
    /// This is used after successful snapshot creation so that
    /// [`SnapshotManager`] can persist the new [`SnapshotState`] asynchronously.
    ///
    /// # Panics
    ///
    /// If currently not in [`SandboxState::Paused`].
    ///
    /// # Errors
    ///
    /// On failure to:
    /// - create the [`SnapshotState`];
    /// - communicate with the [`SnapshotManager`].
    ///
    /// [`SnapshotManager`]: crate::snapman::SnapshotManager
    /// [`SnapshotState`]: crate::worker::runtime::SnapshotState
    #[instrument(level = Level::TRACE, skip_all)]
    async fn snapman_notify_creation(&self) -> Result<()> {
        let SandboxState::Paused(ref sandbox) = self.sandbox_state else {
            error!(sandbox_state = ?self.sandbox_state, "BUG: Notifying SnapshotManager");
            unreachable!("Notifying SnapshotManager while not in SandboxState::Paused")
        };

        match sandbox.snapshot_state() {
            None => Ok(()),
            Some(Ok(state)) => self
                .to
                .snapman
                .notify_new_snapshot(self.function_id.clone(), Box::new(state))
                .await
                .map_err(|err| Error::SnapshotManager {
                    msg: "failed to notify SnapshotManager of new snapshot".into(),
                    err,
                }),
            Some(Err(err)) => {
                error!(error = ?err, "Failed to create sandbox snapshot state: {err:#}");
                Err(Error::CreateState(err))
            }
        }
    }

    /// Notify [`SnapshotManager`] that a previously persisted snapshot has just
    /// been destroyed.
    ///
    /// This is used after successful runtime-side destruction of a snapshotted
    /// [`Sandbox`], so that [`SnapshotManager`] can remove its persisted
    /// metadata (asynchronously).
    ///
    /// [`SnapshotManager`]: crate::snapman::SnapshotManager
    #[instrument(level = Level::TRACE, skip_all)]
    async fn snapman_notify_removal(&self, sandbox_id: SandboxId) -> Result<()> {
        self.to
            .snapman
            .notify_snapshot_removal(self.function_id.clone(), sandbox_id)
            .await
            .map_err(|err| Error::SnapshotManager {
                msg: "failed to notify SnapshotManager of snapshot destruction".into(),
                err,
            })
    }

    /// TODO: doc
    ///
    /// # Old Notes
    ///
    /// - put together a Function invocation request from `Request`'s payload
    /// - extract Function response's payload into a `Response`
    /// - disambiguate Function errors (e.g., status codes, etc), which should be returned to the
    ///   caller, from snaplace errors
    async fn issue_request(
        &mut self,
        req: Req,
        start_state: SandboxStateRef,
    ) -> Result<(Resp, Duration)> {
        let SandboxState::Running(ref mut sandbox) = self.sandbox_state else {
            error!(sandbox_state = ?self.sandbox_state, "BUG: Issuing function request");
            unreachable!("Issuing function request while not in SandboxState::Running")
        };

        // Attempt to retrieve RequestSource's timestamp from Request's headers
        let src_ts = req
            .metadata_map()
            .get(HEADER_KEY_SOURCE_TIMESTAMP_NS)
            .cloned();

        let issue_start = Instant::now();
        let IssuerResponse {
            mut resp,
            connection_duration,
            invocation_duration,
        } = match ::tokio::time::timeout(
            self.issuer_timeout,
            self.issuer
                .issue_request(sandbox.ip_addr(), start_state, req),
        )
        .await
        {
            Ok(Ok(resp)) => resp,
            Ok(Err(err)) => {
                error!(error = ?err, "Failed to handle Function request: {err:#}");
                self.invocation_timings[Timing::Issuer] = issue_start.elapsed().as_nanos() as _;
                return Err(Error::HandleFunctionRequest(err));
            }
            Err(timerr) => {
                error!(
                    error = ?timerr, timeout = ?self.issuer_timeout,
                    "Timed out awaiting request issuing: {timerr:#}"
                );
                self.invocation_timings[Timing::Issuer] = issue_start.elapsed().as_nanos() as _;
                return Err(Error::IssuerTimeout(self.issuer_timeout));
            }
        };
        let issuer_dur = issue_start.elapsed();

        self.invocation_timings[Timing::IssuerConnection] = connection_duration.as_nanos() as _;
        self.invocation_timings[Timing::IssuerInvocation] = invocation_duration.as_nanos() as _;
        self.invocation_timings[Timing::Issuer] = issuer_dur.as_nanos() as _;

        if let Some(handler_dur) = resp
            .metadata_map()
            .get(HEADER_KEY_HANDLER_DURATION)
            .and_then(|s| s.parse().ok().map(Duration::from_nanos))
        {
            self.invocation_timings[Timing::SandboxHandler] = handler_dur.as_nanos() as _;
        }
        if let Some(response_dur) = resp
            .metadata_map()
            .get(HEADER_KEY_RESPONSE_DURATION)
            .and_then(|s| s.parse().ok().map(Duration::from_nanos))
        {
            self.invocation_timings[Timing::SandboxResponse] = response_dur.as_nanos() as _;
        }

        // If we did find a timestamp earlier, add it again as a header
        if let Some(src_ts) = src_ts {
            let _ = resp
                .metadata_map()
                .insert(HEADER_KEY_SOURCE_TIMESTAMP_NS.to_owned(), src_ts);
        }

        sandbox.stats_mut().invocations += 1;

        Ok((resp, issuer_dur))
    }

    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    async fn uncache_sandbox(&mut self) {
        // Immediately before returned _successfully_ back to SandboxPool, `SandboxState::Snapshot`
        // is the only valid state for an _existing_ sandbox which has been snapshotted, and
        // `SandboxState::Nonexistent` for a sandbox that either (1) was not even created at all
        // (i.e., creation failure), or (2) was not snapshotted (and therefore must have been
        // destroyed in `self.shutdown()`).
        match self.sandbox_state {
            SandboxState::Snapshot(ref mut sandbox) if sandbox.has_snapshot() => {
                if let Err(err) = self.runtime.uncache_sandbox(sandbox).await {
                    warn!(error = ?err, "Failed to uncache sandbox: {err:#}");
                }
            }
            SandboxState::Nonexistent => {}
            ref invalid_state => {
                error!(sandbox_state = ?invalid_state, "BUG: Uncaching sandbox");
                unreachable!("Uncaching sandbox while in SandboxState::{invalid_state:?}");
            }
        }
    }

    #[instrument(
        level = Level::DEBUG, skip(self), fields(sandbox.state = ?self.sandbox_state.as_ref())
    )]
    async fn shutdown(&mut self) -> Result<()> {
        // NOTE(ckatsak):
        // - As implemented now, a Worker that did not create a sandbox snapshot
        //   (either by choice or due to failure) may still remain _Idle_. When
        //   that Worker receives `ControlMessage::ShutDown` (e.g., when its
        //   keep-alive timer goes off), it reaches this point.
        // - Normally, the Sandbox should be in Paused state before calling this.
        debug_assert!(
            matches!(self.sandbox_state, SandboxState::Paused(_)),
            "Sandbox should be in Paused state upon shutdown"
        );
        // - If the sandbox does not have a snapshot, there is no point in shutting
        //   it down instead of destroying it. There is no reason why we should wait
        //   for SandboxPool to issue its destruction: shutting it down should be
        //   destruction unless it has snapshots, so we may as well destroy it now
        //   ourselves, and return `Nonexistent` to SandboxPool.
        match &self.sandbox_state {
            SandboxState::Paused(sandbox) if sandbox.has_snapshot() => {
                // typical case of keep-alive timeout for an _Idle_ Worker W/ snapshot
                let SandboxState::Paused(mut sandbox) = self.sandbox_state.take() else {
                    unreachable!("already asserted that we are on SandboxState::Paused");
                };
                if let Err(err) = self.runtime.shutdown_sandbox(&mut sandbox).await {
                    error!(error = ?err, "Error shutting the sandbox down: {err:#}");
                    // NOTE: Paused -> Trashed
                    self.sandbox_state = SandboxState::Trashed(sandbox);
                    return Err(Error::ShutdownSandbox(err));
                }
                // NOTE: Paused -> Snapshot
                self.sandbox_state = SandboxState::Snapshot(sandbox);
            }
            SandboxState::Paused(_) => {
                if let Err(err) = self.destroy_sandbox().await {
                    error!(error = ?err, "Failed to destroy sandbox without snapshot: {err:#}");
                    return Err(err);
                }
                // NOTE: Paused -> Nonexistent (already transited in self.destroy_sandbox)
            }
            _ => {
                // This method should never have been called while not in one
                // of `SandboxState::Paused | SandboxState::Snapshot` (and
                // under the specific conditions shown above).
                warn!(sandbox = ?self.sandbox_state, "Unexpected shut down?");
            }
        }
        Ok(())
    }

    /// # Returns
    ///
    /// - [`Ok`] if now everything looks good enough to gracefully hand the [`Sandbox`] back
    ///   to [`SandboxPool`], ready to be reused in the future. In this case, [`SandboxPool`]
    ///   learns nothing about the failure.
    /// - [`Err`] if the [`Sandbox`] (if any) should be handed back to [`SandboxPool`]
    ///   inside [`Err`] alongside an [`Error`]. In this case, what should [`SandboxPool`]
    ///   do with it? I may refer to such a [`Sandbox`] as "leaked" or "trashed" or similar.
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    #[instrument(level = Level::INFO, skip(self))]
    #[inline(never)]
    #[cold]
    async fn try_handle_failure(&mut self, err: &Error) -> ::std::result::Result<(), ()> {
        match &err {
            Error::PrepareSandbox(_) => {
                match self.sandbox_state {
                    SandboxState::Nonexistent => {
                        // Preparation failed before any Sandbox was attached to this Worker.
                        //
                        // This covers failures on the fresh-create path before `create_sandbox()`
                        // returned a Sandbox back into `prepare_sandbox()`. There is no Sandbox
                        // in `self.sandbox_state` for Pool to recover here; leave the state as
                        // `Nonexistent`.
                    }
                    SandboxState::Snapshot(_) => {
                        // Preparation failed while the Worker still owns snapshot-only state.
                        //
                        // In the current load path, `load_sandbox()` leaves the Worker in
                        // `Snapshot` only until `Runtime::load_sandbox()` succeeds. Once
                        // runtime-side load succeeds, `load_sandbox()` immediately transitions
                        // the Worker to `Running` before later setup (e.g., `setup_cpuset()`).
                        //
                        // Therefore, reaching here means the snapshot has not been rehydrated
                        // by the runtime and can be returned to Pool as reusable snapshot state.
                    }
                    SandboxState::Paused(ref mut sandbox) => {
                        // Preparation failed while the Worker is still classified as owning a
                        // paused Sandbox.
                        //
                        // In the current prepare path, this primarily covers resume-side failures:
                        // `prepare_sandbox()` started from `Paused`, called `resume_sandbox(...)`,
                        // and failed before it could promote the Sandbox to `Running`.
                        //
                        // If this Sandbox has snapshot backing, shut/unload it back to `Snapshot`
                        // so Pool can reuse it. If it has no snapshot backing, destroy it and leave
                        // the Worker in `Nonexistent`. If cleanup fails, return `Err(())` so the
                        // final Worker exit reports the Sandbox as trashed rather than reusable.
                        if sandbox.has_snapshot() {
                            // UnloadVM; if it fails, return Err(()) to hand the sb to Pool as leaked
                            let SandboxState::Paused(mut sandbox) = self.sandbox_state.take()
                            else {
                                unreachable!("already checked that we are on SandboxState::Paused");
                            };
                            if let Err(err) = self.runtime.shutdown_sandbox(&mut sandbox).await {
                                error!(error = ?err, sandbox = ?sandbox, "Failed to shutdown sandbox");
                                // NOTE: Paused -> Trashed
                                self.sandbox_state = SandboxState::Trashed(sandbox);
                                return Err(());
                            }
                            // NOTE: Paused -> Snapshot
                            self.sandbox_state = SandboxState::Snapshot(sandbox);
                        } else {
                            if let Err(err) = self.destroy_sandbox().await {
                                error!(error = ?err, "Failed to destroy sandbox: {err:#}");
                                return Err(());
                            }
                            // NOTE: Paused -> Nonexistent/Trashed (transitioned in self.destroy_sandbox)
                        }
                    }
                    SandboxState::Running(_) => {
                        // Preparation failed after the Sandbox had already become runtime-live.
                        //
                        // In the current snapshot-load path, `load_sandbox()` transitions
                        // `Snapshot -> Running` immediately after `Runtime::load_sandbox()`
                        // succeeds, before later setup (e.g., `setup_cpuset()`). Therefore,
                        // reaching here means the Sandbox must not be returned as untouched
                        // snapshot state.
                        //
                        // First pause it. If it has snapshot backing, shut/unload it back to
                        // `Snapshot` for reuse. If it has no snapshot backing, destroy it. If
                        // pause, shutdown, or destruction fails, return `Err(())` so the final
                        // Worker exit reports the Sandbox as trashed.
                        let SandboxState::Running(mut sandbox) = self.sandbox_state.take() else {
                            unreachable!("already checked SandboxState::Running");
                        };
                        if let Err(err) = self.runtime.pause_sandbox(&mut sandbox).await {
                            error!(error = ?err, sandbox = ?sandbox, "Failed to pause sandbox");
                            // NOTE: Running -> Trashed
                            self.sandbox_state = SandboxState::Trashed(sandbox);
                            return Err(());
                        }
                        // (NOTE: conceptually: Running -> Paused)

                        if sandbox.has_snapshot() {
                            if let Err(err) = self.runtime.shutdown_sandbox(&mut sandbox).await {
                                // NOTE: Running -> (Paused ->) Trashed
                                self.sandbox_state = SandboxState::Trashed(sandbox);
                                error!(
                                    error = ?err, sandbox.state = ?self.sandbox_state,
                                    "Failed to shutdown sandbox: {err:#}"
                                );
                                return Err(());
                            }
                            // NOTE: Running -> (Paused ->) Snapshot
                            self.sandbox_state = SandboxState::Snapshot(sandbox);
                        } else {
                            self.sandbox_state = SandboxState::Paused(sandbox);
                            if let Err(err) = self.destroy_sandbox().await {
                                error!(error = ?err, "Failed to destroy sandbox: {err:#}");
                                return Err(());
                            }
                            // NOTE: Paused -> Nonexistent/Trashed (transitioned in self.destroy_sandbox)
                        }
                    }
                    SandboxState::Trashed(ref sandbox) => {
                        // This is supposedly a terminal state; a Trashed sandbox should never
                        // have reached a preparation phase.
                        error!(error = ?err, ?sandbox, "BUG: Error::PrepareSandbox for SandboxState::Trashed");
                        unreachable!(
                            "BUG: Error::PrepareSandbox for SandboxState::Trashed({sandbox:?}): {err:?}"
                        );
                    }
                }
                Ok(())
            }
            Error::HandleFunctionRequest(_) | Error::IssuerTimeout(_) => {
                // - Any retrying mechanism should hold the Semaphore; therefore, we should not
                // retry here, as the Semaphore has been released. This error being bubbled up
                // here this means that we have retried and failed enough.
                // - TODO: For now, failed invocations are silently dropped. This sounds wrong,
                // regardless of whether we should retry or not: there should be a Response for
                // every Request. When proper queuing is implemented, _maybe_ re-queuing should
                // an option (OTOH, signaling the failure to a higher-level distributed scheduler
                // might be better anyway).
                // - At this point, we might:
                // (1) attempt to pause & unload the VM and return it to Pool for later use;
                // (2) destroy the VM and return nothing to Pool (in this case, we'd also have
                //     to contact SnapshotManager to remove the snapshot, if any?).
                assert!(
                    matches!(self.sandbox_state, SandboxState::Running(_)),
                    "Error::{{HandleFunctionRequest,IssuerTimeout}} should be returned only while SandboxState::Running"
                );
                // Let's go with (1) for snapshotted sandboxes and (2) for non-snapshotted ones

                let SandboxState::Running(mut sandbox) = self.sandbox_state.take() else {
                    unreachable!("already asserted that we are on SandboxState::Running");
                };
                // PauseVM; if it fails, return Err(()) to hand the sandbox to Pool as leaked
                if let Err(err) = self.runtime.pause_sandbox(&mut sandbox).await {
                    error!(error = ?err, sandbox = ?sandbox, "Failed to pause sandbox");
                    // NOTE: Running -> Trashed
                    self.sandbox_state = SandboxState::Trashed(sandbox);
                    return Err(());
                }
                // (NOTE: conceptually: Running -> Paused)

                if sandbox.has_snapshot() {
                    // On failure, return Err(()) to hand over the sandbox to Pool as leaked
                    if let Err(err) = self.runtime.shutdown_sandbox(&mut sandbox).await {
                        error!(error = ?err, sandbox = ?sandbox, "Failed to shutdown sandbox");
                        // NOTE: Running -> (Paused ->) Trashed
                        self.sandbox_state = SandboxState::Trashed(sandbox);
                        return Err(());
                    }
                    // NOTE: Running -> (Paused ->) Snapshot
                    self.sandbox_state = SandboxState::Snapshot(sandbox);
                } else {
                    self.sandbox_state = SandboxState::Paused(sandbox);
                    if let Err(err) = self.destroy_sandbox().await {
                        error!(error = ?err, "Failed to destroy sandbox: {err:#}");
                        return Err(());
                    }
                    // NOTE: Paused -> Nonexistent/Trashed (transitioned in self.destroy_sandbox)
                }
                Ok(())
            }
            Error::ForwardToSink(_) => {
                // - Any internal channel failure that has bubbled up here is probably bad news.
                // - At this point, we might:
                // (1) attempt to pause & unload the VM and return it to Pool for later use;
                // (2) destroy the VM and return nothing to Pool (in this case, we'd also have
                //     to contact SnapshotManager to remove the snapshot, if any?).
                assert!(
                    matches!(self.sandbox_state, SandboxState::Running(_)),
                    "Error::ForwardToSink should be returned only while SandboxState::Running"
                );
                // Let's go with (1) for snapshotted sandboxes and (2) for non-snapshotted ones

                let SandboxState::Running(mut sandbox) = self.sandbox_state.take() else {
                    unreachable!("already asserted that we are on SandboxState::Running");
                };
                // PauseVM; if it fails, return Err(()) to hand the sandbox to Pool as leaked
                if let Err(err) = self.runtime.pause_sandbox(&mut sandbox).await {
                    error!(error = ?err, sandbox = ?sandbox, "Failed to pause sandbox");
                    // NOTE: Running -> Trashed
                    self.sandbox_state = SandboxState::Trashed(sandbox);
                    return Err(());
                }
                // (NOTE: conceptually: Running -> Paused)

                if sandbox.has_snapshot() {
                    // On failure, return Err(()) to hand over the sandbox to Pool as leaked
                    if let Err(err) = self.runtime.shutdown_sandbox(&mut sandbox).await {
                        error!(error = ?err, sandbox = ?sandbox, "Failed to shutdown sandbox");
                        // NOTE: Running -> (Paused ->) Trashed
                        self.sandbox_state = SandboxState::Trashed(sandbox);
                        return Err(());
                    }
                    // NOTE: Running -> (Paused ->) Snapshot
                    self.sandbox_state = SandboxState::Snapshot(sandbox);
                } else {
                    self.sandbox_state = SandboxState::Paused(sandbox);
                    if let Err(err) = self.destroy_sandbox().await {
                        error!(error = ?err, "Failed to destroy sandbox: {err:#}");
                        return Err(());
                    }
                    // NOTE: Paused -> Nonexistent/Trashed (transition in self.destroy_sandbox)
                }
                Ok(())
            }

            Error::PauseSandbox(_) => {
                // Assuming correct SandboxState, retry pausing the Sandbox after a short while.
                // Subsequently, regardless of the outcome, attempt to destroy it.
                let SandboxState::Running(mut sandbox) = self.sandbox_state.take() else {
                    error!(
                        sandbox_state = ?self.sandbox_state,
                        "BUG: Error::PauseSandbox should be returned only while SandboxState::Running"
                    );
                    unreachable!(
                        "BUG: Error::PauseSandbox should be returned only while SandboxState::Running (now: {:?})",
                        self.sandbox_state
                    );
                };
                ::tokio::time::sleep(Duration::from_secs(3)).await;
                match self.runtime.pause_sandbox(&mut sandbox).await {
                    Ok(()) => {
                        // NOTE: Running -> Paused
                        self.sandbox_state = SandboxState::Paused(sandbox);
                        trace!("Runtime successfully paused Sandbox before terminal cleanup");
                    }
                    Err(err) => {
                        self.sandbox_state = SandboxState::Running(sandbox);
                        warn!(error = ?err, "Runtime failed to pause Sandbox: {err:#}");
                    }
                }
                // NOTE: Paused/Running -> Nonexistent/Trashed
                self.destroy_sandbox().await.map_err(|err| {
                    error!(error = ?err, "Failed to destroy_sandbox: {err:#}");
                })
            }

            // - `Init`, `Forward` and `ShutDown` are not in `Worker::handle_request`'s path
            // - `Channel` is always wrapped in `ForwardToSink` or `PrepareSandbox`
            // - `Network` is always wrapped in `PrepareSandbox`
            // - `CreateSandbox`, `LoadSandbox` and `ResumeSandbox` are always wrapped in
            //   `PrepareSandbox`
            // - `CreateSnapshot` is not bubbled up (at least for now; check the related note)
            // - `ShutdownSandbox` and `DestroySandbox` are not in `Worker::handle_request`'s path,
            // they may be returned only by `self.shutdown()`
            // - `SnapshotManager` is not bubbled up for now
            _ => unreachable!(
                "all Error variants returned by Worker::handle_request are already matched"
            ),
        }
    }

    /// # Notes
    ///
    /// - Calling this method __always__ leaves `self.sandbox_state` in one of:
    ///   * [`SandboxState::Nonexistent`], if there was no [`Sandbox`] to destroy,
    ///     or if [`Runtime::destroy_sandbox`] consumed it successfully;
    ///   * [`SandboxState::Trashed`], if [`Runtime::destroy_sandbox`] returns the
    ///     [`Sandbox`] with an error.
    /// - This is only best-effort when called while [`SandboxState::Running`]: it
    ///   depends on the underlying [`Runtime::destroy_sandbox`] implementation.
    ///
    /// In the `Trashed` case, the [`Sandbox`] should be viewed more as a resource
    /// ownership token, as it may be partially destroyed, and must NOT be reused;
    /// this is a terminal state.
    ///
    /// # Panics
    ///
    /// If called while [`SandboxState::Trashed`].
    #[instrument(
        level = Level::DEBUG, skip_all, fields(sandbox.state = ?self.sandbox_state.as_ref())
    )]
    async fn destroy_sandbox(&mut self) -> Result<()> {
        let sandbox = match self.sandbox_state.take() {
            SandboxState::Paused(sb) | SandboxState::Snapshot(sb) => sb,
            SandboxState::Nonexistent => return Ok(()),
            SandboxState::Running(sandbox) => {
                // Currently:
                // - In `rt-fc`, `Runtime::destroy_sandbox` should be fine in this state.
                // - `rt-fcctrd`:
                //   * should be fine destroying _non-snapshotted_ running sandboxes;
                //   * is less forceful for _snapshotted_ running sandboxes: normally, it assumes
                //     the sandbox is paused; it first attempts to unload it (which should SIGKILL
                //     the uVM), and _only then_ to destroy it (releasing its resources). Hence,
                //     currently, if the unload fails, resource deallocation won't even be ever
                //     attempted. That makes this a best-effort.
                warn!(?sandbox, "About to destroy while SandboxState::Running");
                sandbox
            }
            SandboxState::Trashed(ref sandbox) => {
                // For now:
                // - the Runtime contract does not specify whether `Runtime::destroy_sandbox`
                //   should be reentrant (i.e., for a possibly partially destroyed Sandbox);
                // - nowhere do we actually attempt to destroy while in `SandboxState::Trashed`.
                // So let's just declare this a buggy state for now, and perhaps revisit? TODO
                error!(?sandbox, "BUG: Destroying while SandboxState::Trashed");
                unreachable!("BUG: Destroying SandboxState::Trashed({sandbox:?})");
            }
        };
        // NOTE: Transition to SandboxState::Nonexistent (effective already with `.take()`)

        // If the Sandbox has snapshot, keep its ID for post-destruction metadata update:
        let maybe_sandbox_id = sandbox.has_snapshot().then(|| sandbox.id().into());

        match self.runtime.destroy_sandbox(sandbox).await {
            Ok(net_resource) => {
                // If the Sandbox had a snapshot, let `SnapshotManager` (who should be
                // tracking it) know it won't be available anymore.
                if let Some(sandbox_id) = maybe_sandbox_id
                    && let Err(ref e @ Error::SnapshotManager { ref err, .. }) =
                        self.snapman_notify_removal(sandbox_id).await
                {
                    error!(error = ?e, "Failed to notify SnapMan of snapshot destruction: {err:#}")
                }

                match self.to.netman.deallocate(net_resource).await {
                    Ok(()) => Ok(()),
                    Err(err) => {
                        error!(error = ?err, "Failed to release network resource: {err:#}");
                        Err(Error::Network(Box::new(err)))
                    }
                }
            }
            Err(DestroySandboxRuntimeError { sandbox, err }) => {
                error!(error = ?err, "Failed to destroy sandbox: {err:#}");
                // NOTE: Nonexistent -> Trashed
                self.sandbox_state = SandboxState::Trashed(sandbox);

                Err(Error::DestroySandbox(err))
            }
        }
    }

    #[instrument(level = Level::INFO, skip(self))]
    async fn handle_prepare_sandbox_request(&mut self, cpuset: Cpu) -> Result<()> {
        let start_state = *self.sandbox_state.as_ref();
        debug_assert!(
            matches!(
                self.sandbox_state,
                SandboxState::Nonexistent | SandboxState::Snapshot(_)
            ),
            "Pool should have either assigned a snapshot or tasked to create a Sandbox anew"
        );

        self.invocation_timings.clear(); // Not really needed?

        // Prepare the sandbox (create or load it, normally, depending on my state)
        if let Err(err) = self.prepare_sandbox(cpuset).await {
            error!(error = ?err, "Failed to prepare sandbox upon Pool's request: {err:#}");
            self.send_sandbox_prep_result(Err(err.to_string().into_boxed_str()))
                .await;
            return Err(Error::PrepareSandbox(Box::new(err)));
        }
        // NOTE: Transitioned to SandboxState::Running.

        // Pause the sandbox
        if let Err(err) = self.pause_sandbox().await {
            error!("Failed to pause sandbox while preparing it upon Pool's request: {err:#}");
            // TODO: proper error handling/cleanup: what to do with the sandbox? FIXME(XXX)?
            self.send_sandbox_prep_result(Err(err.to_string().into_boxed_str()))
                .await;
            return Err(err);
        }
        // NOTE: Running -> Paused
        let SandboxState::Running(sandbox) = self.sandbox_state.take() else {
            error!(sandbox_state = ?self.sandbox_state, "BUG: Entering _handle_with_sandbox");
            unreachable!("Entering _handle_with_sandbox while not in SandboxState::Running")
        };
        let sandbox_id = sandbox.id().into();
        let mut has_snapshot = sandbox.has_snapshot();
        let stats = *sandbox.stats();
        self.sandbox_state = SandboxState::Paused(sandbox);

        // If `SnapshotCreationTime::OnCreationBeforeInvoc`, also create a snapshot of the Sandbox.
        // However, this is best-effort only; snapshotting failure is only logged, not bubbled up.
        if start_state == SandboxStateRef::Nonexistent
            && self.snap_creation_time == SnapshotCreationTime::OnCreationBeforeInvoc
        {
            debug_assert!(!has_snapshot, "if had snapshot, we'd never reach here");
            if let Err(err) = self.create_snapshot().await {
                debug_assert!(!has_snapshot, "snapshot creation just failed");
                error!(
                    error = ?err,
                    "Failed to create snapshot after preparing it upon Pool's request: {err:#}"
                );
            } else {
                has_snapshot = true;
            }
        }

        self.send_sandbox_prep_result(Ok((sandbox_id, has_snapshot, stats)))
            .await;
        trace!(timings = ?self.invocation_timings);
        Ok(())
    }

    #[instrument(level = Level::INFO, skip(self))]
    async fn handle_snapshot_request(&mut self, sandbox_id: SandboxId) {
        debug_assert_ne!(
            self.snap_creation_time,
            SnapshotCreationTime::Never,
            "Pool should be discarding snapshotting requests earlier"
        );
        debug_assert_eq!(
            self.sandbox_state
                .id()
                .unwrap_or_else(|| sandbox_id.as_str()),
            sandbox_id.as_str(),
            "Pool should be routing snapshotting requests to Workers based on SandboxID"
        );

        self.invocation_timings.clear(); // Not really needed?

        let result = match &self.sandbox_state {
            // Check if already snapshotted to return early
            SandboxState::Snapshot(_) => Ok(()),
            SandboxState::Paused(sandbox) if sandbox.has_snapshot() => Ok(()),
            SandboxState::Paused(_) => self.create_snapshot().await.inspect_err(|err| {
                error!(
                    error = ?err,
                    "Failed to create snapshot upon Pool's request: {err:#}"
                )
            }),
            s @ SandboxState::Nonexistent
            | s @ SandboxState::Running(_)
            | s @ SandboxState::Trashed(_) => {
                error!("I should have never reached here while in SandboxState::{s:?}");
                Err(Error::CreateSnapshotControl(
                    format!("cannot create snapshot of SandboxState::{:?}", s.as_ref())
                        .into_boxed_str(),
                ))
            }
        };

        // Respond to Pool and continue on the loop.
        self.send_snapshot_result(
            result,
            self.sandbox_state
                .id()
                .map_or_else(|| sandbox_id, |sid| sid.into()),
            // NOTE: The only reason SandboxId is provided here, and even passed within
            // `ControlMessage::CreateSnapshot`, is to make sure Worker has a SandboxID
            // when responding to Pool, even when `SandboxState::Nonexistent`. Even
            // though reaching here in such a case is currently impossible (and so must
            // remain in the future), the cost of being extra defensive here is low.
        )
        .await;
    }
}
