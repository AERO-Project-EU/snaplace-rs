use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
};

use tokio::{
    sync::{broadcast, mpsc, oneshot, OwnedSemaphorePermit, Semaphore},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::PollSemaphore;
use tracing::{error, instrument, warn, Level};

use crate::{
    admission::{
        AdmissionFailure, BlockReason, DispatchMode, DispatchRequest, DispatchResponse,
        RejectReason,
    },
    conf::AdmissionConfig,
    metadata::FunctionInfo,
    sbpool::PoolEvent,
    FunctionId, FunctionMetadataStore, InvocationId, Request, Response,
};

/// Lightweight counters for [`AdmissionController`]'s activity.
///
/// These counters are local, best-effort observability only.
/// They are updated by [`AdmissionController`] actor and returned when it
/// exits.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct QuickStats {
    /// Number of requests received by admission from [`request::Source`].
    ///
    /// [`request::Source`]: crate::request::Source
    num_received: u64,
    /// Number of requests successfully forwarded from admission to a [`Worker`].
    ///
    /// [`Worker`]: crate::worker::Worker
    num_dispatched: u64,
    /// Number of requests failed by admission before reaching a [`Worker`]
    ///
    /// [`Worker`]: crate::worker::Worker
    num_failed: u64,
}

/// Request plus admission-local scheduling metadata.
///
/// `RequestEnvelope` exists only while a request is owned by admission:
/// queued in a per-Function queue, temporarily outstanding with Pool, or
/// being failed before it reaches a Worker.
#[derive(Debug)]
struct RequestEnvelope<Req> {
    req: Req,
    function_id: FunctionId,
    enqueued_at: Instant,
    deadline: Instant,
}

/// Admission-side dispatch state for one Function queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FunctionDispatchState {
    /// The queue has work that may be considered by cross-Function scheduling.
    Runnable,
    /// The queue has work, but its head request most recently received
    /// [`DispatchResponse::WouldBlock`] and should not be retried until
    /// a wakeup.
    ///
    /// ## Invariant
    ///
    /// A [`FunctionQueue`] is never left in `Blocked` state while empty.
    /// The state is only set to `Blocked` after requeuing a head request, and
    /// any path that drains a queue must restore its state to [`Runnable`](Self::Runnable).
    Blocked {
        reason: BlockReason,
        blocked_at: Instant,
    },
}

/// FIFO admission queue and dispatch state for one Function.
///
/// Requests remain FIFO within a Function. The state bit controls whether
/// this Function participates in cross-Function scheduling.
#[derive(Debug)]
struct FunctionQueue<Req> {
    queue: VecDeque<RequestEnvelope<Req>>,
    state: FunctionDispatchState,
}

impl<Req> FunctionQueue<Req> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(cap),
            state: FunctionDispatchState::Runnable,
        }
    }

    // NOTE(ckatsak): As, for now, empty queues are never allowed to remain `Blocked`,
    // this helper checks only the dispatch state bit and not queue emptiness.
    //
    /// Returns whether this Function is currently excluded from scheduling.
    ///
    /// For now, empty queues are never allowed to remain `Blocked`, so this
    /// checks only the state bit and relies on the queue-state invariant.
    #[inline]
    fn is_blocked(&self) -> bool {
        matches!(self.state, FunctionDispatchState::Blocked { .. }) //|| self.queue.is_empty()
    }

    #[inline]
    fn is_runnable(&self) -> bool {
        !self.queue.is_empty() && matches!(self.state, FunctionDispatchState::Runnable)
    }
}

/// One in-flight dispatch attempt owned by [`AdmissionController`].
///
/// This is not a Pool reservation. It records the request that admission
/// removed from a Function queue, the reply channel for Pool's decision, and
/// the global execution slot acquired before contacting Pool.
///
/// While this is present, admission waits for Pool instead of issuing another
/// dispatch attempt.
/// - On `Assigned`, the request is forwarded to the Worker.
/// - On `WouldBlock`, the request is pushed back to the front of its Function
///   queue.
/// - On `Reject`, the request is failed.
struct OutstandingDispatch<Req: crate::Request> {
    envelope: RequestEnvelope<Req>,
    from_pool: oneshot::Receiver<DispatchResponse<Req>>,
    slot_permit: OwnedSemaphorePermit,
}

/// Admission actor between `request::Source` and `SandboxPool`.
///
/// The controller owns bounded per-Function queues, global queued-request
/// accounting, deadline expiry, blocked-Function retry state, and the global
/// execution-slot semaphore. It asks Pool for Worker assignment only after a
/// global slot has been acquired.
///
/// Scheduling is FIFO within each Function and "oldest-runnable-head-first"
/// across Functions. Blocked Functions are skipped so one unavailable
/// Function cannot cause global head-of-line blocking.
pub(crate) struct AdmissionController<Req, Resp, Store, FnInfo>
where
    Req: Request,
    Resp: Response,
    Store: FunctionMetadataStore<FnInfo>,
    FnInfo: FunctionInfo,
{
    config: AdmissionConfig,
    from_req_src: mpsc::Receiver<Req>,
    to_resp_sink: mpsc::Sender<Resp>,
    to_pool: mpsc::Sender<DispatchRequest<Req>>,
    from_pool: mpsc::Receiver<PoolEvent>,

    store: ::triomphe::Arc<Store>,
    _function_info: PhantomData<fn() -> FnInfo>,

    queues: HashMap<FunctionId, FunctionQueue<Req>, crate::BuildHasher>,
    /// Number of requests currently held in admission-owned [`FunctionQueue`]s.
    ///
    /// This excludes the one request, if any, stored in `outstanding`, as
    /// that request has already left the queued-capacity accounting.
    total_queued_reqs: usize,
    /// Number of FunctionQueues currently in [`FunctionDispatchState::Blocked`]
    /// state.
    ///
    /// This makes the fallback-period select guard O(1): the event loop can
    /// tell whether any blocked Function exists without scanning all queues
    /// on every iteration.
    /// The value must be updated only through the related transition helpers
    /// ([`Self::mark_function_blocked`], [`Self::mark_function_runnable`],
    /// [`Self::unblock_resource_blocked_functions`], [`Self::unblock_all`]).
    /// ([`Self::expire_queued_requests`] also updates it directly because it
    /// already sweeps all queues.)
    blocked_function_count: usize,

    /// See [`deadline_timer`](AdmissionController::deadline_timer).
    next_deadline: Option<Instant>,
    /// One-shot wakeup for queued-request deadline expiry.
    ///
    /// This timer is armed for `next_deadline`, the cached earliest deadline
    /// among the FIFO heads of all per-Function queues. It is not a periodic
    /// poller: when it fires, `expire_queued_requests()` drains expired queue
    /// heads and then recomputes/rearms the next sleep.
    ///
    /// The timer is intentionally conservative. Enqueue/requeue paths move it
    /// earlier if they introduce a new earliest deadline, while removals may
    /// leave it stale in the "fires too early" direction. Early wakeups are
    /// harmless: the expiry pass finds no expired head, recomputes the next
    /// deadline, and goes back to sleep.
    ///
    /// This is separate from [`liveness_timer`](AdmissionController::liveness_timer),
    /// which "repairs" lost Pool wakeups for blocked Functions.  This timer
    /// enforces admission queue-delay bounds.  Without this wakeup, a request
    /// could remain queued past its `max_queue_delay` and later be dispatched
    /// merely because no other event happened to examine that queue.
    deadline_timer: Option<Pin<Box<::tokio::time::Sleep>>>,

    /// Coarse fallback retry interval for blocked [`FunctionQueue`]s.
    ///
    /// [`PoolEvent`]s are advisory wake-up hints and may be dropped or arrive
    /// stale.  This timer is the independent liveness-repair path: when it
    /// fires, all currently blocked Functions are made runnable so they can
    /// re-query Pool.
    ///
    /// The interval is deliberately delayed and non-bursty. Its first tick is
    /// not immediate, missed ticks are delayed rather than replayed, and the
    /// interval is reset when the first Function in a "blocked epoch" becomes
    /// blocked. This prevents `WouldBlock` from being followed by an immediate
    /// fallback retry.
    liveness_timer: ::tokio::time::Interval,

    /// The single dispatch attempt currently waiting for Pool's reply.
    outstanding: Option<OutstandingDispatch<Req>>,

    /// Reusable acquirer for global concurrent-execution-capacity slots/permits.
    ///
    /// `AdmissionController` acquires one such permit before asking Pool for a
    /// Worker assignment, so the number of in-flight executions cannot exceed
    /// [`AdmissionConfig::max_concurrency`].
    ///
    /// [`PollSemaphore`] avoids rebuilding a boxed `acquire_owned()` future for
    /// every wait. It first tries to acquire immediately, and on contention it
    /// keeps reusable storage for the pending acquire future across `select!`
    /// iterations.
    running_slots: PollSemaphore,
    /// Whether `AdmissionController` is currently waiting for a
    /// concurrent-execution-capacity slot/permit.
    ///
    /// This is the event-loop state bit for [`running_slots`](Self::running_slots):
    /// - when `true`, the `select!` branch polls `PollSemaphore`;
    /// - when `false`, `AdmissionController` should not pre-acquire capacity.
    ///
    /// [`PollSemaphore`] may still retain reusable storage internally after a
    /// previous wait, but no permit is reserved unless this bit drives it to
    /// `Ready(Some(_))`.
    // TODO: perhaps this should remain local in `Self::run`, rather than here?
    waiting_for_slot: bool,

    quit_rx: broadcast::Receiver<()>,

    /// Local lifecycle counters returned by [`Self::run`] on graceful exit.
    stats: QuickStats,
}

impl<Req, Resp, Store, FnInfo> AdmissionController<Req, Resp, Store, FnInfo>
where
    Req: Request,
    Resp: Response,
    Store: FunctionMetadataStore<FnInfo>,
    FnInfo: FunctionInfo,
{
    /// Build an admission controller with empty queues and no in-flight dispatch.
    pub(crate) fn new(
        config: &AdmissionConfig,
        store: ::triomphe::Arc<Store>,
        from_req_src: mpsc::Receiver<Req>,
        to_resp_sink: mpsc::Sender<Resp>,
        to_pool: mpsc::Sender<DispatchRequest<Req>>,
        from_pool: mpsc::Receiver<PoolEvent>,
        quit_rx: broadcast::Receiver<()>,
    ) -> Self {
        let mut liveness_timer = ::tokio::time::interval_at(
            Instant::now() + config.fallback_retry_period,
            config.fallback_retry_period,
        );
        liveness_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Self {
            config: config.clone(),
            from_req_src,
            to_resp_sink,
            to_pool,
            from_pool,
            store,
            _function_info: PhantomData,
            queues: Default::default(),
            total_queued_reqs: 0,
            blocked_function_count: 0,
            next_deadline: None,
            deadline_timer: None,
            outstanding: None,
            liveness_timer,
            running_slots: PollSemaphore::new(Arc::new(Semaphore::new(config.max_concurrency))),
            waiting_for_slot: false,
            quit_rx,
            stats: Default::default(),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    pub(crate) async fn run(&mut self) -> Result<QuickStats, ()> {
        loop {
            // NOTE(ckatsak): we acquire the slot _before_ asking Pool for a Worker assignment.
            // This avoids a potential stall where Pool could have already committed Worker
            // state, but AdmissionController then blocks waiting for a `RunningSlot`.
            //
            // `waiting_for_slot` requests exactly one global execution-slot permit. The
            // wait is local AdmissionController bookkeeping only: it is not tied to a
            // specific FunctionQueue, and it is not any sort of Pool-side reservation.
            if self.should_wait_for_slot() {
                self.waiting_for_slot = true;
            }

            ::tokio::select! {
                quit_res = self.quit_rx.recv() => {
                    match quit_res {
                        Ok(()) => warn!("Received quit notification!"),
                        Err(err) => error!(error = ?err, "Quit channel failure"),
                    }
                    // ¿TODO(ckatsak): Clean up anything or notify anyone before quitting?
                    return Ok(self.stats)
                }

                // new Request incoming from the RequestSource
                req_res = self.from_req_src.recv() => {
                    match req_res {
                        Some(req) => self.handle_new_request(req),
                        None => {
                            // We reach this when the request source has closed
                            // the channel, hence no more requests are expected.
                            warn!("Request source's channel has been closed; exiting...");
                            // ¿TODO(ckatsak): gracefully drain queued requests before exiting?
                            return Ok(self.stats);
                        }
                    }
                }

                // new (advisory) PoolEvent incoming from the Pool
                Some(ev) = self.from_pool.recv() => self.handle_pool_event(ev),

                // Conditionally poll the reusable semaphore acquirer while a
                // slot wait is active.
                // Once this branch completes, AdmissionController either owns
                // a permit to consume in exactly one Pool dispatch attempt, or
                // the semaphore has been closed.
                //
                // The `poll_fn` future is recreated on each loop iteration, but
                // the acquire state is owned by `self.running_slots`. If another
                // `select!` branch wins while the permit is still pending, dropping
                // this temporary future does not abandon the semaphore wait; the
                // next iteration polls the same `PollSemaphore` state again.
                maybe_permit = ::std::future::poll_fn(|cx| {
                    self.running_slots.poll_acquire(cx)
                }), if self.waiting_for_slot => {
                    // Future completed; the active slot wait is resolved;
                    // we should no longer be waiting/polling for a permit.
                    self.waiting_for_slot = false;

                    match maybe_permit {
                        Some(permit) => if let Err(()) = self.handle_acquired_slot(permit) {
                            error!("Pool's channel closed! Exiting...");
                            return Err(());
                        }
                        None => {
                            error!("Semaphore is unexpectedly closed!");
                            return Err(());
                        }
                    }
                }

                // new DispatchResponse incoming from the Pool
                disp_resp_or_err = async {
                    match self.outstanding.as_mut() {
                        Some(outstanding) => (&mut outstanding.from_pool).await,
                        None => {
                            ::std::hint::cold_path();
                            ::std::future::pending().await
                        }
                    }
                }, if self.outstanding.is_some() => {
                    match disp_resp_or_err {
                        Ok(resp) => match self.handle_dispatch_reply(resp).await {
                            Ok(()) => {},
                            Err(()) => {
                                // Reaching this means Pool already assigned a Worker, but fwd'ing
                                // the request to its invocation channel failed because the recv
                                // half has been dropped/closed. This should be exceptional and we
                                // currently treat it as fatal for the controller (?)
                                error!("Failed to handle DispatchResponse; exiting...");
                                return Err(());
                            }
                        }
                        Err(err) => {
                            // Reaching this can only mean that Pool has dropped its Sender half.
                            // This either is a BUG or the Pool is being terminated?
                            // I think Pool cannot be in a terminating phase at this point of its
                            // execution (it must have already selected its dispatch channel over
                            // the quit chan), therefore I think _normally_ it should never close
                            // its oneshot chan at this point; hence this should always be a BUG
                            error!(error = ?err, "Failed to receive DispatchResponse: {err:#}");
                            if let Some(curr) = self.outstanding.take() {
                                self.fail_admission(
                                    &curr.envelope.req,
                                    &curr.envelope.function_id,
                                    AdmissionFailure::Rejected(RejectReason::Internal),
                                );
                            }
                            return Err(());
                        }
                    }
                }

                // queue deadline / expiry timer
                _ = async {
                    self.deadline_timer.as_mut().expect("guard ensures `is_some()`").await
                }, if self.deadline_timer.is_some() => {
                    self.expire_queued_requests(Instant::now());
                }

                // periodically unblock all Functions, as a fallback for liveness guarantee?
                _ = self.liveness_timer.tick(), if self.has_blocked_functions() => self.unblock_all(),
            }
        }
    }

    /// Returns `true` if _any_ __non-empty__ [`FunctionQueue`] is in
    /// [`FunctionDispatchState::Runnable`], and `false` otherwise.
    #[inline]
    fn has_runnable_functions(&self) -> bool {
        self.queues.values().any(FunctionQueue::is_runnable)
    }

    /// Returns whether any [`FunctionQueue`] is in
    /// [`FunctionDispatchState::Blocked`], using the cached counter.
    ///
    /// This intentionally does not scan `queues`; keeping this guard O(1) is
    /// the point of `blocked_function_count`.
    #[inline]
    fn has_blocked_functions(&self) -> bool {
        self.blocked_function_count > 0
    }

    /// Returns whether AdmissionController should begin waiting for a "global
    /// execution slot" (the core resource inside a [`RunningSlot`]).
    ///
    /// We wait only when:
    /// - no Pool dispatch reply is already outstanding,
    /// - no slot wait (i.e., `running_slots.poll_acquire()`) is already active,
    /// - at least one FunctionQueue is currently runnable.
    ///
    /// Note that this is only a local scheduling decision. Runnable work may
    /// disappear before the semaphore future resolves, so callers must re-check
    /// queue state after acquiring the permit.
    #[instrument(level = Level::TRACE, skip_all, ret)]
    #[inline]
    fn should_wait_for_slot(&self) -> bool {
        self.outstanding.is_none() && !self.waiting_for_slot && self.has_runnable_functions()
    }

    /// Turn an acquired semaphore permit into a pending Pool dispatch attempt
    /// (i.e., [`OutstandingDispatch`]).
    ///
    /// This method is the handoff point between:
    /// - AdmissionController-owned waiting/queueing, and
    /// - Pool-owned feasibility / Worker assignment.
    ///
    /// The acquired `permit` is attached to the resulting `OutstandingDispatch`.
    /// If no runnable Function remains by the time we get here, the permit is
    /// intentionally dropped and the slot is released immediately.
    ///
    /// The (raw semaphore-)`permit` will be converted into a [`RunningSlot`]
    /// only after Pool assigns a Worker and `AdmissionController` is about
    /// to forward the request to that Worker---i.e., not here, not now.
    /// This way, `AdmissionController` still reserves execution capacity
    /// before asking Pool, but [`Timing::RunningSlot`] starts only once Pool
    /// has assigned a Worker and admission is about to forward the request.
    ///
    /// Errors here are dispatch-setup failures before Pool has replied.
    ///
    /// [`RunningSlot`]: crate::admission::RunningSlot
    /// [`Timing::RunningSlot`]: crate::metrics::Timing::RunningSlot
    #[instrument(level = Level::TRACE, skip_all)]
    fn handle_acquired_slot(&mut self, permit: OwnedSemaphorePermit) -> Result<(), ()> {
        // NOTE(ckatsak): If both `deadline_timer` and `running_slots` branches were ready at the
        // `select!` and the slot branch (randomly) won, it may dispatch an already-expired head.
        let now = Instant::now();
        if self.next_deadline.is_some_and(|deadline| deadline <= now) {
            self.expire_queued_requests(now);
        }

        // Runnable work was present when `poll_acquire()` started, but that decision
        // may now be stale. For example, the last runnable head may have expired while
        // we were waiting for the permit. In that case we intentionally return here,
        // and let `permit` drop, which releases the slot back to the semaphore.
        let Some(function_id) = self.pick_next_function() else {
            drop(permit); // dropped anyway on return, just being explicit
            return Ok(());
        };

        // From this point on, the chosen head request is committed to exactly one
        // dispatch attempt. It leaves the per-Function queue and is tracked in
        // `self.outstanding` until Pool replies with Assigned / WouldBlock / Reject.
        let fq = self.queues.get_mut(&function_id).unwrap();
        let envelope = fq.queue.pop_front().unwrap();
        self.total_queued_reqs -= 1;
        // NOTE(ckatsak): Let's not eagerly adjust `self.deadline_timer` on removals.
        // The cached timer is allowed to be stale in the "fires too early" direction
        // and will be corrected on the next expiry pass.

        let FunctionDispatchState::Runnable = fq.state else {
            unreachable!("self.pick_next_function() should only be picking Runnable Functions");
        };

        let (respond_to, from_pool) = oneshot::channel();
        match self.to_pool.try_send(DispatchRequest {
            function_id: function_id.clone(),
            mode: DispatchMode::ReuseIdleOrProvision,
            respond_to,
        }) {
            Ok(()) => {
                // `OutstandingDispatch` owns both pieces of in-flight state for this one attempt:
                // - the RequestEnvelope removed from the queue
                // - the acquired semaphore permit
                // This is deliberate: if Pool later replies WouldBlock or Reject, dropping
                // the outstanding state also releases the permit automatically.
                self.outstanding = Some(OutstandingDispatch {
                    envelope,
                    from_pool,
                    slot_permit: permit,
                });

                Ok(())
            }
            Err(err) => {
                ::std::hint::cold_path();
                // NOTE(ckatsak): Normally, we should never reach here: either Pool is failing
                // (slow processing, badly-sized channel, etc) returning `TrySendError::Full`,
                // or it has crashed already returning `TrySendError::Closed`. We may either:
                // - roll back the state and retry, with the danger of CPU churn: the Controller
                //   may end up busy-spinning: acquire permit -> pop head -> try_send full ->
                //   requeue -> acquire permit -> ...
                // - fail this request and try the next one, with the risk of rejecting too many
                //   (possibly all) queued requests while the Pool isn't draining.
                // - just bubble up an `Err` and let AdmissionController (hence, the whole system)
                //   crash and burn.
                // Let's just go with the last one, for now.
                error!(
                    error = ?err, invocation_id = %envelope.req.invocation_id(), %function_id,
                    "Failed to send DispatchRequest to Pool: {err}"
                );

                //// Fail the request. The permit should be released by dropping it.
                //self.fail_admission(
                //    &envelope.req,
                //    &function_id,
                //    AdmissionFailure::Rejected(RejectReason::Internal),
                //);

                // Roll back local state. The permit is released by dropping it.
                let deadline = envelope.deadline;
                fq.queue.push_front(envelope);
                self.total_queued_reqs += 1;
                self.update_deadline_timer(deadline);

                //match err {
                //    mpsc::error::TrySendError::Full(_) => Ok(()),
                //    mpsc::error::TrySendError::Closed(_) => Err(()),
                //}
                Err(())
            }
        }
    }

    /// Replace the cached earliest queued deadline and re-arm
    /// [`AdmissionController`'s expiry timer](Self::deadline_timer).
    ///
    /// If a timer already exists and there is a new deadline, reset the pinned
    /// `Sleep` in place instead of allocating a new pinned+boxed timer. If no
    /// next deadline is provided (i.e., `maybe_next_deadline.is_none()`; e.g.,
    /// when no queued request remains), disable the timer entirely.
    ///
    /// This helper only updates the cached value and timer state; it does not
    /// scan [`FunctionQueue`]s itself. Callers are responsible for passing the
    /// deadline that should become the cached admission wake-up.
    /// - Expiry sweeps pass the true earliest remaining deadline;
    /// - Enqueue/requeue paths pass a deadline only when it is earlier than
    ///   the currently cached value.
    fn set_deadline_timer_to(&mut self, maybe_next_deadline: Option<Instant>) {
        self.next_deadline = maybe_next_deadline;

        match (self.deadline_timer.as_mut(), maybe_next_deadline) {
            (Some(timer), Some(next_deadline)) => timer.as_mut().reset(next_deadline),
            (None, Some(next_deadline)) => {
                self.deadline_timer = Some(Box::pin(::tokio::time::sleep_until(next_deadline)))
            }
            (Some(_), None) => self.deadline_timer = None,
            (None, None) => {}
        }
    }

    /// Expire queued requests whose admission deadlines have passed.
    ///
    /// Per-Function queues are FIFO, and each request's deadline is computed
    /// when it enters admission. Because deadlines within a queue therefore
    /// increase in enqueue order, it is sufficient to inspect and drain only
    /// each FunctionQueue's head until the first non-expired request.
    ///
    /// Expired requests are removed from admission, counted out of
    /// `total_queued_reqs`, and completed with `AdmissionFailure::DeadlinePassed`.
    /// After the sweep, the cached deadline timer is rebuilt from the remaining
    /// queue heads.
    ///
    /// This pass also restores the blocked-state invariant. If the expired
    /// request was the head of a blocked Function, the block applied to that
    /// specific dispatch attempt is now stale, so the Function is marked
    /// runnable again. SandboxPool will still be re-queried before any future
    /// dispatch proceeds.
    ///
    /// The scan is deliberately simple and approximate: it costs O(#functions),
    /// not O(# queued requests), and it may run after an intentionally
    /// conservative timer fires early. That trade-off keeps hot
    /// enqueue/dequeue paths cheap while still bounding how long stale queued
    /// requests can occupy admission capacity.
    #[instrument(level = Level::TRACE, skip_all)]
    fn expire_queued_requests(&mut self, now: Instant) {
        let mut expired = Vec::new();
        let mut num_unblocked = 0;
        let mut next_deadline = None;

        for fq in self.queues.values_mut() {
            let was_blocked = fq.is_blocked();
            let expired_head = fq.queue.front().is_some_and(|re| re.deadline <= now);
            // As long as FunctionQueue head requests are expired, pop them:
            while fq.queue.front().is_some_and(|re| re.deadline <= now) {
                expired.push(fq.queue.pop_front().expect("just checked"));
            }
            // If a blocked queue loses the head Request that caused the block, or is now empty,
            // the block is stale. Restore Runnable state and keep `blocked_function_count` in sync.
            if was_blocked && (expired_head || fq.queue.is_empty()) {
                fq.state = FunctionDispatchState::Runnable;
                num_unblocked += 1;
            }

            // Calculate the new earliest deadline in the same pass, rather than
            // scanning all FunctionQueues again later (to update next deadline):
            if let Some(fn_dl) = fq.queue.front().map(|req| req.deadline) {
                next_deadline = Some(next_deadline.map_or(fn_dl, |curr: Instant| curr.min(fn_dl)));
            }
        }
        self.total_queued_reqs -= expired.len();
        debug_assert!(self.blocked_function_count >= num_unblocked);
        self.blocked_function_count -= num_unblocked;
        self.debug_assert_blocked_count();

        for req_env in expired {
            self.fail_admission(
                &req_env.req,
                &req_env.function_id,
                AdmissionFailure::DeadlinePassed,
            );
        }

        self.set_deadline_timer_to(next_deadline);
    }

    /// Pick the Function whose runnable queue has the oldest head request.
    ///
    /// This implements "oldest-runnable-head-first" across Functions. The
    /// current implementation scans all Function queues linearly; a heap/index
    /// can replace it later if Function count becomes high enough to matter.
    #[instrument(level = Level::TRACE, skip_all, ret)]
    fn pick_next_function(&self) -> Option<FunctionId> {
        self.queues
            .iter()
            .filter(|(_, fq)| fq.is_runnable())
            .min_by_key(|(_, fq)| fq.queue.front().expect("checked non-empty").enqueued_at)
            .map(|(fid, _)| fid.clone())
    }

    /// Update the cached queue-deadline wakeup after inserting or requeueing a
    /// request.
    ///
    /// `AdmissionController` uses `next_deadline` and `deadline_timer` as a
    /// conservative wakeup hint, not as exact bookkeeping of the queue's current
    /// minimum deadline at every mutation.
    ///
    /// Therefore:
    /// - when a request is inserted or requeued, the timer _must_ be moved
    ///   earlier if that request becomes the new earliest queued deadline.
    /// - when requests are removed, we deliberately allow the cached timer
    ///   to remain stale in the "too early" direction.
    ///
    /// That means the timer may occasionally fire before any queued request has
    /// actually expired. In that case `expire_queued_requests()` simply finds no
    /// expired head, recomputes the next earliest deadline, and rearms the timer.
    ///
    /// This keeps the dispatch-path queue mutations cheap while preserving
    /// correct expiry behavior.
    fn update_deadline_timer(&mut self, deadline: Instant) {
        if self.next_deadline.is_none_or(|curr| deadline < curr) {
            self.set_deadline_timer_to(Some(deadline));
        }
    }

    /// Admit one newly received request into the appropriate Function queue.
    ///
    /// This performs admission-only checks: global queued capacity,
    /// per-Function queued capacity, queue metadata creation, deadline assignment,
    /// and deadline timer maintenance. It does not contact Pool directly; dispatch
    /// progress is driven by the main event loop after a global slot is available.
    #[instrument(
        level = Level::TRACE,
        skip_all,
        fields(invocation.id = %req.invocation_id(), function.id = %req.function_id())
    )]
    fn handle_new_request(&mut self, req: Req) {
        self.stats.num_received += 1;

        // If unknown, fail fast
        let fid = FunctionId::from(req.function_id().as_ref());
        if !self.store.function_exists(&fid) {
            self.fail_admission(
                &req,
                &fid,
                AdmissionFailure::Rejected(RejectReason::UnknownFunction),
            );
            return;
        }

        let now = Instant::now();
        // Avoid running the expiry sweep twice for an ingress request. Only sweep on cap-pressure
        // slow paths, where expired queued requests could otherwise cause false QueueFull failures
        let mut checked_expiries = false;

        // If global queue is full, reject.
        if self.total_queued_reqs >= self.config.global_max_queued_reqs {
            // NOTE(ckatsak): Clean up expired heads, so that stale expired
            // requests do not cause QueueFull failures.
            if self.next_deadline.is_some_and(|deadline| deadline <= now) {
                self.expire_queued_requests(now);
            }
            checked_expiries = true;

            // If global queue is still full, reject.
            if self.total_queued_reqs >= self.config.global_max_queued_reqs {
                self.fail_admission(&req, &fid, AdmissionFailure::QueueFullGlobal);
                return;
            }
        }

        // Determine Function's config/overrides
        let func = self.store.registered_function(&fid);
        let fn_overrides = func.admission();
        let fq_cap = fn_overrides
            .and_then(|o| o.max_queued)
            .unwrap_or(self.config.default_max_queued_per_func);
        let fn_max_delay = fn_overrides
            .and_then(|o| o.max_queue_delay)
            .unwrap_or(self.config.default_max_queue_delay);

        let fq = self
            .queues
            .entry(fid.clone())
            .or_insert_with(|| FunctionQueue::with_capacity(fq_cap));

        // If Function's queue is full, reject.
        if fq.queue.len() >= fq_cap {
            // If not already done on `QueueFullGlobal`, clean up stale expired requests and retry.
            if !checked_expiries && self.next_deadline.is_some_and(|deadline| deadline <= now) {
                self.expire_queued_requests(now);
            }
            //checked_expiries = true;

            let fq = self // make the borrow-checker happy
                .queues
                .get(&fid)
                .expect("FunctionQueue should exist by now");
            if fq.queue.len() >= fq_cap {
                self.fail_admission(&req, &fid, AdmissionFailure::QueueFullFunction);
                return;
            }
        }

        // Otherwise, append to Function's queue.
        let deadline = now + fn_max_delay;
        self.queues
            .get_mut(&fid)
            .expect("FunctionQueue should exist by now")
            .queue
            .push_back(RequestEnvelope {
                req,
                function_id: fid,
                enqueued_at: now,
                deadline,
            });
        self.total_queued_reqs += 1;
        self.update_deadline_timer(deadline);
    }

    /// Apply an advisory Pool wakeup to admission-side blocked state.
    ///
    /// Pool events do not assign work and do not guarantee capacity. They only
    /// make selected blocked Functions runnable again so the controller may
    /// issue a fresh dispatch request later.
    #[instrument(level = Level::TRACE, skip_all)]
    fn handle_pool_event(&mut self, ev: PoolEvent) {
        // Pool should only be emitting events for Functions that were
        // previously dispatched through this controller. FunctionQueues
        // are retained after becoming empty, so the queue should exist.
        match ev {
            PoolEvent::WorkerIdle { function_id } => {
                // NOTE: Unblock regardless of `BlockReason` as, for now, in all `BlockReason`
                // cases, a newly _Idle_ Worker may now be able to handle this Function's request.
                self.mark_function_runnable(&function_id);
            }
            PoolEvent::MemoryCapAvailable => {
                // Clear all Functions who are blocked for some resource-related reason.
                self.unblock_resource_blocked_functions();
            }
            PoolEvent::WorkerCapAvailable { function_id } => {
                let Some(fq) = self.queues.get(&function_id) else {
                    ::std::hint::cold_path();
                    warn!(%function_id, "Request to mark unknown FunctionQueue runnable");
                    return;
                };
                if matches!(
                    fq.state,
                    FunctionDispatchState::Blocked {
                        reason: BlockReason::FunctionWorkerCap,
                        ..
                    }
                ) {
                    self.mark_function_runnable(&function_id);
                }
            }
        }
    }

    /// Resolve the current outstanding Pool dispatch attempt.
    ///
    /// The reply determines ownership of the request:
    /// - `Assigned` forwards it to a Worker;
    /// - `WouldBlock` requeues it at the front of its Function queue and marks
    ///   that Function blocked;
    /// - `Reject` completes it as an admission failure.
    ///
    /// Dropping the outstanding state releases the acquired running slot
    /// whenever the request is not forwarded to a Worker.
    ///
    /// # Errors
    ///
    /// On failure to forward the pending request to the Pool-assigned Worker.
    ///
    /// # Panics
    ///
    /// If called while `self.outstanding.is_none()`.
    #[instrument(level = Level::TRACE, skip_all)]
    async fn handle_dispatch_reply(&mut self, resp: DispatchResponse<Req>) -> Result<(), ()> {
        let Some(curr) = self.outstanding.take() else {
            unreachable!("dispatch reply branch enabled only when `outstanding.is_some()`")
        };
        match resp {
            DispatchResponse::Dispatch { worker_ref, cpuset } => {
                // Pool has already made the authoritative assignment decision. Convert
                // the semaphore permit into a `RunningSlot` and forward the request.
                //
                // This avoids a potential stall where Pool could have already committed Worker
                // state, but AdmissionController then blocks waiting for a permit/RunningSlot.
                if let Err(err) = worker_ref
                    .forward(curr.envelope.req, curr.slot_permit.into(), cpuset)
                    .await
                {
                    error!(error = ?err, ?worker_ref, "Failed to forward Request to Worker: {err}");
                    return Err(());
                }
                self.stats.num_dispatched += 1;
            }
            DispatchResponse::WouldBlock { reason } => {
                // Requeue the same head request and mark this Function blocked. `curr`
                // still owns the acquired running slot for this failed attempt; when this
                // arm exits, dropping `curr` releases that slot back to the semaphore.
                let fq = self
                    .queues
                    .get_mut(&curr.envelope.function_id)
                    .expect("FunctionQueue should exist by now");
                let fid = curr.envelope.function_id.clone();
                let deadline = curr.envelope.deadline;
                fq.queue.push_front(curr.envelope);
                self.total_queued_reqs += 1;
                self.update_deadline_timer(deadline);
                self.mark_function_blocked(&fid, reason);
            }
            DispatchResponse::Reject { reason } => {
                // Reject is terminal for this request: do not requeue it. As
                // above, dropping `curr` releases the acquired running slot.
                self.fail_admission(
                    &curr.envelope.req,
                    &curr.envelope.function_id,
                    AdmissionFailure::Rejected(reason),
                );
            }
        }
        Ok(())
    }

    /// Complete a request that failed before reaching a Worker.
    ///
    /// Admission failures are converted into final `Response`s and forwarded to the
    /// ResponseSink on a best-effort, non-blocking path. This method is used for
    /// local queue-capacity failures, queue deadline expiry, and terminal Pool
    /// rejection.
    #[instrument(
        level = Level::TRACE,
        skip_all,
        fields(invocation.id = %req.invocation_id(), function.id = %function_id),
    )]
    fn fail_admission(&mut self, req: &Req, function_id: &FunctionId, failure: AdmissionFailure) {
        // NOTE(ckatsak): This stat remains correct here only as long as `fail_admission` is
        // not called more than once for the same Request.  Also note that this stat increase
        // is the only reason this method requires `&mut self` rather than plain `&self`.
        self.stats.num_failed += 1;

        let (status_code, msg) = match failure {
            AdmissionFailure::QueueFullGlobal => (
                ::tonic::Code::ResourceExhausted,
                "Global admission queue is full",
            ),
            AdmissionFailure::QueueFullFunction => (
                ::tonic::Code::ResourceExhausted,
                "Function's admission queue is full",
            ),
            AdmissionFailure::DeadlinePassed => (
                ::tonic::Code::DeadlineExceeded,
                "Request's deadline exceeded",
            ),

            AdmissionFailure::Rejected(RejectReason::UnknownFunction) => {
                (::tonic::Code::NotFound, "Unknown Function")
            }
            AdmissionFailure::Rejected(RejectReason::Internal) => {
                (::tonic::Code::Internal, "SandboxPool internal error")
            }
            AdmissionFailure::Rejected(RejectReason::ShuttingDown) => (
                ::tonic::Code::FailedPrecondition,
                "SandboxPool is shutting down",
            ),
        };

        let resp = match Resp::try_from_parts(
            &InvocationId::from(req.invocation_id()),
            function_id,
            status_code,
            Default::default(),
            msg.into(),
        ) {
            Ok(resp) => resp,
            Err(err) => {
                error!(
                    error = ?err, invocation.id = %req.invocation_id(), function.id = %function_id,
                    "Dropping admission-failed Request after failure to construct final Response: {err:#}"
                );
                return;
            }
        };

        // If `try_send()` fails, the request is silently dropped.
        // TODO(ckatsak):
        // - Perhaps handling `TrySendError::Full` by appending to a local queue,
        //   and then poll RespSink's channel for a permit on the main event loop?
        // - Perhaps be even more paranoid: avoid constructing the Response here;
        //   have a separate task constructing the Responses and trying sending
        //   them to ResponseSink; silently drop only when unable to send to that?
        if let Err(err) = self.to_resp_sink.try_send(resp) {
            error!(
                error = ?err, invocation.id = %req.invocation_id(), function.id = %function_id,
                "Failed to (immediately) forward admission-failure Response to Sink: {err:#}"
            );
        }
    }
}

/// Helpers related to Functions in [`FunctionDispatchState::Blocked`].
impl<Req, Resp, Store, FnInfo> AdmissionController<Req, Resp, Store, FnInfo>
where
    Req: Request,
    Resp: Response,
    Store: FunctionMetadataStore<FnInfo>,
    FnInfo: FunctionInfo,
{
    /// Debug-check that the cached blocked counter matches queue state.
    #[inline]
    fn debug_assert_blocked_count(&self) {
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!(
                self.blocked_function_count,
                self.queues.values().filter(|fq| fq.is_blocked()).count(),
            );

            debug_assert!(
                self.queues
                    .values()
                    .all(|fq| !fq.is_blocked() || !fq.queue.is_empty()),
                "FunctionQueue must not remain Blocked while empty"
            );
        }
    }

    /// Mark one non-empty FunctionQueue blocked after a `WouldBlock` reply.
    ///
    /// If this starts a new "blocked epoch", reset the fallback interval so
    /// the retry sweep waits a full configured period before firing.
    fn mark_function_blocked(&mut self, function_id: &FunctionId, reason: BlockReason) {
        let fq = self
            .queues
            .get_mut(function_id)
            .expect("FunctionQueue should exist by now");
        debug_assert!(
            !fq.queue.is_empty(),
            "FunctionQueue should not be marked Blocked while empty"
        );

        if !fq.is_blocked() {
            self.blocked_function_count += 1;

            // Start the coarse fallback delay when the blocked epoch begins.
            if self.blocked_function_count == 1 {
                self.liveness_timer
                    .reset_after(self.config.fallback_retry_period);
            }
        }

        fq.state = FunctionDispatchState::Blocked {
            reason,
            blocked_at: Instant::now(),
        };

        self.debug_assert_blocked_count();
    }

    /// Mark `function_id`'s [`FunctionQueue`] [runnable] if it is currently
    /// [blocked].
    ///
    /// [blocked]: FunctionDispatchState::Blocked
    /// [runnable]: FunctionDispatchState::Runnable
    #[inline]
    fn mark_function_runnable(&mut self, function_id: &FunctionId) {
        let Some(fq) = self.queues.get_mut(function_id) else {
            ::std::hint::cold_path();
            warn!(%function_id, "Request to mark unknown FunctionQueue runnable");
            return;
        };

        if fq.is_blocked() {
            fq.state = FunctionDispatchState::Runnable;
            debug_assert!(self.blocked_function_count > 0);
            self.blocked_function_count -= 1;
        }

        self.debug_assert_blocked_count();
    }

    /// Unblock Functions whose last block reason may be cleared by global
    /// capacity changes.
    ///
    /// This intentionally does not clear per-Function _Idle_-only and
    /// Worker cap blocks: those are normally repaired by Function-specific
    /// `WorkerIdle` events or by the coarse fallback sweep.
    fn unblock_resource_blocked_functions(&mut self) {
        let mut unblocked = 0;

        for fq in self.queues.values_mut() {
            use BlockReason::*;
            if matches!(
                fq.state,
                FunctionDispatchState::Blocked {
                    reason: MemoryPressure, // | NoCpu | EvictionInProgress,
                    ..
                }
            ) {
                fq.state = FunctionDispatchState::Runnable;
                unblocked += 1;
            }
        }

        debug_assert!(self.blocked_function_count >= unblocked);
        self.blocked_function_count -= unblocked;
        self.debug_assert_blocked_count();
    }

    /// Fallback liveness repair: mark all blocked Function queues runnable.
    ///
    /// This sweep is `O( # Functions )`, but it only runs on the coarse
    /// fallback interval after at least one Function is blocked. The normal
    /// select guard uses `blocked_function_count` and does not scan.
    ///
    /// Rationale:
    /// - Pool wakeups are considered lossy by design, so admission needs an
    ///   independent liveness-repair path;
    /// - re-enabling a Function is cheap and, importantly, safe because Pool
    ///   remains the authoritative source of current feasibility;
    /// - occasional false retries are acceptable here because this path is not
    ///   part of the normal hot dispatch flow.
    ///
    /// This is deliberately conservative and may be refined later using
    /// [`BlockReason`], elapsed blocked time, or other policy inputs.
    fn unblock_all(&mut self) {
        if self.blocked_function_count == 0 {
            return;
        }

        self.queues
            .values_mut()
            .filter(|fq| fq.is_blocked())
            .for_each(|fq| fq.state = FunctionDispatchState::Runnable);

        self.blocked_function_count = 0;
        self.debug_assert_blocked_count();
    }
}
