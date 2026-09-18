use std::{collections::HashMap, time::Duration};

use itertools::Itertools;
use smallvec::SmallVec;
use tokio::{sync::oneshot, time::Instant};
use tracing::{error, info, instrument, trace, warn, Level};
use triomphe::Arc;

use super::{Error, Result, SandboxPool, WorkerMemAccounting};
use crate::{
    conf::SnapshotCreationTime,
    keepalive, network,
    sbpool::{
        api::{
            self, DestroySandboxOptions, DestroySandboxOutput, PoolControlMessage,
            PrepareSandboxOptions, PrepareSandboxOutput, SandboxProvisioningMode,
        },
        CreateSnapshotError, DestroySandboxError, PrepareSandboxError,
    },
    worker::{self, CreateSnapshotResult, PrepareSandboxResult, Sandbox, WorkerId},
    FunctionId, FunctionMetadataStore, Request, Response, SandboxId,
};

/// Pool-side pending control operation tracked for a live [`Worker`].
///
/// This is deliberately a single slot, not a queue: in the current design, [`SandboxPool`]
/// only reasons about __one__ distinct pending/in-flight control operation per [`Worker`].
/// Compatible duplicate requests may be coalesced by the caller instead of overwriting this entry.
///
/// A [`Worker`] with one of these entries must remain _Active_ until the corresponding
/// control flow is finalized (or otherwise cleaned up on failure/reap).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingControlOp {
    /// A sandbox preparation request has been issued to the [`Worker`] and
    /// [`SandboxPool`] is awaiting either its completion or its cleanup.
    PrepareSandbox,
    /// A sandbox destruction request has been issued to the [`Worker`] and
    /// [`SandboxPool`] is awaiting its completion.
    DestroySandbox,
    /// A snapshot request has been issued to the [`Worker`] and [`SandboxPool`]
    /// is awaiting its completion.
    Snapshot,
}

/// Pending per-request context for an in-flight `PrepareSandbox` operation.
///
/// This stores Pool-side data that must remain associated with the selected
/// [`Worker`] until sandbox preparation is finalized or canceled during reap.
#[derive(Debug, Default)]
pub(super) struct PendingSandboxPrep {
    /// Optional caller-provided best-effort initial keep-alive hint for the
    /// freshly prepared sandbox.
    initial_keepalive_hint: Option<Duration>,
    /// Optional waiter to notify once the preparation completes or fails.
    respond_to:
        Option<oneshot::Sender<::std::result::Result<PrepareSandboxOutput, PrepareSandboxError>>>,
}

/// Pending per-request context for an in-flight `DestroySandbox` operation.
///
/// This stores Pool-side data that must remain associated with the selected
/// [`Worker`] until sandbox destruction is finalized or canceled during reap.
#[derive(Debug, Default)]
pub(super) struct PendingSandboxDestr {
    /// The snapshot-removal intent chosen for this in-flight destroy request.
    ///
    /// This is fixed by the first `DestroySandbox` request that started the
    /// operation. Later coalesced requests do not change it; they only subscribe
    /// to the eventual outcome.
    remove_persisted_snapshot: bool,
    /// All coalesced waiters awaiting the result of this sandbox destruction
    /// request.
    respond_to: SmallVec<
        [oneshot::Sender<::std::result::Result<DestroySandboxOutput, DestroySandboxError>>; 1],
    >,
}

impl PendingSandboxDestr {
    /// Reply successfully to all coalesced `DestroySandbox` waiters.
    ///
    /// `persistent_snapshot_exists` reports whether a reusable snapshot still
    /// exists after the Worker has been reaped.
    ///
    /// # Note
    ///
    /// If the request had asked to remove persisted snapshot state, but reap
    /// still produced a snapshotted sandbox, this helper downgrades the outcome
    /// to an internal failure instead of reporting success.
    #[cold]
    pub(super) fn reply_destroy_success(self, persistent_snapshot_exists: bool) {
        if persistent_snapshot_exists && self.remove_persisted_snapshot {
            self.reply_destroy_internal_failure("failed to remove persisted snapshot");
            warn!("Failed to remove persisted snapshot for `DestroySandbox`");
            return;
        }
        for tx in self.respond_to {
            let _ = tx.send(Ok(DestroySandboxOutput {
                persistent_snapshot_exists,
            }));
        }
    }

    /// Reply to all coalesced `DestroySandbox` waiters with a Worker-reported
    /// failure.
    ///
    /// This is used when the Worker finishes reap processing with
    /// `Err(worker::Error, ..)`, meaning the destroy request reached the Worker
    /// but did not complete successfully there.
    #[cold]
    pub(super) fn reply_destroy_worker_failure(self, err: &worker::Error) {
        let msg = err.to_string().into_boxed_str();
        for tx in self.respond_to {
            let _ = tx.send(Err(DestroySandboxError::Worker(msg.clone())));
        }
    }

    /// Reply to all coalesced `DestroySandbox` waiters with a Pool-side
    /// internal failure.
    ///
    /// This is used for failures outside unrelated to Worker's result; e.g.,
    /// failing to join the Worker task or detecting a contradiction in Pool's
    /// reap-time postconditions.
    #[cold]
    pub(super) fn reply_destroy_internal_failure(self, msg: impl Into<Box<str>>) {
        let msg = msg.into();
        for tx in self.respond_to {
            let _ = tx.send(Err(DestroySandboxError::Internal {
                msg: msg.clone(),
                err: None,
            }));
        }
    }
}

/// Pool-side snapshotting context for a [`Worker`] that was _Idle_ when
/// [`CreateSnapshot`] was requested.
///
/// When snapshotting temporarily activates an _Idle_ [`Worker`], [`SandboxPool`]
/// stops its keep-alive timer and records both the pause timestamp and the
/// original deadline. On successful completion, this information is used to
/// reinstall the timer with the remaining idle time shifted forward by the
/// time spent handling the snapshot request.
///
/// If no such context exists, the snapshot request is treated as having been
/// issued to an already _Active_ [`Worker`].
///
/// [`CreateSnapshot`]: crate::worker::ControlMessage::CreateSnapshot
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PausedTimerInfo {
    /// Timestamp at which the keep-alive timer was stopped.
    ts_pause: Instant,
    /// Original deadline tracked by the keep-alive timer before it was paused.
    deadline: Instant,
}

/// Pending per-request context for an in-flight `CreateSnapshot` operation.
///
/// This stores Pool-side data that must remain associated with the target
/// [`Sandbox`] until snapshot creation is finalized or canceled during reap.
///
/// [`Sandbox`]: crate::worker::runtime::Sandbox
#[derive(Debug, Default)]
pub(super) struct PendingSnapshot {
    /// If present, the target [`Worker`] had previously been _Idle_ and its
    /// paused keep-alive timer must be reinstalled on completion.
    timer_info: Option<PausedTimerInfo>,
    /// All coalesced waiters awaiting the result of this snapshot request.
    respond_to: SmallVec<[oneshot::Sender<::std::result::Result<(), CreateSnapshotError>>; 1]>,
}

/// Pending control-plane bookkeeping owned by [`SandboxPool`].
///
/// This groups transient state related to control operations:
/// - per-[`Worker`] pending control operations that affect lifecycle/FSM decisions
/// - per-request waiters/subscribers that must be notified when a control operation completes
///
/// It deliberately does not store durable data, only in-memory coordination
/// state for live [`Worker`]s and pending/in-flight control-plane requests.
#[derive(Debug, Default)]
pub(super) struct PendingControlRequests {
    /// Pool-side pending control operation by [`WorkerId`]. A [`Worker`] with an entry here
    /// must remain _Active_ until the corresponding control operation has been completed.
    pub(super) op_by_worker: HashMap<WorkerId, PendingControlOp, crate::BuildHasher>,

    /// Pending sandbox preparation requests by [`WorkerId`], including any
    /// waiter to notify and any extra request context needed on completion.
    sandbox_preps: HashMap<WorkerId, PendingSandboxPrep, crate::BuildHasher>,

    /// Pending sandbox destruction requests by [`WorkerId`], including any
    /// waiter to notify and any extra request context needed on completion.
    sandbox_destrs: HashMap<WorkerId, PendingSandboxDestr, crate::BuildHasher>,

    /// Pending snapshot creation requests by [`SandboxId`], including any
    /// coalesced waiters and Pool-side context needed on completion.
    snapshots: HashMap<SandboxId, PendingSnapshot, crate::BuildHasher>,
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
    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // API: Control Request Handling
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline(never)]
    #[cold]
    #[instrument(level = Level::INFO, skip(self))]
    pub(super) async fn handle_control(&mut self, msg: PoolControlMessage) -> Result<()> {
        match msg {
            PoolControlMessage::PrepareSandbox {
                function_id,
                options,
                respond_to,
            } => {
                if let Err((err, maybe_respond_to)) = self
                    .request_sandbox_prep(function_id, options, Some(respond_to))
                    .await
                {
                    if let Some(respond_to) = maybe_respond_to {
                        if let Err(res) = respond_to.send(Err(err)) {
                            // SAFETY: We just wrapped `err` in `Err()`
                            warn!(error = ?res.unwrap_err(), "Failed to respond to SandboxPoolRef");
                        }
                    } else {
                        error!(error = ?err, "Failed to request Sandbox preparation: {err:#}");
                        // Let's not convert it back to `super::Error` to return it;
                        // just log it here as is, and forget about it (for now) (?)
                    }
                }
            }
            PoolControlMessage::DestroySandbox {
                target,
                options,
                respond_to,
            } => match target {
                api::SandboxSelector::SandboxId(sandbox_id) => {
                    if let Err((err, maybe_respond_to)) = self
                        .request_sandbox_destr(sandbox_id, options, Some(respond_to))
                        .await
                    {
                        // TODO
                        if let Some(respond_to) = maybe_respond_to {
                            if let Err(res) = respond_to.send(Err(err)) {
                                // SAFETY: We just wrapped `err` in `Err()`
                                warn!(error = ?res.unwrap_err(), "Failed to respond to SandboxPoolRef");
                            }
                        } else {
                            error!(error = ?err, "Failed to request Sandbox destruction: {err:#}");
                            // Let's not convert it back to `super::Error` to return it;
                            // just log it here as is, and forget about it (for now) (?)
                        }
                    }
                }
            },
            PoolControlMessage::ListSandboxes { filter, respond_to } => todo!(),
            PoolControlMessage::CreateSnapshot {
                sandbox_id,
                respond_to,
            } => {
                if let Err((err, maybe_respond_to)) = self
                    .request_snapshotting(sandbox_id, Some(respond_to))
                    .await
                {
                    if let Some(respond_to) = maybe_respond_to {
                        if let Err(res) = respond_to.send(Err(err)) {
                            // SAFETY: We just wrapped `err` in `Err()`
                            warn!(error = ?res.unwrap_err(), "Failed to respond to SandboxPoolRef");
                        }
                    } else {
                        error!(error = ?err, "Failed to request snapshotting: {err:#}");
                        // Let's not convert it back to `super::Error` to return it;
                        // just log it here as is, and forget about it (for now) (?)
                    }
                }
            }
        }
        Ok(())
    }

    #[instrument(
        level = Level::DEBUG,
        skip(self, maybe_respond_to),
        fields(response.waiter = maybe_respond_to.is_some())
    )]
    #[allow(clippy::type_complexity)]
    async fn request_sandbox_prep(
        &mut self,
        function_id: FunctionId,
        options: PrepareSandboxOptions,
        maybe_respond_to: Option<
            oneshot::Sender<::std::result::Result<PrepareSandboxOutput, PrepareSandboxError>>,
        >,
    ) -> ::std::result::Result<
        (),
        (
            PrepareSandboxError,
            Option<
                oneshot::Sender<::std::result::Result<PrepareSandboxOutput, PrepareSandboxError>>,
            >,
        ),
    > {
        if !self.store.function_exists(&function_id) {
            return Err((
                PrepareSandboxError::UnknownFunction(function_id),
                maybe_respond_to,
            ));
        }

        let mem_requested = self.store.function_memory(&function_id);

        // Choose the starting sandbox state for the new Worker.
        let sandbox = match options.mode {
            SandboxProvisioningMode::PreferSnapshot => self.pick_orphan_snapshot(&function_id),
            SandboxProvisioningMode::RequireSnapshot => {
                match self.pick_orphan_snapshot(&function_id) {
                    Some(sb) => Some(sb),
                    None => return Err((PrepareSandboxError::OutOfSnapshots, maybe_respond_to)),
                }
            }
            SandboxProvisioningMode::ForceFresh => None,
        };

        // If there is enough free memory to spawn a new Worker, (attempt to) do so.
        // Otherwise fail the request, after possibly triggering the eviction policy.
        let worker_id =
            if self.memory.inuse + self.memory.reclaimed + mem_requested <= self.memory.total {
                // Reaching here means that there is free memory to spawn a new Worker.
                match self.spawn_new_worker(&function_id, sandbox) {
                    Ok(worker_id) => worker_id,
                    Err(err) => {
                        return Err((
                            PrepareSandboxError::Internal {
                                msg: "failed while creating new Worker".into(),
                                err: Some(err),
                            },
                            maybe_respond_to,
                        ))
                    }
                }
            } else {
                // If we have reached the configured memory eviction threshold, trigger
                // the eviction policy to eventually get below the threshold again.
                if self.memory.inuse >= self.memory.eviction_thres_hi {
                    self.trigger_eviction_policy().await;
                }
                return Err((PrepareSandboxError::OutOfMemory, maybe_respond_to));
            };

        // If we have reached the configured memory eviction threshold, trigger
        // the eviction policy to eventually get below the threshold again.
        if self.memory.inuse >= self.memory.eviction_thres_hi {
            self.trigger_eviction_policy().await;
        }

        // New Worker is about to handle a control op, thus _Active_.
        if let Err(err) = self.store.insert_active_worker(worker_id, &function_id) {
            // TODO: Interrupt Worker (not normal `ShutDown`) to reap it?
            return Err((
                PrepareSandboxError::Internal {
                    msg: "metadata store: failed to insert _Active_ Worker".into(),
                    err: Some(Error::Metadata(Box::new(err))),
                },
                maybe_respond_to,
            ));
        }

        // Allocate a cpuset to initialize the Sandbox on.
        let cpuset = self.cpuset.alloc(worker_id);

        // Install bookkeeping for pending op (before the Worker can reply)
        let _old_op = self
            .pending
            .op_by_worker
            .insert(worker_id, PendingControlOp::PrepareSandbox);
        debug_assert!(_old_op.is_none(), "new Worker should not have pending op");

        let _old = self.pending.sandbox_preps.insert(
            worker_id,
            PendingSandboxPrep {
                initial_keepalive_hint: options.initial_keepalive_hint,
                respond_to: maybe_respond_to,
            },
        );
        debug_assert!(_old.is_none(), "new Worker should not have pending op");

        // After bookkeeping necessary info about pending ops/senders, send the ctrl msg to Worker
        let worker = self
            .worker_handles
            .get(&worker_id)
            .expect("new Worker must be tracked by now");
        if let Err(err) = worker.prepare_sandbox(cpuset).await {
            error!(error = ?err, %worker_id, %function_id, "Failed to communicate with Worker: {err:#}");

            // Release the cpuset that was just allocated.
            if let Err(cpuset_err) = self.cpuset.release(worker_id) {
                error!(
                    error = ?cpuset_err, %worker_id, %function_id,
                    "Failed to release cpuset after PrepareSandbox send failure: {cpuset_err:#}",
                );
            }

            let maybe_respond_to = self
                .pending
                .sandbox_preps
                .remove(&worker_id)
                .and_then(|op| op.respond_to);
            // Keep the Worker _Active_, "poisoned", to avoid future ctrl op
            // assignments, and let `Self::reap_worker()` do the cleanup.
            //let _old = self.pending.op_by_worker.remove(&worker_id);
            //debug_assert_eq!(_old, Some(PendingControlOp::PrepareSandbox));

            // Best-effort rollback: if the Worker is already finished, reap it
            // directly; otherwise wait for its ReapMe message.
            if worker.is_finished()
                && let Err(reap_err) = self.reap_worker(worker_id, function_id.clone()).await
            {
                error!(
                    error = ?reap_err, %worker_id, %function_id,
                    "Failed to reap Worker after send failure"
                );
            }

            // FIXME(ckatsak): For now, if the Worker dies abruptly and never sends
            // `ReapMe`, it remains _Active_ and stale, along with its allocated
            // resources. Check `Self::reap_finished_workers` for more on this. TODO

            return Err((
                PrepareSandboxError::Internal {
                    msg: format!("failed to communicate with new Worker: {err:#}").into_boxed_str(),
                    err: None,
                },
                maybe_respond_to,
            ));
        }

        Ok(())
    }

    #[instrument(
        level = Level::DEBUG,
        skip(self, maybe_respond_to),
        fields(response.waiter = maybe_respond_to.is_some())
    )]
    #[allow(clippy::type_complexity)]
    async fn request_sandbox_destr(
        &mut self,
        sandbox_id: SandboxId,
        options: DestroySandboxOptions,
        maybe_respond_to: Option<
            oneshot::Sender<::std::result::Result<DestroySandboxOutput, DestroySandboxError>>,
        >,
    ) -> ::std::result::Result<
        (),
        (
            DestroySandboxError,
            Option<
                oneshot::Sender<::std::result::Result<DestroySandboxOutput, DestroySandboxError>>,
            >,
        ),
    > {
        /// To avoid hurting readability by repeatedly returning some sort of `Err((err, sender))`.
        macro_rules! return_md_err_and_chan {
            ($err:ident) => {
                return Err((
                    DestroySandboxError::Internal {
                        msg: "metadata store error".into(),
                        err: Some(Error::Metadata(Box::new($err))),
                    },
                    maybe_respond_to,
                ))
            };
        }

        // First, search for a Worker that currently owns the Sandbox.
        let worker_id = match self.worker_by_sandbox.get(&sandbox_id) {
            // If the Sandbox is currently owned by a Worker, make sure
            // it iss eligible for the control op before proceeding.
            Some(worker_id) => {
                debug_assert_eq!(
                    sandbox_id,
                    self.worker_handles
                        .get(worker_id)
                        .expect("Worker exists since SandboxId->WorkerId exists")
                        .sandbox_id()
                        .expect("Worker's SandboxId is known since SandboxId->WorkerId exists")
                );
                match self.pending.op_by_worker.get(worker_id) {
                    None => {} // No pending control op; we may proceed
                    Some(PendingControlOp::DestroySandbox) => {
                        // Coalesce duplicate sandbox destruction requests
                        let pending_op = self.pending.sandbox_destrs.get_mut(worker_id).expect(
                            "pending op context should exist while PendingControlOp::DestroySandbox",
                        );
                        //pending_op.remove_persisted_snapshot |= options.remove_persisted_snapshot;
                        // ^^ NOTE: Snapshot removal is raced, not coalesced: the first RPC decides
                        // snapshot's fate, while later RPCs only subscribe to the outcome (which
                        // is also why `remove_persisted_snapshot` is preserved in op's context).
                        if let Some(respond_to) = maybe_respond_to {
                            pending_op.respond_to.push(respond_to);
                        }
                        return Ok(());
                    }
                    Some(
                        pending_op @ PendingControlOp::PrepareSandbox
                        | pending_op @ PendingControlOp::Snapshot,
                    ) => {
                        warn!(
                            pending_control_op = ?pending_op,
                            "Rejecting incoming sandbox destruction request due to pending op"
                        );
                        return Err((
                            DestroySandboxError::Busy(
                                format!("PendingControlOp::{pending_op:?}").into_boxed_str(),
                            ),
                            maybe_respond_to,
                        ));
                    }
                }
                *worker_id
            }

            // If the Sandbox is not currently owned by a Worker, it may exist as
            // a snapshot. If the RPC requests snapshot destruction, look for it.
            None if options.remove_persisted_snapshot => {
                // Search among all snapshots.
                let Some((function_id, idx)) =
                    self.snapshots.iter().find_map(|(function_id, snapshots)| {
                        snapshots
                            .iter()
                            .find_position(|sb| sb.id() == sandbox_id)
                            .map(|(idx, _)| (function_id.clone(), idx))
                    })
                else {
                    // If Sandbox not found among snapshots, error out.
                    return Err((DestroySandboxError::NotFound(sandbox_id), maybe_respond_to));
                };

                // Remove it from `self.snapshots` and give it to a newly spawned Worker
                let sandbox = self
                    .snapshots
                    .get_mut(function_id.as_str())
                    .expect("FunctionId just found by scanning `self.snapshots`")
                    .remove(idx);

                match self._spawn_untracked_worker(
                    &function_id,
                    Some(sandbox),
                    WorkerMemAccounting::Uncharged,
                ) {
                    Ok(worker_id) => {
                        // New Worker is immediately _Dying_.
                        if let Err(err) = self.store.insert_dying_worker(worker_id, &function_id) {
                            // TODO: Interrupt Worker (not normal `ShutDown`) to reap it?
                            return_md_err_and_chan!(err)
                        }
                        // Send the newly-spawned _Dying_ Worker a `DestroySandbox` message...
                        if let Err(err) = self
                            .worker_handles
                            .get(&worker_id)
                            .expect("Worker just spawned")
                            .destroy()
                            .await
                        {
                            // TODO: Sandbox leaked, though highly unlikely. Error handling
                            // needs a revamp all around Pool (and probably the crate).
                            warn!(
                                error = ?err, %function_id, %sandbox_id, %worker_id,
                                "Failed to contact dedicated _Dying_ Worker: {err:#}"
                            );
                            return Err((
                                DestroySandboxError::Internal {
                                    msg: format!("failed to contact dedicated Worker: {err:#}")
                                        .into_boxed_str(),
                                    err: Some(Error::Worker(err)),
                                },
                                maybe_respond_to,
                            ));
                        }
                        // ...and return its ID for the next phase.
                        worker_id
                    }
                    Err(err) => {
                        error!(
                            error = ?err, %sandbox_id, %function_id,
                            "Failed to spawn new Worker: {err:#}"
                        );
                        // `_spawn_untracked_worker()` restores any returned Sandbox
                        // on synchronous spawn failure. Failures after successful
                        // spawn are handled by the normal Worker exit/reap path.
                        return Err((
                            DestroySandboxError::Internal {
                                msg: "failed to spawn new Worker".into(),
                                err: Some(err),
                            },
                            maybe_respond_to,
                        ));
                    }
                }
            }

            // If the Sandbox is not currently owned by a Worker, and the RPC
            // request is not interested in snapshot destruction, fail it.
            None => return Err((DestroySandboxError::NotFound(sandbox_id), maybe_respond_to)),
        };

        match self.store.find_idle_worker(worker_id) {
            // If Worker is _Idle_, send it a `DestroySandbox` message after
            // bookkeeping accordingly, and let `reap_worker()` finish the job.
            Ok(Some(function_id)) => {
                if let Err(err) = self
                    .destroy_idle_worker(worker_id, &function_id, options.remove_persisted_snapshot)
                    .await
                {
                    // TODO: Sandbox leaked, though highly unlikely. Error handling
                    // needs a revamp all around Pool (and probably the crate).
                    error!(
                        error = ?err, %sandbox_id, %function_id, %worker_id,
                        "Failed to destroy _Idle_ Worker: {err:#}"
                    );
                    return Err((
                        DestroySandboxError::Internal {
                            msg: format!("failed to destroy Idle Worker: {err:#}").into_boxed_str(),
                            err: Some(err),
                        },
                        maybe_respond_to,
                    ));
                }
            }

            Ok(None) => match self.store.find_active_worker(worker_id) {
                // If Worker is _Active_, and request allows it, send it a `DestroySandbox`
                // msg after bookkeeping accordingly, and let `reap_worker()` finish the job.
                Ok(Some(function_id)) => {
                    // If the request does not wish to destroy "active sandboxes", fail it.
                    if !options.allow_if_active {
                        info!(
                            %sandbox_id, %function_id, %worker_id, request.options = ?options,
                            "Rejecting DestroySandbox request for Active Worker"
                        );
                        return Err((
                            DestroySandboxError::Busy("Worker is Active".into()),
                            maybe_respond_to,
                        ));
                    }
                    // Otherwise, destroy the Worker.
                    if let Err(err) = self
                        .destroy_active_worker(
                            worker_id,
                            &function_id,
                            options.remove_persisted_snapshot,
                        )
                        .await
                    {
                        // TODO: Sandbox leaked, though highly unlikely. Error handling
                        // needs a revamp all around Pool (and probably the crate).
                        error!(
                            error = ?err, %sandbox_id, %function_id, %worker_id,
                            "Failed to destroy _Active_ Worker: {err:#}"
                        );
                        return Err((
                            DestroySandboxError::Internal {
                                msg: format!("failed to destroy Active Worker: {err:#}")
                                    .into_boxed_str(),
                                err: Some(err),
                            },
                            maybe_respond_to,
                        ));
                    }
                }

                Ok(None) => match self.store.find_dying_worker(worker_id) {
                    Ok(Some(_)) => {
                        // If Worker is _Dying_ (be it just spawned or already
                        // existing), just sign up and wait for the upcoming
                        // `reap_worker()` to finish the job (and respond).
                    }
                    Ok(None) => {
                        // Known Worker in unknown state? Probably some serious BUG
                        error!(%sandbox_id, %worker_id, "Unknown Worker state");
                        return Err((
                            DestroySandboxError::Internal {
                                msg: "unknown Worker state".into(),
                                err: None,
                            },
                            maybe_respond_to,
                        ));
                    }
                    Err(err) => return_md_err_and_chan!(err),
                },
                Err(err) => return_md_err_and_chan!(err),
            },
            Err(err) => return_md_err_and_chan!(err),
        }

        // Reaching here means that we should now be just waiting for a Worker to be reaped.

        // Keep track of the newly pending control op.
        let _old_op = self
            .pending
            .op_by_worker
            .insert(worker_id, PendingControlOp::DestroySandbox);
        debug_assert!(
            _old_op.is_none(),
            "cleanup BUG? should have been rejected or coalesced earlier"
        );
        let mut op_ctx = PendingSandboxDestr {
            remove_persisted_snapshot: options.remove_persisted_snapshot,
            respond_to: SmallVec::new(),
        };
        if let Some(respond_to) = maybe_respond_to {
            op_ctx.respond_to.push(respond_to);
        }
        let _old_op = self.pending.sandbox_destrs.insert(worker_id, op_ctx);
        debug_assert!(
            _old_op.is_none(),
            "cleanup BUG? should have been rejected or coalesced earlier"
        );

        Ok(())
    }

    /// Begin destroying an _Idle_ [`Worker`] as part of `DestroySandbox`.
    ///
    /// - If `remove_snapshot` is `false`, this falls back to normal
    ///   [`Self::decommission_idle_worker`], preserving any reusable snapshot
    ///   state.
    /// - If `remove_snapshot` is `true`, it performs the _Idle_ -> _Dying_
    ///   Pool-side bookkeeping and then sends
    ///   [`worker::ControlMessage::DestroySandbox`] to the [`Worker`].
    ///
    /// Completion is asynchronous; this helper only initiates the transition.
    /// Final request completion happens later in [`Self::reap_worker`].
    #[instrument(level = Level::DEBUG, skip(self))]
    async fn destroy_idle_worker(
        &mut self,
        worker_id: WorkerId,
        function_id: &FunctionId,
        remove_snapshot: bool,
    ) -> Result<()> {
        if !remove_snapshot {
            return self.decommission_idle_worker(worker_id, function_id).await;
        }

        self._idle_worker_to_dying(worker_id, function_id)?;

        self.worker_handles
            .get(&worker_id)
            .expect("Worker exists, since state transition just succeeded")
            .destroy()
            .await
            .map_err(Error::Worker)
    }

    /// Begin destroying an _Active_ [`Worker`] as part of `DestroySandbox`.
    ///
    /// - If `remove_snapshot` is `false`, this falls back to normal
    ///   [`Self::deactivate_worker`] behavior with no new keep-alive deadline,
    ///   preserving any reusable snapshot state.
    /// - If `remove_snapshot` is `true`, it performs the _Active_ -> _Dying_
    ///   Pool-side bookkeeping and then sends
    ///   [`worker::ControlMessage::DestroySandbox`] to the [`Worker`].
    ///
    /// Completion is asynchronous; this helper only initiates the transition.
    /// Final request completion happens later in [`Self::reap_worker`].
    #[instrument(level = Level::DEBUG, skip(self))]
    async fn destroy_active_worker(
        &mut self,
        worker_id: WorkerId,
        function_id: &FunctionId,
        remove_snapshot: bool,
    ) -> Result<()> {
        if !remove_snapshot {
            return self.deactivate_worker(worker_id, function_id, None).await;
        }

        self._active_worker_to_dying(worker_id, function_id)?;

        self.worker_handles
            .get(&worker_id)
            .expect("Worker exists, since state transition just succeeded")
            .destroy()
            .await
            .map_err(Error::Worker)
    }

    /// # Errors
    ///
    /// In case of failure, this method returns both a related [`CreateSnapshotError`]
    /// as well as the [`oneshot::Sender`] it was provided (if any), thus allowing its
    /// caller to potentially respond to any waiter(s) properly.
    ///
    // In case of an error:
    // - if <code>maybe_respond_to.[is_some()]</code>, the error is forwarded
    //   to the waiter while this method returns `Ok(())`.
    // - if <code>maybe_respond_to.[is_none()]</code>, the error is returned.
    //
    // [is_none()]: std::option::Option::is_none
    // [is_some()]: std::option::Option::is_some
    #[instrument(
        level = Level::DEBUG,
        skip(self, maybe_respond_to),
        fields(response.waiter = maybe_respond_to.is_some())
    )]
    #[allow(clippy::type_complexity)]
    async fn request_snapshotting(
        &mut self,
        sandbox_id: SandboxId,
        maybe_respond_to: Option<oneshot::Sender<::std::result::Result<(), CreateSnapshotError>>>,
    ) -> ::std::result::Result<
        (),
        (
            CreateSnapshotError,
            Option<oneshot::Sender<::std::result::Result<(), CreateSnapshotError>>>,
        ),
    > {
        /// To avoid hurting readability by repeatedly returning some sort of `Err((err, sender))`.
        macro_rules! return_md_err_and_chan {
            ($err:ident) => {
                return Err((
                    CreateSnapshotError::Internal {
                        msg: "metadata store error".into(),
                        err: Some(Arc::new(Error::Metadata(Box::new($err)))),
                    },
                    maybe_respond_to,
                ))
            };
        }

        if self.worker_aux.config.snapshot_creation_time == SnapshotCreationTime::Never {
            return Err((CreateSnapshotError::Disabled, maybe_respond_to));
        }

        // First, find the Worker that owns this Sandbox, if any
        let Some(worker_id) = self.worker_by_sandbox.get(&sandbox_id) else {
            info!(%sandbox_id, "Sandbox either never existed or already snapshotted");
            return Err((
                CreateSnapshotError::UnassignedSandboxId(sandbox_id),
                maybe_respond_to,
            ));
        };
        let function_id = self
            .worker_handles
            .get(worker_id)
            .expect("exists in `sandboxes_by_worker`; should be here too")
            .function_id();

        // Check for any other control ops pending
        match self.pending.op_by_worker.get(worker_id) {
            None => {} // none pending; we can continue
            Some(PendingControlOp::Snapshot) => {
                // Duplicate snapshotting requests are coalesced.
                if let Some(respond_to) = maybe_respond_to {
                    self.pending
                        .snapshots
                        .get_mut(&sandbox_id)
                        .expect("snapshotting context must exist while PendingControlOp::Snapshot")
                        .respond_to
                        .push(respond_to);
                }
                return Ok(());
            }
            Some(PendingControlOp::PrepareSandbox) => {
                // Receiving a CreateSandbox while still preparing the Sandbox should not be
                // possible, since no caller knows the SandboxId yet. However, callers might
                // send it referring to stale SandboxIds, possibly now reused. This exposes
                // a race: under pending preparation, Pool might have already processed the
                // SandboxId update from the associated Worker, and thus reach here. In this
                // case, let's just log and reject this request. We probably need to think
                // all this carefully if we ever want to support queuing control op requests.
                warn!("Rejecting CreateSnapshot request due to pending PrepareSandbox");
                return Err((
                    CreateSnapshotError::Busy("being created/warmed-up".into()),
                    maybe_respond_to,
                ));
            }
            Some(PendingControlOp::DestroySandbox) => {
                warn!("Rejecting CreateSnapshot request due to pending DestroySandbox");
                return Err((
                    CreateSnapshotError::Busy("being destroyed".into()),
                    maybe_respond_to,
                ));
            }
        }
        // NOTE: Future control op kinds must be handled explicitly above; either coalesce, reject,
        // or queue them. Do not silently overwrite the existing pending control op for a Worker.

        // Stop any keep-alive timer associated with the Worker
        self.keepalive.stop_timer(*worker_id);
        let ts_pause = Instant::now();

        // If the Worker is currently (or soon) available (i.e., _Idle_ or _Active_), make
        // sure it is _Active_ from now on, and send it a new CreateSnapshot message.
        match self.store.remove_idle_worker(*worker_id, function_id) {
            Ok(true) => {
                // If Worker is _Idle_, make it _Active_ and enqueue a CreateSnapshot message.
                if let Err(err) = self.store.insert_active_worker(*worker_id, function_id) {
                    return_md_err_and_chan!(err);
                }
            }
            Ok(false) => match self.store.find_active_worker(*worker_id) {
                // If Worker is _Active_, just enqueue a CreateSnapshot message.
                Ok(Some(_fid)) => debug_assert_eq!(&_fid, function_id),
                Ok(None) => match self.store.find_dying_worker(*worker_id) {
                    // If Worker is _Dying_, fail the operation.
                    Ok(Some(_fid)) => {
                        debug_assert_eq!(&_fid, function_id);
                        info!(%worker_id, %sandbox_id, "Denying snapshot creation request for Dying Worker");
                        return Err((
                            CreateSnapshotError::WorkerDying {
                                worker_id: *worker_id,
                                sandbox_id,
                            },
                            maybe_respond_to,
                        ));
                    }
                    Ok(None) => {
                        // If a known Worker is in unknown state, there is probably some serious BUG
                        error!(%worker_id, %sandbox_id, "Unknown Worker state");
                        return Err((
                            CreateSnapshotError::Internal {
                                msg: "unknown Worker state".into(),
                                err: None,
                            },
                            maybe_respond_to,
                        ));
                    }
                    Err(err) => return_md_err_and_chan!(err),
                },
                Err(err) => return_md_err_and_chan!(err),
            },
            Err(err) => return_md_err_and_chan!(err),
        }

        // Reaching here means that the Worker had so far been either _Idle_ or _Active_; enqueue
        // a CreateSnapshot message

        // Only so-far-_Idle_ Workers have deadlines, which will be useful when processing
        // the corresponding `worker::OutboundMessage` response.
        let old_deadline = self.worker_deadlines.remove(worker_id); // timer stopped earlier
        let timer_info = old_deadline.map(|deadline| PausedTimerInfo { ts_pause, deadline });

        if let Err(err) = self
            .worker_handles
            .get(worker_id)
            .expect("already retrieved earlier")
            .create_snapshot(sandbox_id.clone())
            .await
        {
            error!(error = ?err, %worker_id, "Failed to communicate with Worker: {err:#}");
            return Err((
                CreateSnapshotError::Internal {
                    msg: err.to_string().into_boxed_str(),
                    err: Some(Arc::new(Error::Worker(err))),
                },
                maybe_respond_to,
            ));
        }

        // Keep track of the newly pending control op.
        let _old_ctrl_op = self
            .pending
            .op_by_worker
            .insert(*worker_id, PendingControlOp::Snapshot);
        debug_assert!(
            _old_ctrl_op.is_none(),
            "Currently, we should be tracking at most one pending control op per Worker"
        );

        // Store Pool-side context (including any waiters) to find later, on completion.
        let mut pending_op = PendingSnapshot {
            timer_info,
            respond_to: SmallVec::new(),
        };
        if let Some(respond_to) = maybe_respond_to {
            pending_op.respond_to.push(respond_to);
        }
        let _old_pending = self.pending.snapshots.insert(sandbox_id, pending_op);
        debug_assert!(
            _old_pending.is_none(),
            "pending snapshotting op context should not already exist"
        );

        Ok(())
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Op Finalization
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Cancel or detach any pending control-operation bookkeeping associated
    /// with a [`Worker`] that is about to be reaped.
    ///
    /// This helper:
    /// - removes the pending-op marker from `pending.op_by_worker`,
    /// - fails any pending `CreateSnapshot` or `PrepareSandbox` waiters, since
    ///   those operations did not complete normally,
    /// - removes and returns any pending `DestroySandbox` context so that
    ///   [`Self::reap_worker`] can complete that request based on the
    ///   [`Worker`]'s final exit status.
    ///
    /// The returned [`PendingSandboxDestr`] is not completed here because
    /// `DestroySandbox` completion depends on the result of joining the
    /// [`Worker`] and inspecting what Sandbox state, if any, was handed back.
    pub(super) async fn finalize_control_ops(
        &mut self,
        worker_id: WorkerId,
        maybe_sandbox_id: Option<&SandboxId>,
    ) -> Option<PendingSandboxDestr> {
        let _ctrl_op = self.pending.op_by_worker.remove(&worker_id);

        // Defensively cancel any pending snapshotting requests, to prevent hanging waiters
        if let Some(sandbox_id) = maybe_sandbox_id
            && let Some(pending_op) = self.pending.snapshots.remove(sandbox_id)
        {
            debug_assert_eq!(
                _ctrl_op,
                Some(PendingControlOp::Snapshot),
                "BUG: expected pending context for CreateSnapshot; found for {_ctrl_op:?}"
            );
            // This path is probably abnormal; pending control ops
            // should otherwise have been cleaned up earlier.
            pending_op.respond_to.into_iter().for_each(|tx| {
                let _ = tx.send(Err(CreateSnapshotError::Internal {
                    msg: "reaping Worker; probably abnormal termination".into(),
                    err: None,
                }));
            });
        }

        // Cancel any pending PrepareSandbox control operation.
        if let Some(op) = self.pending.sandbox_preps.remove(&worker_id) {
            debug_assert_eq!(
                _ctrl_op,
                Some(PendingControlOp::PrepareSandbox),
                "BUG: expected pending context for PrepareSandbox; found for {_ctrl_op:?}"
            );
            if let Some(tx) = op.respond_to {
                let _ = tx
                    .send(Err(PrepareSandboxError::Internal {
                        msg: "reaping Worker; probably abnormal termination".into(),
                        err: None,
                    }))
                    .inspect_err(|_| {
                        warn!("Failed to notify subscriber of PrepareSandbox cancellation")
                    })
                    .ok();
            };
        }

        // Complete any pending DestroySandbox control operation.
        let maybe_destr_op_ctx = self.pending.sandbox_destrs.remove(&worker_id);
        if maybe_destr_op_ctx.is_some() {
            debug_assert_eq!(
                _ctrl_op,
                Some(PendingControlOp::DestroySandbox),
                "BUG: expected pending context for DestroySandbox; found for {_ctrl_op:?}"
            );
        }
        maybe_destr_op_ctx
    }

    #[instrument(level = Level::TRACE, skip_all)]
    pub(super) async fn finalize_sandbox_preparation(
        &mut self,
        resp: Box<PrepareSandboxResult>,
    ) -> Result<()> {
        let now = Instant::now();

        trace!(?resp);
        let PrepareSandboxResult {
            worker_id,
            function_id,
            result,
        } = *resp;

        // NOTE: Make sure that failures in Worker state manipulation don't leave subscribed
        // channels (and their associated actors and clients) hanging; always respond.

        let mut ret = Ok(());
        // In any case, release cpuset, which should still be allocated.
        if let Err(err) = self.cpuset.release(worker_id) {
            error!(error = ?err, %worker_id, %function_id, "Parking Active Worker: {err:#}");
            // Reaching here after Sandbox prep, there _must_ be some cpuset allocated,
            // otherwise the control op should not have even been dispatched; i.e., if no
            // cpuset had been allocated, our resource tracking logic is probably wrong!
            ret = Err(Error::CpuSet(err))
        }

        // If Sandbox preparation was successful, update Worker's state _Active_ -> _Idle_,
        // and deallocate the assigned transient (cpuset) resources.
        // However, if Sandbox preparation failed, we should be expecting a ReapMe message
        // from that Worker, so we should probably let `self.reap_worker()` do the cleanup.
        // Similarly, if Pool-side post-preparation processing fails, fail the RPC as well;
        // RPC's promise is to bring a Sandbox to a state to handle subsequent invocations.
        let result = match result {
            Ok((sandbox_id, has_snapshot, stats)) => {
                // If a `PrepareSandboxOptions::initial_keepalive_hint` had been provided by the
                // caller, use it as the initial keep-alive duration for parking this freshly
                // prepared Worker. This bypasses the normal keep-alive assignment policy only
                // for this first parking step; it still does not guarantee lifetime, since
                // eviction, shutdown, or failures may terminate the Worker earlier.
                if let Some(initial_keepalive) = self
                    .pending
                    .sandbox_preps
                    .get(&worker_id)
                    .and_then(|op| op.initial_keepalive_hint)
                {
                    if let Err(err) = self
                        .deactivate_worker(worker_id, &function_id, Some(now + initial_keepalive))
                        .await
                    {
                        error!(
                            error = ?err, %worker_id, %function_id, %sandbox_id, %has_snapshot, ?stats,
                            "Failed to deactivate _Active_ Worker: {err:#}"
                        );
                        ret = Err(err);
                    }
                } else if let Err(err) = self
                    .deactivate_worker_per_keepalive(worker_id, &function_id, now)
                    .await
                {
                    error!(
                        error = ?err, %worker_id, %function_id, %sandbox_id, %has_snapshot, ?stats,
                        "Failed to deactivate _Active_ Worker: {err:#}"
                    );
                    ret = Err(err);
                }

                match ret {
                    Ok(()) => Ok(PrepareSandboxOutput {
                        info: api::SandboxInfo {
                            sandbox_id,
                            function_id,
                            state: api::SandboxControlState::Idle,
                            // NOTE: Technically, the Worker might be already Dying by now,
                            // depending on (1) any provided keep-alive hint, (2) what the policy
                            // suggested. Is accurately reporting it here really important? FIXME?
                            has_snapshot,
                            stats: Some(stats),
                        },
                    }),
                    Err(ref err) => Err(PrepareSandboxError::Internal {
                        msg: format!("Pool-side post-PrepareSandbox processing failed: {err:#}")
                            .into_boxed_str(),
                        err: None, // pointless to fwd it to PoolRef anyway
                    }),
                }
            }
            Err(err_str) => Err(PrepareSandboxError::Worker(err_str)),
        };

        // Notify any subscribed task/channel waiting on this
        trace!(?result, "Forwarding PrepareSandbox response to subscriber");
        self.pending
            .sandbox_preps
            .remove(&worker_id)
            .and_then(|op| {
                op.respond_to.and_then(|tx| {
                    tx.send(result)
                        .inspect_err(|result| {
                            warn!(?result, "Failed to forward PrepareSandbox response")
                        })
                        .ok()
                })
            });

        // Stop tracking the control op---it's over.
        let _old_ctrl_op = self.pending.op_by_worker.remove(&worker_id);
        debug_assert_eq!(_old_ctrl_op, Some(PendingControlOp::PrepareSandbox));

        ret
    }

    #[instrument(level = Level::TRACE, skip_all)]
    pub(super) async fn finalize_snapshotting(
        &mut self,
        resp: Box<CreateSnapshotResult>,
    ) -> Result<()> {
        let now = Instant::now();

        trace!(?resp);
        let CreateSnapshotResult {
            worker_id,
            function_id,
            sandbox_id,
            result,
        } = *resp;

        // NOTE: Make sure that failures in Worker state manipulation don't leave subscribed
        // channels (and their associated actors and clients) hanging; always respond.

        let pending_op = self.pending.snapshots.remove(&sandbox_id);
        let timer_info = pending_op.as_ref().and_then(|op| op.timer_info);

        let ret = if let Some(PausedTimerInfo { ts_pause, deadline }) = timer_info {
            // If the Worker was previously _Idle_, change Worker state (Idle->Active) & reinstall
            // a keep-alive timer (with or without the extra delay---currently excluding it)
            let new_deadline = deadline + (now - ts_pause); // excludes extra delay
            self.deactivate_worker(worker_id, &function_id, Some(new_deadline))
                .await
        } else {
            // If the Worker was previously _Active_, change Worker state (Active->Idle),
            // but consult the KeepAlivePolicy for setting a new keep-alive timer.
            // There should be no cpuset allocated; it should have been
            // deallocated when Worker completed its last invocation.
            self.deactivate_worker_per_keepalive(worker_id, &function_id, now)
                .await
        };

        // Notify any and all subscribed tasks/channels waiting on us
        let result = result.map_err(CreateSnapshotError::SnapshotWorker);
        trace!(?result, "Forwarding snapshotting response to subscribers");
        if let Some(pending) = pending_op {
            pending.respond_to.into_iter().for_each(|tx| {
                let _ = tx.send(result.clone()).inspect_err(|result| {
                    warn!(?result, "Failed to forward snapshotting response")
                });
            });
        }

        // Stop tracking the control op---it's over.
        let _old_ctrl_op = self.pending.op_by_worker.remove(&worker_id);
        debug_assert_eq!(_old_ctrl_op, Some(PendingControlOp::Snapshot));

        ret
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Op Finalization during Shutdown
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Best-effort handling of a sandbox preparation completion received after
    /// Pool has already started shutdown.
    #[instrument(level = Level::INFO, skip(self))]
    pub(super) fn finalize_sandbox_preparation_during_shutdown(
        &mut self,
        resp: Box<PrepareSandboxResult>,
    ) {
        let PrepareSandboxResult {
            worker_id,
            function_id,
            result,
        } = *resp;

        let result = match result {
            // TODO: Maybe fail the op anyway, since the contract of "created
            // Sandbox ready to serve" is broken due to Pool shutdown?
            Ok((sandbox_id, has_snapshot, stats)) => Ok(PrepareSandboxOutput {
                info: api::SandboxInfo {
                    sandbox_id,
                    function_id,
                    state: api::SandboxControlState::Dying,
                    has_snapshot,
                    stats: Some(stats),
                },
            }),
            Err(err_str) => Err(PrepareSandboxError::Worker(err_str)),
        };
        let _ = self.cpuset.release(worker_id);

        // Notify any subscribed task/channel waiting on this
        trace!(?result, "Forwarding PrepareSandbox response to subscriber");
        self.pending
            .sandbox_preps
            .remove(&worker_id)
            .and_then(|op| {
                op.respond_to.and_then(|tx| {
                    tx.send(result)
                        .inspect_err(|result| {
                            warn!(?result, "Failed to forward PrepareSandbox response")
                        })
                        .ok()
                })
            });

        // Stop tracking the control op---it's over.
        let _old_ctrl_op = self.pending.op_by_worker.remove(&worker_id);
        debug_assert_eq!(_old_ctrl_op, Some(PendingControlOp::PrepareSandbox));
    }

    /// Best-effort handling of a snapshot completion received after Pool has
    /// already started shutdown.
    #[instrument(level = Level::INFO, skip(self))]
    pub(super) fn finalize_snapshotting_during_shutdown(
        &mut self,
        resp: Box<CreateSnapshotResult>,
    ) {
        let CreateSnapshotResult {
            worker_id,
            sandbox_id,
            result,
            ..
        } = *resp;

        match self.pending.op_by_worker.remove(&worker_id) {
            Some(PendingControlOp::Snapshot) => {}
            maybe_op => warn!("expected `Some(PendingControlOp::Snapshot)`; found: {maybe_op:?}"),
        }

        if let Some(pending) = self.pending.snapshots.remove(&sandbox_id) {
            let result = result.map_err(CreateSnapshotError::SnapshotWorker);
            trace!(?result, "Forwarding snapshotting response to subscribers");

            pending.respond_to.into_iter().for_each(|tx| {
                let _ = tx.send(result.clone()).inspect_err(|result| {
                    warn!(?result, "Failed to forward snapshotting response")
                });
            });
        }
    }
}
