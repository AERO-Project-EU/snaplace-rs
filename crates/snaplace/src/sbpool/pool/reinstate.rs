use std::{borrow::Cow, collections::HashMap};

use compact_str::{CompactString, ToCompactString};
use either::Either;
use redb::{
    AccessGuard, Database, ReadableDatabase, ReadableMultimapTable, ReadableTable,
    ReadableTableMetadata, StorageError, TableHandle, WriteTransaction,
};
use tracing::{debug, error, info, instrument, trace, warn, Level};
use triomphe::ArcBorrow;

use crate::{
    keepalive,
    metadata::{
        db::{Rmp, Tables},
        FunctionInfo,
    },
    network::{self, NetworkManagerRef},
    sbpool::{Error, SandboxPool},
    worker::{self, runtime::SnapshotState, Sandbox},
    FunctionId, FunctionMetadataStore, Request, Response,
};

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
    /// Reads all snapshots persisted in DB and attempts to reinstate them.
    ///
    /// # Returns
    ///
    /// All reinstated snapshots, as [`Sandbox`]es.
    #[instrument(level = Level::INFO, skip_all)]
    pub(super) async fn reinstate_snapshots_from_db<S: ::std::hash::BuildHasher + Default>(
        &self,
    ) -> Result<HashMap<FunctionId, Vec<Runtime::Sandbox>, S>, Error> {
        let mut snapshots = Default::default();

        let rtxn = self.db.begin_read().map_err(|err| Error::Database {
            msg: "failed to begin read txn".into(),
            source: err.into(),
        })?;

        let tbl_funcs = match rtxn.open_table(Tables::<Runtime>::FUNCTIONS) {
            Ok(tbl) => tbl,
            Err(::redb::TableError::TableDoesNotExist(name)) => {
                info!("No table '{name}' exists; no registered Functions to load from database");
                return Ok(snapshots);
            }
            Err(err) => {
                return Err(Error::Database {
                    msg: "failed to open table FUNCTIONS".into(),
                    source: err.into(),
                })
            }
        };
        let tbl_spf = match rtxn.open_multimap_table(Tables::<Runtime>::SNAPS_PER_FUNC) {
            Ok(tbl) => tbl,
            Err(::redb::TableError::TableDoesNotExist(name)) => {
                warn!("No table '{name}' exists; no Sandbox snapshots to load from database");
                return Ok(snapshots);
            }
            Err(err) => {
                return Err(Error::Database {
                    msg: "failed to open table SNAPS_PER_FUNC".into(),
                    source: err.into(),
                })
            }
        };
        let tbl_snaps = match rtxn.open_table(Tables::<Runtime>::SNAPSHOTS) {
            Ok(tbl) => tbl,
            Err(::redb::TableError::TableDoesNotExist(name)) => {
                warn!("No table '{name}' exists; no Sandbox snapshots to load from database");
                return Ok(snapshots);
            }
            Err(err) => {
                return Err(Error::Database {
                    msg: "failed to open table SNAPSHOTS".into(),
                    source: err.into(),
                })
            }
        };
        let num_snaps = tbl_snaps.len().map_err(|err| Error::Database {
            msg: "failed to retrieve length of table SNAPSHOTS".into(),
            source: err.into(),
        })?;
        info!(
            "Found {num_snaps} saved Sandbox snapshot{} stored in the database",
            if num_snaps == 1 { "" } else { "s" }
        );

        for res_sids in tbl_spf.iter().map_err(|err| Error::Database {
            msg: "failed to iterate through table SNAPS_PER_FUNC".into(),
            source: err.into(),
        })? {
            let (fid, sids) =
                res_sids
                    .map(|(k, v)| (k.value(), v))
                    .map_err(|err| Error::Database {
                        msg: "failed to read Sandbox IDs from database".into(),
                        source: err.into(),
                    })?;

            let func = match tbl_funcs.get(&fid) {
                Ok(Some(v)) => v.value(),
                Ok(None) => continue,
                Err(err) => {
                    return Err(Error::Database {
                        msg: "failed to read Function from database".into(),
                        source: err.into(),
                    })
                }
            };
            let mut rt = Runtime::new(&self.worker_aux.config.runtime, func.info(), None).map_err(
                |err| Error::Runtime {
                    msg: "failed to create new Runtime instance".into(),
                    source: err,
                },
            )?;
            if let Err(err) = rt.init().await {
                error!(error = ?err, ?func, "Failed to initialize Runtime: {err:#}");
                return Err(Error::Runtime {
                    msg: "failed to initialiaze Runtime".into(),
                    source: err,
                });
            }

            for res_sid in sids {
                match Self::reinstate_sandbox(
                    Either::Right(res_sid),
                    func.info(),
                    &tbl_snaps,
                    &mut rt,
                    self.netman.clone(),
                )
                .await
                {
                    Ok(sb) => snapshots.entry(fid.clone()).or_default().push(sb),
                    Err(AuxError::ReinstatementNotImplemented) => return Ok(snapshots), // short-circuit entire call
                    Err(err) => error!(error = ?err, "Failed to reinstate Sandbox: {err:#}"),
                }
            }
        }

        Ok(snapshots)
    }

    /// Delete [`Tables::SNAPSHOTS`].
    pub(super) async fn wipe_db_snapshots(&mut self) -> Result<(), Error> {
        let wtxn = self.db.begin_write().map_err(|err| Error::Database {
            msg: "failed to open write txn".into(),
            source: err.into(),
        })?;
        let _existed = wtxn
            .delete_multimap_table(Tables::<Runtime>::SNAPS_PER_FUNC)
            .map_err(|err| Error::Database {
                msg: format!(
                    "failed to delete table '{}'",
                    Tables::<Runtime>::SNAPS_PER_FUNC
                )
                .into_boxed_str(),
                source: err.into(),
            })?;
        let _existed = wtxn
            .delete_table(Tables::<Runtime>::SNAPSHOTS)
            .map_err(|err| Error::Database {
                msg: format!("failed to delete table '{}'", Tables::<Runtime>::SNAPSHOTS)
                    .into_boxed_str(),
                source: err.into(),
            })?;
        wtxn.commit().map_err(|err| Error::Database {
            msg: format!(
                "failed to commit txn to table '{}'",
                Tables::<Runtime>::SNAPSHOTS.name()
            )
            .into_boxed_str(),
            source: err.into(),
        })
    }

    /// Update [`Tables::SNAPSHOTS`] to include existing snapshots (and only them? TODO).
    //
    // Challenges
    // ==========
    // (1) Make sure all `self.snapshots` are persisted in the DB.
    //     (A) If any of them is NOT already there, then there must be a BUG either in
    //         Workers (unlikely) or in SnapshotManager's writing thread (write failure).
    //     (B) Are their `SandboxStats` up to date? (hint: no, they're not, because we
    //         most probably had to handle invocations since it was created)
    // (2) Any other snapshot stored in DB should be invalid, therefore delete it (?)
    //     (A) How can such "extra" snapshots exist?
    //         (a) They were created in a previous boot, and not cleaned up correctly.
    //         (b) They were created during this boot, failed when run, their Sandbox
    //             was destroyed, but nobody removed them from the DB.
    //     (B) Why do we care? Because they are probably associated with other resources
    //         (net, storage) that has probably NOT been deallocated.
    // Solutions
    // =========
    // (1) => Persist all `self.snapshots` to the SNAPSHOTS table, possibly overwriting
    //        any existing snapshot states.
    //    (A) => If any snapshot was not successfully persisted in DB by snapman's writing
    //           thread, this will be fixed now.
    //    (B) => `SandboxStats` will be updated to the latest values.
    // (2)
    //  * Wiping SNAPSHOTS table and then overwriting it with `self.snapshots` looks like
    //    the easiest/quickest approach. However, it does not attend to (2). As a result:
    //    - OS resources are wasted.
    //    - After reboot, snaplace will be oblivious of resources in use, and may attempt
    //      to reuse them (e.g., kept-alive TAP names, kept-alive DM-thin names, snapshot
    //      device capacity limits?, etc).
    ///
    /// TODO: Document properly
    ///
    /// # Note
    ///
    /// This is a hideous `async fn`, calling multiple `sync fn`s that access and modify storage.
    /// This is bad, as it blocks `tokio` runtime worker threads, which are supposed to progress
    /// futures rather than block on storage syscalls.
    ///
    /// However, since this method is only meant to run when `SandboxPool` shuts down, we do not
    /// really care much about performance at this point, for now, especially as long as this way
    /// guarantees logical correctness.
    #[instrument(level = Level::INFO, skip_all)]
    pub(super) async fn sync_db_snapshots(&mut self) -> Result<(), Error> {
        /// Load all `{ FunctionId: [Sandbox IDs] }` from database ([`Tables::SNAPS_PER_FUNC`])
        /// into memory.
        #[instrument(level = Level::DEBUG, skip_all)]
        fn read_db_spf<Rt, S>(
            db: ArcBorrow<Database>,
            snapshots: &HashMap<FunctionId, Vec<Rt::Sandbox>, S>,
        ) -> Result<HashMap<FunctionId, Vec<CompactString>, S>, Error>
        where
            Rt: worker::Runtime,
            S: ::std::hash::BuildHasher + Default,
        {
            let rtxn = db.begin_read().map_err(|err| Error::Database {
                msg: "failed to open read txn".into(),
                source: err.into(),
            })?;
            let tbl_spf = rtxn
                .open_multimap_table(Tables::<Rt>::SNAPS_PER_FUNC)
                .map_err(|err| Error::Database {
                    msg: "failed to open table SNAPS_PER_FUNC".into(),
                    source: err.into(),
                })?;
            let mut ret: HashMap<_, Vec<_>, S> = Default::default();
            for res_kv in tbl_spf.iter().map_err(|err| Error::Database {
                msg: "failed to loop through table SNAPS_PER_FUNC".into(),
                source: err.into(),
            })? {
                let (fid, sids_iter) =
                    res_kv
                        .map(|(k, v)| (k.value(), v))
                        .map_err(|err| Error::Database {
                            msg: "failed while looping through table SNAPS_PER_FUNC".into(),
                            source: err.into(),
                        })?;
                if ret
                    .insert(
                        fid.clone(),
                        Vec::with_capacity(snapshots.get(&fid).map(Vec::len).unwrap_or(0)),
                    )
                    .is_some()
                {
                    warn!(function.id = %fid, "BUG: Should not stumble upon the same FunctionId twice");
                }
                for res_sid in sids_iter {
                    let sid = res_sid
                        .map_err(|err| Error::Database {
                            msg: "failed to read Sandbox ID".into(),
                            source: err.into(),
                        })?
                        .value();
                    ret.get_mut(&fid).expect("entry just created").push(sid);
                }
            }
            Ok(ret)
        }

        /// Update DB's table `SNAPSHOTS` with the latest `SnapshotState`s of all provided
        /// `sandboxes` (all referring to the same [`FunctionId`], `fid`), and then remove them
        /// from both `sandboxes` and `db_sids` [`Vec`]s (unless the latter is not provided).
        ///
        /// # Errors
        ///
        /// For now, only on failure to open DB's table.
        #[instrument(level = Level::DEBUG, skip_all, fields(function.id = %fid))]
        fn update_and_forget_tracked_snapshots<Rt: worker::Runtime>(
            wtxn: &WriteTransaction,
            fid: &FunctionId,
            sandboxes: &mut Vec<Rt::Sandbox>,
            db_sids: Option<&mut Vec<CompactString>>,
        ) -> Result<(), Error> {
            let mut processed_sids = Vec::with_capacity(sandboxes.len());

            let mut tbl_snaps =
                wtxn.open_table(Tables::<Rt>::SNAPSHOTS)
                    .map_err(|err| Error::Database {
                        msg: "failed to open table SNAPSHOTS".into(),
                        source: err.into(),
                    })?;
            // Loop through all `SandboxPool.snapshots[fid]`...
            for sb in sandboxes.iter() {
                // ... update them in DB ...
                let state = match sb.snapshot_state() {
                    Some(Ok(state)) => state,
                    None => return Ok(()), // no SnapshotStates; short-circuit entire call
                    Some(Err(err)) => {
                        error!(
                            error = ?err, sandbox.id = %sb.id(), ?sb,
                            "Failed to create SnapshotState: {err:#}"
                        );
                        // NOTE: These are not added to `processed_sids`. We may be attempting
                        // to persist them again later, though not much should change anyway...
                        continue;
                    }
                };
                match tbl_snaps.insert(sb.id().to_compact_string(), &state) {
                    Ok(old_state) => {
                        trace!(new.state = ?state, old.state = ?old_state.map(|s| s.value()));
                        // ... and mark to stop tracking them, in both the DB (and in-memory `db_*`
                        // structures) _and_ the SandboxPool.
                        processed_sids.push(state.id().to_compact_string());
                    }
                    Err(err) => warn!(
                        error = ?err, function.id = %fid, sandbox.id = %sb.id(), ?state,
                        "Failed to upsert SnapshotState to table SNAPSHOTS"
                    ),
                }
            }
            // ... stop tracking them in SandboxPool
            sandboxes.retain(|sb| !processed_sids.contains(&sb.id().to_compact_string()));
            if let Some(sids) = db_sids {
                // ... and also in in-memory `db_*` structures
                sids.retain(|sid| !processed_sids.contains(sid));
            }

            Ok(())
        }

        /// Remove `SnapshotState`s associated with all given Sandbox IDs (`sids`) from table
        /// `SNAPSHOTS`.
        ///
        /// # Errors
        ///
        /// For now, only on failure to open DB's table.
        #[instrument(level = Level::DEBUG, skip_all, fields(function.id = %fid))]
        fn remove_db_snapshots<Rt: worker::Runtime>(
            wtxn: &WriteTransaction,
            fid: &FunctionId,
            sids: &[CompactString],
        ) -> Result<(), Error> {
            let mut tbl_snaps =
                wtxn.open_table(Tables::<Rt>::SNAPSHOTS)
                    .map_err(|err| Error::Database {
                        msg: "failed to open table SNAPSHOTS".into(),
                        source: err.into(),
                    })?;

            // Loop through the given SIDs, and delete their corresponding SnapshotStates from
            // table SNAPSHOTS.
            for sid in sids {
                match tbl_snaps.remove(sid) {
                    Ok(Some(_old_db_state)) => {}
                    Ok(None) => debug!(%sid, "Expected SnapshotState in SNAPSHOTS; not found"),
                    Err(err) => error!(
                        error = ?err, function.id = %fid, sandbox.id = %sid,
                        "Failed to remove SnapshotState from table SNAPSHOTS: {err:#}"
                    ),
                }
            }

            Ok(())
        }

        /// Delete the given Sandbox IDs (`sids`) associated with the provided [`FunctionId`]
        /// (i.e., all specified `{fid->[sids]}` pairs) from table `SNAPS_PER_FUNC`.
        ///
        /// # Errors
        ///
        /// For now, only on failure to open DB's table.
        #[instrument(level = Level::DEBUG, skip_all, fields(function.id = %fid))]
        fn remove_db_sids<Rt: worker::Runtime>(
            wtxn: &WriteTransaction,
            fid: &FunctionId,
            sids: &[CompactString],
        ) -> Result<(), Error> {
            let mut tbl_spf = wtxn
                .open_multimap_table(Tables::<Rt>::SNAPS_PER_FUNC)
                .map_err(|err| Error::Database {
                    msg: "failed to open table SNAPS_PER_FUNC".into(),
                    source: err.into(),
                })?;
            for sid in sids {
                match tbl_spf.remove(fid, sid) {
                    Ok(true) => {}
                    Ok(false) => debug!(
                        sandbox.id = %sid, "Expected Sandbox ID in table SNAPS_PER_FUNC; not found"
                    ),
                    Err(err) => error!(
                        error = ?err, function.id = %fid, sandbox.id = %sid,
                        "Failed to remove Sandbox ID from table SNAPS_PER_FUNC: {err:#}"
                    ),
                }
            }

            Ok(())
        }

        /// Insert the given Sandbox IDs (`sids`) associated with the provided [`FunctionId`]
        /// (i.e., all specified `{fid->[sids]}` pairs) to table `SNAPS_PER_FUNC`.
        ///
        /// # Errors
        ///
        /// For now, only on failure to open DB's table.
        #[instrument(level = Level::DEBUG, skip_all, fields(function.id = %fid))]
        fn update_db_sids<Rt: worker::Runtime>(
            wtxn: &WriteTransaction,
            fid: &FunctionId,
            sids: &[CompactString],
        ) -> Result<(), Error> {
            let mut tbl_spf = wtxn
                .open_multimap_table(Tables::<Rt>::SNAPS_PER_FUNC)
                .map_err(|err| Error::Database {
                    msg: "failed to open table SNAPS_PER_FUNC".into(),
                    source: err.into(),
                })?;
            for sid in sids {
                match tbl_spf.insert(fid, sid) {
                    Ok(true) => {}
                    Ok(false) => debug!(
                        sandbox.id = %sid, "Expected Sandbox ID in table SNAPS_PER_FUNC; not found"
                    ),
                    Err(err) => error!(
                        error = ?err, function.id = %fid, sandbox.id = %sid,
                        "Failed to insert Sandbox ID to table SNAPS_PER_FUNC: {err:#}"
                    ),
                }
            }

            Ok(())
        }

        ///////////////////////////////////////////////////////////////////////////////////////////

        // Load all table SNAPS_PER_FUNC into mem...
        let db_spf = read_db_spf::<Runtime, _>(self.db.borrow_arc(), &self.snapshots)?;
        // ...and loop through all of it, to:
        for (fid, mut sids) in db_spf.into_iter() {
            let wtxn = self.db.begin_write().map_err(|err| Error::Database {
                msg: "failed to open write txn".into(),
                source: err.into(),
            })?;
            if self.snapshots.contains_key(&fid) {
                update_and_forget_tracked_snapshots::<Runtime>(
                    &wtxn,
                    &fid,
                    self.snapshots.get_mut(&fid).expect("just checked"),
                    Some(&mut sids),
                )?;
                // After the above, any _remaining_ SID in in-memory db_* structures is not
                // tracked by the SandboxPool, and should be removed from table SNAPSHOTS.
                // But first, attempt to reinstate the Sandbox and destroy it.
                self.reinstate_and_destroy_db_snapshots(&wtxn, &fid, &sids)
                    .await?;
                remove_db_snapshots::<Runtime>(&wtxn, &fid, &sids)?;
            } else {
                // Remove from DB any SnapshotStates persisted there but not tracked by SandboxPool
                // NOTE(ckatsak): Functions that are currently not registered at all (as well as
                // Functions that created no snapshots at all since `snaplace` was last booted)
                // are included in this case. It is OK to remove them from the DB. They may be
                // never registered again anyway, and if they are, they will create their new
                // updated snapshots then, so they will be properly persisted at that later time.
                self.reinstate_and_destroy_db_snapshots(&wtxn, &fid, &sids)
                    .await?;
                remove_db_snapshots::<Runtime>(&wtxn, &fid, &sids)?;
                remove_db_sids::<Runtime>(&wtxn, &fid, &sids)?;
            }
            wtxn.commit().map_err(|err| Error::Database {
                msg: "failed to commit txn to snapshot tables".into(),
                source: err.into(),
            })?;

            let remaining_sids = self.snapshots.remove(&fid);
            info!(
                "#orphans" = self.snapshots.values().map(Vec::len).sum::<usize>(),
                "#functions.with.orphans" = self.snapshots.keys().len(),
                "Remaining orphaned/snapshotted sandboxes"
            );
            if !matches!(remaining_sids.as_ref().map(Vec::len), Some(0)) {
                warn!(function.id = %fid, orphan.sandbox.ids = ?remaining_sids);
            }
        }

        // Store FIDs remaining in `SandboxPool.snapshots` to both snapshot tables
        for (fid, sbs) in &mut self.snapshots {
            let wtxn = self.db.begin_write().map_err(|err| Error::Database {
                msg: "failed to open write txn".into(),
                source: err.into(),
            })?;
            {
                // Update table SNAPS_PER_FUNC
                update_db_sids::<Runtime>(
                    &wtxn,
                    fid,
                    &sbs.iter()
                        .map(|sb| sb.id().to_compact_string())
                        .collect::<Vec<_>>(),
                )?;
                // Update table SNAPSHOTS
                update_and_forget_tracked_snapshots::<Runtime>(&wtxn, fid, sbs, None)?;
            }
            wtxn.commit().map_err(|err| Error::Database {
                msg: "failed to commit txn to snapshot tables".into(),
                source: err.into(),
            })?;
        }

        info!(
            "#orphans" = self.snapshots.values().map(Vec::len).sum::<usize>(),
            "#functions.with.orphans" = self.snapshots.keys().len(),
            "Remaining orphaned/snapshotted sandboxes"
        );
        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(function.id = %fid))]
    async fn reinstate_and_destroy_db_snapshots(
        &mut self,
        txn: &WriteTransaction,
        fid: &FunctionId,
        sids: &[CompactString],
    ) -> Result<(), Error> {
        if sids.is_empty() {
            return Ok(());
        }

        let tbl_funcs = txn
            .open_table(Tables::<Runtime>::FUNCTIONS)
            .map_err(|err| Error::Database {
                msg: "failed to open table FUNCTIONS".into(),
                source: err.into(),
            })?;
        let tbl_snaps = txn
            .open_table(Tables::<Runtime>::SNAPSHOTS)
            .map_err(|err| Error::Database {
                msg: "failed to open table SNAPSHOTS".into(),
                source: err.into(),
            })?;

        let Some(func) = tbl_funcs
            .get(fid)
            .map_err(|err| Error::Database {
                msg: format!("failed to read FunctionInfo for '{fid}'").into_boxed_str(),
                source: err.into(),
            })?
            .map(|v| v.value())
        else {
            warn!(function.id = %fid, "FunctionInfo not found in table FUNCTIONS");
            return Ok(());
        };

        let mut rt =
            Runtime::new(&self.worker_aux.config.runtime, func.info(), None).map_err(|err| {
                error!(error = ?err, ?func, "Failed to instantiate Runtime: {err:#}");
                Error::Runtime {
                    msg: "failed to create new Runtime instance".into(),
                    source: err,
                }
            })?;
        if let Err(err) = rt.init().await {
            error!(error = ?err, ?func, "Failed to initialize Runtime: {err:#}");
            return Err(Error::Runtime {
                msg: "failed to initialiaze Runtime".into(),
                source: err,
            });
        }

        for sid in sids {
            match Self::reinstate_sandbox(
                Either::Left(sid),
                func.info(),
                &tbl_snaps,
                &mut rt,
                self.netman.clone(),
            )
            .await
            {
                Ok(sb) => {
                    if let Err(err) = self.destroy_orphan_sandbox(fid, sb).await {
                        error!(error = ?err, %fid, "Failed to destroy reinstated sandbox: {err:#}");
                    }
                }
                Err(AuxError::ReinstatementNotImplemented) => return Ok(()), // short-circuit entire call
                Err(err) => error!(error = ?err, %fid, "Failed to reinstate Sandbox: {err:#}"),
            }
        }

        Ok(())
    }

    #[instrument(
        level = Level::DEBUG,
        skip_all,
        fields(function.id = %fi.id(), sandbox.id = ::tracing::field::Empty)
    )]
    async fn reinstate_sandbox(
        sid: Either<&CompactString, Result<AccessGuard<'_, Rmp<CompactString>>, StorageError>>,
        fi: &Runtime::FunctionInfo,
        tbl_snaps: &impl ReadableTable<
            Rmp<CompactString>,
            Rmp<<Runtime::Sandbox as Sandbox>::SnapshotState>,
        >,
        rt: &mut Runtime,
        netman: NetworkManagerRef<NetResource>,
    ) -> Result<Runtime::Sandbox, AuxError> {
        let sid = sid.either(
            |s| Ok(Cow::Borrowed(s)),
            |g| {
                g.map_err(|err| Error::Database {
                    msg: "failed to read Sandbox ID from database".into(),
                    source: err.into(),
                })
                .map(|g| Cow::Owned(g.value()))
            },
        )?;
        ::tracing::Span::current().record("sandbox.id", sid.as_ref().as_str());

        let Some(state) = tbl_snaps
            .get(sid.as_ref())
            .map_err(|err| Error::Database {
                msg: format!("failed to read SnapshotState for '{sid}'").into_boxed_str(),
                source: err.into(),
            })?
            .map(|v| v.value())
        else {
            warn!(%sid, "SnapshotState not found in table SNAPSHOTS");
            return Err(AuxError::StateNotFound(sid.to_compact_string()));
        };

        match rt.reinstate_sandbox(fi.id().clone(), state, netman).await {
            Some(Ok(sb)) => Ok(sb),
            Some(Err(err)) => Err(AuxError::Runtime(Error::Runtime {
                msg: "failed to reinstate Sandbox".into(),
                source: err,
            })),
            None => Err(AuxError::ReinstatementNotImplemented),
        }
    }

    #[allow(
        dead_code,
        unused_mut,
        unreachable_code,
        unused_variables,
        unused_assignments
    )] // FIXME
    #[instrument(level = Level::DEBUG, skip_all)]
    async fn reinstate_and<F: AsyncFnMut(Runtime::Sandbox) -> ()>(
        sids: Either<&[CompactString], ::redb::MultimapValue<'_, Rmp<CompactString>>>,
        fid: &FunctionId,
        fi: &Runtime::FunctionInfo,
        netman: NetworkManagerRef<NetResource>,
        rt_config: &Runtime::Config,
        tbl_snaps: &impl ReadableTable<
            Rmp<CompactString>,
            Rmp<<Runtime::Sandbox as Sandbox>::SnapshotState>,
        >,
        mut f: F,
    ) -> Result<(), Error> {
        // - https://users.rust-lang.org/t/implementation-of-send-is-not-general-enough-but-cannot-make-it-more-general/115087
        // - https://github.com/rust-lang/rust/issues/64552
        // - https://github.com/rust-lang/rust/issues/110338
        unimplemented!("BUG(ckatsak): rustc issue: Send not general enough for ArcStr");

        let sids = sids.either(
            |sids| Ok(Cow::Borrowed(sids)),
            |mv| {
                mv.map(|res_ag| {
                    res_ag.map(|ag| ag.value()).map_err(|err| Error::Database {
                        msg: "failed to read Sandbox ID from database".into(),
                        source: err.into(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Cow::Owned)
            },
        )?;
        for sid in sids.iter() {
            let Some(state) = tbl_snaps
                .get(sid)
                .map_err(|err| Error::Database {
                    msg: format!("failed to read SnapshotState for '{sid}' ('{fid}')")
                        .into_boxed_str(),
                    source: err.into(),
                })?
                .map(|v| v.value())
            else {
                warn!(%sid, "SnapshotState not found in table SNAPSHOTS");
                continue;
            };

            let mut rt = match Runtime::new(rt_config, fi, None) {
                Ok(rt) => rt,
                Err(err) => {
                    error!(error = ?err, %sid, ?fi, "Failed to instantiate Runtime: {err:#}");
                    continue;
                }
            };
            if let Err(err) = rt.init().await {
                error!(error = ?err, %sid, ?fi, "Failed to initialize Runtime: {err:#}");
                continue;
            }
            match rt
                .reinstate_sandbox(fid.clone(), state, netman.clone())
                .await
            {
                Some(Ok(sb)) => {
                    let _unit = f(sb).await;
                    // In `reinstate_and_destroy_db_snapshots()`:
                    //  let fid2 = fid.clone();
                    //  let _x = Self::reinstate_and(
                    //      Either::Left(sids),
                    //      &fid.clone(),
                    //      &fi,
                    //      self.netman.clone(),
                    //      &self.worker_aux.config.runtime.clone(),
                    //      &tbl_snaps,
                    //      async move |sb| {
                    //          let fid2 = fid2.clone();
                    //          if let Err(err) = self.destroy_orphan_sandbox(&fid2, sb).await {
                    //              error!(error = ?err, "Failed to destroy reinstated sandbox: {err:#}");
                    //          }
                    //      },
                    //  )
                    //  .await;
                }
                None => return Ok(()), // not implemented by Runtime; short-circuit entire call
                Some(Err(err)) => error!(error = ?err, "Failed to reinstate Sandbox: {err:#}"),
            }
        }

        Ok(())
    }
}

#[derive(Debug, ::thiserror::Error)]
enum AuxError {
    #[error("reinstatement not implemented")]
    ReinstatementNotImplemented,
    #[error("SnapshotState not found in table SNAPSHOTS")]
    StateNotFound(CompactString),
    #[error(transparent)]
    Runtime(Error),
    #[error(transparent)]
    Database(Error),
}

impl From<Error> for AuxError {
    fn from(err: Error) -> Self {
        match err {
            Error::Runtime { .. } => AuxError::Runtime(err),
            Error::Database { .. } => AuxError::Database(err),
            _ => todo!(),
        }
    }
}
