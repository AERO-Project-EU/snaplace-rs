use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use argh::FromArgs;
use redb::{Database, ReadableMultimapTable, ReadableTable};
use tracing::{debug, error, info, warn};

use snaplace::{
    metadata::db::Tables,
    worker::runtime::{fc, fcctrd},
    FunctionId,
};

use crate::{
    snapshots::{read_func_file, SnapshotsCmd, SnapshotsSubCmd},
    Cli, Runtime, SubCmd,
};

/// Delete one or more snapshot entries from the database.
///
/// Note: this does not delete or otherwise modify the snapshot themselves (e.g., associated file
/// or other Runtime-specific state).
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "forget", help_triggers("-h", "--help", "help"))]
pub(super) struct ForgetCmd {
    /// only log (INFO level) the entries that would be deleted, without actually deleting them
    #[argh(switch, short = 'n')]
    dry_run: bool,

    #[allow(clippy::doc_lazy_continuation)]
    /// optionally, provide one or more Function IDs whose tracked snapshots will be removed from
    /// the database.
    ///
    /// - If this option is specified, it is an error to also provide the `--from-func-file` option.
    /// - This option is supported for the following runtimes: `fcctrd`, `fc`
    #[argh(option)]
    func: Vec<FunctionId>,

    #[allow(clippy::doc_lazy_continuation)]
    /// optionally, provide a path to a line-delimited file of Function IDs whose tracked snapshots
    /// will be removed from the database.
    ///
    /// - If this option is specified, it is an error to also provide the `--func` option.
    /// - This option is supported for the following runtimes: `fcctrd`, `fc`
    #[argh(option)]
    from_func_file: Option<PathBuf>,
}

impl ForgetCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Snapshots(SnapshotsCmd {
            db_path,
            cmd:
                SnapshotsSubCmd::Forget(ForgetCmd {
                    dry_run,
                    func,
                    from_func_file,
                }),
        }) = &cli.cmd
        else {
            unreachable!("should not have been routed here");
        };

        let fids = if !func.is_empty() && from_func_file.is_some() {
            bail!("Only one of `--func` and `--from-func-file` should be provided; see --help")
        } else if func.is_empty() && from_func_file.is_none() {
            bail!("No Function IDs specified");
        } else if let Some(func_file_path) = from_func_file {
            Cow::Owned(read_func_file(func_file_path).await?)
        } else {
            Cow::Borrowed(func)
        };

        match cli.runtime {
            Runtime::FirecrackerContainerd => {
                forget::<fcctrd::FirecrackerContainerd>(db_path, &fids, *dry_run).await
            }
            Runtime::Firecracker => forget::<fc::Firecracker>(db_path, &fids, *dry_run).await,
        }
    }
}

pub(super) async fn forget<Rt: ::snaplace::worker::Runtime>(
    db_path: &Path,
    function_ids: &[FunctionId],
    dry_run: bool,
) -> Result<()> {
    let db = Database::builder()
        .open(db_path)
        .with_context(|| format!("failed to open redb file at '{}'", db_path.display()))?;
    let wtxn = db.begin_write().context("failed to open write txn")?;
    {
        let mut tbl_spf = wtxn
            .open_multimap_table(Tables::<Rt>::SNAPS_PER_FUNC)
            .context("failed to open table SNAPS_PER_FUNC")?;
        let mut tbl_snaps = wtxn
            .open_table(Tables::<Rt>::SNAPSHOTS)
            .context("failed to open table SNAPSHOTS")?;

        for fid in function_ids {
            let sids = match tbl_spf.get(fid) {
                Ok(sids) => sids,
                Err(err) => {
                    warn!(error = ?err, function.id = %fid, "Failed to read Function entry: {err:#}");
                    continue;
                }
            };
            for res_sid in sids {
                let sid = match res_sid {
                    Ok(sid_guard) => sid_guard.value(),
                    Err(err) => {
                        warn!(error = ?err, function.id = %fid, "Failed to read next Sandbox ID: {err:#}");
                        continue;
                    }
                };

                if dry_run {
                    match tbl_snaps.get(&sid) {
                        Ok(Some(state_guard)) => info!(snapshot = ?state_guard.value()),
                        Ok(None) => error!(
                            function.id = %fid, sandbox.id = %sid,
                            "Database inconsistency: failed to find snapshot state in table SNAPSHOTS"
                        ),
                        Err(err) => error!(
                            error = ?err, function.id = %fid, sandbox.id = %sid,
                            "Failed to read snapshot state: {err:#}"
                        ),
                    }
                    continue; // printing next snapshot
                }
                assert!(!dry_run);

                match tbl_snaps.remove(&sid) {
                    Ok(Some(state_guard)) => debug!(
                        function.id = %fid, snapshot = ?state_guard.value(), "Removing"
                    ),
                    Ok(None) => error!(
                        function.id = %fid, sandbox.id = %sid,
                        "Database inconsistency: failed to find snapshot state in table SNAPSHOTS"
                    ),
                    Err(err) => error!(
                        error = ?err, function.id = %fid, sandbox.id = %sid,
                        "Failed to remove snapshot state from table SNAPSHOTS: {err:#}"
                    ),
                }
            }

            if !dry_run {
                match tbl_spf.remove_all(fid) {
                    Ok(sids) => match sids
                        .map(|res_guard| res_guard.map(|guard| guard.value()))
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(sids) => info!(
                            function.id = %fid, sandbox.ids = ?sids,
                            "Removing {} Sandbox IDs",
                            sids.len()
                        ),
                        Err(err) => warn!(
                            error = ?err, "Failed to read Sandbox IDs to be removed: {err}"
                        ),
                    },
                    Err(err) => error!(
                        error = ?err,
                        "Failed to remove Sandbox IDs from table SNAPS_PER_FUNC: {err:#}"
                    ),
                }
            }
        }
    }
    wtxn.commit().map_err(|err| {
        error!(error = ?err, "Failed to commit changes to snapshots tables: {err:#}");
        ::anyhow::Error::from(err).context("failed to commit changes to snapshots tables")
    })
}
