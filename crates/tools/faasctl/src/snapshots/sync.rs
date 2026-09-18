use std::{collections::HashSet, io, path::Path};

use anyhow::{Context, Result};
use argh::FromArgs;
use redb::Database;
use tracing::{error, info, warn};

use snaplace::{
    metadata::db::Tables,
    worker::runtime::{fc, fcctrd},
};

use crate::{
    snapshots::{ls::Snapshot, SnapshotFilesExt, SnapshotsCmd, SnapshotsSubCmd},
    Cli, Runtime, SubCmd,
};

/// Checks which of the snapshots stored in the database are actually valid (i.e., their
/// corresponding snapshot files exist in the filesystem paths stored in the database), printing
/// out the invalid ones, and optionally removes the invalid ones (those which files are no
/// longer in the filesystem, essentially syncing the database with the filesystem state).
///
/// Note: output in log, at INFO level.
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "sync", help_triggers("-h", "--help", "help"))]
pub(super) struct SyncCmd {
    /// ndjson(aka jsonl)-formatted (newline-delimited JSON) to stdout
    #[argh(switch, short = 'J')]
    json: bool,

    /// also fix the database entries to reflect the state of the filesystem, deleting nonexistent
    /// snapshots tracked
    #[argh(switch)]
    fix: bool,
}

impl SyncCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Snapshots(SnapshotsCmd {
            ref db_path,
            cmd: SnapshotsSubCmd::Sync(SyncCmd { json, fix }),
        }) = cli.cmd
        else {
            unreachable!("should not have been routed here");
        };

        match cli.runtime {
            Runtime::FirecrackerContainerd => {
                sync::<fcctrd::FirecrackerContainerd>(db_path, json, fix).await
            }
            Runtime::Firecracker => sync::<fc::Firecracker>(db_path, json, fix).await,
        }
    }
}

async fn sync<Rt>(db_path: &Path, json: bool, also_fix: bool) -> Result<()>
where
    Rt: ::snaplace::worker::Runtime,
    <Rt::Sandbox as ::snaplace::worker::Sandbox>::SnapshotState: SnapshotFilesExt,
{
    let incomplete_snaps = incomplete_snapshots::<Rt>(db_path).await?;
    ::tracing::info!("#incomplete.snapshots" = incomplete_snaps.len());
    crate::snapshots::ls::print_snapshots(&incomplete_snaps, json).await;

    if also_fix {
        remove_db_snapshots(db_path, &incomplete_snaps).await?;
    }
    Ok(())
}

/// Returns all snapshots stored in the database whose files appear to not exist in the
/// filesystem, or who appear to not have a snapshot file stored in the database at all
/// (NOTE: this sort of database inconsistency would indicate a bug in FaaSCell).
async fn incomplete_snapshots<Rt>(db_path: &Path) -> Result<Vec<Snapshot<Rt>>>
where
    Rt: ::snaplace::worker::Runtime,
    <Rt::Sandbox as ::snaplace::worker::Sandbox>::SnapshotState: SnapshotFilesExt,
{
    let mut snaps = {
        let db = Database::builder()
            .open_read_only(db_path)
            .with_context(|| format!("failed to open redb file at '{}'", db_path.display()))?;

        crate::snapshots::ls::ls_snapshots(db)
            .await
            .context("failed to list snapshots")?
    };

    let mut sids_incomplete = HashSet::new();
    for snapshot in &snaps {
        if let Err(sid) = check_snapshot(snapshot) {
            sids_incomplete.insert(sid.clone());
        }
    }
    snaps.retain(|s| sids_incomplete.contains(&s.sid));

    Ok(snaps)
}

/// Returns `Err(&SandboxId)` if the corresponding Sandbox is "incomplete"; `Ok(())` otherwise.
//
// TODO: For now, stat(2) errors are just logged, and not returned as failures
// capable of classifying the snapshot unusable, hence for deletion. Perhaps we
// should treat them as such (expect EPERM, I guess)?
fn check_snapshot<Rt>(snapshot: &Snapshot<Rt>) -> ::std::result::Result<(), &::snaplace::SandboxId>
where
    Rt: ::snaplace::worker::Runtime,
    <Rt::Sandbox as ::snaplace::worker::Sandbox>::SnapshotState: SnapshotFilesExt,
{
    let Some(sf) = snapshot.state.state_file() else {
        warn!(?snapshot, "No state file stored in database!");
        return Err(&snapshot.sid);
    };
    match sf.metadata() {
        Ok(_md) => {}
        Err(err) if matches!(err.kind(), io::ErrorKind::NotFound) => {
            info!(?snapshot, "State file not found in filesystem");
            return Err(&snapshot.sid);
        }
        Err(err) => error!(error = ?err, ?snapshot, "Failed to stat(2) state file: {err:#}"),
    }

    let Some(mf) = snapshot.state.memory_file() else {
        warn!(?snapshot, "No memory file stored in database!");
        return Err(&snapshot.sid);
    };
    match mf.metadata() {
        Ok(_md) => {}
        Err(err) if matches!(err.kind(), io::ErrorKind::NotFound) => {
            info!(?snapshot, "Memory file not found in filesystem");
            return Err(&snapshot.sid);
        }
        Err(err) => error!(error = ?err, ?snapshot, "Failed to stat(2) memory file: {err:#}"),
    }

    Ok(())
}

// NOTE: This is a single (transactional/"atomic") batch deletion; it either succeeds
// as a whole, or fails as a whole (in which case nothing is deleted from the DB).
//
// TODO: Maybe move this somewhere else, more centrally, to perhaps enable reuse?
async fn remove_db_snapshots<Rt>(db_path: &Path, snapshots: &[Snapshot<Rt>]) -> Result<()>
where
    Rt: ::snaplace::worker::Runtime,
{
    let db = Database::open(db_path)
        .with_context(|| format!("failed to open redb file at '{}'", db_path.display()))?;
    let wtxn = db.begin_write().context("failed to begin write txn")?;
    {
        let mut tbl_spf = wtxn
            .open_multimap_table(Tables::<Rt>::SNAPS_PER_FUNC)
            .context("failed to open table SNAPS_PER_FUNC")?;
        let mut tbl_snaps = wtxn
            .open_table(Tables::<Rt>::SNAPSHOTS)
            .context("failed to open table SNAPSHOTS")?;

        for snapshot in snapshots {
            if let Err(err) = tbl_spf.remove(&snapshot.fid, &snapshot.sid) {
                error!(
                    error = ?err, function.id = %snapshot.fid, sandbox.id = %snapshot.sid,
                    "Failed to remove entry from table SNAPS_PER_FUNC: {err:#}"
                );
                return Err(err).with_context(|| {
                    format!(
                        "failed to remove entry keyed by ('{}', '{}') from table SNAPS_PER_FUNC",
                        snapshot.fid, snapshot.sid
                    )
                });
            }
            if let Err(err) = tbl_snaps.remove(&snapshot.sid) {
                error!(
                    error = ?err, function.id = %snapshot.fid, sandbox.id = %snapshot.sid,
                    "Failed to remove entry from table SNAPSHOTS: {err:#}"
                );
                return Err(err).with_context(|| {
                    format!(
                        "failed to remove entry keyed by '{}' from table SNAPSHOTS",
                        snapshot.sid
                    )
                });
            }
        }
    }
    wtxn.commit()
        .inspect_err(|err| error!(error = ?err, "Failed to commit write txn: {err:#}"))
        .context("failed to commit write txn")
}
