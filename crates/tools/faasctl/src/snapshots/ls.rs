use std::{borrow::Cow, collections::HashSet, fmt::Debug, path::PathBuf};

use anyhow::{Context, Result};
use argh::FromArgs;
use redb::{
    Database, ReadOnlyDatabase, ReadableDatabase, ReadableMultimapTable, ReadableTableMetadata,
};
use tokio::io::AsyncWriteExt;
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

/// List all Sandbox snapshots tracked in the database.
///
/// Note: output in log, at INFO level.
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "ls", help_triggers("-h", "--help", "help"))]
pub(super) struct ListCmd {
    #[allow(clippy::doc_lazy_continuation)]
    /// optionally, provide one or more Function IDs as a filter; i.e., to only list snapshots
    /// of these Functions stored in the database.
    ///
    /// - If this option is specified along with `--from-func-file`, their union takes effect.
    /// - This option is supported for the following runtimes: `fcctrd`, `fc`
    #[argh(option)]
    func: Vec<FunctionId>,

    #[allow(clippy::doc_lazy_continuation)]
    /// optionally, provide a path to a line-delimited file of Function IDs as a filter; i.e.,
    /// to only list snapshots of these Functions stored in the database.
    ///
    /// - If this option is specified along with `--func`, their union takes effect.
    /// - This option is supported for the following runtimes: `fcctrd`, `fc`
    #[argh(option)]
    from_func_file: Option<PathBuf>,

    /// ndjson(aka jsonl)-formatted (newline-delimited JSON) to stdout
    #[argh(switch, short = 'J')]
    json: bool,
}

impl ListCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Snapshots(SnapshotsCmd {
            db_path,
            cmd:
                SnapshotsSubCmd::List(ListCmd {
                    func,
                    from_func_file,
                    json,
                }),
        }) = &cli.cmd
        else {
            unreachable!("should not have been routed here");
        };

        // If any of `--func` or `--from-func-file` is provided, only list their union
        let mut retained =
            HashSet::<_, ::snaplace::BuildHasher>::from_iter(func.iter().map(Cow::Borrowed));
        if let Some(func_file_path) = from_func_file {
            retained.extend(
                read_func_file(func_file_path)
                    .await?
                    .into_iter()
                    .map(Cow::Owned),
            );
        }

        let db = Database::builder()
            .open_read_only(db_path)
            .with_context(|| format!("failed to open redb file at '{}'", db_path.display()))?;

        match cli.runtime {
            Runtime::FirecrackerContainerd => {
                list::<fcctrd::FirecrackerContainerd>(db, *json, &retained).await
            }
            Runtime::Firecracker => list::<fc::Firecracker>(db, *json, &retained).await,
        }
    }
}

#[derive(::serde::Serialize)]
pub struct Snapshot<Rt: ::snaplace::worker::Runtime> {
    pub fid: ::snaplace::FunctionId,
    pub sid: ::compact_str::CompactString,
    pub state: <Rt::Sandbox as ::snaplace::worker::Sandbox>::SnapshotState,
}

impl<Rt: ::snaplace::worker::Runtime> Debug for Snapshot<Rt> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("function_id", &self.fid)
            .field("sandbox_id", &self.sid)
            .field("state", &self.state)
            .finish()
    }
}

pub(super) async fn list<Rt: ::snaplace::worker::Runtime>(
    db: ReadOnlyDatabase,
    json: bool,
    retained: &HashSet<Cow<'_, FunctionId>, ::snaplace::BuildHasher>,
) -> Result<()> {
    let mut snapshots = ls_snapshots::<Rt>(db)
        .await
        .context("failed to list snapshots")?;
    if !retained.is_empty() {
        snapshots.retain(|s| retained.contains(&s.fid));
    }
    print_snapshots(&snapshots, json).await;
    Ok(())
}

pub async fn ls_snapshots<Rt>(db: ReadOnlyDatabase) -> Result<Vec<Snapshot<Rt>>>
where
    Rt: ::snaplace::worker::Runtime,
{
    let rtxn = db.begin_read().context("failed to begin read txn")?;

    let tbl_funcs = rtxn
        .open_table(Tables::<Rt>::FUNCTIONS)
        .context("failed to open table FUNCTIONS")?;
    let tbl_spf = rtxn
        .open_multimap_table(Tables::<Rt>::SNAPS_PER_FUNC)
        .context("failed to open table SNAPS_PER_FUNC")?;
    let tbl_snaps = rtxn
        .open_table(Tables::<Rt>::SNAPSHOTS)
        .context("failed to open table SNAPSHOTS")?;

    let total_num_snaps = tbl_snaps
        .len()
        .context("failed to query length of table SNAPSHOTS")?;
    debug!(
        "#registered.functions" = tbl_funcs
            .len()
            .context("failed to query length of table FUNCTIONS")?,
        //"#funcs.with.snaps" = tbl_spf
        //    .len()
        //    .context("failed to query length of table SNAPS_PER_FUNC")?,
        "#snapshots" = total_num_snaps,
    );

    let mut ret = Vec::with_capacity(total_num_snaps as _);
    for entry in tbl_spf
        .iter()
        .context("failed to iterate through table SNAPS_PER_FUNC")?
    {
        let (fid, sids) = match entry.map(|(k, v)| (k.value(), v)) {
            Ok(x) => x,
            Err(err) => {
                error!(error = ?err, "Failed to read Function entry: {err:#}");
                continue;
            }
        };
        debug!(function.id = %fid, "#snapshots" = sids.len()); // NOTE(ckatsak): Check .len() docs

        for sid in sids {
            let sid = match sid {
                Ok(sid) => sid.value(),
                Err(err) => {
                    error!(error = ?err, function.id = %fid, "Failed to read next Sandbox ID: {err:#}");
                    continue;
                }
            };

            match tbl_snaps.get(&sid) {
                Ok(Some(state)) => ret.push(Snapshot {
                    fid: fid.clone(),
                    sid,
                    state: state.value(),
                }),
                Ok(None) => error!(
                    function.id = %fid, sandbox.id = %sid,
                    "Database inconsistency: failed to find snapshot state"
                ),
                Err(err) => error!(
                    error = ?err, function.id = %fid, sandbox.id = %sid,
                    "Failed to read snapshot state: {err:#}"
                ),
            }
        }
    }

    Ok(ret)
}

pub async fn print_snapshots<Rt: ::snaplace::worker::Runtime>(
    snapshots: &[Snapshot<Rt>],
    json: bool,
) {
    let mut stdout = ::tokio::io::stdout();

    if !json {
        info!("#snapshots" = snapshots.len());
    }

    for snap in snapshots {
        if json {
            // Output newline-delimted JSON
            match ::serde_json::to_string(snap) {
                Ok(snap_str) => {
                    if let Err(err) = stdout.write_all(snap_str.as_bytes()).await {
                        warn!(error = ?err, "failed to write snapshot to stdout");
                    }
                    if let Err(err) = stdout.write_u8(b'\n').await {
                        warn!(error = ?err, "failed to write newline (after snapshot) to stdout");
                    }
                }
                Err(err) => error!(
                    error = ?err, snapshot = ?snap,
                    "failed to JSON-serialize snapshot: {err:#}"
                ),
            }
        } else {
            // Only log at INFO level
            info!(function.id = %snap.fid, sandbox.id = %snap.sid, state = ?snap.state);
        }
    }
}
//}
