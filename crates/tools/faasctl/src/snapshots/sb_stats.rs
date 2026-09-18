use std::path::Path;

use anyhow::{Context, Result};
use argh::FromArgs;
use redb::{Database, ReadableTable};
use tracing::error;

use snaplace::{
    metadata::db::Tables,
    worker::runtime::{fc::Firecracker, fcctrd::FirecrackerContainerd},
};

use crate::{
    snapshots::{
        ls::{ls_snapshots, print_snapshots},
        SnapshotStateStatsExt, SnapshotsCmd, SnapshotsSubCmd,
    },
    Cli, Runtime, SubCmd,
};

/// Stats of snapshots tracked in the database.
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "stats", help_triggers("-h", "--help", "help"))]
pub(super) struct SbStatsCmd {
    #[argh(subcommand)]
    subcmd: SbStatsSubCmd,
}

impl SbStatsCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Snapshots(SnapshotsCmd {
            db_path: _,
            cmd: SnapshotsSubCmd::SbStats(SbStatsCmd { subcmd }),
        }) = &cli.cmd
        else {
            unreachable!("should not have been routed here");
        };

        match subcmd {
            SbStatsSubCmd::List(ls_cmd) => ls_cmd.run(cli).await,
            SbStatsSubCmd::Zero(zero_cmd) => zero_cmd.run(cli).await,
        }
    }
}

#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand)]
enum SbStatsSubCmd {
    List(ListCmd),
    Zero(ZeroCmd),
}

/// Zero out all Sandbox stats of snapshots tracked in the database.
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "zero-out", help_triggers("-h", "--help", "help"))]
struct ZeroCmd {}

impl ZeroCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Snapshots(SnapshotsCmd { db_path, .. }) = &cli.cmd else {
            unreachable!("should not have been routed here");
        };

        match cli.runtime {
            Runtime::FirecrackerContainerd => zero_out::<FirecrackerContainerd>(db_path).await,
            Runtime::Firecracker => zero_out::<Firecracker>(db_path).await,
        }
    }
}

/// List all Sandbox stats of snapshots tracked in the database, in descending order (i.e., most to
/// least used).
///
/// Note: output in log, at INFO level.
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "ls", help_triggers("-h", "--help", "help"))]
struct ListCmd {
    /// ndjson(aka jsonl)-formatted (newline-delimited JSON) to stdout
    #[argh(switch, short = 'J')]
    json: bool,
}

impl ListCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Snapshots(SnapshotsCmd {
            ref db_path,
            cmd:
                SnapshotsSubCmd::SbStats(SbStatsCmd {
                    subcmd: SbStatsSubCmd::List(ListCmd { json }),
                }),
        }) = cli.cmd
        else {
            unreachable!("should not have been routed here");
        };

        match cli.runtime {
            Runtime::FirecrackerContainerd => list::<FirecrackerContainerd>(db_path, json).await,
            Runtime::Firecracker => list::<Firecracker>(db_path, json).await,
        }
    }
}

pub(super) async fn list<Rt>(db_path: &Path, json: bool) -> Result<()>
where
    Rt: ::snaplace::worker::Runtime,
    <Rt::Sandbox as ::snaplace::worker::Sandbox>::SnapshotState: SnapshotStateStatsExt,
{
    let db = Database::builder()
        .open_read_only(db_path)
        .with_context(|| format!("failed to open redb file at '{}'", db_path.display()))?;
    let mut snapshots = ls_snapshots::<Rt>(db)
        .await
        .context("failed to list snapshots")?;

    // Sort in descending order before printing them out
    snapshots.sort_unstable_by(|a, b| b.state.stats().cmp(a.state.stats()));
    print_snapshots(&snapshots, json).await;

    Ok(())
}

pub(super) async fn zero_out<Rt>(db_path: &Path) -> Result<()>
where
    Rt: ::snaplace::worker::Runtime,
    <Rt::Sandbox as ::snaplace::worker::Sandbox>::SnapshotState: SnapshotStateStatsExt,
{
    let sids = {
        let db = Database::builder()
            .open_read_only(db_path)
            .with_context(|| format!("failed to open redb file at '{}'", db_path.display()))?;
        ls_snapshots::<Rt>(db)
            .await
            .context("failed to list snapshots")?
            .into_iter()
            .map(|s| s.sid)
            .collect::<Vec<_>>()
    };

    let db = Database::open(db_path)
        .with_context(|| format!("failed to open redb file at '{}'", db_path.display()))?;
    let wtxn = db.begin_write().context("failed to begin write txn")?;
    {
        let mut tbl_snaps = wtxn
            .open_table(Tables::<Rt>::SNAPSHOTS)
            .context("failed to open table SNAPSHOTS")?;

        for sid in sids {
            let Some(mut state) = tbl_snaps
                .get(&sid)
                .context("failed to read entry from table SNAPSHOTS")?
                .map(|v| v.value())
            else {
                error!(sandbox.id = %sid, "Entry not found in table SNAPSHOTS");
                continue;
            };

            // Replace existing `SandboxStats` with an empty one
            state.clear_stats();

            match tbl_snaps.insert(&sid, &state) {
                Ok(_old_state) => {}
                Err(err) => error!(
                    error = ?err, sandbox.id = %sid, updated.state = ?state,
                    "Failed to insert entry in table SNAPSHOTS: {err:#}"
                ),
            }
        }
    }
    wtxn.commit()
        .inspect_err(|err| error!(error = ?err, "Failed to commit write txn: {err:#}"))
        .context("failed to commit write txn")
}
