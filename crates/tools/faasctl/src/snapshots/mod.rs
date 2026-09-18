use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use argh::FromArgs;

use snaplace::{metadata::SandboxStats, FunctionId};

use crate::{Cli, SubCmd};

pub mod cp2m;

mod ls;
use ls::ListCmd;

mod mv;
use mv::MoveCmd;

mod sync;
use sync::SyncCmd;

mod sb_stats;
use sb_stats::SbStatsCmd;

mod forget;
use forget::ForgetCmd;

#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand)]
enum SnapshotsSubCmd {
    List(ListCmd),
    Move(MoveCmd),
    Forget(ForgetCmd),
    SbStats(SbStatsCmd),
    Sync(SyncCmd),
}

/// Sandbox snapshot manipulation utilities
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "snap", help_triggers("-h", "--help", "help"))]
pub struct SnapshotsCmd {
    /// path to the (redb) database file
    #[argh(option, short = 'f')]
    db_path: PathBuf,

    #[argh(subcommand)]
    cmd: SnapshotsSubCmd,
}

impl SnapshotsCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Snapshots(SnapshotsCmd { ref cmd, .. }) = cli.cmd else {
            unreachable!("should not have been routed here");
        };

        match cmd {
            SnapshotsSubCmd::List(ls_cmd) => ls_cmd.run(cli).await,
            SnapshotsSubCmd::Move(mv_cmd) => mv_cmd.run(cli).await,
            SnapshotsSubCmd::Forget(fg_cmd) => fg_cmd.run(cli).await,
            SnapshotsSubCmd::SbStats(sb_stats_cmd) => sb_stats_cmd.run(cli).await,
            SnapshotsSubCmd::Sync(sync_cmd) => sync_cmd.run(cli).await,
        }
    }
}

async fn read_func_file(path: impl AsRef<Path>) -> Result<Vec<FunctionId>> {
    ::tokio::fs::read_to_string(&path)
        .await
        .map(|lines| {
            lines
                .split('\n')
                .filter(|&l| !l.is_empty())
                .map(Into::into)
                .collect()
        })
        .with_context(|| format!("failed to read file '{}'", path.as_ref().display()))
}

/// `faasctl`-local extension trait for snapshot states that persist
/// [`SandboxStats`].
///
/// This trait exists solely to let the offline snapshot administration
/// commands operate generically across different `snaplace` runtime-specific
/// snapshot-state types without requiring `snaplace`'s core [`SnapshotState`]
/// trait to grow tooling-specific methods.
///
/// We implement this only for snapshot-state types whose serialized form embeds
/// [`SandboxStats`]. Implementations are expected to return the persisted
/// statistics stored in the snapshot state, not any live runtime state.
///
///
/// [`SnapshotState`]: snaplace::worker::runtime::SnapshotState
pub(crate) trait SnapshotStateStatsExt {
    /// Returns the [`SandboxStats`] persisted in this snapshot state.
    ///
    /// The returned value is expected to reflect the statistics currently
    /// serialized in the snapshot state and may therefore be stale with
    /// respect to any running sandbox derived from it.
    fn stats(&self) -> &SandboxStats;
    /// Resets the persisted [`SandboxStats`] stored in this snapshot state.
    ///
    /// This is intended for offline maintenance flows, such as
    /// `faasctl snap stats zero-out`, that need to preserve the restorable
    /// snapshot while discarding accumulated usage history.
    fn clear_stats(&mut self);
}

impl SnapshotStateStatsExt for ::snaplace::worker::runtime::fcctrd::MicroVmState {
    fn stats(&self) -> &SandboxStats {
        &self.stats
    }
    fn clear_stats(&mut self) {
        self.stats = Default::default();
    }
}

impl SnapshotStateStatsExt for ::snaplace::worker::runtime::fc::MicroVmState {
    fn stats(&self) -> &SandboxStats {
        &self.stats
    }
    fn clear_stats(&mut self) {
        self.stats = Default::default();
    }
}

/// `faasctl`-local extension trait for runtime-specific snapshot-state types
/// that persist filesystem paths to snapshot backing files.
///
/// This trait exists solely to let the offline snapshot administration
/// commands operate generically across different `snaplace` runtime-specific
/// snapshot-state types without requiring `snaplace`'s core [`SnapshotState`]
/// trait to grow file-management methods needed only by tooling.
///
/// We implement this only for snapshot-state types whose serialized form records
/// the paths of the snapshot files that must exist on disk for the snapshot to
/// remain usable. The returned paths are the persisted paths stored in the
/// snapshot state, not paths discovered dynamically from live runtime state.
///
///
/// [`SnapshotState`]: snaplace::worker::runtime::SnapshotState
pub(crate) trait SnapshotFilesExt: ::snaplace::worker::runtime::SnapshotState {
    /// Returns the persisted path of the snapshot state file, if one is stored.
    ///
    /// Implementations may return [`None`] when the snapshot state in the
    /// database is incomplete or when no state-file path has been recorded.
    fn state_file(&self) -> Option<&Path>;
    /// Returns the persisted path of the snapshot memory file, if one is stored.
    ///
    /// Implementations may return [`None`] when the snapshot state in the
    /// database is incomplete or when no memory-file path has been recorded.
    fn memory_file(&self) -> Option<&Path>;
    /// Rewrites the persisted snapshot-file paths stored in this snapshot state.
    ///
    /// This is intended for offline maintenance flows, such as
    /// `faasctl snap mv`, that preserve the snapshot contents while relocating
    /// the backing files in the filesystem and updating the database to point
    /// at the new paths.
    ///
    /// Passing [`None`] for either argument leaves the corresponding persisted
    /// path unchanged.
    fn rewrite_files(&mut self, new_state: Option<&Path>, new_memory: Option<&Path>) -> Result<()>;
}

impl SnapshotFilesExt for ::snaplace::worker::runtime::fcctrd::MicroVmState {
    fn state_file(&self) -> Option<&Path> {
        self.vm.snapshot_state_file()
    }
    fn memory_file(&self) -> Option<&Path> {
        self.vm.snapshot_memory_file()
    }
    fn rewrite_files(&mut self, new_state: Option<&Path>, new_memory: Option<&Path>) -> Result<()> {
        if let Some(new) = new_state {
            self.vm.set_snapshot_state_file(&new)?;
        }
        if let Some(new) = new_memory {
            self.vm.set_snapshot_memory_file(&new)?;
        }
        Ok(())
    }
}

impl SnapshotFilesExt for ::snaplace::worker::runtime::fc::MicroVmState {
    fn state_file(&self) -> Option<&Path> {
        Some(self.snapshot.state().as_std_path())
    }
    fn memory_file(&self) -> Option<&Path> {
        Some(self.snapshot.memory().as_std_path())
    }
    fn rewrite_files(&mut self, new_state: Option<&Path>, new_memory: Option<&Path>) -> Result<()> {
        if new_state.is_none() && new_memory.is_none() {
            return Ok(());
        }
        self.snapshot = ::snaplace::worker::runtime::fc::SnapshotFiles::new(
            new_state
                .map(|p| {
                    ::camino::Utf8Path::from_path(p)
                        .ok_or_else(|| ::anyhow::anyhow!("Non-UTF-8 path '{}'", p.display()))
                })
                .transpose()?
                .unwrap_or_else(|| self.snapshot.state()),
            new_memory
                .map(|p| {
                    ::camino::Utf8Path::from_path(p)
                        .ok_or_else(|| ::anyhow::anyhow!("Non-UTF-8 path '{}'", p.display()))
                })
                .transpose()?
                .unwrap_or_else(|| self.snapshot.memory()),
        );
        Ok(())
    }
}
