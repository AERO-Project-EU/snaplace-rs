use std::{
    borrow::Cow,
    ffi::OsStr,
    io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use argh::FromArgs;
use redb::{Database, ReadableDatabase};
use tokio::task::JoinSet;
use tracing::{debug, error, info, instrument, warn, Level};
use triomphe::{Arc, ArcBorrow};

use snaplace::{
    metadata::db::Tables,
    worker::runtime::{fc, fcctrd},
    FunctionId, SandboxId,
};

use crate::{
    snapshots::{
        cp2m, ls::ls_snapshots, read_func_file, SnapshotFilesExt, SnapshotsCmd, SnapshotsSubCmd,
    },
    Cli, Runtime, SubCmd,
};

/// Move one or more snapshot files to a destination directory
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "mv", help_triggers("-h", "--help", "help"))]
pub(super) struct MoveCmd {
    /// do NOT remove the source file(s) upon copying the data to destination
    #[argh(switch)]
    no_rm: bool,

    #[allow(clippy::doc_lazy_continuation)]
    /// optionally, provide one or more Function IDs whose snapshot files will be used as source
    /// files.
    ///
    /// - If this option is specified, it is an error to also provide either source files as
    /// positional arguments at command line or the `--from-func-file` option.
    /// - This option is supported for the following runtimes: `fcctrd`, `fc`
    #[argh(option)]
    func: Vec<FunctionId>,

    #[allow(clippy::doc_lazy_continuation)]
    /// optionally, provide a path to a line-delimited file of Function IDs whose snapshot files
    /// will be used as source files.
    ///
    /// - If this option is specified, it is an error to also provide either source files as
    /// positional arguments at command line, or the `--func` option.
    /// - This option is supported for the following runtimes: `fcctrd`, `fc`
    #[argh(option)]
    from_func_file: Option<PathBuf>,

    /// destination directory path to move the snapshot files into
    #[argh(positional)]
    dst: PathBuf,

    /// source file path(s) to move
    #[argh(positional)]
    src: Vec<PathBuf>,
}

impl MoveCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Snapshots(SnapshotsCmd {
            db_path,
            cmd:
                SnapshotsSubCmd::Move(MoveCmd {
                    no_rm,
                    func,
                    from_func_file,
                    dst,
                    src,
                }),
        }) = &cli.cmd
        else {
            unreachable!("should not have been routed here");
        };

        // Make sure destination path is a directory
        if !dst
            .metadata()
            .with_context(|| format!("failed to stat(2) destination path '{}'", dst.display()))?
            .is_dir()
        {
            bail!("Destination path '{}' is not a directory", dst.display());
        }

        // Validate input CLI arguments combination
        let num_sources = [!src.is_empty(), !func.is_empty(), from_func_file.is_some()]
            .iter()
            .filter(|&&x| x)
            .count();
        if num_sources == 0 {
            info!("No source files to move");
            return Ok(());
        }
        if num_sources > 1 {
            bail!("Only one of `--func`, `--from-func-file` and positional arguments should be provided; see --help")
        }

        // Populate `src` with snapshot file paths of the Functions specified via either `--func`
        // or `--from-func-file`
        let mut tmp_src = None;
        if src.is_empty() {
            let fids = if !func.is_empty() {
                Cow::Borrowed(func)
            } else if let Some(func_file_path) = from_func_file {
                Cow::Owned(read_func_file(func_file_path).await?)
            } else {
                unreachable!("when no positional args, there is `--func` or `--from-func-file`")
            };
            tmp_src = Some(files_from_function_ids(&cli.runtime, &fids, db_path).await?);
        }
        let src = tmp_src.as_ref().unwrap_or(src);

        // Make sure all provided source file paths are indeed regular files, and error out if not
        let mut invalid_src = Vec::new();
        for path in src {
            match path.metadata() {
                Ok(md) if md.is_file() => {}
                Ok(_md) => invalid_src.push((path, String::from("not a regular file"))),
                Err(err) => invalid_src.push((path, format!("failed to stat(2): {err:#}"))),
            }
        }
        if !invalid_src.is_empty() {
            for (path, reason) in invalid_src {
                error!("Provided source path '{}': {reason}", path.display());
            }
            bail!("Aborted due to invalid input source files");
        }

        // Make sure we can open as many file descriptors as possible, to embarassingly parallelize
        let _ = increase_rlimit_nofile()
            .inspect_err(|err| warn!(error = ?err, "Failed to increase RLIMIT_NOFILE: {err}"));

        let db = Database::open(db_path)
            .with_context(|| format!("failed to open redb file at '{}'", db_path.display()))?;

        match cli.runtime {
            Runtime::FirecrackerContainerd => {
                copy::<fcctrd::FirecrackerContainerd>(dst, src, db, *no_rm).await
            }
            Runtime::Firecracker => copy::<fc::Firecracker>(dst, src, db, *no_rm).await,
        }
    }
}

#[instrument(level = Level::DEBUG, skip_all)]
fn increase_rlimit_nofile() -> Result<()> {
    use rustix::process::{getrlimit, setrlimit, Resource};

    let mut rlim = getrlimit(Resource::Nofile);
    rlim.current = rlim.maximum;
    setrlimit(Resource::Nofile, rlim)
        .with_context(|| format!("failed to configure maximum file descriptor using: {rlim:?}"))
}

async fn files_from_function_ids(
    runtime: &Runtime,
    function_ids: &[FunctionId],
    db_path: impl AsRef<Path>,
) -> Result<Vec<PathBuf>> {
    let db_path = db_path.as_ref();
    let db = Database::builder()
        .open_read_only(db_path)
        .with_context(|| format!("failed to open redb file at '{}'", db_path.display()))?;

    match runtime {
        Runtime::FirecrackerContainerd => Ok(ls_snapshots::<fcctrd::FirecrackerContainerd>(db)
            .await
            .context("failed to list snapshots stored in DB")?
            .into_iter()
            .filter_map(|snap| {
                function_ids.contains(&snap.fid).then(|| {
                    (
                        (snap.sid, snap.fid), // <-- just to log absence
                        snap.state.vm.snapshot_state_file().map(Path::to_path_buf),
                        snap.state.vm.snapshot_memory_file().map(Path::to_path_buf),
                    )
                })
            })
            .fold(Vec::new(), |mut acc, ((sid, fid), sf, mf)| {
                if let (Some(sf), Some(mf)) = (sf, mf) {
                    acc.push(sf);
                    acc.push(mf);
                } else {
                    warn!(function.id = %fid, sandbox.id = %sid, "Missing snapshot file(s); skipping");
                }
                acc
            })),

        Runtime::Firecracker => Ok(ls_snapshots::<fc::Firecracker>(db)
            .await
            .context("failed to list snapshots stored in DB")?
            .into_iter()
            .filter_map(|snap| {
                function_ids.contains(&snap.fid).then(|| {
                    (
                        snap.state.snapshot.state().as_std_path().to_path_buf(),
                        snap.state.snapshot.memory().as_std_path().to_path_buf(),
                    )
                })
            })
            .fold(Vec::new(), |mut acc, (sf, mf)| {
                acc.push(sf);
                acc.push(mf);
                acc
            })),
    }
}

/// # Note
///
/// Assumes each snapshot consists of two files, a `.state` and a `.memory`, where both have
/// the same base name, and that base name is the name of the associated TAP device (and also
/// the name of the associated [`MicroVm`]).
///
///
/// [`MicroVm`]: snaplace::worker::runtime::fcctrd::MicroVm
async fn copy<Rt>(dst: &Path, src: &Vec<PathBuf>, db: Database, no_rm: bool) -> Result<()>
where
    Rt: ::snaplace::worker::Runtime,
    <Rt::Sandbox as ::snaplace::worker::Sandbox>::SnapshotState: SnapshotFilesExt,
{
    let mut js = JoinSet::new();
    let db = Arc::new(db);
    for src in src {
        js.spawn_blocking({
            let src = src.clone();
            let dst = dst.join(src.file_name().expect("checked that != '..'"));
            let db = Arc::clone(&db);

            move || {
                // Make sure we do not overwrite any file at destination path
                match dst.metadata() {
                    Err(err) if matches!(err.kind(), io::ErrorKind::NotFound) => {}
                    Err(err) => {
                        warn!(error = ?err, dst = %dst.display(), "Failed to stat(2) dst path");
                        return Err(err).context("failed to stat(2) destination path");
                    }
                    Ok(_md) => {
                        error!(
                            dst = %dst.display(),
                            "File at destination path already exists; overwriting disallowed"
                        );
                        return Err(io::Error::from(io::ErrorKind::AlreadyExists))
                            .with_context(|| format!("skipping overwrite of '{}'", dst.display()));
                    }
                }

                // Validate source file
                let (sid, is_mem_file) = validate_source::<Rt>(db.borrow_arc(), &src)
                    .with_context(|| {
                        format!(
                            "failed to validate source file '{}'; skipping it",
                            src.display()
                        )
                    })?;

                // Carefully do the actual copying
                cp2m::copy_for_huge_dax_atomic(&src, &dst).with_context(|| {
                    format!(
                        "failed to copy file '{}' -> '{}'",
                        src.display(),
                        dst.display()
                    )
                })?;

                // Update DB
                update_db::<Rt>(db.borrow_arc(), sid, is_mem_file, &src, &dst).with_context(
                    || {
                        format!(
                            "failed to update DB after copying file '{}' to destination '{}'",
                            src.display(),
                            dst.display()
                        )
                    },
                )?;

                // If `--no-rm` not provided, also remove the source file (on best effort basis)
                if !no_rm && let Err(err) = ::std::fs::remove_file(&src) {
                    error!(error = ?err, source.file = %src.display(), "Failed to remove file");
                }

                Ok::<_, ::anyhow::Error>(())
            }
        });
    }

    let mut num_succ = 0;
    let mut num_fail = 0;
    while let Some(r) = js.join_next().await {
        match r {
            Ok(Ok(())) => {
                num_succ += 1;
                debug!("#successful.copies" = %num_succ, "#failed.copies" = %num_fail);
            }
            Ok(Err(err)) => {
                num_fail += 1;
                warn!(error = ?err, "#failed.copies" = %num_fail, "#successful.copies" = %num_succ);
            }
            Err(jerr) => {
                error!(error = ?jerr, "Failed to join tokio task: {jerr:#}");
                num_fail += 1;
                debug!("#failed.copies" = %num_fail, "#successful.copies" = %num_succ);
            }
        }
    }
    info!("#successful.copies" = %num_succ, "#failed.copies" = %num_fail);
    Ok(())
}

/// Validates the given source file, returning `(Sandbox ID, is_memory_file)` on success.
#[instrument(level = Level::TRACE, skip_all, fields(source.file = %src.display()))]
fn validate_source<Rt>(db: ArcBorrow<Database>, src: &Path) -> Result<(SandboxId, bool)>
where
    Rt: ::snaplace::worker::Runtime,
    <Rt::Sandbox as ::snaplace::worker::Sandbox>::SnapshotState: SnapshotFilesExt,
{
    // Parse Sandbox ID
    let sid = match src.file_stem().and_then(OsStr::to_str).map(SandboxId::new) {
        Some(sid) => sid,
        None => {
            error!("Failed to parse Sandbox ID from '{:?}'", src.file_stem());
            bail!("Failed to parse Sandbox ID from '{:?}'", src.file_stem());
        }
    };
    // Parse file extension
    let is_memory_file = match src.extension().and_then(OsStr::to_str) {
        Some("memory") => true,
        Some("state") => false,
        ext @ (Some(_) | None) => bail!("Unexpected file extension: {ext:?}"),
    };
    // Query inode
    let src_ino = src
        .metadata()
        .with_context(|| format!("failed to stat(2) source file '{}'", src.display()))?
        .ino();

    let state = {
        let rtxn = db.begin_read().context("failed to begin write txn")?;
        let tbl_snap = rtxn
            .open_table(Tables::<Rt>::SNAPSHOTS)
            .context("failed to open table SNAPSHOTS")?;
        match tbl_snap.get(&sid) {
            Ok(Some(state)) => state.value(),
            Ok(None) => {
                error!(sandbox.id = %sid, "Sandbox ID not found in table SNAPSHOTS");
                bail!("Sandbox ID '{sid}' not found in table SNAPSHOTS");
            }
            Err(err) => {
                error!(
                    error = ?err, sandbox.id = %sid,
                    "Failed to read state from table SNAPSHOTS: {err:#}"
                );
                return Err(err).with_context(|| {
                    format!("failed to read state of Sandbox '{sid}' from table SNAPSHOTS")
                });
            }
        }
    };

    let stored_file = if is_memory_file {
        state.memory_file()
    } else {
        state.state_file()
    };
    let stored_file = match stored_file {
        Some(path) => path,
        None => {
            error!(
                sandbox.id = %sid,
                source.file = %src.display(),
                "No related snapshot {} file is stored in table SNAPSHOTS",
                if is_memory_file { "memory" } else { "state" },
            );
            bail!(
                "No snapshot {} file related to '{}' is stored in table SNAPSHOTS for Sandbox '{}'",
                if is_memory_file { "memory" } else { "state" },
                src.display(),
                sid,
            );
        }
    };
    let stored_file_md = match stored_file.metadata() {
        Ok(md) => md,
        Err(err) => {
            error!(error = ?err, "Failed to stat(2) file '{}': {err:#}", stored_file.display());
            bail!("Failed to stat(2) file '{}'", stored_file.display());
        }
    };
    if src_ino != stored_file_md.ino() {
        error!(
            sandbox.id = %sid,
            source.file = %src.display(),
            source.file.inode = %src_ino,
            stored.snapshot.file = %stored_file.display(),
            stored.snapshot.file.inode = %stored_file_md.ino(),
            "File paths refer to different files",
        );
        bail!("File paths refer to different files");
    }

    Ok((sid, is_memory_file))
}

/// Updates the stored `MicroVmState` in DB with the new (destination) file path, replacing the
/// old (source) file path.
#[instrument(
    level = Level::DEBUG,
    skip_all,
    fields(sandbox.id = %sid, source.file = %src.display(), destination.file = %dst.display()),
)]
fn update_db<Rt>(
    db: ArcBorrow<Database>,
    sid: SandboxId,
    is_memory_file: bool,
    src: &Path,
    dst: &Path,
) -> Result<()>
where
    Rt: ::snaplace::worker::Runtime,
    <Rt::Sandbox as ::snaplace::worker::Sandbox>::SnapshotState: SnapshotFilesExt,
{
    let wtxn = db.begin_write().context("failed to begin write txn")?;
    {
        let mut tbl_snaps = wtxn
            .open_table(Tables::<Rt>::SNAPSHOTS)
            .context("failed to open table SNAPSHOTS")?;

        let Some(mut state_guard) = tbl_snaps
            .get_mut(&sid)
            .context("failed to read Snapshot State")?
        else {
            bail!("No MicroVmState related to Sandbox ID '{sid}' was found this time!");
        };

        let mut state = state_guard.value();
        if is_memory_file {
            state.rewrite_files(None, Some(dst))
        } else {
            state.rewrite_files(Some(dst), None)
        }
        .context("failed to set snapshot file")?;

        state_guard
            .insert(&state)
            .context("failed to insert updated MicroVmState")?;
    }
    wtxn.commit().map_err(|err| {
        error!(error = ?err, "Failed to commit table SNAPSHOTS changes to DB: {err:#}");
        ::anyhow::Error::from(err).context("failed to commit changes to table SNAPSHOTS")
    })
}
