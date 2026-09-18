use std::{
    borrow::Cow,
    fs::File,
    io::Error as ioError,
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use rustix::fs::{fadvise, stat, Advice};
use tokio::task::JoinSet;
use tracing::error;

/// Error returned while attempting to evict a file from the page cache.
#[derive(Debug, ::thiserror::Error)]
pub enum FileError {
    #[error("failed to open(2) snapshot file '{path}'")]
    Open {
        path: PathBuf,
        #[source]
        source: ioError,
    },
    #[error("failed to stat(2) snapshot file '{path}'")]
    Stat {
        path: PathBuf,
        #[source]
        source: ::rustix::io::Errno,
    },
    #[error("failed to fadvise(2) snapshot file '{path}'")]
    Fadvise {
        path: PathBuf,
        #[source]
        source: ::rustix::io::Errno,
    },
}

/// Advise the kernel that the file's cached pages are no longer needed.
///
/// This opens the file read-only, determines its current length, and applies
/// `POSIX_FADV_DONTNEED` over the whole file.
fn fforget_file(path: PathBuf) -> Result<(), FileError> {
    let f = File::options()
        .read(true)
        .open(&path)
        .map_err(|err| FileError::Open {
            path: path.clone(),
            source: err,
        })?;
    let len = stat(&path)
        .map_err(|err| FileError::Stat {
            path: path.clone(),
            source: err,
        })?
        .st_size;
    fadvise(f, 0, NonZeroU64::new(len as _), Advice::DontNeed)
        .map_err(|err| FileError::Fadvise { path, source: err })
}

/// Evict the provided files from the page cache on blocking worker threads.
///
/// All paths are attempted.  File-operation failures are logged, and the first
/// such error is returned after all tasks have completed.  Join failures are
/// logged but do not replace a file-operation result.
pub async fn uncache_files(paths: &[Cow<'_, Path>]) -> Result<(), FileError> {
    let mut join_set = JoinSet::new();

    for p in paths {
        let _ = join_set.spawn_blocking({
            let path = p.as_ref().to_owned();
            move || fforget_file(path)
        });
    }

    let mut ret = Ok(());
    while let Some(join_res) = join_set.join_next().await {
        match join_res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                error!(error = ?err, "Error while uncaching snapshot file: {err:#}");
                if ret.is_ok() {
                    ret = Err(err);
                }
            }
            Err(join_err) => error!(
                error = ?join_err,
                "Failed to join snapshot file uncaching task on blocking thread: {join_err:#}"
            ),
        }
    }

    ret
}
