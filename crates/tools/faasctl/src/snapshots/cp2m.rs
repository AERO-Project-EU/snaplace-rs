//! Auxiliary module for copying a file in a way that, if the destination filesystem is
//! DAX-enabled (e.g., `ext4+DAX` or `xfs+DAX`), then PMD-sized (2 MiB) _huge DAX_ mappings
//! which may occur later are enabled/"encouraged".
//!
//! Key behaviors:
//! - Preallocate the destination with `fallocate(KEEP_SIZE)` rounded up to 2 MiB, so extents
//!   tend to be big and contiguous without changing the visible size.
//! - Write sequentially in 2 MiB chunks (no sparse holes) (or offload writing to kernel for
//!   performance; e.g., via `sendfile(2)` or `copy_file_range(2)`, either of which may be
//!   employed by [`std::io::copy()`]), then `fsync(2)`s.
//! - Leave final `st_size` equal to the source length; callers can `mmap(2)` any 2 MiB-aligned
//!   subrange they wish.
//!
//! # Note
//!
//! Huge DAX also depends on device/partition start alignment and FS layout. These helpers
//! cannot guarantee PMD faults, but attempts to set a ..."friendly environment" for them.

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    os::fd::AsFd,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use rustix::{
    fs::{self, Advice, FallocateFlags, Mode, OFlags},
    io::Errno,
};
use tracing::{debug, instrument, Level};

/// 2 MiB in bytes (PMD size on x86_64 with 4 KiB base pages).
pub const PMD_SIZE: u64 = 2 << 20;

/// Round up to 2MiB.
#[inline]
fn round_up_pmd(x: u64) -> u64 {
    if x == 0 {
        0
    } else {
        ((x - 1) / PMD_SIZE + 1) * PMD_SIZE
    }
}

/// Copy `src` to `dst` in a way that enables the kernel to form PMD-sized DAX mappings later
/// (if `dst` is on a DAX-enabled filesystem, such as ext4+DAX).
///
/// In particular, this method:
/// - preallocates the `dst` file with `fallocate(KEEP_SIZE)` up to the next 2 MiB;
/// - (hints sequential access with `posix_fadvise(SEQUENTIAL)` for optimal readahead, even
///   though it might be irrelevant for `dst` when it actually is on DAX-enabled filesystem);
/// - copies sequentially in 2 MiB chunks (avoiding holes);
/// - calls `fsync(2)` on `dst` before returning.
#[allow(dead_code)]
#[instrument(
    level = Level::TRACE,
    skip_all,
    fields(src = %src.as_ref().display(), dst = %dst.as_ref().display()),
)]
pub fn copy_for_huge_dax(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    let mut src_f = File::open(&src)?;
    let src_len = src_f.metadata()?.len();

    let mut dst_f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&dst)?;

    // Preallocate full 2 MiB–rounded space (without changing `st_size`)
    let alloc_len = round_up_pmd(src_len);
    if alloc_len > 0 {
        fs::fallocate(dst_f.as_fd(), FallocateFlags::KEEP_SIZE, 0, alloc_len)?;
    }

    // Hint sequential access to enable increased readahead window sizes
    let _ = fs::fadvise(src_f.as_fd(), 0, None, Advice::Sequential).inspect_err(|err| debug!(
            error = ?err, file = %src.as_ref().display(), "posix_fadvise(SEQUENTIAL) failed: {err:#}"
        ));
    let _ = fs::fadvise(dst_f.as_fd(), 0, None, Advice::Sequential).inspect_err(|err| debug!(
            error = ?err, file = %dst.as_ref().display(), "posix_fadvise(SEQUENTIAL) failed: {err:#}"
        ));

    // Copy in PMD-sized chunks
    let mut buf = vec![0u8; PMD_SIZE as _];
    let mut remaining = src_len;
    while remaining > 0 {
        let want = remaining.min(PMD_SIZE) as _;
        // read_exact guarantees we actually fill `want` bytes unless EOF
        src_f.read_exact(&mut buf[..want])?;
        dst_f.write_all(&buf[..want])?;
        remaining -= want as u64;
    }

    fs::fsync(dst_f.as_fd()).map_err(Into::into)
}

/// Atomically copy `src` to `dst` (using [`std::io::copy()`]), in a way that enables the kernel to
/// form PMD-sized DAX mappings later (if `dst` is on a DAX-enabled filesystem, such as ext4+DAX).
#[instrument(
    level = Level::TRACE,
    skip_all,
    fields(src = %src.as_ref().display(), dst = %dst.as_ref().display()),
)]
pub fn copy_for_huge_dax_atomic(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    let dst_path = dst.as_ref();
    let dst_dir = dst_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination has no parent directory",
        )
    })?;

    let mut src_f = File::open(&src)?;
    let src_len = src_f.metadata()?.len();

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp_path = dst_dir.join(format!(".tmp-hdax-{nonce}"));
    let mut tmp_f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)?;

    // 2 MiB-rounded preallocation, `KEEP_SIZE` so visible length tracks actual bytes copied
    let alloc_len = round_up_pmd(src_len);
    if alloc_len > 0 {
        fs::fallocate(tmp_f.as_fd(), FallocateFlags::KEEP_SIZE, 0, alloc_len)?;
    }

    // Do the actual copy, hopefully optimally
    ::std::io::copy(&mut src_f, &mut tmp_f)?;

    // `fdatasync(2)` and then atomically `rename(2)`
    fs::fdatasync(tmp_f.as_fd())?;
    ::std::fs::rename(&tmp_path, dst_path)?;

    // `fsync(2)` the directory to persist new name as well
    let dirfd = fs::openat(
        ::rustix::fs::CWD,
        dst_dir,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    fs::fsync(dirfd.as_fd()).map_err(Into::into)
}

/// Same as [`copy_for_huge_dax`], but attempts ti use `sendfile(2)` first, and falls
/// back to regular `read(2)`s and `write(2)`s only if the former is unavailable.
///
/// Notes:
/// - We still preallocate with `fallocate(KEEP_SIZE)` to encourage big, contiguous
///   extents. The copy method does not change allocation that already exists.
/// - We avoid sparse regions by copying the full range sequentially from offset 0.
/// - `sendfile` may return `EINVAL`/`ENOSYS`/`EOPNOTSUPP`/`EXDEV` on some
///   filesystem/driver combos; in that case we transparently fall back.
#[allow(dead_code)]
#[instrument(
    level = Level::TRACE,
    skip_all,
    fields(src = %src.as_ref().display(), dst = %dst.as_ref().display()),
)]
pub fn copy_for_huge_dax_sendfile(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    let mut src_f = File::open(&src)?;
    let src_len = src_f.metadata()?.len();

    let mut dst_f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&dst)?;

    // Preallocate full 2 MiB–rounded space (without changing `st_size`).
    let alloc_len = round_up_pmd(src_len);
    if alloc_len > 0 {
        fs::fallocate(dst_f.as_fd(), FallocateFlags::KEEP_SIZE, 0, alloc_len)?;
    }

    // Hint sequential access to enable increased readahead window sizes.
    let _ = fs::fadvise(src_f.as_fd(), 0, None, Advice::Sequential).inspect_err(|err| debug!(
            error = ?err, file = %src.as_ref().display(), "posix_fadvise(SEQUENTIAL) failed: {err:#}"
        ));
    let _ = fs::fadvise(dst_f.as_fd(), 0, None, Advice::Sequential).inspect_err(|err| debug!(
            error = ?err, file = %dst.as_ref().display(), "posix_fadvise(SEQUENTIAL) failed: {err:#}"
        ));

    // Try `sendfile(2)` in a loop.
    let mut remaining = src_len as usize;
    let mut used_sendfile = true;
    while remaining > 0 {
        match fs::sendfile(dst_f.as_fd(), src_f.as_fd(), None, remaining.min(1 << 30)) {
            Ok(0) => break, // EOF
            Ok(n) => remaining -= n,
            Err(Errno::XDEV | Errno::INVAL | Errno::NOSYS | Errno::OPNOTSUPP) => {
                // fallback on cross-fs, not implemented, or not supported
                used_sendfile = false;
                break;
            }
            Err(errno) => return Err(errno.into()),
        }
    }

    if !used_sendfile && remaining > 0 {
        // Rewind src and dst offsets and fallback to read(2) & write(2)
        fs::seek(src_f.as_fd(), fs::SeekFrom::Start(0)).map_err(io::Error::from)?;
        fs::seek(dst_f.as_fd(), fs::SeekFrom::Start(0)).map_err(io::Error::from)?;

        // Copy in PMD-sized chunks.
        let mut buf = vec![0u8; PMD_SIZE as _];
        let mut remaining = src_len;
        while remaining > 0 {
            let want = remaining.min(PMD_SIZE) as usize;
            // read_exact guarantees we actually fill `want` bytes unless EOF.
            src_f.read_exact(&mut buf[..want])?;
            dst_f.write_all(&buf[..want])?;
            remaining -= want as u64;
        }
    }

    fs::fsync(dst_f.as_fd()).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;

    use super::*;

    #[test]
    fn round_up() {
        assert_eq!(round_up_pmd(0), 0);
        assert_eq!(round_up_pmd(1), PMD_SIZE);
        assert_eq!(round_up_pmd(PMD_SIZE - 1), PMD_SIZE);
        assert_eq!(round_up_pmd(PMD_SIZE), PMD_SIZE);
        assert_eq!(round_up_pmd(PMD_SIZE + 1), PMD_SIZE * 2);
    }

    /// Basic in-place test; does NOT require DAX.
    #[test]
    fn copies_bytes() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let src_path = tmp.path().join("src.bin");
        let dst_path = tmp.path().join("dst.bin");

        // Create a ~2.5 MiB source file.
        let mut src = File::create(&src_path)?;
        src.write_all(&vec![0xAB; (PMD_SIZE as usize) + (PMD_SIZE as usize / 2)])?;
        drop(src);

        copy_for_huge_dax(&src_path, &dst_path)?;
        let a = fs::read(&src_path)?;
        let b = fs::read(&dst_path)?;
        assert_eq!(a, b);
        Ok(())
    }
}
