//! Minimal cgroup-v2 monitor.
//!
//! Reads:
//! - `memory.current` => bytes (`u64`) gauge
//! - `cpu.stat`       => cumulative counters since cgroup creation
//!
//! Reports (computed from deltas between consecutive samples):
//! - `cpu_core_eq_pct_x100`: core-equivalent CPU% × 100 (fixed-point)
//!
//!   Formula per interval: `cpu% * 100 = (Δusage_usec * 10_000) / Δt_usec`
//!   Interpretation:
//!   * 100.00% => one full core busy for the interval
//!   * 250.00% => 2.5 core-equivalents
//! - `cpu_user_core_eq_pct_x100`: user-mode CPU% × 100 (fixed-point)
//! - `cpu_system_core_eq_pct_x100`: kernel-mode CPU% × 100 (fixed-point)
//!
//! Notes:
//! - `cpu.stat` usage counters are cumulative since cgroup creation.
//! - `memory.current` includes anon + file cache + kernel-charged memory, etc.

use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use fastant::Instant;
use rustix::{
    fd::OwnedFd,
    fs::{open, seek, Mode, OFlags, SeekFrom},
    io::Errno,
};

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("failed to open file '{path}'")]
    Open {
        path: PathBuf,
        #[source]
        source: Errno,
    },
    #[error("failed to read cgroup file '{name}'")]
    Read {
        name: &'static str,
        #[source]
        source: Errno,
    },
    #[error("failed while parsing cgroup file '{0}'")]
    Parse(&'static str),
}

#[derive(Clone, Copy, Debug)]
pub struct Sample {
    /// Unix timestamp in nanoseconds (wall clock).
    pub ts_unix_ns: u128,

    /// `memory.current`, in _bytes_.
    pub mem_curr_bytes: u64,

    /// Core-equivalent CPU utilization over the interval since the previous sample.
    /// - Units: `CPU% × 100` (fixed-point).
    /// - Example: `12345` => `123.45%` (≈ `1.2345` core-equivalents)
    pub cpu_core_eq_pct_x100: u64,

    /// User-mode core-equivalent CPU utilization over the interval since the previous sample.
    ///
    /// Units: `CPU% × 100` (fixed-point).
    pub cpu_user_core_eq_pct_x100: u64,

    /// System-mode (kernel) core-equivalent CPU utilization over the interval since the previous
    /// sample.
    ///
    /// Units: `CPU% × 100` (fixed-point).
    pub cpu_system_core_eq_pct_x100: u64,

    /// Optional but often useful: throttling share over the interval (if `cpu.max` is used).
    /// Units: `CPU% × 100` (fixed-point), core-equivalent.
    pub cpu_throttled_core_eq_pct_x100: u64,
}

const CPU_BUFSZ: usize = 2048;

pub struct CgroupMonitor {
    mem_curr_fd: OwnedFd,
    cpu_stat_fd: OwnedFd,

    // Reused scratch buffers to avoid allocations.
    buf_mem: [u8; 64],
    buf_cpu: [u8; CPU_BUFSZ],

    // State for delta computation.
    last_instant: Instant,
    last_usage_usec: u64,
    last_user_usec: u64,
    last_system_usec: u64,
    last_throttled_usec: u64,
}

impl CgroupMonitor {
    pub fn new(cgroup_path: impl AsRef<Path>) -> Result<Self, Error> {
        let cg_path = cgroup_path.as_ref();

        let memory_current_path = cg_path.join("memory.current");
        let cpu_stat_path = cg_path.join("cpu.stat");

        let open_ro = |path: &Path| -> Result<OwnedFd, Error> {
            open(path, OFlags::RDONLY | OFlags::CLOEXEC, Mode::empty()).map_err(|err| Error::Open {
                path: path.to_path_buf(),
                source: err,
            })
        };
        let mem_curr_fd = open_ro(&memory_current_path)?;
        let cpu_stat_fd = open_ro(&cpu_stat_path)?;

        // Initialize the delta state by reading cpu.stat once
        let mut tmp = [0u8; CPU_BUFSZ];
        let n = read_file(&cpu_stat_fd, &mut tmp).map_err(|err| Error::Read {
            name: "cpu.stat (prime)",
            source: err,
        })?;
        let (usage_usec, user_usec, system_usec, throttled_usec) = parse_cpu_stat_times(&tmp[..n])?;

        Ok(Self {
            mem_curr_fd,
            cpu_stat_fd,
            buf_mem: [0u8; 64],
            buf_cpu: [0u8; CPU_BUFSZ],
            last_instant: Instant::now(),
            last_usage_usec: usage_usec,
            last_user_usec: user_usec,
            last_system_usec: system_usec,
            last_throttled_usec: throttled_usec,
        })
    }

    /// Take a timestamped sample of memory.current and core-equivalent CPU% over the last interval.
    pub fn sample(&mut self) -> Result<Sample, Error> {
        let ts_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime should never precede Epoch")
            .as_nanos();

        // Measure dt using monotonic time.
        let now_inst = Instant::now();
        let dt = now_inst.duration_since(self.last_instant);
        self.last_instant = now_inst;

        // memory.current: single integer + '\n'
        let mem_n = read_file(&self.mem_curr_fd, &mut self.buf_mem).map_err(|err| Error::Read {
            name: "memory.current",
            source: err,
        })?;
        let mem_curr_bytes =
            parse_u64_trim(&self.buf_mem[..mem_n]).ok_or(Error::Parse("memory.current"))?;

        // cpu.stat: few lines "key value\n"
        let cpu_n = read_file(&self.cpu_stat_fd, &mut self.buf_cpu).map_err(|err| Error::Read {
            name: "cpu.stat",
            source: err,
        })?;
        let (usage_usec, user_usec, system_usec, throttled_usec) =
            parse_cpu_stat_times(&self.buf_cpu[..cpu_n])?;

        let du = usage_usec.saturating_sub(self.last_usage_usec);
        let du_user = user_usec.saturating_sub(self.last_user_usec);
        let du_system = system_usec.saturating_sub(self.last_system_usec);
        let dth = throttled_usec.saturating_sub(self.last_throttled_usec);

        self.last_usage_usec = usage_usec;
        self.last_user_usec = user_usec;
        self.last_system_usec = system_usec;
        self.last_throttled_usec = throttled_usec;

        let (
            cpu_core_eq_pct_x100,
            cpu_user_core_eq_pct_x100,
            cpu_system_core_eq_pct_x100,
            cpu_throttled_core_eq_pct_x100,
        ) = if dt.is_zero() {
            (0, 0, 0, 0)
        } else {
            let dt_usec = dt.as_micros(); // wall time in microseconds
            if dt_usec == 0 {
                (0, 0, 0, 0)
            } else {
                // cpu% * 100 = (Δusec * 10000) / Δt_usec
                let cpu_x100 = ((du as u128) * 10_000u128 / dt_usec) as u64;
                let cpu_user_x100 = ((du_user as u128) * 10_000u128 / dt_usec) as u64;
                let cpu_system_x100 = ((du_system as u128) * 10_000u128 / dt_usec) as u64;
                let thr_x100 = ((dth as u128) * 10_000u128 / dt_usec) as u64;
                (cpu_x100, cpu_user_x100, cpu_system_x100, thr_x100)
            }
        };

        Ok(Sample {
            ts_unix_ns,
            mem_curr_bytes,
            cpu_core_eq_pct_x100,
            cpu_user_core_eq_pct_x100,
            cpu_system_core_eq_pct_x100,
            cpu_throttled_core_eq_pct_x100,
        })
    }
}

/// Reads a small pseudo-file from offset 0 into `buf`, without allocating.
///
/// # Note
///
/// Pseudo-files implemented using the kernel's `seq_file` interface, may not support seeking or
/// non-zero offsets.  This function attempts to use `pread(2)` first, and falls back to plain
/// `lseek(2)`+`read(2)` if that fails.
fn read_file(fd: &OwnedFd, buf: &mut [u8]) -> Result<usize, Errno> {
    match ::rustix::io::pread(fd, &mut *buf, 0) {
        Ok(n) => Ok(n),
        Err(err) if err == Errno::SPIPE => {
            seek(fd, SeekFrom::Start(0))?;
            ::rustix::io::read(fd, buf)
        }
        err => err,
    }
}

/// Parse an ASCII u64 with optional leading whitespace and trailing '\n'.
fn parse_u64_trim(bytes: &[u8]) -> Option<u64> {
    let mut i = 0;
    while i < bytes.len() && is_ws(bytes[i]) {
        i += 1;
    }
    let mut v = 0;
    let mut any = false;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_digit() {
            any = true;
            //v = v.checked_mul(10)?.checked_add((b - b'0') as u64)?;
            v = (v * 10) + ((b - b'0') as u64);
            i += 1;
        } else {
            break;
        }
    }
    any.then_some(v)
}

#[inline(always)]
fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\n' | b'\r' | b'\t')
}

/// Parse `cpu.stat` and extract cumulative microsecond counters:
/// - `usage_usec` (total)
/// - `user_usec`
/// - `system_usec`
/// - `throttled_usec`
fn parse_cpu_stat_times(bytes: &[u8]) -> Result<(u64, u64, u64, u64), Error> {
    let mut usage_usec = 0;
    let mut user_usec = 0u64;
    let mut system_usec = 0u64;
    let mut throttled_usec = 0;

    let mut i = 0;
    while i < bytes.len() {
        // parse key [^ws]+
        while i < bytes.len() && is_ws(bytes[i]) {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let key_start = i;
        while i < bytes.len() && !is_ws(bytes[i]) {
            i += 1;
        }
        let key = &bytes[key_start..i];

        // skip ws before value
        while i < bytes.len() && is_ws(bytes[i]) {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }

        // parse value digits
        let val_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        let val = parse_u64_trim(&bytes[val_start..i]).ok_or(Error::Parse("cpu.stat"))?;

        if key == b"usage_usec" {
            usage_usec = val;
        } else if key == b"user_usec" {
            user_usec = val;
        } else if key == b"system_usec" {
            system_usec = val;
        } else if key == b"throttled_usec" {
            throttled_usec = val;
        }

        // skip to end of line
        while i < bytes.len() && bytes[i] != b'\n' {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b'\n' {
            i += 1;
        }
    }

    Ok((usage_usec, user_usec, system_usec, throttled_usec))
}
