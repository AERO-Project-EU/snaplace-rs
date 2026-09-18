use std::{fs, io, num::ParseIntError, time::Duration};

use compact_str::{format_compact, CompactString};
use tracing::{instrument, Level};

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("I/O error: {msg}")]
    Io {
        msg: Box<str>,
        #[source]
        source: io::Error,
    },
    #[error("integer parsing error: {msg}")]
    ParseInt {
        msg: Box<str>,
        #[source]
        source: ParseIntError,
    },
}

pub fn pid_exists(pid: u32) -> bool {
    ::rustix::fs::stat(format_compact!("/proc/{pid}").as_str()).is_ok()
}

/// Repeatedly checks (every `period`) whether the given PID exists, returning `true` when it
/// is **not** found, or `false` if `timeout` elapses and the PID still exists.
///
/// # Returns
///
/// - `true` if `pid` has exited within the given `timeout`
/// - `false` if `timeout` elapsed and `pid` is still present
#[instrument(level = Level::TRACE)]
pub async fn poll_pid_until_exit(pid: u32, period: Duration, timeout: Duration) -> bool {
    let mut ticker = ::tokio::time::interval(period);
    let timeout = ::tokio::time::sleep(timeout);
    ::tokio::pin!(timeout);

    loop {
        ::tokio::select! {
            biased;
            _now = ticker.tick() => {
                if !pid_exists(pid) {
                    return true;
                }
            }
            () = &mut timeout => return false,
        }
    }

    //// TODO: Would pidfd be applicable/preferred here? E.g., something along the lines of:
    //use ::rustix::{
    //    fd::AsFd,
    //    process::{pidfd_open, waitid, Pid, PidfdFlags, WaitId, WaitidOptions},
    //};
    //::tokio::task::spawn_blocking(move || {
    //    let pidfd = pidfd_open(
    //        Pid::from_raw(proc.ppid() as _).unwrap(), // wait for the shim
    //        PidfdFlags::empty(),
    //    )
    //    .expect("TODO");
    //    let _st = waitid(WaitId::PidFd(pidfd.as_fd()), WaitidOptions::EXITED).expect("TODO");
    //})
    //.await
    //.expect("TODO: JoinError");
    //// TODO: though we'd also need async & timeout
}

#[derive(Debug)]
pub struct Process {
    #[allow(dead_code)]
    pid: u32,
    comm: CompactString,
    #[allow(dead_code)]
    state: u8,
    ppid: u32,
    pgrp: u32,

    procfs_path: CompactString,
}

impl Process {
    pub fn stat(pid: u32) -> Result<Self, Error> {
        let path = format_compact!("/proc/{pid}/stat");
        let stat = fs::read_to_string(path.as_str()).map_err(|err| Error::Io {
            msg: format!("failed to read {path:?}").into_boxed_str(),
            source: err,
        })?;
        let stat = stat.split_whitespace().collect::<Vec<_>>();

        assert_eq!(stat[0].parse(), Ok(pid));
        let comm = stat[1][1..stat[1].len() - 1].into(); // remove parentheses
        let state = stat[2].as_bytes()[0];
        let ppid = stat[3].parse().map_err(|err| Error::ParseInt {
            msg: "failed to parse PPID as u32".into(),
            source: err,
        })?;
        let pgrp = stat[4].parse().map_err(|err| Error::ParseInt {
            msg: "failed to parse PGID as u32".into(),
            source: err,
        })?;

        Ok(Self {
            pid,
            comm,
            state,
            ppid,
            pgrp,
            procfs_path: format_compact!("/proc/{pid}"),
        })
    }

    #[inline]
    pub fn pid_exists(&self) -> bool {
        ::rustix::fs::stat(self.procfs_path.as_str()).is_ok()
    }

    #[inline]
    pub fn pid(&self) -> u32 {
        self.pid
    }

    #[inline]
    pub fn comm(&self) -> &str {
        &self.comm
    }

    #[inline]
    pub fn state(&self) -> u8 {
        self.state
    }

    #[inline]
    pub fn ppid(&self) -> u32 {
        self.ppid
    }

    #[inline]
    pub fn pgrp(&self) -> u32 {
        self.pgrp
    }

    #[instrument(level = Level::TRACE)]
    pub async fn poll_pid_until_exit(&self, period: Duration, timeout: Duration) -> bool {
        let mut ticker = ::tokio::time::interval(period);
        let timeout = ::tokio::time::sleep(timeout);
        ::tokio::pin!(timeout);

        loop {
            ::tokio::select! {
                biased;
                _now = ticker.tick() => {
                    if !self.pid_exists() {
                        return true;
                    }
                }
                () = &mut timeout => return false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use tracing::info;
    use tracing_test::traced_test;

    use super::Process;

    #[traced_test]
    #[test]
    fn psutil01() {
        let t_start = Instant::now();
        let p = Process::stat(1).expect("/proc/1/stat"); // ~ 30us on icy2, Linux 5.10
        let elapsed = t_start.elapsed();
        info!("{p:?} in {elapsed:?}");
    }

    #[traced_test]
    #[test]
    fn psutil02() {
        for pid in &[1, 0] {
            let t_start = Instant::now();
            let res = super::pid_exists(*pid); // < 6us on icy2, Linux 5.10
            let elapsed = t_start.elapsed();
            info!("{res:?} in {elapsed:?}");
        }
    }

    #[should_panic]
    #[traced_test]
    #[test]
    fn psutil03() {
        for pid in &[1, 0] {
            let proc = Process::stat(*pid).unwrap();
            let t_start = Instant::now();
            let res = proc.pid_exists(); // < 2us on icy2, Linux 5.10
            let elapsed = t_start.elapsed();
            info!("{res:?} in {elapsed:?}");
        }
    }
}
