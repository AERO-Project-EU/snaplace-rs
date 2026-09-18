use std::{
    io::{self, Write},
    path::PathBuf,
    time::Duration,
};

use fastant::Instant;

use quick_cginfo::{CgroupMonitor, Error};

fn usage() -> ! {
    eprintln!(
        "Usage: quick-cginfo --period-ms <T> [--cgroup <PATH>]\n\n\
         Example: quick-cginfo --period-ms 200 --cgroup /sys/fs/cgroup/faascell\n\n\
         Output units:\n\
           ts_unix_ns: nanoseconds since UNIX_EPOCH\n\
           memory_current_bytes: bytes\n\
           cpu%: CPU% * 100 (e.g., 12345 => 123.45% core-equivalent)\n\
           cpu%_user: user CPU% * 100 (core-equivalent)\n\
           cpu%_system: system CPU% * 100 (core-equivalent)\n\
           cpu%_throttled: throttled CPU% * 100 (core-equivalent)\n"
    );
    ::std::process::exit(2);
}

fn main() -> Result<(), Error> {
    let mut args = ::std::env::args().skip(1);

    let mut period_ms = None;
    let mut cgroup = PathBuf::from("/sys/fs/cgroup"); // FIXME: no `memory.current` for root

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-T" | "--period-ms" => {
                let v = args.next().unwrap_or_else(|| usage());
                period_ms = Some(v.parse().unwrap_or_else(|_| usage()));
            }
            "--cgroup" => {
                let v = args.next().unwrap_or_else(|| usage());
                cgroup = v.into();
            }
            "-h" | "--help" => usage(),
            _ => usage(),
        }
    }

    let period_ms = period_ms.unwrap_or_else(|| usage());
    let period = Duration::from_millis(period_ms);

    let mut mon = CgroupMonitor::new(&cgroup)?;

    // Buffered stdout
    let stdout = io::stdout();
    let mut out = io::BufWriter::new(stdout.lock());

    let mut stderr = io::stderr().lock();

    out.write_fmt(format_args!(
        "{: >20}{: >21}{: >12}{: >12}{: >12}{: >15}\n",
        "nanos_epoch", "memory_current_bytes", "cpu%", "cpu%_user", "cpu%_system", "cpu%_throttled"
    ))
    .unwrap();
    out.flush().ok();

    // Keep a stable cadence (sleep until next tick).
    let mut next = Instant::now();
    loop {
        next += period;
        let now = Instant::now();
        if next > now {
            ::std::thread::sleep(next - now);
        }

        let sample = match mon.sample() {
            Ok(sample) => sample,
            Err(err) => {
                let _ = writeln!(stderr, "Failed to sample cgroup: {err:#}");
                continue;
            }
        };

        writeln!(
            out,
            "{: >20}{: >21}{: >12}{: >12}{: >12}{: >15}",
            sample.ts_unix_ns,
            sample.mem_curr_bytes,
            sample.cpu_core_eq_pct_x100,
            sample.cpu_user_core_eq_pct_x100,
            sample.cpu_system_core_eq_pct_x100,
            sample.cpu_throttled_core_eq_pct_x100
        )
        .ok();
        out.flush().ok();
    }
}
