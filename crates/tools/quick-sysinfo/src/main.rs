use std::{
    io::Write,
    thread::sleep,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use minstant::Instant;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind};

fn main() -> Result<(), ::std::io::Error> {
    let bin_name = ::std::env::args().next().unwrap();
    let interval = ::std::env::args()
        .nth(1)
        .unwrap_or_else(|| panic!("usage:\t$ {bin_name} <INTERVAL(ms,>200)>"))
        .parse()
        .map(Duration::from_millis)
        .unwrap_or_else(|err| panic!("{err}\nusage:\t$ {bin_name} <INTERVAL(ms,>200)>"));

    let refresh_kind = RefreshKind::nothing()
        .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
        .with_memory(MemoryRefreshKind::nothing().with_ram());
    let mut sysinfo = ::sysinfo::System::new_with_specifics(refresh_kind);
    sleep(::sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
    sysinfo.refresh_specifics(refresh_kind);

    let mut stdout = ::std::io::stdout().lock();
    stdout
        .write_fmt(format_args!(
            "{: >20}\t{: >20}\t{: >7}\t{: >7}\t{: >7}\t{: >7}\t{: >13}\t{: >13}\t{: >13}\n",
            "nanos_epoch",
            "nanos_since_start",
            "cpu",
            "load1",
            "load5",
            "load15",
            "mem_used",
            "mem_free",
            "mem_avail",
        )) // pandas.read_csv("output.csv", sep="\\s+")
        .unwrap();

    let start_ts = Instant::now();
    ::ticker::Ticker::new(0.., interval)
        .into_iter()
        .try_for_each(|_| {
            sysinfo.refresh_specifics(refresh_kind);
            let load_avg = ::sysinfo::System::load_average();
            stdout.write_fmt(format_args!(
                "{: >20}\t{: >20}\t{: >.5}\t{: >.5}\t{: >.5}\t{: >.5}\t{: >13}\t{: >13}\t{: >13}\n",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("SystemTime cannot precede Epoch")
                    .as_nanos(),
                (Instant::now() - start_ts).as_nanos(),
                sysinfo.global_cpu_usage(),
                load_avg.one,
                load_avg.five,
                load_avg.fifteen,
                sysinfo.used_memory(),
                sysinfo.free_memory(),
                sysinfo.available_memory(),
            ))
        })
}
