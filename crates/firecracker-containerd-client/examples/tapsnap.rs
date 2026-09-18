//! First, make sure `10-snaplace.link` file has been installed.
//!
//! Then, create the tap interface as follows:
//!
//! ```text
//! # ip tuntap add uvm-tapsnap mode tap
//! # ip addr add 10.254.254.252/31 dev uvm-tapsnap
//! # ip link set dev uvm-tapsnap group "$((0xFAA5CE11))"
//! # ip link set dev uvm-tapsnap up
//! ```
//!
//! Snif packets moving from/to it using:
//!
//! ```text
//! # tcpdump -nnvvveS -i uvm-tapsnap
//! ```

use std::{fmt::Debug, net::Ipv4Addr, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use const_format::concatcp;
use fforget::fforget;
use once_cell::sync::Lazy;
use reqwest::IntoUrl;
use tokio::{
    io::AsyncWriteExt,
    time::{sleep_until, timeout, timeout_at, Instant},
};
use tracing::{debug, error, info, instrument, trace, warn};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use firecracker_containerd_client::{
    AfterSnapshotLoad, Client, FlushData, NetworkInterfaceBuilder, RuntimeSpecSource, Task, Vm,
    VmBuilder,
};

const CONTAINERD_NAMESPACE: &str = "default";
const CONTAINERD_ADDRESS: &str = "/run/firecracker-containerd/containerd.sock";
const CONTAINERD_TTRPC_ADDRESS: &str = concatcp!(CONTAINERD_ADDRESS, ".ttrpc");

const VM_IP_ADDRESS: Ipv4Addr = Ipv4Addr::new(10, 254, 254, 253);

const VMID: &str = "example-rs-tapsnap";
const NGINX_IMAGE_REF: &str = "docker.io/library/nginx:1.25.0";
const SNAPSHOT_KEY: &str = concatcp!(VMID, "-snap");
const NGINX_OCI_RUNTIME_SPEC_FILE_PATH: &str =
    "crates/firecracker-containerd-client/artifacts/spec_templates/tests/nginx:1.25.0.json";

const VM_SNAPSHOT_STATE_FILE: &str = "/tmp/snapshots/example-rs-tapsnap.state";
const VM_SNAPSHOT_MEMORY_FILE: &str = "/tmp/snapshots/example-rs-tapsnap.memory";

static NGINX_URL: Lazy<String> = Lazy::new(|| format!("http://{VM_IP_ADDRESS}:80"));

#[instrument]
async fn http_get(url: impl IntoUrl + Debug, retries: Option<(u32, Duration)>) -> Result<()> {
    let http = ::reqwest::Client::new();
    let t0 = Instant::now();

    let hresp = if retries.is_none() {
        match timeout(Duration::from_millis(500), http.get(url).send()).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(err)) => Err(err).with_context(|| "HTTP GET failure"),
            Err(timeout_err) => Err(timeout_err).with_context(|| "request timed out"),
        }
    } else {
        let (max_retries, tick) = retries.expect("!retries.is_none()");
        let mut attempts = 0;
        loop {
            let deadline = Instant::now() + tick;
            match timeout_at(deadline, http.get(NGINX_URL.as_str()).send()).await {
                Ok(Ok(resp)) => break Ok(resp),
                Ok(_http_err) => sleep_until(deadline).await,
                Err(_timeout_err) => (),
            };
            attempts += 1;
            match attempts {
                attempts if attempts == max_retries => {
                    break Err(anyhow!(
                        "failed to HTTP GET to the function inside the VM after {}",
                        ::humantime::format_duration(tick * max_retries)
                    ));
                }
                attempts if attempts % 10 == 0 => warn!(
                    "failing to HTTP GET to the function inside the VM after {}",
                    ::humantime::format_duration(tick * attempts)
                ),
                _ => (/* continue */),
            }
        }
    }
    .with_context(|| "failed HTTP GET to the function inside the VM")?;

    debug!(
        "HTTP GET duration: {}",
        ::humantime::format_duration(t0.elapsed())
    );
    debug!("workload responded with {}", hresp.status());
    trace!(
        "workload's response body:\n{}",
        hresp
            .text()
            .await
            .with_context(|| "failed to get the full HTTP response text")?
    );

    Ok(())
}

#[instrument(skip(c))]
async fn create_snapshot(c: &Client) -> Result<Vm> {
    let t0 = Instant::now();

    info!("Creating new VM...");
    let mut vm = VmBuilder::new(VMID)
        .network_interface(
            NetworkInterfaceBuilder::new("uvm-tapsnap", VM_IP_ADDRESS, 31, [10, 254, 254, 252])
                .guest_mac_addr([0xAA, 0xFC, 0x00, 0x00, 0x07, 0x01])
                .nameservers([[147, 102, 3, 1], [1, 1, 1, 1]])
                .build(),
        )
        .create(c, Duration::from_secs(2))
        .await
        .with_context(|| "failed to create a new VM")?;
    debug!("Returned: {vm:?}");

    info!("Preparing new (containerd-)snapshot...");
    let mounts = c
        .prepare_snapshot(SNAPSHOT_KEY, NGINX_IMAGE_REF)
        .await
        .with_context(|| "failed to prepare new container's snapshot")?;
    debug!("Returned: {mounts:?}");

    info!("Creating new container...");
    let container = vm
        .container_builder(NGINX_IMAGE_REF, SNAPSHOT_KEY)
        .spec(RuntimeSpecSource::File {
            path: NGINX_OCI_RUNTIME_SPEC_FILE_PATH,
            namespace: vm.namespace(),
            id: vm.id(),
            process_args: "/docker-entrypoint.sh nginx -g daemon off;",
            // NOTE: As long as `NGINX_OCI_RUNTIME_SPEC_FILE_PATH` does include `.process.args`,
            // the value provided in the `process_args` field does not make any difference
        })
        .create(c)
        .await
        .with_context(|| "failed to create container")?;
    debug!("Returned: {container:?}");

    info!("Creating new task...");
    let mut task = Task::create(c, container.id.clone(), mounts.as_slice())
        .await
        .with_context(|| "failed to create new task")?;
    debug!("Returned: {task:?}");

    info!("Starting the task...");
    task.start(c)
        .await
        .with_context(|| "failed to start task")?;

    let t1 = Instant::now();

    info!("Attempting to HTTP GET to the function inside the VM...");
    http_get(NGINX_URL.as_str(), Some((100, Duration::from_millis(100))))
        .await
        .with_context(|| "http_get failed")?;

    let t2 = Instant::now();

    info!("Pausing the VM...");
    vm.pause(c).await.with_context(|| "failed to pause VM")?;
    info!("Creating a VM snapshot...");
    vm.create_snapshot(
        c,
        VM_SNAPSHOT_STATE_FILE,
        VM_SNAPSHOT_MEMORY_FILE,
        FlushData::None,
    )
    .await
    .with_context(|| "failed to create snapshot")?;
    info!("Resuming the VM...");
    vm.resume(c).await.with_context(|| "failed to resume VM")?;

    let t3 = Instant::now();

    info!("Making sure the function in the VM is still functional after resuming it...");
    if let Err(err) = http_get(NGINX_URL.as_str(), None).await {
        error!("The function inside the VM is not functional after resuming it: {err}");
    }
    trace!("{vm:?}");
    //::tokio::time::sleep(Duration::from_secs(10)).await;

    let t4 = Instant::now();

    info!("Unloading the VM...");
    if let Err(err) = vm.unload(c).await {
        warn!("failure while unloading the VM: {err}");
    }

    let t5 = Instant::now();

    info!(
        "Timings:\n- Task creation: {}\n- First response: {} later\n- Snapshot creation: {}\n- UnloadVM: {}\n+ Total: {}",
        ::humantime::format_duration(t1 - t0),
        ::humantime::format_duration(t2 - t1),
        ::humantime::format_duration(t3 - t2),
        ::humantime::format_duration(t5 - t4),
        ::humantime::format_duration(t5 - t0),
    );
    Ok(vm)
}

#[instrument(skip(c))]
async fn load_snapshot(c: &Client, vm: &mut Vm) -> Result<()> {
    let t0 = Instant::now();

    trace!("{vm:?}");
    vm.load_from_snapshot(c, AfterSnapshotLoad::Resume)
        .await
        .with_context(|| "failed to load VM from snapshot")?;
    trace!("{vm:?}");

    http_get(NGINX_URL.as_str(), None)
        .await
        .with_context(|| "failed to HTTP GET")?;

    info!(
        "Function invocation completed in {}",
        ::humantime::format_duration(t0.elapsed())
    );

    info!("Unloading the VM...");
    if let Err(err) = vm.unload(c).await {
        warn!("failure while unloading the VM: {err}");
    }

    Ok(())
}

#[::tokio::main]
async fn main() -> Result<()> {
    ::tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive(
                "firecracker_containerd_client=trace"
                    .parse()
                    .with_context(|| "failed to parse filtering directive")?,
            ),
        )
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .try_init()
        .map_err(|err| anyhow!("failed to initialize tracing subscriber: {err}"))?;

    warn!("Testing 'tracing_subscriber::fmt().with_test_writer()'!");
    warn!(
        "cwd: {}",
        ::std::env::current_dir().expect("getcwd").display()
    );
    if !::tokio::fs::try_exists(NGINX_OCI_RUNTIME_SPEC_FILE_PATH)
        .await
        .context("failed to stat spec file")?
    {
        bail!("OCI Runtime Spec file '{NGINX_OCI_RUNTIME_SPEC_FILE_PATH}' does not exist");
    }

    info!("Instantiating new Client...");
    let c = Client::new(
        CONTAINERD_ADDRESS,
        CONTAINERD_TTRPC_ADDRESS,
        CONTAINERD_NAMESPACE,
    )
    .await
    .with_context(|| "failed to create Client")?;

    info!("Querying for containerd's version");
    let (version, revision) = c
        .version()
        .await
        .with_context(|| "failed to retrieve containerd's version information")?;
    info!("containerd {{ version: {version}, revision: {revision} }}");

    let mut vm = create_snapshot(&c).await?;
    fsync_snapshots(&vm)
        .await
        .context("failed to fsync new snapshots")?;

    for i in 0..10 {
        // FIXME(ckatsak): without sleeping (>100ms on icy1) firecracker is returned EBUSY when
        // attempting to open the tap device while restoring the snapshot!
        // (firecracker v1.1.0; host 6.1.25; guest 5.10)
        ::tokio::time::sleep(Duration::from_millis(100)).await;
        //::tokio::time::sleep(Duration::from_millis(500)).await;

        if i % 2 == 0 {
            // FIXME: Removing the *memory* file from the page cache using posix_fadvise64(2)
            // cripples the restored uVM's networking! The uVM replies to ARM & ICMP requests,
            // and completes the TCP handshake (so its kernel looks fine), but nginx -- our test
            // case userspace workload -- never replies to the HTTP requests, even after several
            // minutes. I guess removing the mapping of the memory file from the page cache
            // somehow ends up fucking up subsequent page faults to it when mmap'ed by the
            // firecracker process ... but only for userspace's memory (kernel works fine?)... WTF?
            ////::tokio::time::sleep(Duration::from_millis(500)).await;
            fforget(
                vm.snapshot_state_file()
                    .ok_or_else(|| anyhow!("Snapshot must have been created successfully"))?,
            )
            .with_context(|| "failed to fforget snapshot file")?;
            //fforget(
            //    vm.snapshot_memory_file()
            //        .ok_or_else(|| anyhow!("Snapshot must have been created successfully"))?,
            //)
            //.with_context(|| "failed to fforget memory file")?;
            ////::tokio::time::sleep(Duration::from_millis(500)).await;
            // NOTE: However, writing to sysfs's `drop_caches` does NOT lead to the same result (!)
            //fork_drop_caches().await.context("failed to drop caches")?;
        }
        //::tokio::time::sleep(Duration::from_millis(500)).await;

        load_snapshot(&c, &mut vm)
            .await
            .with_context(|| "failed to invoke function by loading VM from snapshot")?;
    }

    Ok(())
}

#[instrument]
async fn fsync_snapshots(vm: &Vm) -> Result<()> {
    let (Some(sf), Some(mf)) = (vm.snapshot_state_file(), vm.snapshot_memory_file()) else {
        bail!("Snapshot must have been created successfully");
    };

    let f = ::tokio::fs::OpenOptions::new()
        .read(true)
        .open(sf)
        .await
        .with_context(|| format!("failed to open state file {sf:?}"))?;
    f.sync_all()
        .await
        .with_context(|| format!("failed to fsync state file {sf:?}"))?;

    let f = ::tokio::fs::OpenOptions::new()
        .read(true)
        .open(mf)
        .await
        .with_context(|| format!("failed to open state file {mf:?}"))?;
    f.sync_all()
        .await
        .with_context(|| format!("failed to fsync state file {mf:?}"))?;

    Ok(())
}

#[allow(unused)]
async fn fork_drop_caches() -> Result<()> {
    const PROCFS_PATH: &str = "/proc/sys/vm/drop_caches";
    const DROP: u8 = 1;

    ::tokio::fs::OpenOptions::new()
        .write(true)
        .open(PROCFS_PATH)
        .await
        .with_context(|| format!("failed to open {PROCFS_PATH:?}"))?
        .write_u8(DROP)
        .await
        .with_context(|| format!("failed to write '{DROP}' to {PROCFS_PATH:?}"))
}
