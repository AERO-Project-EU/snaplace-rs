use std::{
    collections::HashMap,
    fmt::Debug,
    path::Path,
    sync::{LazyLock, RwLock},
    time::Duration,
};

use backon::{FibonacciBuilder, Retryable};
use compact_str::{format_compact, CompactString};
use enum_map::EnumMap;
use rustix::{
    io_uring::Signal,
    process::{kill_process_group, pidfd_open, Pid, PidfdFlags},
};
use scopeguard::defer;
use tokio::{
    fs,
    io::{unix::AsyncFd, Interest},
    process::Command,
    time::Instant,
};
use tracing::{debug, error, instrument, trace, warn, Level};

use firecracker_containerd_client::{
    AfterSnapshotLoad, Client, Error as fcctrdError, FlushData, RuntimeSpecSource, Task, VmBuilder,
};

#[cfg(feature = "sched-setaffinity")]
use crate::sbpool::Cpu;
use crate::{
    metadata::{registration, FunctionInfo},
    metrics::{Nanoseconds, Timing},
    network::{NetworkManagerRef, Tap},
    utils::psutils::{self, Process},
    worker::runtime::{
        fcctrd::{
            error::Error,
            sandbox::{MicroVm, MicroVmState},
            FcctrdFunctionInfo, FirecrackerContainerdConfig,
        },
        DestroySandboxRuntimeError, Runtime, Sandbox,
    },
    FunctionId,
};

const FC_COMM: &str = "firecracker";
const FCCTRD_SHIM_COMM: &str = "containerd-shim";

static REGISTERED_IMAGES: LazyLock<RwLock<HashMap<String, usize, crate::BuildHasher>>> =
    LazyLock::new(Default::default);

#[derive(Clone)]
pub struct FirecrackerContainerd {
    config: FirecrackerContainerdConfig,
    client: Option<Client>,
    fi: FcctrdFunctionInfo,
    /// This field stores the key of the (top-level, writable containerd-)snapshot of the sandbox.
    ///
    /// In cases where the runtime is constructed for nonexistent [`Vm`]s (i.e., the Worker started
    /// fresh and is about to create a new sandbox), there is no VMID at the time of the
    /// construction (when [`FirecrackerContainerd::new`] is called), so this field remains `None`,
    /// and is populated later, during [`FirecrackerContainerd::create_sandbox`].
    ///
    /// In cases where the runtime is constructed for already existing [`Vm`]s (i.e., snapshotted
    /// ones), this field can be populated by [`FirecrackerContainerd::new`], since its provided
    /// `sandbox` argument will point to a [`Sandbox`] with a valid VMID.
    snapshot: Option<CompactString>,
}

impl FirecrackerContainerd {
    const CLIENT_TIMEOUT: Duration = Duration::from_millis(7500);

    async fn destroy_microvm(
        &mut self,
        mut uvm: MicroVm,
    ) -> Result<Tap, DestroySandboxRuntimeError<MicroVm>> {
        match (uvm.has_snapshot(), uvm.vm.pid().is_some()) {
            (true, false) => {
                // "Unloaded, snapshot-only" case; from `SandboxState::Snapshot`.
                //
                // uVM is snapshotted; hence, it may or may not have been unloaded in the past.
                // Therefore, we cannot rely on firecracker-containerd's communication with its
                // in-VM agent through the vsock.
                self.destroy_snapshotted_microvm(uvm).await
            }
            (false, true) => {
                // "Loaded, non-snapshotted uVM" case, from `SandboxState::Paused`.
                //
                // uVM is not snapshotted; hence it has definitely not been unloaded in the past.
                // Therefore, we should be able to rely on firecracker-containerd's communication
                // with its in-VM agent through the vsock. Nevertheless, it is important to make
                // sure that no resources are wasted for idle, non-responsive uVM remnants, so we
                // may have to forcefully terminate them if needed.
                self.destroy_idle_microvm(uvm).await
            }
            (true, true) => {
                // "Paused, snapshotted uVM" case, from `SandboxState::Paused`.
                //
                // This should happen for idle/live Workers against a paused snapshotted sandbox.
                if let Err(err) = self.shutdown_sandbox(&mut uvm).await {
                    return Err(DestroySandboxRuntimeError { sandbox: uvm, err });
                }
                self.destroy_snapshotted_microvm(uvm).await
            }
            (false, false) => Err(DestroySandboxRuntimeError {
                sandbox: uvm,
                err: Box::new(Error::DestroyVm(None)),
            }),
        }
    }

    /// TODO: doc
    ///
    /// # Invariants
    ///
    /// - The runtime has been initialized (i.e., `self.client.is_some()`).
    /// - The provided [`MicroVm`]'s underlying [`Vm`] does not have its snapshot file path fields
    /// populated. This is assumed to have been already checked; e.g., `uvm.has_snapshot()` returns
    /// `false`.
    ///
    /// # Errors
    ///
    /// TODO
    ///
    /// # Panics
    ///
    /// - If any of the invariants described above does not hold.
    /// - On improperly handled (TODO) errors...
    ///
    /// [`Vm`]: firecracker_containerd_client::Vm
    #[instrument(level = Level::DEBUG, skip_all)]
    async fn destroy_idle_microvm(
        &mut self,
        uvm: MicroVm,
    ) -> Result<Tap, DestroySandboxRuntimeError<MicroVm>> {
        let client = self.client.as_ref().expect("Runtime is initialized");
        debug_assert!(!uvm.has_snapshot());

        let pid = uvm
            .vm
            .pid()
            .expect("non-snapshotted uVMs should always be associated with a PID");
        let fc_proc = match ::tokio::task::spawn_blocking(move || Process::stat(pid as _)).await {
            Ok(Ok(proc)) => proc,
            Ok(Err(err)) => {
                return Err(DestroySandboxRuntimeError {
                    sandbox: uvm,
                    err: Box::new(err),
                })
            }
            Err(err) => {
                return Err(DestroySandboxRuntimeError {
                    sandbox: uvm,
                    err: Box::new(err),
                })
            }
        };
        assert_eq!(
            fc_proc.comm(),
            FC_COMM,
            "uvm.vm.pid is expected to refer to a firecracker process"
        );
        let ppid = fc_proc.ppid();

        // If a PID does exist, it should mean that firecracker is up and running, though probably
        // paused? Attempt to resume it anwyay. FIXME?
        if let Err(err) = uvm.vm.resume(client).await {
            // TODO: dec timeout ^^^^^^^^^^^^^^ ? normally < 1ms
            warn!(error = ?err, ?uvm.vm, "Failed to resume uVM: {err:#}");
        }

        // If the sandbox has been created and started, and its VM has not been "unloaded" in
        // the past, then firecracker-containerd should be able to communicate with its in-VM
        // agent, hence also able to gracefully delete the task after killing its processes.
        let _task_killed = client
            .kill_task(uvm.vm.id(), Signal::KILL.as_raw() as _, true)
            .await
            .map_err(|err| {
                warn!(error = ?err, "Failed to kill -TERM all Tasks inside the uVM: {err:#}");
                err
            });
        // Since `exit_after_all_tasks_deleted` has been set during VM creation, containerd should
        // handle VM's teardown and clean-up for us, as long as `client.kill_task()` succeeds.
        // NOTE: I think this does not inlude the Container (hence nor its snapshot).

        // Waiting for the Task may block forever, so just DeleteTask instead.
        // ¿NOTE: Looks like we don't need to delay DeleteTask after KillTask
        let task_deleted = match client.delete_task(uvm.vm.id()).await {
            Ok(st) => {
                trace!(exit_status = ?st, "Successfully deleted the Task in the uVM");
                Ok(())
            }
            Err(err) => {
                warn!(error = ?err, "Failed to delete the Task inside the uVM: {err:#}");
                Err(err)
            }
        };

        // Delete the Container, thus triggering firecracker-containerd's garbage collection
        if let Err(err) = client.delete_container(uvm.vm.id()).await {
            error!(error = ?err, "Failed to delete Container: {err:#}");
            return Err(DestroySandboxRuntimeError {
                sandbox: uvm,
                err: Box::new(Error::DeleteContainer(err)),
            });
        }

        // Attempt to synchronously delete container's (containerd-)snapshot (don't consider it
        // an error if it has already been garbage-collected), to avoid conflicts on subsequent
        // VM creations:
        self.remove_container_snapshot().await;

        // FIXME/TODO(ckatsak): Looks like, no matter what we do here, firecracker-containerd
        // needs 5sec to actually shut down and clean up the shim. We may not return earlier
        // than that, because it would signal our caller that the resources (e.g., the TAP
        // device) are available for reuse, whereas they're not. Therefore, we timeout after 6sec:
        // let's wait 4.5sec in StopVM, and then poll every 100ms until shim's PID has exited.

        // If the Task was successfully deleted earlier, attempt to gracefully stop the uVM,
        // though with a short timeout (we cannot afford piling up Workers who slowly shut down).
        if task_deleted.is_ok() {
            // NOTE:
            // - This might actually not be necessary as long as we set the
            // `exit_after_all_tasks_deleted` flag on VM creation. But do it anyway?
            // - In fact, it may be needed only when DeleteTask fails but KillTask succeeds?
            let _vm_stopped = client
                .stop_vm(uvm.vm.id(), Duration::from_millis(4500))
                .await
                .map_err(|err| {
                    warn!(error = ?err, "Failed to StopVM: {err:#}");
                });
        } else {
            // If Task deletion failed and the VM process is still around, just kill -TERM it...
            if fc_proc.pid_exists() {
                match ::rustix::process::kill_process(
                    Pid::from_raw(pid as _).expect("PID should be valid by now"),
                    Signal::TERM,
                ) {
                    Ok(()) => trace!(?pid, "Just killed -TERM the firecracker process"),
                    Err(err) => {
                        error!(error = ?err, ?pid, "Failed to kill -TERM the firecracker process")
                    }
                };
            }
        }
        // ...and poll to find out whether the shim picked it up and gracefully cleaned up before
        // exiting.
        if psutils::poll_pid_until_exit(
            fc_proc.ppid(), // shim
            Duration::from_millis(100),
            Duration::from_millis(1500), // TODO: longer? this is our best shot for a clean exit...
        )
        .await
        {
            return Ok(Tap::Device(uvm.tap));
        }

        warn!("So far failing to \"kinda gracefully\" shut down the VM and its shim...");

        // NOTE: If the next step fails as well, don't return the Tap at all. But what if it
        // partially fails (e.g., the PID does not exist, but the filesystem state has not been
        // cleaned up)? I guess we cannot avoid error on subsequent VM creation in that case..?

        // We failed to gracefully clean up the uVM, so we now attempt to kill -TERM its whole
        // process group (thus, including its parent process, the shim) as a final hopeless
        // means to free up some resources. I think containerd cleans up shim's bundle anyway
        // (thus errors on subsequent VM creation should not persist)?
        // But first, give it some time (?) FIXME
        match ::tokio::task::spawn_blocking(move || Process::stat(ppid as _)).await {
            // NOTE: There might be a race here. Not finding the PID under /proc/ causes no
            // harm. OTOH, finding it while it refers to a different process than we think,
            // can be utterly destructive, even more so considering that we kill its whole
            // process group. As a half-measure, also match on `/proc/<PID>/comm`.
            Ok(Ok(shim)) if shim.comm() == FCCTRD_SHIM_COMM => match kill_process_group(
                Pid::from_raw(shim.pgrp() as _).expect("PGID should be valid by now"),
                Signal::TERM,
            ) {
                Ok(()) => {
                    warn!(proc.stat = ?shim, microvm = ?uvm, "Just killed -TERM the process group");
                    // FIXME Since we _had_ to kill -TERM the whole process group just now, we
                    // cannot be sure that the clean-up has occurred so soon, right? Should we
                    // just wait 5sec more? I guess this is not a major problem as long as the
                    // number of Workers shutting down their VMs is unbound anyway...
                    ::tokio::time::sleep(Duration::from_secs(5)).await;
                    return Ok(Tap::Device(uvm.tap));
                }
                Err(err) => error!(
                    error = ?err,
                    proc.stat = ?shim,
                    microvm = ?uvm,
                    "Failed to kill -TERM process group of indestructable uVM: {err:#}",
                ),
            },
            Ok(Ok(p)) => warn!(proc.stat = ?p, "not a firecracker process"),
            Ok(Err(err)) => {
                warn!(error = ?err, "Failed to look up '/proc/{ppid}/stat': {err:#}");
                // FIXME Since we _had_ to kill -TERM the whole process group just now, we
                // cannot be sure that the clean-up has occurred so soon, right? Should we
                // just wait 5sec more? I guess this is not a major problem as long as the
                // number of Workers shutting down their VMs is unbound anyway...
                ::tokio::time::sleep(Duration::from_secs(5)).await;
                return Ok(Tap::Device(uvm.tap));
            }
            Err(err) => error!(error = ?err, "Failed to join blocking task: {err:#}"),
        };

        // I guess we should we return Err here, to refrain from indicating to the orchestrator
        // that the TAP is ready for reuse?
        Err(DestroySandboxRuntimeError {
            sandbox: uvm,
            err: Box::new(Error::DestroyVm(None)),
        })
    }

    /// TODO: doc
    ///
    /// # Invariants
    ///
    /// - The runtime has been initialized (i.e., `self.client.is_some()`).
    /// - The provided [`MicroVm`]'s underlying [`Vm`] has its snapshot file path fields populated.
    /// This is assumed to have been already checked; e.g., `uvm.has_snapshot()` returns `true`.
    ///
    /// # Errors
    ///
    /// TODO
    ///
    /// # Panics
    ///
    /// If any of the invariants described above does not hold.
    ///
    /// [`Vm`]: firecracker_containerd_client::Vm
    #[instrument(level = Level::DEBUG, skip_all)]
    async fn destroy_snapshotted_microvm(
        &mut self,
        uvm: MicroVm,
    ) -> Result<Tap, DestroySandboxRuntimeError<MicroVm>> {
        let client = self.client.as_ref().expect("Runtime is initialized");
        debug_assert!(uvm.has_snapshot());
        debug_assert!(uvm.vm.pid().is_none());

        // Delete the Container, thus triggering firecracker-containerd's garbage collection
        if let Err(err) = client.delete_container(uvm.vm.id()).await {
            return Err(DestroySandboxRuntimeError {
                sandbox: uvm,
                err: Box::new(Error::DeleteContainer(err)),
            });
        }

        // ¿FIXME  ^^  I guess this won't work in abnormal cases; e.g., if the VM has not been
        // paused/unloaded normally: we shouldn't be able to `Delete` Containers with Tasks whose
        // Status is not "Stopped" (they'd need to be `Kill`ed first, and then probably `Delete`d
        // as well). Overall, in abnormal cases we could try harder than the above.

        // Attempt to synchronously delete container's (containerd-)snapshot (don't consider it
        // an error if it has already been garbage-collected), to avoid conflicts on subsequent
        // VM creations:
        self.remove_container_snapshot().await;

        // unlink(2) snapshot files, on a best-effort basis

        // SAFETY: It is assumed that the caller has already checked that these fields are populated
        let sf = uvm.vm.snapshot_state_file().unwrap();
        let mf = uvm.vm.snapshot_memory_file().unwrap();

        match ::tokio::join!(fs::remove_file(sf), fs::remove_file(mf)) {
            (Ok(()), Ok(())) => {}
            (Err(err), Ok(())) => {
                error!(
                    error = ?err,
                    "Failed to unlink(2) snapshot state file '{}': {err:#}",
                    sf.display(),
                );
            }
            (Ok(()), Err(err)) => {
                error!(
                    error = ?err,
                    "Failed to unlink(2) snapshot memory file '{}': {err:#}",
                    mf.display(),
                );
            }
            (Err(err_sf), Err(err_mf)) => error!(
                error_state = ?err_sf,
                error_memory = ?err_mf,
                "Failed to unlink(2) both snapshot files '{}' and '{}': {err_sf:#}; {err_mf:#}",
                sf.display(),
                mf.display(),
            ),
        }

        return Ok(Tap::Device(uvm.tap));
    }

    /// Attempt to synchronously delete container's (containerd-)snapshot (don't consider it
    /// an error if it has already been garbage-collected), to avoid conflicts on subsequent
    /// VM creations:
    #[instrument(level = Level::TRACE, skip_all)]
    async fn remove_container_snapshot(&self) {
        let client = self.client.as_ref().expect("Runtime is initialized");

        match client
            .remove_container_snapshot(
                self.snapshot
                    .as_ref()
                    .expect(".snapshot should have been populated by now")
                    .as_str(),
                Some(self.config.snapshotter.as_str()),
            )
            .await
        {
            Ok(()) => {}
            Err(fcctrdError::TonicStatus(ref status))
                if matches!(status.code(), ::tonic::Code::NotFound) =>
            {
                debug!(
                    grpc_status = ?status,
                    "Container's snapshot has probably already been garbage-collected: {}",
                    status.message(),
                );
            }
            Err(err) => error!(error = ?err, "Failed to remove container's snapshot: {err:#}"),
        };
    }
}

impl Runtime for FirecrackerContainerd {
    type Config = FirecrackerContainerdConfig;
    type Sandbox = MicroVm;
    type NetResource = Tap;
    type FunctionInfo = FcctrdFunctionInfo;

    const NAME: &'static str = "fcctrd";

    #[inline]
    fn new(
        config: &Self::Config,
        function_info: &Self::FunctionInfo,
        sandbox: Option<&Self::Sandbox>,
    ) -> Result<Self, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        Ok(Self {
            config: config.clone(),
            client: None,
            fi: function_info.clone(),
            snapshot: sandbox.map(|sb| format_compact!("{}-snap", sb.vm.id())),
        })
    }

    #[instrument(level = Level::TRACE, skip_all)]
    async fn init(&mut self) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        self.client = Some(
            Client::with_timeout(
                &self.config.address,
                &self.config.ttrpc_address,
                &self.config.namespace,
                Self::CLIENT_TIMEOUT,
            )
            .await?,
        );
        Ok(())
    }

    // TODO:
    // - Don't leak the tap on failure (?)
    // - Define some error type as an associated type rather than boxing it? Though this would
    // probably be pointless as long as Worker is generic over Runtime. So maybe just use `anyhow`?
    #[instrument(level = Level::DEBUG, skip_all)]
    async fn create_sandbox(
        &mut self,
        function_id: &FunctionId,
        tap_resource: Self::NetResource,
        timings: &mut EnumMap<Timing, Nanoseconds>,
    ) -> Result<Self::Sandbox, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        // Create & configure the tap device
        let tap = match tap_resource {
            Tap::Device(tap) => tap,
            Tap::Builder(tap_builder) => {
                let setup_net_start = Instant::now();
                let tap = tap_builder.build().await.map_err(Box::new)?;
                timings[Timing::SetupResources] +=
                    setup_net_start.elapsed().as_nanos() as Nanoseconds;
                tap
            }
        };

        // Create the firecracker-containerd uVM
        let create_sandbox_start = Instant::now();
        defer! {
            timings[Timing::CreateSandbox] = create_sandbox_start.elapsed().as_nanos() as _;
        }

        let client = self.client.as_ref().expect("Runtime is initialized");
        let vmid = tap.name();

        trace!("Creating new VM...");
        let mut vm = VmBuilder::new(vmid)
            .memory_mib((self.fi.memory.as_u64() >> 20) as _) // first convert to MiB
            .network_interface(
                tap.to_firecracker_if(self.config.nameservers.iter().take(2).copied()),
            )
            // These two are set by default, but we rely on them for shutting down, so emphasize:
            .exit_after_all_tasks_deleted(true)
            .container_count(1)
            .create(client, Self::CLIENT_TIMEOUT) // FIXME: timeout?
            .await
            .map_err(Error::CreateVm)?;
        trace!(?vm, "Created new VM");

        assert!(self.snapshot.is_none(), "snapshot should not exist yet"); // check field's doc
        self.snapshot = Some(format_compact!("{vmid}-snap"));

        trace!("Creating new container...");
        let container_create = vm
            .container_builder(&self.fi.image_ref, self.snapshot.as_ref().unwrap().as_str())
            .spec(RuntimeSpecSource::Template {
                process_args: self
                    .fi
                    .process_args
                    .as_ref()
                    .expect("FunctionInfo should be initialized by now"),
                // ¿FIXME(ckatsak) ^ Use a separate post-registration type instead of FunctionInfo?
                // TODO(ckatsak): Maybe serialize & store Spec in FunctionInfo to further reduce
                // overhead on the critical path? Though CreateVM() probably dominates anyway..
                // XXX
                namespace: vm.namespace(),
                id: vm.id(),
            } as RuntimeSpecSource<'_, '_, '_, &str>)
            .create(client);
        let container = match container_create.await {
            Ok(container) => {
                trace!(
                    "Container {{ id: {}, image: {}, runtime: {:?}, snapshotter: {}, \
                     snapshot_key: {} }}",
                    container.id,
                    container.image,
                    container.runtime,
                    container.snapshotter,
                    container.snapshot_key,
                );
                container
            }
            Err(err) => {
                if let Err(err) = vm.stop(client, Self::CLIENT_TIMEOUT).await {
                    error!(error = ?err, "Failed to stop VM after failing to create Container");
                }
                return Err(Error::CreateContainer(err).into());
            }
        };

        trace!("Preparing new containerd snapshot...");
        // We prepare the snapshots right before creating the Task because otherwise (i.e., if done
        // earlier, e.g., before creating the VM) Task::create() fails reporting that block device
        // `/dev/mapper/fc-dev-thinpool-snap-*` cannot be found. I'm wondering whether that is
        // containerd's garbage collector's in action, and whether a Lease would help in this case.
        // (So, I guess this would actually make any code that doesn't use Leases racy here?) TODO?
        let mounts = match client
            .prepare_snapshot(self.snapshot.as_ref().unwrap().as_str(), &self.fi.image_ref)
            // TODO(ckatsak):  ^^  This calculates parent snapshot's digest right on the critical
            // path for every single request. Populating `FunctionInfo.parent_snapshot` beforehand
            // can avoid this. See `FunctionInfo.process_args` for more on this.
            // Therefore, replace with `Client::prepare_with_snapshotter` when this ^^ is resolved
            .await
        {
            Ok(mounts) => {
                trace!(?mounts);
                mounts
            }
            Err(err) => {
                if let Err(err) = client.delete_container(&container.id).await {
                    error!(error = ?err, "Failed to delete Container after failing to create Task");
                }
                if let Err(err) = vm.stop(client, Self::CLIENT_TIMEOUT).await {
                    error!(error = ?err, "Failed to stop VM after failing to create Task");
                }
                return Err(Error::PrepareContainerSnapshot(err).into());
            }
        };

        trace!("Creating new task...");
        // Prior firecracker-containerd's failure to correctly clean up its shim (including
        // shim's dir) leads to the following error:
        //   TonicStatus(Status {
        //     code: Unknown,
        //     message: "failed to start shim: mkdir /run/firecracker-containerd/io.containerd.runtime.v2.task/default/uvm-0a-XX-XX-XX: file exists",
        //     metadata: MetadataMap { headers: {"content-type": "application/grpc"} },
        //     source: None,
        //   })
        // Retries won't help, as nobody's trying to clean it up anymore.
        // Failure to clean up the shim probably originates in containerd v1.6.8; call stack:
        //   - runtime/v2/task/shim.pb.go:4291
        //   - runtime/v2/task/shim.pb.go:187
        //   - runtime/v2/task/shim.pb.go:146
        // but I have no clue on why it occurs only occasionally, nor how to fix it... FIXME
        // I guess using random UUIDs (rather than TAP names) as VM/container names would
        // "hide" the problem even better (due to less naming conflicts), but let's just
        // leave it like this for now, to highlight the problem.
        // TODO: The above should probably live in an issue rather than code comment.
        //let mut attempt = 0;
        //let mut task = (|| Task::create(client, container.id.clone(), mounts.as_slice()))
        //    .retry(
        //        &::backon::ConstantBuilder::default()
        //            .with_delay(Duration::from_millis(10))
        //            .with_max_times(5)
        //            .with_jitter(), // 50ms < total_delay < 100ms
        //    )
        //    .notify(|err, next_delay| {
        //        debug!(attempt, ?next_delay, error = ?err);
        //        attempt += 1;
        //    })
        //    .await
        //    .map_err(Error::CreateTask)?;
        let mut task = match Task::create(client, container.id.clone(), mounts.as_slice()).await {
            Ok(task) => {
                trace!(?task, "Created");
                task
            }
            Err(err) => {
                if let Err(err) = client.delete_container(&container.id).await {
                    error!(error = ?err, "Failed to delete Container after failing to create Task");
                }
                // I guess the Active snapshot should be garbage-collected anytime now?
                if let Err(err) = vm.stop(client, Self::CLIENT_TIMEOUT).await {
                    error!(error = ?err, "Failed to stop VM after failing to create Task");
                }
                return Err(Error::CreateTask(err).into());
            }
        };

        trace!("Starting the task...");
        task.start(client).await.map_err(Error::StartTask)?; // FIXME: error handling?
        trace!("Task started!");

        Ok(MicroVm::new(function_id, tap, vm))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    #[inline]
    async fn load_sandbox(
        &mut self,
        uvm: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let client = self.client.as_ref().expect("Runtime is initialized");
        uvm.vm
            .load_from_snapshot(client, AfterSnapshotLoad::Resume)
            .await
            .map_err(|err| Error::LoadVmSnapshot(err).into())
    }

    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    async fn pause_sandbox(
        &mut self,
        uvm: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let client = self.client.as_ref().expect("Runtime is initialized");

        let mut attempt = 0;
        (|| uvm.vm.pause(client))
            .retry(
                FibonacciBuilder::default()
                    .with_min_delay(Duration::from_millis(50))
                    .with_jitter()
                    .with_max_times(7), // now, 50ms, 100ms, 150ms, 250ms, 400ms, 650ms; +jitter <50ms
            )
            .when(|e| format!("{e:?}").contains("context deadline exceeded"))
            .notify(|err, next_retry_in| {
                trace!(attempt, error = ?err, ?next_retry_in);
                attempt += 1;
            })
            .await
            .map_err(|err| Error::PauseVm(err).into())
    }

    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    async fn resume_sandbox(
        &mut self,
        uvm: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let client = self.client.as_ref().expect("Runtime is initialized");
        uvm.vm
            .resume(client)
            .await
            .map_err(|err| Error::ResumeVm(err).into())
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    #[inline]
    async fn create_snapshot(
        &mut self,
        uvm: &mut Self::Sandbox,
        state_file_path: impl AsRef<Path> + Send + Sync + Debug,
        memory_file_path: impl AsRef<Path> + Send + Sync + Debug,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let client = self.client.as_ref().expect("Runtime is initialized");
        uvm.vm
            .create_snapshot(client, state_file_path, memory_file_path, FlushData::Fsync)
            .await
            .map_err(|err| Error::CreateVmSnapshot(err).into())
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    #[inline]
    async fn shutdown_sandbox(
        &mut self,
        uvm: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let client = self.client.as_ref().expect("Runtime is initialized");

        // If we do have a non-zero PID for the uVM, open an async-readable pidfd to reliably
        // detect its associated process' termination before returning.
        let pidfd = uvm
            .vm
            .pid()
            .and_then(|pid| Pid::from_raw(pid as _))
            .and_then(|pid| {
                pidfd_open(pid, PidfdFlags::NONBLOCK)
                    .inspect_err(
                        |err| error!(error = ?err, "Failed to pidfd_open(2) {pid}: {err:#}"),
                    )
                    .ok()
            })
            .and_then(|pidfd| {
                AsyncFd::with_interest(pidfd, Interest::READABLE)
                    .inspect_err(|err|
                        error!(error = ?err, "Failed to register READABLE interest for pidfd: {err:#}")
                    )
                    .ok()
            });

        // Sometimes, `containerd-shim-aws-firecracker` (i.e., `firecracker-containerd`'s runtime
        // shim) can return a "ttrpc: closed" error. This is not necessarily bad, but I have yet
        // to look into whether it is _always_ benign (TODO).
        // XXX Let's filter it out for now:
        match uvm.vm.unload(client).await {
            Ok(()) => {}
            Err(ref err @ fcctrdError::Ttrpc(::ttrpc::Error::RpcStatus(ref status)))
                if status.message() == "ttrpc: closed" =>
            {
                warn!(error = ?err, "Shim returned a (probably) harmless error on Vm::unload");
                uvm.vm.clear_pid();
            }
            Err(err) => return Err(Error::UnloadVm(err).into()),
        }

        // If we did have the (non-zero) PID of the uVM, await its termination before returning
        if let Some(pidfd) = pidfd {
            match pidfd.readable().await {
                Ok(mut guard) => {
                    trace!("uvm process died");
                    guard.retain_ready();
                }
                Err(err) => error!(error = ?err, "Failed to await readable pidfd: {err:#}"),
            }
        }

        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn destroy_sandbox(
        &mut self,
        uvm: Self::Sandbox,
    ) -> Result<Self::NetResource, DestroySandboxRuntimeError<Self::Sandbox>> {
        self.destroy_microvm(uvm).await
    }

    /// Remove [`MicroVm`]'s snapshot files from the page cache using `fadvise(2)`.
    #[cfg(feature = "uncache")]
    #[instrument(level = Level::WARN, skip_all, fields(uvm = uvm.vm.id()))]
    async fn uncache_sandbox(
        &mut self,
        uvm: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        use std::{fs::File, io::Error as ioError, num::NonZeroU64, path::PathBuf};

        use rustix::fs::{fadvise, stat, Advice};
        use tokio::task::JoinSet;

        #[derive(Debug, ::thiserror::Error)]
        enum FileError {
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
        /// fforget
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

        let mut join_set = JoinSet::new();

        if let Some(path) = uvm.vm.snapshot_state_file() {
            let _abort_handle = join_set.spawn_blocking({
                let path = path.to_owned();
                move || fforget_file(path)
            });
        }
        if let Some(path) = uvm.vm.snapshot_memory_file() {
            let _abort_handle = join_set.spawn_blocking({
                let path = path.to_owned();
                move || fforget_file(path)
            });
        }

        while let Some(join_res) = join_set.join_next().await {
            match join_res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => warn!(error = ?err, "Error while uncaching snapshot file: {err:#}"),
                Err(join_err) => warn!(
                    error = ?join_err,
                    "Failed to join snapshot file uncaching task on blocking thread: {join_err:#}"
                ),
            }
        }

        Ok(())
    }

    /// FIXME: For now, this method always succeeds, regardless of whether it did actually
    /// enforce the provided resource constraints.
    #[cfg(feature = "sched-setaffinity")]
    #[inline]
    #[instrument(level = Level::TRACE, skip_all, fields(uvm = uvm.vm.id()))]
    async fn setup_cpuset(
        &mut self,
        uvm: &mut Self::Sandbox,
        cpuset: Cpu,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        use rustix::{
            process::Pid,
            thread::{sched_setaffinity, CpuSet},
        };
        use tracing::error;

        if let Some(pid) = uvm.vm.pid() {
            let pid = Pid::from_raw(pid as _).unwrap();

            let mut actual_cpuset = CpuSet::new();
            actual_cpuset.set(cpuset.as_u16() as _);

            if let Err(err) = sched_setaffinity(Some(pid), &actual_cpuset) {
                error!(
                    error = ?err,
                    ?cpuset,
                    "Failed to pin uvm to CPU: sched_setaffinity: {err:#}",
                );
            }
        } else {
            // Should we reach this point, there must be some serious bug either in snaplace
            // or in our firecracker-containerd fork.
            error!(?uvm, "uvm's PID is unset!");
        }

        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(function.id = %fi.id()))]
    async fn register_function(
        config: &Self::Config,
        fi: &mut Self::FunctionInfo,
    ) -> Result<(), registration::Error> {
        let mut rt = Self::new(config, fi, None).map_err(|err| registration::Error::Runtime {
            msg: "failed to instantiate FirecrackerContainerd runtime".into(),
            err: Some(err),
        })?;
        Self::init(&mut rt)
            .await
            .map_err(|err| registration::Error::Runtime {
                msg: "failed to initialize FirecrackerContainerd runtime".into(),
                err: Some(err),
            })?;

        if 0 == *REGISTERED_IMAGES
            .read()
            .expect("read lock")
            .get(&fi.image_ref)
            .unwrap_or(&0)
        {
            Self::ensure_image(config, &fi.image_ref).await?;
        }
        *REGISTERED_IMAGES
            .write()
            .expect("write lock")
            .entry(fi.image_ref.clone())
            .or_insert(0) += 1;

        if fi.parent_snapshot.is_none() || fi.process_args.is_none() {
            rt.finalize_function_info(fi).await?;
        }

        Ok(())
    }

    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    async fn deregister_function(
        _: &Self::Config,
        fi: &Self::FunctionInfo,
    ) -> Result<(), registration::Error> {
        if let Some(cnt) = REGISTERED_IMAGES
            .write()
            .expect("write lock")
            .get_mut(&fi.image_ref)
        {
            *cnt = cnt.saturating_sub(1);
        } else {
            error!(function.info = ?fi, "No registered image found");
        }
        // TODO(ckatsak): For now, we don't actually remove the images from the underlying
        // firecracker-containerd. In the future, I guess we should do that when count zeroes.

        Ok(())
    }

    async fn reinstate_sandbox(
        &mut self,
        function_id: FunctionId,
        state: MicroVmState,
        netman: NetworkManagerRef<Self::NetResource>,
    ) -> Option<Result<Self::Sandbox, Box<dyn ::std::error::Error + Send + Sync + 'static>>> {
        let tap = match netman.request(state.tap.clone()).await {
            Ok(rx) => match rx.await {
                Ok(Ok(Tap::Device(tap))) => tap,
                Ok(Ok(Tap::Builder(tap_builder))) => match tap_builder.build_or_adopt().await {
                    Ok(tap) => tap,
                    Err(err) => {
                        return Some(Err(Box::new(Error::ReinstateSandbox {
                            msg: format!("failed to build or adopt requested {:?}", state.tap)
                                .into_boxed_str(),
                            err: Some(Box::new(err)),
                        })))
                    }
                },
                Ok(Err(err)) => {
                    return Some(Err(Box::new(Error::ReinstateSandbox {
                        msg: format!("failed requested allocation for {:?}", state.tap)
                            .into_boxed_str(),
                        err: Some(Box::new(err)),
                    })))
                }
                Err(err) => {
                    return Some(Err(Box::new(Error::ReinstateSandbox {
                        msg: "failed to receive from netman's oneshot channel".into(),
                        err: Some(Box::new(err)),
                    })))
                }
            },
            Err(err) => {
                return Some(Err(Box::new(Error::ReinstateSandbox {
                    msg: format!("failed to request from netman {:?}", state.tap).into_boxed_str(),
                    err: Some(Box::new(err)),
                })))
            }
        };

        Some(Ok(MicroVm::with_stats(
            &function_id,
            tap,
            state.vm,
            state.stats,
        )))
    }
}

/// Helpers for Function registration.
impl FirecrackerContainerd {
    // To download image `nginx:1.25.0` via `firecracker-ctr`:
    //      # PATH=$PATH:/home/christos/src/fc-ctrd/_build/bin \
    //           /home/christos/src/fc-ctrd/firecracker-control/cmd/containerd/firecracker-ctr \
    //               --address /run/firecracker-containerd/containerd.sock \
    //               -n default \
    //               i \
    //                   pull \
    //                       --snapshotter devmapper \
    //                       docker.io/library/nginx:1.25.0
    #[instrument(level = Level::INFO, skip(config))]
    async fn ensure_image(
        config: &FirecrackerContainerdConfig,
        image_ref: &str,
    ) -> Result<(), registration::Error> {
        let mut cmd = Command::new(&config.ctr);
        cmd.args([
            "--address",
            config.address.to_str().expect("valid path"),
            "--namespace",
            config.namespace.as_str(),
            "images",
            "pull",
            "--snapshotter",
            config.snapshotter.as_str(),
            image_ref,
        ]);
        trace!(?cmd);
        let out = cmd
            .output()
            .await
            .map_err(|err| registration::Error::Runtime {
                msg: "failed to fork/exec firecracker-ctr".into(),
                err: Some(Box::new(err)),
            })?;
        if !out.status.success() {
            error!(
                ?cmd,
                exit_status = %out.status,
                stdout = ?::std::str::from_utf8(&out.stdout),
                stderr = ?::std::str::from_utf8(&out.stderr),
                "Failed to ensure image is present"
            );
            return Err(registration::Error::Runtime {
                msg: format!("firecracker-ctr: {}", out.status).into_boxed_str(),
                err: None,
            });
        }
        trace!("ensured");
        Ok(())
    }

    async fn finalize_function_info(
        &self,
        fi: &mut FcctrdFunctionInfo,
    ) -> Result<(), registration::Error> {
        let client = self.client.as_ref().expect("Runtime is initialized");

        let img_cfg = client
            .get_image_config(&fi.image_ref)
            .await
            .map_err(|err| registration::Error::Runtime {
                msg: "failed to get OCI Image config".into(),
                err: Some(Box::new(err)),
            })?;

        if fi.parent_snapshot.is_none() {
            fi.parent_snapshot = Some(Client::calculate_parent_snapshot(&img_cfg));
        }

        if fi.process_args.is_none() {
            fi.process_args = Some(Client::form_process_args(&img_cfg).map_err(|err| {
                registration::Error::Runtime {
                    msg: "failed to extract command & args for the new process".into(),
                    err: Some(Box::new(err)),
                }
            })?);
        }

        Ok(())
    }
}
