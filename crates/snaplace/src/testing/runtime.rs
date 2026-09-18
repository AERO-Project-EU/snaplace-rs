use std::{fmt::Debug, marker::PhantomData, path::PathBuf};

use enum_map::EnumMap;
use tempfile::TempDir;
use tokio::sync::broadcast;
use tracing::{error, instrument, Level};

use crate::{
    metrics::{Nanoseconds, Timing},
    network::{
        self, NetworkManager, NetworkManagerHandle, NetworkManagerRef, SandboxNetworkingProvider,
    },
    worker::{Runtime, Sandbox},
    FunctionId,
};

/// Shared boxed error type alias used by the runtime test harness APIs.
pub type BoxError = Box<dyn ::std::error::Error + Send + Sync + 'static>;

/// Cleanup result paired with the retained harness storage.
///
/// This is needed because teardown may fail precisely in the cases where the
/// caller most needs the harness's temporary directory for post-mortem
/// inspection (e.g., preserved snapshots, rendered configs, or runtime logs).
/// A plain `Result<TempDir, BoxError>` cannot return both the retained storage
/// and the cleanup error at the same time.
#[derive(Debug)]
pub struct RetainedCleanup {
    tempdir: TempDir,
    cleanup: Result<(), BoxError>,
}

impl RetainedCleanup {
    /// Bundles retained harness storage with the result of teardown.
    ///
    /// This lets callers keep the harness [`TempDir`] for inspection while
    /// still observing whether shutdown or cleanup failed.
    pub fn new(tempdir: TempDir, cleanup: Result<(), BoxError>) -> Self {
        Self { tempdir, cleanup }
    }

    /// Splits this value into the retained [`TempDir`] and the teardown result.
    ///
    /// Callers are meant to inspect the cleanup result first, then decide
    /// whether to keep or drop the returned tempdir.
    pub fn into_parts(self) -> (TempDir, Result<(), BoxError>) {
        (self.tempdir, self.cleanup)
    }
}

/// Test-only helper that lets callers exercise a [`Runtime`] directly without
/// spawning a [`Worker`].
///
/// This owns:
/// - the runtime-specific config payload
/// - the Function metadata
/// - a temporary scratch directory
/// - a private [`NetworkManager`] used to allocate and deallocate networking
///   resources
///
/// [`Worker`]: crate::worker::Worker
#[derive(Debug)]
pub struct RuntimeHarness<Rt: Runtime> {
    /// Runtime configuration used to construct harnessed runtimes.
    config: Rt::Config,
    /// Function metadata associated with this harness.
    function_info: Rt::FunctionInfo,
    /// Temporary directory used for snapshot and scratch files.
    tempdir: TempDir,
    /// Shutdown signal for the private network manager.
    quit_tx: broadcast::Sender<()>,
    /// Join handle for the private network manager task.
    netman_handle: NetworkManagerHandle,
    /// Harness-owned reference into the private network manager.
    netman: NetworkManagerRef<Rt::NetResource>,
    /// Marker tying the harness to the runtime type parameter.
    _runtime: PhantomData<Rt>,
}

impl<Rt> RuntimeHarness<Rt>
where
    Rt: Runtime,
{
    fn internal_new<N>(
        config: Rt::Config,
        function_info: Rt::FunctionInfo,
        net_provider: N,
        mut tempdir: Option<TempDir>,
    ) -> Result<Self, BoxError>
    where
        N: SandboxNetworkingProvider<Resource = Rt::NetResource>,
    {
        if tempdir.is_none() {
            tempdir = Some(TempDir::new().map_err(Box::new)?);
        }
        let (quit_tx, quit_rx) = broadcast::channel(1);
        let (netman_handle, netman) =
            NetworkManager::spawn(net_provider, quit_rx).map_err(Box::new)?;

        Ok(Self {
            config,
            function_info,
            tempdir: tempdir.unwrap(),
            quit_tx,
            netman_handle,
            netman,
            _runtime: PhantomData,
        })
    }

    /// Create a new harness with a private [`NetworkManager`] and temporary
    /// scratch directory.
    #[instrument(level = Level::DEBUG, skip(net_provider), err(Debug))]
    pub fn new<N>(
        config: Rt::Config,
        function_info: Rt::FunctionInfo,
        net_provider: N,
    ) -> Result<Self, BoxError>
    where
        N: SandboxNetworkingProvider<Resource = Rt::NetResource>,
    {
        Self::internal_new(config, function_info, net_provider, None)
    }

    /// Create a new harness with a private [`NetworkManager`] and the provided
    /// temporary scratch directory.
    ///
    /// Also see [`Self::shutdown_retaining_storage`].
    #[instrument(level = Level::DEBUG, skip(net_provider), err(Debug))]
    pub fn with_tempdir<N>(
        config: Rt::Config,
        function_info: Rt::FunctionInfo,
        net_provider: N,
        tempdir: TempDir,
    ) -> Result<Self, BoxError>
    where
        N: SandboxNetworkingProvider<Resource = Rt::NetResource>,
    {
        Self::internal_new(config, function_info, net_provider, Some(tempdir))
    }

    /// Return the runtime-specific configuration owned by this harness.
    pub fn config(&self) -> &Rt::Config {
        &self.config
    }

    /// Return the Function metadata owned by this harness.
    pub fn function_info(&self) -> &Rt::FunctionInfo {
        &self.function_info
    }

    /// Create a fresh, zeroed [`Timing`]s buffer for [`Runtime`] calls.
    pub fn fresh_timings(&self) -> EnumMap<Timing, Nanoseconds> {
        EnumMap::default()
    }

    /// Return snapshot state and memory file paths under harness's tempdir.
    #[instrument(level = Level::DEBUG, skip(self), err(Debug), ret)]
    pub fn snapshot_paths(&self, label: &str) -> Result<(PathBuf, PathBuf), BoxError> {
        let dir = self.tempdir.path().join(label);
        ::std::fs::create_dir_all(&dir).map_err(Box::new)?;
        Ok((dir.join("state.bin"), dir.join("memory.bin")))
    }

    /// Instantiate and initialize a [`Runtime`] directly, without a [`Worker`].
    ///
    /// [`Worker`]: crate::worker::Worker
    #[instrument(level = Level::DEBUG, skip(self), err(Debug))]
    pub async fn init_runtime(&self, sandbox: Option<&Rt::Sandbox>) -> Result<Rt, BoxError> {
        let mut runtime = Rt::new(&self.config, &self.function_info, sandbox)?;
        runtime.init().await?;
        Ok(runtime)
    }

    /// Register the harness Function metadata through the [`Runtime`].
    #[instrument(level = Level::DEBUG, skip(self), err(Debug))]
    pub async fn register_function(&mut self) -> Result<(), crate::metadata::registration::Error> {
        Rt::register_function(&self.config, &mut self.function_info).await
    }

    /// Deregister the harness Function metadata through the [`Runtime`].
    #[instrument(level = Level::DEBUG, skip(self), err(Debug))]
    pub async fn deregister_function(&self) -> Result<(), crate::metadata::registration::Error> {
        Rt::deregister_function(&self.config, &self.function_info).await
    }

    /// Register other (than the harness's) Function metadata through the
    /// [`Runtime`].
    ///
    /// # Note
    ///
    /// This Function cannot be deregistered throughout the execution of all
    /// tests in the same binary (and maybe even later, if the registry is
    /// persistent). Nevertheless, the [`FunctionId`] includes a random (v4)
    /// [`Uuid`], so the chance of collision should be acceptable (especially
    /// as long as all [`Runtime`] implementations have an in-memory registry).
    ///
    /// [`Uuid`]: uuid::Uuid
    #[instrument(level = Level::DEBUG, skip(self), err(Debug))]
    pub async fn register_other_function(
        &mut self,
        other: &mut Rt::FunctionInfo,
    ) -> Result<(), crate::metadata::registration::Error> {
        Rt::register_function(&self.config, other).await
    }

    /// Allocate a [networking resource] from the harness-owned [`NetworkManager`].
    ///
    /// [networking resource]: crate::network::Resource
    #[instrument(level = Level::DEBUG, skip(self), err(Debug), ret)]
    pub async fn allocate_net_resource(&self) -> Result<Rt::NetResource, BoxError> {
        let rx = self.netman.allocate().await.map_err(Box::new)?;
        rx.await
            .map_err(Box::new)?
            .map_err(|err| Box::new(err) as _)
    }

    /// Return a [networking resource] to the harness-owned [`NetworkManager`].
    ///
    /// [networking resource]: crate::network::Resource
    #[instrument(level = Level::DEBUG, skip(self), err(Debug))]
    pub async fn deallocate_net_resource(
        &self,
        net_resource: Rt::NetResource,
    ) -> Result<(), BoxError> {
        self.netman
            .deallocate(net_resource)
            .await
            .map_err(|err| Box::new(err) as _)
    }

    /// Auxiliary (composing) method to destroy a [`Sandbox`] and return its
    /// [networking resource] to the harness-owned [`NetworkManager`].
    ///
    /// If [`Runtime::destroy_sandbox`] fails, the recovered [`Sandbox`] ownership
    /// (carried in [`DestroySandboxRuntimeError`]) is discarded with the boxed
    /// error. Tests that need to inspect or quarantine the returned Sandbox should
    /// call [`Runtime::destroy_sandbox`] directly.
    ///
    /// [`DestroySandboxRuntimeError`]: crate::worker::runtime::DestroySandboxRuntimeError
    /// [networking resource]: crate::network::Resource
    #[instrument(level = Level::DEBUG, skip(self, runtime), err(Debug))]
    pub async fn destroy_sandbox(
        &self,
        runtime: &mut Rt,
        sandbox: Rt::Sandbox,
    ) -> Result<(), BoxError> {
        let net_resource = runtime.destroy_sandbox(sandbox).await?;
        self.deallocate_net_resource(net_resource).await
    }

    /// Auxiliary (composing) method to shut down a [`Sandbox`], then destroy
    /// it and deallocate its [networking resource].
    ///
    /// [networking resource]: crate::network::Resource
    #[instrument(level = Level::DEBUG, skip(self, runtime), err(Debug))]
    pub async fn shutdown_and_destroy_sandbox(
        &self,
        runtime: &mut Rt,
        mut sandbox: Rt::Sandbox,
    ) -> Result<(), BoxError> {
        runtime.shutdown_sandbox(&mut sandbox).await?;
        self.destroy_sandbox(runtime, sandbox).await
    }

    /// Auxiliary (composing) method to recreate a runtime from a snapshot-only
    /// [`Sandbox`], then destroy it and deallocate its [networking resource].
    ///
    /// This is useful for tests that need the runtime context rebuilt from the
    /// sandbox before teardown.
    ///
    /// [networking resource]: crate::network::Resource
    #[instrument(level = Level::DEBUG, skip(self), err(Debug))]
    pub async fn destroy_sandbox_with_new_runtime(
        &self,
        sandbox: Rt::Sandbox,
    ) -> Result<(), BoxError> {
        let mut runtime = self.init_runtime(Some(&sandbox)).await?;
        self.destroy_sandbox(&mut runtime, sandbox).await
    }

    /// ["Reinstate"] a [`Sandbox`] (using the harness-owned [`NetworkManager`]).
    ///
    /// ["Reinstate"]: crate::worker::Runtime::reinstate_sandbox
    #[instrument(level = Level::DEBUG, skip(self, runtime), err(Debug), ret)]
    pub async fn reinstate_sandbox(
        &self,
        runtime: &mut Rt,
        function_id: FunctionId,
        state: <Rt::Sandbox as Sandbox>::SnapshotState,
    ) -> Result<Option<Rt::Sandbox>, BoxError> {
        match runtime
            .reinstate_sandbox(function_id, state, self.netman.clone())
            .await
        {
            None => Ok(None),
            Some(Ok(sandbox)) => Ok(Some(sandbox)),
            Some(Err(err)) => Err(err),
        }
    }

    /// Shut down the harness and reap its private [`NetworkManager`].
    #[instrument(level = Level::DEBUG, skip(self), err(Debug))]
    pub async fn shutdown(self) -> Result<(), BoxError> {
        let Self {
            quit_tx,
            netman_handle,
            netman,
            ..
        } = self;
        let _ = quit_tx.send(());
        drop(netman);
        match netman_handle.reap().await {
            Ok(Ok(())) | Ok(Err(network::Error::UnexpectedShutDown)) => Ok(()),
            Ok(Err(err)) => Err(Box::new(err) as _),
            Err(err) => Err(Box::new(err) as _),
        }
    }

    /// Same as [`Self::shutdown`], but always returns the owned [`TempDir`].
    ///
    /// The returned [`RetainedCleanup`] carries both the retained tempdir and the
    /// result of shutting down the harness networking/background tasks.  This is
    /// useful when the caller must inspect persisted state even if shutdown fails.
    ///
    /// Also see [`Self::with_tempdir`].
    #[instrument(level = Level::DEBUG, skip(self))]
    pub async fn shutdown_retaining_storage(self) -> RetainedCleanup {
        let Self {
            quit_tx,
            tempdir,
            netman_handle,
            netman,
            ..
        } = self;
        let _ = quit_tx.send(());
        drop(netman);
        let cleanup = match netman_handle.reap().await {
            Ok(Ok(())) | Ok(Err(network::Error::UnexpectedShutDown)) => Ok(()),
            Ok(Err(err)) => Err(Box::new(err) as _),
            Err(err) => Err(Box::new(err) as _),
        };

        RetainedCleanup::new(tempdir, cleanup)
    }
}

/// Cleanup guard.
///
/// This is intentionally narrow: it tracks whether Function registration
/// happened and guarantees that the harness is deregistered and shut down
/// on cleanup, but it does not own runtimes or sandboxes.
#[derive(Debug)]
pub struct RuntimeHarnessGuard<Rt: Runtime> {
    /// Harness state protected by this guard.
    harness: Option<RuntimeHarness<Rt>>,
    /// Whether the guarded Function has been registered.
    ///
    /// Why track this?
    /// - Prevent spurious cleanup errors: Not every (future) test necessarily reaches the point
    ///   of Function registration. If [`Self::cleanup`] always blindly deregisters the Function,
    ///   it might throw a `NotFound` error in some scenarios. By tracking `self.registered`, the
    ///   harness should know exactly when Function deregistration is actually required.
    /// - State isolation between tests: These integration tests interact with a metadata registry
    ///   (even if it's an in-memory one). If a test registers a Function and then fails, that
    ///   Function state is "leaked." If the harness doesn't track this to clean it up, subsequent
    ///   tests might fail with errors like `AlreadyExists` or interact with stale data.
    registered: bool,
}

impl<Rt> RuntimeHarnessGuard<Rt>
where
    Rt: Runtime,
{
    /// Wrap the provided harness in a new guard.
    pub fn new(harness: RuntimeHarness<Rt>) -> Self {
        Self {
            harness: Some(harness),
            registered: false,
        }
    }

    /// Borrow the guarded harness.
    ///
    /// # Panics
    ///
    /// If no underlying [`RuntimeHarness`] exists.
    pub fn harness(&self) -> &RuntimeHarness<Rt> {
        self.harness
            .as_ref()
            .expect("harness should still be present")
    }

    /// Borrow the guarded harness mutably.
    ///
    /// # Panics
    ///
    /// If no underlying [`RuntimeHarness`] exists.
    pub fn harness_mut(&mut self) -> &mut RuntimeHarness<Rt> {
        self.harness
            .as_mut()
            .expect("harness should still be present")
    }

    /// Register the guarded Function metadata through the [`Runtime`].
    ///
    /// # Panics
    ///
    /// If no underlying [`RuntimeHarness`] exists (which should be impossible).
    #[instrument(level = Level::DEBUG, skip(self), err(Debug))]
    pub async fn register_function(&mut self) -> Result<(), crate::metadata::registration::Error> {
        self.harness
            .as_mut()
            .expect("harness should still be present")
            .register_function()
            .await?;
        self.registered = true;
        Ok(())
    }

    /// Deregister the guarded Function, then shut down the guarded harness.
    ///
    /// If multiple cleanup steps fail, the first error is returned and later
    /// errors are logged.
    #[instrument(level = Level::DEBUG, skip(self), err(Debug))]
    pub async fn cleanup(mut self) -> Result<(), BoxError> {
        let Some(harness) = self.harness.take() else {
            return Ok(());
        };

        let mut first_err: Option<BoxError> = None;

        if self.registered
            && let Err(err) = harness.deregister_function().await
        {
            error!(error = ?err, "Failed to deregister Function during guard cleanup: {err:#}");
            first_err = Some(Box::new(err));
        }

        if let Err(err) = harness.shutdown().await {
            error!(error = ?err, "Failed to shut down runtime harness during guard cleanup: {err:#}");
            if first_err.is_none() {
                first_err = Some(err);
            }
        }

        first_err.map_or(Ok(()), Err)
    }

    /// Same as [`Self::cleanup`], but preserves the harness's [`TempDir`].
    ///
    /// On success, returns a [`RetainedCleanup`] containing the retained tempdir
    /// and the first cleanup failure, if any, across deregistration and harness
    /// shutdown.  This is useful for tests that must keep runtime artifacts for
    /// post-mortem inspection when cleanup does not complete cleanly.
    ///
    /// Also see [`RuntimeHarness::with_tempdir`] and
    /// [`RuntimeHarness::shutdown_retaining_storage`].
    #[instrument(level = Level::DEBUG, skip(self), err(Debug))]
    pub async fn cleanup_retaining_storage(mut self) -> Result<RetainedCleanup, BoxError> {
        let Some(harness) = self.harness.take() else {
            return Err("harness along with its storage have already been cleaned up".into());
        };

        let mut first_err: Option<BoxError> = None;

        if self.registered
            && let Err(err) = harness.deregister_function().await
        {
            error!(error = ?err, "Failed to deregister Function during guard cleanup: {err:#}");
            first_err = Some(Box::new(err));
        }

        let RetainedCleanup { tempdir, cleanup } = harness.shutdown_retaining_storage().await;
        if let Err(err) = cleanup {
            error!(
                error = ?err,
                "Failed to shut down runtime harness during guard cleanup: {err:#}"
            );
            if first_err.is_none() {
                first_err = Some(err);
            }
        }

        Ok(RetainedCleanup::new(tempdir, first_err.map_or(Ok(()), Err)))
    }
}

/// Example run:
///
/// ```console
/// $ cargo t 'testing::runtime' --package snaplace --features=test-utils
/// ```
#[cfg(test)]
mod tests {
    use std::{borrow::Cow, net::Ipv4Addr, path::Path};

    use ubyte::{ByteUnit, ToByteUnit};

    use crate::{
        metadata::{registration, SandboxStats},
        metrics::{Nanoseconds, Timing},
        network::{self, SandboxNetworkingProvider},
        worker::{
            runtime::{DestroySandboxRuntimeError, SnapshotState},
            Runtime, Sandbox,
        },
        FunctionId,
    };

    use super::*;

    /// Minimal network resource used by the harness self-tests.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ProbeResource {
        /// Descriptor value returned by the probe provider.
        descriptor: usize,
    }

    impl network::Resource for ProbeResource {
        type Descriptor = usize;
    }

    /// In-memory networking provider used to test the harness itself.
    #[derive(Debug, Default)]
    struct ProbeProvider {
        /// Next descriptor value to hand out.
        next: usize,
    }

    impl SandboxNetworkingProvider for ProbeProvider {
        type Resource = ProbeResource;

        fn alloc(&mut self) -> network::Result<Self::Resource> {
            self.next += 1;
            Ok(ProbeResource {
                descriptor: self.next,
            })
        }

        async fn request(&mut self, desc: usize) -> network::Result<Self::Resource> {
            Ok(ProbeResource { descriptor: desc })
        }

        async fn dealloc(&mut self, _net_rsrc: Self::Resource) -> network::Result<()> {
            Ok(())
        }
    }

    /// Sandbox fixture used to exercise the generic runtime contract.
    #[derive(Debug)]
    struct ProbeSandbox {
        /// Statistics returned through the [`Sandbox`] API.
        stats: SandboxStats,
        /// Function identity associated with the sandbox.
        function_id: FunctionId,
        /// Network resource owned by the sandbox.
        net: ProbeResource,
        /// Snapshot state associated with the sandbox.
        state: ProbeState,
    }

    /// Snapshot state used by the probe sandbox.
    #[derive(Debug, Clone, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize)]
    struct ProbeState {
        /// Stable snapshot identifier.
        id: String,
    }

    impl SnapshotState for ProbeState {
        fn id(&self) -> Cow<'_, str> {
            Cow::Borrowed(&self.id)
        }
    }

    impl Sandbox for ProbeSandbox {
        type SnapshotState = ProbeState;

        fn ip_addr(&self) -> Ipv4Addr {
            Ipv4Addr::new(127, 0, 0, 1)
        }

        fn stats(&self) -> &crate::metadata::SandboxStats {
            &self.stats
        }

        fn stats_mut(&mut self) -> &mut crate::metadata::SandboxStats {
            &mut self.stats
        }

        fn function_id(&self) -> FunctionId {
            self.function_id.clone()
        }

        fn id(&self) -> &str {
            &self.state.id
        }

        fn has_snapshot(&self) -> bool {
            true
        }

        fn monitor_targets(&self) -> Vec<crate::snapman::MonitorTarget> {
            Vec::new()
        }

        fn snapshot_state(
            &self,
        ) -> Option<Result<Self::SnapshotState, Box<dyn ::std::error::Error + Send + Sync + 'static>>>
        {
            Some(Ok(self.state.clone()))
        }
    }

    /// Probe runtime used to validate the harness without external services.
    #[derive(Debug)]
    struct ProbeRuntime;

    impl Runtime for ProbeRuntime {
        type Config = ();
        type Sandbox = ProbeSandbox;
        type NetResource = ProbeResource;
        type FunctionInfo = ProbeRuntimeInfo;

        const NAME: &'static str = "probe";

        fn new(
            _config: &Self::Config,
            _function_info: &Self::FunctionInfo,
            _sandbox: Option<&Self::Sandbox>,
        ) -> Result<Self, BoxError> {
            Ok(Self)
        }

        async fn init(&mut self) -> Result<(), BoxError> {
            Ok(())
        }

        async fn create_sandbox(
            &mut self,
            function_id: &FunctionId,
            net_resource: Self::NetResource,
            _timings: &mut EnumMap<Timing, Nanoseconds>,
        ) -> Result<Self::Sandbox, BoxError> {
            let id = format!("{}-{}", function_id, net_resource.descriptor);
            Ok(ProbeSandbox {
                stats: SandboxStats::default(),
                function_id: function_id.clone(),
                net: net_resource,
                state: ProbeState { id },
            })
        }

        async fn load_sandbox(&mut self, _sandbox: &mut Self::Sandbox) -> Result<(), BoxError> {
            Ok(())
        }

        async fn pause_sandbox(&mut self, _sandbox: &mut Self::Sandbox) -> Result<(), BoxError> {
            Ok(())
        }

        async fn resume_sandbox(&mut self, _sandbox: &mut Self::Sandbox) -> Result<(), BoxError> {
            Ok(())
        }

        async fn create_snapshot(
            &mut self,
            _sandbox: &mut Self::Sandbox,
            _state_file_path: impl AsRef<Path> + Send + Sync + Debug,
            _memory_file_path: impl AsRef<Path> + Send + Sync + Debug,
        ) -> Result<(), BoxError> {
            Ok(())
        }

        async fn shutdown_sandbox(&mut self, _sandbox: &mut Self::Sandbox) -> Result<(), BoxError> {
            Ok(())
        }

        async fn destroy_sandbox(
            &mut self,
            sandbox: Self::Sandbox,
        ) -> Result<Self::NetResource, DestroySandboxRuntimeError<Self::Sandbox>> {
            Ok(sandbox.net)
        }

        async fn register_function(
            _config: &Self::Config,
            _function_info: &mut Self::FunctionInfo,
        ) -> Result<(), crate::metadata::registration::Error> {
            Ok(())
        }

        async fn deregister_function(
            _config: &Self::Config,
            _function_info: &Self::FunctionInfo,
        ) -> Result<(), crate::metadata::registration::Error> {
            Ok(())
        }

        async fn reinstate_sandbox(
            &mut self,
            function_id: FunctionId,
            state: <Self::Sandbox as Sandbox>::SnapshotState,
            _netman: network::NetworkManagerRef<Self::NetResource>,
        ) -> Option<Result<Self::Sandbox, BoxError>> {
            Some(Ok(ProbeSandbox {
                stats: SandboxStats::default(),
                function_id,
                net: ProbeResource { descriptor: 0 },
                state,
            }))
        }
    }

    /// Minimal Function metadata used by the probe runtime.
    #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize)]
    struct ProbeRuntimeInfo;

    impl crate::metadata::FunctionInfo for ProbeRuntimeInfo {
        fn id(&self) -> &FunctionId {
            static ID: ::std::sync::LazyLock<FunctionId> =
                ::std::sync::LazyLock::new(|| FunctionId::from("probe.fn"));
            &ID
        }

        fn memory(&self) -> ByteUnit {
            1.mebibytes()
        }
    }

    impl TryFrom<crate::metadata::registration::pb::RegisterFunctionRequest> for ProbeRuntimeInfo {
        type Error = registration::Error;

        fn try_from(
            _req: crate::metadata::registration::pb::RegisterFunctionRequest,
        ) -> Result<Self, Self::Error> {
            Ok(Self)
        }
    }

    /// Build a probe harness for the runtime self-tests.
    fn probe_harness() -> RuntimeHarness<ProbeRuntime> {
        RuntimeHarness::new((), ProbeRuntimeInfo, ProbeProvider::default()).expect("probe harness")
    }

    /// Ensure snapshot files are created underneath the harness tempdir.
    #[::tokio::test(name = "rt-harn-selftest")]
    async fn snapshot_paths_live_under_tempdir() {
        let harness = probe_harness();
        let (state, memory) = harness.snapshot_paths("alpha").expect("snapshot paths");
        assert!(state.starts_with(harness.tempdir.path()));
        assert!(memory.starts_with(harness.tempdir.path()));
    }

    /// Test allocation and deallocation through the harness-owned network manager.
    #[::tokio::test(name = "rt-harn-selftest")]
    async fn allocate_and_deallocate_network_resources() {
        let harness = probe_harness();
        let net = harness.allocate_net_resource().await.expect("resource");
        harness
            .deallocate_net_resource(net)
            .await
            .expect("deallocate");
    }

    /// Test a full fresh-runtime lifecycle without a worker actor.
    #[::tokio::test(name = "rt-harn-selftest")]
    async fn runtime_round_trip_without_worker() {
        let harness = probe_harness();
        let mut runtime = harness.init_runtime(None).await.expect("runtime");

        let mut timings = harness.fresh_timings();
        let net = harness.allocate_net_resource().await.expect("resource");
        let sandbox = runtime
            .create_sandbox(&FunctionId::from("probe.fn"), net, &mut timings)
            .await
            .expect("sandbox");
        assert_eq!(sandbox.id(), "probe.fn-1");
        harness
            .destroy_sandbox(&mut runtime, sandbox)
            .await
            .expect("destroy");
    }

    /// Test shutting down a live sandbox before destroying it.
    #[::tokio::test(name = "rt-harn-selftest")]
    async fn shutdown_and_destroy_live_sandbox() {
        let harness = probe_harness();
        let mut runtime = harness.init_runtime(None).await.expect("runtime");

        let mut timings = harness.fresh_timings();
        let net = harness.allocate_net_resource().await.expect("resource");
        let sandbox = runtime
            .create_sandbox(&FunctionId::from("probe.fn"), net, &mut timings)
            .await
            .expect("sandbox");
        harness
            .shutdown_and_destroy_sandbox(&mut runtime, sandbox)
            .await
            .expect("shutdown and destroy");
    }

    /// Test destroying a snapshot-only sandbox using a reconstructed runtime.
    #[::tokio::test(name = "rt-harn-selftest")]
    async fn destroy_snapshot_only_sandbox_with_new_runtime() {
        let harness = probe_harness();
        let mut runtime = harness.init_runtime(None).await.expect("runtime");

        let mut timings = harness.fresh_timings();
        let net = harness.allocate_net_resource().await.expect("resource");
        let sandbox = runtime
            .create_sandbox(&FunctionId::from("probe.fn"), net, &mut timings)
            .await
            .expect("sandbox");
        harness
            .destroy_sandbox_with_new_runtime(sandbox)
            .await
            .expect("destroy snapshot-only sandbox");
    }

    /// Test harness-mediated reinstatement of a sandbox snapshot state.
    #[::tokio::test(name = "rt-harn-selftest")]
    async fn reinstatement_round_trip() {
        let harness = probe_harness();
        let mut runtime = harness.init_runtime(None).await.expect("runtime");
        let state = ProbeState {
            id: String::from("probe.fn-7"),
        };
        let sandbox = harness
            .reinstate_sandbox(&mut runtime, FunctionId::from("probe.fn"), state.clone())
            .await
            .expect("reinstated");
        let sandbox = sandbox.expect("some sandbox");
        assert_eq!(sandbox.snapshot_state().unwrap().unwrap(), state);
    }

    /// Ensure harness shutdown reaps the private network manager cleanly.
    #[::tokio::test(name = "rt-harn-selftest")]
    async fn harness_shutdown_reaps_network_manager() {
        let harness = probe_harness();
        harness.shutdown().await.expect("shutdown");
    }

    /// Ensure the guard deregisters and shuts down the harness cleanly.
    #[::tokio::test(name = "rt-harn-selftest")]
    async fn harness_guard_cleanup_reaps_network_manager() {
        let harness = probe_harness();
        let mut guard = RuntimeHarnessGuard::new(harness);
        guard.register_function().await.expect("register");
        guard.cleanup().await.expect("cleanup");
    }

    /// Ensure the guard cleanup path also works when registration never ran.
    #[::tokio::test(name = "rt-harn-selftest")]
    async fn harness_guard_cleanup_without_registration() {
        let harness = probe_harness();
        let guard = RuntimeHarnessGuard::new(harness);
        guard.cleanup().await.expect("cleanup");
    }
}
