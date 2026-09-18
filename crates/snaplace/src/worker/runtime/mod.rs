#[cfg(feature = "rt-fc")]
pub mod fc;
#[cfg(feature = "rt-fcctrd")]
pub mod fcctrd;
#[cfg(feature = "__toy")]
pub mod toy;

use std::{borrow::Cow, fmt::Debug, net::Ipv4Addr, path::Path};

use enum_map::EnumMap;
use serde::{Deserialize, Serialize};

use crate::{
    metadata::{registration, FunctionInfo, SandboxStats},
    metrics::{Nanoseconds, Timing},
    network,
    sbpool::Cpu,
    snapman::MonitorTarget,
    FunctionId,
};

/// Error returned by [`Runtime::destroy_sandbox`] on failure to
/// "fully"/correctly destroy a [`Sandbox`].
///
/// The returned [`Sandbox`] is considered as some sort of "ownership token",
/// probably for accounting/diagnostic reasons, and to be quarantined.
/// It must _NOT_ be treated as a reusable or in a known state / intact
/// [`Sandbox`]; [`Runtime::destroy_sandbox`] may have successfully destroyed
/// some of its resources before failing, thus trashing it.
#[derive(Debug, ::thiserror::Error)]
#[error("Runtime failed to destroy Sandbox")]
pub struct DestroySandboxRuntimeError<S> {
    /// Sandbox ownership recovered from the failed destroy attempt.
    ///
    /// This is only for quarantine/accounting; it must NOT be treated as a
    /// reusable/intact [`Sandbox`] in a known state.
    pub(super) sandbox: S,

    /// Runtime-specific cause of the failed destruction attempt.
    #[source]
    pub(super) err: Box<dyn ::std::error::Error + Send + Sync + 'static>,
}

// TODO(ckatsak): Should probably add a constructor for out-of-tree Runtime impls.
impl<S> DestroySandboxRuntimeError<S> {
    pub fn into_parts(self) -> (S, Box<dyn ::std::error::Error + Send + Sync + 'static>) {
        (self.sandbox, self.err)
    }
}

pub trait Runtime
where
    Self: Sized + Send + Sync + 'static,
{
    /// Runtime-specific configuration carried by [`SnaplaceConfig`] and
    /// passed to this runtime when constructing [`Worker`]s and serving
    /// runtime-dependent control-plane operations.
    ///
    /// `snaplace` generic code carries this type as a parameter (instead of
    /// using a global runtime-config enum). Each `Runtime` implementation
    /// defines its own `Config` type, and binaries (such as `faascell`)
    /// deserialize the appropriate payload for the selected runtime.
    ///
    /// [`SnaplaceConfig`]: crate::conf::SnaplaceConfig
    /// [`Worker`]: crate::worker::Worker
    type Config: Clone + Debug + Send + Sync + 'static;
    /// The type of Function [`Sandbox`]es managed by this `Runtime`.
    type Sandbox: Sandbox;
    /// The type of [networking resources] employed by this `Runtime` and its [`Sandbox`]es.
    ///
    /// [networking resources]: crate::network::Resource
    type NetResource: network::Resource;
    /// The type of [`FunctionInfo`] that describes each Function to this `Runtime`.
    type FunctionInfo: FunctionInfo;

    /// A string that uniquely identifies this `Runtime` in `snaplace`.
    const NAME: &'static str;

    /// Instantiate the runtime to be passed to the [`Worker`] at spawn time.
    ///
    /// # Note
    ///
    /// This is a synchronous (rather than `async`) call that is meant to be quick, as it is
    /// run by the [`SandboxPool`] task (thus blocking it).
    /// Any other (possibly more complex) initialization is expected to take place in
    /// [`Runtime::init`], which is run by the [`Worker`] task (and therefore does not block
    /// the [`SandboxPool`]).
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    fn new(
        config: &Self::Config,
        function_info: &Self::FunctionInfo,
        sandbox: Option<&Self::Sandbox>,
    ) -> Result<Self, Box<dyn ::std::error::Error + Send + Sync + 'static>>;

    /// Properly and fully initialize the runtime.
    ///
    /// # Note
    ///
    /// This is where any complex/heavy initialization should take place (rather than in
    /// [`Runtime::new`]), as it is asynchronous and run by the [`Worker`] task (hence not blocking
    /// the [`SandboxPool`] nor any other major actor).
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    fn init(
        &mut self,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send;

    /// Spawn a new `Self::Sandbox` for the provided [`FunctionId`], given the provided
    /// `Self::NetResource`.
    ///
    /// A co-operative `Runtime` implementation should also update `timings` accordingly; in this
    /// case in particular, only the following (if applicable):
    /// - [`Timing::CreateSandbox`]
    /// - [`Timing::SetupResources`]
    fn create_sandbox(
        &mut self,
        function_id: &FunctionId,
        net_resource: Self::NetResource,
        timings: &mut EnumMap<Timing, Nanoseconds>,
    ) -> impl Future<
        Output = Result<Self::Sandbox, Box<dyn ::std::error::Error + Send + Sync + 'static>>,
    > + Send;

    fn load_sandbox(
        &mut self,
        sandbox: &mut Self::Sandbox,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send;

    fn pause_sandbox(
        &mut self,
        sandbox: &mut Self::Sandbox,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send;

    fn resume_sandbox(
        &mut self,
        sandbox: &mut Self::Sandbox,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send;

    fn create_snapshot(
        &mut self,
        sandbox: &mut Self::Sandbox,
        state_file_path: impl AsRef<Path> + Send + Sync + Debug,
        memory_file_path: impl AsRef<Path> + Send + Sync + Debug,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send;

    fn shutdown_sandbox(
        &mut self,
        sandbox: &mut Self::Sandbox,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send;

    fn destroy_sandbox(
        &mut self,
        sandbox: Self::Sandbox,
    ) -> impl Future<Output = Result<Self::NetResource, DestroySandboxRuntimeError<Self::Sandbox>>> + Send;

    fn register_function(
        config: &Self::Config,
        function_info: &mut Self::FunctionInfo,
    ) -> impl Future<Output = Result<(), registration::Error>> + Send;

    fn deregister_function(
        config: &Self::Config,
        function_info: &Self::FunctionInfo,
    ) -> impl Future<Output = Result<(), registration::Error>> + Send;

    /// Setup any resources allocated for the [`Sandbox`].
    ///
    /// This is the default, empty implementation. `Runtime` implementations are encouraged to
    /// override it in whatever way makes sense for them.
    ///
    /// Note -- FIXME
    ///
    /// This method is provided only a [`Cpu`] for now. This is temporary: in the future it will
    /// be provided with a bigger `struct Resources { .. }` (which will include [`Cpu`] as well).
    #[inline]
    fn setup_cpuset(
        &mut self,
        _sandbox: &mut Self::Sandbox,
        _cpuset: Cpu,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send
    {
        ::futures::future::ready(Ok(()))
    }

    /// Remove the [`Sandbox`] from the cache.
    ///
    /// This is the default, empty implementation. `Runtime` implementations are encouraged to
    /// override it, if it makes sense for them.
    #[inline]
    fn uncache_sandbox(
        &mut self,
        _sandbox: &mut Self::Sandbox,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send
    {
        ::futures::future::ready(Ok(()))
    }

    /// This method can be overriden by `Runtime` implementations to restore one of their
    /// [`Runtime::Sandbox`]es from its provided state ([`Sandbox::SnapshotState`]).
    ///
    /// # Returns
    ///
    /// `snaplace` expects the following:
    ///
    /// - [`None`], if the method is not implemented (the default trait implementation).
    /// - <code>[Some]\([Ok]\([Self::Sandbox]\)\)</code>, if the [`Sandbox`] has been successfully
    ///   reinstated from its [`Sandbox::SnapshotState`].
    /// - <code>[Some]\([Err]\(err\)\)</code>, if the method __is__ implemented by the `Runtime`,
    ///   but the reinstatement of the [`Sandbox`] has failed (and will _not_ be retried).
    ///
    /// # Note
    ///
    /// `snaplace` calls this only _at boot_; never in a "hot path". Therefore, long-running
    /// operations are kind of acceptable with respect to system's performance.
    #[inline]
    fn reinstate_sandbox(
        &mut self,
        _function_id: FunctionId,
        _state: <Self::Sandbox as Sandbox>::SnapshotState,
        _netman: network::NetworkManagerRef<Self::NetResource>, // FIXME(ckatsak): I don't like this here
    ) -> impl Future<
        Output = Option<
            Result<Self::Sandbox, Box<dyn ::std::error::Error + Send + Sync + 'static>>,
        >,
    > + Send {
        ::futures::future::ready(None)
    }
}

pub trait Sandbox: Debug + Send + Sync + 'static {
    /// The type in which the `Sandbox` type can be serialized along with all information necessary
    /// to later (upon deserialization) be recreated and fully functional.
    ///
    /// For this to happen, snapshot support in the associated [`Runtime`] is obviously required
    /// (hence the type's name).
    /// In other words, [`Sandbox::has_snapshot`] should always return `true` for a `Sandbox`
    /// deserialized from its `SnapshotState`.
    ///
    /// `Sandbox` implementations that do not support snapshotting (or are just not interested
    /// in restorable snapshots across FaaSCell reboots) can use the unit type (`()`) as their
    /// `Sandbox::SnapshotState` and skip overriding methods [`Sandbox::snapshot_state`] and
    /// [`Sandbox::from_snapshot_state`].
    type SnapshotState: SnapshotState;

    fn ip_addr(&self) -> Ipv4Addr; // FIXME(ckatsak): not all Sandbox impls have IP addresses?
    fn stats(&self) -> &SandboxStats;
    fn stats_mut(&mut self) -> &mut SandboxStats;
    fn function_id(&self) -> FunctionId;
    fn id(&self) -> &str;

    /// Returns true if this `Sandbox` can be restored from a snapshot.
    fn has_snapshot(&self) -> bool;

    /// Returns [`MonitorTarget`]s that identify this `Sandbox` and can be used by
    /// [`PerformanceMonitor`]s.
    ///
    /// [`PerformanceMonitor`]: crate::snapman::PerformanceMonitor
    fn monitor_targets(&self) -> Vec<MonitorTarget>;

    /// This method can be overriden by `Sandbox` implementations to return its restorable state
    /// as a [`Sandbox::SnapshotState`].
    ///
    /// # Returns
    ///
    /// `snaplace` expects the following:
    ///
    /// - [`None`], if the method is not implemented (the default trait implementation).
    /// - <code>[Some]\([Ok]\([Sandbox::SnapshotState]\)\)</code>, if `Sandbox`'s state has been
    ///   successfully created.
    /// - <code>[Some]\([Err]\(err\)\)</code>, if the method __is__ implemented by `Sandbox`, but
    ///   the creation of [`Sandbox::SnapshotState`] has failed (and will _not_ be retried).
    ///
    /// # Note
    ///
    /// `snaplace` calls this in a kind of "hot path". Therefore, the quicker this finishes the
    /// better it is for system's performance.
    #[inline]
    fn snapshot_state(
        &self,
    ) -> Option<Result<Self::SnapshotState, Box<dyn ::std::error::Error + Send + Sync + 'static>>>
    {
        None
    }
}

/// See [`Sandbox::SnapshotState`].
pub trait SnapshotState
where
    Self: Serialize + for<'de> Deserialize<'de> + Eq + Debug + Send + Sync + 'static,
{
    /// Returns an ID that uniquely identifies the associated [`Sandbox`] (hence this
    /// `SnapshotState` too).
    ///
    /// # Note
    ///
    /// In the special case of `impl SnapshotState for ()`, which may be used in [`Sandbox`]
    /// implementations that do not support snapshotting (or are just not interested in restorable
    /// snapshots across FaaSCell reboots) this method _always_ returns the empty string, `""`.
    fn id(&self) -> Cow<'_, str>;
}

impl SnapshotState for () {
    #[inline]
    fn id(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }
}
