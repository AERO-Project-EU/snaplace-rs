use std::{collections::HashSet, time::Duration};

use ubyte::ByteUnit;

use crate::{
    keepalive::{eviction, PoolContext},
    utils::sd_static::SlowdownsCsvError,
    FunctionId,
};

/// A [keep-alive policy] meant to work together with the [`SlowdownsStatic`][1]
/// (aka "sd-static") [snapshot placement policy].
///
/// At initialization, it is provided with a set of [`FunctionId`]s, which are assumed to refer to
/// all Functions whose `Sandbox`es should stay "warm".
///
/// To decide the keep-alive duration to be assigned to a `Sandbox`:
/// - If its [`FunctionId`] refers to a Function that __is__ classifed as "warm":
///   1. If `self.duration.is_some()`, then the `Sandbox` is kept alive for that
///      configured keep-alive duration.
///   2. If `self.duration.is_none()`, then the `Sandbox` is kept alive [forever](Self::FOREVER).
/// - If its [`FunctionId`] refers to a Function that is __NOT__ classified as
///   "warm", the `Sandbox` should __not__ be kept alive, therefore:
///   1. `assign()` returns [`None`] (if `self.non_warm_duration.is_none()`, i.e.,
///      normally and by default).
///   2. If `self.non_warm_duration.is_some()` (i.e., the keep-alive duration for
///      __non__-warm Functions is overriden), then the `Sandbox` is kept alive
///      for this configured keep-alive duration.
///
/// # Notes
///
/// Unless total memory capacity is not an issue, always configure a keep-alive duration, to make
/// sure that Functions are scaled down eventually.
///
/// ## Thoughts on Eviction
///
/// Do we have to check whether warm Functions can fit into the available memory
/// capacity? How could we possibly do this?
/// - Would it be correct to assume that 1 sandbox for each such Function remains warm?
///   Probably not, since multiple sandboxes for the same Function can be kept alive at
///   any time by this policy.
/// - ...?
///
/// [`eviction::PolicyExcept`] is probably the wrong abstraction for this policy. We
/// would probably need something that includes the notion of priority for Functions
/// that are supposed to be kept always warm, against others that are permitted to
/// shut down.
///
/// Is this really a problem for _this_ `keepalive::Policy` though?
/// - `keepalive::SlowdownsStatic` returns a non-zero keepalive duration only for
///   Functions that are supposed to be kept _always_ alive, and always suggests to
///   immediately shut down sandboxes of all other Functions (with the exception of
///   [`non_warm_duration`]).
/// - Therefore, at any time (again, except for [`non_warm_duration`]), any _Idle_
///   Workers tracked by the Pool own sandboxes of Functions that should indeed
///   be kept always alive.
/// - This means that there is no difference in priority among all _Idle_ Workers under
///   `keepalive::SlowdownsStatic`, so no point in using an [`eviction::PolicyExcept`] to
///   exclude "some" Functions from eviction: all _Idle_ Workers refer to high-priority
///   Functions. A plain LRU eviction policy (e.g., [`eviction::LruWorker`]) suffices.
/// - This might not be the case for other keep-alive policies that might be developed
///   in the future, where both high- and low-priority Functions might be kept alive
///   under various circumstances and/or for various durations. Would such policies
///   require a new eviction policy abstraction?
///
///
/// [`FunctionId`]: crate::FunctionId
/// [`eviction::LruWorker`]: crate::sbpool::keepalive::eviction::LruWorker
/// [`eviction::PolicyExcept`]: crate::sbpool::keepalive::eviction::PolicyExcept
/// [keep-alive policy]: crate::sbpool::keepalive::Policy
/// [snapshot placement policy]: crate::snapman::placement::PlacementAlgorithm
/// [1]: crate::snapman::placement::sd_static::SlowdownsStatic
/// [`non_warm_duration`]: crate::conf::KeepAliveConfig::SlowdownsStatic::non_warm_duration
#[derive(Debug, Clone)]
pub struct SlowdownsStatic<E> {
    /// All Functions classified/treated as "warm" when assigning keep-alive.
    ///
    /// In `faascell`, this is the union of:
    /// - [`FunctionId`]s classified as [`KeepAlive`] by the (paired) `sd-static`
    ///   [snapshot placement policy].
    /// - [`FunctionId`]s listed in the keep-alive [`fixed_warm_functions_path`].
    ///
    /// [`KeepAlive`]: crate::snapman::placement::sd_static::SnapshotDestination::KeepAlive
    /// [snapshot placement policy]: crate::snapman::placement::PlacementAlgorithm
    /// [`fixed_warm_functions_path`]: crate::conf::KeepAliveConfig::SlowdownsStatic::fixed_warm_functions_path
    warm_functions: HashSet<FunctionId, crate::BuildHasher>,

    /// Keep-alive duration for "warm" Functions; `None` means [`Self::FOREVER`].
    duration: Option<Duration>,

    /// Keep-alive duration override for non-warm Functions; `None`/omission
    /// means shutdown.
    non_warm_duration: Option<Duration>,

    /// [Eviction policy] delegated to for memory-pressure decisions.
    ///
    /// [Eviction policy]: crate::sbpool::keepalive::eviction::Policy
    eviction_policy: E,
}

impl<E> SlowdownsStatic<E> {
    /// A definition for a "very long" time to keep a Function `Sandbox` alive.
    pub const FOREVER: Duration = Duration::from_secs(86400); // 1 day

    pub fn new(
        functions: impl IntoIterator<Item = FunctionId>,
        duration: Option<Duration>,
        non_warm_duration: Option<Duration>,
        eviction_policy: E,
    ) -> Result<Self, SlowdownsCsvError> {
        Ok(Self {
            warm_functions: HashSet::from_iter(functions),
            duration,
            non_warm_duration,
            eviction_policy,
        })
    }
}

impl<E: eviction::Policy> eviction::Policy for SlowdownsStatic<E> {
    #[inline]
    fn evict<FmdStore, FunctionInfo>(
        &mut self,
        pctx: PoolContext<'_, FmdStore, FunctionInfo>,
        mem_to_reclaim: ByteUnit,
    ) where
        FmdStore: crate::FunctionMetadataStore<FunctionInfo>,
    {
        self.eviction_policy.evict(pctx, mem_to_reclaim);
    }
}

impl<E: eviction::Policy> super::Policy for SlowdownsStatic<E> {
    #[inline]
    fn assign<FmdStore, FunctionInfo>(
        &mut self,
        _: PoolContext<'_, FmdStore, FunctionInfo>,
        function_id: &FunctionId,
    ) -> Option<Duration>
    where
        FmdStore: crate::FunctionMetadataStore<FunctionInfo>,
    {
        self.warm_functions
            .contains(function_id)
            .then_some(self.duration.unwrap_or(Self::FOREVER))
            .or(self.non_warm_duration)
    }
}
