use std::collections::HashSet;

use itertools::Itertools;
use rand::{rngs::SmallRng, seq::IteratorRandom};
use ubyte::ByteUnit;

use crate::{sbpool::pool::PoolContext, FunctionId, FunctionMetadataStore};

pub trait Policy: Send + 'static {
    /// Suggests which [`Worker`]\(s) (along with their associated [`Sandbox`]es) should
    /// be shut down to reclaim the specified amount of memory (`mem_to_reclaim`, in MiB).
    ///
    /// The auxiliary [`PoolContext`] passed as an argument provides:
    /// - information tracked by the [`SandboxPool`] that might be useful for this decision;
    /// - an exclusive reference to a <code>[Vec]<([FunctionId], [WorkerId])></code> to store
    ///   the results.
    ///
    /// [`FunctionId`]: crate::FunctionId
    /// [`PoolContext`]: crate::sbpool::pool::PoolContext
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    /// [WorkerId]: crate::worker::WorkerId
    fn evict<FmdStore, FunctionInfo>(
        &mut self,
        pctx: PoolContext<'_, FmdStore, FunctionInfo>,
        mem_to_reclaim: ByteUnit,
    ) where
        FmdStore: FunctionMetadataStore<FunctionInfo>;
}

/// An [eviction policy] that suggests [`Worker`]\(s) for eviction, but also takes into
/// account Functions whose [`Worker`]s must not be evicted.
///
/// [`Worker`]: crate::worker::Worker
/// [eviction policy]: crate::sbpool::keepalive::eviction::Policy
pub trait PolicyExcept: Policy {
    fn evict_except_set<FmdStore, FunctionInfo>(
        &mut self,
        pctx: PoolContext<'_, FmdStore, FunctionInfo>,
        mem_to_reclaim: ByteUnit,
        exclude: &HashSet<FunctionId, crate::BuildHasher>,
    ) where
        FmdStore: FunctionMetadataStore<FunctionInfo>;

    /// A more generic version of `PolicyExcept::evict_except_set`.
    ///
    /// The default implementation merely collects the `excluded` Iterator into
    /// a `HashSet` and calls `PolicyExcept::evict_except_set`.
    fn evict_except<FmdStore, FunctionInfo>(
        &mut self,
        pctx: PoolContext<'_, FmdStore, FunctionInfo>,
        mem_to_reclaim: ByteUnit,
        excluded: Option<impl IntoIterator<Item = FunctionId>>,
    ) where
        FmdStore: FunctionMetadataStore<FunctionInfo>,
    {
        match excluded.map(|xcl| xcl.into_iter().collect()) {
            Some(exclude) => self.evict_except_set(pctx, mem_to_reclaim, &exclude),
            None => self.evict(pctx, mem_to_reclaim),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Memory eviction policy that always suggests not to evict any [`Worker`] at all.
///
/// [`Worker`]: crate::worker::Worker
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOp;

impl Policy for NoOp {
    #[inline(always)]
    fn evict<FmdStore: FunctionMetadataStore<FunctionInfo>, FunctionInfo>(
        &mut self,
        _: PoolContext<'_, FmdStore, FunctionInfo>,
        _: ByteUnit,
    ) {
    }
}

impl PolicyExcept for NoOp {
    #[inline(always)]
    fn evict_except_set<FmdStore: FunctionMetadataStore<FunctionInfo>, FunctionInfo>(
        &mut self,
        _: PoolContext<'_, FmdStore, FunctionInfo>,
        _: ByteUnit,
        _: &HashSet<FunctionId, crate::BuildHasher>,
    ) {
    }

    #[inline(always)]
    fn evict_except<FmdStore: FunctionMetadataStore<FunctionInfo>, FunctionInfo>(
        &mut self,
        _: PoolContext<'_, FmdStore, FunctionInfo>,
        _: ByteUnit,
        _: Option<impl IntoIterator<Item = FunctionId>>,
    ) {
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Memory eviction policy that suggests randomly chosen [`Worker`]s for eviction.
///
/// [`Worker`]: crate::worker::Worker
#[derive(Debug, Clone)]
pub struct Random {
    rng: SmallRng,
}

impl Default for Random {
    fn default() -> Self {
        Self {
            rng: ::rand::make_rng(),
        }
    }
}

impl Policy for Random {
    fn evict<FmdStore: FunctionMetadataStore<FunctionInfo>, FunctionInfo>(
        &mut self,
        pctx: PoolContext<'_, FmdStore, FunctionInfo>,
        mut mem_to_reclaim: ByteUnit,
    ) {
        let mut idle_candidates = pctx.store.iter_idle().collect::<Vec<_>>();

        while mem_to_reclaim > 0 && !idle_candidates.is_empty() {
            let chosen = idle_candidates
                .iter()
                .choose(&mut self.rng)
                .cloned()
                .expect("Idle Workers do exist");
            mem_to_reclaim -= pctx.store.function_memory(&chosen.0);
            idle_candidates.retain(|w| *w != chosen);
            pctx.victims.push(chosen);
        }
    }
}

impl PolicyExcept for Random {
    fn evict_except_set<FmdStore: FunctionMetadataStore<FunctionInfo>, FunctionInfo>(
        &mut self,
        pctx: PoolContext<'_, FmdStore, FunctionInfo>,
        mut mem_to_reclaim: ByteUnit,
        excluded: &HashSet<FunctionId, crate::BuildHasher>,
    ) {
        let mut idle_candidates = pctx
            .store
            .iter_idle()
            .filter(|(fid, _)| !excluded.contains(fid.as_str()))
            .collect::<Vec<_>>();

        while mem_to_reclaim > 0 && !idle_candidates.is_empty() {
            let chosen = idle_candidates
                .iter()
                .choose(&mut self.rng)
                .cloned()
                .expect("Idle Workers do exist");
            mem_to_reclaim -= pctx.store.function_memory(&chosen.0);
            idle_candidates.retain(|w| *w != chosen);
            pctx.victims.push(chosen);
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Memory eviction policy that suggests the least recently used [`Worker`]s for eviction, based
/// on their deadlines as tracked by the [`SandboxPool`].
///
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
#[derive(Debug, Clone, Copy, Default)]
pub struct LruWorker;

impl LruWorker {
    const _PMSG: &'static str = "all Idle Workers should have an associated deadline";
}

impl Policy for LruWorker {
    fn evict<FmdStore: FunctionMetadataStore<FunctionInfo>, FunctionInfo>(
        &mut self,
        pctx: PoolContext<'_, FmdStore, FunctionInfo>,
        mut mem_to_reclaim: ByteUnit,
    ) {
        pctx.store
            .iter_idle()
            .sorted_unstable_by(|(_, xwid), (_, ywid)| {
                pctx.worker_deadlines
                    .get(xwid)
                    .expect(Self::_PMSG)
                    .cmp(pctx.worker_deadlines.get(ywid).expect(Self::_PMSG))
            })
            .take_while_inclusive(|(fid, _)| {
                mem_to_reclaim -= pctx.store.function_memory(fid);
                mem_to_reclaim > 0
            })
            .for_each(|chosen| pctx.victims.push(chosen))
    }
}

impl PolicyExcept for LruWorker {
    fn evict_except_set<FmdStore: FunctionMetadataStore<FunctionInfo>, FunctionInfo>(
        &mut self,
        pctx: PoolContext<'_, FmdStore, FunctionInfo>,
        mut mem_to_reclaim: ByteUnit,
        exclude: &HashSet<FunctionId, crate::BuildHasher>,
    ) {
        pctx.store
            .iter_idle()
            .filter(|(fid, _)| !exclude.contains(fid.as_str()))
            .sorted_unstable_by(|(_, xwid), (_, ywid)| {
                pctx.worker_deadlines
                    .get(xwid)
                    .expect(Self::_PMSG)
                    .cmp(pctx.worker_deadlines.get(ywid).expect(Self::_PMSG))
            })
            .take_while_inclusive(|(fid, _)| {
                mem_to_reclaim -= pctx.store.function_memory(fid);
                mem_to_reclaim > 0
            })
            .for_each(|chosen| pctx.victims.push(chosen))
    }
}
