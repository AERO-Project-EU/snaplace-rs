use std::time::Duration;

use ubyte::ByteUnit;

use crate::{keepalive::eviction, sbpool::pool::PoolContext, FunctionId, FunctionMetadataStore};

#[derive(Debug, Clone, Copy)]
pub struct Fixed<E> {
    ttl: Option<Duration>,
    eviction_policy: E,
}

impl<E> Fixed<E> {
    pub fn new(maybe_duration: Option<Duration>, eviction_policy: E) -> Self {
        Self {
            ttl: maybe_duration,
            eviction_policy,
        }
    }
}

impl<E: eviction::Policy> super::Policy for Fixed<E> {
    #[inline(always)]
    fn assign<FmdStore, FunctionInfo>(
        &mut self,
        _: PoolContext<'_, FmdStore, FunctionInfo>,
        _: &FunctionId,
    ) -> Option<Duration>
    where
        FmdStore: FunctionMetadataStore<FunctionInfo>,
    {
        self.ttl
    }
}

impl<E: eviction::Policy> eviction::Policy for Fixed<E> {
    #[inline(always)]
    fn evict<FmdStore, FunctionInfo>(
        &mut self,
        pctx: PoolContext<'_, FmdStore, FunctionInfo>,
        mem_to_reclaim: ByteUnit,
    ) where
        FmdStore: FunctionMetadataStore<FunctionInfo>,
    {
        self.eviction_policy.evict(pctx, mem_to_reclaim);
    }
}
