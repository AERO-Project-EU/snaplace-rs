#[cfg(feature = "fmd-store-dash")]
mod dash;
mod stdhash;

#[cfg(feature = "fmd-store-dash")]
pub use dash::DashMapStore;
pub use stdhash::StdHashMapFmdStore;

use triomphe::Arc;
use ubyte::ByteUnit;

use crate::{
    metadata::{
        registration::{self, RegisteredFunction},
        FunctionStats,
    },
    worker::WorkerId,
    FunctionId,
};

pub trait FunctionMetadataStore<FunctionInfo>: Send + Sync + 'static {
    // FIXME(ckatsak): Now that `FunctionMetadataStore` is "concurrency-enabled", and that the
    // `FunctionMetadata` trait no longer exists, there is probably no point in having this
    // associated type either.
    /// The type that represents the Function's metadata stored.
    type FunctionMetadata;

    /// The error type returned by methods of implementations of this trait.
    type Error: ::std::error::Error + Send + Sync + 'static;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // General
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Returns `true` if the provided [`FunctionId`] has been registered with the
    /// `FunctionMetadataStore`.
    fn function_exists(&self, function_id: &FunctionId) -> bool;

    /// Returns the requested memory registered with the provided [`FunctionId`] (earlier, through
    /// [`Self::register_function`]).
    fn function_memory(&self, function_id: &FunctionId) -> ByteUnit;

    /// Returns [`RegisteredFunction`], a way to access the (runtime-specific)
    /// [`FunctionInfo`] and the [`FunctionAdmissionOverrides`] that have been
    /// registered with the provided [`FunctionId`] (earlier, through
    /// [`Self::register_function`]).
    ///
    /// [`FunctionAdmissionOverrides`]: crate::metadata::registration::FunctionAdmissionOverrides
    fn registered_function(
        &self,
        function_id: &FunctionId,
    ) -> Arc<RegisteredFunction<FunctionInfo>>;

    /// Returns the [`FunctionStats`] associated with the provided [`FunctionId`], as they have
    /// been collected so far.
    fn function_stats(&self, function_id: &FunctionId) -> Arc<FunctionStats>;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Registration
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Store a newly registered Function record.
    ///
    /// Implementations must reject duplicate [`FunctionId`]s.
    fn register_function(
        &self,
        function: RegisteredFunction<FunctionInfo>,
    ) -> Result<(), registration::Error>;
    fn deregister_function(
        &self,
        function_id: &FunctionId,
    ) -> Result<RegisteredFunction<FunctionInfo>, registration::Error>;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Idle Workers
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Search through all stored [`Self::FunctionMetadata`] to return the [`FunctionId`] that this
    /// _Idle_ [`Worker`] is associated with, or `None` if no such _Idle_ [`Worker`] exists.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn find_idle_worker(&self, worker_id: WorkerId) -> Result<Option<FunctionId>, Self::Error>;

    /// Search through all stored [`Self::FunctionMetadata`] to **remove** the provided
    /// [`WorkerId`] among the _Idle_ [`Worker`]s being tracked, returning the [`FunctionId`]
    /// that the _Idle_ [`Worker`] was found to be associated with, or `None` if no such _Idle_
    /// [`Worker`] exists.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn find_remove_idle_worker(
        &self,
        worker_id: WorkerId,
    ) -> Result<Option<FunctionId>, Self::Error>;

    /// **Remove** the _Idle_ [`Worker`] associated with the provided `worker_id` and
    /// `function_id`, returning `true` if that _Idle_ [`Worker`] was indeed found, or
    /// `false` if no such _Idle_ [`Worker`] exists.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn remove_idle_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<bool, Self::Error>;

    /// Store the provided [`WorkerId`] as an _Idle_ [`Worker`] for the provided [`FunctionId`].
    ///
    /// [`Worker`]: crate::worker::Worker
    fn insert_idle_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<(), Self::Error>;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Active Workers
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Search through all stored [`Self::FunctionMetadata`] to return the [`FunctionId`] that this
    /// _Active_ [`Worker`] is associated with, or `None` if no such _Active_ [`Worker`] exists.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn find_active_worker(&self, worker_id: WorkerId) -> Result<Option<FunctionId>, Self::Error>;

    /// Search through all stored [`Self::FunctionMetadata`] to **remove** the provided
    /// [`WorkerId`] among the _Active_ [`Worker`]s being tracked, returning the [`FunctionId`]
    /// that the _Active_ [`Worker`] was found to be associated with, or `None` if no such _Active_
    /// [`Worker`] exists.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn find_remove_active_worker(
        &self,
        worker_id: WorkerId,
    ) -> Result<Option<FunctionId>, Self::Error>;

    /// **Remove** the _Active_ [`Worker`] associated with the provided `worker_id` and
    /// `function_id`, returning `true` if that _Active_ [`Worker`] was indeed found, or
    /// `false` if no such _Active_ [`Worker`] exists.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn remove_active_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<bool, Self::Error>;

    /// Store the provided [`WorkerId`] as an _Active_ [`Worker`] for the provided [`FunctionId`].
    ///
    /// [`Worker`]: crate::worker::Worker
    fn insert_active_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<(), Self::Error>;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Dying Workers
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Search through all stored [`Self::FunctionMetadata`] to return the [`FunctionId`] that this
    /// _Dying_ [`Worker`] is associated with, or `None` if no such _Dying_ [`Worker`] exists.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn find_dying_worker(&self, worker_id: WorkerId) -> Result<Option<FunctionId>, Self::Error>;

    /// Search through all stored [`Self::FunctionMetadata`] to **remove** the provided
    /// [`WorkerId`] among the _Dying_ [`Worker`]s being tracked, returning the [`FunctionId`]
    /// that the _Dying_ [`Worker`] was found to be associated with, or `None` if no such _Dying_
    /// [`Worker`] exists.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn find_remove_dying_worker(
        &self,
        worker_id: WorkerId,
    ) -> Result<Option<FunctionId>, Self::Error>;

    /// **Remove** the _Dying_ [`Worker`] associated with the provided `worker_id` and
    /// `function_id`, returning `true` if that _Dying_ [`Worker`] was indeed found, or
    /// `false` if no such _Dying_ [`Worker`] exists.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn remove_dying_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<bool, Self::Error>;

    /// Store the provided [`WorkerId`] as an _Dying_ [`Worker`] for the provided [`FunctionId`].
    ///
    /// [`Worker`]: crate::worker::Worker
    fn insert_dying_worker(
        &self,
        worker: WorkerId,
        function_id: &FunctionId,
    ) -> Result<(), Self::Error>;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Counters
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Returns the number of _Idle_ [`Worker`]s currently tracked for the
    /// provided `function_id`, or for all Functions if `None` provided.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn num_idle(&self, function_id: Option<&FunctionId>) -> usize;

    /// Returns the number of _Active_ [`Worker`]s currently tracked for the
    /// provided `function_id`, or for all Functions if `None` provided.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn num_active(&self, function_id: Option<&FunctionId>) -> usize;

    /// Returns the number of _Dying_ [`Worker`]s currently tracked for the
    /// provided `function_id`, or for all Functions if `None` provided.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn num_dying(&self, function_id: Option<&FunctionId>) -> usize;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Iterators
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////
    // FIXME(ckatsak): Returning `!Copy` owned types, especially `Arc`'d ones, like `FunctionId`
    // could be too slow for the critical path (e.g., `keepalive::Policy`, `eviction::Policy`,
    // etc). Perhaps returning something like
    // `Iterator<Item = (FunctionId, Iterator<Item = WorkerId>)>` would be preferable, if even
    // possible at all?
    // TODO(ckatsak): Is it worth to change `Box<dyn Iterator>` to `impl Iterator` and statically
    // dispatch store implementations? Or does the performance benefits of static dispatch not
    // worth the added complexity?

    /// Iterate through [`FunctionId`] and [`WorkerId`] tuples of all stored _Idle_ [`Worker`]s
    /// using the returned [`Iterator`].
    ///
    /// [`Worker`]: crate::worker::Worker
    fn iter_idle(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>>;

    /// Iterate through [`FunctionId`] and [`WorkerId`] tuples of all stored _Active_ [`Worker`]s
    /// using the returned [`Iterator`].
    ///
    /// [`Worker`]: crate::worker::Worker
    fn iter_active(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>>;

    /// Iterate through [`FunctionId`] and [`WorkerId`] tuples of all stored _Dying_ [`Worker`]s
    /// using the returned [`Iterator`].
    ///
    /// [`Worker`]: crate::worker::Worker
    fn iter_dying(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>>;

    /// Iterate through [`FunctionId`] and [`WorkerId`] tuples of all stored [`Worker`]s
    /// using the returned [`Iterator`].
    ///
    /// [`Worker`]: crate::worker::Worker
    fn iter_all(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>> {
        Box::new(
            self.iter_idle()
                .chain(self.iter_active())
                .chain(self.iter_dying()),
        )
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Worker state changing methods
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Change the [`Worker`]'s state from _Active_ to _Idle_.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn worker_active_to_idle<'f>(
        &self,
        worker_id: WorkerId,
        function_id: impl Into<Option<&'f FunctionId>>,
    ) -> Result<(), Self::Error>;

    /// Change the [`Worker`]'s state from _Idle_ to _Dying_.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn worker_idle_to_dying<'f>(
        &self,
        worker_id: WorkerId,
        function_id: impl Into<Option<&'f FunctionId>>,
    ) -> Result<(), Self::Error>;

    /// Change the [`Worker`]'s state from _Active_ to _Dying_.
    ///
    /// [`Worker`]: crate::worker::Worker
    fn worker_active_to_dying<'f>(
        &self,
        worker_id: WorkerId,
        function_id: impl Into<Option<&'f FunctionId>>,
    ) -> Result<(), Self::Error>;
}
