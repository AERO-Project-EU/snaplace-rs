use std::{
    collections::HashMap,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use tracing::{error, instrument, warn, Level};
use triomphe::Arc;

use crate::{
    metadata::{
        fmd::stdhash::StdHashMapFmd,
        registration::{self, RegisteredFunction},
        FunctionInfo, FunctionMetadataStore, FunctionStats,
    },
    worker::WorkerId,
    FunctionId,
};

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("existing Worker's ID collides with new one's (WorkerId: `{0}`)")]
    WorkerIdCollision(WorkerId),

    #[error("Worker `{worker_id}` not found ({msg:?})")]
    WorkerNotFound {
        worker_id: WorkerId,
        msg: Option<Box<str>>,
    },

    #[error("FunctionId `{0}` not found in Store")]
    FunctionNotFound(FunctionId),
}

#[derive(Debug)]
pub struct StdHashMapFmdStore<FunctionInfo>(
    RwLock<HashMap<FunctionId, StdHashMapFmd<FunctionInfo>, crate::BuildHasher>>,
);

impl<FunctionInfo> Default for StdHashMapFmdStore<FunctionInfo> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<FunctionInfo> StdHashMapFmdStore<FunctionInfo> {
    #[inline(always)]
    fn read(
        &self,
    ) -> RwLockReadGuard<'_, HashMap<FunctionId, StdHashMapFmd<FunctionInfo>, crate::BuildHasher>>
    {
        self.0.read().expect("store lock should not be poisoned")
    }

    #[inline(always)]
    fn write(
        &self,
    ) -> RwLockWriteGuard<'_, HashMap<FunctionId, StdHashMapFmd<FunctionInfo>, crate::BuildHasher>>
    {
        self.0.write().expect("store lock should not be poisoned")
    }

    #[instrument(level = Level::WARN, skip_all)]
    #[cold]
    #[inline(never)]
    // FIXME: This is only called when first inserting a new Worker (i.e., an _Active_ one);
    // however it may occur on transition as well (i.e., when changing an _Active_ Worker to
    // _Idle_, etc), but it is not checked accordingly. So: either call it whenever it should
    // be called, or just remove this and handle collisions differently.
    fn collision(worker_id: WorkerId, function_id: &FunctionId) -> Result<(), Error> {
        error!("Existing active Worker (ID: `{worker_id:?}`; Function: `{function_id}`) collides with new one");
        Err(Error::WorkerIdCollision(worker_id))
    }
}

impl<FuncInfo: FunctionInfo> FunctionMetadataStore<FuncInfo> for StdHashMapFmdStore<FuncInfo> {
    type FunctionMetadata = StdHashMapFmd<FuncInfo>;
    type Error = Error;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // General
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline]
    fn function_exists(&self, function_id: &FunctionId) -> bool {
        self.read().contains_key(function_id)
    }

    #[inline]
    fn function_memory(&self, function_id: &FunctionId) -> ::ubyte::ByteUnit {
        self.read()
            .get(function_id)
            .expect("function should already exist")
            .registered_function
            .info()
            .memory()
    }

    #[inline]
    fn registered_function(&self, function_id: &FunctionId) -> Arc<RegisteredFunction<FuncInfo>> {
        Arc::clone(
            &self
                .read()
                .get(function_id)
                .expect("function should already exist")
                .registered_function,
        )
    }

    #[inline]
    fn function_stats(&self, function_id: &FunctionId) -> Arc<FunctionStats> {
        self.read()
            .get(function_id)
            .expect("function should already exist")
            .stats
            .clone()
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Registration
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[instrument(level = Level::DEBUG, skip_all)]
    fn register_function(
        &self,
        function: RegisteredFunction<FuncInfo>,
    ) -> Result<(), registration::Error> {
        let id = function.info().id().clone();
        let mut store = self.write();

        if store.contains_key(&id) {
            return Err(registration::Error::AlreadyExists(id));
        }
        let none = store.insert(id, StdHashMapFmd::new(function));
        debug_assert!(none.is_none());

        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    fn deregister_function(
        &self,
        function_id: &FunctionId,
    ) -> Result<RegisteredFunction<FuncInfo>, registration::Error> {
        let mut store = self.write();

        let workers_count = {
            let fmd = store
                .get(function_id)
                .ok_or_else(|| registration::Error::NotFound(function_id.clone()))?;
            fmd.active_workers.len() + fmd.idle_workers.len() + fmd.dying_workers.len()
        };
        if workers_count > 0 {
            return Err(registration::Error::InUse {
                fid: function_id.clone(),
                msg: format!("{workers_count} Workers alive").into_boxed_str(),
            });
        }

        let fmd = store
            .remove(function_id)
            .expect("function should already exist"); // SAFETY: we just read it holding RW lock

        Ok(Arc::unwrap_or_clone(fmd.registered_function))
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Idle Workers
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline]
    fn find_idle_worker(&self, worker_id: WorkerId) -> Result<Option<FunctionId>, Self::Error> {
        Ok(self
            .read()
            .iter()
            .find_map(|(fid, fmd)| fmd.idle_workers.contains(&worker_id).then_some(fid.clone())))
    }

    #[inline]
    fn find_remove_idle_worker(
        &self,
        worker_id: WorkerId,
    ) -> Result<Option<FunctionId>, Self::Error> {
        match self.find_idle_worker(worker_id) {
            Ok(Some(function_id)) => match self.remove_idle_worker(worker_id, &function_id) {
                Ok(true) => Ok(Some(function_id)),
                Ok(false) => Ok(None),
                Err(err) => Err(err),
            },
            res_opt => res_opt,
        }
    }

    #[inline]
    fn remove_idle_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<bool, Self::Error> {
        Ok(self
            .write()
            .get_mut(function_id)
            .expect("function should already exist")
            .idle_workers
            .remove(&worker_id))
    }

    /// Store the provided [`WorkerId`] as an _Idle_ [`Worker`] for [`FunctionId`].
    ///
    /// # Error
    ///
    /// In case of [`WorkerId`] collision.
    ///
    /// # Old Note
    ///
    /// This is *extremely* unlikely using UUIDv4: it is supposed to provide sufficient randomness.
    /// If such a collision does occur though, for now, we do not handle it at all: we leak the old
    /// `Worker` along with all of its resources (including any `Sb` or persisted snapshot), hence
    /// we're fucked :)
    ///
    /// [`Worker`]: crate::worker::Worker
    #[inline]
    fn insert_idle_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<(), Self::Error> {
        if !self
            .write()
            .get_mut(function_id)
            .expect("function should already exist")
            .idle_workers
            .insert(worker_id)
        {
            // ¿TODO: For now, let's just tear everything down if it happens?
            Self::collision(worker_id, function_id)?;
        }
        Ok(())
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Active Workers
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline]
    fn find_active_worker(&self, worker_id: WorkerId) -> Result<Option<FunctionId>, Self::Error> {
        Ok(self.read().iter().find_map(|(fid, fmd)| {
            fmd.active_workers
                .contains(&worker_id)
                .then_some(fid.clone())
        }))
    }

    #[inline]
    fn find_remove_active_worker(
        &self,
        worker_id: WorkerId,
    ) -> Result<Option<FunctionId>, Self::Error> {
        match self.find_active_worker(worker_id) {
            Ok(Some(function_id)) => match self.remove_active_worker(worker_id, &function_id) {
                Ok(true) => Ok(Some(function_id)),
                Ok(false) => Ok(None),
                Err(err) => Err(err),
            },
            res_opt => res_opt,
        }
    }

    #[inline]
    fn remove_active_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<bool, Self::Error> {
        Ok(self
            .write()
            .get_mut(function_id)
            .expect("function should already exist")
            .active_workers
            .remove(&worker_id))
    }

    /// Store the provided [`WorkerId`] as an _Active_ [`Worker`] for [`FunctionId`].
    ///
    /// # Error
    ///
    /// In case of [`WorkerId`] collision.
    ///
    /// # Old Note
    ///
    /// This is *extremely* unlikely using UUIDv4: it is supposed to provide sufficient randomness.
    /// If such a collision does occur though, for now, we do not handle it at all: we leak the old
    /// `Worker` along with all of its resources (including any `Sb` or persisted snapshot), hence
    /// we're fucked :)
    ///
    /// [`Worker`]: crate::worker::Worker
    #[inline]
    fn insert_active_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<(), Self::Error> {
        if !self
            .write()
            .get_mut(function_id)
            .expect("function should already exist")
            .active_workers
            .insert(worker_id)
        {
            // ¿TODO: For now, let's just tear everything down if it happens?
            Self::collision(worker_id, function_id)?;
        }
        Ok(())
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Dying Workers
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline]
    fn find_dying_worker(&self, worker_id: WorkerId) -> Result<Option<FunctionId>, Self::Error> {
        Ok(self.read().iter().find_map(|(fid, fmd)| {
            fmd.dying_workers
                .contains(&worker_id)
                .then_some(fid.clone())
        }))
    }

    #[inline]
    fn find_remove_dying_worker(
        &self,
        worker_id: WorkerId,
    ) -> Result<Option<FunctionId>, Self::Error> {
        Ok(self
            .write()
            .iter_mut()
            .find_map(|(fid, fmd)| fmd.dying_workers.remove(&worker_id).then_some(fid.clone())))
    }

    #[inline]
    fn remove_dying_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<bool, Self::Error> {
        Ok(self
            .write()
            .get_mut(function_id)
            .expect("function should already exist")
            .dying_workers
            .remove(&worker_id))
    }

    /// Store the provided [`WorkerId`] as an _Dying_ [`Worker`] for [`FunctionId`].
    ///
    /// # Error
    ///
    /// In case of [`WorkerId`] collision.
    ///
    /// # Old Note
    ///
    /// This is *extremely* unlikely using UUIDv4: it is supposed to provide sufficient randomness.
    /// If such a collision does occur though, for now, we do not handle it at all: we leak the old
    /// `Worker` along with all of its resources (including any `Sb` or persisted snapshot), hence
    /// we're fucked :)
    ///
    /// [`Worker`]: crate::worker::Worker
    #[inline]
    fn insert_dying_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<(), Self::Error> {
        if !self
            .write()
            .get_mut(function_id)
            .expect("function should already exist")
            .dying_workers
            .insert(worker_id)
        {
            // ¿TODO: For now, let's just tear everything down if it happens?
            Self::collision(worker_id, function_id)?;
        }
        Ok(())
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Counters
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    fn num_idle(&self, function_id: Option<&FunctionId>) -> usize {
        function_id.map_or_else(
            || self.read().values().map(|s| s.idle_workers.len()).sum(),
            |fid| {
                self.read()
                    .get(fid)
                    .map(|s| s.idle_workers.len())
                    .unwrap_or(0)
            },
        )
    }

    fn num_active(&self, function_id: Option<&FunctionId>) -> usize {
        function_id.map_or_else(
            || self.read().values().map(|s| s.active_workers.len()).sum(),
            |fid| {
                self.read()
                    .get(fid)
                    .map(|s| s.active_workers.len())
                    .unwrap_or(0)
            },
        )
    }

    fn num_dying(&self, function_id: Option<&FunctionId>) -> usize {
        function_id.map_or_else(
            || self.read().values().map(|s| s.dying_workers.len()).sum(),
            |fid| {
                self.read()
                    .get(fid)
                    .map(|s| s.dying_workers.len())
                    .unwrap_or(0)
            },
        )
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Iterators
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline]
    fn iter_idle(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>> {
        Box::new(
            self.read()
                .iter()
                .flat_map(|(function_id, fmd)| {
                    ::std::iter::repeat(function_id.clone()).zip(fmd.idle_workers.iter().copied())
                })
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn iter_active(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>> {
        Box::new(
            self.read()
                .iter()
                .flat_map(|(function_id, fmd)| {
                    ::std::iter::repeat(function_id.clone()).zip(fmd.active_workers.iter().copied())
                })
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn iter_dying(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>> {
        Box::new(
            self.read()
                .iter()
                .flat_map(|(function_id, fmd)| {
                    ::std::iter::repeat(function_id.clone()).zip(fmd.dying_workers.iter().copied())
                })
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn iter_all(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>> {
        let m = self.read();
        Box::new(
            m.iter()
                .flat_map(|(fid, fmd)| {
                    ::std::iter::repeat(fid.clone()).zip(fmd.idle_workers.iter().copied())
                })
                .chain(m.iter().flat_map(|(fid, fmd)| {
                    ::std::iter::repeat(fid.clone()).zip(fmd.active_workers.iter().copied())
                }))
                .chain(m.iter().flat_map(|(fid, fmd)| {
                    ::std::iter::repeat(fid.clone()).zip(fmd.dying_workers.iter().copied())
                }))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Worker state changing methods
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline]
    fn worker_active_to_idle<'f>(
        &self,
        worker_id: WorkerId,
        function_id: impl Into<Option<&'f FunctionId>>,
    ) -> Result<(), Self::Error> {
        // First, make sure we have a FunctionId (looking up for it if we don't):
        let function_id = function_id.into().map_or_else(
            || {
                self.find_active_worker(worker_id)
                    .expect("StdHashMapFmdStore::find_active_worker() never fails")
                    .ok_or_else(
                        #[cold]
                        || Error::WorkerNotFound {
                            worker_id,
                            msg: Some("no such Active Worker".into()),
                        },
                    )
            },
            |fid| Ok(fid.clone()),
        )?;
        let mut m = self.write();
        // Alternatively, we can look up for it while holding the write lock:
        //let function_id = function_id
        //    .into()
        //    .map_or_else(
        //        || {
        //            m.iter()
        //                .find_map(|(fid, fmd)| {
        //                    fmd.active_workers.contains(&worker_id).then_some(fid)
        //                })
        //                .ok_or_else(
        //                    #[cold]
        //                    || Error::WorkerNotFound {
        //                        worker_id,
        //                        msg: Some("no such Active Worker".into()),
        //                    },
        //                )
        //        },
        //        Ok,
        //    )?
        //    .clone();

        // At this point, we know the FunctionId and we hold the write lock
        if !m
            .get_mut(&function_id)
            .ok_or_else(
                #[cold]
                || Error::FunctionNotFound(function_id.clone()),
            )?
            .active_workers
            .remove(&worker_id)
        {
            return Err(Error::WorkerNotFound {
                worker_id,
                msg: Some("no such Active Worker".into()),
            });
        }
        if !m
            .get_mut(&function_id)
            //.ok_or_else(#[cold] || Error::FunctionNotFound(function_id.clone()))?
            .expect("function should already exist")
            .idle_workers
            .insert(worker_id)
        {
            // ¿TODO: For now, let's just tear everything down if it happens?
            Self::collision(worker_id, &function_id)?;
        }
        Ok(())
    }

    #[inline]
    fn worker_idle_to_dying<'f>(
        &self,
        worker_id: WorkerId,
        function_id: impl Into<Option<&'f FunctionId>>,
    ) -> Result<(), Self::Error> {
        // First, make sure we have a FunctionId (looking up for it if we don't):
        let function_id = function_id.into().map_or_else(
            || {
                self.find_idle_worker(worker_id)
                    .expect("StdHashMapFmdStore::find_idle_worker() never fails")
                    .ok_or_else(
                        #[cold]
                        || Error::WorkerNotFound {
                            worker_id,
                            msg: Some("no such Idle Worker".into()),
                        },
                    )
            },
            |fid| Ok(fid.clone()),
        )?;

        let mut m = self.write();
        // At this point, we know the FunctionId and we hold the write lock
        if !m
            .get_mut(&function_id)
            .ok_or_else(
                #[cold]
                || Error::FunctionNotFound(function_id.clone()),
            )?
            .idle_workers
            .remove(&worker_id)
        {
            return Err(Error::WorkerNotFound {
                worker_id,
                msg: Some("no such Idle Worker".into()),
            });
        }
        if !m
            .get_mut(&function_id)
            .expect("function should already exist")
            .dying_workers
            .insert(worker_id)
        {
            // ¿TODO: For now, let's just tear everything down if it happens?
            Self::collision(worker_id, &function_id)?;
        }
        Ok(())
    }

    #[inline]
    fn worker_active_to_dying<'f>(
        &self,
        worker_id: WorkerId,
        function_id: impl Into<Option<&'f FunctionId>>,
    ) -> Result<(), Self::Error> {
        // First, make sure we have a FunctionId (looking up for it if we don't):
        let function_id = function_id.into().map_or_else(
            || {
                self.find_active_worker(worker_id)
                    .expect("StdHashMapFmdStore::find_active_worker() never fails")
                    .ok_or_else(
                        #[cold]
                        || Error::WorkerNotFound {
                            worker_id,
                            msg: Some("no such Active Worker".into()),
                        },
                    )
            },
            |fid| Ok(fid.clone()),
        )?;

        let mut m = self.write();
        // At this point, we know the FunctionId and we hold the write lock
        if !m
            .get_mut(&function_id)
            .ok_or_else(
                #[cold]
                || Error::FunctionNotFound(function_id.clone()),
            )?
            .active_workers
            .remove(&worker_id)
        {
            return Err(Error::WorkerNotFound {
                worker_id,
                msg: Some("no such Active Worker".into()),
            });
        }
        if !m
            .get_mut(&function_id)
            .expect("function should already exist")
            .dying_workers
            .insert(worker_id)
        {
            // ¿TODO: For now, let's just tear everything down if it happens?
            Self::collision(worker_id, &function_id)?;
        }
        Ok(())
    }
}
