use dashmap::{DashMap, Entry};
use tracing::{error, instrument, warn, Level};
use triomphe::Arc;

use crate::{
    metadata::{
        fmd::stdhash::StdHashMapFmd,
        registration::{self, RegisteredFunction},
        FunctionInfo, FunctionStats,
    },
    worker::WorkerId,
    FunctionId, FunctionMetadataStore,
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
pub struct DashMapStore<FunctionInfo>(
    DashMap<FunctionId, StdHashMapFmd<FunctionInfo>, crate::BuildHasher>,
);

impl<FunctionInfo> Default for DashMapStore<FunctionInfo> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<FuncInfo: FunctionInfo> DashMapStore<FuncInfo> {
    fn idle_workers_by_function_id(&self) -> Vec<(FunctionId, Vec<WorkerId>)> {
        self.0
            .iter()
            .map(|rm| {
                let (fid, fmd) = rm.pair();
                (fid.clone(), fmd.idle_workers.iter().cloned().collect())
            })
            .collect()
    }

    fn active_workers_by_function_id(&self) -> Vec<(FunctionId, Vec<WorkerId>)> {
        self.0
            .iter()
            .map(|rm| {
                let (fid, fmd) = rm.pair();
                (fid.clone(), fmd.active_workers.iter().cloned().collect())
            })
            .collect()
    }

    fn dying_workers_by_function_id(&self) -> Vec<(FunctionId, Vec<WorkerId>)> {
        self.0
            .iter()
            .map(|rm| {
                let (fid, fmd) = rm.pair();
                (fid.clone(), fmd.dying_workers.iter().cloned().collect())
            })
            .collect()
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

impl<FuncInfo: FunctionInfo> FunctionMetadataStore<FuncInfo> for DashMapStore<FuncInfo> {
    type FunctionMetadata = StdHashMapFmd<FuncInfo>;
    type Error = Error;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // General
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline]
    fn function_exists(&self, function_id: &FunctionId) -> bool {
        self.0.contains_key(function_id)
    }

    #[inline]
    fn function_memory(&self, function_id: &FunctionId) -> ::ubyte::ByteUnit {
        self.0
            .get(function_id)
            .expect("function should already exist")
            .registered_function
            .info()
            .memory()
    }

    #[inline]
    fn registered_function(
        &self,
        function_id: &FunctionId,
    ) -> Arc<registration::RegisteredFunction<FuncInfo>> {
        Arc::clone(
            &self
                .0
                .get(function_id)
                .expect("function should already exist")
                .registered_function,
        )
    }

    #[inline]
    fn function_stats(&self, function_id: &FunctionId) -> Arc<FunctionStats> {
        self.0
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
        match self.0.entry(function.info().id().clone()) {
            Entry::Vacant(entry) => {
                let _entry = entry.insert_entry(StdHashMapFmd::new(function));
                Ok(())
            }
            Entry::Occupied(_) => Err(registration::Error::AlreadyExists(
                function.info().id().clone(),
            )),
        }
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    fn deregister_function(
        &self,
        function_id: &FunctionId,
    ) -> Result<RegisteredFunction<FuncInfo>, registration::Error> {
        let entry = match self.0.entry(function_id.clone()) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(_) => return Err(registration::Error::NotFound(function_id.clone())),
        };

        let workers_count = {
            let fmd = entry.get();
            fmd.active_workers.len() + fmd.idle_workers.len() + fmd.dying_workers.len()
        };
        if workers_count > 0 {
            return Err(registration::Error::InUse {
                fid: function_id.clone(),
                msg: format!("{workers_count} Workers alive").into_boxed_str(),
            });
        }

        let fmd = entry.remove();

        Ok(Arc::unwrap_or_clone(fmd.registered_function))
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Idle Workers
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline]
    fn find_idle_worker(&self, worker_id: WorkerId) -> Result<Option<FunctionId>, Self::Error> {
        Ok(self.0.iter().find_map(|e| {
            let (fid, fmd) = (e.key(), e.value());
            fmd.idle_workers.contains(&worker_id).then_some(fid.clone())
        }))
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
            .0
            .get_mut(function_id)
            .expect("function should already exist")
            .value_mut()
            .idle_workers
            .remove(&worker_id))
    }

    #[inline]
    fn insert_idle_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<(), Self::Error> {
        if !self
            .0
            .get_mut(function_id)
            .expect("function should already exist")
            .value_mut()
            .idle_workers
            .insert(worker_id)
        {
            //unreachable!("DashMap: WorkerId `{worker_id}` collision during insertion!")
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

    fn find_active_worker(&self, worker_id: WorkerId) -> Result<Option<FunctionId>, Self::Error> {
        Ok(self.0.iter().find_map(|e| {
            let (fid, fmd) = (e.key(), e.value());
            fmd.active_workers
                .contains(&worker_id)
                .then_some(fid.clone())
        }))
    }

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

    fn remove_active_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<bool, Self::Error> {
        Ok(self
            .0
            .get_mut(function_id)
            .expect("function should already exist")
            .value_mut()
            .active_workers
            .remove(&worker_id))
    }

    fn insert_active_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<(), Self::Error> {
        if !self
            .0
            .get_mut(function_id)
            .expect("function should already exist")
            .value_mut()
            .active_workers
            .insert(worker_id)
        {
            //unreachable!("DashMap: WorkerId `{worker_id}` collision during insertion!")
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

    fn find_dying_worker(&self, worker_id: WorkerId) -> Result<Option<FunctionId>, Self::Error> {
        Ok(self.0.iter().find_map(|e| {
            let (fid, fmd) = (e.key(), e.value());
            fmd.dying_workers
                .contains(&worker_id)
                .then_some(fid.clone())
        }))
    }

    fn find_remove_dying_worker(
        &self,
        worker_id: WorkerId,
    ) -> Result<Option<FunctionId>, Self::Error> {
        match self.find_dying_worker(worker_id) {
            Ok(Some(function_id)) => match self.remove_dying_worker(worker_id, &function_id) {
                Ok(true) => Ok(Some(function_id)),
                Ok(false) => Ok(None),
                Err(err) => Err(err),
            },
            res_opt => res_opt,
        }
    }

    fn remove_dying_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<bool, Self::Error> {
        Ok(self
            .0
            .get_mut(function_id)
            .expect("function should already exist")
            .value_mut()
            .dying_workers
            .remove(&worker_id))
    }

    fn insert_dying_worker(
        &self,
        worker_id: WorkerId,
        function_id: &FunctionId,
    ) -> Result<(), Self::Error> {
        if !self
            .0
            .get_mut(function_id)
            .expect("function should already exist")
            .value_mut()
            .dying_workers
            .insert(worker_id)
        {
            //unreachable!("DashMap: WorkerId `{worker_id}` collision during insertion!")
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
            || self.0.iter().map(|s| s.idle_workers.len()).sum(),
            |fid| self.0.get(fid).map(|s| s.idle_workers.len()).unwrap_or(0),
        )
    }

    fn num_active(&self, function_id: Option<&FunctionId>) -> usize {
        function_id.map_or_else(
            || self.0.iter().map(|s| s.active_workers.len()).sum(),
            |fid| self.0.get(fid).map(|s| s.active_workers.len()).unwrap_or(0),
        )
    }

    fn num_dying(&self, function_id: Option<&FunctionId>) -> usize {
        function_id.map_or_else(
            || self.0.iter().map(|s| s.dying_workers.len()).sum(),
            |fid| self.0.get(fid).map(|s| s.dying_workers.len()).unwrap_or(0),
        )
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Iterators
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[inline]
    fn iter_idle(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>> {
        //let ret = Box::new(
        //    self.0
        //        .iter()
        //        .flat_map(|e| {
        //            let (fid, fmd) = e.pair();
        //            let ret =
        //                ::std::iter::repeat(fid.clone()).zip(fmd.idle_workers.iter().copied());
        //            //ret.collect::<Vec<_>>().into_iter()
        //            ret;
        //            todo!()
        //        })
        //        .collect::<Vec<_>>()
        //        .into_iter(),
        //);
        //ret

        // FIXME(ckatsak): Too many allocations & atomic ops given that's on the critical path?
        Box::new(
            self.idle_workers_by_function_id()
                .into_iter()
                .flat_map(|(fid, wids)| ::std::iter::repeat(fid.clone()).zip(wids)),
        )
    }

    fn iter_active(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>> {
        // FIXME(ckatsak): Too many allocations & atomic ops given that's on the critical path?
        Box::new(
            self.active_workers_by_function_id()
                .into_iter()
                .flat_map(|(fid, wids)| ::std::iter::repeat(fid.clone()).zip(wids)),
        )
    }

    fn iter_dying(&self) -> Box<dyn Iterator<Item = (FunctionId, WorkerId)>> {
        // FIXME(ckatsak): Too many allocations & atomic ops given that's on the critical path?
        Box::new(
            self.dying_workers_by_function_id()
                .into_iter()
                .flat_map(|(fid, wids)| ::std::iter::repeat(fid.clone()).zip(wids)),
        )
    }

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

        match self.0.entry(function_id.clone()) {
            ::dashmap::Entry::Occupied(mut e) => {
                let fmd = e.get_mut();
                if !fmd.active_workers.remove(&worker_id) {
                    Err(Error::WorkerNotFound {
                        worker_id,
                        msg: Some("no such Active Worker".into()),
                    })
                } else {
                    if !fmd.idle_workers.insert(worker_id) {
                        // ¿TODO: For now, let's just tear everything down if it happens?
                        Self::collision(worker_id, &function_id)?;
                    }
                    Ok(())
                }
            }
            ::dashmap::Entry::Vacant(_) => Err(Error::FunctionNotFound(function_id.clone())),
        }
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

        match self.0.entry(function_id.clone()) {
            ::dashmap::Entry::Occupied(mut e) => {
                let fmd = e.get_mut();
                if !fmd.idle_workers.remove(&worker_id) {
                    Err(Error::WorkerNotFound {
                        worker_id,
                        msg: Some("no such Idle Worker".into()),
                    })
                } else {
                    if !fmd.dying_workers.insert(worker_id) {
                        // ¿TODO: For now, let's just tear everything down if it happens?
                        Self::collision(worker_id, &function_id)?;
                    }
                    Ok(())
                }
            }
            ::dashmap::Entry::Vacant(_) => Err(Error::FunctionNotFound(function_id.clone())),
        }
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

        match self.0.entry(function_id.clone()) {
            ::dashmap::Entry::Occupied(mut e) => {
                let fmd = e.get_mut();
                if !fmd.active_workers.remove(&worker_id) {
                    Err(Error::WorkerNotFound {
                        worker_id,
                        msg: Some("no such Active Worker".into()),
                    })
                } else {
                    if !fmd.dying_workers.insert(worker_id) {
                        // ¿TODO: For now, let's just tear everything down if it happens?
                        Self::collision(worker_id, &function_id)?;
                    }
                    Ok(())
                }
            }
            ::dashmap::Entry::Vacant(_) => Err(Error::FunctionNotFound(function_id.clone())),
        }
    }
}
