use std::marker::PhantomData;

use async_trait::async_trait;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, instrument, trace, Level};
use triomphe::Arc;

use crate::{
    metadata::{
        db::Tables,
        registration::{
            pb::{
                function_registration_server::FunctionRegistration, DeregisterFunctionRequest,
                DeregisterFunctionResponse, ListFunctionsRequest, ListFunctionsResponse,
                RegisterFunctionRequest, RegisterFunctionResponse,
            },
            RegisteredFunction, MAX_ALLOWED_FUNCTION_MEM,
        },
        FunctionInfo,
    },
    worker, FunctionId, FunctionMetadataStore,
};

#[derive(Debug)]
pub struct Registrar<Rt: worker::Runtime, Store> {
    store: Arc<Store>,
    db: Arc<Database>,
    runtime_config: Rt::Config,

    _phantom: PhantomData<fn() -> Rt>,
}

impl<Rt, Store> Registrar<Rt, Store>
where
    Rt: worker::Runtime,
    Store: FunctionMetadataStore<Rt::FunctionInfo>,
{
    /// Construct and initialize a new `Registrar` instrance, including checking the database for
    /// any registered Functions stored, and registering them with both [`FunctionMetadataStore`]
    /// and the underlying [`Runtime`].
    ///
    /// [`Runtime`]: worker::Runtime
    pub async fn new(
        store: Arc<Store>,
        db: Arc<Database>,
        runtime_config: &Rt::Config,
    ) -> Result<Self, super::Error> {
        let registrar = Self {
            store,
            db,
            runtime_config: runtime_config.clone(),
            _phantom: PhantomData,
        };

        registrar.load_from_db().await?;

        Ok(registrar)
    }

    /// Check the database for any registered Functions stored, and register them with
    /// [`FunctionMetadataStore`] and the underlying [`Runtime`] as well.
    ///
    /// [`Runtime`]: worker::Runtime
    async fn load_from_db(&self) -> Result<(), super::Error> {
        let rtxn = self.db.begin_read().map_err(|err| super::Error::Database {
            msg: "failed to begin read txn".into(),
            err: err.into(),
        })?;

        let funcs_tbl = match rtxn.open_table(Tables::<Rt>::FUNCTIONS) {
            Ok(tbl) => tbl,
            Err(::redb::TableError::TableDoesNotExist(name)) => {
                info!("No table '{name}' exists; no registered Functions to load from database");
                return Ok(());
            }
            Err(err) => {
                return Err(super::Error::Database {
                    msg: "failed to open table FUNCTIONS".into(),
                    err: err.into(),
                })
            }
        };
        let num_funcs = funcs_tbl.len().map_err(|err| super::Error::Database {
            msg: "failed to retrieve length of table FUNCTIONS".into(),
            err: err.into(),
        })?;
        info!(
            "Found {num_funcs} registered Function{} stored in the database",
            if num_funcs == 1 { "" } else { "s" }
        );

        for res_func in funcs_tbl.iter().map_err(|err| super::Error::Database {
            msg: "failed to iterate through table FUNCTIONS".into(),
            err: err.into(),
        })? {
            let (_fid, mut func) =
                res_func
                    .map(|(k, v)| (k.value(), v.value()))
                    .map_err(|err| super::Error::Database {
                        msg: "failed to read Function from database".into(),
                        err: err.into(),
                    })?;
            // Register Function with Runtime.
            if let Err(err) = Rt::register_function(&self.runtime_config, &mut func.info).await {
                error!(
                    error = ?err, function.info = ?func.info,
                    "Runtime failed to register stored Function: {err:#}",
                );
                return Err(err);
            };
            // Store FunctionInfo in FunctionMetadataStore.
            self.store.register_function(func)?;
        }

        Ok(())
    }

    fn store_to_db(
        db: Arc<Database>,
        func: &RegisteredFunction<Rt::FunctionInfo>,
    ) -> Result<(), super::Error> {
        let wtxn = db.begin_write().map_err(|err| {
            error!(error = ?err, function = ?func, "Failed to open write txn: {err:#}");
            super::Error::Database {
                msg: "failed to open write txn".into(),
                err: err.into(),
            }
        })?;
        {
            let mut tbl =
                wtxn.open_table(Tables::<Rt>::FUNCTIONS)
                    .map_err(|err| super::Error::Database {
                        msg: "failed to open table FUNCTIONS".into(),
                        err: err.into(),
                    })?;
            if let Some(old_func) =
                tbl.insert(func.info.id(), func)
                    .map_err(|err| super::Error::Database {
                        msg: "failed to insert new Function to the database".into(),
                        err: err.into(),
                    })?
            {
                // NOTE(ckatsak): Currently, this should be unreachable, as we check for it earlier
                let old_func = old_func.value();
                error!(old.function = ?old_func, new.function = ?func, "Overwriting Function");
            };
        }
        wtxn.commit().map_err(|err| super::Error::Database {
            msg: "failed to commit txn to table FUNCTIONS".into(),
            err: err.into(),
        })
    }

    fn delete_from_db(db: Arc<Database>, fid: &FunctionId) -> Result<(), super::Error> {
        let wtxn = db.begin_write().map_err(|err| {
            error!(error = ?err, function.id = ?fid, "Failed to open write txn: {err:#}");
            super::Error::Database {
                msg: "failed to open write txn".into(),
                err: err.into(),
            }
        })?;
        {
            let mut tbl =
                wtxn.open_table(Tables::<Rt>::FUNCTIONS)
                    .map_err(|err| super::Error::Database {
                        msg: "failed to open table FUNCTIONS".into(),
                        err: err.into(),
                    })?;
            let _db_fi = tbl.remove(fid).map_err(|err| super::Error::Database {
                msg: "failed to remove new Function to the database".into(),
                err: err.into(),
            })?;
        }
        wtxn.commit().map_err(|err| super::Error::Database {
            msg: "failed to commit txn to table FUNCTIONS".into(),
            err: err.into(),
        })
    }
}

#[async_trait]
impl<Runtime, Store> FunctionRegistration for Registrar<Runtime, Store>
where
    Runtime: worker::Runtime,
    Store: FunctionMetadataStore<Runtime::FunctionInfo>,
{
    #[instrument(level = Level::INFO, skip_all, fields(function.id = ::tracing::field::Empty))]
    async fn register_function(
        &self,
        request: ::tonic::Request<RegisterFunctionRequest>,
    ) -> Result<::tonic::Response<RegisterFunctionResponse>, ::tonic::Status> {
        let (md, ext, req) = request.into_parts();
        trace!(request = ?req, metadata_map = ?md, extensions = ?ext);

        // TODO(ckatsak): move this right before Runtime registration?
        let admission = req
            .admission
            .as_ref()
            .map(TryInto::try_into)
            .transpose()
            .map_err(|err| {
                error!(
                    error = ?err, ?req,
                    "error parsing submitted admission overrides for Function: {err:#}"
                );
                ::tonic::Status::invalid_argument(format!(
                    "error parsing submitted admission overrides for Function: {err:#} in {req:?}",
                ))
            })?;

        // TODO(ckatsak): Does this really need to take ownership?
        let mut fi = Runtime::FunctionInfo::try_from(req).map_err(|err| {
            error!(
                error = ?err,
                "Failed to convert RegisterFunctionRequest -> Runtime::FunctionInfo: {err:#}",
            );
            ::tonic::Status::invalid_argument(err.to_string())
        })?;
        trace!(function_info = ?fi);
        ::tracing::Span::current().record("function.id", fi.id().as_str());

        // Fail fast if the Function is already registered; do not overwrite.
        if self.store.function_exists(fi.id()) {
            // TODO(ckatsak): Maybe check whether FunctionInfo remains unchanged, to return Ok(..)?
            error!("Function already exists; rejecting registration request");
            return Err(::tonic::Status::from(super::Error::AlreadyExists(
                fi.id().clone(),
            )));
        }

        // Validate requested memory.
        let mem_req = fi.memory();
        if mem_req > MAX_ALLOWED_FUNCTION_MEM {
            return Err(::tonic::Status::invalid_argument(format!(
                "memory requested = {mem_req} > {MAX_ALLOWED_FUNCTION_MEM} = max allowed memory",
            )));
        }

        // Register Function with Runtime.
        Runtime::register_function(&self.runtime_config, &mut fi).await.inspect_err(|err|
            error!(error = ?err, function.info = ?fi, "Runtime failed to register new Function: {err:#}")
        )?;

        ::tokio::task::spawn_blocking({
            let db = Arc::clone(&self.db);
            let store = Arc::clone(&self.store);

            move || {
                let func = RegisteredFunction {
                    info: fi,
                    admission,
                };
                // Store FunctionInfo in database
                Self::store_to_db(db, &func).inspect_err(
                    |err| error!(error = ?err, "Failed to store new Function to database: {err:#}"),
                )?;
                // Store FunctionInfo in FunctionMetadataStore.  NOTE(ckatsak): This
                // enables invocations of the new Function even before this RPC returns.
                store.register_function(func).inspect_err(|err| error!(
                    error = ?err,
                    "Failed to store newly registered Function in FunctionMetadataStore: {err:#}",
                ))
            }
        })
        .await
        .map_err(|err| ::tonic::Status::internal(format!("error joining tokio task: {err:#}")))??;

        Ok(::tonic::Response::new(RegisterFunctionResponse {}))
    }

    #[instrument(level = Level::INFO, skip_all, fields(function.id = ::tracing::field::Empty))]
    async fn deregister_function(
        &self,
        request: ::tonic::Request<DeregisterFunctionRequest>,
    ) -> Result<::tonic::Response<DeregisterFunctionResponse>, ::tonic::Status> {
        let (md, ext, req) = request.into_parts();
        trace!(request = ?req, metadata_map = ?md, extensions = ?ext);
        let function_id = FunctionId::from(req.function_id);
        ::tracing::Span::current().record("function.id", function_id.as_str());

        // NOTE(ckatsak): Currently, Function deregistration is not as simple as registration.
        // - Since SandboxPool (really, all actors that read/write from/to FunctionMetadataStore)
        //   does not employ some sort of invocation-wide transaction to do that (other than (too)
        //   fine-grained locks), removing a Function from FunctionMetadataStore could catch some
        //   invocation of that Function amid completion. Currently, this would cause panics when
        //   interacting with data from FunctionMetadataStore. Even if these panics are refactored
        //   to errors, I think that invocations underway ought to be completed unobtrusively.
        // - Apart from FunctionMetadataStore, there are other places where per-Function data are
        //   retained; e.g., SandboxPool's snapshots (which correspond to Sandboxes that have to
        //   be destroyed upon deregistration, which would probably be a significant overhead for
        //   SandboxPool to handle in the critical path), MetricsCollector's hashmap, etc. There
        //   is no straightforward way to inform all these of the deregistration, for now.
        // - I believe that the first step would be to mark a Function as "deregistered" in
        //   FunctionMetadataStore, so that no more new incoming invocation requests are handled
        //   for that Function by SandboxPool. Later (or maybe even at the next boot, assuming
        //   registered functions are persisted across boots), when the Function to-be-deregistered
        //   is cold, some sort of garbage-collecting actor could notify everyone to safely remove
        //   their entries for all "deregistered" Functions.
        // FIXME(ckatsak): For the above reasons, the following implementation is wrong/incomplete,
        // and works only partially; i.e.:
        // - it leaves uncollected garbage data scattered across all actors behind it,
        // - it panics; to avoid that, the system must be completely cold.

        let func = ::tokio::task::spawn_blocking({
            let store = Arc::clone(&self.store);
            let db = Arc::clone(&self.db);

            move || {
                // Remove from Store.  NOTE(ckatsak): This also prevents
                // any invocation that may occur before this RPC returns.
                let func = store.deregister_function(&function_id).inspect_err(|err| {
                    error!(
                        error = ?err,
                        "Failed to remove registered Function from FunctionMetadataStore: {err:#}",
                    )
                })?;

                // Remove from database.  NOTE(ckatsak): As per the note in the beginning of this
                // method, this should probably not happen here, but at a garbage collection phase.
                Self::delete_from_db(db, func.info.id()).inspect_err(|err| {
                    error!(
                        error = ?err, ?func, "Failed to delete registered Function from database: {err:#}"
                    )
                })?;

                Ok::<_, super::Error>(func)
            }
        })
        .await
        .map_err(|err| ::tonic::Status::internal(format!("error joining tokio task: {err:#}")))??;

        // Deregister from Runtime
        if let Err(err) = Runtime::deregister_function(&self.runtime_config, &func.info).await {
            error!(error = ?err, registered.function = ?func, "Runtime failed to deregister Function: {err:#}")
        }

        Ok(::tonic::Response::new(DeregisterFunctionResponse {}))
    }

    type ListFunctionsStream = ReceiverStream<Result<ListFunctionsResponse, ::tonic::Status>>;

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn list_functions(
        &self,
        request: ::tonic::Request<ListFunctionsRequest>,
    ) -> Result<::tonic::Response<Self::ListFunctionsStream>, ::tonic::Status> {
        let (md, ext, req) = request.into_parts();
        trace!(request = ?req, metadata_map = ?md, extensions = ?ext);

        // TODO
        Err(::tonic::Status::unimplemented("TODO"))
    }
}
