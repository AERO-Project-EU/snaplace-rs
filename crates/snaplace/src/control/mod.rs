mod error;
pub mod sandbox;

use redb::Database;
use tokio::{
    net::{TcpListener, UnixListener},
    sync::broadcast,
    task::JoinHandle,
};
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::transport::Server;
use tracing::{error, instrument, warn, Level};
use triomphe::Arc;

use crate::{
    conf::Address,
    control::{
        error::Error,
        sandbox::{pb::sandbox_lifecycle_server::SandboxLifecycleServer, SandboxLifecycleService},
    },
    metadata::registration::{
        pb::function_registration_server::FunctionRegistrationServer, Registrar,
    },
    sbpool::SandboxPoolRef,
    worker::Runtime,
    FunctionMetadataStore,
};

#[derive(Debug)]
pub(crate) struct ControlPlaneServerHandle {
    handle: JoinHandle<Result<(), Error>>,
}

impl ControlPlaneServerHandle {
    pub(crate) async fn reap(self) -> Result<Result<(), Error>, ::tokio::task::JoinError> {
        self.handle.await
    }
}

pub(crate) async fn spawn<Store, Rt>(
    config_rt: &Rt::Config,
    addr: Address,
    pool_ref: SandboxPoolRef,
    store: Arc<Store>,
    db: Arc<Database>,
    mut quit_rx: broadcast::Receiver<()>,
) -> Result<ControlPlaneServerHandle, Error>
where
    Rt: Runtime,
    Store: FunctionMetadataStore<Rt::FunctionInfo>,
{
    let registration = init_registration::<Rt, _>(config_rt, store, db).await?;
    let sandbox_lc = init_sandbox_lifecycle(pool_ref).await;

    let shutdown = async move {
        match quit_rx.recv().await {
            Ok(()) => warn!("Received quit notification!"),
            Err(err) => error!(error = ?err, "Failed to receive from quit channel"),
        }
    };

    let handle =
        ::tokio::spawn(async move { serve(addr, registration, sandbox_lc, shutdown).await });

    Ok(ControlPlaneServerHandle { handle })
}

async fn init_registration<Rt, Store>(
    config_rt: &Rt::Config,
    store: Arc<Store>,
    db: Arc<Database>,
) -> Result<FunctionRegistrationServer<Registrar<Rt, Store>>, Error>
where
    Rt: Runtime,
    Store: FunctionMetadataStore<Rt::FunctionInfo>,
{
    let registrar = Registrar::<Rt, _>::new(store, db, config_rt)
        .await
        .map_err(|err| Error::Registration {
            msg: "Registrar failed to load registered Functions from database".into(),
            source: err,
        })?;
    Ok(FunctionRegistrationServer::new(registrar))
}

async fn init_sandbox_lifecycle(
    pool_ref: SandboxPoolRef,
) -> SandboxLifecycleServer<SandboxLifecycleService> {
    SandboxLifecycleServer::new(SandboxLifecycleService::new(pool_ref))
}

#[instrument(name = "control_plane", level = Level::INFO, skip_all, fields(?address))]
async fn serve<Rt, Store>(
    address: Address,
    registration: FunctionRegistrationServer<Registrar<Rt, Store>>,
    sandbox_lifecycle: SandboxLifecycleServer<SandboxLifecycleService>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Error>
where
    Rt: Runtime,
    Store: FunctionMetadataStore<Rt::FunctionInfo>,
{
    match address {
        Address::Net(addr) => {
            let inc = TcpListenerStream::new(TcpListener::bind(&addr).await.map_err(|err| {
                Error::Io {
                    msg: format!("failed to bind TCP socket to {addr:?}").into_boxed_str(),
                    source: err,
                }
            })?);

            Server::builder()
                .add_service(registration)
                .add_service(sandbox_lifecycle)
                .serve_with_incoming_shutdown(inc, shutdown)
                .await
                .map_err(Error::Tonic)
        }
        Address::Uds(path) => {
            let inc =
                UnixListenerStream::new(UnixListener::bind(&path).map_err(|err| Error::Io {
                    msg: format!("failed to bind Unix socket to {path:?}").into_boxed_str(),
                    source: err,
                })?);

            Server::builder()
                .add_service(registration)
                .add_service(sandbox_lifecycle)
                .serve_with_incoming_shutdown(inc, shutdown)
                .await
                .map_err(Error::Tonic)
        }
    }
}

//fn serve<Rt, Store>(
//    address: Address,
//    registration: FunctionRegistrationServer<Registrar<Rt, Store>>,
//    shutdown: impl Future<Output = ()>,
//) -> Instrumented<impl Future<Output = Result<(), ControlApiError>>>
//where
//    Rt: Runtime,
//    Store: FunctionMetadataStore<Rt::FunctionInfo>,
//{
//    async move {
//        match address {
//            Address::Net(addr) => {
//                let inc =
//                    TcpListenerStream::new(TcpListener::bind(&addr).await.map_err(|err| {
//                        ControlApiError::Io {
//                            msg: format!("failed to bind TCP socket to {addr:?}")
//                                .into_boxed_str(),
//                            source: err,
//                        }
//                    })?);
//
//                Server::builder()
//                    .add_service(registration)
//                    .serve_with_incoming_shutdown(inc, shutdown)
//                    .await
//                    .map_err(ControlApiError::Tonic)
//            }
//            Address::Uds(path) => {
//                let inc =
//                    UnixListenerStream::new(UnixListener::bind(&path).map_err(|err| {
//                        ControlApiError::Io {
//                            msg: format!("failed to bind Unix socket to {path:?}")
//                                .into_boxed_str(),
//                            source: err,
//                        }
//                    })?);
//
//                Server::builder()
//                    .add_service(registration)
//                    .serve_with_incoming_shutdown(inc, shutdown)
//                    .await
//                    .map_err(ControlApiError::Tonic)
//            }
//        }
//    }
//    .instrument(info_span!("control_plane"))
//}
