use std::num::ParseIntError;

use triomphe::Arc;

use crate::{
    network,
    worker::{self, WorkerId},
    FunctionId, SandboxId,
};

pub(super) type Result<T> = ::std::result::Result<T, self::Error>;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("initialization error: {msg}")]
    Init {
        msg: Box<str>,
        #[source]
        source: Option<Box<Self>>,
    },

    /// Failure related to `cpuset`.
    #[error("cpuset error")]
    CpuSet(#[source] CpuSetError),

    #[error("failed to contact AdmissionController: {0}")]
    AdmissionUnresponsive(Box<str>),

    #[error("failed to spawn new Worker")]
    WorkerCreation(#[source] worker::Error),

    #[error("error while manipulating metadata")]
    Metadata(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),

    #[error("keepalive timer for WorkerId `{0}` timed out trying to notify SandboxPool")]
    KeepAliveTimerSendTimeout(WorkerId),

    /// A catch-all variant for [`worker::Error`]s.
    #[error(transparent)]
    Worker(#[from] worker::Error),

    #[error("failed to join Worker `{0}`")]
    WorkerJoin(WorkerId),

    #[error("networking error: {msg}")]
    Network {
        msg: Box<str>,
        #[source]
        source: network::Error,
    },

    #[error("runtime error: {msg}")]
    Runtime {
        msg: Box<str>,
        #[source]
        source: Box<dyn ::std::error::Error + Send + Sync + 'static>,
    },

    #[error("database error during SandboxPool operations: {msg}")]
    Database {
        msg: Box<str>,
        #[source]
        source: ::redb::Error,
    },
}

#[derive(Debug, ::thiserror::Error)]
pub enum CpuSetError {
    #[error("failed to parse cpuset range: {msg}")]
    ParseCpuRange {
        msg: Box<str>,
        #[source]
        source: Option<ParseIntError>,
    },
    #[error("no cpuset allocation tracked for Worker {0}")]
    AllocationNotFound(WorkerId),
}

#[derive(Debug, ::thiserror::Error)]
pub enum PrepareSandboxError {
    #[error("cannot prepare Sandbox for unknown Function '{0}'")]
    UnknownFunction(FunctionId), // gRPC: `NotFound`

    #[error("no snapshot currently available")]
    OutOfSnapshots, // gRPC: `ResourceExhausted`

    #[error("not enough free memory")]
    OutOfMemory, // gRPC: `ResourceExhausted`

    #[error("internal error: {msg}")] // gRPC: `Internal`
    Internal {
        msg: Box<str>,
        #[source]
        err: Option<Error>,
    },

    #[error("Worker reported failure to prepare sandbox: {0}")]
    Worker(Box<str>), // gRPC: `Internal`
}

#[derive(Debug, ::thiserror::Error)]
pub enum DestroySandboxError {
    #[error("Sandbox '{0}' not found")]
    NotFound(SandboxId), // gRPC: `NotFound`

    #[error("Sandbox is currently busy: {0}")]
    Busy(Box<str>), // gRPC: `Unavailable`

    #[error("internal error: {msg}")] // gRPC: `Internal`
    Internal {
        msg: Box<str>,
        #[source]
        err: Option<Error>,
    },

    #[error("Worker reported failure to destroy sandbox: {0}")]
    Worker(Box<str>), // gRPC: `Internal`
}

#[derive(Debug, ::thiserror::Error)]
pub enum ListSandboxesError {
    // TODO
}

#[derive(Debug, Clone, ::thiserror::Error)]
pub enum CreateSnapshotError {
    #[error("Snapshotting is disabled")]
    Disabled, // gRPC: `FailedPrecondition`

    #[error("No SandboxId '{0}' currently assigned to a Worker")]
    UnassignedSandboxId(SandboxId), // gRPC: `NotFound`; maybe `FailedPrecondition`?

    #[error("Sandbox is currently busy: {0}")]
    Busy(Box<str>), // gRPC: `Unavailable`

    #[error("cannot create snapshot of Sandbox '{sandbox_id}' through dying Worker '{worker_id}'")]
    // gRPC: `FailedPrecondition`; maybe `Aborted` fits better?
    WorkerDying {
        worker_id: WorkerId,
        sandbox_id: SandboxId,
    },

    #[error("internal error: {msg}")] // gRPC: `Internal`
    Internal {
        msg: Box<str>,
        #[source]
        err: Option<Arc<Error>>, // `Arc`'d only for `impl Clone`
    },

    #[error("Worker reported failure to create snapshot: {0}")]
    SnapshotWorker(Box<str>), // gRPC: `Internal`
}
