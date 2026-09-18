use std::time::Duration;

use crate::{snapman, worker::WorkerId};

pub(super) type Result<T> = ::std::result::Result<T, self::Error>;

/// Synchronous [`Worker::spawn`] failure before a [`Worker`] task exists.
///
/// Unlike failures from [`Worker::run`], this cannot be handled by
/// [`SandboxPool::reap_worker`], because no [`Worker`] task has been spawned
/// yet and no [`ReapMe`] message can be emitted. If [`SandboxPool`] had
/// already removed a [`Sandbox`] from its own snapshot set and passed it
/// into [`Worker::spawn`], this error carries that [`Sandbox`] back, so
/// [`SandboxPool`] can restore ownership.
///
/// [`ReapMe`]: crate::worker::messages::OutboundMessage::ReapMe
/// [`Sandbox`]: crate::worker::runtime::Sandbox
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`SandboxPool::reap_worker`]: crate::sbpool::SandboxPool::reap_worker
/// [`Worker`]: crate::worker::Worker
/// [`Worker::run`]: crate::worker::Worker::run
/// [`Worker::spawn`]: crate::worker::Worker::spawn
#[derive(Debug, ::thiserror::Error)]
#[error("Worker (sync) initialization error: {err}")]
pub struct SpawnError<S> {
    pub err: Error,
    pub sandbox: Option<S>,
}

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("failed to initialize Worker: {dscr}")]
    Init {
        dscr: Box<str>,
        #[source]
        source: Box<dyn ::std::error::Error + Send + Sync + 'static>,
    },

    #[error("failed to forward Request to Worker `{0}`: {1}")]
    Forward(WorkerId, Box<str>),

    #[error("failed to send ShutDown message to Worker `{0}`: {1}")]
    ShutDown(WorkerId, Box<str>),
    //
    // TODO: Variant `Join` for `tokio::JoinError` (or sth) in `WorkerHandle::reap`?
    //
    #[error("internal channel failure: {0}")]
    Channel(
        Box<str>,
        #[source] Box<dyn ::std::error::Error + Send + Sync + 'static>,
    ),

    #[error("error while managing networking resources")]
    Network(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Errors in handle_request itself
    ///////////////////////////////////////////////////////////////////////////////////////////////
    //
    #[error("failed to prepare the sandbox")]
    PrepareSandbox(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[error("runtime failed to create new sandbox")]
    CreateSandbox(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[error("runtime failed to load sandbox")]
    LoadSandbox(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[error("runtime failed to pause sandbox")]
    PauseSandbox(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[error("runtime failed to resume sandbox")]
    ResumeSandbox(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[error("runtime failed to setup cpuset")]
    SetupCpuset(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[error("runtime failed to create snapshot for sandbox")]
    CreateSnapshot(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[error("runtime failed to shut the sandbox down")]
    ShutdownSandbox(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[error("runtime failed to destroy the sandbox")]
    DestroySandbox(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),

    #[error("snapshot manager error: {msg}")]
    SnapshotManager {
        msg: Box<str>,
        #[source]
        err: snapman::Error,
    },
    #[error("failed to handle control-plane snapshotting request: {0}")]
    CreateSnapshotControl(Box<str>),
    #[error("failed to create sandbox snapshot state")]
    CreateState(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),

    #[error("failed to handle the function request")]
    HandleFunctionRequest(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[error("stopped awaiting RequestIssuer after {0:?}")]
    IssuerTimeout(Duration),

    #[error("failed to forward Function's Response to Sink")]
    ForwardToSink(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),
}
