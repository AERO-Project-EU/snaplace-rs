use std::path::PathBuf;

type BoxedStdErr = Box<dyn ::std::error::Error + Send + Sync + 'static>;

/// Errors produced by the Firecracker (`rt-fc`) runtime.
#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("failed to initialize runtime: {0}")]
    Init(String),

    #[error("failed API-driven Firecracker setup: {msg}")]
    ApiSetup { msg: Box<str>, err: ::wick::Error },

    #[error("I/O error: {msg}")]
    Io {
        msg: Box<str>,
        #[source]
        err: ::std::io::Error,
    },

    #[error("path '{0}' is not UTF-8")]
    Utf8(PathBuf),

    #[error("failed to create new uVM")]
    CreateVm(#[source] BoxedStdErr),
    #[error("failed to load uVM from snapshot")]
    LoadSnapshot(#[source] BoxedStdErr),
    #[error("{0}: snapshot not found")]
    NoSnapshot(Box<str>),
    #[error("failed to pause uVM")]
    PauseVm(#[source] BoxedStdErr),
    #[error("failed to resume uVM")]
    ResumeVm(#[source] BoxedStdErr),
    #[error("failed to create uVM snapshot")]
    CreateSnapshot(#[source] BoxedStdErr),
    //#[error("failed to shut uVM down")]
    //ShutDown(#[source] BoxedStdErr),
    //#[error("failed to shut uVM down")]
    //DestroySandbox(#[source] BoxedStdErr),
    #[error("failed to reinstate VM from SnapshotState: {msg}")]
    ReinstateSandbox {
        msg: Box<str>,
        #[source]
        err: Option<BoxedStdErr>,
    },
}
