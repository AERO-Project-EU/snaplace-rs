use crate::utils::psutils;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("failed to initialize runtime: {0}")]
    Init(String),

    #[error("failed to create new VM")]
    CreateVm(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to pause VM")]
    PauseVm(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to resume VM")]
    ResumeVm(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to create new VM snapshot")]
    CreateVmSnapshot(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to load VM from snapshot")]
    LoadVmSnapshot(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to unload VM")]
    UnloadVm(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to stop VM")]
    StopVm(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to destroy VM")]
    DestroyVm(#[source] Option<Box<Self>>),

    #[error("failed to prepare container snapshot")]
    PrepareContainerSnapshot(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to create new container")]
    CreateContainer(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to delete container")]
    DeleteContainer(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to create new task")]
    CreateTask(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to start task")]
    StartTask(#[source] ::firecracker_containerd_client::Error),

    #[error("failed to reinstate VM from SnapshotState: {msg}")]
    ReinstateSandbox {
        msg: Box<str>,
        #[source]
        err: Option<Box<dyn ::std::error::Error + Send + Sync + 'static>>,
    },

    #[error("psutil error: {msg}")]
    ProcessUtil {
        msg: Box<str>,
        #[source]
        source: psutils::Error,
    },
}
