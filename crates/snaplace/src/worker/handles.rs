use std::fmt::{Debug, Display};

use tokio::{
    sync::mpsc,
    task::{JoinError, JoinHandle},
};

use crate::{
    admission::RunningSlot,
    sbpool::{Cpu, WorkerMemAccounting},
    worker::{ControlMessage, Error, Invocation, Result, WorkerId},
    FunctionId, Request, SandboxId,
};

/// This is an "owning" handle of a [`Worker`] task; i.e., it may be owned only by a single
/// entity (in our case, [`SandboxPool`]), and it can be used to join the task, or to create
/// [`WorkerRef`]s.
///
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
pub struct WorkerHandle<S, Req: Request> {
    id: WorkerId,
    function_id: FunctionId,
    sandbox_id: Option<SandboxId>,

    /// [`SandboxPool`]-side memory-accounting classification for this [`Worker`].
    ///
    /// This records where Pool has charged the Worker's Function memory while
    /// the Worker is live. The value is initialized at spawn time and updated
    /// by Pool when it performs lifecycle transitions such as _Active_->_Dying_
    /// or _Idle_->_Dying_. It is then used at reap-time to adjust the correct
    /// memory bucket.
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    mem_accounting: WorkerMemAccounting,

    // NOTE: We store this here so that we can clone it in new `WorkerRef`s. Therefore,
    // a `Worker`'s invocation channel never closes as long as a `WorkerHandle` exists!
    tx_invocations: mpsc::Sender<Invocation<Req>>,
    // Control messages are only issued by the Pool through the owning handle.
    tx_control: mpsc::Sender<ControlMessage>,

    handle: JoinHandle<super::WorkerExit<S>>,
}

impl<S, Req: Request> Debug for WorkerHandle<S, Req> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("WorkerHandle")
            .field("id", &self.id)
            .field("function_id", &self.function_id)
            .field("sandbox_id", &self.sandbox_id)
            .finish()
    }
}

impl<S, Req: Request> WorkerHandle<S, Req> {
    #[inline]
    pub(super) fn new(
        id: WorkerId,
        function_id: FunctionId,
        sandbox_id: Option<SandboxId>,
        mem_accounting: WorkerMemAccounting,
        tx_invocations: mpsc::Sender<Invocation<Req>>,
        tx_control: mpsc::Sender<ControlMessage>,
        handle: JoinHandle<super::WorkerExit<S>>,
    ) -> Self {
        Self {
            id,
            function_id,
            sandbox_id,
            mem_accounting,
            tx_invocations,
            tx_control,
            handle,
        }
    }

    /// Retrieve the [`WorkerId`] of the [`Worker`].
    ///
    /// [`WorkerId`]: crate::worker::WorkerId
    /// [`Worker`]: crate::worker::Worker
    #[inline(always)]
    pub(crate) fn id(&self) -> WorkerId {
        self.id
    }

    /// Retrieve the ID of the Function that this [`Worker`] can handle.
    ///
    /// [`Worker`]: crate::worker::Worker
    #[inline(always)]
    pub(crate) fn function_id(&self) -> &FunctionId {
        &self.function_id
    }

    /// Retrieve the ID of the [`Sandbox`] that this [`Worker`] owns.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`Worker`]: crate::worker::Worker
    #[inline(always)]
    pub(crate) fn sandbox_id(&self) -> Option<&SandboxId> {
        self.sandbox_id.as_ref()
    }

    /// Set the ID of the [`Sandbox`] that this [`Worker`] owns.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`Worker`]: crate::worker::Worker
    #[inline(always)]
    pub(crate) fn set_sandbox_id(&mut self, sandbox_id: SandboxId) {
        self.sandbox_id = Some(sandbox_id)
    }

    /// Retrieve the [`WorkerMemAccounting`] of this [`Worker`].
    ///
    /// [`Worker`]: crate::worker::Worker
    #[inline]
    pub(crate) fn memory_accounting(&self) -> WorkerMemAccounting {
        self.mem_accounting
    }

    /// Update the [`WorkerMemAccounting`] of this [`Worker`].
    ///
    /// [`Worker`]: crate::worker::Worker
    #[inline]
    pub(crate) fn set_memory_accounting(&mut self, mem_accounting: WorkerMemAccounting) {
        self.mem_accounting = mem_accounting
    }

    /// Create a [`WorkerRef`] to forward [`Request`]s to the [`Worker`].
    ///
    /// [`Request`]: crate::request::Request
    /// [`Worker`]: crate::worker::Worker
    #[inline(always)]
    pub(crate) fn new_ref(&self) -> WorkerRef<Req> {
        WorkerRef {
            id: self.id,
            tx_invocations: self.tx_invocations.clone(),
        }
    }

    /// Query internal [`JoinHandle`] whether it [`is_finished`], returning the result.
    ///
    /// [`is_finished`]: tokio::task::JoinHandle::is_finished
    #[inline]
    pub(crate) fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Send a [`ControlMessage::ShutDown`] message to the [`Worker`].
    ///
    /// [`ControlMessage::ShutDown`]: crate::worker::ControlMessage::ShutDown
    /// [`Worker`]: crate::worker::Worker
    #[inline]
    pub(crate) async fn shutdown(&self) -> Result<()> {
        self.tx_control
            .send(ControlMessage::ShutDown)
            .await
            .map_err(|err| Error::ShutDown(self.id, err.to_string().into_boxed_str()))
    }

    /// Send a [`ControlMessage::DestroySandbox`] message to the [`Worker`].
    ///
    /// [`ControlMessage::DestroySandbox`]: crate::worker::ControlMessage::DestroySandbox
    /// [`Worker`]: crate::worker::Worker
    #[inline]
    pub(crate) async fn destroy(&self) -> Result<()> {
        self.tx_control
            .send(ControlMessage::DestroySandbox)
            .await
            .map_err(|err| Error::ShutDown(self.id, err.to_string().into_boxed_str()))
    }

    /// Consume this [`WorkerHandle`] to join the [`Worker`] task.
    ///
    /// [`Worker`]: crate::worker::Worker
    #[inline(always)]
    pub(crate) async fn reap(self) -> ::std::result::Result<super::WorkerExit<S>, JoinError> {
        self.handle.await
    }

    /// Send a [`ControlMessage::PrepareSandbox`] to the [`Worker`].
    ///
    /// [`ControlMessage::PrepareSandbox`]: crate::worker::ControlMessage::PrepareSandbox
    /// [`Worker`]: crate::worker::Worker
    #[inline]
    pub(crate) async fn prepare_sandbox(&self, cpuset: Cpu) -> Result<()> {
        self.tx_control
            .send(ControlMessage::PrepareSandbox { cpuset })
            .await
            .map_err(|err| {
                Error::Channel(
                    "sending ControlMessage::PrepareSandbox to Worker".into(),
                    err.to_string().into(),
                )
            })
    }

    /// Send a [`ControlMessage::CreateSnapshot`] to the [`Worker`].
    ///
    /// [`ControlMessage::CreateSnapshot`]: crate::worker::ControlMessage::CreateSnapshot
    /// [`Worker`]: crate::worker::Worker
    #[inline]
    pub(crate) async fn create_snapshot(&self, sandbox_id: SandboxId) -> Result<()> {
        self.tx_control
            .send(ControlMessage::CreateSnapshot { sandbox_id })
            .await
            .map_err(|err| {
                Error::Channel(
                    "sending ControlMessage::CreateSnapshot to Worker".into(),
                    err.to_string().into(),
                )
            })
    }
}

/// This is a reference to the [`Worker`] task for anyone that needs to send it a message (mostly,
/// for the [`AdmissionController`] really).
///
/// [`AdmissionController`]: crate::admission::AdmissionController
/// [`Worker`]: crate::worker::Worker
#[derive(Clone)]
pub(crate) struct WorkerRef<Req: Request> {
    id: WorkerId,
    tx_invocations: mpsc::Sender<Invocation<Req>>,
}

impl<Req: Request> Debug for WorkerRef<Req> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_tuple("WorkerRef").field(&self.id).finish()
    }
}

impl<Req: Request> Display for WorkerRef<Req> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "WorkerRef-{}", self.id)
    }
}

impl<Req: Request> WorkerRef<Req> {
    /// Retrieve the [`WorkerId`] of the [`Worker`].
    ///
    /// [`WorkerId`]: crate::worker::WorkerId
    /// [`Worker`]: crate::worker::Worker
    #[inline(always)]
    pub(crate) fn id(&self) -> WorkerId {
        self.id
    }

    /// Forward a [`Request`] to the [`Worker`], along with an associated acquired [`RunningSlot`]
    /// and the allocated [`Cpu`].
    ///
    /// [`Cpu`]: crate::sbpool::Cpu
    /// [`Request`]: crate::request::Request
    /// [`RunningSlot`]: crate::admission::RunningSlot
    /// [`Worker`]: crate::worker::Worker
    #[inline]
    pub(crate) async fn forward(
        &self,
        req: Req,
        running_slot: RunningSlot,
        cpuset: Cpu,
    ) -> Result<()> {
        self.tx_invocations
            .send(Invocation {
                req,
                running_slot,
                cpuset,
            })
            .await
            .map_err(|err| Error::Forward(self.id(), err.to_string().into_boxed_str()))
    }
}
