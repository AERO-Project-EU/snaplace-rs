use std::time::Duration;

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{instrument, warn, Level};

use crate::{
    metadata::SandboxStats,
    sbpool::error::{
        CreateSnapshotError, DestroySandboxError, ListSandboxesError, PrepareSandboxError,
    },
    FunctionId, SandboxId,
};

// NOTE(ckatsak): Three distinct naming families:
// - Worker -> Pool: ...Result
// - Pool -> PoolRef -> caller: ...Output
// - gRPC layer: protobuf ...Response

#[derive(Debug)]
pub(super) enum PoolControlMessage {
    PrepareSandbox {
        function_id: FunctionId,
        options: PrepareSandboxOptions,
        respond_to: oneshot::Sender<Result<PrepareSandboxOutput, PrepareSandboxError>>,
    },
    DestroySandbox {
        target: SandboxSelector,
        options: DestroySandboxOptions,
        respond_to: oneshot::Sender<Result<DestroySandboxOutput, DestroySandboxError>>,
    },
    ListSandboxes {
        filter: ListSandboxesFilter,
        respond_to: oneshot::Sender<Result<Vec<SandboxInfo>, ListSandboxesError>>,
    },
    CreateSnapshot {
        sandbox_id: SandboxId,
        respond_to: oneshot::Sender<Result<(), CreateSnapshotError>>,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum SandboxSelector {
    SandboxId(SandboxId),
}

#[derive(Debug, Clone)]
pub(crate) enum SandboxControlState {
    Active,
    Idle,
    Dying,
    SnapshotOnly,
}

#[derive(Debug, Clone)]
pub(crate) struct SandboxInfo {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) function_id: FunctionId,
    pub(crate) state: SandboxControlState,
    pub(crate) has_snapshot: bool,
    pub(crate) stats: Option<SandboxStats>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PrepareSandboxOptions {
    /// How to provision the requested [`Sandbox`]?
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    pub(crate) mode: SandboxProvisioningMode,

    /// Optional best-effort initial keep-alive duration for the freshly prepared [`Sandbox`].
    ///
    /// If provided, [`SandboxPool`] uses it instead of consulting the normal keep-alive
    /// assignment policy for the first transition from _Active_ to _Idle_ after preparation.
    /// This still does not guarantee lifetime; eviction, shutdown, or failures may terminate
    /// the [`Sandbox`] earlier.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    pub(crate) initial_keepalive_hint: Option<Duration>,
}

/// Policy for provisioning the requested [`Sandbox`].
///
/// [`Sandbox`]: crate::worker::runtime::Sandbox
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum SandboxProvisioningMode {
    /// Prefer hydrating an existing snapshot, but fall back to creating a fresh
    /// [`Sandbox`] if no snapshot is available.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    #[default]
    PreferSnapshot,
    /// Require the new [`Sandbox`] to be a hydrated snapshot; fail if none exists
    /// (rather than creating a fresh one).
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    RequireSnapshot,
    /// Force the creation of a new [`Sandbox`], even if a snapshot is available.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    ForceFresh,
}

#[derive(Debug, Clone)]
pub(crate) struct PrepareSandboxOutput {
    pub(crate) info: SandboxInfo,
}

#[derive(Debug, Clone)]
pub(crate) struct DestroySandboxOptions {
    /// Attempt to destroy the [`Sandbox`] regardless of its status; i.e., even
    /// if it is currently active.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    pub(crate) allow_if_active: bool,
    /// Also attempt to remove any [`Sandbox`]'s persisted snapshot file(s).
    ///
    /// ## Note
    ///
    /// Failure to remove such files does not lead to RPC failure; the result is
    /// communicated via [`DestroySandboxOutput::persistent_snapshot_exists`].
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    pub(crate) remove_persisted_snapshot: bool,
}

impl Default for DestroySandboxOptions {
    fn default() -> Self {
        Self {
            allow_if_active: true,
            remove_persisted_snapshot: false,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DestroySandboxOutput {
    /// Set (`true`) if a snapshot file(s) are still persisted after handling
    /// the sandbox destruction request; otherwise cleared (`false`).
    pub(crate) persistent_snapshot_exists: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ListSandboxesFilter {
    pub(crate) function_id: Option<FunctionId>,
    pub(crate) state: Option<SandboxControlState>,
}

#[derive(Debug, Clone)]
pub(crate) struct CreateSnapshotOutput {
    pub(crate) sandbox_id: SandboxId,
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct SandboxPoolHandle {
    to_pool_ctrl: mpsc::Sender<PoolControlMessage>,
    handle: JoinHandle<Result<(), super::Error>>,
}

impl SandboxPoolHandle {
    pub(super) fn new(
        to_pool_ctrl: mpsc::Sender<PoolControlMessage>,
        handle: JoinHandle<Result<(), super::Error>>,
    ) -> Self {
        Self {
            to_pool_ctrl,
            handle,
        }
    }

    pub(crate) fn new_ref(&self) -> SandboxPoolRef {
        SandboxPoolRef {
            to_pool_ctrl: self.to_pool_ctrl.clone(),
        }
    }

    pub(crate) async fn reap(self) -> Result<Result<(), super::Error>, ::tokio::task::JoinError> {
        self.handle.await
    }
}

#[derive(Clone)]
pub(crate) struct SandboxPoolRef {
    to_pool_ctrl: mpsc::Sender<PoolControlMessage>,
}

impl SandboxPoolRef {
    /// # Errors
    ///
    /// This method returns an <code>[Err]\([PrepareSandboxRequestError]\)</code> on failure:
    /// - contacting [`SandboxPool`] to send the control message (including timeout);
    /// - receiving a response from the [`SandboxPool`] after having already sent the message;
    /// - conducting the above before timing out;
    /// - returned by [`SandboxPool`] while processing the control message.
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    #[instrument(level = Level::INFO, skip(self))]
    pub(crate) async fn prepare_sandbox(
        &self,
        function_id: FunctionId,
        options: PrepareSandboxOptions,
    ) -> Result<PrepareSandboxOutput, PrepareSandboxRequestError> {
        const SEND_TIMEOUT: Duration = Duration::from_secs(5);
        const REPLY_TIMEOUT: Duration = Duration::from_secs(60);

        let (respond_to, from_pool) = oneshot::channel();

        // Try send the `PoolControlMessage::PrepareSandbox` to Pool, but fail after timeout.
        match self
            .to_pool_ctrl
            .send_timeout(
                PoolControlMessage::PrepareSandbox {
                    function_id,
                    options,
                    respond_to,
                },
                SEND_TIMEOUT,
            )
            .await
        {
            Ok(()) => {}
            Err(err @ mpsc::error::SendTimeoutError::Closed(_)) => {
                warn!("Contacting Pool to PrepareSandbox failed: {err}");
                return Err(PrepareSandboxRequestError::ChannelClosed {
                    which: "control".into(),
                });
            }
            Err(err @ mpsc::error::SendTimeoutError::Timeout(_)) => {
                warn!("Contacting Pool to PrepareSandbox failed: {err}");
                return Err(PrepareSandboxRequestError::SendTimeout);
            }
        }

        // Try receiving Pool's response
        match ::tokio::time::timeout(REPLY_TIMEOUT, from_pool).await {
            Ok(Ok(Ok(ret))) => Ok(ret),
            Ok(Ok(Err(err))) => {
                warn!(error = ?err, "Pool failed while handling PrepareSandbox: {err:#}");
                Err(PrepareSandboxRequestError::Pool(err))
            }
            Ok(Err(rcv_err)) => {
                warn!("Failed to receive Pool's response for PrepareSandbox: {rcv_err}");
                Err(PrepareSandboxRequestError::ChannelClosed {
                    which: "reply".into(),
                })
            }
            Err(timerr) => {
                warn!("Timed out waiting for Pool's reply to PrepareSandbox: {timerr}");
                Err(PrepareSandboxRequestError::ReplyTimeout)
            }
        }
    }

    /// # Errors
    ///
    /// This method returns an <code>[Err]\([DestroySandboxRequestError]\)</code> on failure:
    /// - contacting [`SandboxPool`] to send the control message (including timeout);
    /// - receiving a response from the [`SandboxPool`] after having already sent the message;
    /// - conducting the above before timing out;
    /// - returned by [`SandboxPool`] while processing the control message.
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    #[instrument(level = Level::INFO, skip(self))]
    pub(crate) async fn destroy_sandbox(
        &self,
        target: SandboxSelector,
        options: DestroySandboxOptions,
    ) -> Result<DestroySandboxOutput, DestroySandboxRequestError> {
        const SEND_TIMEOUT: Duration = Duration::from_secs(5);
        const REPLY_TIMEOUT: Duration = Duration::from_secs(60);

        let (respond_to, from_pool) = oneshot::channel();

        // Try send the `PoolControlMessage::DestroySandbox` to Pool, but fail after timeout.
        match self
            .to_pool_ctrl
            .send_timeout(
                PoolControlMessage::DestroySandbox {
                    target,
                    options,
                    respond_to,
                },
                SEND_TIMEOUT,
            )
            .await
        {
            Ok(()) => {}
            Err(err @ mpsc::error::SendTimeoutError::Closed(_)) => {
                warn!("Contacting Pool to DestroySandbox failed: {err}");
                return Err(DestroySandboxRequestError::ChannelClosed {
                    which: "control".into(),
                });
            }
            Err(err @ mpsc::error::SendTimeoutError::Timeout(_)) => {
                warn!("Contacting Pool to PrepareSandbox failed: {err}");
                return Err(DestroySandboxRequestError::SendTimeout);
            }
        }

        // Try receiving Pool's response
        match ::tokio::time::timeout(REPLY_TIMEOUT, from_pool).await {
            Ok(Ok(Ok(ret))) => Ok(ret),
            Ok(Ok(Err(err))) => {
                warn!(error = ?err, "Pool failed while handling DestroySandbox: {err:#}");
                Err(DestroySandboxRequestError::Pool(err))
            }
            Ok(Err(rcv_err)) => {
                warn!("Failed to receive Pool's response for DestroySandbox: {rcv_err}");
                Err(DestroySandboxRequestError::ChannelClosed {
                    which: "reply".into(),
                })
            }
            Err(timerr) => {
                warn!("Timed out waiting for Pool's reply to DestroySandbox: {timerr}");
                Err(DestroySandboxRequestError::ReplyTimeout)
            }
        }
    }

    pub(crate) async fn list_sandboxes(
        &self,
        filter: ListSandboxesFilter,
    ) -> Result<Vec<SandboxInfo>, ListSandboxesError> {
        todo!()
    }

    /// # Errors
    ///
    /// This method returns an <code>[Err]\([CreateSnapshotRequestError]\)</code> on failure:
    /// - contacting [`SandboxPool`] to send the control message (including timeout);
    /// - receiving a response from the [`SandboxPool`] after having already sent the message;
    /// - conducting the above before timing out;
    /// - returned by [`SandboxPool`] while processing the control message.
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    #[instrument(level = Level::INFO, skip(self))]
    pub(crate) async fn create_snapshot(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<CreateSnapshotOutput, CreateSnapshotRequestError> {
        const SEND_TIMEOUT: Duration = Duration::from_secs(5);
        const REPLY_TIMEOUT: Duration = Duration::from_secs(60);

        let (respond_to, from_pool) = oneshot::channel();

        // Try send the `PoolControlMessage::CreateSnapshot` to Pool, but fail after timeout.
        match self
            .to_pool_ctrl
            .send_timeout(
                PoolControlMessage::CreateSnapshot {
                    sandbox_id: sandbox_id.clone(),
                    respond_to,
                },
                SEND_TIMEOUT,
            )
            .await
        {
            Ok(()) => {}
            Err(err @ mpsc::error::SendTimeoutError::Closed(_)) => {
                warn!("Contacting Pool to CreateSnapshot failed: {err}");
                return Err(CreateSnapshotRequestError::ChannelClosed {
                    which: "control".into(),
                });
            }
            Err(err @ mpsc::error::SendTimeoutError::Timeout(_)) => {
                warn!("Contacting Pool to CreateSnapshot failed: {err}");
                return Err(CreateSnapshotRequestError::SendTimeout);
            }
        }

        // Try receiving Pool's response
        match ::tokio::time::timeout(REPLY_TIMEOUT, from_pool).await {
            Ok(Ok(Ok(()))) => Ok(CreateSnapshotOutput { sandbox_id }),
            Ok(Ok(Err(err))) => {
                warn!(error = ?err, "Pool failed while handling CreateSnapshot: {err:#}");
                Err(CreateSnapshotRequestError::Pool(err))
            }
            Ok(Err(rcv_err)) => {
                warn!("Failed to receive Pool's response for CreateSnapshot: {rcv_err}");
                Err(CreateSnapshotRequestError::ChannelClosed {
                    which: "reply".into(),
                })
            }
            Err(timerr) => {
                warn!("Timed out waiting for Pool's reply to CreateSnapshot: {timerr}");
                Err(CreateSnapshotRequestError::ReplyTimeout)
            }
        }
    }
}

#[derive(Debug, ::thiserror::Error)]
pub(crate) enum PrepareSandboxRequestError {
    #[error("SandboxPool failed to process sandbox preparation request")]
    Pool(#[source] PrepareSandboxError),

    #[error("timed out sending sandbox preparation request to SandboxPool")]
    SendTimeout, // gRPC unavailable

    #[error("timed out waiting for SandboxPool's response for sandbox preparation request")]
    ReplyTimeout, // gRPC deadline_exceeded

    #[error("SandboxPool's {which} channel closed")] // "control"/"reply"
    ChannelClosed { which: Box<str> }, // gRPC unavailable
}

#[derive(Debug, ::thiserror::Error)]
pub(crate) enum DestroySandboxRequestError {
    #[error("SandboxPool failed to process sandbox destruction request")]
    Pool(#[source] DestroySandboxError),

    #[error("timed out sending sandbox destruction request to SandboxPool")]
    SendTimeout, // gRPC unavailable

    #[error("timed out waiting for SandboxPool's response for sandbox destruction request")]
    ReplyTimeout, // gRPC deadline_exceeded

    #[error("SandboxPool's {which} channel closed")] // "control"/"reply"
    ChannelClosed { which: Box<str> }, // gRPC unavailable
}

#[derive(Debug, ::thiserror::Error)]
pub(crate) enum CreateSnapshotRequestError {
    #[error("SandboxPool failed to process snapshotting request")]
    Pool(#[source] CreateSnapshotError),

    #[error("timed out sending snapshotting request to SandboxPool")]
    SendTimeout, // gRPC unavailable

    #[error("timed out waiting for SandboxPool's response for snapshotting request")]
    ReplyTimeout, // gRPC deadline_exceeded

    #[error("SandboxPool's {which} channel closed")] // "control"/"reply"
    ChannelClosed { which: Box<str> }, // gRPC unavailable
}
