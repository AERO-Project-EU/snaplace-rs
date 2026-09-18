use std::time::Duration;

use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task::{JoinError, JoinHandle},
    time::{interval_at, Instant, MissedTickBehavior},
};
use tracing::{debug, error, info, instrument, trace, warn, Level};

use crate::network::{self, Error, Result, SandboxNetworkingProvider};

#[derive(Debug)]
enum Message<NetResource: network::Resource> {
    /// Allocate a new [`network::Resource`].
    Allocate {
        respond_to: oneshot::Sender<Result<NetResource>>,
    },
    /// Allocate a [`network::Resource`] with the specific characteristics specified in the
    /// provided [`network::Resource::Descriptor`].
    Request {
        desc: NetResource::Descriptor,
        respond_to: oneshot::Sender<Result<NetResource>>,
    },
    /// Deallocate this [`network::Resource`].
    Deallocate(NetResource),
}

/// # Termination
///
/// Under normal circumstances, the [`NetworkManager`] does not terminate unless both the handle
/// (always `1`; i.e., the [`NetworkManagerHandle`]) and all refs (`> 1`, owned by [`SandboxPool`]
/// and _Active_ & _Idle_ [`Worker`]s; i.e., [`NetworkManagerRef`]s) to it have been dropped (so
/// that its channels' sender halves are dropped).
///
/// [`SandboxPool`]: crate::sbpool::SandboxPool
/// [`Worker`]: crate::worker::Worker
#[derive(Debug)]
pub(crate) struct NetworkManager<N: SandboxNetworkingProvider> {
    provider: N,

    rx: mpsc::Receiver<Message<N::Resource>>,
    quit: broadcast::Receiver<()>,
}

impl<N: SandboxNetworkingProvider> NetworkManager<N> {
    pub(crate) fn spawn(
        provider: N,
        quit: broadcast::Receiver<()>,
    ) -> Result<(NetworkManagerHandle, NetworkManagerRef<N::Resource>)> {
        let (tx, rx) = mpsc::channel(64); // FIXME: buf cap?

        let mut netman = Self { provider, rx, quit };
        let handle = ::tokio::spawn(async move { netman.run().await });

        Ok((NetworkManagerHandle { handle }, NetworkManagerRef { tx }))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn run(&mut self) -> Result<()> {
        loop {
            ::tokio::select! {
                biased;
                quit_res = self.quit.recv() => {
                    match quit_res {
                        Ok(()) => warn!("Received quit notification!"),
                        Err(err) => error!(error = ?err, "Quit channel failure"),
                    }
                    break;
                }
                msg_opt = self.rx.recv() => {
                    let Some(msg) = msg_opt else {
                        error!("All handle/refs have been unexpectedly dropped! Exiting...");
                        return Err(Error::UnexpectedShutDown);
                    };
                    trace!(?msg, "Received new message");
                    match msg {
                        Message::Allocate { respond_to } => {
                            self.alloc(None, respond_to).await
                        }
                        Message::Request { desc, respond_to } => {
                            self.alloc(Some(desc), respond_to).await
                        }
                        Message::Deallocate(net_rsrc) => {
                            if let Err(err) = self.provider.dealloc(net_rsrc).await {
                                error!(error = ?err, "Failed to deallocate net resource: {err:#}");
                                // ¿TODO: We cannot meaningfully handle this, can we? Maybe requeue
                                // to retry with some timeout?
                            }
                        }
                    }
                }
            }
        }
        self.shutdown().await;
        Ok(())
    }

    /// Auxiliary function factoring in the similarities of handling [`Message::Allocate`] and
    /// [`Message::Request`].
    #[instrument(level = Level::TRACE, skip_all)]
    async fn alloc(
        &mut self,
        desc: Option<<N::Resource as network::Resource>::Descriptor>,
        respond_to: oneshot::Sender<Result<N::Resource>>,
    ) {
        let rsrc = if let Some(desc) = desc {
            self.provider.request(desc).await
        } else {
            self.provider.alloc()
        };
        trace!(
            net_resource = ?rsrc,
            "Responding to networking resource allocation request"
        );
        if let Err(res) = respond_to.send(rsrc) {
            error!(
                failed_response = ?res,
                "Caller probably dropped; failed to respond"
            );
            if let Ok(rsrc) = res {
                // FIXME(ckatsak): Should we queue the deallocation for later?
                if let Err(err) = self.provider.dealloc(rsrc).await {
                    error!(
                        error = ?err,
                        "Failed to deallocate net resource: {err:#}"
                    );
                }
            }
        }
    }

    /// See the [`Termination` section][term].
    ///
    /// [term]: struct.NetworkManager.html#termination
    #[inline(never)]
    #[cold]
    #[instrument(level = Level::INFO, skip_all)]
    async fn shutdown(&mut self) {
        let start = Instant::now();

        const TICK: Duration = Duration::from_secs(2);
        let mut interval = interval_at(start + TICK, TICK);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            ::tokio::select! {
                biased;
                msg_opt = self.rx.recv() => {
                    match msg_opt {
                        None => break,
                        Some(Message::Allocate { respond_to }) => {
                            debug!("Refusing allocation request...");
                            if let Err(_shutdown_err) = respond_to.send(Err(Error::ShuttingDown)) {
                                debug!("Failed to refuse allocation request");
                            }
                        }
                        Some(Message::Request { desc, respond_to }) => {
                            debug!(descriptor = ?desc, "Refusing allocation request...");
                            if let Err(_shutdown_err) = respond_to.send(Err(Error::ShuttingDown)) {
                                debug!(descriptor = ?desc, "Failed to refuse allocation request");
                            }
                        }
                        Some(Message::Deallocate(net_rsrc)) => {
                            if let Err(err) = self.provider.dealloc(net_rsrc).await {
                                error!(error = ?err, "Failed to deallocate net resource: {err:#}");
                                // ¿TODO: We cannot meaningfully handle this, can we? Maybe retry
                                // with some timeout?
                            }
                        }
                    }
                }
                now = interval.tick() => {
                    info!(
                        "Waiting ({} so far) for all handle/refs to be dropped before exiting...",
                        ::humantime::format_duration(now - start)
                    );
                }
            }
        }
        debug!(
            "All handle/refs dropped (after {}); shutting down provider...",
            ::humantime::format_duration(Instant::now() - start)
        );

        let start = Instant::now();
        if let Err(err) = self.provider.shutdown().await {
            error!(error = ?err, "Failed to shut down Sandbox networking provider: {err:#}");
        }
        debug!(
            "Provider shut down (after {}); exiting",
            ::humantime::format_duration(Instant::now() - start)
        );
    }
}

/// This is an "owning" handle of a [`NetworkManager`] task; i.e., it may be owned only by a single
/// entity (in our case, [`Orchestrator`]), and it can be used to join the task, or to create
/// [`NetworkManagerRef`]s.
///
/// # Note
///
/// In the past, `NetworkManagerHandle` was able to create new [`NetworkManagerRef`]s; it was
/// defined and implemented like this:
///
/// ```compile_fail
/// #[derive(Debug)]
/// pub(crate) struct NetworkManagerHandle<NetResource: network::Resource> {
///     tx: mpsc::Sender<Message<NetResource>>,
///     handle: JoinHandle<Result<()>>,
/// }
/// impl<NetResource: network::Resource> NetworkManagerHandle<NetResource> {
///     pub(crate) fn new_ref(&self) -> NetworkManagerRef<NetResource> {
///         NetworkManagerRef {
///             tx: self.tx.clone(),
///         }
///     }
///     pub(crate) async fn reap(self) -> ::std::result::Result<Result<()>, JoinError> {
///         drop(self.tx);
///         self.handle.await
///     }
/// }
/// ```
/// Therefore, it was the only type returned by [`NetworkManager::spawn`] too.
///
/// Nowadays, however, it is merely a wrapper for [`JoinHandle`], so that it can be composed into
/// [`Orchestrator`] while exposing the type parameter of the [`SandboxNetworkingProvider`]
/// implementation associated with it only via [`Orchestrator::spawn`].
///
/// But:
/// - Since type parameters are exposed in [`Orchestrator::spawn`] anyway, is there any point in
///   keeping the [`Orchestrator`] struct clear of them?
/// - Even if there is, I am not sure if this is the best way to arrange all such actor handles.
///
/// [`Orchestrator`]: crate::Orchestrator
/// [`Orchestrator::spawn`]: crate::Orchestrator::spawn
#[derive(Debug)]
pub(crate) struct NetworkManagerHandle {
    handle: JoinHandle<Result<()>>,
}

impl NetworkManagerHandle {
    pub(crate) async fn reap(self) -> ::std::result::Result<Result<()>, JoinError> {
        self.handle.await
    }
}

#[derive(Debug)]
pub struct NetworkManagerRef<NetResource: network::Resource> {
    tx: mpsc::Sender<Message<NetResource>>,
}

impl<NetResource: network::Resource> Clone for NetworkManagerRef<NetResource> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<NetResource: network::Resource> NetworkManagerRef<NetResource> {
    /// Allocate a new [`network::Resource`].
    #[inline]
    pub(crate) async fn allocate(&self) -> Result<oneshot::Receiver<Result<NetResource>>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Message::Allocate { respond_to: tx })
            .await
            .map_err(|err| Error::Forward(err.to_string().into_boxed_str()))?;
        Ok(rx)
    }

    /// Allocate a [`network::Resource`] with the specific characteristics specified in the
    /// provided [`network::Resource::Descriptor`].
    pub(crate) async fn request(
        &self,
        desc: NetResource::Descriptor,
    ) -> Result<oneshot::Receiver<Result<NetResource>>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Message::Request {
                desc,
                respond_to: tx,
            })
            .await
            .map_err(|err| Error::Forward(err.to_string().into_boxed_str()))?;
        Ok(rx)
    }

    /// Deallocate this [`network::Resource`].
    #[inline]
    pub(crate) async fn deallocate(&self, net_resource: NetResource) -> Result<()> {
        self.tx
            .send(Message::Deallocate(net_resource))
            .await
            .map_err(|err| Error::Forward(err.to_string().into_boxed_str()))
    }
}
