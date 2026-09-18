//! [`Sink`] receives [`Response`]s through a `mpsc` channel, and broadcasts them to "Sink-workers"
//! (one spawned for every client connected to the gRPC server) through a `broadcast` channel.

use std::{
    io::{self, ErrorKind},
    net::{SocketAddr, ToSocketAddrs},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use compact_str::{format_compact, CompactString};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tracing::{
    debug, debug_span, error, info, info_span, instrument, trace, warn, Instrument, Level,
};

use snaplace::response::{Error, HEADER_KEY_SINK_TIMESTAMP_NS};

use crate::{
    pb::{self, response::sink_server::SinkServer},
    timestamping::TimestampingStream,
    Response,
};

#[derive(Debug)]
struct SinkGrpcServer<Resp = Response> {
    sink_broadcast: broadcast::Sender<Resp>,
    buffer_size: usize,

    next_id: AtomicU64,
}

impl<Resp> SinkGrpcServer<Resp> {
    fn new(buffer_size: usize, sink_broadcast: broadcast::Sender<Resp>) -> Self {
        Self {
            sink_broadcast,
            buffer_size,
            next_id: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl pb::response::sink_server::Sink for SinkGrpcServer {
    type SnaplaceResponsesStream =
        TimestampingStream<ReceiverStream<Result<Response, ::tonic::Status>>>;

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn snaplace_responses(
        &self,
        request: ::tonic::Request<pb::response::Subscribe>,
    ) -> Result<::tonic::Response<Self::SnaplaceResponsesStream>, ::tonic::Status> {
        let remote_addr = request
            .remote_addr()
            .map(|sa| format_compact!("{sa}"))
            .unwrap_or_else(|| CompactString::const_new("-"));
        let sink_worker_id = self.next_id.fetch_add(1, Ordering::AcqRel);

        let (to_client, from_sink_worker) = mpsc::channel(self.buffer_size); // FIXME: chan cap?
        let mut from_sink = self.sink_broadcast.subscribe();

        ::tokio::spawn(
            async move {
                loop {
                    match from_sink.recv().await {
                        Ok(resp) => {
                            let resp_span =
                                debug_span!("new-response", "invocation_id={}", resp.invocation_id);
                            let _resp_span_guard = resp_span.enter();
                            trace!("Received new response from sink: {resp:?}");
                            match to_client
                                .send_timeout(Ok(resp), Duration::from_millis(200)) // FIXME
                                .await
                            {
                                Ok(()) => (/* Response queued to be sent to the client */),
                                Err(mpsc::error::SendTimeoutError::Timeout(_resp_res)) => {
                                    warn!("Timed out trying to queue response; dropping it");
                                }
                                Err(mpsc::error::SendTimeoutError::Closed(_resp_res)) => {
                                    warn!("Client appears disconnected; exiting");
                                    break;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(num_lost)) => {
                            warn!(lost_messages = num_lost, "Lagging behind");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            warn!("Sink's sending half has been closed; exiting");
                            break;
                        }
                    }
                }
            }
            .instrument(info_span!(
                "sink-worker",
                "id={sink_worker_id}, remote_addr={remote_addr}",
            )),
        );

        Ok(::tonic::Response::new(TimestampingStream::new(
            ReceiverStream::new(from_sink_worker),
            HEADER_KEY_SINK_TIMESTAMP_NS,
        )))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Sink<Resp = Response> {
    addr: SocketAddr,
    buffer_size: usize,

    tx_for_snaplace_workers: mpsc::Sender<Resp>,
    from_snaplace_workers: mpsc::Receiver<Resp>,

    to_sink_workers: broadcast::Sender<Resp>,
}

impl Sink {
    /// Create a new `Sink`, with the given `buffer_size` for its channels, which serves on the
    /// given `addr`.
    ///
    /// # Panics
    ///
    /// When [`ToSocketAddrs::to_socket_addrs`] returns an empty iterator, which probably happens
    /// when/if any required name resolution fails.
    pub fn new(addr: impl ToSocketAddrs, buffer_size: usize) -> Result<Self, io::Error> {
        let addr = addr
            .to_socket_addrs()
            .map_err(|err| {
                io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("failed to convert input to SocketAddr: {err:#}"),
                )
            })?
            .collect::<Vec<_>>()
            .remove(0);

        let (tx_for_snaplace_workers, from_snaplace_workers) = mpsc::channel(buffer_size);
        let (to_sink_workers, _) = broadcast::channel(buffer_size);

        Ok(Self {
            addr,
            buffer_size,
            tx_for_snaplace_workers,
            from_snaplace_workers,
            to_sink_workers,
        })
    }

    /// Sink's shutting down procedure when receiving a quit notification.
    #[instrument(level = Level::DEBUG, skip_all)]
    #[cold]
    async fn shutdown(
        mut self,
        server_handle: JoinHandle<Result<(), ::tonic::transport::Error>>,
    ) -> Result<(), Error> {
        // As long as I, `Sink`, hold a tx half (necessary earlier to clone and pass it to anyone
        // that asks), the associated rx half (i.e., `self.from_snaplace_workers`) never returns
        // `None`, so `Orchestrator::spawn` hangs. For that reason, I drop my tx half first...
        drop(self.tx_for_snaplace_workers);

        // ...and then drain snaplace Workers' channel until it is closed (which indicates that all
        // Workers and SandboxPool have exited too).
        while let Some(resp) = self.from_snaplace_workers.recv().await {
            match self.to_sink_workers.send(resp) {
                Ok(num_workers) => {
                    trace!("Broadcasted response to {num_workers} sink-workers");
                }
                Err(broadcast::error::SendError(resp)) => {
                    error!(resp = ?resp, "No sink-workers alive; dropping response");
                }
            }
        }
        info!("Stopped receiving Responses from snaplace Workers");

        // Reaching this means that SandboxPool has exited (so all its Workers have exited too).
        // Now poll until our sink-workers (one per client connection) are done forwarding the
        // responses...
        while !self.to_sink_workers.is_empty() {
            ::tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // ...and drop the tx half so that sink-workers can die, thus closing all connections.
        drop(self.to_sink_workers);
        // Give them some more time? I don't know if this makes any sense, but it doesn't matter at
        // this point:
        ::tokio::time::sleep(Duration::from_millis(100)).await;
        // So finally, stop gRPC serving and exit.
        server_handle.abort();
        match server_handle.await {
            Ok(Ok(())) => trace!("gRPC server exited normally"), // XXX: unreachable, right?
            Ok(Err(srv_err)) => warn!(
                error = ?srv_err,
                "gRPC server joined normally but exited with error: {srv_err:?}",
            ),
            Err(join_err) if join_err.is_cancelled() => debug!("gRPC server task cancelled"),
            Err(join_err) => warn!(
                error = ?join_err,
                "error joining gRPC server task: {join_err:?}",
            ),
        }
        // FIXME: Always return Ok?
        Ok(())
    }
}

#[async_trait]
impl ::snaplace::response::Sink for Sink {
    type Response = Response;

    #[inline]
    fn tx(&self) -> mpsc::Sender<Self::Response> {
        self.tx_for_snaplace_workers.clone()
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn run(mut self, mut quit_rx: broadcast::Receiver<()>) -> Result<(), Error> {
        let server_handle = ::tokio::spawn({
            let buffer_size = self.buffer_size;
            let sink_broadcast = self.to_sink_workers.clone();
            let addr = self.addr;

            async move {
                Server::builder()
                    //.max_concurrent_streams(1)
                    //.initial_stream_window_size(4096)
                    //.initial_connection_window_size(4096)
                    //.max_frame_size(None)
                    .add_service(SinkServer::new(SinkGrpcServer::new(
                        4 * buffer_size,
                        sink_broadcast,
                    )))
                    .serve(addr)
                    .await
            }
        });

        loop {
            ::tokio::select! {
                quit_res = quit_rx.recv() => {
                    match quit_res {
                        Ok(()) => warn!("Response sink received a quit notification"),
                        Err(err) => error!(error = ?err, "Failed to receive from quit_rx: {err}"),
                    }
                    break;
                }
                opt_resp = self.from_snaplace_workers.recv() => {
                    // NOTE: If snaplace Workers' channel has been closed, it is probably time
                    // to exit, since SandboxPool itself should be holding the last sending half.
                    let resp = match opt_resp {
                        Some(resp) => resp,
                        None => {
                            warn!("Snaplace Workers' channel appears to be closed");
                            break;
                        },
                    };
                    match self.to_sink_workers.send(resp) {
                        Ok(num_workers) => {
                            trace!("Broadcasted response to {num_workers} sink-workers");
                        },
                        Err(broadcast::error::SendError(resp)) => {
                            error!(response = ?resp, "No sink-workers alive; dropping response");
                        },
                    }
                }
            }
        }

        self.shutdown(server_handle).await
    }
}
