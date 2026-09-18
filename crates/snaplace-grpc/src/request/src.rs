use std::{
    io::{self, ErrorKind},
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    sync::{broadcast, mpsc},
    time::sleep,
};
use tokio_stream::StreamExt;
use tonic::{transport::Server, Streaming};
use tracing::{error, info, instrument, trace, warn, Level};

use snaplace::request::{Error, HEADER_KEY_SOURCE_TIMESTAMP_NS};

use crate::{
    pb::{self, request::source_server::SourceServer},
    timestamping::TimestampingStream,
    Request,
};

#[derive(Debug)]
struct SourceGrpcServer<Req = Request> {
    to_dispatcher: mpsc::Sender<Req>,
}

impl<Req> SourceGrpcServer<Req> {
    const THROTTLE_DURATION: Duration = Duration::from_millis(100);

    fn new(to_dispatcher: mpsc::Sender<Req>) -> Self {
        Self { to_dispatcher }
    }
}

#[async_trait]
impl pb::request::source_server::Source for SourceGrpcServer<Request> {
    async fn snaplace_requests(
        &self,
        request: ::tonic::Request<Streaming<Request>>,
    ) -> Result<::tonic::Response<pb::request::Ack>, ::tonic::Status> {
        let mut stream =
            TimestampingStream::new(request.into_inner(), HEADER_KEY_SOURCE_TIMESTAMP_NS);
        while let Some(req) = stream.next().await {
            trace!("Just received {req:?} from the network stream");
            let req = match req {
                Ok(req) => req,
                Err(err) => {
                    error!(error = ?err, "Failed to receive a Request from the network: {err:#}");
                    // ¿FIXME: Just throttle and continue?
                    sleep(Self::THROTTLE_DURATION).await;
                    continue;
                }
            };
            trace!("Sending {req:?} to Dispatcher's channel...");
            if let Err(err) = self.to_dispatcher.send(req).await {
                error!("Failed to send the request to Dispatcher: {err:#}");
                // ¿FIXME: Just throttle and continue?
                sleep(Self::THROTTLE_DURATION).await;
            }
        }
        Ok(::tonic::Response::new(pb::request::Ack {}))
    }
}

#[derive(Debug)]
pub struct Source {
    addr: SocketAddr,
}

impl Source {
    /// Create a new `Source` that serves on the given `addr`.
    ///
    /// # Panics
    ///
    /// When [`ToSocketAddrs::to_socket_addrs`] returns an empty iterator, which probably happens
    /// when/if any required name resolution fails.
    pub fn new(addr: impl ToSocketAddrs) -> Result<Self, io::Error> {
        Ok(Self {
            addr: addr
                .to_socket_addrs()
                .map_err(|err| {
                    io::Error::new(
                        ErrorKind::InvalidInput,
                        format!("failed to convert input to SocketAddr: {err:#}"),
                    )
                })?
                .collect::<Vec<_>>()
                .remove(0),
        })
    }
}

#[async_trait]
impl ::snaplace::request::Source for Source {
    type Request = Request;

    #[instrument(level = Level::TRACE, skip_all, err(Display))]
    async fn run(
        self,
        to_dispatcher: mpsc::Sender<Self::Request>,
        mut quit_rx: broadcast::Receiver<()>,
    ) -> Result<(), Error> {
        let serve_grpc = Server::builder()
            //.max_concurrent_streams(1)
            //.initial_stream_window_size(4096)
            //.initial_connection_window_size(4096)
            //.max_frame_size(None)
            .add_service(SourceServer::new(SourceGrpcServer::new(to_dispatcher)))
            .serve(self.addr);

        ::tokio::select! {
            quit_res = quit_rx.recv() => {
                match quit_res {
                    Ok(()) => warn!("request source exiting after receiving a quit signal!"),
                    Err(err) => return Err(Error::Receive {
                        msg: String::from("failed to receive from quit_rx").into_boxed_str(),
                        source: err
                    }),
                }
            }
            srv_res = serve_grpc => {
                match srv_res {
                    Ok(()) => info!("Done serving gRPC"),
                    Err(err) => {
                        error!(error = ?err, "Error while serving gRPC: {err}");
                        return Err(Error::Runtime {
                            msg: String::from("error while serving gRPC").into_boxed_str(),
                            source: Box::new(err),
                        });
                    }
                }
            }
        }

        Ok(())
    }
}
