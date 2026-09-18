use std::{io, net::ToSocketAddrs, time::Duration};

use tokio::{
    sync::mpsc,
    task::{JoinError, JoinHandle},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;
use tracing::{error, info, instrument, trace, warn_span, Instrument, Level};

use faasrail_loadgen::{source::backend::Backend, InvocationId, WorkloadRequest};

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("gRPC client's channel's rcv half is closed")]
    Channel,

    #[error("I/O error: {msg}")]
    Io {
        msg: Box<str>,
        #[source]
        err: Option<io::Error>,
    },

    #[error("request forwarding timed out; channel is full")]
    Timeout,

    #[error("failed to initialize the gRPC endpoint")]
    TonicTransport(#[source] ::tonic::transport::Error),

    #[error("gRPC failure")]
    TonicStatus(#[source] Box<::tonic::Status>),
}

#[derive(Debug)]
pub struct SnaplaceGrpcSource {
    to_client: mpsc::Sender<::snaplace_grpc::Request>,
    handle: JoinHandle<Result<(), Error>>,
}

impl SnaplaceGrpcSource {
    pub fn spawn(addr: impl ToSocketAddrs) -> Result<Self, Error> {
        let src_addr = addr
            .to_socket_addrs()
            .map_err(|err| Error::Io {
                msg: "failed to resolve input source address".into(),
                err: Some(err),
            })?
            .next()
            .ok_or_else(|| Error::Io {
                msg: "failed to resolve input source address".into(),
                err: None,
            })?;

        let ep = Endpoint::from_shared(format!("http://{src_addr}"))
            .map_err(Error::TonicTransport)?
            .connect_lazy();
        let mut client = ::snaplace_grpc::SourceClient::new(ep);

        let (to_client, from_worker) = mpsc::channel(1 << 15); // FIXME: chan cap?
        let handle = ::tokio::spawn(
            async move {
                let _resp_ack = client
                    .snaplace_requests(ReceiverStream::new(from_worker))
                    .await
                    .map_err(|err| Error::TonicStatus(Box::new(err)))?;
                info!("Client-side streaming ended");
                Ok(())
            }
            .instrument(warn_span!("snaplace-grpc-client")),
        );

        Ok(Self { to_client, handle })
    }

    pub async fn reap(self) -> Result<Result<(), Error>, JoinError> {
        self.handle.await
    }

    pub async fn abort(self) {
        self.handle.abort();
    }

    pub fn new_ref(&self) -> SnaplaceGrpcClientRef {
        SnaplaceGrpcClientRef {
            to_client: self.to_client.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnaplaceGrpcClientRef {
    to_client: mpsc::Sender<::snaplace_grpc::Request>,
}

impl Backend for SnaplaceGrpcClientRef {
    type Error = Error;

    #[instrument(level = Level::INFO, skip(self, wreq))]
    #[inline]
    async fn issue(
        &mut self,
        invocation_id: InvocationId,
        wreq: &WorkloadRequest,
        minute: u16,
        timeout: Duration,
    ) -> Result<(), Self::Error> {
        let req = ::snaplace_grpc::Request {
            invocation_id: invocation_id.into_string(),
            function_id: wreq.bench.as_str().into(),
            metadata_map: Default::default(),
            payload: wreq.payload.clone().into(),
        };

        match self.to_client.send_timeout(req, timeout).await {
            Ok(()) => Ok(()),
            Err(mpsc::error::SendTimeoutError::Timeout(req)) => {
                error!("Request forwarding timed out: the channel is full");
                trace!(?req);
                Err(Error::Timeout)
            }
            Err(mpsc::error::SendTimeoutError::Closed(req)) => {
                error!("gRPC client's channel's rcv half is closed!");
                trace!(?req);
                Err(Error::Channel)
            }
        }
    }
}
