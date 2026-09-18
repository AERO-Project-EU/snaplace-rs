use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tracing::{error, info, instrument, warn, Level};

use faasrail_loadgen::sink::backend::Backend;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("I/O error: {msg}")]
    Io {
        msg: Box<str>,
        #[source]
        err: Option<io::Error>,
    },

    #[error("failed to initialize the gRPC endpoint")]
    TonicTransport(#[source] ::tonic::transport::Error),

    #[error("gRPC failure")]
    TonicStatus(#[source] Box<::tonic::Status>),
}

#[derive(Debug)]
pub struct SnaplaceGrpcSink {
    addr: SocketAddr,
}

impl SnaplaceGrpcSink {
    pub fn new(addr: impl ToSocketAddrs) -> Result<Self, Error> {
        let addr = addr
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

        Ok(Self { addr })
    }
}

impl Backend for SnaplaceGrpcSink {
    type Error = Error;
    type Response = ::snaplace_grpc::Response;

    #[instrument(level = Level::INFO, skip_all)]
    async fn run(
        self,
        to_appender: mpsc::Sender<Self::Response>,
        mut quit_rx: broadcast::Receiver<()>,
    ) -> Result<u64, Self::Error> {
        #[inline]
        async fn handle_res(
            res: Result<Option<::snaplace_grpc::Response>, ::tonic::Status>,
            num_resps: &mut u64,
            to_appender: &mpsc::Sender<::snaplace_grpc::Response>,
        ) -> Result<(), ()> {
            match res {
                Ok(Some(resp)) => {
                    info!(%resp.invocation_id, %resp.status_code);
                    *num_resps += 1;
                    if let Err(err) = to_appender
                        .send_timeout(resp, Duration::from_millis(250))
                        .await
                    {
                        error!(error = ?err, "Failed to send to appender: {err:#}");
                    }
                    return Ok(());
                }
                Ok(None) => info!("Response stream closed"),
                Err(status) => error!(?status, "Response stream failure from remote"),
            }
            Err(())
        }

        // Create the client and start the server-side streaming RPC to FaaSCell
        let mut client = ::snaplace_grpc::SinkClient::connect(format!("http://{}", self.addr))
            .await
            .map_err(Error::TonicTransport)?;
        let mut resp_stream = client
            .snaplace_responses(::snaplace_grpc::Subscribe {})
            .await
            .map_err(|err| Error::TonicStatus(Box::new(err)))?
            .into_inner();

        // Receive Responses and forward them to FileAppender until a quit notification is received
        let mut num_resps = 0;
        loop {
            ::tokio::select! {

                res = quit_rx.recv() => {
                    warn!(received = ?res, "Notification from quit channel");
                    break;
                }

                res = resp_stream.try_next() => {
                    if handle_res(res, &mut num_resps, &to_appender).await.is_err() {
                        break;
                    }
                }

            }
        }

        // Now that a quit notification has been received, attempt to drain the stream from
        // Responses of possible ongoing Function invocations, until the stream looks idle for
        // the specified duration.
        info!("Draining Response stream...");
        const TICK: Duration = Duration::from_secs(3);
        const MAX_TICKS: u32 = 15;
        let mut ticker = ::tokio::time::interval(TICK);
        let mut __num_resps_last_checked = 0;
        let mut __num_ticks_idle = 0;
        loop {
            ::tokio::select! {

                res = resp_stream.try_next() => {
                    if handle_res(res, &mut num_resps, &to_appender).await.is_err() {
                        break;
                    }
                }

                _now = ticker.tick()=> {
                    // Wait until no new Responses have arrived from the stream
                    // for the last MAX_TICKS ticks before stopping checking it
                    if num_resps > __num_resps_last_checked {
                        __num_ticks_idle = 0;
                        __num_resps_last_checked = num_resps;
                        info!(
                            "Waiting until Response stream is idle for {:?}",
                            TICK * MAX_TICKS
                        );
                    } else {
                        __num_ticks_idle += 1;
                        ::tracing::event!(
                            Level::INFO,
                            "idle.for" = ?__num_ticks_idle * TICK,
                            "exiting.in" = ?(MAX_TICKS - __num_ticks_idle) * TICK,
                        );
                    }
                    if __num_ticks_idle == MAX_TICKS {
                        break;
                    }
                }

            }
        }

        info!("Exiting...");
        Ok::<_, Error>(num_resps)
    }
}

::static_assertions::assert_impl_all!(SnaplaceGrpcSink: Send);
::static_assertions::assert_impl_all!(::faasrail_loadgen::sink::SinkClient<SnaplaceGrpcSink>: Send);
