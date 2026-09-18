use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};
use tracing::{instrument, warn, Level};

use crate::request::{toy::req::ToyStringRequest, Error, Source};

#[derive(Debug)]
pub struct ToyReqGen {
    curr: u64,
    limit: u64,
}

impl ToyReqGen {
    pub fn new(limit: u64) -> Self {
        Self { curr: 0, limit }
    }
}

#[async_trait]
impl Source for ToyReqGen {
    type Request = ToyStringRequest;

    #[instrument(level = Level::TRACE, skip_all, fields(limit = self.limit), ret, err(Display))]
    async fn run(
        mut self,
        to_dispatcher: mpsc::Sender<Self::Request>,
        mut quit_rx: broadcast::Receiver<()>,
    ) -> Result<(), Error> {
        while self.curr < self.limit {
            ::tokio::select! {
                biased;
                quit_res = quit_rx.recv() => {
                    match quit_res {
                        Ok(()) => warn!("request source exiting after receiving a quit signal!"),
                        Err(err) => return Err(Error::Receive {
                            msg: "failed to receive from quit_rx".to_string().into_boxed_str(),
                            source: err
                        }),
                    }
                    break
                }
                send_res = to_dispatcher.send(format!("req{:09}", self.curr).into()) => {
                    if let Err(err) = send_res {
                        return Err(Error::Send {
                            msg: format!("failed to send new request down the channel: {err}")
                                .into_boxed_str(),
                        })
                    }
                }
            }
            self.curr += 1;
        }
        Ok(())
    }
}
