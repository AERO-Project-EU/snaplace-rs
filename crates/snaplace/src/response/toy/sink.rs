use std::{fmt::Debug, path::Path, time::SystemTime};

use async_trait::async_trait;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::{broadcast, mpsc},
};
use tracing::{instrument, warn, Level};

use crate::response::{
    toy::ToyStringResponse, Error, Response, Sink, HEADER_KEY_SINK_TIMESTAMP_NS,
};

#[derive(Debug)]
pub struct ToyRespSink<R: Response> {
    bfw: BufWriter<File>,
    count: usize,

    tx: mpsc::Sender<R>,
    rx: mpsc::Receiver<R>,
}

impl<R: Response> ToyRespSink<R> {
    const BUFFER_CAP: usize = 1 << 14; // 16 KiB

    pub async fn new(
        path: impl AsRef<Path>,
        chan_size: impl Into<Option<usize>>,
    ) -> Result<Self, Error> {
        let (tx, rx) = mpsc::channel(chan_size.into().unwrap_or(1));
        Ok(Self {
            bfw: BufWriter::with_capacity(
                Self::BUFFER_CAP,
                OpenOptions::new()
                    .read(false)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)
                    .await
                    .map_err(|err| Error::Runtime {
                        msg: format!("failed to open file {}", path.as_ref().display())
                            .into_boxed_str(),
                        source: Box::new(err),
                    })?,
            ),
            count: 0,
            tx,
            rx,
        })
    }
}

#[async_trait]
impl Sink for ToyRespSink<ToyStringResponse> {
    type Response = ToyStringResponse;

    #[inline]
    fn tx(&self) -> mpsc::Sender<Self::Response> {
        self.tx.clone()
    }

    #[instrument(level = Level::TRACE, skip_all)]
    async fn run(mut self, mut quit_rx: broadcast::Receiver<()>) -> Result<(), Error> {
        loop {
            ::tokio::select! {
                biased;
                quit_res = quit_rx.recv() => {
                    match quit_res {
                        Ok(()) => {
                            warn!("response sink flushing after receiving a quit signal!");
                            self.bfw.flush().await.map_err(|err| Error::Runtime {
                                msg: String::from("failed to flush data to underlying file")
                                    .into_boxed_str(),
                                source: Box::new(err),
                            })?;
                            warn!("response sink exiting after receiving a quit signal!");
                        }
                        Err(err) => return Err(Error::Receive {
                            msg: "failed to receive from quit_rx".to_string().into_boxed_str(),
                            source: err
                        }),
                    }
                    break
                }
                Some(mut resp) = self.rx.recv() => {
                    self.count += 1;
                    let _ = resp.metadata_map().insert(
                        HEADER_KEY_SINK_TIMESTAMP_NS.to_owned(),
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos()
                            .to_string(),
                    );
                    self.bfw
                        .write_all(format!("{resp:?}\n").as_bytes())
                        .await
                        .map_err(|err| Error::Runtime {
                            msg: format!("failed to write `{resp}`").into_boxed_str(),
                            source: Box::new(err),
                        })?;
                }
            }
        }
        Ok(())
    }
}
