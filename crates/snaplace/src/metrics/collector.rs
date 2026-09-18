// TODO(ckatsak): Now that Collector's key is always `crate::metrics::Timing`, should it be named
// `TimingsCollector` or something, so that we can have some other kind of `MetricsCollector` in
// the future?

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::OpenOptions,
    io::{self, BufWriter, Write},
    os::{fd::AsFd, unix::fs::OpenOptionsExt},
    path::Path,
    time::Duration,
};

use compact_str::{CompactString, ToCompactString};
use enum_map::EnumMap;
use num_traits::ToPrimitive;
use rustix::fs::{fchmod, Mode};
use serde::{ser::SerializeMap, Serialize};
use tokio::{
    sync::{
        mpsc::{self, error::SendTimeoutError},
        oneshot,
    },
    task::JoinHandle,
};
use tracing::{debug, error, info, info_span, instrument, warn, Level};

use crate::{
    metrics::{RollingStats, Timing},
    FunctionId, InvocationId,
};

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("{msg}: I/O error")]
    Io {
        msg: Box<str>,
        #[source]
        err: io::Error,
    },
}

#[derive(Debug, Clone)]
struct InvocationMetrics<V> {
    function_id: FunctionId,
    sandbox_id: CompactString,
    invocation_id: InvocationId,
    metrics: EnumMap<Timing, V>,
}

/// Serialized as `{"$INVOCATION_ID":{"sandbox_id":"$SANDBOX_ID", ...$TIMINGS }}`, with
/// `$SANDBOX_ID` being an empty string when missing.
impl<V: Serialize> Serialize for InvocationMetrics<V> {
    fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        /// Auxiliary struct to efficiently serialize SandboxId into a single flat keyspace
        /// along with Timings, as the value part of the JSONL entry for the invocation.
        #[derive(Debug)]
        struct InvocationMetricsValue<'a, V> {
            sandbox_id: &'a str,
            metrics: &'a EnumMap<Timing, V>,
        }

        impl<V: Serialize> Serialize for InvocationMetricsValue<'_, V> {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut map = serializer.serialize_map(Some(1 + self.metrics.len()))?;
                map.serialize_entry("sandbox_id", self.sandbox_id)?;
                for (timing, value) in self.metrics {
                    map.serialize_entry(&timing, value)?;
                }
                map.end()
            }
        }

        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(
            &self.invocation_id,
            &InvocationMetricsValue {
                sandbox_id: &self.sandbox_id,
                metrics: &self.metrics,
            },
        )?;
        map.end()
    }
}

#[derive(Debug)]
pub(crate) struct MetricsCollector<V>
where
    V: Default + Serialize + Send + Debug,
{
    // TODO(ckatsak): For now, we only keep track of a single timing (i.e., `Timing::Issuer`), but
    // should we want to keep track of more of them, we need to be careful about what kind of data
    // structure we introduce, because `RollingStats` is huge (352(680) B in release(debug) mode).
    rolling_timing: HashMap<FunctionId, RollingStats<V>, crate::BuildHasher>,

    rx: mpsc::Receiver<MetricMessage<V>>,

    to_writer: mpsc::Sender<InvocationMetrics<V>>,
    writer_handle: JoinHandle<Result<(), Error>>,

    quit_rx: mpsc::Receiver<()>,
}

#[derive(Debug)]
enum MetricMessage<V>
where
    V: Default + Serialize + Send + Debug,
{
    /// Store new metrics for an invocation.
    StoreMetrics(InvocationMetrics<V>),

    /// Get any rolling metrics that may have been collected for a Function so far.
    GetRollingMetrics {
        function_id: FunctionId,
        respond_to: oneshot::Sender<Option<RollingStats<V>>>,
    },
}

#[derive(Debug)]
pub(crate) struct MetricsCollectorHandle<V>
where
    V: Default + Serialize + Send + Debug,
{
    to_collector: mpsc::Sender<MetricMessage<V>>,
    quit_tx: mpsc::Sender<()>,
    handle: JoinHandle<Result<(), Error>>,
}

#[derive(Debug, Clone)]
pub(crate) struct MetricsCollectorRef<V>
where
    V: Default + Serialize + Send + Debug,
{
    to_collector: mpsc::Sender<MetricMessage<V>>,
}

impl<V> MetricsCollector<V>
where
    V: Default + Clone + ToPrimitive + Serialize + Send + 'static + Debug,
{
    /// The capacity of the channel between [`Worker`]s and the `MetricsCollector`.
    const COLLECTOR_CHANNEL_SIZE: usize = 4096;

    /// The initial capacity of the internal [`HashMap`] where `MetricsCollector` does the
    /// bookkeeping of per-Function rolling stats.
    const INIT_NUM_FUNCTIONS: usize = 1 << 9;

    /// The capacity of the channel between the `MetricsCollector` and its writer thread.
    ///
    /// NOTE(ckatsak): Let's keep this small for now, so that we identify quickly if/when
    /// the `MetricsCollector` can ever really be bottleneck for the `Worker`s.
    const WRITER_CHANNEL_SIZE: usize = 1; // FIXME?
    /// The capacity of the buffer used internally by writer thread's [`BufWriter`].
    const WRITER_BUFSZ: usize = 1 << 14; // 16KiB

    /// The (octal representation of the) [`Mode`] of the (output) timings file.
    const TIMINGS_FILE_MODE: u32 = 0o664;

    #[instrument(level = Level::TRACE, skip_all, fields(path = %path.as_ref().display()))]
    pub fn spawn(path: impl AsRef<Path>) -> MetricsCollectorHandle<V> {
        let (to_collector, rx) = mpsc::channel(Self::COLLECTOR_CHANNEL_SIZE);
        let (quit_tx, quit_rx) = mpsc::channel(1);

        let (to_writer, mut from_collector) = mpsc::channel(Self::WRITER_CHANNEL_SIZE);
        let writer_handle = ::tokio::task::spawn_blocking({
            let path = path.as_ref().to_path_buf();

            move || {
                let thread_span = info_span!("metrics_writer", target_file = ?path);
                let _span_guard = thread_span.enter();

                let mut f = OpenOptions::new()
                    .mode(Self::TIMINGS_FILE_MODE) // but also later chmod(2) for umask
                    .append(true)
                    .create(true)
                    .truncate(false)
                    .open(&path)
                    .map_err(|err| Error::Io {
                        msg: format!("failed to open file '{}' for appending", path.display())
                            .into_boxed_str(),
                        err,
                    })
                    .inspect_err(|err| {
                        error!(error = ?err, "Failed to open file '{}': {err:#}", path.display())
                    })?;

                fchmod(f.as_fd(), Mode::from(Self::TIMINGS_FILE_MODE)).map_err(|err| {
                    error!(
                        "Failed to fchmod('{}', {:#o}): {err:#}",
                        path.display(),
                        Self::TIMINGS_FILE_MODE
                    );
                    Error::Io {
                        msg: format!(
                            "failed to fchmod('{}', {:#o})",
                            path.display(),
                            Self::TIMINGS_FILE_MODE
                        )
                        .into_boxed_str(),
                        err: err.into(),
                    }
                })?;

                let mut bw = BufWriter::with_capacity(Self::WRITER_BUFSZ, &mut f);

                info!("Opened file for appending; now entering main loop");
                // This writer thread should exit when the Collector's sending half is dropped
                // and drained..
                while let Some(metrics) = from_collector.blocking_recv() {
                    if let Err(err) = ::serde_json::to_writer(&mut bw, &metrics) {
                        warn!(error = ?err, ?metrics, "Failed to serialize metrics: {err:#}");
                    } else if let Err(err) = bw.write_all(b"\n") {
                        warn!(error = ?err, ?metrics, "Failed to append new line: {err:#}");
                    }
                    // NOTE(ckatsak): I guess we do not really need to flush the buffer
                    // on every measurement, since the privilege domain switch (due to
                    // write(2)) may still be costly (i.e., despite the page cache).
                }

                info!("Attempting to exit gracefully: flushing buffer & fsync(2)'ing file");
                bw.into_inner()
                    .map_err(|err| Error::Io {
                        msg: "failed to flush buffer".into(),
                        err: err.into_error(),
                    })
                    .and_then(|f| {
                        f.sync_all().map_err(|err| Error::Io {
                            msg: "failed to fsync(2) metrics file".into(),
                            err,
                        })
                    })
            }
        });

        let collector = Self {
            rolling_timing: HashMap::with_capacity_and_hasher(
                Self::INIT_NUM_FUNCTIONS,
                crate::BuildHasher::default(),
            ),
            rx,
            to_writer,
            writer_handle,
            quit_rx,
        };
        let handle = ::tokio::spawn(async move { collector.run().await });

        MetricsCollectorHandle {
            to_collector,
            quit_tx,
            handle,
        }
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn run(mut self) -> Result<(), Error> {
        loop {
            ::tokio::select! {
                _ = self.quit_rx.recv() => break,
                msg_opt = self.rx.recv() => {
                    if let Some(msg) = msg_opt {
                        self.handle_metric_msg(msg).await;
                    } else {
                        // Reaching here means that all sender halves (including the one in the
                        // owning handle) have been dropped, so it's probably time to go. This
                        // is not possible in regular cases, but I haven't thought if it may
                        // occur in any case of failure.
                        break
                    }
                }
            }
        }

        drop(self.to_writer);
        debug!("Reaping the writer thread...");
        match self.writer_handle.await {
            Ok(Ok(())) => debug!("Writer thread joined successfully"),
            Ok(Err(err)) => error!(error = ?err, "Writer thread joined with error: {err:#}"),
            Err(jerr) => error!(error = ?jerr, "Failed to join writer thread: {jerr:#}"),
        }

        debug!("Exiting...");
        Ok(())
    }

    #[instrument(level = Level::TRACE, skip(self))]
    #[inline]
    async fn handle_metric_msg(&mut self, msg: MetricMessage<V>) {
        match msg {
            MetricMessage::StoreMetrics(invocation_metrics) => {
                if let Err(err) = self.to_writer.send(invocation_metrics.clone()).await {
                    // FIXME(ckatsak): If the writer thread is dead, there is no point in
                    // collecting metrics. For now though, just log the error and move on
                    error!(error = ?err, "Error forwarding metrics to writer thread: {err:#}");
                }
                self.rolling_timing
                    .entry(invocation_metrics.function_id)
                    .or_default()
                    .add(invocation_metrics.metrics[Timing::Issuer].clone());
            }
            MetricMessage::GetRollingMetrics {
                function_id,
                respond_to,
            } => {
                let rs = self.rolling_timing.get(&function_id).cloned();
                if let Err(_rs) = respond_to.send(rs) {
                    error!("Failed to respond to rolling stats query; receiver dropped?");
                }
            }
        }
    }
}

impl<V> MetricsCollectorHandle<V>
where
    V: Default + Serialize + Send + Debug,
{
    /// Create a [`MetricsCollectorRef`] to send [`MetricMessage`]s to this [`MetricsCollector`].
    #[inline]
    pub fn new_ref(&self) -> MetricsCollectorRef<V> {
        MetricsCollectorRef {
            to_collector: self.to_collector.clone(),
        }
    }

    pub async fn shutdown(self) -> Result<Result<(), Error>, ::tokio::task::JoinError> {
        if let Err(err) = self.quit_tx.send(()).await {
            warn!(error = ?err, "Failed to send quit signal to MetricsCollector: {err:#}");
        }
        self.handle.await
    }
}

impl<V> MetricsCollectorRef<V>
where
    V: Default + Serialize + Send + Debug,
{
    const CHANNEL_TIMEOUT: Duration = Duration::from_millis(200);

    async fn retry_send(&self, msg: MetricMessage<V>) {
        // First identify whether there is already a bottleneck at MetricsCollector's channel...
        let mut msg = match self.to_collector.try_send(msg) {
            Ok(()) => return,
            Err(mpsc::error::TrySendError::Full(msg)) => {
                warn!("MetricsCollector's channel is full!");
                msg
            }
            // FIXME(ckatsak): No point in retrying if the channel is closed
            Err(mpsc::error::TrySendError::Closed(msg)) => msg,
        };

        // ...if indeed there is, just keep retrying...
        while let Err(err) = self
            .to_collector
            .send_timeout(msg, Self::CHANNEL_TIMEOUT)
            .await
        {
            warn!(error = ?err, "Failed to contact MetricsCollector: {err:#}");
            msg = match err {
                // FIXME(ckatsak): No point in retrying if the channel is closed
                SendTimeoutError::Closed(msg) | SendTimeoutError::Timeout(msg) => msg,
            };
            debug!("Retrying to send {msg:?} to MetricsCollector...");
        }
    }

    /// Store new metrics for an invocation.
    ///
    /// # Warning
    ///
    /// The [`MetricsCollector`] assumes that empty strings are not valid Sandbox IDs, hence
    /// serializes `sandbox_id.is_none()` as `""`.
    #[inline]
    pub async fn store_all(
        &self,
        function_id: FunctionId,
        sandbox_id: Option<&str>,
        invocation_id: InvocationId,
        metrics: EnumMap<Timing, V>,
    ) {
        debug_assert_ne!(sandbox_id, Some(""), "Sandbox ID cannot be empty string");
        self.retry_send(MetricMessage::StoreMetrics(InvocationMetrics {
            function_id,
            sandbox_id: sandbox_id.unwrap_or("").to_compact_string(),
            invocation_id,
            metrics,
        }))
        .await
    }

    /// Get any rolling metrics that may have been collected for a Function so far.
    #[inline]
    pub async fn get_metrics(&self, function_id: FunctionId) -> Option<RollingStats<V>> {
        let (respond_to, rx) = oneshot::channel();
        self.retry_send(MetricMessage::GetRollingMetrics {
            function_id,
            respond_to,
        })
        .await;
        rx.await
            .inspect_err(
                |err| error!(error = ?err, "Failed to receive from MetricsCollector: {err:#}"),
            )
            .ok()
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Context;
    use compact_str::{format_compact, CompactString};
    use enum_map::EnumMap;
    use tracing::info;
    use tracing_test::traced_test;

    use super::InvocationMetrics;
    use crate::{
        metrics::{collector::RollingStats, Nanoseconds, Timing},
        FunctionId,
    };

    #[test]
    #[traced_test]
    fn invocation_metrics_serde() -> ::anyhow::Result<()> {
        let iid = format_compact!("invocation_serde_01");
        let function_id = FunctionId::from("test-function-id");
        let cases = [
            (CompactString::from("test-sandbox-id"), "test-sandbox-id"),
            (CompactString::from(""), ""),
        ];

        for (sandbox_id, sandbox_id_expected) in cases {
            let timings = EnumMap::<Timing, Nanoseconds>::default();
            let timings_value = ::serde_json::to_value(timings)
                .with_context(|| format!("failed to serialize {timings:?}"))?;

            let im = InvocationMetrics {
                function_id: function_id.clone(),
                sandbox_id: sandbox_id.clone(),
                invocation_id: iid.clone(),
                metrics: timings,
            };
            let im = ::serde_json::to_string(&im)
                .with_context(|| format!("failed to serialize {im:?}"))?;
            info!("custom ::serde::Serialize impl:\n{im}");
            let im_value: ::serde_json::Value =
                ::serde_json::from_str(&im).context("failed to parse serialized metrics JSON")?;

            let mut expected_inner = ::serde_json::Map::with_capacity(
                1 + timings_value.as_object().map_or(0, |m| m.len()),
            );
            expected_inner.insert(
                "sandbox_id".to_string(),
                ::serde_json::Value::String(sandbox_id_expected.to_string()),
            );
            if let ::serde_json::Value::Object(timings_obj) = timings_value {
                for (key, value) in timings_obj {
                    expected_inner.insert(key, value);
                }
            } else {
                anyhow::bail!("timings did not serialize to a JSON object");
            }
            let mut expected_outer = ::serde_json::Map::with_capacity(1);
            expected_outer.insert(iid.to_string(), ::serde_json::Value::Object(expected_inner));
            let expected = ::serde_json::Value::Object(expected_outer);

            assert_eq!(im_value, expected);
        }

        Ok(())
    }

    // Here mostly for compile-time checks for now
    #[test]
    fn inspect_rolling_in_maps() {
        let _timings = EnumMap::<Timing, RollingStats<u128>>::default();
        //let _underlying_enum_map_array: [FunctionMetrics<u128>; 256]      = [FunctionMetrics::<u128>::default(); 256];                    // size = 174080, align = 0x8
        //let _underlying_enum_map_array: [Box<FunctionMetrics<u128>>; 256] = [Box::<FunctionMetrics<u128>>::new(Default::default()); 256]; // size = 2048, align = 0x8
        //
        //let _map = HashMap::<FunctionId, EnumMap<Timing, Option<Box<FunctionMetrics<u128>>>>>::default();
        //let _underlying_enum_map_array: [Option<Box<FunctionMetrics<u128>>>; 256] = [None; 256];                                          // size = 2048, align = 0x8
        //
        let _map = HashMap::<(FunctionId, Timing), RollingStats<u128>, _>::with_hasher(
            crate::BuildHasher::default(),
        );
        //
        //let _em = EnumMap::<Timing, HashMap<FunctionId, FunctionMetrics<u128>>>::default();
        //let _underlying_enum_map_array: [HashMap<FunctionId, FunctionMetrics<u128>>; 256]              = [Default::default(); 256];       // size = 12288, align = 0x10
        //let _underlying_enum_map_array: [Option<HashMap<FunctionId, FunctionMetrics<u128>>>; 256]      = [None; 256];                     // size = 12288, align = 0x10
        //let _em = EnumMap::<Timing, Option<Box<HashMap<FunctionId, FunctionMetrics<u128>>>>>::default();
        //let _underlying_enum_map_array: [Option<Box<HashMap<FunctionId, FunctionMetrics<u128>>>>; 256] = [None; 256];                     // size = 2048, align = 0x8
        //
        let mut map = HashMap::<FunctionId, EnumMap<Timing, RollingStats<u128>>, _>::with_hasher(
            crate::BuildHasher::default(),
        );
        let _timings = map.entry("skata".into()).or_default();
        let entry = map.entry("skata".into());
        let _entry = entry.and_modify(|timings| {
            let fm = &mut timings[Timing::SandboxResponse];
            fm.add(1000);
        });
        let _fm = &mut map.get_mut("skata").unwrap()[Timing::SandboxResponse];
    }
}
