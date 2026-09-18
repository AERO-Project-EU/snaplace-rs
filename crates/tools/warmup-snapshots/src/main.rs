use std::{
    collections::HashMap,
    io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, Context, Result};
use argh::FromArgs;
use compact_str::format_compact;
use dashmap::DashMap;
use futures::{stream::SelectAll, StreamExt};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::{broadcast, mpsc, Notify},
    task::JoinSet,
};
use tokio_stream::wrappers::SignalStream;
use tracing::{debug, error, info, info_span, instrument, trace, warn, Instrument, Level};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use faasrail_loadgen::{
    fixer::fix_fbpml_payload,
    sink::SinkBackend,
    source::{SourceBackend, SourceClient},
    InvocationId, WorkloadRequest,
};

use faasrail_snaplace_grpc::{
    sink::SnaplaceGrpcSink,
    source::{SnaplaceGrpcClientRef, SnaplaceGrpcSource},
};

const DEFAULT_SOURCE_ADDR: &str = "localhost:60051";
const DEFAULT_SINK_ADDR: &str = "localhost:60052";
const DEFAULT_MINIO_HOSTPORT: &str = "localhost:59000";
const DEFAULT_MINIO_BUCKET_NAME: &str = "snaplace-fbpml";

/// Utility to warm up FaaSCell (e.g., to allow it to pre-populate the devices with Sandbox
/// snapshots)
#[derive(Debug, Clone, FromArgs)]
struct Cli {
    /// path to input CSV file
    #[argh(option)]
    csv: PathBuf,

    /// number of Requests-Per-Minute that should trigger the warm-up (default: 1)
    #[argh(option, short = 't', default = "1")]
    rpm_trigger_point: u32,
    /// maximum number of concurrent requests to issue (maybe fewer, but no more)
    #[argh(option, short = 'c')]
    max_concurr_requests: u32,
    /// maximum duration to wait for the response of each request (default: 120sec)
    #[argh(option, from_str_fn(parse_dur), default = "Duration::from_secs(120)")]
    response_timeout: Duration,

    /// HOST:PORT formatted address of faascell's request source
    #[argh(option, default = "String::from(DEFAULT_SOURCE_ADDR)")]
    source_address: String,
    /// HOST:PORT formatted address of faascell's response sink
    #[argh(option, default = "String::from(DEFAULT_SINK_ADDR)")]
    sink_address: String,

    /// HOST:PORT formatted address of MinIO server
    #[argh(option, default = "String::from(DEFAULT_MINIO_HOSTPORT)")]
    minio_address: String,
    /// name of the MinIO bucket
    #[argh(option, default = "String::from(DEFAULT_MINIO_BUCKET_NAME)")]
    minio_bucket: String,
}

fn parse_dur(arg: &str) -> ::std::result::Result<Duration, String> {
    ::humantime::parse_duration(arg)
        .map_err(|err| format!("CLI failed to parse duration '{arg}': {err:#}"))
}

fn setup_signals_handler(shutdown: broadcast::Sender<()>) -> Result<()> {
    let mut signals = [
        ("ALRM", signal(SignalKind::alarm())),
        ("HUP", signal(SignalKind::hangup())),
        ("INT", signal(SignalKind::interrupt())),
        ("QUIT", signal(SignalKind::quit())),
        ("TERM", signal(SignalKind::terminate())),
        ("USR1", signal(SignalKind::user_defined1())),
        ("USR2", signal(SignalKind::user_defined2())),
        ("PIPE", signal(SignalKind::pipe())),
    ]
    .into_iter()
    .try_fold(SelectAll::new(), |mut sig_stream, (sig, s)| {
        sig_stream.push(SignalStream::new(
            s.with_context(|| format!("failed to setup listener for SIG{sig}"))?,
        ));
        Ok::<_, ::anyhow::Error>(sig_stream)
    })
    .context("failed to setup signal listeners")?;

    let _h = ::tokio::spawn(async move {
        while signals.next().await.is_some() {
            warn!("Signal received; sending shutdown notification");
            if let Err(err) = shutdown.send(()) {
                error!(error = ?err, "Failed to send shutdown notification!");
                panic!("failed to send shutdown notification: {err:#}");
            }
        }
    });

    Ok(())
}

fn prepare_requests(cli: Cli) -> Result<Vec<(WorkloadRequest, Vec<InvocationId>)>> {
    let timestamp = format!(
        "{:010}", // 10 digits
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("cannot be earlier than UNIX_EPOCH")
            .as_secs()
    );
    SourceClient::parse_csv(&cli.csv)
        .with_context(|| format!("failed to parse input CSV at {:?}", cli.csv))?
        .into_iter()
        .enumerate()
        .filter_map(|(i, row)| match row.rpm.iter().max() {
            None => Some(Err(anyhow!("found empty RPM array (Function {i})"))),
            Some(&max_rpm) if max_rpm < cli.rpm_trigger_point => None,
            Some(&max_rpm) => Some(
                ::serde_json::from_str::<WorkloadRequest>(&row.mapped_wreq)
                    .with_context(|| {
                        format!(
                            "failed to deserialize WorkloadRequest from {:?} (Function {i})",
                            row.mapped_wreq
                        )
                    })
                    .map(|wreq| (wreq, max_rpm.min(cli.max_concurr_requests))),
            ),
        })
        .collect::<Result<Vec<_>, _>>()
        .context("error while processing the CSV input")?
        .into_iter()
        .map(|(mut wreq, num_reqs)| {
            fix_fbpml_payload(&mut wreq, &cli.minio_address, &cli.minio_bucket)
                .with_context(|| format!("failed to fix_fbpml_payload for {wreq:?}"))?;
            let invoc_ids = (0..num_reqs)
                .map(|i| format_compact!("WARMUP-{timestamp}-{}-{i:03}", wreq.bench))
                .collect();
            Ok((wreq, invoc_ids))
        })
        .collect()
}

#[instrument(
    level = Level::INFO,
    skip_all,
    fields(function.id = %wreq.bench, num.invocations = invocation_ids.len()),
)]
async fn warm_up(
    client: &mut SnaplaceGrpcClientRef,
    wreq: WorkloadRequest,
    invocation_ids: Vec<InvocationId>,
    map: Arc<DashMap<InvocationId, Arc<Notify>, ::snaplace::BuildHasher>>,
    timeout: Duration,
) -> Result<()> {
    const TIMEOUT: Duration = Duration::from_millis(5);

    let mut workers = JoinSet::new();
    for invocation_id in invocation_ids {
        workers.spawn({
            let wreq = wreq.clone();
            let mut client = client.clone();
            let map = Arc::clone(&map);
            let span = info_span!("worker", invocation.id = %invocation_id.as_str());
            async move {
                client
                    .issue(invocation_id.clone(), &wreq, u16::MAX, TIMEOUT)
                    .await
                    .with_context(|| format!("failed to issue {wreq:?})"))?;

                let notif = Arc::new(Notify::new());
                let _old_n = map.insert(invocation_id.clone(), Arc::clone(&notif));
                assert!(_old_n.is_none(), "InvocationIds are expected to be unique");
                match ::tokio::time::timeout(timeout, notif.notified()).await {
                    Ok(()) => trace!(%invocation_id, "Response retrieved"),
                    Err(err) => warn!("timed out waiting for response: {err}"),
                }

                Ok::<_, ::anyhow::Error>(())
            }
            .instrument(span)
        });
    }

    while let Some(res) = workers.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(error = ?err, "Joined task returned error: {err:#}"),
            Err(jerr) => error!(error = ?jerr, "Failed to join a worker thread: {jerr:#}"),
        }
    }

    Ok(())
}

async fn warm_up_all(
    cli: Cli,
    mut client: SnaplaceGrpcClientRef,
    map: Arc<DashMap<InvocationId, Arc<Notify>, ::snaplace::BuildHasher>>,
) -> Result<()> {
    let timeout = cli.response_timeout;
    let request_parts = prepare_requests(cli).context("failed to prepare requests")?;
    let max_invs = request_parts
        .iter()
        .map(|(_, inv_ids)| inv_ids.len())
        .sum::<usize>();

    let mut warmed_up =
        HashMap::with_capacity_and_hasher(request_parts.len(), ::snaplace::BuildHasher::default());
    for (wreq, invocation_ids) in request_parts {
        if !warmed_up.contains_key(&wreq.bench) {
            warmed_up.insert(wreq.bench.clone(), invocation_ids.len());
            warm_up(&mut client, wreq, invocation_ids, Arc::clone(&map), timeout)
                .await
                .context("failed to warm up function")?;
        }
    }

    info!(
        "Warmed up by sending {} (vs {max_invs}) requests for {} (vs {}) unique benchmarks",
        warmed_up.values().sum::<usize>(),
        warmed_up.len(),
        warmed_up.capacity()
    );
    Ok(())
}

#[::tokio::main]
async fn main() -> Result<()> {
    ::tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_thread_ids(true)
        .with_line_number(true)
        //.with_thread_names(true)
        .try_init()
        .map_err(|err| anyhow!("failed to initialize tracing subscriber: {err:#}"))?;

    let cli = ::argh::from_env::<Cli>();
    trace!("{cli:?}");

    let (shutdown, _) = broadcast::channel(1);
    setup_signals_handler(shutdown.clone())?;

    //
    // Prepare source client
    //
    let source = SnaplaceGrpcSource::spawn(&cli.source_address)
        .context("failed to spawn SnaplaceGrpcSource")?;

    //
    // Prepare response logger
    //
    let map = Arc::new(DashMap::<_, Arc<Notify>, _>::with_capacity_and_hasher(
        cli.max_concurr_requests as _,
        ::snaplace::BuildHasher::default(),
    ));
    let (to_resp_logger, mut from_sink) = mpsc::channel::<::snaplace_grpc::Response>(1 << 8);
    let resp_logger = ::tokio::spawn({
        let map = Arc::clone(&map);
        async move {
            while let Some(resp) = from_sink.recv().await {
                debug!(?resp);
                if let Some((_bench, notify)) = map.remove(resp.invocation_id.as_str()) {
                    notify.notify_one();
                }
            }
        }
        .instrument(info_span!("response-logger"))
    });

    //
    // Prepare sink client
    //
    let sink = SnaplaceGrpcSink::new(&cli.sink_address)
        .context("failed to initialize SnaplaceGrpcSink")?;
    let sink = ::tokio::spawn({
        let shutdown = shutdown.subscribe();
        async move { sink.run(to_resp_logger, shutdown).await }
            .instrument(info_span!("snaplace-grpc-sink"))
    });

    //
    // Warm up everything
    //
    warm_up_all(cli, source.new_ref(), Arc::clone(&map))
        .await
        .context("error while warming shit up")?;

    //
    //  Shutdown
    //
    match shutdown.send(()) {
        Ok(n) => info!("Shutdown signal sent to {n} receivers..."),
        Err(err) => error!(error = ?err, "Failed to send shutdown signal: {err:#}"),
    }
    // NOTE(ckatsak): Currently, the Source is probably unaware of the shutdown signal; so:
    //  - if we `.await` it, it will never return;
    //  - if we abort it, it will return an error (thus interrupting our `try_join!()`).
    // We should probably join only the Sink and the resp_logger, and only *after* that just
    // abort the Source?
    match ::tokio::try_join!(sink, resp_logger) {
        Ok((sink, ())) => {
            match sink {
                Ok(num_responses) => info!(?num_responses, "Sink task joined"),
                Err(err) => error!(error = ?err, "Joined failed Sink task: {err:#}"),
            }
            info!("Aborting Source task...");
            source.abort().await;
        }
        Err(err) => error!(error = ?err, "Failed to join Sink and RespLogger tasks: {err:#}"),
    }

    Ok(())
}
