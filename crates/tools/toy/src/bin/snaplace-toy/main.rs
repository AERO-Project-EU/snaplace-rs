use std::{
    fs::OpenOptions,
    io::{self, BufReader},
    path::Path,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use argh::FromArgs;
use tracing::trace;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};
use ubyte::ToByteUnit;

use snaplace::{
    conf::{LocalNetProviderConfig, SnaplaceConfig},
    keepalive::{self, eviction},
    metadata::{registration::RegisteredFunction, StdHashMapFmdStore},
    network::providers::plain_tap::PlainTapDevices,
    request::toy::ToyReqGen,
    response::toy::{ToyRespSink, ToyStringResponse},
    snapman::placement,
    worker::{
        issuer::toy::ToyIssuer,
        runtime::toy::{ToyFunctionInfo, ToyRt},
        Runtime,
    },
    FunctionId, FunctionMetadataStore, Orchestrator,
};

const CHAN_SIZE: usize = 16;
const SINK_OUTFILE: &str = "sink.out";

/// Binary making use of snaplace's `__toy` feature to debug synchronization issues among actors.
#[derive(Debug, FromArgs)]
struct Cli<RtCfg>
where
    RtCfg: for<'de> ::serde::Deserialize<'de>,
{
    /// path to SnaplaceConfig file
    #[argh(option, short = 'c', from_str_fn(json_snap_config))]
    config: SnaplaceConfig<RtCfg>,

    /// number of requests to emit
    #[argh(option, short = 'n', default = "10")]
    num_requests: u32,
}

fn json_snap_config<RtCfg>(arg: &str) -> ::std::result::Result<SnaplaceConfig<RtCfg>, String>
where
    RtCfg: for<'de> ::serde::Deserialize<'de>,
{
    get_config(arg).map_err(|err| err.to_string())
}

fn get_config<RtCfg>(path: impl AsRef<Path>) -> Result<SnaplaceConfig<RtCfg>>
where
    RtCfg: for<'de> ::serde::Deserialize<'de>,
{
    let mut br = BufReader::with_capacity(
        1 << 14, // 4 pages should always be enough
        OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| "failed to open config file")?,
    );
    ::serde_json::from_reader(&mut br).with_context(|| "failed to deserialize JSON config")
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

    let cli = ::argh::from_env::<Cli<<ToyRt as Runtime>::Config>>();
    trace!("{:#?}", cli.config);

    // sandbox networking
    let LocalNetProviderConfig::PlainTaps(ref net_config) = cli.config.network.provider else {
        bail!(
            "expected PlainTapsConfig rather than: '{:?}'",
            cli.config.network.provider
        );
    };
    let sb_net = PlainTapDevices::new(net_config).context("invalid PlainTapsConfig?")?;

    // source & sink
    let req_src = ToyReqGen::new(cli.num_requests as u64);
    trace!("{req_src:?}");
    let resp_sink = ToyRespSink::<ToyStringResponse>::new(SINK_OUTFILE, CHAN_SIZE)
        .await
        .with_context(|| "failed to create ToyRespSink")?;
    trace!("{resp_sink:?}");

    let store = StdHashMapFmdStore::default();
    // Deliberately leave the last "Function" unregistered to see it being ignored
    (0..cli.num_requests - 1).for_each(|i| {
        let _res = store.register_function(RegisteredFunction::new(
            ToyFunctionInfo {
                id: FunctionId::from(format!("req{i:09}")),
                memory: (10 * i).mebibytes(),
                image_ref: format!("docker.io/ckatsak/nonexistent:0.0.{i}"),
            },
            None,
        ));
    });

    // keepalive & snapshot placement
    let placement = placement::Fixed::new(Some("/tmp/snapshots"));
    let keepalive = keepalive::Fixed::new(Some(Duration::ZERO), eviction::NoOp);

    // spawn & run
    let orch = Orchestrator::<ToyRt>::spawn::<_, _, ToyIssuer, _, _, _>(
        cli.config, req_src, resp_sink, keepalive, placement, sb_net,
    )
    .await
    .with_context(|| "failed to spawn Orchestrator")?;

    orch.run()
        .await
        .with_context(|| "Orchestrator failed while running")
}
