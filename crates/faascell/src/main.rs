mod version;
use version::{SHORT_VERSION, VERSION_INFO};

use std::{
    fs::OpenOptions,
    io::{self, BufReader},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use argh::FromArgs;
use tracing::{info, instrument, trace, Level};
use tracing_appender::non_blocking::NonBlockingBuilder;
use tracing_flame::FlameLayer;
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer},
    layer::SubscriberExt,
    EnvFilter,
};

use issuer_aerofb::AeroFbRequestIssuer;
use snaplace::{
    conf::{KeepAliveConfig, NetworkConfig, PlacementConfig, SnaplaceConfig},
    keepalive::{self, eviction},
    network::providers::SandboxNetworkingProvider,
    snapman::placement,
    worker::Runtime,
    Orchestrator,
};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: ::mimalloc::MiMalloc = ::mimalloc::MiMalloc;

cfg_select! {
    all(feature = "rt-fc", feature = "rt-fcctrd") => {
        compile_error!("Features 'rt-fc' and 'rt-fcctrd' are mutually exclusive and cannot be enabled together.");
    }
    feature = "rt-fc" => {
        type Rt = ::snaplace::worker::runtime::fc::Firecracker;
    }
    feature = "rt-fcctrd" => {
        type Rt = ::snaplace::worker::runtime::fcctrd::FirecrackerContainerd;
    }
    doc => {
        type Rt = ::snaplace::worker::runtime::fc::Firecracker;
    }
    _ => {
        compile_error!("One of 'rt-fc' or 'rt-fcctrd' features must be enabled.");
    }
}

/// The [sandbox networking resource] used in this deployment.
///
/// # Notes
///
/// #### Why is it here, defined as a type alias?
///
/// To facilitate the definition of the [`init_networking`] function, which sets up the
/// [`SandboxNetworkingProvider`].
///
/// #### Future plans
///
/// - [`init_networking()`] needs a concrete <code>[SandboxNetworkingProvider]::[Resource]</code>
///   type specified.
/// - The latter needs to be the same type as [`worker::Runtime`]'s associated [`NetResource`].
/// - Therefore, we need the concrete type for [`worker::Runtime`] specified as well.
///
/// It seems that we gradually develop a tangled mess of types, which can only get worse when more
/// implementations of [`worker::Runtime`] come into play (probably gated by cargo features?).
///
/// _**TODO(ckatsak):** Tackling the above should probably lead us to multiple binary crates,
/// instead of a cargo feature hell (?)_
///
/// [sandbox networking resource]: ::snaplace::network::providers::Resource
/// [`SandboxNetworkingProvider`]: ::snaplace::network::providers::SandboxNetworkingProvider
/// [SandboxNetworkingProvider]: ::snaplace::network::providers::SandboxNetworkingProvider
/// [Resource]: ::snaplace::network::providers::SandboxNetworkingProvider::Resource
/// [`worker::Runtime`]: ::snaplace::worker::Runtime
/// [`NetResource`]: ::snaplace::worker::Runtime::NetResource
pub type NetResource = <Rt as Runtime>::NetResource;

/// The FaaSCell orchestrator, based on snaplace.
#[derive(Debug, FromArgs)]
struct Cli {
    /// path to SnaplaceConfig file
    #[argh(option, short = 'c')]
    config: Option<PathBuf>,

    // snaplace-grpc
    //
    /// the `IP_ADDR:PORT` the request source should serve on
    #[argh(option, default = "String::from(\"0.0.0.0:60051\")")]
    source_addr: String,

    /// the `IP_ADDR:PORT` the response sink should serve on
    #[argh(option, default = "String::from(\"0.0.0.0:60052\")")]
    sink_addr: String,

    // Logging
    //
    /// log to stderr
    #[argh(switch)]
    log_stderr: bool,

    /// log to file
    #[argh(option)]
    log_file: Option<PathBuf>,

    /// file for the folded stack trace of tracing-flame
    #[argh(option)]
    flame_file: Option<PathBuf>,

    /// emit events when creating and dropping a span of a permitted level
    #[argh(switch)]
    span_events: bool,

    // Version
    //
    /// display short version and exit
    #[argh(switch, short = 'V', long = "short")]
    short_version: bool,
    /// display version & build information and exit
    #[argh(switch)]
    version: bool,
}

/// Can be used with [`argh`] to automatically parse [`SnaplaceConfig`] or fail; e.g.:
/// ```ignore
/// #[derive(FromArgs)]
/// struct Cli {
///     /// path to SnaplaceConfig file
///     #[argh(option, short = 'c', from_str_fn(json_snap_config))]
///     config: SnaplaceConfig,
///     // . . .
/// }
/// ```
#[allow(dead_code)]
fn json_snap_config<RtCfg>(arg: &str) -> ::std::result::Result<SnaplaceConfig<RtCfg>, String>
where
    RtCfg: for<'de> ::serde::Deserialize<'de>,
{
    get_config(arg).map_err(|err| format!("{err:#}"))
}

fn get_config<RtCfg>(path: impl AsRef<Path>) -> Result<SnaplaceConfig<RtCfg>>
where
    RtCfg: for<'de> ::serde::Deserialize<'de>,
{
    let mut br = BufReader::with_capacity(
        1 << 14,
        OpenOptions::new()
            .read(true)
            .open(&path)
            .with_context(|| format!("failed to open config file '{}'", path.as_ref().display()))?,
    );
    ::serde_json::from_reader(&mut br).with_context(|| {
        format!(
            "failed to deserialize JSON config at '{}'",
            path.as_ref().display()
        )
    })
}

#[allow(dyn_drop)]
fn init_tracing(
    Cli {
        log_stderr,
        log_file,
        flame_file,
        span_events,
        ..
    }: &Cli,
) -> Result<Vec<Box<dyn Drop>>> {
    let mut span_events = if *span_events {
        FmtSpan::NEW | FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };
    if flame_file.is_some() {
        span_events |= FmtSpan::ACTIVE;
    }

    let mut guards: Vec<Box<dyn Drop>> = Vec::with_capacity(4);

    let layer_stderr = if *log_stderr {
        let (writer_stderr, guard_stderr) = {
            let nonblock_builder = NonBlockingBuilder::default().thread_name("writer-stderr");
            #[cfg(debug_assertions)]
            let nonblock_builder = nonblock_builder.lossy(false); // no lost logs on debug builds
            nonblock_builder.finish(io::stderr())
        };
        guards.push(Box::new(guard_stderr));
        Some(
            Layer::new()
                .with_span_events(span_events.clone())
                .with_thread_ids(true)
                .with_line_number(true)
                .with_writer(writer_stderr),
        )
    } else {
        None
    };

    let layer_logfile = if let Some(path) = log_file {
        let dirname = path
            .parent()
            .with_context(|| format!("failed to extract dirname from '{}'", path.display()))?;
        let basename = path
            .file_name()
            .with_context(|| format!("failed to extract basename from '{}'", path.display()))?;

        let (writer_logfile, guard_logfile) = {
            let nonblock_builder = NonBlockingBuilder::default().thread_name("writer-logfile");
            #[cfg(debug_assertions)]
            let nonblock_builder = nonblock_builder.lossy(false); // no lost logs on debug builds
            nonblock_builder.finish(::tracing_appender::rolling::never(dirname, basename))
        };
        guards.push(Box::new(guard_logfile));
        Some(
            Layer::new()
                .with_span_events(span_events)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_writer(writer_logfile),
        )
    } else {
        None
    };

    let layer_flame = if let Some(path) = flame_file {
        let dirname = path
            .parent()
            .with_context(|| format!("failed to extract dirname from '{}'", path.display()))?;
        let basename = path
            .file_name()
            .with_context(|| format!("failed to extract basename from '{}'", path.display()))?;

        let (nonblock_writer, nonblock_guard) = NonBlockingBuilder::default()
            .thread_name("writer-flame")
            .lossy(false) // no lossy logs for flamegraphs
            .finish(::tracing_appender::rolling::never(dirname, basename));

        let flame_layer = FlameLayer::new(nonblock_writer)
            .with_module_path(true)
            .with_file_and_line(true)
            .with_empty_samples(false)
            .with_threads_collapsed(true);
        let flame_guard = flame_layer.flush_on_drop();

        // ¿FIXME: Must `flame_guard` be dropped before `nonblock_guard`?
        guards.push(Box::new(flame_guard));
        guards.push(Box::new(nonblock_guard));
        Some(flame_layer)
    } else {
        None
    };

    let subscriber = ::tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(layer_stderr)
        .with(layer_logfile)
        .with(layer_flame);

    ::tracing::subscriber::set_global_default(subscriber)
        .context("failed to set global tracing subscriber")?;

    Ok(guards)
}

/// Plain (presumably blocking?) stderr logger
#[allow(dead_code)]
fn plain_stderr_logger(span_events: bool) -> Result<()> {
    ::tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(if span_events {
            FmtSpan::NEW | FmtSpan::CLOSE
        } else {
            FmtSpan::NONE
        })
        .with_thread_ids(true)
        .with_line_number(true)
        //.with_thread_names(true)
        .try_init()
        .map_err(|err| anyhow!("failed to initialize tracing subscriber: {err:#}"))
}

async fn real_main() -> Result<()> {
    let cli = ::argh::from_env::<Cli>();

    if cli.version {
        println!("{VERSION_INFO}");
        return Ok(());
    } else if cli.short_version {
        println!("{SHORT_VERSION}");
        return Ok(());
    }

    let config = cli
        .config
        .as_ref()
        .ok_or_else(|| anyhow!("Required option '--config' not provided; see '--help'"))
        .and_then(get_config::<<Rt as Runtime>::Config>)?;

    //plain_stderr_logger(cli.span_events)?;
    let _guards = init_tracing(&cli).context("failed to initialize tracing")?;
    info!("{VERSION_INFO}");
    info!("{cli:?}");
    info!("{config:?}");

    let sandbox_net = init_networking(&config.network)
        .await
        .context("failed to initialize sandbox networking provider")?;

    let req_src = ::snaplace_grpc::Source::new(&cli.source_addr)
        .context("failed to instantiate gRPC request source")?;
    trace!("{req_src:?}");
    let resp_sink = ::snaplace_grpc::Sink::new(&cli.sink_addr, 4096)
        // FIXME: buffer size -------------------------------- ^^^^ ?
        .context("failed to instantiate gRPC response sink")?;
    trace!("{resp_sink:?}");

    let (keepalive_pol, placement_pol) = init_keepalive_and_placement(&config).await?;

    let orch = Orchestrator::<Rt>::spawn::<
        _,
        _,
        AeroFbRequestIssuer<::snaplace_grpc::Request, ::snaplace_grpc::Response>,
        _,
        _,
        _,
    >(
        config,
        req_src,
        resp_sink,
        keepalive_pol,
        placement_pol,
        sandbox_net,
    )
    .await
    .context("failed to spawn Orchestrator")?;

    orch.run()
        .await
        .context("Orchestrator failed while running")
}

fn main() -> Result<()> {
    ::tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("faascell-tokio")
        .thread_keep_alive(Duration::from_secs(3600))
        .max_blocking_threads(512)
        .build()
        .context("failed to initialize tokio runtime")?
        .block_on(real_main())
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// net
//
///////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sbnet-tap")]
#[instrument(level = Level::INFO)]
async fn init_networking(
    config: &NetworkConfig,
) -> Result<impl SandboxNetworkingProvider<Resource = NetResource>> {
    let ::snaplace::conf::LocalNetProviderConfig::PlainTaps(ref config) = config.provider else {
        bail!("Net provider `PlainTapDevices` expects `PlainTapsConfig` rather than: '{config:?}'")
    };
    ::snaplace::network::providers::plain_tap::PlainTapDevices::new(config)
        .context("failed to initialize the `PlainTapDevices` provider")
}

#[cfg(feature = "sbnet-tap-pool")]
#[instrument(level = Level::INFO)]
async fn init_networking(
    config: &NetworkConfig,
) -> Result<impl SandboxNetworkingProvider<Resource = NetResource>> {
    let ::snaplace::conf::LocalNetProviderConfig::PoolTaps(ref config) = config.provider else {
        bail!("Net provider `PoolTapDevices` expects `PoolTapsConfig` rather than: '{config:?}'")
    };
    ::snaplace::network::providers::pool_tap::PoolTapDevices::new(config)
        .await
        .context("failed to initialize the `PoolTapDevices` provider")
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// pol-all-fixed-*
//
///////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(any(
    feature = "pol-all-fixed-noev",
    feature = "pol-all-fixed-evrand",
    feature = "pol-all-fixed-evlru",
))]
#[instrument(level = Level::INFO, skip_all)]
async fn init_keepalive_and_placement<RtCfg>(
    config: &SnaplaceConfig<RtCfg>,
) -> Result<(keepalive::Fixed<impl eviction::Policy>, placement::Fixed)> {
    let keepalive_pol = init_keepalive(&config.sandbox_pool.keepalive)
        .await
        .context("failed to initialize keep-alive policy")?;
    let placement_pol = init_placement(&config.placement)
        .await
        .context("failed to initialize Placement Algorithm")?;

    Ok((keepalive_pol, placement_pol))
}

#[cfg(feature = "pol-ka-fixed")]
#[instrument(level = Level::INFO, fields(keepalive = "Fixed"))]
async fn init_keepalive(
    config: &KeepAliveConfig,
) -> Result<keepalive::Fixed<impl eviction::Policy>> {
    use anyhow::anyhow;
    use tracing::error;

    match config {
        KeepAliveConfig::Fixed { duration, .. } => {
            Ok(keepalive::Fixed::new(*duration, init_eviction()))
        }
        conf => {
            error!("Expected KeepAliveConfig::Fixed; found: {conf:?}");
            Err(anyhow!("Configuration error: not a KeepAliveConfig::Fixed"))
        }
    }
}

#[cfg(feature = "pol-plcm-fixed")]
#[instrument(level = Level::INFO, fields(placement = "Fixed"))]
async fn init_placement(config: &PlacementConfig) -> Result<placement::Fixed> {
    use anyhow::bail;
    use tracing::{error, info};

    let path = match &config {
        PlacementConfig::Fixed { path } => match path {
            Some(p) if !p.as_os_str().is_empty() => match p.try_exists() {
                Ok(true) => {
                    info!("Path {p:?} already exists");
                    Some(p)
                }
                Ok(false) => {
                    info!("Path {p:?} does not exist; attempting to recursively mkdir...");
                    if let Err(err) = ::tokio::fs::create_dir_all(&p).await {
                        error!(error = ?err, "Failed to recursively mkdir {p:?}: {err:#}");
                        return Err(err).with_context(|| {
                            "Fixed Placement Algorithm: failed to recursively mkdir {p:?}"
                        });
                    }
                    Some(p)
                }
                Err(err) => {
                    error!(error = ?err, "Failed to check if path {p:?} exists: {err:#}");
                    return Err(err).context(
                        "Fixed Placement Algorithm: failed to check if path {p:?} exists",
                    );
                }
            },
            Some(p) if p.as_os_str().is_empty() => None,
            Some(_) => unreachable!("PathBuf can be either empty or non-empty"),
            None => None,
        },
        conf => {
            error!("Expected PlacementConfig::Fixed; found: {conf:?}");
            bail!("Configuration error: not a PlacementConfig::Fixed");
        }
    };

    Ok(placement::Fixed::new(path))
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// pol-sd-static
//
///////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "pol-ka-sd-static")]
use snaplace::FunctionId;

#[cfg(feature = "pol-sd-static")] // FIXME(ckatsak): which feature to use as guard?!
#[instrument(level = Level::INFO, skip_all)]
async fn init_keepalive_and_placement<RtCfg>(
    config: &SnaplaceConfig<RtCfg>,
) -> Result<(
    keepalive::sd_static::SlowdownsStatic<impl eviction::Policy>,
    placement::SlowdownsStatic,
)> {
    let placement = init_placement(&config.placement)
        .await
        .context("failed to initialize Placement Algorithm")?;
    let keepalive = init_keepalive(
        &config.sandbox_pool.keepalive,
        placement.functions_kept_alive(),
    )
    .await
    .context("failed to initialize keep-alive policy")?;

    Ok((keepalive, placement))
}

#[cfg(feature = "pol-ka-sd-static")]
#[instrument(level = Level::INFO, skip(functions), fields(keepalive = "SlowdownsStatic"))]
async fn init_keepalive(
    config: &KeepAliveConfig,
    functions: impl IntoIterator<Item = FunctionId>,
) -> Result<keepalive::sd_static::SlowdownsStatic<impl eviction::Policy>> {
    use anyhow::anyhow;
    use tracing::error;

    match config {
        KeepAliveConfig::SlowdownsStatic {
            duration,
            fixed_warm_functions_path,
            non_warm_duration,
            ..
        } => keepalive::sd_static::SlowdownsStatic::new(
            functions.into_iter().chain(
                fixed_warm_functions_path
                    .as_ref()
                    .map(snaplace::utils::sd_static::read_function_ids)
                    .transpose()?
                    .unwrap_or_default(),
            ),
            *duration,
            *non_warm_duration,
            init_eviction(),
        )
        .context("failed to construct new SlowdownsStatic keep-alive policy"),
        conf => {
            error!("Expected KeepAliveConfig::SlowdownsStatic; found: {conf:?}");
            Err(anyhow!(
                "Configuration error: not a KeepAliveConfig::SlowdownsStatic"
            ))
        }
    }
}

#[cfg(feature = "pol-plcm-sd-static")]
#[instrument(level = Level::INFO, fields(placement = "SlowdownsStatic"))]
async fn init_placement(config: &PlacementConfig) -> Result<placement::SlowdownsStatic> {
    use anyhow::anyhow;
    use tracing::error;

    match &config {
        PlacementConfig::SlowdownsStatic(config) => placement::SlowdownsStatic::new(config)
            .context("failed to construct new SlowdownsStatic snapshot placement policy"),
        conf => {
            error!("Expected PlacementConfig::SlowdownsStatic; found: {conf:?}");
            Err(anyhow!(
                "Configuration error: not a PlacementConfig::SlowdownsStatic"
            ))
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// pol-ev-*
//
///////////////////////////////////////////////////////////////////////////////////////////////////

fn init_eviction() -> impl eviction::Policy {
    #[cfg(feature = "pol-ev-noop")]
    {
        eviction::NoOp
    }
    #[cfg(feature = "pol-ev-rand")]
    {
        eviction::Random::default()
    }
    #[cfg(feature = "pol-ev-lru")]
    {
        eviction::LruWorker
    }
}
