use std::{path::PathBuf, str::FromStr, time::Duration};

use ipnet::Ipv4Net;
use serde::Deserialize;
use ubyte::ByteUnit;

use crate::{snapman::placement, utils::ser_de::deserialize_byteunit};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SnaplaceConfig<RtCfg> {
    /// Configuration related to the [`Orchestrator`].
    ///
    /// [`Orchestrator`]: crate::Orchestrator
    pub orchestrator: OrchestratorConfig,

    pub network: NetworkConfig,

    /// Configuration related to the [`SandboxPool`].
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    #[serde(alias = "vmpool")]
    pub sandbox_pool: SandboxPoolConfig,

    /// Configuration related to the [`Worker`]s.
    ///
    /// [`Worker`]: crate::worker::Worker
    pub workers: WorkerConfig<RtCfg>,

    /// Configuration related to the [`AdmissionController`].
    ///
    /// [`AdmissionController`]: crate::admission::AdmissionController
    #[serde(alias = "dispatcher")]
    pub admission: AdmissionConfig,

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // SnapshotManager
    #[serde(alias = "snapshot_placement")]
    pub placement: PlacementConfig,

    pub devices: DevicesConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OrchestratorConfig {
    /// Address of control plane API gRPC server.
    ///
    /// This should only be one of:
    /// (a) `"HOST:PORT"`, to bind a TCP socket to;
    /// (b) `"/path/to/unix/domain.socket"`, to bind a Unix Domain Socket to.
    pub address: Address,

    /// Path to the database file.
    pub db_path: PathBuf,

    /// Path to the file where all [`Worker`]s' [`Timing`]s accumulated by the [`MetricsCollector`]
    /// should be stored, JSON-serialized.
    ///
    /// [`MetricsCollector`]: crate::metrics::MetricsCollector
    /// [`Timing`]: crate::metrics::Timing
    /// [`Worker`]: crate::worker::Worker
    pub timings_path: PathBuf,
}

/// System-wide defaults for admission queuing and retry behavior.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AdmissionConfig {
    /// Maximum number of invocations that can be _actively handled concurrently_
    /// (i.e., number of running Functions making progress) by the system at any
    /// single time.
    pub max_concurrency: usize,

    /// Maximum number of [`Request`]s that can be queued between
    /// [`AdmissionController`] and [`request::Source`].
    ///
    /// [`AdmissionController`]: crate::admission::AdmissionController
    /// [`Request`]: crate::Request
    /// [`request::Source`]: crate::request::Source
    //
    // FIXME(ckatsak): This is a temporary workaround for avoiding buffering of incoming requests
    // at the TCP (or HTTP2?) layer, so that we can correctly timestamp them. TODO: In the future,
    // request::Source should probably `try_send()` each Request to AdmissionController, and
    // immediately discard them on failure.
    pub queue_size: usize,

    /// Maximum _total_ number of requests that may be held in admission-owned
    /// Function queues at once.
    ///
    /// This excludes any request already removed from a queue and stored in
    /// `OutstandingDispatch`. When the limit is reached, new arrivals are
    /// rejected; older queued requests are preserved.
    pub global_max_queued_reqs: usize,
    /// Default maximum number of requests that admission may queue for one
    /// Function, unless the Function provides an override.
    ///
    /// When this bound is reached, new arrivals for that Function are rejected
    /// (while preserving the order of already queued requests).
    pub default_max_queued_per_func: usize,
    /// Default maximum time a request may remain queued in admission before it
    /// is failed for waiting too long, unless the Function provides an override.
    ///
    /// This bound applies only while the request is still queued in admission,
    /// not after it has become a pending dispatch attempt.
    #[serde(with = "humantime_serde")]
    pub default_max_queue_delay: Duration,
    /// Default _per-Function_ upper bound on dispatchable Worker capacity,
    /// unless the Function provides an override.
    ///
    /// [`SandboxPool`] interprets this as a limit on `Active(F) + Idle(F)`;
    /// `Dying` Workers do not count against the limit.
    ///
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    pub default_max_live_instances_per_func: usize,
    /// Coarse retry period for blocked per-Function admission queues.
    ///
    /// [`AdmissionController`] primarily relies on advisory [`PoolEvent`]s to wake
    /// blocked Functions.  Because those wakeups are intentionally lossy and may
    /// become stale, it also uses this periodic fallback to restore liveness.
    ///
    /// On each tick, blocked Functions are made runnable again and must re-query
    /// `SandboxPool` before any dispatch attempt proceeds.  This is therefore a
    /// liveness-repair mechanism, not a permission to run and not a scheduling
    /// policy by itself.
    ///
    /// This interval should remain coarse: it is not intended to be a hot-path
    /// polling loop.  Lower values reduce recovery latency after lost wakeups,
    /// while higher values reduce admission churn under blocked conditions.
    ///
    /// Few thoughts on some indicative values/ranges:
    /// - 50ms: probably too eager for a lossy-repair mechanism; starts to look like polling.
    /// - 100ms: maybe ok since we care about sub-second tail latency and Function count is small?
    /// - 250ms: perhaps good default for modest responsiveness with low overhead?
    /// - 500ms: safer default to prioritize minimal admission churn?
    /// - 1s+: acceptable only if occasional extra latency after lost wakeups is fine.
    ///
    ///
    /// [`AdmissionController`]: crate::admission::AdmissionController
    /// [`PoolEvent`]: crate::sbpool::PoolEvent
    #[serde(with = "humantime_serde")]
    pub fallback_retry_period: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SandboxPoolConfig {
    #[serde(alias = "cpus")]
    #[serde(alias = "cpu_cores")]
    #[serde(default)]
    pub cpuset: String,

    /// Maximum amount of memory that can be dedicated to Function [`Sandbox`]es.
    ///
    /// See [`deserialize_byteunit`] for information on how this field is parsed and evaluated.
    ///
    /// ## Notes
    ///
    /// - A sophisticated solution would employ memory cgroups to count against this limit. In
    ///   addition to that, CPU shares could be provided proportionally to each sandbox's memory
    ///   requests, in a manner similar to what the major FaaS providers already appear to be doing
    ///   out there.
    /// - A dummy solution would be to just count each sandbox against this limit (e.g., the
    ///   [`SandboxPool`] could do that) and start evicting idle sandboxes (_Idle_ [`Worker`]s)
    ///   when the limit is reached (e.g., at 90% or something) so that more sandboxes can be
    ///   spawned when needed (disallowing any sandbox creation or snapshot hydration that
    ///   surpasses the limit).
    /// - In any of the above solutions, we have not really accounted for the memory required by
    ///   our system itself, nor by the [`Runtime`] implementation (e.g., firecracker-containerd
    ///   and all its shims)!
    ///
    /// [`deserialize_byteunit`]: crate::utils::ser_de::deserialize_byteunit
    /// [`Runtime`]: crate::worker::Runtime
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    /// [`Worker`]: crate::worker::Worker
    #[serde(alias = "total_memory")]
    #[serde(deserialize_with = "deserialize_byteunit")]
    pub max_memory: ByteUnit,

    /// Fraction (i.e., [`f64`] between `0.0` and `1.0`) of [`max_memory`] at
    /// which the configured [eviction policy] (if any) is triggered, aiming to
    /// reduce this fraction down to the configured [low watermark percentage].
    ///
    /// ## Notes
    ///
    /// - Make sure `0.0 <= eviction_lo_watermark_pct <= eviction_hi_watermark_pct <= 1.0`.
    /// - Both this and the [low watermark percentage] default to `1.0` (i.e,
    ///   no proactive evictions at all).
    /// - To avoid decommissioning a large number of [`Worker`]s simultaneously,
    ///   keep the range between watermarks relatively small (e.g., perhaps 10-15%).
    ///
    ///
    /// [eviction policy]: crate::sbpool::keepalive::eviction::Policy
    /// [`max_memory`]: Self::max_memory
    /// [low watermark percentage]: Self::eviction_lo_watermark_pct
    #[serde(
        alias = "eviction_hi_watermark_pct",
        alias = "eviction_watermark_pct_hi"
    )]
    #[serde(default = "default_eviction_watermark_pct")]
    pub eviction_hi_watermark_pct: f64,
    /// Fraction (i.e., [`f64`] between `0.0` and `1.0`) of [`max_memory`] at
    /// which the configured [eviction policy] (if any) should aim for when
    /// triggered (i.e., when usage reaches the corresponding configured
    /// [high watermark percentage]).
    ///
    /// ## Notes
    ///
    /// - Make sure `0.0 <= eviction_lo_watermark_pct <= eviction_hi_watermark_pct <= 1.0`.
    /// - Both this and the [high watermark percentage] default to `1.0` (i.e,
    ///   no proactive evictions at all).
    /// - To avoid decommissioning a large number of [`Worker`]s simultaneously,
    ///   keep the range between watermarks relatively small (e.g., perhaps 10-15%).
    ///
    ///
    /// [eviction policy]: crate::sbpool::keepalive::eviction::Policy
    /// [`max_memory`]: Self::max_memory
    /// [high watermark percentage]: Self::eviction_hi_watermark_pct
    #[serde(
        alias = "eviction_lo_watermark_pct",
        alias = "eviction_watermark_pct_lo"
    )]
    #[serde(default = "default_eviction_watermark_pct")]
    pub eviction_lo_watermark_pct: f64,

    /// TODO
    #[serde(default)]
    pub keepalive: KeepAliveConfig,

    /// When set to `true`, [`SandboxPool`] does not instruct the underlying [`Runtime`] to remove
    /// any (and all) snapshotted [`Sandbox`] before shutting down.
    /// This is essential for snapshot reuse across boots.
    ///
    /// # Notes
    ///
    /// - Defaults to `false`.
    /// - It is ignored when the underlying [`Runtime`] does not support snapshotting at all.
    ///
    /// [`Runtime`]: crate::worker::Runtime
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    #[serde(alias = "reuse_snapshots")]
    #[serde(default)]
    pub retain_snapshots: bool,
}

fn default_eviction_watermark_pct() -> f64 {
    1.
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct NetworkConfig {
    #[serde(flatten)]
    pub provider: LocalNetProviderConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum LocalNetProviderConfig {
    PlainTaps(PlainTapsConfig),
    PoolTaps(PoolTapsConfig),
}

/// Example:
///
/// ```json
/// "network": {
///     "plain_taps": {
///         "subnet": "10.0.0.0/8"
///     }
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PlainTapsConfig {
    pub subnet: Ipv4Net,
}

/// Example:
///
/// ```json
/// "network": {
///     "pool_taps": {
///         "subnet": "10.0.0.0/8",
///         "capacity": 16384
///     }
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub struct PoolTapsConfig {
    pub subnet: Ipv4Net,
    // TODO(dchar): Should this be the same with `AdmissionControllerConfig.max_concurrency`?
    pub capacity: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WorkerConfig<RtCfg> {
    pub runtime: RtCfg,

    /// Configuration related to the [`PerformanceMonitor`]; i.e., the kind of
    /// profiling requested by a [`Worker`] about a running [`Sandbox`].
    ///
    /// [`PerformanceMonitor`]: crate::snapman::PerformanceMonitor
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`Worker`]: crate::worker::Worker
    #[serde(default)]
    pub perf: PerfConfig,

    /// When should [`Worker`]s create a snapshot of their [`Sandbox`]?
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`Worker`]: crate::worker::Worker
    #[serde(default)]
    pub snapshot_creation_time: SnapshotCreationTime,

    /// Timeout override for [function request issuing](crate::worker::RequestIssuer).
    ///
    /// This instructs [`Worker`] to time out [`RequestIssuer::issue_request`]
    /// after this duration has elapsed.
    ///
    /// Fine-grained timeouts (e.g., connection vs read) are currently not
    /// supported through the [`Worker`].
    ///
    /// Defaults to [`DEFAULT_ISSUER_TIMEOUT`].
    ///
    ///
    /// [`DEFAULT_ISSUER_TIMEOUT`]: crate::worker::DEFAULT_ISSUER_TIMEOUT
    /// [`RequestIssuer::issue_request`]: crate::worker::RequestIssuer::issue_request
    /// [`Worker`]: crate::worker::Worker
    #[serde(default = "default_issuer_timeout")]
    #[serde(with = "humantime_serde")]
    pub issuer_timeout: Duration,
}

fn default_issuer_timeout() -> Duration {
    crate::worker::DEFAULT_ISSUER_TIMEOUT
}

/// When should [`Worker`]s create a snapshot of their [`Sandbox`]?
///
/// [`Sandbox`]: crate::worker::runtime::Sandbox
/// [`Worker`]: crate::worker::Worker
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotCreationTime {
    /// Only when explicitly requested through the control plane.
    OnlyOnDemand,
    /// Upon [`Sandbox`] creation, right _after_ handling the first invocation.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    #[default]
    OnCreationAfterInvoc,
    /// Upon [`Sandbox`] creation, right _before_ handling the first invocation.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    OnCreationBeforeInvoc,
    /// Never; not even on demand.  Snapshotting is considered disabled.
    Never,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "mode", content = "config")]
#[non_exhaustive]
pub enum PerfConfig {
    #[default]
    Simple,
}

//#[derive(Debug, Clone, Deserialize)]
//#[serde(rename_all = "snake_case")]
//#[serde(tag = "type", content = "config")]
//pub enum RequestSourceConfig {
//    Toy,
//}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "policy", content = "config")]
#[non_exhaustive]
#[cfg_attr(test, derive(PartialOrd, PartialEq))]
pub enum PlacementConfig {
    /// Use a sandbox snapshot placement policy that places them all to a specific filesystem
    /// path (i.e., the provided `path`), or never creates sandbox snapshots (if no `path` is
    /// provided at all, or if given `null` or an empty string).
    ///
    /// ## Example Configuration
    ///
    /// ```json
    /// "snapshot_placement": {
    ///     "policy": "fixed",
    ///     "config": {
    ///         "path": "/tmp/snapshots"
    ///     }
    /// }
    /// ```
    Fixed { path: Option<PathBuf> },

    /// The [`SlowdownsStatic`] (aka "sd-static") [snapshot placement policy].
    ///
    /// ## Example configuration
    ///
    /// ```json
    /// "snapshot_placement": {
    ///     "policy": "sd-static",
    ///     "config": {
    ///         "csv_path": "/tmp/path/to/sd_b64__running_slot__p95.csv",
    ///         "slowdown_threshold_cold": 1.6,
    ///         "warm_device": "optane_dcpm",
    ///         "devices": {
    ///             "flash_ssd": {
    ///                 "mountpoint": "/opt/ckatsak/snapshots",
    ///                 "slowdown_threshold": 2.7
    ///             },
    ///             "optane_nvme": {
    ///                 "mountpoint": "/mnt/optane_nvme/christos/snapshots",
    ///                 "slowdown_threshold": 3.8
    ///             },
    ///             "optane_dcpm": {
    ///                 "mountpoint": "/mnt/pmem0/ckatsak/snapshots",
    ///                 "slowdown_threshold": 4.9
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// [`SlowdownsStatic`]: crate::snapman::placement::SlowdownsStatic
    /// [snapshot placement policy]: crate::snapman::placement::PlacementAlgorithm
    #[serde(alias = "sd-static", alias = "sd_static")]
    SlowdownsStatic(placement::sd_static::Config),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "policy", content = "config")]
#[non_exhaustive]
#[cfg_attr(test, derive(PartialOrd, PartialEq))]
pub enum KeepAliveConfig {
    /// A [keep-alive policy] that **always** makes the same decision.
    ///
    /// ## Example configurations
    ///
    /// - Keep all `Worker`s alive for 10 minutes (i.e., except if the [eviction policy] kicks in):
    ///
    /// ```json
    /// "sandbox_pool": {
    ///     "keepalive": {
    ///         "policy": "fixed",
    ///         "config": {
    ///             "duration": "10min",
    ///             "eviction": { ... }
    ///         },
    ///     },
    ///     ...
    /// },
    /// ```
    ///
    /// - Never keep `Worker`s alive; shut them down immediately after handling each `Request`:
    ///
    /// ```json
    /// "sandbox_pool": {
    ///     "keepalive": {
    ///         "policy": "fixed",
    ///         "config": {
    ///             "duration": null,
    ///             "eviction": { ... }
    ///         },
    ///     },
    ///     ...
    /// },
    /// ```
    ///
    /// or just:
    ///
    /// ```json
    /// "sandbox_pool": {
    ///     "keepalive": {
    ///         "policy": "fixed",
    ///         "config": {
    ///             "eviction": { ... }
    ///         },
    ///     },
    ///     ...
    /// },
    /// ```
    ///
    /// - Using `Duration::ZERO` technically works, but it leads to a keep-alive timer which
    ///   goes off immediately, so the sandbox remains up until `SandboxPool` reaps that timer.
    ///   This may or may not allow for additional `Request`(s) to be queued to the same `Worker`
    ///   (depending on the order that `SandboxPool` handles its events):
    ///
    /// ```json
    /// "sandbox_pool": {
    ///     "keepalive": {
    ///         "policy": "fixed",
    ///         "config": {
    ///             "duration": "0us",
    ///             "eviction": { ... }
    ///         },
    ///     },
    ///     ...
    /// },
    /// ```
    ///
    /// [keep-alive policy]: crate::sbpool::keepalive::Policy
    /// [eviction policy]: crate::sbpool::keepalive::eviction::LruWorker
    Fixed {
        /// The duration that an _Idle_ [`Worker`] (along with its sandbox) will be kept alive
        /// (unless it either stops being _Idle_ or is evicted earlier).
        ///
        /// [`Worker`]: crate::worker::Worker
        #[serde(default)]
        #[serde(with = "humantime_serde")]
        duration: Option<Duration>,

        /// The configuration of the interoperating [eviction policy].
        ///
        /// [eviction policy]: crate::sbpool::keepalive::eviction::Policy
        #[serde(default)]
        eviction: MemoryEvictionConfig,
    },

    /// A [keep-alive policy] that works together with the [`SlowdownsStatic`] (aka "sd-static")
    /// [snapshot placement policy].
    ///
    /// ## Example configuration
    ///
    /// ```json
    /// "sandbox_pool": {
    ///     "keepalive": {
    ///         "policy": "sd-static",
    ///         "config": {
    ///             "duration": "1 min",
    ///             "eviction": { ... }
    ///         },
    ///     }
    /// },
    /// ```
    ///
    /// [keep-alive policy]: crate::sbpool::keepalive::Policy
    /// [`SlowdownsStatic`]: crate::snapman::placement::SlowdownsStatic
    /// [snapshot placement policy]: crate::snapman::placement::PlacementAlgorithm
    #[serde(alias = "sd-static", alias = "sd_static")]
    SlowdownsStatic {
        /// The duration to keep _Idle_ Workers (along with their sandboxes) of Functions marked
        /// as "warm" alive for (unless they either stop being _Idle_ or get evicted earlier).
        ///
        /// Functions not marked as "warm" are never kept alive under this policy, unless
        /// included in the (optionally provided) file at [`fixed_warm_functions_path`]
        /// (or the [`non_warm_duration`] is set to override all "no keep-alive" decisions
        /// with a minimum [`Duration`]).
        ///
        /// If the duration is [`None`] (e.g., omitted, missing during deserialization), it is
        /// interpreted as "very long" (though, again, _only_ for Functions marked as "warm"),
        /// which corresponds to [`keepalive::SlowdownsStatic::FOREVER`];
        ///
        /// [`keepalive::SlowdownsStatic::FOREVER`]:
        /// crate::sbpool::keepalive::sd_static::SlowdownsStatic::FOREVER
        /// [`fixed_warm_functions_path`]: crate::conf::KeepAliveConfig::SlowdownsStatic::fixed_warm_functions_path
        /// [`non_warm_duration`]: crate::conf::KeepAliveConfig::SlowdownsStatic::non_warm_duration
        #[serde(default)]
        #[serde(with = "humantime_serde")]
        duration: Option<Duration>,

        /// Optionally provided path to a newline-delimited file containing a list of
        /// [`FunctionId`]s which should always be kept alive, bypassing any (possibly conflicting)
        /// verdict of (the otherwise co-operative) [placement::SlowdownsStatic].
        ///
        /// This might be useful to, e.g., provide a fixed keep-alive to popular Functions, without
        /// changing the assignedment of device where their snapshot files are stored.
        ///
        /// [`FunctionId`]: crate::FunctionId
        /// [placement::SlowdownsStatic]: crate::snapman::placement::SlowdownsStatic
        fixed_warm_functions_path: Option<PathBuf>,

        /// Optional override of the minimum assignable [`Duration`] (i.e., on
        /// Functions classified as __non__-warm).
        ///
        /// Normally, [`SlowdownsStatic`] either:
        /// - assigns [`duration`](KeepAliveConfig::SlowdownsStatic::duration)
        ///   (to Functions classified as "warm" by the (paired) [`sd-static`]
        ///   [snapshot placement policy] and/or specified as "fixed warm" through
        ///   [`fixed_warm_functions_path`](KeepAliveConfig::SlowdownsStatic::fixed_warm_functions_path),
        ///   or
        /// - assigns [`None`] to the rest of the (__non__-warm) Functions.
        ///
        /// This (optional) knob overrides the latter case (of `None` for the rest
        /// of the __non__-warm Functions) with the specified [`Duration`].
        ///
        /// [`sd-static`]: crate::snapman::placement::sd_static::SlowdownsStatic
        /// [snapshot placement policy]: crate::snapman::placement::PlacementAlgorithm
        /// [`SlowdownsStatic`]: crate::sbpool::keepalive::sd_static::SlowdownsStatic
        #[serde(default)]
        #[serde(with = "humantime_serde")]
        non_warm_duration: Option<Duration>,

        /// The configuration of the interoperating [eviction policy].
        ///
        /// [eviction policy]: crate::sbpool::keepalive::eviction::Policy
        #[serde(default)]
        eviction: MemoryEvictionConfig,
    },
}

impl Default for KeepAliveConfig {
    fn default() -> Self {
        Self::Fixed {
            duration: Some(Duration::from_secs(60 * 10)), // 10 min
            eviction: MemoryEvictionConfig::NoOp {},
        }
    }
}

/// Configuration of [`keepalive::Policy`]'s interoperating [`eviction::Policy`].
///
/// # Note
///
/// For now, this configuration is effectively useless, since none of the [`eviction::Policy`]
/// implementations need any internal configuration from the user. Therefore, apart from their
/// selection (which, for now, happens at compile-time via cargo features, to enable static
/// dispatch), there is no need to ever read this struct. We have it in place, however, in
/// anticipation of future [`eviction::Policy`] implementations that might require user
/// configuration at run-time.
///
/// [`eviction::Policy`]: crate::sbpool::keepalive::eviction::Policy
/// [`keepalive::Policy`]: crate::sbpool::keepalive::Policy
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "policy", content = "config")]
#[non_exhaustive]
#[cfg_attr(test, derive(PartialOrd, PartialEq))]
pub enum MemoryEvictionConfig {
    /// An [eviction policy] that always suggests not to evict any `Worker` at all.
    ///
    /// ## Example configuration
    ///
    /// ```json
    /// "sandbox_pool": {
    ///     "keepalive": {
    ///         "config": {
    ///             "eviction": {
    ///                 "policy": "none",
    ///                 "config": {}
    ///             },
    ///             ...
    ///         },
    ///         ...
    ///     },
    ///     ...
    /// },
    /// ```
    ///
    /// [eviction policy]: crate::sbpool::keepalive::eviction::NoOp
    #[serde(alias = "none")]
    NoOp {},

    /// An [eviction policy] that suggests `Worker`s for eviction completely at random.
    ///
    /// ## Example configuration
    ///
    /// ```json
    /// "sandbox_pool": {
    ///     "keepalive": {
    ///         "config": {
    ///             "eviction": {
    ///                 "policy": "random",
    ///                 "config": {}
    ///             },
    ///             ...
    ///         },
    ///         ...
    ///     },
    ///     ...
    /// },
    /// ```
    ///
    /// [eviction policy]: crate::sbpool::keepalive::eviction::Random
    #[serde(alias = "rand")]
    Random {},

    /// An [eviction policy] that suggests `Worker`s for eviction based on the recency of their
    /// use.
    ///
    /// ## Example configuration
    ///
    /// ```json
    /// "sandbox_pool": {
    ///     "keepalive": {
    ///         "config": {
    ///             "eviction": {
    ///                 "policy": "lru",
    ///                 "config": {}
    ///             },
    ///             ...
    ///         },
    ///         ...
    ///     },
    ///     ...
    /// },
    /// ```
    ///
    /// [eviction policy]: crate::sbpool::keepalive::eviction::LruWorker
    #[serde(alias = "lru")]
    LruWorker {},
}

impl Default for MemoryEvictionConfig {
    fn default() -> Self {
        Self::NoOp {}
    }
}

/// Address to bind a socket to.
///
/// This should only be one of:
/// (a) `"HOST:PORT"`, to bind a network socket to;
/// (b) `"/path/to/unix/domain.socket"`, to bind a Unix Domain Socket to.
///
/// Traits [`FromStr`] and [`Deserialize`] are implemented, however when parsing this
/// type mind the following:
/// - any string that does not contain `':'` is considered a valid [`Address::Uds`];
/// - strings that contain at least one `':'` are split on its last occurence, and if:
///   * the head does not contain a `'/'`, and
///   * the tail can be parsed as [`u16`],
///
///   then they are considered valid [`Address::Net`].
///
/// # Note
///
/// We deliberately avoid using [`ToSocketAddrs`] in [`FromStr`] and [`Deserialize`]
/// implementations to avoid network access (for address resolution) as a requirement
/// for plain parsing.
///
/// [`ToSocketAddrs`]: ::std::net::ToSocketAddrs
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Address {
    /// A [`String`] in `"HOST:PORT"` format, to bind a network socket to.
    Net(String),
    /// A [`PathBuf`] for a filesystem path, to bind a Unix domain socket to.
    Uds(PathBuf),
}

#[derive(Debug, ::thiserror::Error)]
#[error("failed to parse '{0}' as valid net or unix socket address")]
pub struct AddressParseError(String);
impl FromStr for Address {
    type Err = AddressParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.rsplit_once(':').map_or_else(
            || Ok(Address::Uds(PathBuf::from(s))),
            |(host, port)| {
                (!host.contains('/') && port.parse::<u16>().is_ok())
                    .then(|| Address::Net(s.to_owned()))
                    .ok_or_else(|| AddressParseError(s.to_owned()))
            },
        )
    }
}

struct AddressVisitor;

impl<'de> ::serde::de::Visitor<'de> for AddressVisitor {
    type Value = Address;

    fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        formatter.write_str("a string in 'HOST:PORT' format or a filesystem path")
    }

    // `s` is a `&str` borrowed directly from the input; no allocation has occurred
    fn visit_str<E: ::serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        v.parse().map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D: ::serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(AddressVisitor)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(transparent)]
pub struct DevicesConfig {
    pub devices: Vec<Device>,
}

// NOTE(ckatsak): Both struct variants are the same for now, but let's not factor them
// into a single type until we make sure that's all we need to know/model about them.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "access", content = "info")]
pub enum Device {
    Direct {
        name: String,
        mountpoint: PathBuf,
        latency: u8,
    },
    Cached {
        name: String,
        mountpoint: PathBuf,
        latency: u8,
    },
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{read_dir, OpenOptions},
        io::BufReader,
        path::PathBuf,
        time::Duration,
    };

    use anyhow::{Context, Result};
    use tracing::debug;
    use tracing_test::traced_test;

    use super::{KeepAliveConfig, MemoryEvictionConfig, PlacementConfig, SnaplaceConfig};
    use crate::snapman::placement;

    const CFG_DIR: &str = "../../artifacts/faascell/config/";

    #[cfg(feature = "rt-fcctrd")]
    #[test]
    #[traced_test]
    fn parse_fcctrd_config_artifacts() -> Result<()> {
        for de in read_dir(CFG_DIR).with_context(|| format!("failed to read_dir in {CFG_DIR:?}"))? {
            let direntry = de.context("DirEntry")?;
            let config_path = direntry.path();
            if let Some(file_name) = direntry.file_name().to_str()
                && !file_name.contains("fcctrd")
            {
                debug!("Skipping config file {config_path:?}");
                continue;
            }
            let br = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .open(&config_path)
                    .context("failed to read config file")?,
            );
            let c: SnaplaceConfig<crate::worker::runtime::fcctrd::FirecrackerContainerdConfig> =
                ::serde_json::from_reader(br).with_context(|| {
                    format!("failed to deserialize config file {config_path:?}!")
                })?;
            debug!("Config artifact {config_path:?} ==> {c:#?}");
        }
        Ok(())
    }

    #[cfg(feature = "rt-fc")]
    #[test]
    #[traced_test]
    fn parse_fc_config_artifacts() -> Result<()> {
        for de in read_dir(CFG_DIR).with_context(|| format!("failed to read_dir in {CFG_DIR:?}"))? {
            let direntry = de.context("DirEntry")?;
            let config_path = direntry.path();
            if let Some(file_name) = direntry.file_name().to_str()
                && !(file_name.contains("fc_") || file_name.contains("fc."))
            {
                debug!("Skipping config file {config_path:?}");
                continue;
            }
            let br = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .open(&config_path)
                    .context("failed to read config file")?,
            );
            let c: SnaplaceConfig<crate::worker::runtime::fc::FirecrackerConfig> =
                ::serde_json::from_reader(br).with_context(|| {
                    format!("failed to deserialize config file {config_path:?}!")
                })?;
            debug!("Config artifact {config_path:?} ==> {c:#?}");
        }
        Ok(())
    }

    #[test]
    fn config_keepalive() {
        //
        // fixed
        //
        assert_eq!(
            ::serde_json::from_str::<KeepAliveConfig>(
                r#"{
                    "policy": "fixed",
                    "config": {
                        "duration": "0us"
                    }
                }"#
            )
            .unwrap(),
            KeepAliveConfig::Fixed {
                duration: Some(Duration::ZERO),
                eviction: MemoryEvictionConfig::default(),
            }
        );
        assert_eq!(
            ::serde_json::from_str::<KeepAliveConfig>(
                r#"{
                    "policy": "fixed",
                    "config": {
                        "duration": null
                    }
                }"#
            )
            .unwrap(),
            KeepAliveConfig::Fixed {
                duration: None,
                eviction: MemoryEvictionConfig::default(),
            },
        );
        assert_eq!(
            ::serde_json::from_str::<KeepAliveConfig>(
                r#"{
                    "policy": "fixed",
                    "config": {}
                }"#
            )
            .unwrap(),
            KeepAliveConfig::Fixed {
                duration: None,
                eviction: MemoryEvictionConfig::default(),
            },
        );
        assert_eq!(
            ::serde_json::from_str::<KeepAliveConfig>(
                r#"{
                    "policy": "fixed",
                    "config": {
                        "duration": "15 min",
                        "eviction": {
                            "policy": "lru",
                            "config": {}
                        }
                    }
                }"#
            )
            .unwrap(),
            KeepAliveConfig::Fixed {
                duration: Some(Duration::from_secs(15 * 60)),
                eviction: MemoryEvictionConfig::LruWorker {},
            },
        );

        //
        // sd-static
        //
        assert_eq!(
            ::serde_json::from_str::<KeepAliveConfig>(
                r#"{
                    "policy": "sd-static",
                    "config": {
                        "duration": "0h"
                    }
                }"#
            )
            .unwrap(),
            KeepAliveConfig::SlowdownsStatic {
                duration: Some(Duration::ZERO),
                non_warm_duration: None,
                fixed_warm_functions_path: None,
                eviction: MemoryEvictionConfig::default(),
            },
        );
        assert_eq!(
            ::serde_json::from_str::<KeepAliveConfig>(
                r#"{
                    "policy": "sd-static",
                    "config": {
                        "duration": "7h",
                        "non_warm_duration": "1s",
                        "eviction": {
                            "policy": "rand",
                            "config": {}
                        }
                    }
                }"#
            )
            .unwrap(),
            KeepAliveConfig::SlowdownsStatic {
                duration: Some(Duration::from_secs(7 * 60 * 60)),
                non_warm_duration: Some(Duration::from_secs(1)),
                fixed_warm_functions_path: None,
                eviction: MemoryEvictionConfig::Random {},
            },
        );
    }
    #[test]
    fn config_keepalive_errs() {
        ::serde_json::from_str::<KeepAliveConfig>(
            r#"{
                "policy": "fixed",
                "config": null
            }"#,
        )
        .expect_err(r#""config" should not be null"#);

        ::serde_json::from_str::<KeepAliveConfig>(
            r#"{
                "policy": "fixed",
                "config": {
                    "duration": ""
                }
            }"#,
        )
        .expect_err(r#""duration" should be a valid duration"#);
    }

    #[test]
    fn config_placement() {
        assert_eq!(
            ::serde_json::from_str::<PlacementConfig>(
                r#"{
                    "policy": "fixed",
                    "config": {
                        "path": "/tmp/snapshots"
                    }
                }"#
            )
            .unwrap(),
            PlacementConfig::Fixed {
                path: Some(PathBuf::from("/tmp/snapshots")),
            }
        );
        assert_eq!(
            ::serde_json::from_str::<PlacementConfig>(
                r#"{
                    "policy": "fixed",
                    "config": {
                        "path": null
                    }
                }"#
            )
            .unwrap(),
            PlacementConfig::Fixed { path: None }
        );
        assert_eq!(
            ::serde_json::from_str::<PlacementConfig>(
                r#"{
                    "policy": "fixed",
                    "config": {}
                }"#
            )
            .unwrap(),
            PlacementConfig::Fixed { path: None }
        );

        assert_eq!(
            ::serde_json::from_str::<PlacementConfig>(
                r#"{
                    "policy": "sd-static",
                    "config": {
                        "csv_path": "skata.csv",
                        "slowdown_threshold_cold": 1.42,
                        "warm_device": "optane_dcpm",
                        "devices": {
                            "flash_ssd": {
                                "mountpoint": "/flash/ssd/",
                                "slowdown_threshold": 8.9
                            },
                            "optane_nvme": {
                                "mountpoint": "/optane/nvme/",
                                "slowdown_threshold": 6.7
                            },
                            "optane_dcpm": {
                                "mountpoint": "/optane/dcpm/",
                                "slowdown_threshold": 4.5
                            }
                        }
                    }
                }"#
            )
            .unwrap(),
            PlacementConfig::SlowdownsStatic(placement::sd_static::Config {
                csv_path: PathBuf::from("skata.csv"),
                slowdown_threshold_cold: 1.42,
                warm_device: placement::sd_static::SnapshotDestination::OptaneDCPM,
                devices: [
                    (
                        placement::sd_static::SnapshotDestination::FlashSSD,
                        placement::sd_static::Device {
                            mountpoint: PathBuf::from("/flash/ssd/"),
                            slowdown_threshold: Some(8.9)
                        }
                    ),
                    (
                        placement::sd_static::SnapshotDestination::OptaneNVMe,
                        placement::sd_static::Device {
                            mountpoint: PathBuf::from("/optane/nvme/"),
                            slowdown_threshold: Some(6.7)
                        }
                    ),
                    (
                        placement::sd_static::SnapshotDestination::OptaneDCPM,
                        placement::sd_static::Device {
                            mountpoint: PathBuf::from("/optane/dcpm/"),
                            slowdown_threshold: Some(4.5)
                        }
                    ),
                ]
                .into(),
                fixed_warm_functions_path: None,
            })
        )
    }
    #[test]
    fn config_placement_errs() {
        ::serde_json::from_str::<PlacementConfig>(
            r#"{
                "policy": "fixed",
                "config": null
            }"#,
        )
        .expect_err(r#""config" cannot be null"#);
    }
}
