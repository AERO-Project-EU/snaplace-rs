use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use enum_map::{enum_map, EnumMap};
use enum_map_derive::Enum;
use serde::Deserialize;
use tracing::warn;

use crate::{
    snapman::{self, perfmon, PlacementAlgorithm, SnapshotPaths},
    utils::sd_static::{read_function_ids, SlowdownsCsvError, SlowdownsCsvReadIter},
    FunctionId,
};

// NOTE(ckatsak): Unit testing in `crate::conf` requires `PlacementConfig` to impl `PartialEq` and
// `PartialOrd`. `HashMap` does not impl the latter. We thus employ the newtype pattern to define
// `Map`, so that we alternate between `BTreeMap` (unit testing only) and `HashMap` (otherwise):
#[cfg(not(test))]
pub(crate) type Map<K, V> = ::std::collections::HashMap<K, V, crate::BuildHasher>;
#[cfg(test)]
pub(crate) type Map<K, V> = ::std::collections::BTreeMap<K, V>;

/// Configuration struct for the [`SlowdownsStatic`] snapshot placement algorithm.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(PartialOrd, PartialEq))]
pub struct Config {
    /// Path to the CSV file containing the calculated slowdown of each Function, based on past
    /// measurements[^1].
    ///
    /// [^1]: Eventually such slowdowns could be calculated online, but this is not supported yet.
    pub(crate) csv_path: PathBuf,

    /// Cold-start execution slowdown threshold, under which a Function is _always_ booted anew,
    /// without any snapshot.
    ///
    /// This value is also used as the slowdown threshold for any [`Device`] which does not specify
    /// its own [`slowdown_threshold`].
    ///
    /// [`slowdown_threshold`]: Device::slowdown_threshold
    #[serde(alias = "slowdown_threshold")]
    pub(crate) slowdown_threshold_cold: f64,

    /// All devices available to this placement policy for storing Sandbox snapshots.
    pub(crate) devices: Map<SnapshotDestination, Device>,

    /// The device to store snapshots of Functions marked as "warm" on.
    ///
    /// Possible values, along with the behavior they impose:
    /// - <code>[SnapshotDestination]::{[OptaneDCPM]|[OptaneNVMe]|[FlashSSD]}</code>: Store
    ///   `Sandbox` snapshots of Functions marked as "warm" to the corresponding [device's path].
    /// - [`SnapshotDestination::None`]: Do **NOT** create any snapshot for `Sandbox`es of
    ///   Functions marked as "warm". Note:
    ///   <div class="warning">
    ///   As a result, such <code>Sandbox</code>es would have to be cold-booted anew
    ///   in case they are evicted by the configured <code>eviction::Policy</code>.
    ///   </div>
    /// - [`SnapshotDestination::KeepAlive`]: For now, this is treated in the same way as
    ///   [`SnapshotDestination::None`], described right above.
    ///
    ///
    /// [OptaneDCPM]: SnapshotDestination::OptaneDCPM
    /// [OptaneNVMe]: SnapshotDestination::OptaneNVMe
    /// [FlashSSD]: SnapshotDestination::FlashSSD
    /// [device's path]: Device::mountpoint
    /// [1]: crate::sbpool::keepalive::eviction::Policy
    pub(crate) warm_device: SnapshotDestination,

    /// Optionally provided path to a newline-delimited file containing a list of [`FunctionId`]s
    /// which should always be considered "warm", regardless of their slowdowns.
    ///
    /// [`FunctionId`]: crate::FunctionId
    pub(crate) fixed_warm_functions_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(PartialOrd, PartialEq))]
pub(crate) struct Device {
    /// Path where snapshots assigned to this device should be stored.
    ///
    /// It is assumed that this path includes the mountpoint of the device.
    pub(crate) mountpoint: PathBuf,

    /// The slowdown threshold for a Function's execution time, under which it is considered
    /// acceptable for that Function to be served by snapshots residing in this device.
    ///
    /// If omitted, [`sd_static::Config`]'s `slowdown_threshold_cold` is used as the threshold
    /// of this device as well.
    ///
    /// [`sd_static::Config`]: Config
    pub(crate) slowdown_threshold: Option<f64>,
}

#[derive(Debug, Clone, Copy, Enum, PartialEq, Eq, Hash, Deserialize)]
#[cfg_attr(test, derive(PartialOrd, Ord))]
pub(crate) enum SnapshotDestination {
    /// Sandboxes should always boot anew (i.e., cold start).
    #[serde(alias = "none")]
    None,

    /// Sandboxes should remain as warm as possible (handled by the keep-alive policy in place).
    #[serde(alias = "keepalive", alias = "keep-alive", alias = "keep_alive")]
    KeepAlive,

    /// Sandbox snapshots should be stored on the Optane DCPM device.
    #[serde(alias = "optane_dcpm")]
    OptaneDCPM,

    /// Sandbox snapshots should be stored on the Optane NVMe SSD device.
    #[serde(alias = "optane_nvme")]
    OptaneNVMe,

    /// Sandbox snapshots should be stored on the Flash SSD device.
    #[serde(alias = "flash_ssd")]
    FlashSSD,
}

#[derive(Debug, Clone)]
pub struct SlowdownsStatic {
    /// See [`Config::devices`].
    devices: EnumMap<SnapshotDestination, Option<Device>>,
    /// Parsed [`Config::csv_path`].
    slowdowns: HashMap<FunctionId, SnapshotDestination, crate::BuildHasher>,
    /// See [`Config::warm_device`].
    warm_device: SnapshotDestination,
}

impl SlowdownsStatic {
    pub fn new(config: &Config) -> Result<Self, SlowdownsCsvError> {
        let devices = enum_map! {
            SnapshotDestination::None | SnapshotDestination::KeepAlive => None,
            dst => Some(
                config
                    .devices
                    .get(&dst)
                    .expect("Missing device from Config; TODO(ckatsak): handle w/ proper Error")
                    .clone(),
            )
        };

        let fixed_warm_function_ids = config
            .fixed_warm_functions_path
            .as_ref()
            .map(read_function_ids)
            .transpose()?
            .unwrap_or_default();

        let slowdowns = SlowdownsCsvReadIter::new(&config.csv_path)?
            .map(|entry| {
                let entry = entry?;
                let snap_dst = if fixed_warm_function_ids.contains(&entry.bench) {
                    SnapshotDestination::KeepAlive // kept warm; handled by the KeepAlivePolicy
                } else if entry.sd_cold < config.slowdown_threshold_cold {
                    SnapshotDestination::None // always cold boot
                } else if entry.sd_sflash
                    < config.devices[&SnapshotDestination::FlashSSD]
                        .slowdown_threshold
                        .unwrap_or(config.slowdown_threshold_cold)
                {
                    SnapshotDestination::FlashSSD
                } else if entry.sd_soptan
                    < config.devices[&SnapshotDestination::OptaneNVMe]
                        .slowdown_threshold
                        .unwrap_or(config.slowdown_threshold_cold)
                {
                    SnapshotDestination::OptaneNVMe
                } else if entry.sd_spmem
                    < config.devices[&SnapshotDestination::OptaneDCPM]
                        .slowdown_threshold
                        .unwrap_or(config.slowdown_threshold_cold)
                {
                    SnapshotDestination::OptaneDCPM
                } else {
                    SnapshotDestination::KeepAlive // kept warm; handled by the KeepAlivePolicy
                };
                Ok::<_, SlowdownsCsvError>((entry.bench, snap_dst))
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            devices,
            slowdowns,
            warm_device: config.warm_device,
        })
    }

    /// Returns all [`FunctionId`]s that we (the `SlowdownsStatic` snapshot placement policy)
    /// expect to be handled by the [`keepalive::Policy`] in effect (rather than us).
    ///
    /// [`keepalive::Policy`]: crate::sbpool::keepalive::Policy
    pub fn functions_kept_alive(&self) -> HashSet<FunctionId, crate::BuildHasher> {
        self.slowdowns
            .iter()
            .filter_map(|(fid, dst)| matches!(dst, SnapshotDestination::KeepAlive).then_some(fid))
            .cloned()
            .collect()
    }
}

impl PlacementAlgorithm for SlowdownsStatic {
    type PerformanceMonitor = perfmon::NoOp;

    #[inline]
    fn update_metrics(
        &mut self,
        _function_id: &FunctionId,
        _state: crate::worker::SandboxStateRef,
        _metrics: &<<Self::PerformanceMonitor as perfmon::PerformanceMonitor>::Handle as perfmon::PerformanceMonitorHandle>::Metrics,
        _issuer_duration: ::std::time::Duration,
    ) -> Result<(), snapman::Error> {
        Ok(())
    }

    fn query_path(
        &mut self,
        function_id: &FunctionId,
    ) -> Result<Option<SnapshotPaths>, snapman::Error> {
        const _MSG: &str = "all 3 devices found present during initialization";
        const _DST: SnapshotDestination = SnapshotDestination::OptaneNVMe;

        match self.slowdowns.get(function_id) {
            // If the Function is meant to be always cold-booted, instruct not to create a snapshot:
            Some(SnapshotDestination::None) => Ok(None),

            // If the Function is meant to be kept alive, the verdict depends on `self.warm_device`:
            Some(SnapshotDestination::KeepAlive)
                if matches!(self.warm_device, SnapshotDestination::None) =>
            {
                Ok(None)
            }
            Some(SnapshotDestination::KeepAlive)
                if matches!(self.warm_device, SnapshotDestination::KeepAlive) =>
            {
                Ok(None) // ... for now
            }
            Some(SnapshotDestination::KeepAlive) => {
                let warm_dev = self.devices[self.warm_device].as_ref().expect(_MSG);
                Ok(Some(SnapshotPaths {
                    state: warm_dev.mountpoint.clone(),
                    memory: warm_dev.mountpoint.clone(),
                }))
            }

            // If the Function is assigned to a specific device, return its paths:
            Some(dst) => Ok(Some(SnapshotPaths {
                state: self.devices[*dst].as_ref().expect(_MSG).mountpoint.clone(),
                memory: self.devices[*dst].as_ref().expect(_MSG).mountpoint.clone(),
            })),

            // TODO: How to handle unknown FunctionIds?
            // - For now, maybe store snapshot on Optane NVMe to avoid both:
            //   * long delays (due to cold boots or slow devices), and
            //   * occupying extra memory (by keeping them warm).
            // - Note, however, that this messes with our slowdown calculations.
            // - Eventually, online slowdown calculation should address it, I guess (?)
            None => {
                warn!(?function_id, "Unknown Function; storing to {_DST:?}...");
                Ok(Some(SnapshotPaths {
                    state: self.devices[_DST].as_ref().expect(_MSG).mountpoint.clone(),
                    memory: self.devices[_DST].as_ref().expect(_MSG).mountpoint.clone(),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anyhow::{Context, Result};
    use tracing::{debug, trace};
    use tracing_test::traced_test;

    use super::{Config, Device, SlowdownsStatic, SnapshotDestination};

    // Run with `-- --nocapture --ignored`
    #[ignore = "temporary; using specific paths on rootfs of icy2"]
    #[test]
    #[traced_test]
    fn sd_static_plcm_01() -> Result<()> {
        let policy = SlowdownsStatic::new(&Config {
            //csv_path: PathBuf::from("/opt/ckatsak/basenums08/run05/sd_rns.csv"),
            //csv_path: "/tmp/tmpfs/csv__b64_rns_p95__sd_7/sd_b64__running_slot__p95.csv".into(),
            //csv_path: "/tmp/tmpfs/csv__b16_rns_mean__sd_10/sd_b16__running_slot__mean.csv".into(),
            csv_path: "/tmp/tmpfs/csv__pop_b16_rns_mean__sd_10/sd_b16__running_slot__mean.csv"
                .into(),
            slowdown_threshold_cold: 10.,
            warm_device: SnapshotDestination::OptaneDCPM,
            devices: [
                (
                    SnapshotDestination::FlashSSD,
                    Device {
                        slowdown_threshold: Some(10.),
                        mountpoint: PathBuf::from("/opt/ckatsak/snapshots"),
                    },
                ),
                (
                    SnapshotDestination::OptaneNVMe,
                    Device {
                        mountpoint: PathBuf::from("/mnt/optane_nvme/christos/snapshots"),
                        slowdown_threshold: Some(10.),
                    },
                ),
                (
                    SnapshotDestination::OptaneDCPM,
                    Device {
                        mountpoint: PathBuf::from("/mnt/pmem0/ckatsak/snapshots"),
                        slowdown_threshold: Some(10.),
                    },
                ),
            ]
            .into(),
            fixed_warm_functions_path: None,
        })
        .context("failed to construct SlowdownsStatic policy")?;
        trace!("{policy:#?}");

        let nc = policy
            .slowdowns
            .iter()
            .filter(|&(_, &dst)| matches!(dst, SnapshotDestination::None))
            .count();
        let nfl = policy
            .slowdowns
            .iter()
            .filter(|&(_, &dst)| matches!(dst, SnapshotDestination::FlashSSD))
            .count();
        let nopt = policy
            .slowdowns
            .iter()
            .filter(|&(_, &dst)| matches!(dst, SnapshotDestination::OptaneNVMe))
            .count();
        let npm = policy
            .slowdowns
            .iter()
            .filter(|&(_, &dst)| matches!(dst, SnapshotDestination::OptaneDCPM))
            .count();
        let nw = policy
            .slowdowns
            .iter()
            .filter(|&(_, &dst)| matches!(dst, SnapshotDestination::KeepAlive))
            .count();
        debug!(
            "\n- #cold = {nc}\n- #flash = {nfl}\n- #optane = {nopt}\n- #pmem = {npm}\n- #warm = {nw}"
        );

        Ok(())
    }
}
