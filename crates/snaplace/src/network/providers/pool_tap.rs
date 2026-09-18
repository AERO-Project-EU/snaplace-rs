use std::{collections::VecDeque, net::Ipv4Addr};

use compact_str::{format_compact, CompactString};
use ipnet::Ipv4Net;
use tokio::task::JoinSet;
use tracing::{error, instrument, trace, warn, Level};

use crate::{
    conf::PoolTapsConfig,
    network::{
        ip_utils::subnet_pool::fifo_exact::FifoExactIpv4SubnetPool, tap::TapInfo, Error, Result,
        SandboxNetworkingProvider, Tap, TapBuilder,
    },
};

// TODO(dchar): I think that since this is common with `plain_tap.rs` it makes
// more sense to live in `mod.rs`.
/// The prefix length used for microVMs' IP subnets.
pub const PREFIX_LEN: u8 = 31;

/// Not the theoretical limit, but it is safe to assume that at no point in time will there be need
/// more than [`u16::MAX`] TAP devices.
const MAX_POOL_SIZE: u16 = u16::MAX;

#[derive(Debug)]
pub struct PoolTapDevices {
    ip_pool: FifoExactIpv4SubnetPool<PREFIX_LEN>,
    free: VecDeque<(Ipv4Net, Tap)>,
    rtnl: ::rtnetlink::Handle,
}

impl PoolTapDevices {
    pub async fn new(config: &PoolTapsConfig) -> Result<Self> {
        let mut ip_pool = FifoExactIpv4SubnetPool::new(config.subnet).inspect_err(|err| {
            error!(error = ?err, "Failed to allocate new Ipv4SubnetPool: {err:#}");
        })?;

        let (conn, rtnl, _) = ::rtnetlink::new_connection().map_err(|err| {
            const MSG: &str = "could not create new rtnetlink connection";
            error!(error = ?err, MSG);
            Error::Io {
                msg: MSG.into(),
                source: err,
            }
        })?;
        ::tokio::spawn(conn);

        let pool_size = ::std::cmp::min(
            1 << (PREFIX_LEN - config.subnet.prefix_len()),
            ::std::cmp::min(config.capacity, usize::from(MAX_POOL_SIZE)),
        );

        let mut tap_tasks = JoinSet::new();
        for _ in 0..pool_size {
            let Some(subnet) = ip_pool.get_next() else {
                error!("No more IPv4 subnets are available in the pool");
                return Err(Error::NoMoreIpv4Subnets);
            };

            let mut hosts = subnet.hosts();
            let gw = hosts.next().expect("subnet's first host");
            let vm_ip = hosts.next().expect("subnet's last host");
            debug_assert!(hosts.next().is_none(), "this subnet is not /31");

            let name = Self::resource_name(vm_ip).expect("impl below; never None");
            let builder = TapBuilder::new(
                rtnl.clone(),
                TapInfo {
                    name,
                    mac_addr: None,
                    ip_addr: vm_ip,
                    prefix_len: PREFIX_LEN,
                    gateway: gw,
                },
            );

            let _abort_h = tap_tasks.spawn(async move {
                builder
                    .build_or_adopt()
                    .await
                    .map(|tap| (subnet, Tap::Device(tap)))
            });
        }
        let free = tap_tasks
            .join_all()
            .await
            .into_iter()
            .collect::<Result<VecDeque<_>>>()?;

        Ok(Self {
            ip_pool,
            free,
            rtnl,
        })
    }

    /// Construct the name of the [`Self::Resource`] (the [`Tap`]) based on the provided
    /// [`Ipv4Addr`].
    ///
    /// [`Self::Resource`]: SandboxNetworkingProvider::Resource
    #[inline]
    fn resource_name(ip_addr: Ipv4Addr) -> Option<CompactString> {
        let [a, b, c, d] = ip_addr.octets();
        Some(format_compact!("uvm-{a:02x}-{b:02x}-{c:02x}-{d:02x}"))
    }
}

// FIXME(ckatsak): When to replenish the pool? TODO:
// - Drainage detection
//   * configurable percentage threshold, with a sane default (when at ~10% maybe?)
//   * could happen in any of `alloc`, `request`, `dealloc` (or all of them)
// - Upon detection, create a number of `TapBuilder`s (maybe 10% more?), and spawn tasks to
//   asynchronously create the actual `Tap`s.
// - Who will reap these tasks, when, and how will the newly created `Tap`s be collected?
//   * If we involve `NetworkManager`, the latter should offer some sort of generic job offloading
//     mechanism that simply orchestrates their execution (driving the `Future`s), collects the
//     results (new type parameters? type erasure? meh..), and passes them on to the original
//     caller (i.e., `PlainTapDevices`, in this case) to manage.
//     - Pro: `NetworkManager` can drive the `Future`s to their completion one-by-one through its
//       main event loop channel, only when there are no pending `network::Message`s (e.g., via a
//       `biased` `::tokio::select!`). This should minimize the upfront overhead of TAP creation.
//     - Pro: if designed well, this will be usable by future `SandboxNetworkingProvider` impls.
//     - Con: complexity
//   * If we avoid involving the `NetworkManager`, we have to:
//     - store the TAP creation `Future`s in `PoolTapDevices`;
//     - decide _when_ to drive their completion (i.e., at which point should we `.await` them);
//       in other words, we have to decide exactly when the TAP creation overhead will be incurred
//       among `alloc`, `request` and `dealloc`.
//       * in `alloc`: the caller (Worker) waiting for its resource (and possibly many more
//         concurrent callers) will _all_ pay the cost of TAP creation; this is probably our worst
//         choice.
//       * in `request`: currently this is not a real option, as `request` is only called on
//         snapshot reinstatement, so the pool will never be replenished while actually running.
//       * in `dealloc`: this is probably our least bad option: the caller will not be waiting to
//         handle a new invocation request (so, we will not be on its hot path); however there may
//         be `network::Message`s from other `Worker`s queued for their `alloc`s (and we will
//         certainly be in _their_ hot path).
//     - Pro: simpler and contained implementation
//     - Con: under load, TAP creation overheads burden the (potentially multiple) callers
//   * Renounce structured concurrency, spawning orphaned tasks and letting the `tokio` runtime
//     drive them to completion. In this case, I guess we should introduce locks to access the
//     pool, so that these tasks can independently add their created `Tap`s when they are ready?
//     - Pro: simpler and contained implementation
//     - Con: introduction of locks? TAP creation overheads (presumably) equally burden all tokio
//       tasks in the system (rather than incurring right on the hot paths), but are also augmented
//       by (similaryly equally distributed) locking overheads every time the pool is accessed (?)
impl SandboxNetworkingProvider for PoolTapDevices {
    type Resource = Tap;

    #[instrument(level = Level::TRACE, skip_all)]
    fn alloc(&mut self) -> Result<Self::Resource> {
        // Fast path: if there is a free `Tap` in the pool, take it.
        if let Some((_subnet, tap)) = self.free.pop_back() {
            return Ok(tap);
        }

        // Slow path: if no more free `Tap`s in the pool, allocate resources for a `TapBuilder`,
        // thus offloading the real overhead of TAP creation to the caller (i.e., `Worker`).
        let Some(subnet) = self.ip_pool.get_next() else {
            error!("No more IPv4 subnets are available in the pool");
            return Err(Error::NoMoreIpv4Subnets);
        };

        let mut hosts = subnet.hosts();
        let gw = hosts.next().expect("subnet's first host");
        let vm_ip = hosts.next().expect("subnet's last host");
        debug_assert!(hosts.next().is_none(), "this subnet is not /31");

        let name = Self::resource_name(vm_ip).expect("impl above; never None");

        Ok(Tap::Builder(TapBuilder::new(
            self.rtnl.clone(),
            TapInfo {
                name,
                mac_addr: None,
                ip_addr: vm_ip,
                prefix_len: PREFIX_LEN,
                gateway: gw,
            },
        )))
    }

    // TODO(dchar): `VecDeque` is highly inappropriate data structure for the `request`
    // functionality. Both searching and removing (which results in copies) cost. But `request` is
    // off the critical path.
    #[instrument(level = Level::TRACE, skip(self))]
    async fn request(&mut self, tap_info: TapInfo) -> Result<Self::Resource> {
        let req_subnet = Ipv4Net::new_assert(tap_info.gateway, PREFIX_LEN);

        // Is the requested tap already in the pool?
        for (i, t) in self.free.iter().enumerate() {
            if t.0 == req_subnet {
                return Ok(self.free.remove(i).unwrap().1); // SAFETY: Due to enumeration we know
                                                           // `i` cannot be out of bounds
            }
        }

        // The tap is not cached, follow the `PlainTapDevices::request` logic
        match self.ip_pool.get(req_subnet) {
            Ok(resp_subnet) => debug_assert_eq!(req_subnet, resp_subnet),
            Err(err) => {
                error!(error = ?err, "Failed to get specific IPv4 subnet: {err:#}");
                return Err(Error::SubnetPool(err));
            }
        }

        TapBuilder::new(self.rtnl.clone(), tap_info)
            .build_or_adopt()
            .await
            .inspect_err(
                |err| warn!(error = ?err, "Failed to build or adopt requested Tap: {err:#}"),
            )
            .map(Tap::Device)
    }

    // TODO(dchar): Do not actually deallocate the Tap device, but add it to the `PoolTapDevices.free` list.
    // This raises a question: when should we clean the resources properly? Should we implement the `Drop`
    // trait for `Tap` devices and call something like `Tap::delete_tap` there?
    #[instrument(level = Level::TRACE, skip(self))]
    async fn dealloc(&mut self, tap: Self::Resource) -> Result<()> {
        debug_assert_eq!(PREFIX_LEN, tap.prefix_len());
        if let Tap::Device(ref tap_device) = tap {
            let subnet = Ipv4Net::new_assert(tap_device.info().gateway, PREFIX_LEN);
            self.free.push_front((subnet, tap));
        } else {
            // TODO(dchar): Add proper error handling
            unreachable!("TODO: Add proper error handling")
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        let mut tap_destr_tasks = self
            .free
            .drain(..)
            .map(|(_addr, tap)| {
                let rtnl = self.rtnl.clone();
                let subnet =
                    Ipv4Net::new(tap.ip_addr(), PREFIX_LEN).expect("const PREFIX_LEN always sane");

                async move {
                    trace!(?tap, "Destroying...");
                    match tap {
                        Tap::Device(tap) => tap.destroy(&rtnl).await.inspect_err(|err| {
                            error!(error = ?err, "Failed to destroy TAP device in {subnet}: {err:#}");
                        })?,
                        Tap::Builder(_builder) => todo!("Is this even reachable?"), // FIXME(ckatsak)
                    }
                    // NOTE(ckatsak): No point in freeing IP subnets on shutdown.
                    Ok::<_, Error>(())
                }
            })
            .collect::<::tokio::task::JoinSet<_>>();
        while let Some(res) = tap_destr_tasks.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => error!(error = ?err, "Failed to destroy Tap: {err:#}"),
                Err(jerr) => error!(error = ?jerr, "Failed to join Tap destruction task: {jerr:#}"),
            }
        }
        Ok(())
    }
}
