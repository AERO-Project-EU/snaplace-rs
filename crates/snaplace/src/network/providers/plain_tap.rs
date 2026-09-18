use std::net::Ipv4Addr;

use compact_str::{format_compact, CompactString};
use ipnet::Ipv4Net;
use tracing::{error, instrument, trace, warn, Level};

use crate::{
    conf::PlainTapsConfig,
    network::{
        ip_utils::subnet_pool::fifo_exact::FifoExactIpv4SubnetPool, tap::TapInfo, Error, Result,
        SandboxNetworkingProvider, Tap, TapBuilder,
    },
};

/// The prefix length used for microVMs' IP subnets.
pub const PREFIX_LEN: u8 = 31;

#[derive(Debug)]
pub struct PlainTapDevices {
    ip_pool: FifoExactIpv4SubnetPool<PREFIX_LEN>,
    rtnl: ::rtnetlink::Handle,
}

impl PlainTapDevices {
    pub fn new(config: &PlainTapsConfig) -> Result<Self> {
        let ip_pool = FifoExactIpv4SubnetPool::new(config.subnet).inspect_err(|err| {
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

        Ok(Self { ip_pool, rtnl })
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

impl SandboxNetworkingProvider for PlainTapDevices {
    type Resource = Tap;

    // Returns `TapBuilder`, rather than `TapDevice`, to offload the
    // overhead of creation to the caller (i.e., `Worker`).
    #[instrument(level = Level::TRACE, skip_all)]
    fn alloc(&mut self) -> Result<Self::Resource> {
        let Some(subnet) = self.ip_pool.get_next() else {
            error!("No more IPv4 subnets are available in the pool");
            return Err(Error::NoMoreIpv4Subnets);
        };

        let mut hosts = subnet.hosts();
        let gw = hosts.next().expect("subnet's first host");
        let vm_ip = hosts.next().expect("subnet's last host");
        debug_assert!(hosts.next().is_none(), "this subnet is not /31");

        let name = Self::resource_name(vm_ip).unwrap(); // SAFETY: impl above, never None

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

    #[instrument(level = Level::TRACE, skip(self))]
    async fn request(&mut self, tap_info: TapInfo) -> Result<Tap> {
        let req_subnet = Ipv4Net::new_assert(tap_info.gateway, PREFIX_LEN);
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

    #[instrument(level = Level::TRACE, skip(self))]
    async fn dealloc(&mut self, tap: Self::Resource) -> Result<()> {
        debug_assert_eq!(PREFIX_LEN, tap.prefix_len());
        let ip_addr = tap.ip_addr();
        let subnet = Ipv4Net::new(ip_addr, PREFIX_LEN).expect("const PREFIX_LEN always sane");
        if let Tap::Device(tap) = tap {
            tap.destroy(&self.rtnl).await?;
            // NOTE(ckatsak): Only return the subnet to the IP pool if the TapDevice has
            // been removed successfully.
        }
        trace!("Freeing IP resources for {ip_addr}/{PREFIX_LEN}");
        self.ip_pool.give_back(subnet);
        Ok(())
    }
}
