use std::net::Ipv4Addr;

use futures::{future::ready, TryStreamExt};
use mac_address::MacAddress;
use rtnetlink::packet_route::{
    address::AddressAttribute,
    link::{LinkAttribute, LinkFlags, LinkMessage},
    route::RouteAttribute,
};
use tracing::error;

use crate::network::{ip_utils::link::Link, Error, DEVGROUP};

/// Assign the IPv4 address `ip_addr` to `link`.
///
/// Equivalent to `iproute2`'s:
///
/// ```bash
/// # ip addr add "$ip_addr"/"$prefix_len" dev "$link.name"
/// ```
///
/// # Note
///
/// In case of `/31` or `/32` subnet, no broadcast address is assigned.
#[inline]
pub(crate) async fn ip_addr_add(
    rtnl: &::rtnetlink::Handle,
    link: &mut Link,
    ip_addr: Ipv4Addr,
    prefix_len: u8,
) -> Result<(), Error> {
    // Add the new IP address
    let mut addr_add_req = rtnl.address().add(link.index(), ip_addr.into(), prefix_len);

    // In case of /31 and /32 subnets, there are no net nor brd addresses
    if prefix_len > 30 {
        addr_add_req
            .message_mut()
            .attributes
            .retain(|nla| !matches!(nla, AddressAttribute::Broadcast(_)));
    }

    addr_add_req.execute().await.map_err(|err| {
        error!(error = ?err, ?link, "Could not assign {ip_addr} to link: {err:#}");
        Error::RouteNetlink(err)
    })?;

    *link = Link::by_index(rtnl, link.index()).await.inspect_err(
        |err| error!(error = ?err, ?link, "Error looking up link after adding IP address: {err:#}"),
    )?;

    Ok(())
}

/// Flush all protocol (i.e., IPv4 and IPv6) addresses of the provided `link`.
///
/// Equivalent to `iproute2`'s:
///
/// ```bash
/// # ip addr flush dev "$link.name"
/// ```
///
/// # Errors
///
/// This function attempts to flush as many addresses as it can before returning (i.e., it does
/// not return on the first error it encounters, as long as there are more addresses to remove).
///
/// It may fail in three cases:
///  1. while enumerating the addresses,
///  2. while deleting an address,
///  3. when looking up the underlying link to update the provided `link`.
///
/// In the first two cases, the provided `link` will be looked up and updated before returning, so
/// that it correctly represents the actual underlying link.
/// In the third case, though, the looking up of the link itself must have failed, so we cannot be
/// sure whether the provided `link` still represents (i.e., up to date wrt) the underlying link.
#[allow(dead_code)]
pub async fn ip_addr_flush(rth: &::rtnetlink::Handle, link: &mut Link) -> Result<(), Error> {
    let mut ret = Ok(());
    let mut link_needs_update = false;

    let mut addrs = rth
        .address()
        .get()
        .set_link_index_filter(link.index())
        .execute();
    loop {
        match addrs.try_next().await {
            Ok(Some(addr)) => match rth.address().del(addr).execute().await {
                Ok(()) => {
                    // If any of the address deletions succeeds, we have
                    // to update the given `link` before returning.
                    link_needs_update = true;
                }
                Err(err) => {
                    error!(error = ?err, ?link, "Failed to delete an address: {err:#}");
                    ret = Err(Error::RouteNetlink(err));
                    // Continue to the next address (if any)
                }
            },
            Ok(None) => break,
            Err(err) => {
                error!(error = ?err, ?link, "Failed while looping through addresses: {err:#}");
                ret = Err(Error::RouteNetlink(err));
                break;
            }
        }
    }

    if link_needs_update {
        *link = Link::by_index(rth, link.index()).await.inspect_err(|err| {
            error!(
                error = ?err, ?link, "Error looking up link after deleting IP address(es): {err:#}",
            )
        })?;
    }
    ret
}

/// Add `link` to a device group.
///
/// Equivalent to `iproute2`'s:
///
/// ```bash
/// # ip link set dev "$link.name" group "$DEVGROUP"
/// ```
///
/// This can be used by `iptables`' [`devgroup` module][devgroup] to match device group or a
/// packet's incoming/outgoing interface. E.g.:
/// ```bash
/// # iptables -A FORWARD -o "$HOST_IF" -m devgroup --src-group "$((0xFAA5CE11))" -j ACCEPT
/// ```
/// or equivalently using `nftables`:
/// ```bash
/// # iptables-translate -A FORWARD -o '$HOST_IF' -m devgroup --src-group "$((0xFAA5CE11))" -j ACCEPT
/// nft add rule ip filter FORWARD oifname "$HOST_IF" iifgroup 0xfaa5ce11 counter accept
/// ```
///
/// [devgroup]: https://manpages.debian.org/unstable/iptables/iptables-extensions.8.en.html#devgroup
#[inline]
pub(crate) async fn set_link_group(
    rtnl: &::rtnetlink::Handle,
    link: &mut Link,
) -> Result<(), Error> {
    let mut lmsg = LinkMessage::default();
    lmsg.header.index = link.index();
    lmsg.attributes.push(LinkAttribute::Group(DEVGROUP));
    let set_req = rtnl.link().set(lmsg);

    set_req.execute().await.map_err(|err| {
        error!(error = ?err, ?link, "Could not change the group of link: {err:#}");
        Error::RouteNetlink(err)
    })?;

    *link = Link::by_index(rtnl, link.index()).await.inspect_err(|err| {
        error!(error = ?err, ?link, "Error looking up link after changing its group: {err:#}")
    })?;

    Ok(())
}

/// Set the `link`'s state to `UP`.
///
/// Equivalent to `iproute2`'s:
///
/// ```bash
/// # ip link set "$link.name" up
/// ```
#[inline]
pub(crate) async fn set_link_up(rtnl: &::rtnetlink::Handle, link: &mut Link) -> Result<(), Error> {
    let mut lmsg = LinkMessage::default();
    lmsg.header.index = link.index();
    lmsg.header.flags |= LinkFlags::Up;
    lmsg.header.change_mask |= LinkFlags::Up;
    let set_req = rtnl.link().set(lmsg);

    set_req.execute().await.map_err(|err| {
        error!(error = ?err, ?link, "Could not set link to UP: {err:#}");
        Error::RouteNetlink(err)
    })?;

    *link = Link::by_index(rtnl, link.index()).await.inspect_err(
        |err| error!(error = ?err, ?link, "Error looking up link after setting it to UP: {err:#}"),
    )?;

    Ok(())
}

/// Set the `link`'s MAC address to the given `address`.
///
/// Equivalent to `iproute2`'s:
///
/// ```bash
/// # ip link set dev "$link.name" address "$address"
/// ```
pub(crate) async fn set_link_address(
    rtnl: &::rtnetlink::Handle,
    link: &mut Link,
    address: &MacAddress,
) -> Result<(), Error> {
    let mut lmsg = LinkMessage::default();
    lmsg.header.index = link.index();
    lmsg.attributes
        .push(LinkAttribute::Address(address.bytes().to_vec()));
    let set_req = rtnl.link().set(lmsg);

    set_req.execute().await.map_err(|err| {
        error!(error = ?err, ?link, "Could not set MAC address '{address}' to link: {err:#}");
        Error::RouteNetlink(err)
    })?;

    *link = Link::by_index(rtnl, link.index()).await.inspect_err(|err| {
        error!(error = ?err, ?link, "Error looking up link after changing its MAC address: {err:#}")
    })?;

    Ok(())
}

/// See the Notes section in [`flush_routes`] (where this constant was also supposed to be used).
#[allow(dead_code)]
pub const ROUTE_FLUSH_PATH: &str = "/proc/sys/net/ipv4/route/flush";

/// Flush all routes (both IPv4 and IPv6) related to the provided `link`.
///
/// Equivalent to `iproute2`'s:
///
/// ```bash
/// # ip route flush dev "$link.name"
/// ```
///
/// # Errors
///
/// This function attempts to flush as many routes as it can before returning (i.e., it does
/// not return on the first error it encounters, as long as there are more routes to remove).
///
/// It may fail in two cases:
///  1. while enumerating the routes,
///  2. while deleting a route.
//  3. when attempting to flush the IPv4 route cache (by writing to [`ROUTE_FLUSH_PATH`]).
///
/// # Notes
///
/// - IPv4 caching is probably unnecessary/no-op nowadays (Linux >=v3.6), according to [this][1]
///   and its [linked][2] [references][3].
///
/// [1]: https://gitlab.com/openconnect/vpnc-scripts/-/merge_requests/30
/// [2]: https://lwn.net/Articles/507852/
/// [3]: https://git.kernel.org/pub/scm/linux/kernel/git/netdev/net-next.git/commit/?id=89aef8921bfbac22f00e04f8450f6e447db13e42
#[allow(dead_code)]
pub async fn flush_routes(rth: &::rtnetlink::Handle, link: &mut Link) -> Result<(), Error> {
    let mut ret = Ok(());

    let mut routes_stream = rth
        .route()
        .get(Default::default())
        .execute()
        .try_filter(|route| {
            ready(route.attributes.iter().any(|ra| {
                ra == &RouteAttribute::Oif(link.index())
                //|| ra == &RouteAttribute::Iif(link.index()) // NOTE(ckatsak): Omitted because
                // iproute2's filter `dev "$link.name"` appears to be treating it only as oif:
                // https://github.com/iproute2/iproute2/blob/866e1d107b7de68ca1fcd1d4d5ffecf9d96bff30/ip/iproute.c#L1893-L1896
            }))
        });
    loop {
        match routes_stream.try_next().await {
            Ok(Some(route)) => {
                if let Err(err) = rth.route().del(route).execute().await {
                    error!(error = ?err, ?link, "Error while flushing a route: {err:#}");
                    ret = Err(Error::RouteNetlink(err));
                }
            }
            Ok(None) => {
                // NOTE(ckatsak): The following is probably unnecessary; see Notes section above
                //if let Err(err) = ::tokio::fs::write(ROUTE_FLUSH_PATH, b"-1").await {
                //    error!(error = ?err, "Failed to flush IPv4 route cache: {err:#}");
                //    if ret.is_ok() {
                //        ret = Err(Error::Io {
                //            msg: "failed to flush IPv4 route cache".into(),
                //            source: err,
                //        })
                //    };
                //}
                return ret;
            }
            Err(err) => {
                error!(error = ?err, ?link, "Error while looping through routes: {err:#}");
                return Err(Error::RouteNetlink(err));
            }
        }
    }
}
