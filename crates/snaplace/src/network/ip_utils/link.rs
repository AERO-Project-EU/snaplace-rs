use std::collections::HashSet;

use compact_str::CompactString;
use futures::{StreamExt, TryStreamExt};
use mac_address::{MacAddress, MacParseError};
use rtnetlink::packet_route::link::{LinkAttribute, LinkMessage};
use tracing::error;

use crate::network::Error;

#[derive(Debug, Clone)]
pub(crate) struct Link(pub LinkMessage);

impl Link {
    /// Look up a link in rtnetlink by its name.
    pub async fn by_name(
        rtnl: &::rtnetlink::Handle,
        name: impl Into<CompactString>,
    ) -> Result<Link, Error> {
        let name = name.into();
        Ok(Link(
            rtnl.link()
                .get()
                .match_name(name.to_string())
                .execute()
                .try_next()
                .await
                .map_err(|err| {
                    error!(error = ?err, "Error looking up link '{name}' by name");
                    Error::RouteNetlink(err)
                })?
                //.map_err(Error::RouteNetlink)?
                .ok_or_else(|| {
                    error!("Link '{name}' was not found");
                    Error::LinkNotFound
                })?,
        ))
    }

    /// Look up a link in rtnetlink by its index.
    pub async fn by_index(rtnl: &::rtnetlink::Handle, index: u32) -> Result<Link, Error> {
        Ok(Link(
            rtnl.link()
                .get()
                .match_index(index)
                .execute()
                .try_next()
                .await
                .map_err(|err| {
                    error!(error = ?err, "Error looking up link '{index}' by index");
                    Error::RouteNetlink(err)
                })?
                .ok_or_else(|| {
                    error!("Link with index '{index}' was not found");
                    Error::LinkNotFound
                })?,
        ))
    }

    /// Get the link's index, as retrieved by `rtnetlink`.
    #[inline(always)]
    pub fn index(&self) -> u32 {
        self.0.header.index
    }

    /// Retrieve the link's group, if returned by `rtnetlink`.
    #[allow(dead_code)] // Used to need this, so let's keep it around
    #[inline(always)]
    pub fn group(&self) -> Option<u32> {
        for nla in &self.0.attributes {
            if let LinkAttribute::Group(group) = nla {
                return Some(*group);
            }
        }
        None
    }

    /// Retrieve the link's MAC address, as returned by `rtnetlink`.
    ///
    /// # Errors
    ///
    /// An error is returned only in cases that:
    /// - the returned address cannot be parsed as a valid one;
    /// - no MAC address was found among the returned NLAs.
    pub fn mac_addr(&self) -> Result<MacAddress, Error> {
        for nla in self.0.attributes.iter() {
            //eprintln!("\t- nla: {nla:x?}");
            if let LinkAttribute::Address(mac) = nla {
                //eprintln!("mac-before: {mac:x?}");
                let mac = mac
                    .as_slice()
                    .try_into()
                    .map_err(|bytes| Error::ParseMacAddr {
                        src: format!("{bytes:?}").into_boxed_str(),
                        source: MacParseError::InvalidLength,
                    })?;
                //eprintln!("mac-after: {mac:x?}");
                return Ok(MacAddress::new(mac));
            }
        }
        Err(Error::MissingNla {
            about: "interface group".into(),
        })
    }

    /// Query rtnetlink to return all MAC addresses used by links, system-wide.
    #[allow(dead_code)]
    pub async fn mac_addresses_in_use<S: ::std::hash::BuildHasher + Default>(
        rtnl: &::rtnetlink::Handle,
    ) -> Result<HashSet<MacAddress, S>, Error> {
        rtnl.link()
            .get()
            .execute()
            .map_err(Error::RouteNetlink)
            .map(|r| r.map(Link).and_then(|l| l.mac_addr()))
            .try_collect()
            .await
    }

    /// Query rtnetlink and return all links in the system.
    #[allow(dead_code)]
    pub async fn list_all(rtnl: &::rtnetlink::Handle) -> Result<Vec<Link>, Error> {
        rtnl.link()
            .get()
            .execute()
            .map_ok(Link)
            .try_collect()
            .await
            .map_err(Error::RouteNetlink)
    }
}

::static_assertions::assert_impl_all!(Link: Send);

mod __old_unused {
    use compact_str::CompactString;
    use futures::TryStreamExt;
    use mac_address::{MacAddress, MacParseError};
    use rtnetlink::packet_route::link::{LinkAttribute, LinkMessage};
    use tracing::error;

    use crate::network::Error;

    #[allow(dead_code)]
    pub async fn mac_addr_by_link_name(
        rtnl: &::rtnetlink::Handle,
        name: impl Into<CompactString>,
    ) -> Result<MacAddress, Error> {
        let name = name.into();

        let mut links = rtnl.link().get().match_name(name.to_string()).execute();
        match links.try_next().await {
            Ok(Some(link)) => {
                for nla in link.attributes.into_iter() {
                    if let LinkAttribute::Address(mac) = nla {
                        let mac = mac.try_into().map_err(|bytes| Error::ParseMacAddr {
                            src: format!("{bytes:?}").into_boxed_str(),
                            source: MacParseError::InvalidLength,
                        })?;
                        return Ok(MacAddress::new(mac));
                    }
                }
                error!("Link '{name}' was not found");
                Err(Error::LinkNotFound)
            }
            Ok(None) => {
                error!("Link '{name}' was not found");
                Err(Error::LinkNotFound)
            }
            Err(err) => Err(Error::RouteNetlink(err)),
        }
    }

    #[allow(dead_code)]
    pub async fn link_mac_addr_by_name(
        rtnl: &::rtnetlink::Handle,
        name: impl Into<CompactString>,
    ) -> Result<MacAddress, Error> {
        //let link = link_by_name(rtnl, name.into()).await?;
        //mac_address(&link).await

        link_mac_addr(&link_by_name(rtnl, name).await?).await
    }

    #[allow(dead_code)]
    pub async fn link_by_name(
        rtnl: &::rtnetlink::Handle,
        name: impl Into<CompactString>,
    ) -> Result<LinkMessage, Error> {
        let name = name.into();
        rtnl.link()
            .get()
            .match_name(name.to_string())
            .execute()
            .try_next()
            .await
            .map_err(|err| {
                error!(error = ?err, "Error looking up link by name '{name}'");
                Error::RouteNetlink(err)
            })?
            .ok_or_else(|| {
                error!("Link '{name}' was not found");
                Error::LinkNotFound
            })
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub async fn link_index(link: &LinkMessage) -> u32 {
        link.header.index
    }

    #[allow(dead_code)]
    pub async fn link_mac_addr(link: &LinkMessage) -> Result<MacAddress, Error> {
        for nla in link.attributes.iter() {
            if let LinkAttribute::Address(mac) = nla {
                let mac = mac
                    .as_slice()
                    .try_into()
                    .map_err(|bytes| Error::ParseMacAddr {
                        src: format!("{bytes:?}").into_boxed_str(),
                        source: MacParseError::InvalidLength,
                    })?;
                return Ok(MacAddress::new(mac));
            }
        }
        error!("Could not find LinkAttribute::Address for link");
        Err(Error::LinkNotFound) // FIXME?
    }
}
