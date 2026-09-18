use std::net::Ipv4Addr;

use super::ip_utils::subnet_pool::SubnetPoolError;

pub type Result<T> = ::std::result::Result<T, self::Error>;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("IP subnet pool error")]
    SubnetPool(#[from] SubnetPoolError),

    #[error("I/O error: {msg}")]
    Io {
        msg: Box<str>,
        #[source]
        source: ::tokio::io::Error,
    },

    #[error("error initializing new tap device")]
    TapInit(#[source] Box<Self>),

    /// Failure while creating a new tap device (`ioctl(TUNSETIFF)`).
    #[error("failed to create new tap interface (TUNSETIFF)")]
    TunSetIff(#[source] ::rustix::io::Errno),

    /// Failure while making a tap device persistent (`ioctl(TUNSETPERSIST)`).
    #[error("failed to make the tap interface persistent (TUNSETPERSIST)")]
    TunSetPersist(#[source] ::rustix::io::Errno),

    /// Failure while awaiting on a [`JoinHandle`] of a `tokio` task spawned in the blocking thread
    /// pool.
    ///
    /// [`JoinHandle`]: ::tokio::task::JoinHandle
    #[error("failed to join blocking tokio task")]
    JoinTask(#[source] ::tokio::task::JoinError),

    /// Failure while looking up a link.
    #[error("could not find the link via rtnetlink")]
    LinkNotFound,

    /// Failure related to `rtnetlink`.
    #[error("error retrieved from the rtnetlink crate")]
    RouteNetlink(#[source] ::rtnetlink::Error),

    /// Error parsing MAC address.
    #[error("error parsing MAC address from: '{src}'")]
    ParseMacAddr {
        src: Box<str>,
        #[source]
        source: ::mac_address::MacParseError,
    },

    /// The MAC address attempted to be assigned to an interface was already in use by another.
    #[error("MAC address '{0}' already in use")]
    MacAddrInUse(::mac_address::MacAddress),

    /// No NLA related to "`about`" was returned by `rtnetlink`.
    #[error("rtnetlink returned no NLA related to '{about}'")]
    MissingNla { about: Box<str> },

    /// Emitted by the [`NetworkManager`] when no more IP addresses can be made available through
    /// its [IPv4 subnet pool].
    ///
    /// [`NetworkManager`]: super::NetworkManager
    /// [IPv4 subnet pool]: crate::network::ip_utils::subnet_pool
    #[error("no more IPv4 subnets are available at the pool")]
    NoMoreIpv4Subnets,

    /// Error forwarding a message to the [`NetworkManager`].
    ///
    /// [`NetworkManager`]: super::NetworkManager
    #[error("failed to forward allocation request to NetworkManager: {0}")]
    Forward(Box<str>),

    /// Returned by [`TapBuilder::build`] in case of failure to create a new [`TapDevice`], to
    /// allow its caller to free any associated resources (which cannot be retrieved via the
    /// [`TapBuilder`] itself anymore, since the latter is consumed by [`TapBuilder::build`]).
    ///
    /// [`TapBuilder`]: super::tap::TapBuilder
    /// [`TapBuilder::build`]: super::tap::TapBuilder::build
    /// [`TapDevice`]: super::tap::TapDevice
    #[error("failed to build a new TAP device for {ip_addr}/{prefix_len}")]
    TapBuild {
        ip_addr: Ipv4Addr,
        prefix_len: u8,
        #[source]
        source: Box<Self>,
    },

    /// Returned by [`TapBuilder::build_or_adopt`] in case of failure to adopt an existing TAP
    /// device (after attempting to create a new one), to return a corresponding [`TapDevice`].
    ///
    /// [`TapBuilder::build_or_adopt`]: super::tap::TapBuilder::build_or_adopt
    /// [`TapDevice`]: super::tap::TapDevice
    #[error("failed to adopt busy TAP device '{name}' ({ip_addr}/{prefix_len})")]
    TapAdopt {
        name: Box<str>,
        ip_addr: Ipv4Addr,
        prefix_len: u8,
        #[source]
        source: Box<Self>,
    },

    /// Error sent by [`NetworkManager`] as a response to network allocation requests incoming
    /// while the [`NetworkManager`] is shutting down.
    ///
    /// [`NetworkManager`]: super::NetworkManager
    #[error("NetworkManager is currently shutting down; refusing all allocation requests")]
    ShuttingDown,

    /// Error returned by [`NetworkManager::run`] when all its senders have been dropped
    /// unexpectedly; i.e., without a prior quit notification followed by the graceful termination
    /// and cleanup procedure.
    ///
    /// [`NetworkManager::run`]: super::NetworkManager::run
    #[error("All NetworkManager's handles and refs have been unexpectedly dropped")]
    UnexpectedShutDown,
}
