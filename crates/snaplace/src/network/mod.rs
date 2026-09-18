mod error;
mod ip_utils;
mod manager;
pub mod providers;
mod tap;

pub use error::Error;
pub use error::Result;
pub(crate) use manager::NetworkManager;
pub(crate) use manager::NetworkManagerHandle;
pub(crate) use manager::NetworkManagerRef;
pub use providers::Resource;
pub use providers::SandboxNetworkingProvider;
pub(crate) use tap::Tap;
pub(crate) use tap::TapBuilder;
#[allow(unused_imports)]
pub(crate) use tap::TapDevice;
pub use tap::TapInfo;

/// All tap (or maybe any other?) interfaces managed by `snaplace` belong to a single group, to:
/// - greatly facilitate `{ip,nf}tables` setup: it may now occur only once, at the beginning,
///   for the whole devgroup;
/// - reduce the overhead of creating a new [`TapDevice`]: no more need to create a separate
///   `{ip,nf}tables` rule per [`TapDevice`] along with its initialization.
///
/// # References
///
/// - <https://serverfault.com/a/985167/253191>
/// - <https://unix.stackexchange.com/a/683166/65000>
/// - <https://manpages.debian.org/unstable/iptables/iptables-extensions.8.en.html#devgroup>
/// - <https://wiki.nftables.org/wiki-nftables/index.php/Matching_packet_metainformation#Matching_by_interface>
pub const DEVGROUP: u32 = 0xFAA5CE11;
