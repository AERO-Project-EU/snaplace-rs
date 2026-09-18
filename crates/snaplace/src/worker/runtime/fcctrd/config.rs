use std::{net::Ipv4Addr, path::PathBuf};

use compact_str::CompactString;
use serde::Deserialize;

/// Configuration for communicating with firecracker-containerd (useful to [`Worker`]s).
///
/// [`Worker`]: crate::worker::Worker
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct FirecrackerContainerdConfig {
    pub address: PathBuf,
    pub ttrpc_address: PathBuf,
    pub namespace: String,
    pub snapshotter: CompactString,
    pub nameservers: Vec<Ipv4Addr>,
    pub ctr: PathBuf,
}

impl FirecrackerContainerdConfig {
    pub const DEFAULT_ADDRESS: &'static str = "/var/run/firecracker-containerd/containerd.sock";
    pub const DEFAULT_TTRPC_ADDRESS: &'static str =
        "/var/run/firecracker-containerd/containerd.sock.ttrpc";
    pub const DEFAULT_NAMESPACE: &'static str = "default";
    pub const DEFAULT_SNAPSHOTTER: &'static str = "devmapper";
    pub const DEFAULT_NAMESERVERS: [Ipv4Addr; 2] =
        [Ipv4Addr::new(1, 1, 1, 1), Ipv4Addr::new(1, 0, 0, 1)];
    /// As a default value, assume that the `ctr` binary built for firecracker-containerd is
    /// already in `PATH`.
    pub const DEFAULT_CTR: &'static str = "firecracker-ctr";
}

impl Default for FirecrackerContainerdConfig {
    fn default() -> Self {
        Self {
            address: PathBuf::from(Self::DEFAULT_ADDRESS),
            ttrpc_address: PathBuf::from(Self::DEFAULT_TTRPC_ADDRESS),
            namespace: String::from(Self::DEFAULT_NAMESPACE),
            snapshotter: CompactString::from(Self::DEFAULT_SNAPSHOTTER),
            nameservers: Self::DEFAULT_NAMESERVERS.into(),
            ctr: PathBuf::from(Self::DEFAULT_CTR),
        }
    }
}
