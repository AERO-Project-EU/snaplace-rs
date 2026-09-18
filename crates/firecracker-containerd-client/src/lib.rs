mod client;
mod container;
mod error;
mod netif;
pub mod oci;
mod task;
mod vm;

pub use client::Client;
pub use client::FlushData;
pub use container::Builder as ContainerBuilder;
pub use error::{Error, Result};
pub use netif::NetworkInterfaceBuilder;
pub use oci::RuntimeSpecSource;
pub use task::Task;
pub use vm::{AfterSnapshotLoad, Builder as VmBuilder, Vm};

pub use firecracker_containerd_ttrpc::types::FirecrackerNetworkInterface;

/// The (firecracker-)containerd snapshotter used by default (i.e., unless specified otherwise by
/// the caller).
pub const DEFAULT_SNAPSHOTTER: &str = "devmapper"; // in: Client, Vm (for ContainerBuilder)
