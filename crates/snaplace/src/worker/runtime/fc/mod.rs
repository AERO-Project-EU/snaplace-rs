mod error;
mod info;
#[path = "snaplace.runtime.fc.rs"]
pub mod pb;
mod runtime;
mod sandbox;

pub use error::Error;
pub use info::FcFunctionInfo;
pub use runtime::Firecracker;
pub use sandbox::MicroVmState;
pub use sandbox::SnapshotFiles;

///////////////////////////////////////////////////////////////////////////////////////////////////

use std::path::PathBuf;

use camino::Utf8PathBuf;

/// Configuration for directly managing Firecracker microVMs (used by [`Worker`]s).
///
/// [`Worker`]: crate::worker::Worker
#[derive(Debug, Clone, ::serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FirecrackerConfig {
    /// The path to the parent directory that contains one directory for each [`MicroVm`].
    ///
    /// [`MicroVm`]: crate::worker::runtime::fc::sandbox::MicroVm
    pub uvms_root_path: Utf8PathBuf,

    /// Path to the directory containing rootfs images for registered Functions.
    ///
    /// The runtime currently resolves a Function's rootfs by deriving a file
    /// name from its [`FunctionId`] and looking for the corresponding image
    /// under this directory.
    pub uvms_rootfs_path: PathBuf,

    /// Path to the Firecracker VMM binary executed for sandbox create/load.
    pub firecracker_bin: PathBuf,

    /// Path to the guest Linux kernel image passed to newly created uVMs.
    pub kernel_img: PathBuf,

    /// Selects the guest-kernel serial-console verbosity profile.
    ///
    /// This controls which predefined kernel boot-parameter set is used
    /// for newly created uVMs, affecting guest console visibility and
    /// `systemd`-related serial-console behavior.
    /// For now, it does not change the host-side Firecracker logger
    /// configuration.
    ///
    /// If omitted, this defaults to [`KernelVerbosity::Silent`].
    ///
    /// [`KernelVerbosity::Silent`]: runtime::KernelVerbosity::Silent
    #[serde(default)]
    pub kernel_verbosity: runtime::KernelVerbosity,
}

/// `fc` runtime tests w/ the Runtime harness (of the `test-utils` feature).
#[cfg(all(test, feature = "test-utils"))]
mod tests;
