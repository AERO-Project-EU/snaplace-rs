pub mod issuer;
pub mod runtime;

mod actor;
mod error;
mod handles;
mod messages;
mod types;

// expose as part of crate's API
pub use error::Error;
pub use issuer::RequestIssuer;
pub use runtime::Runtime;
pub use runtime::Sandbox;
pub use types::SandboxStateRef;

// re-export throughout the crate
pub(crate) use actor::Worker;
pub(crate) use error::SpawnError;
pub(crate) use handles::WorkerHandle;
pub(crate) use handles::WorkerRef;
pub(crate) use messages::CreateSnapshotResult;
pub(crate) use messages::OutboundMessage;
pub(crate) use messages::PrepareSandboxResult;
pub(crate) use types::OutChannels;
pub(crate) use types::SandboxExit;
pub(crate) use types::WorkerExit;
pub(crate) use types::WorkerId;

// re-export only to submodules
use error::Result;
use messages::ControlMessage;
use messages::Invocation;

/// The maximum duration a [`Worker`] may wait for [`RequestIssuer::issue_request`]
/// to return before timing out.
///
/// Enforced when no [`WorkerConfig::issuer_timeout`] overrides it.
///
/// [`WorkerConfig::issuer_timeout`]: crate::conf::WorkerConfig::issuer_timeout
pub const DEFAULT_ISSUER_TIMEOUT: ::std::time::Duration = ::std::time::Duration::from_secs(300);

#[cfg(feature = "__toy")]
::static_assertions::assert_impl_all!(
    Worker<
        runtime::toy::ToyRt,
        crate::request::toy::ToyStringRequest,
        crate::response::toy::ToyStringResponse,
        issuer::toy::ToyIssuer,
        crate::network::Tap,
    >: Send,
);
