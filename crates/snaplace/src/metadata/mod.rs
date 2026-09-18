pub mod db;
mod fmd;
pub mod registration;
mod stats;
mod store;

pub use registration::FunctionInfo;
pub use stats::FunctionStats;
pub use stats::SandboxStats;
#[cfg(feature = "fmd-store-dash")]
pub use store::DashMapStore;
pub use store::FunctionMetadataStore;
pub use store::StdHashMapFmdStore;

/// A type whose values uniquely identify Functions registered with the system.
pub type FunctionId = ::arcstr::ArcStr;
