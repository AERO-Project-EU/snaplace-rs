mod admission;
pub mod conf;
pub mod control;
pub mod error;
pub mod metadata;
pub mod metrics;
pub mod network;
pub mod orchestrator;
pub mod request;
pub mod response;
mod sbpool;
pub mod snapman;
#[cfg(feature = "test-utils")]
pub mod testing;
pub mod utils;
pub mod worker;

pub use error::Error;
pub use error::Result;
pub use metadata::FunctionId;
pub use metadata::FunctionMetadataStore;
pub use orchestrator::Orchestrator;
pub use request::Request;
pub use request::Source as RequestSource;
pub use response::Response;
pub use response::Sink as ResponseSink;
pub use sbpool::keepalive;

/// Every Function invocation has a unique ID, represented by this type.
///
/// For now, we hope that this ID is <= 24 bytes long, and consider it to be a stack-allocated
/// string.
pub type InvocationId = ::compact_str::CompactString;
//pub type InvocationId = ::arcstr::ArcStr;

// TODO: Define opaque types (rather than newtype) for all ID types.
pub type SandboxId = ::compact_str::CompactString;

#[cfg(any(
    all(feature = "hasher-a", feature = "hasher-fold"),
    all(feature = "hasher-a", feature = "hasher-gx"),
    all(feature = "hasher-fold", feature = "hasher-gx"),
))]
compile_error!("Enable at most one of: hasher-a, hasher-fold, hasher-gx");
/// The [`BuildHasher`] used for maps and sets throughout the crate.
///
/// [`BuildHasher`]: ::std::hash::BuildHasher
#[cfg(feature = "hasher-a")]
pub type BuildHasher = ::ahash::RandomState;
#[cfg(feature = "hasher-fold")]
pub type BuildHasher = ::foldhash::fast::RandomState;
#[cfg(all(
    feature = "hasher-gx",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub type BuildHasher = ::gxhash::GxBuildHasher;
#[cfg(all(
    feature = "hasher-gx",
    not(any(target_arch = "x86_64", target_arch = "aarch64"))
))]
pub type BuildHasher = ::std::hash::RandomState;
#[cfg(not(any(feature = "hasher-a", feature = "hasher-fold", feature = "hasher-gx")))]
pub type BuildHasher = ::std::hash::RandomState;

pub mod version {
    /// Short one-line version information.
    pub const SHORT_VERSION: &str = ::const_format::formatcp!(
        "{} v{}-g{}{}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        env!("VERGEN_GIT_SHA"),
        match env!("VERGEN_GIT_DIRTY").as_bytes() {
            b"true" => "-dirty",
            _ => "",
        },
    );

    /// Extended version and build information.
    pub const VERSION_INFO: &str = ::const_format::formatcp!("{SHORT_VERSION}\n\n{BUILD_INFO}");

    /// Extended build information.
    pub const BUILD_INFO: &str = ::const_format::formatcp!(
        r#"Build Information:
          builder: {} @ {}
      builder CPU: {}
        timestamp: {}
  git:
           branch: {}
              SHA: {}
        timestamp: {}
            dirty: {}
  rustc:
          version: {}
          channel: {}
      host triple: {}
  cargo:
    target triple: {}
    debug profile: {}
        opt-level: {}
         features: {}
     dependencies: {}
        "#,
        env!("VERGEN_SYSINFO_USER"),
        env!("VERGEN_SYSINFO_OS_VERSION"),
        env!("VERGEN_SYSINFO_CPU_BRAND"),
        env!("VERGEN_BUILD_TIMESTAMP"),
        //
        env!("VERGEN_GIT_BRANCH"),
        env!("VERGEN_GIT_SHA"),
        env!("VERGEN_GIT_COMMIT_TIMESTAMP"),
        env!("VERGEN_GIT_DIRTY"),
        //
        env!("VERGEN_RUSTC_SEMVER"),
        env!("VERGEN_RUSTC_CHANNEL"),
        env!("VERGEN_RUSTC_HOST_TRIPLE"),
        //
        env!("VERGEN_CARGO_TARGET_TRIPLE"),
        env!("VERGEN_CARGO_DEBUG"),
        env!("VERGEN_CARGO_OPT_LEVEL"),
        env!("VERGEN_CARGO_FEATURES"),
        env!("VERGEN_CARGO_DEPENDENCIES"),
    );
}
