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
pub const VERSION_INFO: &str = ::const_format::formatcp!(
    "{SHORT_VERSION}\n\n{BUILD_INFO}\n\n{}",
    ::snaplace::version::VERSION_INFO
);

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
