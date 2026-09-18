//! Example run:
//!
//! ```console
//! # SNAPLACE_TEST_FCCTRD=1 SNAPLACE_TEST_FCCTRD_CTR='/local/christos/src/fc-ctrd/firecracker-control/cmd/containerd/firecracker-ctr' cargo test -p snaplace --features test-utils --lib fcctrd -- --nocapture
//! ```
//! or equivalently:
//! ```console
//! # SNAPLACE_TEST_FCCTRD=1 SNAPLACE_TEST_FCCTRD_CTR='/local/christos/src/fc-ctrd/firecracker-control/cmd/containerd/firecracker-ctr' cargo nextest run -p snaplace --features test-utils --lib fcctrd --no-capture
//! ```

use std::{env, future::Future, path::PathBuf, sync::LazyLock};

use compact_str::{CompactString, ToCompactString};
use ipnet::Ipv4Net;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tracing::{error, instrument, trace, Level};
use ubyte::ToByteUnit;
use uuid::Uuid;

use crate::{
    conf::PlainTapsConfig,
    network::providers::plain_tap::PlainTapDevices,
    testing::runtime::{RuntimeHarness, RuntimeHarnessGuard},
    worker::runtime::{
        fcctrd::{FcctrdFunctionInfo, FirecrackerContainerd, FirecrackerContainerdConfig},
        Runtime, Sandbox,
    },
    FunctionId,
};

/// Shared boxed error type used by the `fcctrd` runtime tests.
type BoxError = Box<dyn ::std::error::Error + Send + Sync + 'static>;

/// Serializes the env-gated integration tests so they do not race each other.
static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Environment-derived test fixture for [`FirecrackerContainerd`].
///
/// Encapsulates information similar to [`FirecrackerContainerdConfig`].
#[derive(Debug, Clone)]
struct FcctrdTestEnv {
    /// Firecracker-containerd API socket path.
    address: PathBuf,
    /// Firecracker-containerd TTRPC socket path.
    ttrpc_address: PathBuf,
    /// Firecracker-containerd namespace.
    namespace: String,
    /// Firecracker-containerd snapshotter name.
    snapshotter: CompactString,
    /// Path to firecracker-containerd' `ctr` binary.
    ctr: PathBuf,
    /// Function image reference used for tests.
    image: String,
    /// TAP subnet used by [`PlainTapDevices`], the [`SandboxNetworkingProvider`]
    /// employed for the tests.
    ///
    /// [`SandboxNetworkingProvider`]: crate::network::SandboxNetworkingProvider
    subnet: Ipv4Net,
}

impl FcctrdTestEnv {
    /// Name of the environment variable that enables the integration tests.
    const ENABLE_VAR: &'static str = "SNAPLACE_TEST_FCCTRD";
    /// Environment variable that overrides the API socket path.
    const ADDRESS_VAR: &'static str = "SNAPLACE_TEST_FCCTRD_ADDRESS";
    /// Environment variable that overrides the TTRPC socket path.
    const TTRPC_VAR: &'static str = "SNAPLACE_TEST_FCCTRD_TTRPC";
    /// Environment variable that overrides the containerd namespace.
    const NAMESPACE_VAR: &'static str = "SNAPLACE_TEST_FCCTRD_NAMESPACE";
    /// Environment variable that overrides the snapshotter name.
    const SNAPSHOTTER_VAR: &'static str = "SNAPLACE_TEST_FCCTRD_SNAPSHOTTER";
    /// Environment variable that overrides the Firecracker-containerd helper.
    const CTR_VAR: &'static str = "SNAPLACE_TEST_FCCTRD_CTR";
    /// Environment variable that overrides the test Function image.
    const IMAGE_VAR: &'static str = "SNAPLACE_TEST_FCCTRD_IMAGE";
    /// Environment variable that overrides the TAP subnet.
    const SUBNET_VAR: &'static str = "SNAPLACE_TEST_FCCTRD_SUBNET";

    /// Return `true` when the integration tests are explicitly enabled.
    #[instrument(level = Level::TRACE)]
    fn enabled() -> bool {
        matches!(env::var(Self::ENABLE_VAR).as_deref(), Ok("1"))
    }

    /// Reject non-root execution when the integration tests are enabled.
    #[instrument(level = Level::TRACE)]
    fn ensure_root() -> Result<(), BoxError> {
        if ::rustix::process::geteuid() == ::rustix::fs::Uid::ROOT {
            return Ok(());
        }

        Err(Box::new(::std::io::Error::new(
            ::std::io::ErrorKind::PermissionDenied,
            "SNAPLACE_TEST_FCCTRD requires root",
        )))
    }

    /// Read the integration-test fixture from the environment.
    fn read() -> Result<Option<Self>, BoxError> {
        if !Self::enabled() {
            return Ok(None);
        }

        Self::ensure_root()?;

        Ok(Some(Self {
            address: PathBuf::from(
                env::var(Self::ADDRESS_VAR)
                    .unwrap_or_else(|_| FirecrackerContainerdConfig::DEFAULT_ADDRESS.into()),
            ),
            ttrpc_address: PathBuf::from(
                env::var(Self::TTRPC_VAR)
                    .unwrap_or_else(|_| FirecrackerContainerdConfig::DEFAULT_TTRPC_ADDRESS.into()),
            ),
            namespace: env::var(Self::NAMESPACE_VAR)
                .unwrap_or_else(|_| FirecrackerContainerdConfig::DEFAULT_NAMESPACE.into()),
            snapshotter: env::var(Self::SNAPSHOTTER_VAR)
                .unwrap_or_else(|_| FirecrackerContainerdConfig::DEFAULT_SNAPSHOTTER.into())
                .to_compact_string(),
            ctr: env::var(Self::CTR_VAR)
                .unwrap_or_else(|_| FirecrackerContainerdConfig::DEFAULT_CTR.into())
                .into(),
            image: env::var(Self::IMAGE_VAR)
                .unwrap_or_else(|_| "docker.io/library/nginx:1.25.0".into()),
            subnet: env::var(Self::SUBNET_VAR)
                .unwrap_or_else(|_| "10.242.242.0/24".into())
                .parse()
                .map_err(|err| {
                    Box::new(::std::io::Error::new(
                        ::std::io::ErrorKind::InvalidInput,
                        format!("invalid subnet: {err}"),
                    )) as BoxError
                })?,
        }))
    }

    /// Build a runtime configuration for the configured test fixture.
    #[instrument(level = Level::TRACE)]
    fn runtime_config(&self) -> FirecrackerContainerdConfig {
        FirecrackerContainerdConfig {
            address: self.address.clone(),
            ttrpc_address: self.ttrpc_address.clone(),
            namespace: self.namespace.clone(),
            snapshotter: self.snapshotter.clone(),
            //nameservers: vec![], // TODO: maybe make them configurable too
            ctr: self.ctr.clone(),
            ..Default::default()
        }
    }

    /// Build Function metadata for the configured test image.
    #[instrument(level = Level::TRACE)]
    fn function_info(&self, id: FunctionId) -> FcctrdFunctionInfo {
        FcctrdFunctionInfo {
            id,
            image_ref: self.image.clone(),
            memory: 512.mebibytes(),
            parent_snapshot: None,
            process_args: None,
        }
    }

    /// Build a TAP provider for the configured subnet.
    #[instrument(level = Level::TRACE)]
    fn net_provider(&self) -> Result<PlainTapDevices, BoxError> {
        PlainTapDevices::new(&PlainTapsConfig {
            subnet: self.subnet,
        })
        .map_err(|err| Box::new(err) as _)
    }
}

/// Create a unique Function identifier for a given test case.
#[instrument(level = Level::TRACE)]
fn test_function_id(test_name: &str) -> FunctionId {
    FunctionId::from(format!(
        "fcctrd.{test_name}.{}.{}",
        ::std::process::id(),
        Uuid::new_v4()
    ))
}

/// Build a runtime harness guard and matching Function identifier for a test
/// case.
#[instrument(level = Level::TRACE)]
fn build_harness(
    env: &FcctrdTestEnv,
    test_name: &str,
) -> Result<(RuntimeHarnessGuard<FirecrackerContainerd>, FunctionId), BoxError> {
    let fid = test_function_id(test_name);
    let harness = RuntimeHarnessGuard::new(RuntimeHarness::new(
        env.runtime_config(),
        env.function_info(fid.clone()),
        env.net_provider()?,
    )?);
    Ok((harness, fid))
}

/// Build a runtime harness guard and matching Function identifier for a test
/// case, using the provided [`TempDir`] as scratch directory.
#[instrument(level = Level::TRACE)]
fn build_harness_with_tempdir(
    env: &FcctrdTestEnv,
    test_name: &str,
    tempdir: TempDir,
) -> Result<(RuntimeHarnessGuard<FirecrackerContainerd>, FunctionId), BoxError> {
    let fid = test_function_id(test_name);
    let harness = RuntimeHarnessGuard::new(RuntimeHarness::with_tempdir(
        env.runtime_config(),
        env.function_info(fid.clone()),
        env.net_provider()?,
        tempdir,
    )?);
    Ok((harness, fid))
}

/// Record a cleanup failure while preserving the first error to return.
fn note_cleanup_error(first_err: &mut Option<BoxError>, err: BoxError, step: &'static str) {
    error!(%step, error = ?err, "fcctrd test cleanup step failed");
    if first_err.is_none() {
        *first_err = Some(err);
    }
}

/// Run all cleanup steps and prefer the original test failure over cleanup errors.
#[instrument(level = Level::DEBUG, err(Debug))]
fn finish_test(
    result: Result<(), BoxError>,
    cleanup: Result<(), BoxError>,
) -> Result<(), BoxError> {
    match (result, cleanup) {
        (Err(err), _) => Err(err),
        (Ok(()), Err(err)) => Err(err),
        (Ok(()), Ok(())) => Ok(()),
    }
}

/// Run a single env-gated `fcctrd` test case under the shared lock.
async fn run_fcctrd_test<F, Fut>(test_name: &'static str, test_fn: F) -> Result<(), BoxError>
where
    F: FnOnce(FcctrdTestEnv, &'static str) -> Fut,
    Fut: Future<Output = Result<(), BoxError>>,
{
    let Some(env) = FcctrdTestEnv::read()? else {
        return Ok(());
    };

    let _guard = TEST_LOCK.lock().await;
    test_fn(env, test_name).await
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// Test drivers
//
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Verify registration finalizes missing `fcctrd` metadata.
#[::tokio::test(name = "rt-harn-fcctrd")]
#[::tracing_test::traced_test]
#[instrument(level = Level::DEBUG)]
async fn register_function_finalizes_missing_metadata() -> Result<(), BoxError> {
    run_fcctrd_test(
        "register_function_finalizes_missing_metadata",
        |env, test_name| async move {
            let (mut guard, _) = build_harness(&env, test_name)?;

            let result = async {
                guard
                    .register_function()
                    .await
                    .inspect_err(|error| error!(?error))?;

                assert!(guard.harness().function_info().parent_snapshot.is_some());
                assert!(guard.harness().function_info().process_args.is_some());

                Ok(())
            }
            .await;

            let cleanup = guard.cleanup().await;
            finish_test(result, cleanup)
        },
    )
    .await
}

/// Test fresh [`Sandbox`] lifecycle operations and snapshot creation.
#[::tokio::test(name = "rt-harn-fcctrd")]
#[::tracing_test::traced_test]
#[instrument(level = Level::DEBUG)]
async fn create_pause_resume_snapshot_shutdown_destroy() -> Result<(), BoxError> {
    run_fcctrd_test(
        "create_pause_resume_snapshot_shutdown_destroy",
        |env, test_name| async move {
            let (mut guard, fid) = build_harness(&env, test_name)?;
            let mut rt = None;
            let mut sandbox = None;

            let result = async {
                guard.register_function().await?;

                // Initialize runtime & create sandbox
                //
                rt = Some(guard.harness().init_runtime(None).await?);
                let net = guard.harness().allocate_net_resource().await?;
                let mut timings = guard.harness().fresh_timings();
                sandbox = Some(
                    rt.as_mut()
                        .expect("runtime should be initialized")
                        .create_sandbox(&fid, net, &mut timings)
                        .await
                        .inspect_err(|error| error!(?error))?,
                );
                trace!(?timings);

                // Pause & resume sandbox
                //
                rt.as_mut()
                    .expect("runtime should be initialized")
                    .pause_sandbox(sandbox.as_mut().expect("sandbox should exist"))
                    .await
                    .inspect_err(|error| error!(?error))?;
                rt.as_mut()
                    .expect("runtime should be initialized")
                    .resume_sandbox(sandbox.as_mut().expect("sandbox should exist"))
                    .await
                    .inspect_err(|error| error!(?error))?;
                rt.as_mut()
                    .expect("runtime should be initialized")
                    .pause_sandbox(sandbox.as_mut().expect("sandbox should exist"))
                    .await
                    .inspect_err(|error| error!(?error))?;

                // Sandbox snapshotting
                //
                let (state_path, memory_path) = guard.harness().snapshot_paths("lifecycle")?;
                rt.as_mut()
                    .expect("runtime should be initialized")
                    .create_snapshot(
                        sandbox.as_mut().expect("sandbox should exist"),
                        &state_path,
                        &memory_path,
                    )
                    .await
                    .inspect_err(|error| error!(?error))?;
                assert!(sandbox
                    .as_ref()
                    .expect("sandbox should exist")
                    .has_snapshot());
                assert!(sandbox
                    .as_ref()
                    .expect("sandbox should exist")
                    .snapshot_state()
                    .is_some());
                assert!(state_path.exists());
                assert!(memory_path.exists());

                // Shutdown sandbox
                //
                guard
                    .harness()
                    .shutdown_and_destroy_sandbox(
                        rt.as_mut().expect("runtime should be initialized"),
                        sandbox.take().expect("sandbox should exist"),
                    )
                    .await
                    .inspect_err(|error| error!(?error))?;
                rt = None;

                Ok(())
            }
            .await;

            let cleanup = async {
                let mut first_err = None;

                if let Some(sandbox) = sandbox.take() {
                    match rt.as_mut() {
                        Some(rt) => {
                            if let Err(err) = guard
                                .harness()
                                .shutdown_and_destroy_sandbox(rt, sandbox)
                                .await
                            {
                                note_cleanup_error(
                                    &mut first_err,
                                    err,
                                    "failed to clean up live sandbox during test cleanup",
                                );
                            }
                        }
                        None => note_cleanup_error(
                            &mut first_err,
                            Box::new(::std::io::Error::other(
                                "sandbox remained without a runtime for cleanup",
                            )),
                            "failed to clean up live sandbox during test cleanup",
                        ),
                    }
                }

                if let Err(err) = guard.cleanup().await {
                    note_cleanup_error(
                        &mut first_err,
                        err,
                        "failed to clean up runtime harness during test cleanup",
                    );
                }

                first_err.map_or(Ok(()), Err)
            }
            .await;

            finish_test(result, cleanup)
        },
    )
    .await
}

/// Test snapshot reinstatement across a simulated reboot boundary.
#[::tokio::test(name = "rt-harn-fcctrd")]
#[::tracing_test::traced_test]
#[instrument(level = Level::DEBUG)]
async fn reinstate_snapshot_after_simulated_reboot() -> Result<(), BoxError> {
    run_fcctrd_test(
        "reinstate_snapshot_after_simulated_reboot",
        |env, test_name| async move {
            let (harness_orig, fid) = build_harness(&env, test_name)?;
            let mut harness_orig = Some(harness_orig);
            let mut harness_rest = None;
            let mut rt_a = None;
            let mut uvm_original = None;
            let mut uvm_restored = None;
            let mut source_cleanup_err = None;

            let result = async {
                // Phase A: create a fresh uVM, snapshot it, and unload it, so the captured
                //          `MicroVmState` represents a snapshot-only Sandbox.
                harness_orig
                    .as_mut()
                    .expect("(original) harness should exist")
                    .register_function()
                    .await?;

                rt_a = Some(
                    harness_orig
                        .as_ref()
                        .expect("(original) harness should exist")
                        .harness()
                        .init_runtime(None)
                        .await?,
                );
                let net = harness_orig
                    .as_ref()
                    .expect("(original) harness should exist")
                    .harness()
                    .allocate_net_resource()
                    .await?;
                let mut timings = harness_orig
                    .as_ref()
                    .expect("(original) harness should exist")
                    .harness()
                    .fresh_timings();
                uvm_original = Some(
                    rt_a.as_mut()
                        .expect("runtime should be initialized")
                        .create_sandbox(&fid, net, &mut timings)
                        .await
                        .inspect_err(|error| error!(?error))?,
                );
                trace!(?timings);
                // TODO(ckatsak): Ideally, we could be issuing a request here, but this would
                // probably require changes to the harness as well.

                rt_a.as_mut()
                    .expect("runtime should be initialized")
                    .pause_sandbox(uvm_original.as_mut().expect("sandbox should exist"))
                    .await
                    .inspect_err(|error| error!(?error))?;
                let (state_path, memory_path) = harness_orig
                    .as_ref()
                    .expect("(original) harness should exist")
                    .harness()
                    .snapshot_paths("reinstate")?;
                rt_a.as_mut()
                    .expect("runtime should be initialized")
                    .create_snapshot(
                        uvm_original.as_mut().expect("sandbox should exist"),
                        &state_path,
                        &memory_path,
                    )
                    .await
                    .inspect_err(|error| error!(?error))?;
                assert!(state_path.exists());
                assert!(memory_path.exists());

                rt_a.as_mut()
                    .expect("runtime should be initialized")
                    .shutdown_sandbox(uvm_original.as_mut().expect("sandbox should exist"))
                    .await
                    .inspect_err(|error| error!(?error))?;
                let state = uvm_original
                    .as_ref()
                    .expect("sandbox should exist")
                    .snapshot_state()
                    .expect("snapshot state")
                    .expect("snapshot state ok");

                // Simulate the process exit: the restoration harness must
                // not depend on the original harness still being alive.
                rt_a = None;
                // Carefully clean up the original harness, retain its owned
                // `TempDir` (where snapshot files) are placed for reuse.
                let mut fi_orig = harness_orig
                    .as_ref()
                    .expect("(original) harness should exist")
                    .harness()
                    .function_info()
                    .clone();
                let retained = harness_orig
                    .take()
                    .expect("(original) harness should exist")
                    .cleanup_retaining_storage()
                    .await
                    .inspect_err(|err| error!(
                        error = ?err,
                        "Failed to clean up (original) harness before restore",
                    ))?;
                let (tempdir, cleanup) = retained.into_parts();
                if let Err(err) = cleanup {
                    let tempdir_path = tempdir.keep();
                    error!(
                        path = %tempdir_path.display(),
                        error = ?err,
                        "Failed to clean up (original) harness before restore",
                    );
                    return Err(err);
                }

                // TODO(ckatsak): Is there still a failure window here and before `harness_rest` is
                // established, where `uvm_original` may exist w/o any harness able to destroy it?

                // Phase B: rebuild a fresh runtime harness, adopt the persisted
                //          TAP, and then load the snapshot into a running uVM.
                let (hb, _) = build_harness_with_tempdir(&env, test_name, tempdir)?;
                harness_rest = Some(hb);
                harness_rest
                    .as_mut()
                    .expect("(restoration) harness should exist")
                    .harness_mut()
                    .register_other_function(&mut fi_orig)
                    .await?;
                let mut rt_b = harness_rest
                    .as_ref()
                    .expect("(restoration) harness should exist")
                    .harness()
                    .init_runtime(None)
                    .await?;
                uvm_restored = Some(
                    harness_rest
                        .as_ref()
                        .expect("(restoration) harness should exist")
                        .harness()
                        .reinstate_sandbox(&mut rt_b, fid.clone(), state)
                        .await?
                        .expect("reinstated sandbox"),
                );
                rt_b.load_sandbox(uvm_restored.as_mut().expect("reinstated sandbox should exist"))
                    .await
                    .inspect_err(|error| error!(?error))?;
                // TODO(ckatsak): Ideally, we could be issuing a request here, but this would
                // probably require changes to the harness as well.
                rt_b.pause_sandbox(uvm_restored.as_mut().expect("restored sandbox should exist"))
                    .await
                    .inspect_err(|error| error!(?error))?;

                // Destroy the restored uVM first, while its snapshot files are still present.
                harness_rest
                    .as_ref()
                    .expect("(restoration) harness should exist")
                    .harness()
                    .destroy_sandbox_with_new_runtime(
                        uvm_restored.take().expect("restored sandbox should exist")
                    )
                    .await
                    .inspect_err(|error| error!(?error))?;

                // Now, that reinstatement is completed _and_ the uVM has been successfully
                // destroyed (through `uvm_restored`, which is essentially the post-reinstatement
                // handle of _the same_ resources), we may safely drop `uvm_original`, so that we
                // do not attempt to cleanup the same resources twice (which will fail the test).
                uvm_original = None;

                Ok(())
            }
            .await;

            let cleanup = async {
                let mut first_err = source_cleanup_err.take();

                // Cleanup prefers the restoration harness when possible, because by this point
                // the original harness may already have been intentionally torn down.
                if let Some(uvm_restored) = uvm_restored.take() {
                    if let Some(harness_rest) = harness_rest.as_ref() {
                        if let Err(err) = harness_rest
                            .harness()
                            .destroy_sandbox_with_new_runtime(uvm_restored)
                            .await
                        {
                            note_cleanup_error(
                                &mut first_err,
                                err,
                                "failed to clean up restored sandbox during cleanup",
                            );
                        }
                    } else {
                        note_cleanup_error(
                            &mut first_err,
                            Box::new(::std::io::Error::other(
                                "restored sandbox remained without a harness for cleanup",
                            )),
                            "failed to clean up restored sandbox during cleanup",
                        );
                    }
                }

                if let Some(uvm_original) = uvm_original.take() {
                    if let Some(harness_rest) = harness_rest.as_ref() {
                        if let Err(err) = harness_rest
                            .harness()
                            .destroy_sandbox_with_new_runtime(uvm_original)
                            .await
                        {
                            note_cleanup_error(
                                &mut first_err,
                                err,
                                "failed to clean up original snapshot-only sandbox during cleanup",
                            );
                        }
                    } else if let Some(harness_orig) = harness_orig.as_ref() {
                        if let Err(err) = harness_orig
                            .harness()
                            .destroy_sandbox_with_new_runtime(uvm_original)
                            .await
                        {
                            note_cleanup_error(
                                &mut first_err,
                                err,
                                "failed to clean up original snapshot-only sandbox during cleanup",
                            );
                        }
                    } else {
                        note_cleanup_error(
                            &mut first_err,
                            Box::new(::std::io::Error::other(
                                "original snapshot-only sandbox remained without a harness for cleanup",
                            )),
                            "failed to clean up original snapshot-only sandbox during cleanup",
                        );
                    }
                }

                if let Some(harness_rest) = harness_rest.take() {
                    match harness_rest.cleanup_retaining_storage().await {
                        Ok(retained) => {
                            let (tempdir, cleanup) = retained.into_parts();
                            let cleanup_failed = cleanup.is_err();
                            if let Err(err) = cleanup {
                                note_cleanup_error(
                                    &mut first_err,
                                    err,
                                    "failed to clean up (restoration) harness during test cleanup",
                                );
                            }

                            if result.is_err() || cleanup_failed {
                                let tempdir_path = tempdir.keep();
                                error!(
                                    path = %tempdir_path.display(),
                                    "retaining (restoration) harness storage after failed test or cleanup",
                                );
                            }
                        }
                        Err(err) => {
                            note_cleanup_error(
                                &mut first_err,
                                err,
                                "failed to clean up (restoration) harness during test cleanup",
                            );
                        }
                    }
                }

                if let Some(harness_orig) = harness_orig.take()
                    && let Err(err) = harness_orig.cleanup().await
                {
                    note_cleanup_error(
                        &mut first_err,
                        err,
                        "failed to clean up (original) harness during test cleanup",
                    );
                }

                first_err.map_or(Ok(()), Err)
            }
            .await;

            finish_test(result, cleanup)
        },
    )
    .await
}
