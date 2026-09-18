//! Live integration tests for the native Firecracker runtime.
//!
//! These tests are compiled with `test-utils`, but they are inert by default.
//! Set `SNAPLACE_TEST_FC=1` to run them against a real Firecracker binary,
//! kernel image, rootfs image, and host networking setup. They require root
//! because the runtime creates TAP devices and starts real microVM processes.
//!
//! Example run:
//! ```console
//! SNAPLACE_TEST_FC=1 \
//!     SNAPLACE_TEST_FC_KERNEL_IMG='/path/to/vmlinux' \
//!     SNAPLACE_TEST_FC_ROOTFS='/path/to/rootfs.ext4' \
//!     SNAPLACE_TEST_FC_FIRECRACKER_BIN='/path/to/firecracker' \
//!     cargo test -p snaplace --features test-utils,fmd-store-dash,uncache --lib fc -- --nocapture
//! ```
//! or equivalently:
//! ```console
//! SNAPLACE_TEST_FC=1 \
//!     SNAPLACE_TEST_FC_KERNEL_IMG='/path/to/vmlinux' \
//!     SNAPLACE_TEST_FC_ROOTFS='/path/to/rootfs.ext4' \
//!     SNAPLACE_TEST_FC_FIRECRACKER_BIN='/path/to/firecracker' \
//!     cargo nextest run -p snaplace --features test-utils,fmd-store-dash,uncache --lib fc --no-capture
//! ```

use std::{env, future::Future, io::ErrorKind, path::PathBuf, sync::LazyLock};

use anyhow::{anyhow, Context, Error, Result};
use camino::Utf8PathBuf;
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
    worker::{
        runtime::fc::{runtime::Firecracker, FcFunctionInfo, FirecrackerConfig},
        Runtime, Sandbox,
    },
    FunctionId,
};

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

// Runtime registration derives the rootfs basename from the FunctionId prefix
// and then looks for `${prefix}.ext4`. Keep the fixture prefix stable so these
// tests do not depend on the caller's rootfs filename.
const ROOTFS_PREFIX: &str = "rootfs";
const ROOTFS_BASENAME: &str = "rootfs.ext4";

#[derive(Debug, Clone)]
struct FcTestEnv {
    firecracker_bin: PathBuf,
    kernel_img: PathBuf,
    rootfs: PathBuf,
    subnet: Ipv4Net,
}

impl FcTestEnv {
    const ENABLE_VAR: &'static str = "SNAPLACE_TEST_FC";
    const FIRECRACKER_BIN_VAR: &'static str = "SNAPLACE_TEST_FC_FIRECRACKER_BIN";
    const KERNEL_IMG_VAR: &'static str = "SNAPLACE_TEST_FC_KERNEL_IMG";
    const ROOTFS_VAR: &'static str = "SNAPLACE_TEST_FC_ROOTFS";
    const SUBNET_VAR: &'static str = "SNAPLACE_TEST_FC_SUBNET";

    fn enabled() -> bool {
        matches!(env::var(Self::ENABLE_VAR).as_deref(), Ok("1"))
    }

    fn ensure_root() -> Result<()> {
        if ::rustix::process::geteuid() == ::rustix::fs::Uid::ROOT {
            return Ok(());
        }

        Err(anyhow!("SNAPLACE_TEST_FC requires root"))
    }

    fn read_path(var: &'static str) -> Result<PathBuf> {
        let value = env::var(var)
            .map_err(|err| anyhow!("missing required environment variable {var}: {err}"))?;
        Ok(PathBuf::from(value))
    }

    fn read_existing_path(var: &'static str) -> Result<PathBuf> {
        let path = Self::read_path(var)?;
        ::std::fs::canonicalize(&path).with_context(|| {
            format!("path from {var} does not exist or cannot be resolved: {path:?}")
        })
    }

    fn read() -> Result<Option<Self>> {
        if !Self::enabled() {
            return Ok(None);
        }

        Self::ensure_root()?;

        let firecracker_bin = env::var(Self::FIRECRACKER_BIN_VAR)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("firecracker"));
        let kernel_img = Self::read_existing_path(Self::KERNEL_IMG_VAR)?;
        let rootfs = Self::read_existing_path(Self::ROOTFS_VAR)?;
        let subnet = env::var(Self::SUBNET_VAR)
            .unwrap_or_else(|_| "10.242.242.0/24".into())
            .parse()
            .map_err(|err| anyhow!("invalid subnet: {err}"))?;

        Ok(Some(Self {
            firecracker_bin,
            kernel_img,
            rootfs,
            subnet,
        }))
    }

    fn prepare_rootfs_dir(&self, tempdir: &TempDir) -> Result<PathBuf> {
        let rootfs_dir = tempdir.path().join("rootfs");
        ::std::fs::create_dir_all(&rootfs_dir)?;

        // Present the caller-provided image under the runtime's expected name,
        // without copying a potentially large rootfs into every test tempdir.
        let alias = rootfs_dir.join(ROOTFS_BASENAME);
        match ::std::os::unix::fs::symlink(&self.rootfs, &alias) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err.into()),
        }

        Ok(rootfs_dir)
    }

    fn runtime_config(&self, tempdir: &TempDir) -> Result<FirecrackerConfig> {
        let uvms_root_path = Utf8PathBuf::from_path_buf(tempdir.path().join("uvms"))
            .map_err(|path| anyhow!("temporary uVM root path is not UTF-8: {path:?}"))?;
        let uvms_rootfs_path = self.prepare_rootfs_dir(tempdir)?;

        Ok(FirecrackerConfig {
            uvms_root_path,
            uvms_rootfs_path,
            firecracker_bin: self.firecracker_bin.clone(),
            kernel_img: self.kernel_img.clone(),
        })
    }

    fn function_id(&self, test_name: &str) -> FunctionId {
        FunctionId::from(format!(
            "{ROOTFS_PREFIX}-{test_name}-{}-{}",
            ::std::process::id(),
            Uuid::new_v4()
        ))
    }

    fn function_info(&self, id: FunctionId) -> FcFunctionInfo {
        FcFunctionInfo {
            id,
            memory: 512.mebibytes(),
            vcpu_count: 1,
            entrypoint: "/bin/sh".into(),
        }
    }

    fn net_provider(&self) -> Result<PlainTapDevices> {
        PlainTapDevices::new(&PlainTapsConfig {
            subnet: self.subnet,
        })
        .map_err(into_anyhow)
    }
}

fn build_harness(
    env: &FcTestEnv,
    test_name: &str,
) -> Result<(RuntimeHarnessGuard<Firecracker>, FunctionId)> {
    let tempdir = TempDir::new().context("failed to create temporary directory")?;
    build_harness_with_tempdir(env, test_name, tempdir)
}

fn build_harness_with_tempdir(
    env: &FcTestEnv,
    test_name: &str,
    tempdir: TempDir,
) -> Result<(RuntimeHarnessGuard<Firecracker>, FunctionId)> {
    let fid = env.function_id(test_name);
    let function_info = env.function_info(fid.clone());
    let guard = build_harness_with_tempdir_and_function_info(env, function_info, tempdir)?;
    Ok((guard, fid))
}

fn build_harness_with_tempdir_and_function_info(
    env: &FcTestEnv,
    function_info: FcFunctionInfo,
    tempdir: TempDir,
) -> Result<RuntimeHarnessGuard<Firecracker>> {
    let config = env.runtime_config(&tempdir)?;
    let harness = RuntimeHarness::with_tempdir(config, function_info, env.net_provider()?, tempdir)
        .map_err(into_anyhow)?;
    Ok(RuntimeHarnessGuard::new(harness))
}

// RuntimeHarness still exposes boxed errors because it is shared by runtimes
// whose public trait methods return boxed errors. These tests use anyhow for
// local context, so convert at the boundary.
fn into_anyhow(err: impl ::std::fmt::Display) -> Error {
    anyhow!("{err}")
}

fn note_cleanup_error(first_err: &mut Option<Error>, err: Error, step: &'static str) {
    error!(%step, error = ?err, "fc runtime test cleanup step failed");
    if first_err.is_none() {
        *first_err = Some(err);
    }
}

fn finish_test(result: Result<()>, cleanup: Result<()>) -> Result<()> {
    // Preserve the primary test failure if one occurred; cleanup errors matter
    // only when the test body itself succeeded.
    match (result, cleanup) {
        (Err(err), _) => Err(err),
        (Ok(()), Err(err)) => Err(err),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn run_fc_test<F, Fut>(test_name: &'static str, test_fn: F) -> Result<()>
where
    F: FnOnce(FcTestEnv, &'static str) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let Some(env) = FcTestEnv::read()? else {
        return Ok(());
    };

    // The live tests share host TAP names, global Firecracker runtime state,
    // and rootfs registration state; run them serially within this test binary.
    let _guard = TEST_LOCK.lock().await;
    test_fn(env, test_name).await
}

#[::tokio::test(name = "rt-harn-fc")]
#[::tracing_test::traced_test]
#[instrument(level = Level::DEBUG)]
async fn register_function_resolves_rootfs() -> Result<()> {
    // Minimal smoke test for the rootfs registration contract: a FunctionId
    // with the fixed `rootfs-...` prefix must resolve to the tempdir alias.
    run_fc_test(
        "register_function_resolves_rootfs",
        |env, test_name| async move {
            let (mut guard, _fid) = build_harness(&env, test_name)?;

            let result: Result<()> = async {
                guard.register_function().await.map_err(into_anyhow)?;
                Ok(())
            }
            .await;

            let cleanup: Result<()> = async {
                guard.cleanup().await.map_err(into_anyhow)?;
                Ok(())
            }
            .await;
            finish_test(result, cleanup)
        },
    )
    .await
}

#[::tokio::test(name = "rt-harn-fc")]
#[::tracing_test::traced_test]
#[instrument(level = Level::DEBUG)]
async fn snapshot_state_requires_snapshot() -> Result<()> {
    run_fc_test(
        "snapshot_state_requires_snapshot",
        |env, test_name| async move {
            let (mut guard, fid) = build_harness(&env, test_name)?;
            let mut rt = None;
            let mut uvm = None;

            // Create a live sandbox without snapshotting it; the runtime should
            // report a snapshot-state error rather than fabricate state.
            let result: Result<()> = async {
                guard.register_function().await.map_err(into_anyhow)?;

                rt = Some(
                    guard
                        .harness()
                        .init_runtime(None)
                        .await
                        .map_err(into_anyhow)?,
                );
                let tap = guard
                    .harness()
                    .allocate_net_resource()
                    .await
                    .map_err(into_anyhow)?;
                let mut timings = guard.harness().fresh_timings();
                uvm = Some(
                    rt.as_mut()
                        .expect("runtime should be initialized")
                        .create_sandbox(&fid, tap, &mut timings)
                        .await
                        .map_err(into_anyhow)?,
                );

                match uvm.as_ref().expect("sandbox should exist").snapshot_state() {
                    Some(Err(_)) => {}
                    other => {
                        return Err(anyhow!(
                            "unexpected snapshot_state result on a fresh sandbox: {other:?}"
                        ));
                    }
                }

                guard
                    .harness()
                    .shutdown_and_destroy_sandbox(
                        rt.as_mut().expect("runtime should be initialized"),
                        uvm.take().expect("sandbox should exist"),
                    )
                    .await
                    .map_err(into_anyhow)?;
                rt = None;

                Ok(())
            }
            .await;

            // Cleanup is explicit because failures can happen after the uVM is
            // created but before the normal destroy path takes ownership of it.
            let cleanup: Result<()> = async {
                let mut first_err = None;

                if let Some(uvm) = uvm.take() {
                    match rt.as_mut() {
                        Some(rt) => {
                            if let Err(err) = guard
                                .harness()
                                .shutdown_and_destroy_sandbox(rt, uvm)
                                .await
                                .map_err(into_anyhow)
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
                            anyhow!("sandbox remained without a runtime for cleanup"),
                            "failed to clean up live sandbox during test cleanup",
                        ),
                    }
                }

                if let Err(err) = guard.cleanup().await.map_err(into_anyhow) {
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

#[::tokio::test(name = "rt-harn-fc")]
#[::tracing_test::traced_test]
#[instrument(level = Level::DEBUG)]
async fn create_pause_resume_snapshot_shutdown_destroy() -> Result<()> {
    run_fc_test(
        "create_pause_resume_snapshot_shutdown_destroy",
        |env, test_name| async move {
            let (mut guard, fid) = build_harness(&env, test_name)?;
            let mut rt = None;
            let mut uvm = None;

            // Exercise the ordinary lifecycle in one runtime instance:
            // create, pause, resume, snapshot, shut down, and destroy.
            let result: Result<()> = async {
                guard.register_function().await.map_err(into_anyhow)?;

                rt = Some(
                    guard
                        .harness()
                        .init_runtime(None)
                        .await
                        .map_err(into_anyhow)?,
                );
                let tap = guard
                    .harness()
                    .allocate_net_resource()
                    .await
                    .map_err(into_anyhow)?;
                let mut timings = guard.harness().fresh_timings();
                uvm = Some(
                    rt.as_mut()
                        .expect("runtime should be initialized")
                        .create_sandbox(&fid, tap, &mut timings)
                        .await
                        .map_err(into_anyhow)?,
                );
                trace!(?timings);

                rt.as_mut()
                    .expect("runtime should be initialized")
                    .pause_sandbox(uvm.as_mut().expect("sandbox should exist"))
                    .await
                    .map_err(into_anyhow)?;
                rt.as_mut()
                    .expect("runtime should be initialized")
                    .resume_sandbox(uvm.as_mut().expect("sandbox should exist"))
                    .await
                    .map_err(into_anyhow)?;
                rt.as_mut()
                    .expect("runtime should be initialized")
                    .pause_sandbox(uvm.as_mut().expect("sandbox should exist"))
                    .await
                    .map_err(into_anyhow)?;

                let (state_path, memory_path) = guard
                    .harness()
                    .snapshot_paths("lifecycle")
                    .map_err(into_anyhow)?;
                rt.as_mut()
                    .expect("runtime should be initialized")
                    .create_snapshot(
                        uvm.as_mut().expect("sandbox should exist"),
                        &state_path,
                        &memory_path,
                    )
                    .await
                    .map_err(into_anyhow)?;
                assert!(uvm.as_ref().expect("sandbox should exist").has_snapshot());
                assert!(uvm
                    .as_ref()
                    .expect("sandbox should exist")
                    .snapshot_state()
                    .is_some());
                assert!(state_path.exists());
                assert!(memory_path.exists());

                guard
                    .harness()
                    .shutdown_and_destroy_sandbox(
                        rt.as_mut().expect("runtime should be initialized"),
                        uvm.take().expect("sandbox should exist"),
                    )
                    .await
                    .map_err(into_anyhow)?;
                rt = None;

                Ok(())
            }
            .await;
            // Mirror the explicit cleanup pattern from the error-state test so
            // partial lifecycle failures still attempt to tear down Firecracker.
            let cleanup: Result<()> = async {
                let mut first_err = None;

                if let Some(uvm) = uvm.take() {
                    match rt.as_mut() {
                        Some(rt) => {
                            if let Err(err) = guard
                                .harness()
                                .shutdown_and_destroy_sandbox(rt, uvm)
                                .await
                                .map_err(into_anyhow)
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
                            anyhow!("sandbox remained without a runtime for cleanup"),
                            "failed to clean up live sandbox during test cleanup",
                        ),
                    }
                }

                match guard.cleanup_retaining_storage().await.map_err(into_anyhow) {
                    Ok(retained) => {
                        let (tempdir, cleanup) = retained.into_parts();
                        let cleanup_failed = cleanup.is_err();
                        if let Err(err) = cleanup {
                            note_cleanup_error(
                                &mut first_err,
                                into_anyhow(err),
                                "failed to clean up runtime harness during test cleanup",
                            );
                        }

                        if result.is_err() || cleanup_failed {
                            let tempdir_path = tempdir.keep();
                            error!(
                                path = %tempdir_path.display(),
                                "retaining runtime harness storage after failed test or cleanup",
                            );
                        }
                    }
                    Err(err) => note_cleanup_error(
                        &mut first_err,
                        err,
                        "failed to clean up runtime harness during test cleanup",
                    ),
                }

                first_err.map_or(Ok(()), Err)
            }
            .await;

            finish_test(result, cleanup)
        },
    )
    .await
}

#[::tokio::test(name = "rt-harn-fc")]
#[::tracing_test::traced_test]
#[instrument(level = Level::DEBUG)]
async fn reinstate_snapshot_after_simulated_reboot() -> Result<()> {
    run_fc_test(
        "reinstate_snapshot_after_simulated_reboot",
        |env, test_name| async move {
            let (guard_orig, fid) = build_harness(&env, test_name)?;
            let mut guard_orig = Some(guard_orig);
            let mut guard_rest = None;
            let mut rt_a = None;
            let mut uvm_original = None;
            let mut uvm_restored = None;

            // Phase A creates a real uVM, snapshots it, shuts down only the VMM
            // process, and retains the tempdir to simulate host-side state that
            // survives a worker/runtime restart.
            let result: Result<()> = async {
                guard_orig
                    .as_mut()
                    .expect("(original) harness should exist")
                    .register_function()
                    .await
                    .map_err(into_anyhow)?;

                rt_a = Some(
                    guard_orig
                        .as_ref()
                        .expect("(original) harness should exist")
                        .harness()
                        .init_runtime(None)
                        .await
                        .map_err(into_anyhow)?,
                );
                let tap = guard_orig
                    .as_ref()
                    .expect("(original) harness should exist")
                    .harness()
                    .allocate_net_resource()
                    .await
                    .map_err(into_anyhow)?;
                let mut timings = guard_orig
                    .as_ref()
                    .expect("(original) harness should exist")
                    .harness()
                    .fresh_timings();
                uvm_original = Some(
                    rt_a.as_mut()
                        .expect("runtime should be initialized")
                        .create_sandbox(&fid, tap, &mut timings)
                        .await
                        .map_err(into_anyhow)?,
                );
                trace!(?timings);

                rt_a.as_mut()
                    .expect("runtime should be initialized")
                    .pause_sandbox(uvm_original.as_mut().expect("sandbox should exist"))
                    .await
                    .map_err(into_anyhow)?;
                let (state_path, memory_path) = guard_orig
                    .as_ref()
                    .expect("(original) harness should exist")
                    .harness()
                    .snapshot_paths("reinstate")
                    .map_err(into_anyhow)?;
                rt_a.as_mut()
                    .expect("runtime should be initialized")
                    .create_snapshot(
                        uvm_original.as_mut().expect("sandbox should exist"),
                        &state_path,
                        &memory_path,
                    )
                    .await
                    .map_err(into_anyhow)?;
                assert!(state_path.exists());
                assert!(memory_path.exists());

                rt_a.as_mut()
                    .expect("runtime should be initialized")
                    .shutdown_sandbox(uvm_original.as_mut().expect("sandbox should exist"))
                    .await
                    .map_err(into_anyhow)?;
                let state = uvm_original
                    .as_ref()
                    .expect("sandbox should exist")
                    .snapshot_state()
                    .expect("snapshot state")
                    .expect("snapshot state ok");

                rt_a = None;

                // Build the second harness around the original FunctionInfo so
                // registration, reinstate, and later cleanup all refer to the
                // same FunctionId and rootfs registry entry.
                let fi_orig = guard_orig
                    .as_ref()
                    .expect("(original) harness should exist")
                    .harness()
                    .function_info()
                    .clone();
                let retained = guard_orig
                    .take()
                    .expect("(original) harness should exist")
                    .cleanup_retaining_storage()
                    .await
                    .map_err(into_anyhow)
                    .inspect_err(|err| {
                        error!(
                            error = ?err,
                            "Failed to clean up (original) harness before restore",
                        )
                    })?;
                let (tempdir, cleanup) = retained.into_parts();
                if let Err(err) = cleanup {
                    let tempdir_path = tempdir.keep();
                    error!(
                        path = %tempdir_path.display(),
                        error = ?err,
                        "Failed to clean up (original) harness before restore",
                    );
                    return Err(into_anyhow(err).into());
                }

                let hb = build_harness_with_tempdir_and_function_info(&env, fi_orig, tempdir)?;
                guard_rest = Some(hb);
                guard_rest
                    .as_mut()
                    .expect("(restoration) harness should exist")
                    .register_function()
                    .await
                    .map_err(into_anyhow)?;

                // Phase B reconstructs the runtime around the snapshot-only
                // sandbox state, loads the snapshot into a new Firecracker
                // process, and then destroys it through a fresh runtime context.
                let mut rt_b = guard_rest
                    .as_ref()
                    .expect("(restoration) harness should exist")
                    .harness()
                    .init_runtime(Some(uvm_original.as_ref().expect("sandbox should exist")))
                    .await
                    .map_err(into_anyhow)?;
                uvm_restored = Some(
                    guard_rest
                        .as_ref()
                        .expect("(restoration) harness should exist")
                        .harness()
                        .reinstate_sandbox(&mut rt_b, fid.clone(), state)
                        .await
                        .map_err(into_anyhow)?
                        .expect("reinstated sandbox"),
                );
                rt_b.load_sandbox(
                    uvm_restored
                        .as_mut()
                        .expect("reinstated sandbox should exist"),
                )
                .await
                .map_err(into_anyhow)?;
                rt_b.pause_sandbox(
                    uvm_restored
                        .as_mut()
                        .expect("restored sandbox should exist"),
                )
                .await
                .map_err(into_anyhow)?;

                guard_rest
                    .as_ref()
                    .expect("(restoration) harness should exist")
                    .harness()
                    .destroy_sandbox_with_new_runtime(
                        uvm_restored.take().expect("restored sandbox should exist"),
                    )
                    .await
                    .map_err(into_anyhow)?;

                uvm_original = None;

                Ok(())
            }
            .await;

            // There are two harnesses and potentially two sandbox handles here.
            // Cleanup proceeds from the restored sandbox back to the original
            // retained-storage harness so each object is destroyed by a runtime
            // that still knows its uVM directory and network resource.
            let cleanup: Result<()> = async {
                let mut first_err = None;

                if let Some(uvm_restored) = uvm_restored.take() {
                    if let Some(guard_rest) = guard_rest.as_ref() {
                        if let Err(err) = guard_rest
                            .harness()
                            .destroy_sandbox_with_new_runtime(uvm_restored)
                            .await
                            .map_err(into_anyhow)
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
                            anyhow!("restored sandbox remained without a harness for cleanup"),
                            "failed to clean up restored sandbox during cleanup",
                        );
                    }
                }

                if let Some(guard_rest) = guard_rest.take()
                    && let Err(err) = guard_rest.cleanup().await.map_err(into_anyhow)
                {
                    note_cleanup_error(
                        &mut first_err,
                        err,
                        "failed to clean up restoration harness during cleanup",
                    );
                }

                if let Some(guard_orig) = guard_orig.take() {
                    if let Some(mut rt_a) = rt_a.take() {
                        if let Some(uvm_original) = uvm_original.take()
                            && let Err(err) = guard_orig
                                .harness()
                                .shutdown_and_destroy_sandbox(&mut rt_a, uvm_original)
                                .await
                                .map_err(into_anyhow)
                        {
                            note_cleanup_error(
                                &mut first_err,
                                err,
                                "failed to clean up original sandbox during cleanup",
                            );
                        }

                        if let Err(err) = guard_orig.cleanup().await.map_err(into_anyhow) {
                            note_cleanup_error(
                                &mut first_err,
                                err,
                                "failed to clean up original harness during cleanup",
                            );
                        }
                    } else if uvm_original.take().is_some() {
                        note_cleanup_error(
                            &mut first_err,
                            anyhow!("original sandbox remained without a runtime for cleanup"),
                            "failed to clean up original sandbox during cleanup",
                        );
                    } else if let Err(err) = guard_orig.cleanup().await.map_err(into_anyhow) {
                        note_cleanup_error(
                            &mut first_err,
                            err,
                            "failed to clean up original harness during cleanup",
                        );
                    }
                } else if uvm_original.take().is_some() {
                    note_cleanup_error(
                        &mut first_err,
                        anyhow!("original sandbox remained after storage was retained"),
                        "failed to clean up original sandbox during cleanup",
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
