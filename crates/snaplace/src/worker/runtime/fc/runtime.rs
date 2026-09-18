use std::{
    borrow::Cow,
    fmt::Debug,
    hash::BuildHasher,
    io,
    net::Ipv4Addr,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::{LazyLock, OnceLock},
    time::Duration,
};

use aho_corasick::AhoCorasick;
use async_pidfd::AsyncPidFd;
use camino::Utf8Path;
use compact_str::{CompactString, ToCompactString};
use const_format::formatcp;
use dashmap::DashMap;
use enum_map::EnumMap;
use scopeguard::defer;
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    net::{unix::pipe, UnixStream},
    process::{Child, Command},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{error, instrument, trace, trace_span, warn, Level, Span};
use wick::Api;

use crate::{
    metadata::registration,
    metrics::{Nanoseconds, Timing},
    network::{Tap, TapDevice},
    worker::{
        runtime::{
            fc::{
                error::Error,
                sandbox::{MicroVm, SnapshotFiles},
                FcFunctionInfo, FirecrackerConfig,
            },
            DestroySandboxRuntimeError,
        },
        Runtime, Sandbox,
    },
    FunctionId,
};

/// Base name of the Unix socket serving Firecracker's control API.
const API_SOCK_BASENAME: &str = "api.sock";
/// Base name of Firecracker logger's named pipe.
const LOGGING_FIFO_BASENAME: &str = "log.fifo";
/// Base name of Firecracker metrics' named pipe.
const METRICS_FIFO_BASENAME: &str = "mtr.fifo";
/// Base name of Firecracker's vsock Unix domain socket.
const VSOCK_UDS_BASENAME: &str = "v.sock";
/// Base name of JSON file with uVM configuration.
const UVM_CONFIG_BASENAME: &str = "uvm.json";
/// Rootfs image file extension.
const ROOTFS_IMG_EXT: &str = "ext4";
/// Default log level for Firecracker's logger.
const DEFAULT_VMM_LOG_LEVEL: &str = "Warning";
/// Firecracker uVM configuration template.
const UVM_CONFIG_TEMPLATE: &str = include_str!("uvm_config_tmpl.json.in");

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Preset guest-kernel verbosity levels for `rt-fc` uVMs.
///
/// Each variant selects one of this [`Runtime`]'s predefined kernel boot-parameter
/// profiles.  The profiles mainly differ in whether the 8250 serial console is
/// exposed and whether guest userspace logs are forwarded to it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ::serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KernelVerbosity {
    /// Disable the guest serial console entirely.
    ///
    /// This is the quietest profile: no `ttyS0`, no kernel console on the
    /// Firecracker serial device, and no serial log output from guest
    /// userspace.
    #[default]
    #[serde(alias = "none", alias = "quiet", alias = "off")]
    Silent,
    /// Keep the guest serial console available, but reduce guest userspace
    /// noise on it.
    ///
    /// This enables `ttyS0` as the kernel console, but tells `systemd` not to
    /// forward journald output there and not to spawn a serial getty.
    #[serde(alias = "low", alias = "min")]
    Minimal,
    /// Enable the guest serial console and forward guest logs to it.
    ///
    /// This is the most chatty profile and is useful while debugging guest
    /// boot and early userspace behavior.
    #[serde(alias = "high", alias = "max", alias = "full")]
    Verbose,
}

impl KernelVerbosity {
    /// Returns the predefined guest-kernel boot-parameter string for this
    /// verbosity level.
    ///
    /// This selects one of the `*_KERNEL_BOOT_PARAMS` presets used when
    /// rendering a fresh uVM's Firecracker JSON config.
    ///
    ///  The returned string controls whether the guest serial console is
    /// disabled, kept quiet, or used as a verbose logging path during boot and
    /// early userspace.
    #[inline]
    pub fn boot_params(&self) -> &'static str {
        match self {
            KernelVerbosity::Silent => SILENT_KERNEL_BOOT_PARAMS,
            KernelVerbosity::Minimal => MINIVERB_KERNEL_BOOT_PARAMS,
            KernelVerbosity::Verbose => VERBOSE_KERNEL_BOOT_PARAMS,
        }
    }
}

/// Shared baseline guest kernel command line.
///
/// Includes the common hardware, panic, and init settings used by all guest
/// boot-param variants in this runtime. Variants append serial-console and
/// systemd-specific options on top of this base.
#[cfg(target_arch = "x86_64")]
pub const BASE_KERNEL_BOOT_PARAMS: &str = "i8042.nokbd i8042.noaux i8042.nomux ipv6.disable=1 reboot=k panic=1 swiotlb=noforce nomodule random.trust_cpu=on tsc=reliable ro init=/sbin/overlay-init";
#[cfg(target_arch = "aarch64")]
pub const BASE_KERNEL_BOOT_PARAMS: &str = "ipv6.disable=1 reboot=k panic=1 swiotlb=noforce nomodule random.trust_cpu=on ro init=/sbin/overlay-init";
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
compile_error!("`rt-fc` is only supported for x86_64 and aarch64");

/// Baseline boot params with the 8250 serial console disabled.
///
/// This is the quietest (and presumably the fastest boot-time-wise) variant:
/// no `ttyS0`, no kernel console on the Firecracker serial device, and thus no
/// serial log output.
pub const SILENT_KERNEL_BOOT_PARAMS: &str = formatcp!("{BASE_KERNEL_BOOT_PARAMS} 8250.nr_uarts=0");

/// Similar to [`SILENT_KERNEL_BOOT_PARAMS`], but additionally:
/// - `8250.nr_uarts=1`: allow at most one 8250 UART to register (which makes
///   `ttyS0` available in our single-UART guest setup);
/// - `console=ttyS0`: explicitly make Firecracker's serial device the kernel
///   console (hence `printk` output & `/dev/console` -> `ttyS0`);
/// - `systemd.journald.forward_to_console=yes`: tell `systemd-journald` to
///   forward logs to the system console.
///
/// # Note
///
/// - Make sure guest kernel is built with `CONFIG_SERIAL_8250{,_CONSOLE}=y`.
/// - `systemd.journald.forward_to_console` is ignored by non-systemd guests.
/// - <https://docs.kernel.org/admin-guide/kernel-parameters.html>
/// - <https://www.freedesktop.org/software/systemd/man/latest/systemd-journald.service.html>
#[cfg(target_arch = "x86_64")]
pub const VERBOSE_KERNEL_BOOT_PARAMS: &str = formatcp!("{BASE_KERNEL_BOOT_PARAMS} 8250.nr_uarts=1 console=ttyS0,115200n8 systemd.journald.forward_to_console=yes");
#[cfg(target_arch = "aarch64")]
pub const VERBOSE_KERNEL_BOOT_PARAMS: &str = formatcp!("{BASE_KERNEL_BOOT_PARAMS} keep_bootcon 8250.nr_uarts=1 console=ttyS0,115200n8 systemd.journald.forward_to_console=yes");

/// Similar to [`SILENT_KERNEL_BOOT_PARAMS`], but additionally:
/// - `8250.nr_uarts=1`: allow at most one 8250 UART to register (which makes
///   `ttyS0` available in our single-UART guest setup);
/// - `console=ttyS0`: explicitly make Firecracker's serial device the kernel
///   console (hence `printk` output & `/dev/console` -> `ttyS0`);
/// - `systemd.journald.forward_to_console=no`: tell `systemd-journald` __not__
///   to forward logs to the system console;
/// - `systemd.getty_auto=no`: tell `systemd-getty-generator` not to enable
///   a `serial-getty@.service` for active kernel consoles (hence no `agetty`,
///   no login prompt, etc).
///
/// # Notes
///
/// - Make sure guest kernel is built with `CONFIG_SERIAL_8250{,_CONSOLE}=y`.
/// - `systemd.journald.forward_to_console` and `systemd.getty_auto` are
///   ignored by non-systemd guests.
/// - <https://docs.kernel.org/admin-guide/kernel-parameters.html>
/// - <https://www.freedesktop.org/software/systemd/man/latest/systemd-journald.service.html>
/// - <https://www.freedesktop.org/software/systemd/man/latest/systemd-getty-generator.html>
#[cfg(target_arch = "x86_64")]
pub const MINIVERB_KERNEL_BOOT_PARAMS: &str = formatcp!("{BASE_KERNEL_BOOT_PARAMS} 8250.nr_uarts=1 console=ttyS0,115200n8 systemd.journald.forward_to_console=no systemd.getty_auto=no");
#[cfg(target_arch = "aarch64")]
pub const MINIVERB_KERNEL_BOOT_PARAMS: &str = formatcp!("{BASE_KERNEL_BOOT_PARAMS} keep_bootcon 8250.nr_uarts=1 console=ttyS0,115200n8 systemd.journald.forward_to_console=no systemd.getty_auto=no");
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Populates placeholders in uVMs' configuration JSON file.
static AUTOMATON: OnceLock<AhoCorasick> = OnceLock::new();
static BUILD_HASHER: LazyLock<crate::BuildHasher> = LazyLock::new(Default::default);
static ROOTFS_REGISTRY: OnceLock<DashMap<FunctionId, PathBuf, crate::BuildHasher>> =
    OnceLock::new();

/// Placeholder variables in uVMs' configuration JSON file.
mod var {
    pub const KERNEL_IMG_PATH: &str = "$SNAPLACE_KERNEL_IMG_PATH";
    pub const KERNEL_BOOT_ARGS: &str = "$SNAPLACE_KERNEL_BOOT_ARGS";
    pub const UVM_ROOTFS: &str = "$SNAPLACE_UVM_ROOTFS";
    pub const VCPU_COUNT: &str = "$SNAPLACE_VCPU_COUNT";
    pub const MEM_SIZE_MIB: &str = "$SNAPLACE_MEM_SIZE_MIB";
    pub const HOST_TAP_NAME: &str = "$SNAPLACE_HOST_TAP_NAME";
    pub const VSOCK_GUEST_CID: &str = "$SNAPLACE_VSOCK_GUEST_CID";
    pub const VSOCK_UDS_PATH: &str = "$SNAPLACE_VSOCK_UDS_PATH";
    pub const VMM_LOG_LVL: &str = "$SNAPLACE_VMM_LOG_LVL";
    pub const VMM_LOG_PATH: &str = "$SNAPLACE_VMM_LOG_PATH";
    pub const VMM_METRICS_PATH: &str = "$SNAPLACE_VMM_METRICS_PATH";
}

const UVM_CONFIG_PATTERNS: [&str; 11] = [
    formatcp!("\"{}\"", var::VCPU_COUNT),
    formatcp!("\"{}\"", var::MEM_SIZE_MIB),
    formatcp!("\"{}\"", var::VSOCK_GUEST_CID),
    var::KERNEL_IMG_PATH,
    var::KERNEL_BOOT_ARGS,
    var::UVM_ROOTFS,
    var::HOST_TAP_NAME,
    var::VSOCK_UDS_PATH,
    var::VMM_LOG_LVL,
    var::VMM_LOG_PATH,
    var::VMM_METRICS_PATH,
];

static MKDIR_UVMS_ROOT_DIR: ::tokio::sync::OnceCell<()> = ::tokio::sync::OnceCell::const_new();

fn lookup_rootfs_path(function_id: &FunctionId) -> Result<PathBuf, Error> {
    ROOTFS_REGISTRY
        .get_or_init(Default::default)
        .get(function_id)
        .map(|entry| entry.value().clone())
        .ok_or_else(|| {
            Error::Init(format!(
                "missing rootfs path for FunctionId '{}'",
                function_id.as_str()
            ))
        })
}

/// Derive a guest vsock CID from a microVM identifier.
///
/// Firecracker requires a non-reserved guest CID for each vsock-enabled VM.
/// This helper hashes `vm_id` and maps it into the usable CID range
/// `[3, u32::MAX - 1]`, avoiding the reserved low CIDs `0`, `1`, and `2`, as
/// well as [`u32::MAX`], the wildcard CID.
///
/// # Notes
///
/// - The mapping is deterministic for a given `vm_id` under the current hasher.
/// - It is only a best-effort uniqueness scheme; collisions are still possible.
/// - It is used only to pick a valid, stable-enough guest CID for runtime
///   configuration.
#[inline]
fn vsock_guest_cid(vm_id: &str) -> u32 {
    3 + (BUILD_HASHER.hash_one(vm_id) as u32 % (u32::MAX - 3))
}

pub struct Firecracker {
    config: FirecrackerConfig,
    function_info: FcFunctionInfo,

    /// Host-side identifier of the uVM currently associated with this runtime.
    ///
    /// This name is used as the basename for the per-uVM directory under
    /// [`FirecrackerConfig::uvms_root_path`], and therefore also determines
    /// paths such as the API socket, FIFO files, vsock UDS, and the JSON
    /// config file managed for this uVM.
    ///
    /// ## Notes
    ///
    /// This is populated at one of these points:
    /// - in [`Self::new`], if this [`Runtime`] is instantiated with an existing
    ///   [`MicroVm`];
    /// - in [`Self::create_sandbox`], when this [`Runtime`] creates a fresh uVM.
    ///
    /// A value here means this [`Runtime`] is bound to a specific host-side
    /// uVM identity, but does not by itself imply that a Firecracker process
    /// is currently running for that uVM.
    uvm_name: Option<CompactString>,

    client: Option<::wick::Client>,
}

impl Runtime for Firecracker {
    type Config = FirecrackerConfig;
    type Sandbox = MicroVm;
    type NetResource = Tap;
    type FunctionInfo = FcFunctionInfo;

    const NAME: &'static str = "fc";

    fn new(
        config: &Self::Config,
        function_info: &Self::FunctionInfo,
        maybe_uvm: Option<&Self::Sandbox>,
    ) -> Result<Self, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        // TODO(ckatsak):
        // * [ ] unpack (or clone) runtime config
        //    - [ ] channel to main Runtime actor (TODO: is there a better way?)
        //    - [ ] populate `self.firecracker_bin`
        //    - [ ] populate `self.kernel_img`
        //    - [ ] populate `self.uvms_root_path`
        // * [ ] populate `self.function_info`
        // * [ ] if `maybe_uvm.is_some()`, populate `self.uvm_name`

        Ok(Self {
            config: config.clone(),
            function_info: function_info.clone(),
            uvm_name: maybe_uvm.map(|uvm| uvm.id().to_compact_string()),
            client: None,
        })
    }

    #[instrument(level = Level::TRACE, skip_all)]
    async fn init(&mut self) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        // TODO(ckatsak):
        // * attempt to create uVM directory (?)
        //   - populate `self.api_sock_path`
        //   - create FIFOs for uVM's logger and metrics
        // * "register" with main Runtime actor (TODO: why?)
        // * contact main Runtime actor to learn rootfs_path

        // This is right on the hot path. Should we assume/force the operator to create it a priori?
        if let Err(err) = MKDIR_UVMS_ROOT_DIR
            .get_or_try_init(|| async {
                ::tokio::fs::create_dir_all(&self.config.uvms_root_path).await
            })
            .await
        {
            error!(
                error = ?err, "Failed to create uVMs root directory '{}': {err:#}",
                self.config.uvms_root_path
            );
            return Err(Box::new(Error::Io {
                msg: format!(
                    "failed to create uVMs root directory '{}'",
                    self.config.uvms_root_path
                )
                .into_boxed_str(),
                err,
            }));
        }

        // NOTE: if we have a `uvm_name` here, we must have adopted an already existing uVM; hence
        // its root dir should be already populated. We don't really need to ensure any files here:
        // - `create_sandbox` ensures them itself;
        // - `load_sandbox` still has to deal with logging/metrics FIFOs and API sock anyway;
        // - Pool's orphan `destroy_sandbox` does not need to ensure these files to delete them;
        // - `reinstate_sandbox` does not depend on populated `uvm_name` to begin with.

        Ok(())
    }

    // FIXME(ckatsak): Failures during Sandbox creation leak the provided net resource. This is not
    // specific to this Runtime impl; also affects `fcctrd`. TODO: Maybe change method signature to
    // also return the net resource on failure, along with the Error?
    #[instrument(level = Level::DEBUG, skip_all)]
    async fn create_sandbox(
        &mut self,
        function_id: &FunctionId,
        tap_resource: Self::NetResource,
        timings: &mut EnumMap<Timing, Nanoseconds>,
    ) -> Result<Self::Sandbox, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        debug_assert!(
            self.uvm_name.is_none() && self.client.is_none(),
            "BUG: Creating new Sandbox while already owning one"
        );

        // Create & configure the tap device
        let setup_net_start = Instant::now();
        let tap = match tap_resource {
            Tap::Device(tap) => tap,
            Tap::Builder(tap_builder) => tap_builder.build().await.map_err(Box::new)?,
        };
        timings[Timing::SetupResources] += setup_net_start.elapsed().as_nanos() as Nanoseconds;

        // Create the uVM
        let create_sandbox_start = Instant::now();
        defer! {
            timings[Timing::CreateSandbox] += create_sandbox_start.elapsed().as_nanos() as Nanoseconds;
        }

        let vm_id = tap.name().to_compact_string();

        // Ensure filesystem is in workable state
        let uvm_dir_path = self.config.uvms_root_path.join(vm_id.as_str());
        match ::tokio::task::spawn_blocking({
            let uvm_dir_path = uvm_dir_path.clone();
            let sp = trace_span!(parent: &Span::current(), "ensure_uvm_files").or_current();

            move || sp.in_scope(|| Self::ensure_uvm_files(&uvm_dir_path, EnsureFilesMode::Creation))
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                error!(error = ?err, "Failed to ensure uVM files: {err:#}");
                return Err(Error::CreateVm(Box::new(err)).into()); // FIXME: error kinds & handling
            }
            Err(err) => {
                error!(error = ?err, "Failed to join tokio task: {err:#}");
                return Err(Error::CreateVm(Box::new(err)).into()); // FIXME: error kinds & handling
            }
        }
        let api_sock_path = uvm_dir_path.join(API_SOCK_BASENAME);
        let config_file_path = uvm_dir_path.join(UVM_CONFIG_BASENAME);

        // Populate uVM's config file
        let rootfs_path = lookup_rootfs_path(function_id)?;
        let config_json = self.render_uvm_config(&rootfs_path, &tap, &uvm_dir_path, &vm_id);
        fs::write(&config_file_path, config_json)
            .await
            .map_err(|err| Error::Io {
                msg: format!("failed to write uVM config file '{config_file_path}'")
                    .into_boxed_str(),
                err,
            })?;

        // fork+exec firecracker and pump emitted logs & metrics them into our configured logger
        let fcc = ::wick::Client::new(&api_sock_path);
        let mut fc_proc = initialize_fc_process(
            &fcc,
            &self.config.firecracker_bin,
            vm_id.as_str(),
            InitFiles::Config(&config_file_path),
        )
        .await
        .inspect_err(|err| {
            error!(error = ?err, "Failed to initialize_fc_process: {err:#}");
        })?;

        // Wait for the API server to spawn, as a minimal indication of initialization. Perhaps
        // ~150ms is too short? Fail fast if the Firecracker child dies before that happens.
        if let Err(err) = await_fc_api_server(fcc.socket_path(), || fc_proc.try_wait()).await {
            error!(error = ?err, "Failed awaiting API server: {err:#}");
            fc_proc.kill_and_wait().await;
            return Err(Box::new(Error::Io {
                msg: "awaiting Firecracker's API server".into(),
                err,
            }));
        }
        // Also check `wick::Client::describe_instance` to verify uVM ID and
        // State::Running before returning, as we do in `Self::load_sandbox`.
        match fcc.describe_instance().await {
            Ok(instance_info)
                if instance_info.state == ::wick::models::instance_info::State::Running
                    && instance_info.id == vm_id =>
            {
                trace!(?instance_info, "uVM successfully started")
            }
            Ok(instance_info) => {
                error!(
                    ?instance_info,
                    "started uVM either has invalid ID or appears not to be Running"
                );
                fc_proc.kill_and_wait().await;
                return Err(Error::CreateVm(
                    format!("started uVM in invalid state: {instance_info:?}").into(),
                )
                .into()); // FIXME: error kinds & handling?
            }
            Err(err) => {
                error!(error = ?err, "Failed to query VMM's InstanceInfo: {err:#}");
                fc_proc.kill_and_wait().await;
                return Err(Error::CreateVm(Box::new(err)).into()); // FIXME: error kinds & handling?
            }
        }

        self.uvm_name = Some(vm_id);
        self.client = Some(fcc);

        Ok(MicroVm {
            stats: Default::default(),
            function_id: function_id.clone(),
            tap,
            proc: Some(fc_proc),
            snapshot: None,
        })
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn load_sandbox(
        &mut self,
        uvm: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        debug_assert!(
            uvm.proc.is_none(),
            "BUG: loading snapshot for Sandbox with child process"
        );

        if !uvm.has_snapshot() {
            error!(uvm.id = %uvm.id(),"Attempting to load non-snapshotted Sandbox");
            return Err(Box::new(Error::NoSnapshot(
                format!("loading {}", uvm.id()).into_boxed_str(),
            )));
            // FIXME: error kinds & handling
        }
        // From now on, `sandbox.snapshot.unwrap()` should be safe.

        // TODO(ckatsak):
        // * [ ] check if directory is still there?
        //   - [ ] `rm $api_sock_path`?
        //   - [ ] what about logs and metrics files?
        //   - [ ] what about vsock path?
        // * [ ] fork+exec firecracker
        // * [ ] setup logging & metrics via API server
        //   - [ ] what about vsock? it probably can be overriden, but is it needed?
        // * [ ] PUT /snapshot/load
        // * [ ] PUT /vm {"state":"Resumed"}

        let vm_id = self.uvm_name.as_ref().expect("uvm_name must be set by now");
        let uvm_dir_path = self.config.uvms_root_path.join(vm_id.as_str());

        // Ensure filesystem is in workable state
        match ::tokio::task::spawn_blocking({
            let uvm_dir_path = uvm_dir_path.clone();
            let sp = trace_span!(parent: &Span::current(), "ensure_uvm_files").or_current();

            move || sp.in_scope(|| Self::ensure_uvm_files(&uvm_dir_path, EnsureFilesMode::Loading))
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                error!(error = ?err, "Failed to ensure uVM files: {err:#}");
                return Err(Error::LoadSnapshot(Box::new(err)).into()); // FIXME: error kinds & handling
            }
            Err(err) => {
                error!(error = ?err, "Failed to join tokio task: {err:#}");
                return Err(Error::LoadSnapshot(Box::new(err)).into()); // FIXME: error kinds & handling
            }
        }

        let api_sock_path = uvm_dir_path.join(API_SOCK_BASENAME);
        let fcc = ::wick::Client::new(&api_sock_path);

        // fork+exec firecracker and pump emitted logs & metrics them into our configured logger
        let fc_proc = initialize_fc_process(
            &fcc,
            &self.config.firecracker_bin,
            vm_id,
            InitFiles::Fifos {
                logging: uvm_dir_path.join(LOGGING_FIFO_BASENAME).as_path(),
                metrics: uvm_dir_path.join(METRICS_FIFO_BASENAME).as_path(),
            },
        )
        .await
        .inspect_err(|err| {
            error!(error = ?err, "Failed to initialize_fc_process: {err:#}");
        })?; // TODO?FIXME(XXX)

        if let Err(err) = fcc
            .load_snapshot(::wick::models::SnapshotLoadParams {
                track_dirty_pages: Some(false),
                mem_file_path: None,
                mem_backend: Some(::wick::models::MemoryBackend {
                    backend_type: ::wick::models::memory_backend::BackendType::File,
                    backend_path: uvm
                        .snapshot
                        .as_ref()
                        .expect("snapshot existence checked earlier")
                        .memory()
                        .into(),
                }),
                snapshot_path: uvm
                    .snapshot
                    .as_ref()
                    .expect("snapshot existence checked earlier")
                    .state()
                    .into(),
                resume_vm: Some(true),
                network_overrides: None,
                clock_realtime: None,
            })
            .await
        {
            error!(error = ?err, "Failed to 'PUT /snapshot/load': {err:#}");
            fc_proc.kill_and_wait().await;
            return Err(Error::LoadSnapshot(Box::new(err)).into()); // FIXME: error kinds & handling
        }

        // Perhaps `wick::Client::describe_instance` before returning, to ensure
        // the uVM is indeed in `State::Running`? Maybe it's unnecessary?
        match fcc.describe_instance().await {
            Ok(instance_info)
                if instance_info.state == ::wick::models::instance_info::State::Running
                    && instance_info.id == vm_id =>
            {
                trace!(?instance_info, "uVM successfully restored")
            }
            Ok(instance_info) => {
                error!(
                    ?instance_info,
                    "restored uVM either has invalid ID or appears not to be Running"
                );
                fc_proc.kill_and_wait().await;
                return Err(Error::LoadSnapshot(
                    format!("restored uVM in invalid state: {instance_info:?}").into(),
                )
                .into()); // FIXME: error kinds & handling?
            }
            Err(err) => {
                error!(error = ?err, "Failed to query VMM's InstanceInfo: {err:#}");
                fc_proc.kill_and_wait().await;
                return Err(Error::LoadSnapshot(Box::new(err)).into()); // FIXME: error kinds & handling?
            }
        }

        uvm.proc = Some(fc_proc);
        self.client = Some(fcc);

        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn pause_sandbox(
        &mut self,
        _: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let fcc = self.client.as_ref().ok_or_else(|| {
            Box::new(Error::PauseVm(
                "BUG: attempting to pause uVM without client".into(),
            )) // FIXME: error kinds & handling
        })?;

        fcc.patch_vm(::wick::models::Vm {
            state: ::wick::models::vm::State::Paused,
        })
        .await
        .map_err(|err| {
            error!(error = ?err, "Failed to pause uVM: {err:#}");
            Error::PauseVm(Box::new(err)).into() // FIXME: error kinds & handling
        })
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn resume_sandbox(
        &mut self,
        _: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let fcc = self.client.as_ref().ok_or_else(|| {
            Box::new(Error::ResumeVm(
                "BUG: attempting to resume uVM without client".into(),
            )) // FIXME: error kinds & handling
        })?;

        fcc.patch_vm(::wick::models::Vm {
            state: ::wick::models::vm::State::Resumed,
        })
        .await
        .map_err(|err| {
            error!(error = ?err, "Failed to resume uVM: {err:#}");
            Error::ResumeVm(Box::new(err)).into() // FIXME: error kinds & handling
        })
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn create_snapshot(
        &mut self,
        uvm: &mut Self::Sandbox,
        state_file_path: impl AsRef<Path> + Send + Sync + Debug,
        memory_file_path: impl AsRef<Path> + Send + Sync + Debug,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        if uvm.has_snapshot() {
            warn!("Snapshotting an already snapshotted uVM!");
            // FIXME: We should probably fail this? TODO
        }

        let fcc = self.client.as_ref().ok_or_else(|| {
            Box::new(Error::CreateSnapshot(
                "BUG: attempting to snapshot uVM without client".into(),
            )) // FIXME: error kinds & handling
        })?;

        let state_file_path: ::camino::Utf8PathBuf = state_file_path
            .as_ref()
            .to_str()
            .ok_or_else(|| Error::Utf8(state_file_path.as_ref().to_path_buf()))?
            .into();
        let memory_file_path: ::camino::Utf8PathBuf = memory_file_path
            .as_ref()
            .to_str()
            .ok_or_else(|| Error::Utf8(memory_file_path.as_ref().to_path_buf()))?
            .into();

        if let Err(err) = fcc
            .create_snapshot(::wick::models::SnapshotCreateParams {
                snapshot_type: Some(::wick::models::snapshot_create_params::SnapshotType::Full),
                snapshot_path: state_file_path.clone(),
                mem_file_path: memory_file_path.clone(),
            })
            .await
        {
            error!(error = ?err, "Failed to create uVM snapshot: {err:#}");
            return Err(Error::CreateSnapshot(Box::new(err)).into());
            // FIXME: error kinds & handling
        }

        uvm.snapshot = Some(SnapshotFiles::new(state_file_path, memory_file_path));

        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn shutdown_sandbox(
        &mut self,
        uvm: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let Some(fc_proc) = uvm.proc.take() else {
            warn!("Attempted to shut down a uVM that is already down");
            return Ok(());
        };

        fc_proc.kill_and_wait().await;

        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn destroy_sandbox(
        &mut self,
        mut uvm: Self::Sandbox,
    ) -> Result<Self::NetResource, DestroySandboxRuntimeError<Self::Sandbox>> {
        // Kill the process
        //self.shutdown_sandbox(&mut uvm).await?;
        if let Some(fc_proc) = uvm.proc.take() {
            fc_proc.kill_and_wait().await;
        }

        let mut js = ::tokio::task::JoinSet::new();
        // Recursively remove uVM's directory and its contents
        let _ = js.spawn_blocking({
            let uvm_dir_path = self.config.uvms_root_path.join(
                self.uvm_name
                    .as_ref()
                    .expect("BUG: Destroying Sandbox while not owning one")
                    .as_str(),
            );
            move || {
                let _ = ::std::fs::remove_dir_all(&uvm_dir_path).inspect_err(|err| {
                    error!(
                        error = ?err, dir.path = %uvm_dir_path,
                        "Failed to recursively remove uVM's directory: {err:#}"
                    )
                });
            }
        });
        // Best-effort unlink any snapshot files
        if let Some(snap_files) = uvm.snapshot.take() {
            let _ = js.spawn_blocking(move || {
                let (sf, mf) = (snap_files.state(), snap_files.memory());
                let _ = ::rustix::fs::unlink(sf.as_std_path()).inspect_err(|errno| {
                    error!(
                        error = ?errno, snapshot.file = %sf, "Failed to unlink: {errno:#}"
                    )
                });
                let _ = ::rustix::fs::unlink(mf.as_std_path()).inspect_err(|errno| {
                    error!(
                        error = ?errno, snapshot.file = %mf, "Failed to unlink: {errno:#}"
                    )
                });
            });
        }

        // Reset self's fields
        let _ = self.uvm_name.take();
        let _ = self.client.take();

        while let Some(jres) = js.join_next().await {
            if let Err(jerr) = jres {
                error!(error = ?jerr, "Failed to join tokio blocking-thread task: {jerr:#}");
            }
        }

        Ok(Tap::Device(uvm.tap))
    }

    /// Remove [`MicroVm`]'s snapshot files from the page cache using `fadvise(2)`.
    #[cfg(feature = "uncache")]
    #[instrument(level = Level::INFO, skip_all, fields(uvm = uvm.id()))]
    async fn uncache_sandbox(
        &mut self,
        uvm: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        if let Some(ref snap_files) = uvm.snapshot {
            let _ = crate::utils::uncache::uncache_files(&[
                Cow::Borrowed(snap_files.state().as_std_path()),
                Cow::Borrowed(snap_files.memory().as_std_path()),
            ])
            .await
            .inspect_err(
                |err| warn!(error = ?err, "Failed while uncaching snapshot files: {err:#}"),
            );
        }
        Ok(())
    }

    /// FIXME: For now, this method always succeeds, regardless of whether it did actually
    /// enforce the provided resource constraints.
    #[cfg(feature = "sched-setaffinity")]
    #[instrument(level = Level::TRACE, skip_all, fields(uvm = uvm.id()))]
    async fn setup_cpuset(
        &mut self,
        uvm: &mut Self::Sandbox,
        cpuset: crate::sbpool::Cpu,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        use rustix::{
            process::Pid,
            thread::{sched_setaffinity, CpuSet},
        };

        if let Some(pid) = uvm.proc.as_ref().and_then(|p| p.proc.id()) {
            let pid = Pid::from_raw(pid as _).unwrap();

            let mut actual_cpuset = CpuSet::new();
            actual_cpuset.set(cpuset.as_u16() as _);

            let _ = sched_setaffinity(Some(pid), &actual_cpuset).inspect_err(|errno| {
                error!(
                    error = ?errno, ?cpuset,
                    "Failed to pin uvm to CPU: sched_setaffinity: {errno:#}",
                )
            });
        } else {
            // We should never reach this point; otherwise, there must be some BUG in snaplace.
            error!(?uvm, "uVM does not have a PID?!");
        }

        Ok(())
    }

    /// # Note
    ///
    /// For now, this runtime assumes that Function IDs are of the form `${PREFIX}-${SUFFIX}`,
    /// and that the rootfs of the corresponding Function is located at
    /// <code>[FirecrackerConfig::uvms_rootfs_path]/${PREFIX}.ext4</code>.
    #[instrument(level = Level::DEBUG, skip_all, fields(function.id = %function_info.id))]
    async fn register_function(
        config: &Self::Config,
        function_info: &mut Self::FunctionInfo,
    ) -> Result<(), registration::Error> {
        let Some((rootfs_file_basename, _remainder)) = function_info.id.split_once('-') else {
            // NOTE(ckatsak): See function's docs about the assumed format of Function ID. FIXME?
            return Err(registration::Error::Runtime {
                msg: format!(
                    "expected Function ID in the form '$PREFIX-$SUFFIX'; got '{}'",
                    function_info.id
                )
                .into_boxed_str(),
                err: None,
            });
        };
        let rootfs_path = config
            .uvms_rootfs_path
            .join(format!("{rootfs_file_basename}.{ROOTFS_IMG_EXT}"));
        if let Err(err) = fs::metadata(&rootfs_path).await {
            return Err(registration::Error::Runtime {
                msg: format!("missing rootfs image at '{}'", rootfs_path.display())
                    .into_boxed_str(),
                err: Some(Box::new(err)),
            });
        }

        ROOTFS_REGISTRY
            .get_or_init(Default::default)
            .insert(function_info.id.clone(), rootfs_path);

        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(function.id = %function_info.id))]
    async fn deregister_function(
        _: &Self::Config,
        function_info: &Self::FunctionInfo,
    ) -> Result<(), registration::Error> {
        ROOTFS_REGISTRY
            .get_or_init(Default::default)
            .remove(function_info.id.as_str());

        Ok(())
    }

    async fn reinstate_sandbox(
        &mut self,
        function_id: FunctionId,
        state: <Self::Sandbox as Sandbox>::SnapshotState,
        netman: crate::network::NetworkManagerRef<Self::NetResource>,
    ) -> Option<Result<Self::Sandbox, Box<dyn ::std::error::Error + Send + Sync + 'static>>> {
        let tap = match netman.request(state.tap.clone()).await {
            Ok(rx) => match rx.await {
                Ok(Ok(Tap::Device(tap))) => tap,
                Ok(Ok(Tap::Builder(tap_builder))) => match tap_builder.build_or_adopt().await {
                    Ok(tap) => tap,
                    Err(err) => {
                        return Some(Err(Box::new(Error::ReinstateSandbox {
                            msg: format!("failed to build or adopt requested {:?}", state.tap)
                                .into_boxed_str(),
                            err: Some(Box::new(err)),
                        })))
                    }
                },
                Ok(Err(err)) => {
                    return Some(Err(Box::new(Error::ReinstateSandbox {
                        msg: format!("failed requested allocation for {:?}", state.tap)
                            .into_boxed_str(),
                        err: Some(Box::new(err)),
                    })))
                }
                Err(err) => {
                    return Some(Err(Box::new(Error::ReinstateSandbox {
                        msg: "failed to receive from netman's oneshot channel".into(),
                        err: Some(Box::new(err)),
                    })))
                }
            },
            Err(err) => {
                return Some(Err(Box::new(Error::ReinstateSandbox {
                    msg: format!("failed to request from netman {:?}", state.tap).into_boxed_str(),
                    err: Some(Box::new(err)),
                })))
            }
        };

        Some(Ok(MicroVm {
            stats: state.stats,
            function_id,
            tap,
            proc: None,
            snapshot: Some(state.snapshot),
        }))
    }
}

/// Filesystem-preparation mode for a uVM directory.
///
/// This controls how aggressively [`Firecracker::ensure_uvm_files`] cleans up
/// stale files before Firecracker is started.
///
/// - [`Creation`](Self::Creation) is used when creating a fresh uVM from a
///   rendered JSON config file.  In addition to ensuring FIFOs and unlinking
///   stale sockets, it also removes any stale JSON config file.
/// - [`Loading`](Self::Loading) is used when restoring from snapshot.  It keeps
///   the existing snapshot-related files in place and skips removing the config
///   file, because startup proceeds through the API rather than a rendered JSON
///   config file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum EnsureFilesMode {
    /// Used when creating a fresh uVM from a rendered JSON config file.  In
    /// addition to ensuring FIFOs and unlinking stale sockets, it also removes
    /// any stale JSON config file.
    #[default]
    Creation,
    /// Used when restoring from snapshot.  It keeps the existing snapshot-related
    /// files in place and skips removing the config file, because startup
    /// proceeds through the API rather than a rendered JSON config file.
    Loading,
}

impl Firecracker {
    /// Auxiliary function that ensures the following:
    /// - uVM's directory exists;
    /// - Firecracker logger's and metrics' filesystem nodes exist there, and
    ///   they are FIFOs;
    /// - uVM's API Unix domain socket path does not exist (i.e., it's available
    ///   for use);
    /// - if `mode` is not [`EnsureFilesMode::Loading`], filesystem node for
    ///   uVM's JSON config file does not exist at all.
    fn ensure_uvm_files(uvm_dir_path: &Utf8Path, mode: EnsureFilesMode) -> Result<(), Error> {
        use rustix::{
            fs::{mkdir, mkfifoat, open, statat, unlinkat, AtFlags, FileType, Mode, OFlags},
            io::Errno,
        };

        macro_rules! io_err {
            ($errno:ident, $sc:literal) => {{
                Error::Io {
                    msg: format!(concat!("failed to ", $sc, " '{}'"), uvm_dir_path)
                        .into_boxed_str(),
                    err: $errno.into(),
                }
            }};
            ($errno:ident, $sc:literal, $BASENAME:ident) => {{
                Error::Io {
                    msg: format!(
                        concat!("failed to ", $sc, " '{}/{}'"),
                        uvm_dir_path, $BASENAME,
                    )
                    .into_boxed_str(),
                    err: $errno.into(),
                }
            }};
        }

        // Create & open uVM dir
        let mut dir_existed = false;
        match mkdir(uvm_dir_path.as_std_path(), Mode::RWXU | Mode::RWXG) {
            Ok(()) => {}
            Err(errno) if errno == Errno::EXIST => dir_existed = true,
            Err(errno) => return Err(io_err!(errno, "mkdir(2)")),
        }
        let dirfd = open(
            uvm_dir_path.as_std_path(),
            OFlags::RDONLY | OFlags::DIRECTORY,
            Mode::RWXU | Mode::RWXG,
        )
        .map_err(|errno| io_err!(errno, "open(2)"))?;

        // TODO(ckatsak): Since Firecracker v1.14.0, if the logger's and metrics' nodes do not
        // already exist, Firecracker creates regular files for them automatically. However, if
        // we want these to be FIFOs, we still need to manually create them. Re-evaluate this.

        // Create logger's FIFO
        match mkfifoat(&dirfd, LOGGING_FIFO_BASENAME, Mode::RWXU | Mode::RWXG) {
            Ok(()) => {}
            Err(errno) if errno == Errno::EXIST => {
                let stat = statat(&dirfd, LOGGING_FIFO_BASENAME, AtFlags::SYMLINK_NOFOLLOW)
                    .map_err(|errno| io_err!(errno, "statat(2)", LOGGING_FIFO_BASENAME))?;
                if !FileType::from_raw_mode(stat.st_mode).is_fifo() {
                    unlinkat(&dirfd, LOGGING_FIFO_BASENAME, AtFlags::empty())
                        .map_err(|errno| io_err!(errno, "unlinkat(2)", LOGGING_FIFO_BASENAME))?;
                    mkfifoat(&dirfd, LOGGING_FIFO_BASENAME, Mode::RWXU | Mode::RWXG)
                        .map_err(|errno| io_err!(errno, "mkfifoat(2)", LOGGING_FIFO_BASENAME))?;
                }
            }
            Err(errno) => return Err(io_err!(errno, "mkfifoat(2)", LOGGING_FIFO_BASENAME)),
        }

        // Create metrics' FIFO
        match mkfifoat(&dirfd, METRICS_FIFO_BASENAME, Mode::RWXU | Mode::RWXG) {
            Ok(()) => {}
            Err(errno) if errno == Errno::EXIST => {
                let stat = statat(&dirfd, METRICS_FIFO_BASENAME, AtFlags::SYMLINK_NOFOLLOW)
                    .map_err(|errno| io_err!(errno, "statat(2)", METRICS_FIFO_BASENAME))?;
                if !FileType::from_raw_mode(stat.st_mode).is_fifo() {
                    unlinkat(&dirfd, METRICS_FIFO_BASENAME, AtFlags::empty())
                        .map_err(|errno| io_err!(errno, "unlinkat(2)", METRICS_FIFO_BASENAME))?;
                    mkfifoat(&dirfd, METRICS_FIFO_BASENAME, Mode::RWXU | Mode::RWXG)
                        .map_err(|errno| io_err!(errno, "mkfifoat(2)", METRICS_FIFO_BASENAME))?;
                }
            }
            Err(errno) => return Err(io_err!(errno, "mkfifoat(2)", METRICS_FIFO_BASENAME)),
        }

        if dir_existed {
            match unlinkat(&dirfd, API_SOCK_BASENAME, AtFlags::empty()) {
                Ok(()) => {}
                Err(errno) if errno == Errno::NOENT => {}
                Err(errno) => return Err(io_err!(errno, "unlinkat(2)", API_SOCK_BASENAME)),
            }
            match unlinkat(&dirfd, VSOCK_UDS_BASENAME, AtFlags::empty()) {
                Ok(()) => {}
                Err(errno) if errno == Errno::NOENT => {}
                Err(errno) => return Err(io_err!(errno, "unlinkat(2)", VSOCK_UDS_BASENAME)),
            }
            if mode != EnsureFilesMode::Loading {
                match unlinkat(&dirfd, UVM_CONFIG_BASENAME, AtFlags::empty()) {
                    Ok(()) => {}
                    Err(errno) if errno == Errno::NOENT => {}
                    Err(errno) => return Err(io_err!(errno, "unlinkat(2)", UVM_CONFIG_BASENAME)),
                }
            }
            // FIXME(ckatsak): Rather than unlink(2)ing UVM_CONFIG, maybe populate it here (with
            // `O_TRUNC`) and get this over with? Though this would the require parameters of
            // the specific Function, (hence either `self` or just a `FunctionInfo` instance).
        }

        Ok(())
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn render_uvm_config(
        &self,
        rootfs_path: &Path,
        tap: &TapDevice,
        uvm_dir_path: &Utf8Path,
        vm_id: &str,
    ) -> String {
        let vmm_log_path = uvm_dir_path.join(LOGGING_FIFO_BASENAME);
        let vmm_metrics_path = uvm_dir_path.join(METRICS_FIFO_BASENAME);
        let vsock_uds_path = uvm_dir_path.join(VSOCK_UDS_BASENAME);
        let kernel_img = self.config.kernel_img.to_string_lossy();
        let rootfs_p = rootfs_path.to_string_lossy();

        let mut vcpu_count = ::itoa::Buffer::new();
        let mut memory_mib = ::itoa::Buffer::new();
        let mut vsock_cid = ::itoa::Buffer::new();

        let kernel_boot_args = format!(
            "{} {}",
            self.config.kernel_verbosity.boot_params(),
            ip_boot_param(tap)
        );
        trace!(?kernel_boot_args);

        let replacements = [
            vcpu_count.format(self.function_info.vcpu_count),
            memory_mib.format(self.function_info.memory.as_u64() >> 20),
            vsock_cid.format(vsock_guest_cid(vm_id)),
            kernel_img.as_ref(),
            &kernel_boot_args,
            rootfs_p.as_ref(),
            tap.name(),
            vsock_uds_path.as_ref(),
            DEFAULT_VMM_LOG_LEVEL,
            vmm_log_path.as_ref(),
            vmm_metrics_path.as_ref(),
        ];

        AUTOMATON
            .get_or_init(|| {
                AhoCorasick::new(UVM_CONFIG_PATTERNS)
                    .expect("failed to initialize AhoCorasick for uVM config template")
            })
            .replace_all(UVM_CONFIG_TEMPLATE, &replacements)
    }
}

/// Returns an appropriate `ip=...` kernel boot CLI argument based on the
/// configuration of the provided [`TapDevice`].
///
/// See <https://www.kernel.org/doc/Documentation/filesystems/nfs/nfsroot.txt>.
///
fn ip_boot_param(tap: &TapDevice) -> String {
    /// # Examples
    /// ```
    /// prefix_to_mask(0)  == 0.0.0.0
    /// prefix_to_mask(24) == 255.255.255.0
    /// prefix_to_mask(31) == 255.255.255.254
    /// prefix_to_mask(32) == 255.255.255.255
    /// ```
    fn prefix_to_mask(prefix_len: u8) -> Ipv4Addr {
        assert!(prefix_len <= Ipv4Addr::BITS as u8);

        let mask = if prefix_len == 0 {
            0
        } else {
            u32::MAX << (Ipv4Addr::BITS as u8 - prefix_len)
        };
        // This should work on both big- and little-endian hosts.
        Ipv4Addr::from(mask)
    }

    const HOSTNAME: &str = "";
    const DEVICE: &str = "eth0";
    const AUTOCONF: &str = "off";
    const DNS0_IP: &str = "1.1.1.1"; // TODO: Make nameservers configurable?
    const DNS1_IP: &str = "1.0.0.1";
    const NTP0_IP: &str = "";

    let client_ip = tap.ip_addr();
    let gw_ip = tap.gateway();
    let netmask = prefix_to_mask(tap.prefix_len());

    format!("ip={client_ip}::{gw_ip}:{netmask}:{HOSTNAME}:{DEVICE}:{AUTOCONF}:{DNS0_IP}:{DNS1_IP}:{NTP0_IP}")
}

/// Runtime-only state for a running Firecracker VMM process.
///
/// This owns the child process, its pidfd, and the asynchronous tasks that pump
/// Firecracker stdio, log FIFO, and metrics FIFO output into tracing. It is not
/// part of the sandbox's serializable snapshot state; a snapshotted sandbox is
/// represented with `proc: None` and can later be loaded by starting a fresh
/// Firecracker process.
///
/// Use [`Self::kill_and_wait`] when the process must be torn down deterministically.
pub(super) struct FcProcessState {
    proc: Child,
    pidfd: AsyncPidFd,

    stdout_h: JoinHandle<io::Result<()>>,
    stderr_h: JoinHandle<io::Result<()>>,

    logging_h: JoinHandle<io::Result<()>>,
    metrics_h: JoinHandle<io::Result<()>>,
    cancel: DropGuard, // cancel FIFO-reading tasks on drop
}

impl Debug for FcProcessState {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("FcProcessState")
            .field("pid", &self.proc.id())
            .finish()
    }
}

impl FcProcessState {
    #[inline]
    pub fn pid(&self) -> Option<u32> {
        self.proc.id()
    }

    /// Polls the Firecracker child process without blocking.
    ///
    /// Returns `Ok(Some(status))` if the process has already exited,
    /// `Ok(None)` if it is still running, and propagates any OS error from
    /// [`tokio::process::Child::try_wait`].
    #[inline]
    pub fn try_wait(&mut self) -> io::Result<Option<::std::process::ExitStatus>> {
        self.proc.try_wait()
    }

    #[instrument(level = Level::TRACE, skip_all)]
    async fn kill_and_wait(mut self) {
        //self.cancel.disarm().cancel();
        drop(self.cancel);

        if let Err(err) = self.proc.kill().await {
            error!(error = ?err, "Failed to kill Firecracker process: {err:#}");
        }

        match self.stdout_h.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(error = ?err, "I/O error on stdout task: {err:#}"),
            Err(err) => warn!(error = ?err, "Failed to join stdout task: {err:#}"),
        }
        match self.stderr_h.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(error = ?err, "I/O error on stderr task: {err:#}"),
            Err(err) => warn!(error = ?err, "Failed to join stderr task: {err:#}"),
        }

        match self.logging_h.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(error = ?err, "I/O error on logging task: {err:#}"),
            Err(err) => warn!(error = ?err, "Failed to join logging task: {err:#}"),
        }
        match self.metrics_h.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(error = ?err, "I/O error on metrics task: {err:#}"),
            Err(err) => warn!(error = ?err, "Failed to join metrics task: {err:#}"),
        }
    }
}

/// Repeatedly attempts to connect to the Unix Domain Socket at the given path.
///
/// # Errors
///
/// Returns either the [`io::Error`] returned by the last [`UnixStream::connect`]
/// attempt, or an [`io::ErrorKind::Other`] if the child is found to have died.
///
/// # Notes
///
/// This function only attempts to [`UnixStream::connect`], and checks whether
/// the child process is still alive.
/// Perhaps [`wick::Client::describe_instance`] provides more information,
/// e.g., validating uVM credentials, etc?
///
/// ## Retries
///
/// Starting at __500us__, capping at __10ms__, for a total of __20 times__,
/// the expected delays (in __ms__) of the sequence should roughly (because of
/// the added jitter) be:
///
/// |  i   | Raw Value | Saturated | Cum Sum |
/// | :--- | :---      | :---      | :---    |
/// | 1    | 0.5       | 0.5       | 0.5     |
/// | 2    | 0.5       | 0.5       | 1.0     |
/// | 3    | 1.0       | 1.0       | 2.0     |
/// | 4    | 1.5       | 1.5       | 3.5     |
/// | 5    | 2.5       | 2.5       | 6.0     |
/// | 6    | 4.0       | 4.0       | 10.0    |
/// | 7    | 6.5       | 6.5       | 16.5    |
/// | 8    | 10.5      | 10.0      | 26.5    |
/// | 9    | 17.0      | 10.0      | 36.5    |
/// | 10   | 27.5      | 10.0      | 46.5    |
/// | 11   | 44.5      | 10.0      | 56.5    |
/// | 12   | 72.0      | 10.0      | 66.5    |
/// | 13   | 116.5     | 10.0      | 76.5    |
/// | 14   | 188.5     | 10.0      | 86.5    |
/// | 15   | 305.0     | 10.0      | 96.5    |
/// | 16   | 493.5     | 10.0      | 106.5   |
/// | 17   | 798.5     | 10.0      | 116.5   |
/// | 18   | 1292.0    | 10.0      | 126.5   |
/// | 19   | 2090.5    | 10.0      | 136.5   |
/// | 20   | 3382.5    | 10.0      | 146.5   |
#[instrument(level = Level::TRACE, skip_all, fields(sock = %sock_path.as_ref().display()))]
async fn await_fc_api_server<F>(sock_path: impl AsRef<Path>, mut child_status: F) -> io::Result<()>
where
    F: FnMut() -> io::Result<Option<ExitStatus>>,
{
    const _MAX_DELAY: Duration = Duration::from_millis(10);
    const _MAX_ATTEMPTS: i32 = 20;

    let (mut prev_delay, mut curr_delay) = (Duration::ZERO, Duration::from_micros(500));
    let mut last_err = None;

    for attempt in 0.._MAX_ATTEMPTS {
        if let Some(exit_status) = child_status()? {
            return Err(io::Error::other(format!(
                "Firecracker exited before its API socket becomes ready: {exit_status:?}"
            )));
        }

        match UnixStream::connect(&sock_path.as_ref()).await {
            Ok(stream) => {
                trace!(peer.cred = ?stream.peer_cred());
                return Ok(());
            }
            Err(err) => {
                trace!(error = ?err, next.in = ?curr_delay);
                last_err = Some(err);
            }
        }

        if attempt + 1 < 20 {
            ::tokio::time::sleep(curr_delay).await;
            (prev_delay, curr_delay) = (curr_delay, (prev_delay + curr_delay).min(_MAX_DELAY));
        }
    }

    if let Some(exit_status) = child_status()? {
        return Err(io::Error::other(format!(
            "Firecracker exited before its API socket becomes ready: {exit_status:?}"
        )));
    }

    Err(last_err.unwrap_or_else(|| io::Error::other("Firecracker API socket did not become ready")))
}

/// Executes the provided Firecracker binary with the given arguments:
/// - `vm_id`: the new uVM's unique ID;
/// - `api_sock_path`: path to VMM's API server's Unix socket on the host;
/// - `maybe_config_file_path`: optional path to the JSON file for uVM
///   configuration on the host.
///
/// # Errors
///
/// - In case of failure to fork/exec the new process.
/// - If `pidfd_open(2)` fails after the new process is running.
///
/// When this function fails, it always makes sure it has cleaned up any new
/// process it may have spawned.
///
/// # Panics
///
/// - If [`tokio::process::Child::id`] fails to return a PID right after
///   successfully spawning the new Firecracker process.
///
/// # Notes
///
/// - The returned [`Child::stdout`] and [`Child::stderr`] should have been
///   captured (via [`Stdio::piped`]) and be available for consumption by this
///   function's caller.
#[instrument(level = Level::TRACE, skip_all)]
async fn fork_exec_fc(
    firecracker_bin_path: impl AsRef<Path>,
    vm_id: &str,
    api_sock_path: impl AsRef<Path>,
    maybe_config_file_path: Option<&Utf8Path>,
) -> Result<(Child, AsyncPidFd), Error> {
    let mut fc_process_cmd = Command::new(firecracker_bin_path.as_ref());
    fc_process_cmd
        .arg("--id")
        .arg(vm_id)
        .arg("--api-sock")
        .arg(api_sock_path.as_ref())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true); // NOTE(ckatsak): we probably want this as last-ditch attempt?
    if let Some(config_file_path) = maybe_config_file_path {
        fc_process_cmd.arg("--config-file").arg(config_file_path);
    }

    let mut fc_process = fc_process_cmd.spawn().map_err(|err| Error::Io {
        msg: "failed to fork/exec firecracker process".into(),
        err,
    })?;
    let pid = fc_process
        .id()
        .expect("child has not been polled to completion") as _;

    let pidfd = match AsyncPidFd::from_pid(pid) {
        Ok(pidfd) => pidfd,
        Err(err) => {
            error!(error = ?err, "Failed to pidfd_open(2) for PID {pid}: {err:#}");
            fc_process.kill().await.map_err(|err| Error::Io {
                msg: format!("failed to SIGKILL and reap child process {pid}").into_boxed_str(),
                err,
            })?;
            return Err(Error::Io {
                msg: format!("failed to pidfd_open(2) child process {pid}").into_boxed_str(),
                err,
            });
        }
    };

    Ok((fc_process, pidfd))
}

/// Files used to initialize Firecracker logging and metrics.
///
/// - `Config` means Firecracker is started from a rendered JSON config file
///   that already contains the logger and metrics paths.
/// - `Fifos` means Firecracker is started without a config file and the FIFO
///   paths are installed later through the API.
#[derive(Debug)]
enum InitFiles<'p> {
    Config(&'p Utf8Path),
    Fifos {
        logging: &'p Utf8Path,
        metrics: &'p Utf8Path,
    },
}

/// Start a Firecracker process and attach output pump tasks.
///
/// - For config-file boot, this starts Firecracker with `--config-file`.
/// - For snapshot-load boot, this starts Firecracker with only an API socket
///   and then configures logger and metrics FIFOs through the API.
/// - In both cases, stdout, stderr, log FIFO, and metrics FIFO readers are
///   spawned before returning the resulting process state.
#[instrument(level = Level::DEBUG, skip_all)]
async fn initialize_fc_process(
    fcc: &::wick::Client,
    fc_bin_path: impl AsRef<Path>,
    vm_id: &str,
    init_files: InitFiles<'_>,
) -> Result<FcProcessState, Error> {
    /// Spawns a task that ingests the data in the provided stdio stream
    /// (`stdout` or `stderr`), pumping them into our configured logger,
    /// stopping when EOF is reached (i.e., when the writing Firecracker
    /// process closes the corresponds stdio stream).
    #[inline]
    fn spawn_stdio_pump(
        id: CompactString,
        stream_name: &'static str,
        reader: impl AsyncRead + Unpin + Send + 'static,
    ) -> JoinHandle<io::Result<()>> {
        ::tokio::task::spawn(async move {
            let mut lines = BufReader::new(reader).lines();

            while let Some(output_line) = lines.next_line().await? {
                trace!(target: "fc.stdio", %id, stream = %stream_name, %output_line);
            }

            Ok(())
        })
    }

    /// Spawns a task that ingests the data in the provided FIFO path, pumping
    /// them into our configured logger, stopping when the provided `token` is
    /// [cancelled].
    ///
    /// [cancelled]: CancellationToken::cancelled
    #[inline]
    fn spawn_fifo_pump(
        id: CompactString,
        stream_name: &'static str,
        path: Cow<'_, Utf8Path>,
        token: CancellationToken,
    ) -> JoinHandle<io::Result<()>> {
        ::tokio::task::spawn({
            let p = path.into_owned();

            async move { fifo_pump_until_cancelled(p, stream_name, id.as_str(), token).await }
        })
    }

    // Two possible cases cases (corresponding to Create and Load, for now):
    // - If given a uVM JSON config file, use it to create a new uVM, with
    //   everything properly configured (including the FIFOs);
    // - if no uVM JSON config file is provided, we should fork/exec a Firecracker
    //   process, and then manually configure logging & metrics FIFOs (through API).
    // In both cases, at this step we end up with a `(child, pidfd)` and all
    // process's streams available.
    let (mut child, pidfd) = match init_files {
        InitFiles::Config(config_path) => {
            // NOTE(ckatsak): Caller should have already rendered the final uVM JSON config file
            fork_exec_fc(fc_bin_path, vm_id, fcc.socket_path(), Some(config_path)).await?
        }
        InitFiles::Fifos { logging, metrics } => {
            // NOTE(ckatsak): Caller should have already ensured FIFOs at the specified paths
            let (mut child, pidfd) =
                fork_exec_fc(fc_bin_path, vm_id, fcc.socket_path(), None).await?;

            // We are probably faster than Firecracker here; retry until connection succeeds?
            let _ = await_fc_api_server(fcc.socket_path(), || child.try_wait())
                .await
                .inspect_err(|err| warn!(error = ?err));
            // Log, try once more on the next API call, and crash there if still fails.

            // NOTE: As long as Firecracker `open(FIFO, O_RDWR | O_NONBLOCK)`,
            // letting it open it before our own readers do is fine.

            // Setup logging FIFO
            if let Err(err) = fcc
                .put_logger(::wick::models::Logger {
                    level: if ::tracing::enabled!(::tracing::Level::TRACE)
                        || ::tracing::enabled!(::tracing::Level::DEBUG)
                    {
                        Some(::wick::models::logger::Level::Trace)
                    } else {
                        // TODO: `DEFAULT_VMM_LOG_LEVEL` rather than hardcoding (again)?
                        Some(::wick::models::logger::Level::Warning)
                    },
                    log_path: Some(logging.to_path_buf()),
                    show_level: Some(true),
                    show_log_origin: Some(true),
                    module: None,
                })
                .await
            {
                error!(error = ?err, "Failed to configure logger: {err:#}");
                if let Err(err) = child.kill().await {
                    error!(error = ?err, "Failed to SIGKILL Firecracker process: {err:#}");
                }
                return Err(Error::ApiSetup {
                    msg: "configuring logger".into(),
                    err,
                });
            }
            // Setup metrics FIFO
            if let Err(err) = fcc
                .put_metrics(::wick::models::Metrics {
                    metrics_path: metrics.to_path_buf(),
                })
                .await
            {
                error!(error = ?err, "Failed to configure metrics: {err:#}");
                if let Err(err) = child.kill().await {
                    error!(error = ?err, "Failed to SIGKILL Firecracker process: {err:#}");
                }
                return Err(Error::ApiSetup {
                    msg: "configuring metrics".into(),
                    err,
                });
            }

            (child, pidfd)
        }
    };

    let stdout_h = child
        .stdout
        .take()
        .map(|stdout| spawn_stdio_pump(vm_id.into(), "stdout", stdout))
        //.ok_or_else(|| todo!("some descriptive error?"))?;
        .expect("stdio streams should be available for consumption");
    let stderr_h = child
        .stderr
        .take()
        .map(|stderr| spawn_stdio_pump(vm_id.into(), "stderr", stderr))
        //.ok_or_else(|| todo!("some descriptive error?"))?;
        .expect("stdio streams should be available for consumption");

    // NOTE(ckatsak): Spawn the FIFO-reading tasks; there are only two valid alternatives:
    // [ ] if our reader tasks rely on EOF to exit (i.e., they read-only open the FIFO; i.e.,
    //     `pipe::OpenOptions::new().read_write(false)`), the writer (Firecracker) must have
    //     already opened its end (or our reader tasks will exit immediately)
    // [x] if our reader tasks do NOT rely on EOF to exit (i.e., they read-write open the FIFO;
    //     i.e., `pipe::OpenOptions::new().read_write(true)`), their loop must be structured in
    //     a way that they can be notified (channel? `::tokio_util::CancellationToken`?) when
    //     they have to break the loop to be reaped (when the process exits).
    let token = CancellationToken::new();
    let (log_path, mtr_path) = match init_files {
        InitFiles::Fifos { logging, metrics } => (Cow::Borrowed(logging), Cow::Borrowed(metrics)),
        InitFiles::Config(config_file_path) => {
            let parent = config_file_path
                .parent()
                .expect("this should exist and be uvm_dir_path");
            (
                Cow::Owned(parent.join(LOGGING_FIFO_BASENAME)),
                Cow::Owned(parent.join(METRICS_FIFO_BASENAME)),
            )
        }
    };
    let logging_h = spawn_fifo_pump(vm_id.into(), "logging", log_path, token.clone());
    let metrics_h = spawn_fifo_pump(vm_id.into(), "metrics", mtr_path, token.clone());

    Ok(FcProcessState {
        proc: child,
        pidfd,
        stdout_h,
        stderr_h,
        logging_h,
        metrics_h,
        cancel: token.drop_guard(),
    })
}

/// Ingest the data in the provided FIFO, pumping it into our configured logger.
///
/// This variation read-only opens the FIFO, stopping when EOF is returned.
/// Therefore, this should only be used _after_ the writer (i.e., the Firecracker
/// process in this case) has opened its own end; otherwise, this function returns
/// immediately because it immediately "reads" EOF.
///
/// Also see [`fifo_pump_until_cancelled`].
#[allow(dead_code)]
#[instrument(level = Level::TRACE, skip_all)]
async fn fifo_pump_until_eof(
    path: impl AsRef<Path>,
    stream_name: &'static str,
    id: &str,
) -> io::Result<()> {
    let fifo_rx = pipe::OpenOptions::new()
        //.read_write(false) // no need to clear this; read-only is the default
        .unchecked(true) // skip `stat(2)`
        .open_receiver(path)?;

    let mut lines = BufReader::new(fifo_rx).lines();

    while let Some(msg) = lines.next_line().await? {
        trace!(target: "fc.fifo", %id, stream = %stream_name, %msg);
    }

    Ok(())
}

/// Ingest the data in the provided FIFO, pumping it into our configured logger.
///
/// This variation allows opening the FIFO independently of its writer (i.e., the
/// Firecracker process in this case), and depending on the  [`CancellationToken`]
/// for stopping (rather than EOF).
///
/// # Notes
///
/// - See <https://tokio.rs/tokio/topics/shutdown> for [`CancellationToken`].
/// - See [`pipe::OpenOptions::read_write`] docs for the "resilient Receiver",
///   on which this function variation basically relies.
/// - Also see [`fifo_pump_until_eof`].
#[instrument(level = Level::TRACE, skip_all)]
async fn fifo_pump_until_cancelled(
    path: impl AsRef<Path>,
    stream_name: &'static str,
    id: &str,
    token: CancellationToken,
) -> io::Result<()> {
    let fifo_rx = pipe::OpenOptions::new()
        .read_write(true)
        .unchecked(true) // skip `stat(2)`
        .open_receiver(path)?;

    let mut lines = BufReader::new(fifo_rx).lines();

    loop {
        ::tokio::select! {
            _ = token.cancelled() => break,
            line = lines.next_line() => match line? {
                Some(msg) => trace!(target: "fc.fifo", %id, stream = %stream_name, %msg),
                None => {
                    // EOF? With `read_write(true)`, `None` does not mean that Firecracker just
                    // closed its end. Let's treat it as unexpected termination / broken invariant.
                    break
                },
            }
        }
    }

    Ok(())
}
