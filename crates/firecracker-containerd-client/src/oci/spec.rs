use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    path::Path,
    sync::OnceLock,
};

use aho_corasick::AhoCorasick;
use const_format::concatcp;
use oci_spec::runtime::{
    Capabilities, Capability, LinuxBuilder, LinuxCapabilitiesBuilder, LinuxDeviceBuilder,
    LinuxDeviceCgroupBuilder, LinuxDeviceType, LinuxNamespaceBuilder, LinuxNamespaceType,
    LinuxResourcesBuilder, MountBuilder, PosixRlimitBuilder, PosixRlimitType, ProcessBuilder,
    RootBuilder, Spec, SpecBuilder, UserBuilder,
};
use tracing::{instrument, Level};

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub enum RuntimeSpecSource<'s, 'ns, 'id, P: AsRef<Path> + Debug> {
    /// Use an existing OCI Runtime [`Spec`].
    Spec(&'s Spec),

    /// Use the OCI Runtime [`Spec`] found in the template file at `path`, after applying the
    /// specified `namespace` and `id`.
    ///
    /// # Note
    ///
    /// If `.process.args` is already populated in the JSON file at the given `path`, the value
    /// provided in `process_args` will be ignored (no replacement will take place at all).
    File {
        path: P,
        process_args: &'s str,
        namespace: &'ns str,
        id: &'id str,
    },

    /// Construct a new OCI Runtime [`Spec`] using the specified `namespace` and `id`.
    Builder {
        process_args: &'s str,
        namespace: &'ns str,
        id: &'id str,
    },

    /// Use the OCI Runtime [`Spec`] template ([`OCI_RT_SPEC_TEMPLATE`]), after applying the
    /// specified `namespace` and `id`.
    Template {
        process_args: &'s str,
        namespace: &'ns str,
        id: &'id str,
    },
}

/// TODO: doc
pub const OCI_RT_SPEC_TEMPLATE: &str =
    include_str!("../../artifacts/spec_templates/oci_rt_spec_tmpl.json.in");

static AUTOMATON: OnceLock<AhoCorasick> = OnceLock::new();

/// Populate OCI Runtime Spec's `.process.args` with a single-element JSON array with this string
/// value (i.e., <code>.process.args = [PROCESS_ARGS_PLACEHOLDER]</code>) to enable replacing it
/// at runtime.
///
/// # Note
///
/// For now, this replacement takes place as simple text substitution, so the exact format
/// specified above is assumed.
pub const PROCESS_ARGS_PLACEHOLDER: &str = "$SNAPLACE_PROCESS_ARGS";
const PROCESS_ARGS_PATTERN: &str = concatcp!("[\"", PROCESS_ARGS_PLACEHOLDER, "\"]");

pub const CONTAINERD_NAMESPACE_PATTERN: &str = "$SNAPLACE_CONTAINERD_NAMESPACE";
pub const VMID_PATTERN: &str = "$SNAPLACE_VMID";

impl<'s, 'ns, 'id, P: AsRef<Path> + Debug> RuntimeSpecSource<'s, 'ns, 'id, P> {
    #[instrument(level = Level::TRACE)]
    pub async fn into_json(self) -> Result<Vec<u8>> {
        fn fill_template(
            haystack: &str,
            process_args: &str,
            namespace: &str,
            id: &str,
        ) -> Result<Vec<u8>> {
            Ok(AUTOMATON
                .get_or_init(|| {
                    AhoCorasick::new([
                        PROCESS_ARGS_PATTERN,
                        CONTAINERD_NAMESPACE_PATTERN,
                        VMID_PATTERN,
                    ])
                    .expect("aho-corasick automaton: default constructor")
                })
                .replace_all(
                    haystack,
                    &[
                        ::serde_json::to_string(
                            process_args.split(' ').collect::<Vec<_>>().as_slice(),
                        )
                        .map_err(|err| Error::Json {
                            msg: format!("failed to serialize {process_args:?}").into_boxed_str(),
                            source: err,
                        })?
                        .as_str(),
                        namespace,
                        id,
                    ],
                )
                .into_bytes())
        }

        match self {
            RuntimeSpecSource::Spec(spec) => {
                ::serde_json::to_vec(spec).map_err(|err| Error::Json {
                    msg: String::from("failed to serialize OCI spec").into_boxed_str(),
                    source: err,
                })
            }
            RuntimeSpecSource::File {
                path,
                process_args,
                namespace,
                id,
            } => fill_template(
                &::tokio::fs::read_to_string(&path)
                    .await
                    .map_err(|err| Error::Io {
                        msg: format!("failed to read Spec file '{}'", path.as_ref().display()),
                        source: err,
                    })?,
                process_args,
                namespace,
                id,
            ),
            RuntimeSpecSource::Builder {
                process_args,
                namespace,
                id,
            } => ::serde_json::to_vec(&generate_fcctrd_spec(process_args, namespace, id)).map_err(
                |err| Error::Json {
                    msg: String::from("failed to serialize OCI spec").into_boxed_str(),
                    source: err,
                },
            ),
            RuntimeSpecSource::Template {
                process_args,
                namespace,
                id,
            } => fill_template(OCI_RT_SPEC_TEMPLATE, process_args, namespace, id),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// Auxiliary Constants
//
///////////////////////////////////////////////////////////////////////////////////////////////////

const OCI_VERSION: &str = "1.0.2-dev";
const VMID_KEY: &str = "aws.firecracker.vm.id";
const DEFAULT_CAPS: [Capability; 14] = [
    Capability::Chown,
    Capability::DacOverride,
    Capability::Fsetid,
    Capability::Fowner,
    Capability::Mknod,
    Capability::NetRaw,
    Capability::Setgid,
    Capability::Setuid,
    Capability::Setfcap,
    Capability::Setpcap,
    Capability::NetBindService,
    Capability::SysChroot,
    Capability::Kill,
    Capability::AuditWrite,
];

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// Auxiliary Functions
//
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Annotate [`Spec`] with a VMID as expected by firecracker-containerd.
pub fn set_vmid(spec: &mut Spec, vm_id: &str) {
    let mut new_annotations = if let Some(annotations) = spec.annotations().as_ref() {
        annotations.clone()
    } else {
        HashMap::with_capacity(1)
    };
    let _ = new_annotations.insert(VMID_KEY.into(), vm_id.into());
    let _ = spec.set_annotations(Some(new_annotations));
}

/// Set container's [`Spec`] to use host's (i.e., VM's) `net` and `uts` namespaces.
///
/// # Panics
///
/// If the provided [`Spec`] is not already initialized.
///
/// # Note
///
/// Mind that a Runtime [`Spec`] cannot set [`hostname`][1] (nor [`domainname`][2], which is
/// present only in later OCI Runtime spec versions) without a **private** `uts` namespace (see
/// runc [v1.0.0][3], or some [more recent commit][4]).
///
/// [1]: https://github.com/opencontainers/runtime-spec/blob/v1.0.2/config.md#hostname
/// [2]: https://github.com/opencontainers/runtime-spec/blob/v1.1.0-rc.3/config.md#domainname
/// [3]: https://github.com/opencontainers/runc/blob/v1.0.0/libcontainer/configs/validate/validator.go#L93-L95
/// [4]: https://github.com/opencontainers/runc/blob/b338accc78a38550d193e588ccbdc9bcbdf234cf/libcontainer/configs/validate/validator.go#L69-L77
pub fn set_vm_network(spec: &mut Spec) {
    let linux = spec.linux().as_ref().expect(".linux should not be None");
    let namespaces = linux
        .namespaces()
        .as_ref()
        .expect(".linux.namespaces should not be None");
    let new_namespaces = namespaces
        .clone()
        .into_iter()
        .filter(|ns| {
            !matches!(
                ns.typ(),
                LinuxNamespaceType::Network | LinuxNamespaceType::Uts
            )
        })
        .collect();
    let mut new_linux = linux.clone();
    let _ = new_linux.set_namespaces(Some(new_namespaces));
    let _ = spec.set_linux(Some(new_linux));

    let mounts = spec.mounts().as_ref().expect(".mounts should not be None");
    let mut new_mounts = mounts.clone();
    new_mounts.extend([
        MountBuilder::default()
            .destination("/etc/resolv.conf")
            .typ("bind")
            .source("/etc/resolv.conf")
            .options(["rbind".into(), "ro".into()])
            .build()
            .expect("failed to build OCI Runtime Spec's .mount[/etc/resolv.conf]"),
        MountBuilder::default()
            .destination("/etc/hosts")
            .typ("bind")
            .source("/etc/hosts")
            .options(["rbind".into(), "ro".into()])
            .build()
            .expect("failed to build OCI Runtime Spec's .mount[/etc/hosts]"),
    ]);
    let _ = spec.set_mounts(Some(new_mounts));
}

/// The assumed major device number in the guest for KVM's paravirtualized ptp clock.
pub const PTP_KVM_CHARDEV_MAJOR: u32 = 253;
/// The assumed major device number in the guest for KVM's paravirtualized ptp clock.
pub const PTP_KVM_CHARDEV_MINOR: u32 = 0;

/// Configure container's [`Spec`] to include access to guest's (i.e., container's host) KVM's ptp
/// clock, as well as permissions to set the System clock based on it.
///
/// # Panics
///
/// If the provided [`Spec`] is not already initialized (specifically, if `.linux` is absent).
///
/// # Warning
///
/// For now, it is assumed that clock's character device is present in the guest at `"/dev/ptp0"`,
/// with device numbers <code>[PTP_KVM_CHARDEV_MAJOR]:[PTP_KVM_CHARDEV_MINOR]</code>.
/// **FIXME: This should be configurable at runtime.**
pub fn allow_ptp_kvm_sync(spec: &mut Spec) {
    let linux = spec.linux().as_ref().expect(".linux should not be None");

    let new_device = LinuxDeviceBuilder::default()
        .path("/dev/ptp0")
        .typ(LinuxDeviceType::C)
        .major(PTP_KVM_CHARDEV_MAJOR)
        .minor(PTP_KVM_CHARDEV_MINOR)
        .file_mode(444u32) // yes, it's really supposed to be given as decimal
        .uid(0u32)
        .gid(0u32)
        .build()
        .expect("failed to build OCI Runtime Spec's .linux.devices[ptp_kvm]");
    // Create or extend `.linux.devices`
    let new_devices = if let Some(devices) = linux.devices().as_ref() {
        let mut new_devices = devices.clone();
        new_devices.push(new_device);
        new_devices
    } else {
        vec![new_device]
    };

    let new_dev_allow = LinuxDeviceCgroupBuilder::default()
        .allow(true)
        .typ(LinuxDeviceType::C)
        .major(PTP_KVM_CHARDEV_MAJOR)
        .minor(PTP_KVM_CHARDEV_MINOR)
        .access("rwm")
        .build()
        .expect("failed to build OCI Runtime Spec's .linux.resources.devices[ptp_kvm]");
    // Create or extend `.linux.resources`
    let new_resources = if let Some(resources) = linux.resources().as_ref() {
        // Create or extend `.linux.resources.devices`
        let new_dev_allowz = if let Some(dev_allowz) = resources.devices().as_ref() {
            let mut new_dev_allowz = dev_allowz.clone();
            new_dev_allowz.push(new_dev_allow);
            new_dev_allowz
        } else {
            vec![new_dev_allow]
        };

        let mut new_resources = resources.clone();
        let _ = new_resources.set_devices(Some(new_dev_allowz));
        new_resources
    } else {
        LinuxResourcesBuilder::default()
            .devices([new_dev_allow])
            .build()
            .expect("failed to build OCI Runtime Spec's .linux.resources")
    };

    // Set updated .linux
    let mut new_linux = linux.clone();
    let _ = new_linux.set_devices(Some(new_devices));
    let _ = new_linux.set_resources(Some(new_resources));
    let _ = spec.set_linux(Some(new_linux));

    let process = spec.process().as_ref().expect("TODO");
    // Create or extend `.process.capabilities`
    let new_caps = if let Some(caps) = process.capabilities().as_ref() {
        let add_cap_sys_time = |caps: &Option<Capabilities>| {
            let mut new_caps = caps
                .as_ref()
                .cloned()
                .unwrap_or_else(|| HashSet::with_capacity(1));
            new_caps.insert(Capability::SysTime);
            new_caps
        };
        let new_bounding = add_cap_sys_time(caps.bounding());
        let new_effective = add_cap_sys_time(caps.effective());
        let new_permitted = add_cap_sys_time(caps.permitted());
        LinuxCapabilitiesBuilder::default()
            .bounding(new_bounding)
            .effective(new_effective)
            .permitted(new_permitted)
            .build()
            .expect("failed to build OCI Runtime Spec's .process.capabilities")
    } else {
        LinuxCapabilitiesBuilder::default()
            .bounding(HashSet::from([Capability::SysTime]))
            .effective(HashSet::from([Capability::SysTime]))
            .permitted(HashSet::from([Capability::SysTime]))
            .build()
            .expect("failed to build OCI Runtime Spec's .process.capabilities")
    };
    // Set updated .process
    let mut new_process = process.clone();
    let _ = new_process.set_capabilities(Some(new_caps));
    let _ = spec.set_process(Some(new_process));
}

/// TODO: doc
///
/// # Arguments
///
/// - `process_args` is the command to execute upon creating the container (similar to Docker's
///   `entrypoint`/`cmd`, but already merged/resolved)
/// - `ns` is the containerd namespace
/// - `id` is containerd's `Container.ID` (which is probably just `VMID` for us)
///
/// # Panics
///
/// Panicking is an internal BUG; callers should never be able to trigger a panic.
///
/// # Notes
///
/// ```rust
/// use firecracker_containerd_client::oci::spec::{
///     allow_ptp_kvm_sync, generate_default_unix_spec, generate_fcctrd_spec, set_vmid,
///     set_vm_network,
/// };
/// # let process_args = "TEST_ENTRYPOINT --with commands";
/// # let namespace = "TEST_NAMESPACE";
/// # let id = "TEST_ID";
///
/// let mut spec1 = generate_default_unix_spec(process_args, namespace, id);
/// set_vm_network(&mut spec1);
/// set_vmid(&mut spec1, id);
/// allow_ptp_kvm_sync(&mut spec1);
///
/// let spec2 = generate_fcctrd_spec(process_args, namespace, id);
///
/// assert_eq!(spec1, spec2);
/// ```
pub fn generate_fcctrd_spec(process_args: &str, namespace: &str, id: &str) -> Spec {
    let mut caps = HashSet::with_capacity(DEFAULT_CAPS.len() + 1);
    caps.extend(&DEFAULT_CAPS);
    caps.insert(Capability::SysTime);

    SpecBuilder::default()
        .version(OCI_VERSION)
        .root(
            RootBuilder::default()
                .path("rootfs")
                .readonly(false) // FIXME(ckatsak): should be `false`, right?
                .build()
                .expect("failed to build OCI Runtime Spec's .root"),
        )
        .hostname(String::new())
        .annotations([(VMID_KEY.to_owned(), id.to_owned())])
        .process(
            ProcessBuilder::default()
                .cwd("/")
                .no_new_privileges(true)
                .env(["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".into()])
                .args(
                    process_args
                        .split(' ')
                        .map(|s| s.to_owned())
                        .collect::<Vec<_>>(),
                )
                .user(
                    UserBuilder::default()
                        .uid(0u32)
                        .gid(0u32)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .process.user"),
                )
                .capabilities(
                    LinuxCapabilitiesBuilder::default()
                        .bounding(caps.clone())
                        .effective(caps.clone())
                        .permitted(caps)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .process.capabilities"),
                )
                .rlimits([PosixRlimitBuilder::default()
                    .typ(PosixRlimitType::RlimitNofile)
                    .hard(1024u64)
                    .soft(1024u64)
                    .build()
                    .expect("failed to build OCI Runtime Spec's .process.rlimits")])
                .build()
                .expect("failed to build OCI Runtime Spec's .process"),
        )
        .linux(
            LinuxBuilder::default()
                .masked_paths([
                    "/proc/acpi".into(),
                    "/proc/asound".into(),
                    "/proc/kcore".into(),
                    "/proc/keys".into(),
                    "/proc/latency_stats".into(),
                    "/proc/timer_list".into(),
                    "/proc/timer_stats".into(),
                    "/proc/sched_debug".into(),
                    "/sys/firmware".into(),
                    "/proc/scsi".into(),
                ])
                .readonly_paths([
                    "/proc/bus".into(),
                    "/proc/fs".into(),
                    "/proc/irq".into(),
                    "/proc/sys".into(),
                    "/proc/sysrq-trigger".into(),
                ])
                .cgroups_path(Path::new("/").join(namespace).join(id))
                .devices([LinuxDeviceBuilder::default()
                    .path("/dev/ptp0")
                    .typ(LinuxDeviceType::C)
                    .major(PTP_KVM_CHARDEV_MAJOR)
                    .minor(PTP_KVM_CHARDEV_MINOR)
                    .file_mode(444u32) // yes, it's really supposed to be given as decimal
                    .uid(0u32)
                    .gid(0u32)
                    .build()
                    .expect("failed to build OCI Runtime Spec's .linux.devices[ptp_kvm]")])
                .resources(
                    LinuxResourcesBuilder::default()
                        .devices([
                            LinuxDeviceCgroupBuilder::default()
                                .allow(false)
                                .access("rwm")
                                .build()
                                .expect(
                                    "failed to build OCI Runtime Spec's .linux.resources.devices[*]",
                                ),
                            LinuxDeviceCgroupBuilder::default()
                                .allow(true)
                                .typ(LinuxDeviceType::C)
                                .major(PTP_KVM_CHARDEV_MAJOR)
                                .minor(PTP_KVM_CHARDEV_MINOR)
                                .access("rwm")
                                .build()
                                .expect(
                                    "failed to build OCI Runtime Spec's .linux.resources.devices[ptp_kvm]",
                                ),
                        ])
                        .build()
                        .expect("failed to build OCI Runtime Spec's .linux.resources"),
                )
                .namespaces([
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Pid)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .linux.namespaces[pid]"),
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Ipc)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .linux.namespaces[ipc]"),
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Mount)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .linux.namespaces[mount]"),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .linux"),
        )
        .mounts([
            MountBuilder::default()
                .destination("/proc")
                .typ("proc")
                .source("proc")
                .options(["nosuid".into(), "noexec".into(), "nodev".into()])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/proc]"),
            MountBuilder::default()
                .destination("/dev")
                .typ("tmpfs")
                .source("tmpfs")
                .options([
                    "nosuid".into(),
                    "strictatime".into(),
                    "mode=755".into(),
                    "size=65536k".into(),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/dev]"),
            MountBuilder::default()
                .destination("/dev/pts")
                .typ("devpts")
                .source("devpts")
                .options([
                    "nosuid".into(),
                    "noexec".into(),
                    "newinstance".into(),
                    "ptmxmode=0666".into(),
                    "mode=0620".into(),
                    "gid=5".into(),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/dev/pts]"),
            MountBuilder::default()
                .destination("/dev/shm")
                .typ("tmpfs")
                .source("shm")
                .options([
                    "nosuid".into(),
                    "noexec".into(),
                    "nodev".into(),
                    "mode=1777".into(),
                    "size=65536k".into(),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/dev/shm]"),
            MountBuilder::default()
                .destination("/dev/mqueue")
                .typ("mqueue")
                .source("mqueue")
                .options(["nosuid".into(), "noexec".into(), "nodev".into()])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/dev/mqueue]"),
            MountBuilder::default()
                .destination("/sys")
                .typ("sysfs")
                .source("sysfs")
                .options([
                    "nosuid".into(),
                    "noexec".into(),
                    "nodev".into(),
                    "ro".into(),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/sys]"),
            MountBuilder::default()
                .destination("/run")
                .typ("tmpfs")
                .source("tmpfs")
                .options([
                    "nosuid".into(),
                    "strictatime".into(),
                    "mode=755".into(),
                    "size=65536k".into(),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/run]"),
            MountBuilder::default()
                .destination("/etc/resolv.conf")
                .typ("bind")
                .source("/etc/resolv.conf")
                .options(["rbind".into(), "ro".into()])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/etc/resolv.conf]"),
            MountBuilder::default()
                .destination("/etc/hosts")
                .typ("bind")
                .source("/etc/hosts")
                .options(["rbind".into(), "ro".into()])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/etc/hosts]"),
        ])
        .build()
        .expect("failed to build OCI Runtime Spec")
}

/// TODO: doc
///
/// # Arguments
///
/// - `process_args` is the command to execute upon creating the container (similar to Docker's
///   `entrypoint`/`cmd`, but already merged/resolved)
/// - `ns` is the containerd namespace
/// - `id` is containerd's `Container.ID` (which is probably just `VMID` for us)
///
/// # Panics
///
/// Panicking is an internal BUG; callers should never be able to trigger a panic.
pub fn generate_default_unix_spec(process_args: &str, namespace: &str, id: &str) -> Spec {
    SpecBuilder::default()
        .version(OCI_VERSION)
        .root(
            RootBuilder::default()
                .path("rootfs")
                .readonly(false) // FIXME(ckatsak): should be `false`, right?
                .build()
                .expect("failed to build OCI Runtime Spec's .root"),
        )
        .hostname(String::new())
        .process(
            ProcessBuilder::default()
                .cwd("/")
                .no_new_privileges(true)
                .env(["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".into()])
                .args(
                    process_args
                        .split(' ')
                        .map(|s| s.to_owned())
                        .collect::<Vec<_>>(),
                )
                .user(
                    UserBuilder::default()
                        .uid(0u32)
                        .gid(0u32)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .process.user"),
                )
                .capabilities(
                    LinuxCapabilitiesBuilder::default()
                        .bounding(DEFAULT_CAPS)
                        .effective(DEFAULT_CAPS)
                        .permitted(DEFAULT_CAPS)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .process.capabilities"),
                )
                .rlimits([PosixRlimitBuilder::default()
                    .typ(PosixRlimitType::RlimitNofile)
                    .hard(1024u64)
                    .soft(1024u64)
                    .build()
                    .expect("failed to build OCI Runtime Spec's .process.rlimits")])
                .build()
                .expect("failed to build OCI Runtime Spec's .process"),
        )
        .linux(
            LinuxBuilder::default()
                .masked_paths([
                    "/proc/acpi".into(),
                    "/proc/asound".into(),
                    "/proc/kcore".into(),
                    "/proc/keys".into(),
                    "/proc/latency_stats".into(),
                    "/proc/timer_list".into(),
                    "/proc/timer_stats".into(),
                    "/proc/sched_debug".into(),
                    "/sys/firmware".into(),
                    "/proc/scsi".into(),
                ])
                .readonly_paths([
                    "/proc/bus".into(),
                    "/proc/fs".into(),
                    "/proc/irq".into(),
                    "/proc/sys".into(),
                    "/proc/sysrq-trigger".into(),
                ])
                .cgroups_path(Path::new("/").join(namespace).join(id))
                .resources(
                    LinuxResourcesBuilder::default()
                        .devices([LinuxDeviceCgroupBuilder::default()
                            .allow(false)
                            .access("rwm")
                            .build()
                            .expect(
                                "failed to build OCI Runtime Spec's .linux.resources.devices[*]",
                            )])
                        .build()
                        .expect("failed to build OCI Runtime Spec's .linux.resources"),
                )
                .namespaces([
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Pid)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .linux.namespaces[pid]"),
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Ipc)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .linux.namespaces[ipc]"),
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Uts)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .linux.namespaces[uts]"),
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Mount)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .linux.namespaces[mount]"),
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Network)
                        .build()
                        .expect("failed to build OCI Runtime Spec's .linux.namespaces[net]"),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .linux"),
        )
        .mounts([
            MountBuilder::default()
                .destination("/proc")
                .typ("proc")
                .source("proc")
                .options(["nosuid".into(), "noexec".into(), "nodev".into()])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/proc]"),
            MountBuilder::default()
                .destination("/dev")
                .typ("tmpfs")
                .source("tmpfs")
                .options([
                    "nosuid".into(),
                    "strictatime".into(),
                    "mode=755".into(),
                    "size=65536k".into(),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/dev]"),
            MountBuilder::default()
                .destination("/dev/pts")
                .typ("devpts")
                .source("devpts")
                .options([
                    "nosuid".into(),
                    "noexec".into(),
                    "newinstance".into(),
                    "ptmxmode=0666".into(),
                    "mode=0620".into(),
                    "gid=5".into(),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/dev/pts]"),
            MountBuilder::default()
                .destination("/dev/shm")
                .typ("tmpfs")
                .source("shm")
                .options([
                    "nosuid".into(),
                    "noexec".into(),
                    "nodev".into(),
                    "mode=1777".into(),
                    "size=65536k".into(),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/dev/shm]"),
            MountBuilder::default()
                .destination("/dev/mqueue")
                .typ("mqueue")
                .source("mqueue")
                .options(["nosuid".into(), "noexec".into(), "nodev".into()])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/dev/mqueue]"),
            MountBuilder::default()
                .destination("/sys")
                .typ("sysfs")
                .source("sysfs")
                .options([
                    "nosuid".into(),
                    "noexec".into(),
                    "nodev".into(),
                    "ro".into(),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/sys]"),
            MountBuilder::default()
                .destination("/run")
                .typ("tmpfs")
                .source("tmpfs")
                .options([
                    "nosuid".into(),
                    "strictatime".into(),
                    "mode=755".into(),
                    "size=65536k".into(),
                ])
                .build()
                .expect("failed to build OCI Runtime Spec's .mount[/run]"),
        ])
        .build()
        .expect("failed to build OCI Runtime Spec")
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::BufWriter};

    use anyhow::Context;
    use oci_spec::runtime::Spec;
    use tokio::time::Instant;

    use crate::oci::{
        spec::{
            generate_fcctrd_spec, CONTAINERD_NAMESPACE_PATTERN, PROCESS_ARGS_PLACEHOLDER,
            VMID_PATTERN,
        },
        RuntimeSpecSource,
    };

    #[::tokio::test]
    async fn spec_source_file() {
        let _json = RuntimeSpecSource::File {
            path: "artifacts/spec_templates/tests/nginx:1.25.0.json",
            process_args: "test-entrypoint --with commands",
            namespace: "test-ns",
            id: "test-id",
        }
        .into_json()
        .await
        .expect("failed to convert RuntimeSpecSource to JSON");
    }

    /// Generate the OCI runtime spec template with:
    ///     $ cargo t tests::export_json_fcctrd_spec -- --nocapture --ignored
    #[test]
    #[ignore = "produces artifacts/spec_templates/oci_rt_spec_tmpl.json.in"]
    fn export_json_fcctrd_spec() -> ::anyhow::Result<()> {
        const BASE_DIR: &str = "/tmp";

        let now = ::humantime::Timestamp::from(::std::time::SystemTime::now());

        // Using spec.save()
        let spec = generate_fcctrd_spec(
            PROCESS_ARGS_PLACEHOLDER,
            CONTAINERD_NAMESPACE_PATTERN,
            VMID_PATTERN,
        );
        spec.save(format!("{BASE_DIR}/oci_rt_spec_saved_{now}.json"))
            .context("failed to save OCI Runtime Spec's JSON file")?;

        // Using serde_json and BufWriter
        let bw = BufWriter::with_capacity(
            1 << 14,
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(format!("{BASE_DIR}/oci_rt_spec_serd_{now}.json"))
                .context("failed to open new file for writing")?,
        );
        ::serde_json::to_writer(bw, &spec)
            .context("failed to serialize and export OCI Runtime Spec")
    }

    #[test]
    #[ignore = "throwaway for testing out aho-corasick"]
    fn ac01() -> ::anyhow::Result<()> {
        let t0 = ::std::time::Instant::now();
        let ac = ::aho_corasick::AhoCorasick::new([
            PROCESS_ARGS_PLACEHOLDER,
            CONTAINERD_NAMESPACE_PATTERN,
            VMID_PATTERN,
        ])
        .context("failed to build AhoCorasick automaton")?;
        let t1 = ::std::time::Instant::now();
        eprintln!(
            "AhoCorasick automaton constructed in {}",
            ::humantime::format_duration(t0 - t1)
        );

        assert_eq!(ac.kind(), ::aho_corasick::AhoCorasickKind::DFA);
        assert!(ac.memory_usage() < ::fforget::page_size().expect("page size"));

        Ok(())
    }

    #[::tokio::test]
    async fn spec_source_template() -> ::anyhow::Result<()> {
        const TEST_PROCESS_ARGS: &str = "test-entrypoint --with commands";
        const TEST_NAMESPACE: &str = "test-namespace";
        const TEST_VMID: &str = "test-vmid";

        // Pre-warm the AUTOMATON to see the actual times later
        let _json_tmpl_prewarm = ac02(TEST_PROCESS_ARGS, TEST_NAMESPACE, TEST_VMID)
            .await
            .context("failed to pre-warm AUTOMATON")?;

        // JSON Spec produced by RuntimeSpecSource::Template
        let t_start = Instant::now();
        let json_tmpl = ac02(TEST_PROCESS_ARGS, TEST_NAMESPACE, TEST_VMID).await?;
        let t_tmpl = t_start.elapsed();
        eprintln!("T_tmpl = {}", ::humantime::format_duration(t_tmpl));
        let spec_tmpl = ::serde_json::from_slice(&json_tmpl)
            .context("failed to deserialize JSON (produced by Template) into Spec")?;

        // JSON Spec produced by RuntimeSpecSource::File
        const FILE_PATH: &str = "artifacts/spec_templates/oci_rt_spec_tmpl.json.in";
        let t_start = Instant::now();
        let json_file = RuntimeSpecSource::File {
            path: FILE_PATH,
            process_args: TEST_PROCESS_ARGS,
            namespace: TEST_NAMESPACE,
            id: TEST_VMID,
        }
        .into_json()
        .await
        .context("failed to convert RuntimeSpecSource to JSON")?;
        let t_file = t_start.elapsed();
        eprintln!("T_file = {}", ::humantime::format_duration(t_file));
        let spec_file = ::serde_json::from_slice(&json_file)
            .context("failed to deserialize JSON (produced by File) into Spec")?;

        assert_eq!(
            ::serde_json::from_slice::<Spec>(&json_tmpl)
                .context("failed to deserialize JSON (produced by Template) into Spec")?,
            ::serde_json::from_slice::<Spec>(&json_file)
                .context("failed to deserialize JSON (produced by File) into Spec")?,
            "_deserialized_ OCI Runtime Specs should be identical",
        );

        let t_start = Instant::now();
        let spec_gen = generate_fcctrd_spec(TEST_PROCESS_ARGS, TEST_NAMESPACE, TEST_VMID);
        let t_gen = t_start.elapsed();
        eprintln!("T_gen = {}", ::humantime::format_duration(t_gen));

        // For now, these three Specs should be identical, since: `gen`->`file`->`tmpl`
        assert_eq!(spec_gen, spec_tmpl);
        assert_eq!(spec_gen, spec_file);
        assert_eq!(spec_tmpl, spec_file);

        // NOTE: It is okay for `json_tmpl` and `json_file` not to be byte-for-byte identical
        // (e.g., capabilities can be unordered)
        //::tokio::fs::write("/tmp/tmpl.json", &json_tmpl)
        //    .await
        //    .context("failed to write template JSON bytes to file")?;
        //::tokio::fs::write("/tmp/file.json", &json_file)
        //    .await
        //    .context("failed to write file JSON bytes to file")?;
        //assert_eq!(
        //    json_tmpl, json_file,
        //    "produced JSON is not byte-for-byte identical"
        //);

        Ok(())
    }

    async fn ac02(process_args: &str, namespace: &str, id: &str) -> ::anyhow::Result<Vec<u8>> {
        let spec: RuntimeSpecSource<'_, '_, '_, &str> = RuntimeSpecSource::Template {
            process_args,
            namespace,
            id,
        };
        spec.into_json()
            .await
            .context("failed to convert RuntimeSpecSource to JSON")
    }
}
