use std::{path::Path, time::Duration};

use protobuf::MessageField;
use tracing::{instrument, Level};

use firecracker_containerd_ttrpc::{
    firecracker::CreateVMRequest,
    types::{FirecrackerMachineConfiguration, FirecrackerNetworkInterface, FirecrackerRootDrive},
};

use crate::{Client, Error, Result, Vm};

#[derive(Debug, Clone)]
pub struct Builder {
    vm_id: String,
    vcpu_count: u32,
    mem_size_mib: u32,
    kernel_img_path: Option<String>,
    kernel_args: Option<String>,
    root_drive: Option<FirecrackerRootDrive>,
    net_ifaces: Vec<FirecrackerNetworkInterface>,
    ctr_count: i32,
    exit_after_all_tasks_deleted: bool,
}

impl Builder {
    /// Default number of a MicroVM's vCPUs, unless specified otherwise.
    pub const DEFAULT_VCPU_COUNT: u32 = 1;

    /// Default MicroVM's guest phisical memory size, in MiB, unless specified otherwise.
    pub const DEFAULT_MEM_SIZE_MIB: u32 = 128;

    /// Default command line arguments for the guest kernel.
    pub const DEFAULT_KERNEL_ARGS: &'static str = "i8042.nokbd i8042.noaux 8250.nr_uarts=0 ipv6.disable=1 noapic reboot=k panic=1 pci=off nomodules ro systemd.unified_cgroup_hierarchy=0 systemd.journald.forward_to_console systemd.unit=firecracker.target init=/sbin/overlay-init";

    pub fn new(vm_id: impl Into<String>) -> Self {
        Self {
            vm_id: vm_id.into(),
            vcpu_count: Self::DEFAULT_VCPU_COUNT,
            mem_size_mib: Self::DEFAULT_MEM_SIZE_MIB,
            kernel_img_path: None,
            kernel_args: None,
            root_drive: None,
            net_ifaces: Vec::new(),
            ctr_count: 1,
            exit_after_all_tasks_deleted: true,
        }
    }

    /// Number of vCPUs of the new [`Vm`].
    ///
    /// This is optional.
    /// If not set, [`Self::DEFAULT_VCPU_COUNT`] will be used.
    #[inline]
    pub fn vcpus(mut self, vcpu_count: u32) -> Self {
        debug_assert!(vcpu_count > 0);
        self.vcpu_count = vcpu_count;
        self
    }

    /// Guest physical memory size for the new [`Vm`], in `MiB`s.
    ///
    /// This is optional.
    /// If not set, [`Self::DEFAULT_MEM_SIZE_MIB`] will be used.
    #[inline]
    pub fn memory_mib(mut self, mem_size_mib: u32) -> Self {
        debug_assert!(mem_size_mib > 0);
        self.mem_size_mib = mem_size_mib;
        self
    }

    pub fn kernel_image_path(mut self, path: impl AsRef<Path>) -> Result<Self> {
        self.kernel_img_path = Some(
            path.as_ref()
                .to_str()
                .ok_or_else(|| Error::Utf(path.as_ref().to_string_lossy().into()))?
                .to_string(),
        );
        Ok(self)
    }

    /// Kernel command-line arguments.
    ///
    /// This is optional.
    /// If not set, [`Self::DEFAULT_KERNEL_ARGS`] will be used.
    #[inline]
    pub fn kernel_args(mut self, cmd_line_args: impl Into<String>) -> Self {
        self.kernel_args = Some(cmd_line_args.into());
        self
    }

    /// Set the root drive for the new [`Vm`].
    ///
    /// This is optional.
    /// If not set, the default rootfs image in `firecracker-containerd`'s
    /// configuration will be used.
    pub fn root_drive(mut self, path: impl AsRef<Path>, read_only: bool) -> Result<Self> {
        self.root_drive = Some(FirecrackerRootDrive {
            HostPath: path
                .as_ref()
                .to_str()
                .ok_or_else(|| Error::Utf(path.as_ref().to_string_lossy().into()))?
                .to_string(),
            IsWritable: !read_only,
            ..Default::default()
        });
        Ok(self)
    }

    #[inline]
    pub fn network_interface(mut self, iface: FirecrackerNetworkInterface) -> Self {
        self.net_ifaces.push(iface);
        self
    }

    #[inline]
    pub fn container_count(mut self, container_count: usize) -> Self {
        debug_assert!(container_count < i32::MAX as usize);
        self.ctr_count = container_count as i32;
        self
    }

    #[inline]
    pub fn exit_after_all_tasks_deleted(mut self, exit: bool) -> Self {
        self.exit_after_all_tasks_deleted = exit;
        self
    }

    #[instrument(level = Level::TRACE, skip(self, client))]
    #[inline]
    pub async fn create(self, client: &Client, timeout: Duration) -> Result<Vm> {
        client
            .create_vm(CreateVMRequest {
                VMID: self.vm_id,
                KernelImagePath: self.kernel_img_path.unwrap_or_default(),
                KernelArgs: self
                    .kernel_args
                    .unwrap_or_else(|| Self::DEFAULT_KERNEL_ARGS.to_string()),
                RootDrive: self.root_drive.map(MessageField::some).unwrap_or_default(),
                NetworkInterfaces: self.net_ifaces,
                ContainerCount: self.ctr_count,
                ExitAfterAllTasksDeleted: self.exit_after_all_tasks_deleted,
                TimeoutSeconds: timeout.as_secs() as u32,
                MachineCfg: MessageField::some(FirecrackerMachineConfiguration {
                    MemSizeMib: self.mem_size_mib,
                    VcpuCount: self.vcpu_count,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
    }
}
