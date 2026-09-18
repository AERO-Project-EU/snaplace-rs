use std::{borrow::Cow, net::Ipv4Addr};

use serde::{Deserialize, Serialize};

use firecracker_containerd_client::Vm;

use crate::{
    metadata::SandboxStats,
    network::{TapDevice, TapInfo},
    snapman::MonitorTarget,
    worker::runtime::{Sandbox, SnapshotState},
    FunctionId,
};

/// Wrapper over a [`Vm`].
#[derive(Debug, Serialize, Deserialize)]
pub struct MicroVm {
    stats: SandboxStats,
    function_id: FunctionId,

    /// The [`TapDevice`] associated with this [`Vm`] (and this [`Vm`] only).
    pub(super) tap: TapDevice,
    //
    // TODO(ckatsak): Do we need to store the associated block device as well? If needed, can we?
    //
    pub(super) vm: Vm,
}

impl MicroVm {
    pub(crate) fn new(function_id: &FunctionId, tap: TapDevice, vm: Vm) -> Self {
        Self {
            stats: Default::default(),
            function_id: function_id.clone(),
            tap,
            vm,
        }
    }

    pub(super) fn with_stats(
        function_id: &FunctionId,
        tap: TapDevice,
        vm: Vm,
        stats: SandboxStats,
    ) -> Self {
        Self {
            stats,
            function_id: function_id.clone(),
            tap,
            vm,
        }
    }
}

impl Sandbox for MicroVm {
    type SnapshotState = MicroVmState;

    #[inline(always)]
    fn ip_addr(&self) -> Ipv4Addr {
        self.tap.ip_addr()
    }

    #[inline(always)]
    fn stats(&self) -> &SandboxStats {
        &self.stats
    }

    #[inline(always)]
    fn stats_mut(&mut self) -> &mut SandboxStats {
        &mut self.stats
    }

    #[inline(always)]
    fn function_id(&self) -> FunctionId {
        self.function_id.clone()
    }

    #[inline(always)]
    fn id(&self) -> &str {
        debug_assert_eq!(self.vm.id(), self.tap.name());
        self.tap.name()
    }

    #[inline]
    fn has_snapshot(&self) -> bool {
        self.vm.has_snapshot()
    }

    #[inline]
    fn monitor_targets(&self) -> Vec<MonitorTarget> {
        let pid = self.vm.pid().map(MonitorTarget::Pid);
        let cg = self
            .vm
            .cgroup_path()
            .filter(|path| !path.as_os_str().is_empty())
            .map(|path| MonitorTarget::Cgroup(path.to_path_buf()));

        [pid, cg].into_iter().flatten().collect()
    }

    #[inline]
    fn snapshot_state(
        &self,
    ) -> Option<Result<Self::SnapshotState, Box<dyn ::std::error::Error + Send + Sync + 'static>>>
    {
        Some(Ok(MicroVmState {
            stats: self.stats,
            //function_id: self.function_id.clone(),
            tap: self.tap.info().clone(),
            vm: self.vm.clone(),
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicroVmState {
    pub stats: SandboxStats, // NOTE(ckatsak): Include this? Or let it always be "per-boot"?
    //pub function_id: FunctionId, // Unnecessary since states in DB are key'ed by FunctionId anyway
    pub tap: TapInfo,
    pub vm: Vm,
    // FIXME(ckatsak): Do we need to store information about the associated block device as well
    // here?
}

impl PartialEq for MicroVmState {
    fn eq(&self, other: &Self) -> bool {
        // NOTE(ckatsak): Skip `SandboxStats` for comparison: it's a runtime thing updated
        //                on every invocation, and thus does not really determine equality.
        self.tap == other.tap && self.vm == other.vm
    }
}
impl Eq for MicroVmState {}

impl SnapshotState for MicroVmState {
    #[inline]
    fn id(&self) -> Cow<'_, str> {
        debug_assert_eq!(self.vm.id(), self.tap.name.as_str());
        Cow::Borrowed(self.tap.name.as_str())
    }
}
