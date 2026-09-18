use std::{borrow::Cow, fmt::Debug, net::Ipv4Addr};

use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    metadata::SandboxStats,
    network::{TapDevice, TapInfo},
    snapman::MonitorTarget,
    worker::{
        runtime::{fc::runtime::FcProcessState, SnapshotState},
        Sandbox,
    },
    FunctionId,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotFiles {
    state: Utf8PathBuf,
    memory: Utf8PathBuf,
}

impl SnapshotFiles {
    pub fn new(state: impl AsRef<Utf8Path>, memory: impl AsRef<Utf8Path>) -> Self {
        Self {
            state: state.as_ref().into(),
            memory: memory.as_ref().into(),
        }
    }

    #[inline]
    pub fn state(&self) -> &Utf8Path {
        self.state.as_path()
    }

    #[inline]
    pub fn memory(&self) -> &Utf8Path {
        self.memory.as_path()
    }
}

/// Runtime-side representation of a `rt-fc` microVM.
///
/// This bundles the sandbox's host networking resource, optional live process
/// state, and optional persisted snapshot metadata.
pub struct MicroVm {
    pub(super) stats: SandboxStats,
    pub(super) function_id: FunctionId,

    /// The [`TapDevice`] associated with this `FcMicroVm` (and this `FcMicroVm` only).
    pub(super) tap: TapDevice,

    /// Live Firecracker process state, present only while the sandbox is
    /// currently running on the host.
    pub(super) proc: Option<FcProcessState>,

    /// Persisted snapshot files for this sandbox, if a snapshot has been created.
    pub(super) snapshot: Option<SnapshotFiles>,
}

impl Debug for MicroVm {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("MicroVm")
            .field("id", &self.id())
            .field("function_id", &self.function_id.as_str())
            .field("proc", &self.proc)
            .field("tap", &self.tap)
            .field("snapshot", &self.snapshot)
            .finish()
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
        self.tap.name()
    }

    #[inline]
    fn has_snapshot(&self) -> bool {
        self.snapshot.is_some()
    }

    fn monitor_targets(&self) -> Vec<MonitorTarget> {
        self.proc
            .as_ref()
            .and_then(FcProcessState::pid)
            .map(|pid| MonitorTarget::Pid(pid as _)) // FIXME: this should be i32 in the first place
            .into_iter()
            .collect()
    }

    #[inline]
    fn snapshot_state(
        &self,
    ) -> Option<Result<Self::SnapshotState, Box<dyn ::std::error::Error + Send + Sync + 'static>>>
    {
        let Some(snapshot) = &self.snapshot else {
            warn!("Attempted to create SnapshotState for a non-snapshotted Sandbox!");
            return Some(Err(Box::new(super::Error::NoSnapshot(
                "snapshot state creation".into(),
            ))));
        };
        Some(Ok(MicroVmState {
            stats: self.stats,
            tap: self.tap.info().clone(),
            snapshot: snapshot.clone(),
        }))
    }
}

/// Serializable snapshot-restoration state for a (`rt-fc`) Firecracker uVM.
///
/// This is the portion of [`MicroVm`] that must survive process teardown so
/// the [`Sandbox`] can later be reinstated and restored from its persisted
/// snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicroVmState {
    pub stats: SandboxStats,
    pub tap: TapInfo,
    pub snapshot: SnapshotFiles,
}

impl PartialEq for MicroVmState {
    fn eq(&self, other: &Self) -> bool {
        // NOTE(ckatsak): Skip `SandboxStats` for comparison: it's a runtime thing updated
        //                on every invocation, and thus does not really determine equality.
        self.tap == other.tap && self.snapshot == other.snapshot // && ...? Is there any point in
                                                                 //       also comparing snapshots?
    }
}
impl Eq for MicroVmState {}

impl SnapshotState for MicroVmState {
    fn id(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.tap.name.as_str())
    }
}
