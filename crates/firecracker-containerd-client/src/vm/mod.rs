mod builder;
pub use builder::Builder;

use std::{
    fmt::Debug,
    path::{Path, PathBuf},
    time::Duration,
};

use compact_str::{CompactString, ToCompactString};

use firecracker_containerd_ttrpc::firecracker::CreateVMResponse;

use crate::{client::Client, error::Result, ContainerBuilder, Error, FlushData};

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct Vm {
    id: CompactString,
    namespace: CompactString,

    #[cfg_attr(feature = "serde", serde(skip))] // always (de)ser it as None; it's a runtime thing
    pid: Option<u64>,
    socket_path: Option<PathBuf>,
    log_fifo_path: Option<PathBuf>,
    metrics_path: Option<PathBuf>,
    cgroup_path: Option<PathBuf>,

    snapshot_state_file: Option<PathBuf>,
    snapshot_memory_file: Option<PathBuf>,
}

// NOTE(ckatsak): impl Drop ?
//
// No, let's consider `Vm` to be a pure data type, for now. The owner of a `Vm` is probably also
// expected to be the owner of a `Client`. She decides when VMs are created (or loaded from VM
// snapshots), and can store their metadata (i.e., the `Vm` type) across creations/deletions.
//
// Or maybe better: `Vm` should probably be composed into another type that should StopVM() when
// dropped. This parent type would be decoupled from the `Client`, so that connections won't have
// to be re-established.

impl Vm {
    /// Instantiates a new [`Vm`] based on the given [`CreateVMResponse`] and the associated
    /// firecracker-containerd namespace.
    #[inline]
    pub fn from_response(resp: CreateVMResponse, ns: impl ToCompactString) -> Self {
        Self {
            id: resp.VMID.to_compact_string(),
            namespace: ns.to_compact_string(),

            pid: (resp.PID != 0).then_some(resp.PID),
            socket_path: Some(resp.SocketPath.into()),
            log_fifo_path: Some(resp.LogFifoPath.into()),
            metrics_path: Some(resp.MetricsFifoPath.into()),
            cgroup_path: Some(resp.CgroupPath.into()),

            snapshot_state_file: None,
            snapshot_memory_file: None,
        }
    }

    #[inline(always)]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    #[inline(always)]
    pub fn pid(&self) -> Option<u64> {
        self.pid
    }

    #[inline(always)]
    pub fn socket_path(&self) -> Option<&Path> {
        self.socket_path.as_deref()
    }

    #[inline(always)]
    pub fn log_fifo_path(&self) -> Option<&Path> {
        self.log_fifo_path.as_deref()
    }

    #[inline(always)]
    pub fn metrics_path(&self) -> Option<&Path> {
        self.metrics_path.as_deref()
    }

    #[inline(always)]
    pub fn cgroup_path(&self) -> Option<&Path> {
        self.cgroup_path.as_deref()
    }

    #[inline]
    pub fn has_snapshot(&self) -> bool {
        debug_assert!(
            (self.snapshot_state_file.is_some() && self.snapshot_memory_file.is_some())
                || (self.snapshot_state_file.is_none() && self.snapshot_memory_file.is_none())
        );
        self.snapshot_state_file.is_some() && self.snapshot_memory_file.is_some()
    }

    #[inline(always)]
    pub fn snapshot_state_file(&self) -> Option<&Path> {
        self.snapshot_state_file.as_deref()
    }

    #[inline(always)]
    pub fn snapshot_memory_file(&self) -> Option<&Path> {
        self.snapshot_memory_file.as_deref()
    }

    /// Changes the filesystem path of stored state file that is part of the snapshot tracked
    /// by this `Vm`.
    ///
    /// Also see [`Vm::set_snapshot_memory_file`].
    ///
    /// # Errors
    ///
    /// [`Error::NoSnapshot`] if no snapshot at all is currently associated with this `Vm`.
    pub fn set_snapshot_state_file(&mut self, new_path: &impl AsRef<Path>) -> Result<()> {
        if !self.has_snapshot() {
            return Err(Error::NoSnapshot);
        }
        self.snapshot_state_file = Some(new_path.as_ref().to_path_buf());
        Ok(())
    }

    /// Changes the filesystem path of stored memory file that is part of the snapshot tracked
    /// by this `Vm`.
    ///
    /// Also see [`Vm::set_snapshot_state_file`].
    ///
    /// # Errors
    ///
    /// [`Error::NoSnapshot`] if no snapshot at all is currently associated with this `Vm`.
    pub fn set_snapshot_memory_file(&mut self, new_path: &impl AsRef<Path>) -> Result<()> {
        if !self.has_snapshot() {
            return Err(Error::NoSnapshot);
        }
        self.snapshot_memory_file = Some(new_path.as_ref().to_path_buf());
        Ok(())
    }

    #[inline]
    pub async fn pause(&self, client: &Client) -> Result<()> {
        client.pause_vm(self.id.as_str()).await
    }

    #[inline]
    pub async fn resume(&self, client: &Client) -> Result<()> {
        client.resume_vm(self.id.as_str()).await
    }

    /// Creates a new snapshot of this [`Vm`], persisted in files at the provided paths
    /// `state_file_path` and `memory_file_path`.
    ///
    /// Before contacting firecracker-containerd, it validates the paths by making sure that:
    /// - the directory hierarchy in the given paths exists
    /// - any stale snapshot files at the provided paths are `unlink(2)`ed
    ///
    /// If snapshot creation is successful, the given paths are stored in the `Vm` and accessible
    /// through [`Self::snapshot_state_file`] and [`Self::snapshot_memory_file`] for later use.
    /// (This is why exclusive access is needed. For an alternative, check
    /// [`Client::create_vm_snapshot`]).
    ///
    /// # Errors
    ///
    /// In case of any failure:
    /// - in validating the provided paths, or
    /// - reported by firecracker-containerd
    pub async fn create_snapshot<S, M>(
        &mut self,
        client: &Client,
        state_file_path: S,
        memory_file_path: M,
        flush_data: FlushData,
    ) -> Result<()>
    where
        S: AsRef<Path> + Debug,
        M: AsRef<Path> + Debug,
    {
        client
            .create_vm_snapshot(
                self.id.as_str(),
                &state_file_path,
                &memory_file_path,
                flush_data,
            )
            .await?;

        self.snapshot_state_file = Some(state_file_path.as_ref().to_owned());
        self.snapshot_memory_file = Some(memory_file_path.as_ref().to_owned());
        Ok(())
    }

    #[inline]
    pub async fn stop(&mut self, client: &Client, timeout: Duration) -> Result<()> {
        client.stop_vm(self.id.as_str(), timeout).await?;
        self.pid = None;
        Ok(())
    }

    #[inline]
    pub async fn load_from_snapshot(
        &mut self,
        client: &Client,
        resume: AfterSnapshotLoad,
    ) -> Result<()> {
        if !self.has_snapshot() {
            return Err(Error::NoSnapshot);
        }
        self.pid = client
            .load_vm_snapshot(
                self.id.as_str(),
                self.snapshot_state_file.as_ref().expect("already checked"),
                self.snapshot_memory_file.as_ref().expect("already checked"),
                resume,
            )
            .await?
            .and_then(|pid| (pid != 0).then_some(pid));
        Ok(())
    }

    #[inline]
    pub async fn unload(&mut self, client: &Client) -> Result<()> {
        client.unload_vm(self.id.as_str()).await?;
        self.pid = None;
        Ok(())
    }

    #[inline]
    pub fn clear_pid(&mut self) {
        self.pid = None
    }

    /// Initialize a [`ContainerBuilder`] with the given parameters, using this [`Vm`]'s ID as the
    /// [`Container`]'s ID.
    ///
    /// [`Container`]: containerd_client::services::v1::Container
    pub fn container_builder<I, S, P>(
        &self,
        image: I,
        snapshot_key: S,
    ) -> ContainerBuilder<'_, '_, '_, P>
    where
        I: Into<String> + Debug,
        S: Into<String> + Debug,
        P: AsRef<Path> + Debug,
    {
        ContainerBuilder {
            id: self.id.to_string(),
            image: image.into(),
            snapshot_key: snapshot_key.into(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum AfterSnapshotLoad {
    /// After loading the VM from the snapshot, also resume it.
    #[default]
    Resume,
    /// After loading the VM from the snapshot, do *not* resume it.
    NoResume,
}

impl PartialEq for Vm {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.namespace == other.namespace
    }
}
impl Eq for Vm {}
