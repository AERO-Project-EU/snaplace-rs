use std::{collections::HashMap, fmt::Debug, path::Path};

use containerd_client::services::v1::{container::Runtime, Container, CreateContainerRequest};
use prost_types::Any;
use tracing::{instrument, Level};

use crate::{
    client::Client,
    error::{Error, Result},
    oci::RuntimeSpecSource,
    DEFAULT_SNAPSHOTTER,
};

const FIRECRACKER_CONTAINERD_RUNTIME_NAME: &str = "aws.firecracker";
const SPEC_TYPE_URL: &str = "types.containerd.io/opencontainers/runtime-spec/1/Spec";

#[derive(Debug)]
pub struct Builder<'s, 'ns, 'id, P: AsRef<Path> + Debug> {
    /// ID is the user-specified identifier.
    ///
    /// This field cannot be updated.
    pub(crate) id: String,
    /// Area to include arbitrary data on containers.
    ///
    /// The combined size of a key/value pair cannot exceed 4096 bytes.
    pub(crate) labels: Option<HashMap<String, String>>, // default exists
    /// Contains the reference of the image used to build the specification and
    /// (containerd-)snapshots for running this container.
    ///
    /// If this field is updated, the spec and rootfs needed to updated, as well.
    pub(crate) image: String,
    /// Runtime to use for executing this container.
    pub(crate) runtime: Option<Runtime>, // default exists
    /// Runtime-specific spec to be used when creating the container.
    pub(crate) spec: Option<RuntimeSpecSource<'s, 'ns, 'id, P>>, // TODO(ckatsak): mandatory but not in constructor
    /// Specifies the snapshotter name used for rootfs.
    pub(crate) snapshotter: String, // default exists
    /// Specifies the snapshot key to use for the container’s root filesystem. When starting a task
    /// from this container, a caller should look up the mounts from the snapshot service and
    /// include those on the task create request.
    ///
    /// Snapshots referenced in this field will not be garbage collected.
    //
    // This field is set to empty when the rootfs is not a snapshot. (NOTE: mandatory for us)
    pub(crate) snapshot_key: String,
}

impl<P: AsRef<Path> + Debug> Default for Builder<'_, '_, '_, P> {
    fn default() -> Self {
        Self {
            id: String::new(),
            labels: None,
            image: String::new(),
            runtime: None,
            spec: None,
            snapshotter: DEFAULT_SNAPSHOTTER.to_string(),
            snapshot_key: String::new(),
        }
    }
}

impl<'s, 'ns, 'id, P: AsRef<Path> + Debug> Builder<'s, 'ns, 'id, P> {
    pub fn label<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String> + Debug,
        V: Into<String> + Debug,
    {
        let _ = self
            .labels
            .get_or_insert_with(Default::default)
            .insert(key.into(), value.into());
        self
    }

    /// Set the runtime to use for executing this container.
    pub fn runtime(mut self, runtime: Runtime) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Set the runtime-specific spec to be used when creating the container.
    pub fn spec(mut self, source: RuntimeSpecSource<'s, 'ns, 'id, P>) -> Self {
        self.spec = Some(source);
        self
    }

    /// Specify the snapshotter name to use for rootfs.
    pub fn snapshotter(mut self, snapshotter: impl Into<String> + Debug) -> Self {
        self.snapshotter = snapshotter.into();
        self
    }

    #[instrument(level = Level::TRACE, skip(self, client))]
    pub async fn create(self, client: &Client) -> Result<Container> {
        client
            .create_container(CreateContainerRequest {
                container: Some(Container {
                    id: self.id,
                    labels: self.labels.unwrap_or_default(),
                    image: self.image,
                    runtime: self.runtime.or_else(|| {
                        Some(Runtime {
                            name: FIRECRACKER_CONTAINERD_RUNTIME_NAME.to_string(),
                            options: None,
                        })
                    }),
                    spec: Some(Any {
                        type_url: SPEC_TYPE_URL.to_string(),
                        value: self
                            .spec
                            .ok_or_else(|| Error::EmptyField("container::Builder.spec".into()))?
                            .into_json()
                            .await?,
                    }),
                    snapshotter: self.snapshotter,
                    snapshot_key: self.snapshot_key,
                    ..Default::default()
                }),
            })
            .await
    }
}
