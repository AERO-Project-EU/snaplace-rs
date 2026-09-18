use std::{fmt::Debug, io, path::Path, time::Duration};

use compact_str::{CompactString, ToCompactString};
use containerd_client::{
    services::v1::{
        containers_client::ContainersClient,
        content_client::ContentClient,
        images_client::ImagesClient,
        namespaces_client::NamespacesClient,
        snapshots::{
            snapshots_client::SnapshotsClient, ListSnapshotsRequest, PrepareSnapshotRequest,
            RemoveSnapshotRequest,
        },
        tasks_client::TasksClient,
        version_client::VersionClient,
        Container, CreateContainerRequest, CreateNamespaceRequest, CreateTaskRequest,
        DeleteContainerRequest, DeleteContentRequest, DeleteImageRequest, DeleteNamespaceRequest,
        DeleteTaskRequest, GetImageRequest, Image, KillRequest, ListContainersRequest,
        ListContentRequest, ListImagesRequest, ListNamespacesRequest, Namespace,
        ReadContentRequest, StartRequest, WaitRequest,
    },
    tonic::{transport::Channel, Request},
    types::Mount,
    with_namespace,
};
use futures::TryFutureExt;
use oci_spec::image::{Arch, ImageConfiguration, ImageIndex, ImageManifest, MediaType, Os};
use sha2::{Digest, Sha256};
use tokio::fs;
use tracing::{debug, instrument, trace, warn, Level};
use ttrpc::{
    asynchronous::Client as TtrpcClient,
    context::{self, Context},
};

use firecracker_containerd_ttrpc::{
    fccontrol_ttrpc::FirecrackerClient,
    firecracker::{
        CreateVMRequest, CreateVMSnapshotRequest, LoadVMSnapshotRequest, PauseVMRequest,
        ResumeVMRequest, StopVMRequest, UnloadVMRequest,
    },
};

use crate::{
    error::{Error, Result},
    vm::Vm,
    AfterSnapshotLoad, Task, DEFAULT_SNAPSHOTTER,
};

#[derive(Debug, Clone, Copy)]
pub enum FlushData {
    Fsync,
    None,
}

/// TODO: Documentation
///
/// # Notes
/// - A `Client` instance is associated with a single containerd namespace, which is used across
///   all containerd RPC calls (unless specified otherwise).
#[derive(Clone)]
pub struct Client {
    /// The (containerd-)namespace that this client instance is bound to.
    namespace: CompactString,

    /// The timeout to be used by this client in the TTRPC contexts when communicating with the
    /// fc-control plugin.
    timeout: Duration,

    /// gRPC transport channel to create (firecracker-)containerd gRPC clients.
    ctrd_chan: Channel,

    /// TTRPC client to the fc-control plugin of firecracker-containerd.
    fcc: FirecrackerClient,
}

impl Client {
    /// The default timeout of the TTRPC contexts, if none is specified when instantiating the
    /// `Client`.
    pub const CONTEXT_TIMEOUT: Duration = Duration::from_secs(10);

    // According to github.com/containerd/containerd@v1.6.8/namespaces/ttrpc.go
    const TTRPC_HEADER_NAMESPACE_KEY: &'static str = "containerd-namespace-ttrpc";

    /// Create a new `Client` instance.
    #[instrument(level = Level::TRACE)]
    pub async fn new<A, T, N>(addr: A, ttrpc_addr: T, namespace: N) -> Result<Self>
    where
        A: AsRef<Path> + Debug,
        T: AsRef<Path> + Debug,
        N: ToCompactString + Debug,
    {
        debug!("Creating new firecracker-containerd client...");
        let ctrd_chan = ::containerd_client::connect(addr).await?;

        debug!("Creating new fc-control client...");
        let fcc = FirecrackerClient::new(
            TtrpcClient::connect(format!("unix://{}", ttrpc_addr.as_ref().display()).as_str())
                .await?,
        );

        Ok(Self {
            namespace: namespace.to_compact_string(),
            timeout: Self::CONTEXT_TIMEOUT,
            ctrd_chan,
            fcc,
        })
    }

    /// Like [`Client::new`], but also specify the timeout of the [TTRPC `Context`][ttrpc_ctx].
    ///
    /// [ttrpc_ctx]: ::ttrpc::context::Context
    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn with_timeout<A, T, N>(
        addr: A,
        ttrpc_addr: T,
        namespace: N,
        timeout: Duration,
    ) -> Result<Self>
    where
        A: AsRef<Path> + Debug,
        T: AsRef<Path> + Debug,
        N: ToCompactString + Debug,
    {
        let mut client = Self::new(addr, ttrpc_addr, namespace).await?;
        client.timeout = timeout;
        Ok(client)
    }

    /// Retrieve the name of the containerd namespace that this client operates in.
    #[inline]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Create a new [TTRPC context][ttrpc_ctx] with the specified `timeout`.
    ///
    /// [ttrpc_ctx]: ::ttrpc::context::Context
    pub fn new_context(&self, timeout: Duration) -> Context {
        let mut ctx = context::with_timeout(timeout.as_nanos() as i64);
        ctx.add(
            Self::TTRPC_HEADER_NAMESPACE_KEY.to_string(),
            self.namespace.to_string(),
        );
        ctx
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Version
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Retrieve a `(version, revision)` tuple from the firecracker-containerd we are connected to.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn version(&self) -> Result<(String, String)> {
        let mut version = VersionClient::new(self.ctrd_chan.clone());
        let resp = version.version(()).await.map_err(Box::new)?.into_inner();
        Ok((resp.version, resp.revision))
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Namespaces
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Attempt to create the firecracker-containerd namespace associated with this `Client`.
    ///
    /// # Returns
    ///
    /// The new firecracker-containerd [`Namespace`].
    ///
    /// # Notes
    ///
    /// - No check is performed prior to sending the [`CreateNamespaceRequest`] of whether the
    /// namespace already exists.
    /// - No labels are attached to the new namespace.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn create_namespace(&self) -> Result<Namespace> {
        let mut namespaces = NamespacesClient::new(self.ctrd_chan.clone());

        let req = CreateNamespaceRequest {
            namespace: Some(Namespace {
                name: self.namespace.to_string(),
                ..Default::default()
            }),
        };
        trace!("{req:?}");

        // NOTE(ckatsak): Namespaces themselves are not namespaced, but the events published by
        // firecracker-containerd on their creation & deletion appear to be namespaced.
        // Server-side handling can be found in services/namespaces/local.go in the codebase of
        // containerd (v1.6.8). Over there, we observe that:
        // - Create uses the received Context's namespace to publish the NamespaceCreate event;
        //   therefore, the caller (i.e., us, here) should probably have set it properly. Mind that
        //   event publishing happens after the namespace has been created, therefore we should be
        //   able to use the newly created namespace for that.
        // - Delete always publishes the NamespaceDelete event in the namespace that has just been
        //   deleted, regardless of the namespace that has been set by the caller in the received
        //   Context; therefore, we could probably leave it empty as well in this case.
        // - List (as well as Get) does not publish any event at all.
        let resp = namespaces
            .create(with_namespace!(req, self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner();
        trace!("{resp:?}");

        resp.namespace
            .ok_or_else(|| Error::EmptyField("CreateNamespaceResponse.Namespace".into()))
    }

    /// Attempt to list all firecracker-containerd namespaces, optionally using a provided `filter`
    /// string.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn list_namespaces<S>(&self, filter: Option<S>) -> Result<Vec<Namespace>>
    where
        S: Into<String> + Debug,
    {
        let mut namespaces = NamespacesClient::new(self.ctrd_chan.clone());

        let req = ListNamespacesRequest {
            filter: filter.map(|f| f.into()).unwrap_or_default(),
        };
        trace!("{req:?}");

        let resp = namespaces.list(req).await.map_err(Box::new)?.into_inner();
        trace!("{resp:?}");

        Ok(resp.namespaces)
    }

    /// Attempt to delete the firecracker-containerd namespace associated with this `Client`.
    ///
    /// # Notes
    ///
    /// - No check is performed prior to sending the [`DeleteNamespaceRequest`] of whether the
    /// namespace exists at all.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn delete_namespace(&self) -> Result<()> {
        let mut namespaces = NamespacesClient::new(self.ctrd_chan.clone());

        let req = DeleteNamespaceRequest {
            name: self.namespace.to_string(),
        };
        trace!("{req:?}");

        // NOTE(ckatsak): Namespaces are not namespaced themselves, but the events published by
        // firecracker-containerd on their creation & deletion appear to be namespaced.
        // Server-side handling can be found in services/namespaces/local.go in the codebase of
        // containerd (v1.6.8). Over there, we observe that:
        // - Create uses the received Context's namespace to publish the NamespaceCreate event;
        //   therefore, the caller (i.e., us, here) should probably have set it properly. Mind that
        //   event publishing happens after the namespace has been created, therefore we should be
        //   able to use the newly created namespace for that.
        // - Delete always publishes the NamespaceDelete event in the namespace that has just been
        //   deleted, regardless of the namespace that has been set by the caller in the received
        //   Context; therefore, we could probably leave it empty as well in this case.
        // - List (as well as Get) does not publish any event at all.
        namespaces
            .delete(with_namespace!(req, self.namespace))
            .await
            .map_err(Box::new)?;

        Ok(())
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Images
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Retrieve the containerd [`Image`] associated with the given `name`.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn get_image(&self, name: impl Into<String> + Debug) -> Result<Image> {
        let mut images = ImagesClient::new(self.ctrd_chan.clone());

        let req = GetImageRequest { name: name.into() };
        trace!("{req:?}");

        let resp = images
            .get(with_namespace!(req, self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner();
        trace!("{resp:?}");

        resp.image
            .ok_or_else(|| Error::EmptyField("GetImageResponse.Image".into()))
    }

    /// List all images (which pass the containerd `filters` provided) in this namespace.
    // TODO(ckatsak): Use IntoIterator<Item = S> to be more flexible?
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn list_images<S>(&self, filters: Option<Vec<S>>) -> Result<Vec<Image>>
    where
        S: Into<String> + Debug,
    {
        let mut images = ImagesClient::new(self.ctrd_chan.clone());

        let req = ListImagesRequest {
            filters: filters
                .map(|fs| fs.into_iter().map(S::into).collect())
                .unwrap_or_default(),
        };
        trace!("{req:?}");

        let resp = images
            .list(with_namespace!(req, self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner();
        trace!("{resp:?}");

        Ok(resp.images)
    }

    /// Delete the containerd image associated with the given `name`, optionally waiting for the
    /// deletion to complete at containerd before returning to the caller.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn delete_image<S>(&self, name: S, sync: bool) -> Result<()>
    where
        S: Into<String> + Debug,
    {
        let mut images = ImagesClient::new(self.ctrd_chan.clone());

        let req = DeleteImageRequest {
            name: name.into(),
            sync,
            target: None,
        };
        trace!("{req:?}");

        let _unit_resp = images
            .delete(with_namespace!(req, self.namespace))
            .await
            .map_err(Box::new)?;
        Ok(())
    }

    /// Retrieve the OCI [`ImageConfiguration`] associated with the given `image_ref`.
    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn get_image_config<I>(&self, image_ref: I) -> Result<ImageConfiguration>
    where
        I: Into<String> + Debug,
    {
        const DOCKER_INDEX: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
        const DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";

        /// https://github.com/containerd/containerd/blob/8a6c8a96c0de336b15cbdc4693605add6868c264/docs/content-flow.md#image-format
        async fn handle_index(c: &Client, blob: &[u8]) -> Result<ImageConfiguration> {
            let img_index: ImageIndex =
                ::serde_json::from_slice(blob).map_err(|err| Error::Json {
                    msg: String::from("failed to deserialize OCI image index").into_boxed_str(),
                    source: err,
                })?;

            let img_manifest_dscr = img_index
                .manifests()
                .iter()
                .find(|manifest_entry| match manifest_entry.platform() {
                    Some(p) => {
                        #[cfg(target_arch = "x86_64")]
                        {
                            matches!(p.architecture(), &Arch::Amd64) && matches!(p.os(), &Os::Linux)
                        }
                        #[cfg(target_arch = "aarch64")]
                        {
                            matches!(p.architecture(), &Arch::ARM64) && matches!(p.os(), &Os::Linux)
                            //&& matches!(p.variant().as_ref().map(|s| s.as_str()), Some("v8"))
                        }
                    }
                    None => false,
                })
                .ok_or_else(|| Error::ManifestNotFound(img_index.manifests().to_owned()))?;

            let blob = c
                .read_content_blob(img_manifest_dscr.digest().to_string())
                .await?;

            handle_manifest(c, &blob).await
        }

        /// https://github.com/containerd/containerd/blob/8a6c8a96c0de336b15cbdc4693605add6868c264/docs/content-flow.md#image-format
        async fn handle_manifest(c: &Client, blob: &[u8]) -> Result<ImageConfiguration> {
            let img_manifest: ImageManifest =
                ::serde_json::from_slice(blob).map_err(|err| Error::Json {
                    msg: String::from("failed to deserialize OCI image manifest").into_boxed_str(),
                    source: err,
                })?;
            let img_config_dscr = img_manifest.config();
            let blob = c
                .read_content_blob(img_config_dscr.digest().to_string())
                .await?;
            ::serde_json::from_slice(&blob).map_err(|err| Error::Json {
                msg: String::from("failed to deserialize OCI image configuration").into_boxed_str(),
                source: err,
            })
        }

        let img = self.get_image(image_ref).await?;

        // Retrieve descriptor for the image.
        // This may refer either to a Manifest or to an Index (i.e., list of Manifests).
        //
        // NOTE(ckatsak): This is an (OCI) Content Descriptor as defined in `containerd_client`
        // crate, which appears to not be 100% compliant:
        // - crate `containerd_client`: https://docs.rs/containerd-client/0.3.0/containerd_client/types/struct.Descriptor.html
        // - crate `oci-spec`: https://docs.rs/oci-spec/0.6.0/oci_spec/image/struct.Descriptor.html
        // - OCI spec: https://github.com/opencontainers/image-spec/blob/v1.0.2/manifest.md
        // - OCI spec (go reference impl): https://pkg.go.dev/github.com/opencontainers/image-spec@v1.0.2/specs-go/v1
        // Therefore we identify the MediaType and use crate `oci-spec` for Descriptors from now on
        let img_dscr = img
            .target
            .ok_or_else(|| Error::EmptyField("Image.target".into()))?;
        let media_type = MediaType::from(img_dscr.media_type.as_str());
        trace!("Found media type '{media_type}' ({media_type:?})");

        // Retrieve image config from content store
        let blob = self.read_content_blob(&img_dscr.digest).await?;

        match media_type {
            MediaType::ImageIndex => handle_index(self, &blob).await,
            MediaType::ImageManifest => handle_manifest(self, &blob).await,
            MediaType::Other(media_type) => match media_type {
                media_type if media_type == DOCKER_INDEX => handle_index(self, &blob).await,
                media_type if media_type == DOCKER_MANIFEST => handle_manifest(self, &blob).await,
                media_type => Err(Error::UnexpectedMediaType(media_type)),
            },
            media_type => Err(Error::UnexpectedMediaType(media_type.to_string())),
        }
    }

    /// Join the values in `.config.entrypoint` and `.config.cmd` of the given OCI
    /// [`ImageConfiguration`] into a single `String` to be used as the value in OCI
    /// [Runtime `Spec`]'s `.process.args`.
    ///
    /// [Runtime `Spec`]: ::oci_spec::runtime::Spec
    #[instrument(level = Level::TRACE)]
    pub fn form_process_args(img_config: &ImageConfiguration) -> Result<String> {
        // Make sure at least one of `.config.entrypoint` or `.config.cmd` fields are present in
        // the OCI Image config
        let cfg = img_config
            .config()
            .as_ref()
            .ok_or_else(|| Error::OciImageConfigMissingField(".config".into()))?;
        if cfg.entrypoint().is_none() && cfg.cmd().is_none() {
            return Err(Error::OciImageConfigMissingField(
                "`.config.entrypoint` nor `.config.cmd`".into(),
            ));
        }

        // Join the String Vec(s) into a single String, interspersed with ' '
        let (num_strs, total_str_len) = cfg
            .entrypoint()
            .iter()
            .flatten()
            .chain(cfg.cmd().iter().flatten())
            .map(String::len)
            .fold((0, 0), |(num, total_len), str_len| {
                (num + 1, total_len + str_len)
            });
        let mut ret = String::with_capacity(total_str_len + num_strs - 1); // num_strs-1 spaces
        let mut strs = cfg
            .entrypoint()
            .iter()
            .flatten()
            .chain(cfg.cmd().iter().flatten());
        (&mut strs).take(num_strs - 1).for_each(|s| {
            ret.push_str(s);
            ret.push(' ');
        });
        ret.push_str(strs.next().expect("exactly 1 String should remain"));
        debug_assert!(strs.next().is_none());
        debug_assert_eq!(ret.capacity(), total_str_len + num_strs - 1);
        debug_assert_eq!(ret.len(), total_str_len + num_strs - 1);
        Ok(ret)

        //Ok(::itertools::intersperse(
        //    cfg.entrypoint()
        //        .iter()
        //        .flatten()
        //        .chain(cfg.cmd().iter().flatten())
        //        .map(String::as_str),
        //    " ",
        //)
        //.collect::<String>())
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // containerd snapshots
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Given an OCI [`ImageConfiguration`], calculate a new container's parent snapshot's digest
    /// based on the digests of the diff layers.
    #[instrument(level = Level::TRACE, skip_all)]
    pub fn calculate_parent_snapshot(img_config: &ImageConfiguration) -> String {
        let mut iter = img_config.rootfs().diff_ids().iter();
        let mut ret = iter
            .next()
            .map_or_else(String::new, |layer_digest| layer_digest.clone());
        while let Some(layer_digest) = iter.by_ref().next() {
            let mut hasher = Sha256::new();
            hasher.update(&ret);
            hasher.update(" ");
            hasher.update(layer_digest);
            let digest = ::hex::encode(hasher.finalize());
            ret = format!("sha256:{digest}");
        }

        ret
    }

    /// Retrieve the OCI [`ImageConfiguration`] associated with the given `image_ref`, and use it
    /// to calculate the parent snapshot's digest based on the digests of the diff layers.
    #[instrument(level = Level::TRACE, skip_all)]
    #[inline]
    pub async fn find_parent_snapshot<I>(&self, image_ref: I) -> Result<String>
    where
        I: Into<String> + Debug,
    {
        self.get_image_config(image_ref).await.map(|img_config| {
            trace!(?img_config);
            Self::calculate_parent_snapshot(&img_config)
        })
    }

    /// Prepare a (containerd-)snapshot identified by `key`, using [`DEFAULT_SNAPSHOTTER`] and
    /// the snapshot of image `image_ref` as its parent snapshot.
    #[instrument(level = Level::TRACE, skip(self))]
    #[inline]
    pub async fn prepare_snapshot<K, I>(&self, key: K, image_ref: I) -> Result<Vec<Mount>>
    where
        K: Into<String> + Debug,
        I: Into<String> + Debug,
    {
        let parent = self.find_parent_snapshot(image_ref).await?;
        self.prepare_with_snapshotter(DEFAULT_SNAPSHOTTER, key, parent)
            .await
    }

    /// Prepare a (containerd-)snapshot identified by `key`, using the given `snapshotter` and
    /// `parent` as its parent snapshot.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn prepare_with_snapshotter<S, K, P>(
        &self,
        snapshotter: S,
        key: K,
        parent: P,
    ) -> Result<Vec<Mount>>
    where
        S: Into<String> + Debug,
        K: Into<String> + Debug,
        P: Into<String> + Debug,
    {
        let mut snapshots = SnapshotsClient::new(self.ctrd_chan.clone());

        let req = PrepareSnapshotRequest {
            snapshotter: snapshotter.into(),
            key: key.into(),
            parent: parent.into(),
            ..Default::default()
        };
        Ok(snapshots
            .prepare(with_namespace!(req, &self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner()
            .mounts)
    }

    /// Delete the (containerd-)snapshot identified by `key` under the given `snapshotter`.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn remove_container_snapshot<K, S>(
        &self,
        key: K,
        snapshotter: Option<S>,
    ) -> Result<()>
    where
        K: Into<String> + Debug,
        S: Into<String> + Debug,
    {
        let mut snapshots = SnapshotsClient::new(self.ctrd_chan.clone());

        let req = RemoveSnapshotRequest {
            snapshotter: snapshotter
                .map(S::into)
                .unwrap_or_else(|| DEFAULT_SNAPSHOTTER.to_string()),
            key: key.into(),
        };
        trace!("{req:?}");

        let _unit_resp = snapshots
            .remove(with_namespace!(req, &self.namespace))
            .await
            .map_err(Box::new)?;
        Ok(())
    }

    /// Attempt to delete all firecracker-containerd snapshots in the namespace associated with
    /// this `Client`.
    ///
    /// # Errors
    ///
    /// In case of removal failures, the associated snapshot keys will be returned through the
    /// returned `Error`.
    // TODO(ckatsak): Use IntoIterator<Item = S> to be more flexible?
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn remove_all_container_snapshots(
        &self,
        snapshotter: Option<impl Into<String> + Debug>,
        filters: Option<Vec<impl Into<String> + Debug>>,
    ) -> Result<()> {
        let mut snapshots = SnapshotsClient::new(self.ctrd_chan.clone());

        let snapshotter = snapshotter
            .map(|s| s.into())
            .unwrap_or_else(|| DEFAULT_SNAPSHOTTER.to_string());

        let req = ListSnapshotsRequest {
            snapshotter: snapshotter.clone(),
            filters: filters
                .map(|fs| fs.into_iter().map(|f| f.into()).collect())
                .unwrap_or_default(),
        };
        trace!("{req:?}");

        let mut snap_stream = snapshots
            .list(with_namespace!(req, self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner();

        let mut failed_keys = Vec::new();
        while let Some(resp) = snap_stream.message().await.map_err(Box::new)? {
            trace!("{resp:?}");

            for info in resp.info {
                match self
                    .remove_container_snapshot(&info.name, Some(&snapshotter))
                    .await
                {
                    Ok(()) => (),
                    Err(Error::TonicStatus(status)) => {
                        failed_keys.push((info.name, status));
                    }
                    Err(err) => Err(err)?,
                }
            }
        }

        if failed_keys.is_empty() {
            Ok(())
        } else {
            Err(Error::MassDeletion(failed_keys))
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Content
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Read the blob associated with the given `digest` from containerd's Content Store.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn read_content_blob(&self, digest: impl Into<String> + Debug) -> Result<Vec<u8>> {
        let mut content = ContentClient::new(self.ctrd_chan.clone());

        let req = ReadContentRequest {
            digest: digest.into(),
            offset: 0,
            size: 0,
        };
        trace!("{req:?}");

        let mut msg_stream = content
            .read(with_namespace!(req, self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner();

        let mut blob = Vec::new();
        while let Some(resp) = msg_stream.message().await.map_err(|err| {
            Error::ReadContent(format!("while reading a gRPC stream message: {err:#}"))
        })? {
            trace!(
                "received new ReadContentResponse; (offset, len) = ({}, {}) B",
                resp.offset,
                resp.data.len()
            );
            blob.extend_from_slice(&resp.data);
        }

        if blob.is_empty() {
            Err(Error::ReadContent(String::from("empty blob")))
        } else {
            Ok(blob)
        }
    }

    /// Delete all firecracker-containerd content blobs in the namespace associated with this
    /// `Client`.
    ///
    /// # Errors
    ///
    /// In case of deletion failures, the associated content digests will be returned through the
    /// returned `Error`.
    // TODO(ckatsak): Use IntoIterator<Item = S> to be more flexible?
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn delete_all_content<S>(&self, filters: Option<Vec<S>>) -> Result<()>
    where
        S: Into<String> + Debug,
    {
        let mut content = ContentClient::new(self.ctrd_chan.clone());

        let req = ListContentRequest {
            filters: filters
                .map(|fs| fs.into_iter().map(S::into).collect())
                .unwrap_or_default(),
        };
        trace!("{req:?}");

        let mut blob_stream = content
            .list(with_namespace!(req, self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner();

        let mut failed_keys = Vec::new();
        while let Some(resp) = blob_stream.message().await.map_err(Box::new)? {
            trace!("{resp:?}");

            for info in resp.info {
                let req = DeleteContentRequest {
                    digest: info.digest.clone(),
                };
                trace!("{req:?}");

                match content.delete(with_namespace!(req, self.namespace)).await {
                    Ok(_unit_resp) => (),
                    Err(status) => {
                        failed_keys.push((info.digest, Box::new(status)));
                    }
                }
            }
        }

        if failed_keys.is_empty() {
            Ok(())
        } else {
            Err(Error::MassDeletion(failed_keys))
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Containers
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[instrument(level = Level::TRACE, skip(self))]
    pub(crate) async fn create_container(&self, req: CreateContainerRequest) -> Result<Container> {
        let mut containers = ContainersClient::new(self.ctrd_chan.clone());

        let resp = containers
            .create(with_namespace!(req, &self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner();
        trace!("{resp:?}");

        resp.container
            .ok_or_else(|| Error::EmptyField("CreateContainerResponse.Container".into()))
    }

    /// Delete the container associated with the provided `container_id`.
    // TODO(ckatsak): Use IntoIterator<Item = S> to be more flexible?
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn list_containers<S>(&self, filters: Option<Vec<S>>) -> Result<Vec<Container>>
    where
        S: Into<String> + Debug,
    {
        let mut containers = ContainersClient::new(self.ctrd_chan.clone());

        let req = ListContainersRequest {
            filters: filters
                .map(|fs| fs.into_iter().map(S::into).collect())
                .unwrap_or_default(),
        };
        trace!("{req:?}");

        let resp = containers
            .list(with_namespace!(req, self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner();
        trace!("{resp:?}");

        Ok(resp.containers)
    }

    /// Delete the container associated with the provided `container_id`.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn delete_container<C>(&self, container_id: C) -> Result<()>
    where
        C: Into<String> + Debug,
    {
        let mut containers = ContainersClient::new(self.ctrd_chan.clone());

        let req = DeleteContainerRequest {
            id: container_id.into(),
        };
        trace!("{req:?}");

        let _unit_resp = containers
            .delete(with_namespace!(req, &self.namespace))
            .await
            .map_err(Box::new)?;
        Ok(())
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Tasks
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn create_task<C>(&self, container_id: C, rootfs: &[Mount]) -> Result<Task>
    where
        C: Into<String> + Debug,
    {
        let mut tasks = TasksClient::new(self.ctrd_chan.clone());

        let req = CreateTaskRequest {
            container_id: container_id.into(),
            rootfs: Vec::from(rootfs),
            ..Default::default()
        };
        trace!("{req:?}");
        let req = with_namespace!(req, &self.namespace);

        let resp = tasks.create(req).await.map_err(Box::new)?.into_inner();
        trace!("{resp:?}");

        Ok(resp.into())
    }

    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn start_task(&self, task: &mut Task) -> Result<()> {
        let mut tasks = TasksClient::new(self.ctrd_chan.clone());

        let req = StartRequest {
            container_id: task.container_id().to_string(),
            //exec_id: task.pid.to_string(), // XXX(ckatsak): ExecID != PID
            ..Default::default()
        };
        trace!("{req:?}");
        let req = with_namespace!(req, &self.namespace);

        let resp = tasks.start(req).await.map_err(Box::new)?.into_inner();
        trace!("{resp:?}");

        debug!(
            "old (task.pid): {}, new (resp.pid): {}",
            task.pid(),
            resp.pid
        );
        task.set_pid(resp.pid);

        Ok(())
    }

    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn kill_task<C>(&self, container_id: C, signal_no: u32, all: bool) -> Result<()>
    where
        C: Into<String> + Debug,
    {
        let mut tasks = TasksClient::new(self.ctrd_chan.clone());

        let req = KillRequest {
            container_id: container_id.into(),
            signal: signal_no,
            all,
            ..Default::default()
        };
        trace!("{req:?}");

        let _empty_resp = tasks
            .kill(with_namespace!(req, &self.namespace))
            .await
            .map_err(Box::new)?;
        Ok(())
    }

    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn wait_task<C>(&self, container_id: C) -> Result<u32>
    where
        C: Into<String> + Debug,
    {
        let mut tasks = TasksClient::new(self.ctrd_chan.clone());

        let req = WaitRequest {
            container_id: container_id.into(),
            ..Default::default()
        };
        trace!("{req:?}");

        let resp = tasks
            .wait(with_namespace!(req, &self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner();
        trace!("{resp:?}");

        Ok(resp.exit_status)
    }

    /// Delete the task with the provided `container_id`, returning the reported `exit_status` as a
    /// `u32`.
    ///
    /// This method is used internally by [`Task::delete`].
    ///
    /// # Notes
    ///
    /// - Calling this method does not update any [`Task`] instance associated with the
    /// `container_id` that may or may not exist. Callers should use [`Task::delete`] on that task
    /// if they want it to remain up to date.
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn delete_task(&self, container_id: impl Into<String> + Debug) -> Result<u32> {
        let mut tasks = TasksClient::new(self.ctrd_chan.clone());

        let req = DeleteTaskRequest {
            container_id: container_id.into(),
        };
        trace!("{req:?}");

        let resp = tasks
            .delete(with_namespace!(req, &self.namespace))
            .await
            .map_err(Box::new)?
            .into_inner();
        trace!("{resp:?}");

        Ok(resp.exit_status)
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // VMs
    ///////////////////////////////////////////////////////////////////////////////////////////////

    #[instrument(level = Level::TRACE, skip(self))]
    pub(crate) async fn create_vm(&self, req: CreateVMRequest) -> Result<Vm> {
        let resp = self
            .fcc
            .create_vm(self.new_context(self.timeout), &req)
            .await?;
        trace!("{resp:?}");

        Ok(Vm::from_response(resp, &self.namespace))
    }

    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn pause_vm(&self, vm_id: impl Into<String> + Debug) -> Result<()> {
        let req = PauseVMRequest {
            VMID: vm_id.into(),
            ..Default::default()
        };
        trace!("{req:?}");

        let _empty_resp = self
            .fcc
            .pause_vm(self.new_context(self.timeout), &req)
            .await?;
        Ok(())
    }

    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn resume_vm(&self, vm_id: impl Into<String> + Debug) -> Result<()> {
        let req = ResumeVMRequest {
            VMID: vm_id.into(),
            ..Default::default()
        };
        trace!("{req:?}");

        let _empty_resp = self
            .fcc
            .resume_vm(self.new_context(self.timeout), &req)
            .await?;
        Ok(())
    }

    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn create_vm_snapshot<ID, S, M>(
        &self,
        vm_id: ID,
        state_file_path: S,
        memory_file_path: M,
        flush_data: FlushData,
    ) -> Result<()>
    where
        ID: Into<String> + Debug,
        S: AsRef<Path> + Debug,
        M: AsRef<Path> + Debug,
    {
        async fn prepare_path(path: &Path) -> Result<()> {
            if path.is_file() {
                trace!("Removing stale snapshot file '{}'", path.display());
                fs::remove_file(path).await.map_err(|err| {
                    warn!(error = ?err, "Failed to remove file '{}': {err:#}", path.display());
                    Error::Io {
                        msg: format!("failed to remove file '{}'", path.display()),
                        source: err,
                    }
                })?;
            } else {
                let dir = path.parent().ok_or_else(|| {
                    warn!("Failed to get parent directory for '{}'", path.display());
                    Error::Io {
                        msg: format!("failed to process path '{}'", path.display()),
                        source: io::Error::other(format!(
                            "failed to find parent directory for '{}'",
                            path.display()
                        )),
                    }
                })?;
                fs::create_dir_all(&dir).await.map_err(|err| {
                    warn!(
                        error = ?err,
                        "Failed to recursively create directories '{}': {err:#}",
                        dir.display()
                    );
                    Error::Io {
                        msg: format!(
                            "failed to recursively create directories '{}'",
                            dir.display()
                        ),
                        source: err,
                    }
                })?;
            }
            Ok(())
        }

        async fn fsync_path(path: &Path) -> Result<()> {
            fs::OpenOptions::new()
                .read(true)
                .open(&path)
                .and_then(|f| async move { f.sync_all().await })
                .await
                .map_err(|err| Error::Io {
                    msg: "failed to open(2) & fsync(2) file {path:?}".to_owned(),
                    source: err,
                })
        }

        ::tokio::try_join!(
            prepare_path(state_file_path.as_ref()),
            prepare_path(memory_file_path.as_ref())
        )?;

        let req = CreateVMSnapshotRequest {
            VMID: vm_id.into(),
            SnapshotPath: state_file_path
                .as_ref()
                .to_str()
                .ok_or_else(|| Error::Utf(state_file_path.as_ref().to_string_lossy().into()))?
                .to_string(),
            MemFilePath: memory_file_path
                .as_ref()
                .to_str()
                .ok_or_else(|| Error::Utf(memory_file_path.as_ref().to_string_lossy().into()))?
                .to_string(),
            ..Default::default()
        };
        trace!("{req:?}");

        let _empty_resp = self
            .fcc
            .create_vm_snapshot(self.new_context(self.timeout), &req)
            .await?;

        if let FlushData::Fsync = flush_data {
            ::tokio::try_join!(
                fsync_path(state_file_path.as_ref()),
                fsync_path(memory_file_path.as_ref())
            )?;
        }

        Ok(())
    }

    /// TODO: Documentation
    ///
    /// # Arguments
    ///
    /// - `vm_id` is the ID of the VM to be stopped
    /// - `timeout` is the timeout to be supplied to both [`StopVMRequest`] *and* its associated
    /// [TTRPC context].
    ///
    /// # Note / TODO
    ///
    /// IIUC, in theory, the timeout included in [`StopVMRequest`] can be different from the one
    /// included in the [TTRPC context].
    /// However, in practice, I observe that supplying those two with a different value each, none
    /// of them is applied; firecracker-containerd's `defaultStopVMTimeout` is applied instead (in
    /// fact, the RPC returns after ~4.94s).
    /// By skimming through firecracker-containerd's code, I cannot really explain this, but I
    /// don't have time for this now, so I just enforce the provided `timeout` to be supplied to
    /// both [`StopVMRequest`] *and* its associated [TTRPC context] (i.e., ignoring `Client`'s
    /// configured timeout).
    ///
    /// [TTRPC context]: ttrpc::context::Context
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn stop_vm<ID>(&self, vm_id: ID, timeout: Duration) -> Result<()>
    where
        ID: Into<String> + Debug,
    {
        let req = StopVMRequest {
            VMID: vm_id.into(),
            TimeoutSeconds: timeout.as_secs() as u32,
            ..Default::default()
        };
        trace!("{req:?}");

        let _empty_resp = self
            .fcc
            .stop_vm(self.new_context(timeout), &req)
            //
            //.stop_vm(
            //    self.new_context(Duration::from_nanos((timeout.as_nanos() as f64 * 1.5) as _)),
            //    &req,
            //)
            .await?;
        Ok(())
    }

    /// TODO: Documentation
    ///
    /// # Returns
    ///
    /// The PID of the (new) VM that was just loaded from the snapshot, if it has been made
    /// available by firecracker-containerd (for now, it depends on the jailer used).
    ///
    /// # Arguments
    ///
    /// - `vm_id` is the ID of the VM to be loaded from the snapshot
    /// - TODO
    /// - `resume` indicates whether the VM should also be resumed while being restored from the
    /// snapshot
    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn load_vm_snapshot(
        &self,
        vm_id: impl Into<String> + Debug,
        snapshot_path: impl AsRef<Path> + Debug,
        mem_file_path: impl AsRef<Path> + Debug,
        resume: AfterSnapshotLoad,
    ) -> Result<Option<u64>> {
        let req = LoadVMSnapshotRequest {
            VMID: vm_id.into(),
            SnapshotPath: snapshot_path
                .as_ref()
                .to_str()
                .ok_or_else(|| Error::Utf(snapshot_path.as_ref().to_string_lossy().into()))?
                .to_string(),
            MemFilePath: mem_file_path
                .as_ref()
                .to_str()
                .ok_or_else(|| Error::Utf(mem_file_path.as_ref().to_string_lossy().into()))?
                .to_string(),
            ResumeVM: matches!(resume, AfterSnapshotLoad::Resume),
            ..Default::default()
        };
        trace!("{req:?}");

        let resp = self
            .fcc
            .load_vm_snapshot(self.new_context(self.timeout), &req)
            .await?;
        Ok((resp.PID != 0).then_some(resp.PID))
    }

    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn unload_vm(&self, vm_id: impl Into<String> + Debug) -> Result<()> {
        let req = UnloadVMRequest {
            VMID: vm_id.into(),
            ..Default::default()
        };
        trace!("{req:?}");

        let _empty_resp = self
            .fcc
            .unload_vm(self.new_context(self.timeout), &req)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use tracing::error;
    use tracing_test::traced_test;

    use crate::VmBuilder;

    use super::*;

    const DEFAULT_FIRECRACKER_CONTAINERD_ADDRESS: &str =
        "/run/firecracker-containerd/containerd.sock";
    const DEFAULT_FCCONTROL_TTRPC_ADDRESS: &str =
        "/run/firecracker-containerd/containerd.sock.ttrpc";
    const DEFAULT_CONTAINERD_NAMESPACE: &str = "default";

    #[::tokio::test]
    #[traced_test]
    #[should_panic]
    async fn container_count() {
        let Ok(c) = Client::new(
            DEFAULT_FIRECRACKER_CONTAINERD_ADDRESS,
            DEFAULT_FCCONTROL_TTRPC_ADDRESS,
            DEFAULT_CONTAINERD_NAMESPACE,
        )
        .await
        else {
            error!("Failed to create new Client");
            return;
        };

        match VmBuilder::new("test_container_count")
            .container_count(usize::MAX) // NOTE: should panic on debug builds
            .create(&c, Duration::from_secs(1))
            .await
        {
            Ok(mut vm) => {
                error!("Failed to panic, and created the VM");
                if let Err(err) = vm.stop(&c, Duration::from_secs(1)).await {
                    error!(
                        "Failed to panic, created the VM, and failed to stop it afterwards: {err}"
                    );
                }
            }
            Err(err) => {
                error!("Failed to panic, but did not create the VM: {err}");
            }
        };
    }

    async fn test_process_args(image_ref: &str) -> ::anyhow::Result<String> {
        let c = Client::new(
            DEFAULT_FIRECRACKER_CONTAINERD_ADDRESS,
            DEFAULT_FCCONTROL_TTRPC_ADDRESS,
            DEFAULT_CONTAINERD_NAMESPACE,
        )
        .await
        .context("failed to create new Client")?;

        let img_cfg = c
            .get_image_config(image_ref)
            .await
            .context("failed to retrieve OCI image configuration")?;
        Client::form_process_args(&img_cfg).context("failed to calculate process args")
    }

    #[::tokio::test]
    #[traced_test]
    async fn test_process_args_nginx() -> ::anyhow::Result<()> {
        const IMAGE_REF: &str = "docker.io/library/nginx:1.25.0";
        assert_eq!(
            test_process_args(IMAGE_REF).await?,
            "/docker-entrypoint.sh nginx -g daemon off;"
        );
        Ok(())
    }

    #[::tokio::test]
    #[traced_test]
    async fn test_process_args_helloworld() -> ::anyhow::Result<()> {
        const IMAGE_REF: &str = "docker.io/ckatsak/snaplace-fbpml-helloworld:0.0.2-dev";
        assert_eq!(
            test_process_args(IMAGE_REF).await?,
            "/usr/local/bin/python3 /bench/server.py"
        );
        Ok(())
    }
}
