use containerd_client::tonic;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("failed to convert '{0}' into valid UTF-8")]
    Utf(Box<str>),

    // The field in containerd's gRPC response with the reported key is empty.
    //#[error("field '{0}' in firecracker-containerd's response is empty")]
    /// The field that corresponds to the reported key is empty, but it should not be.
    #[error("field {0:?} is empty")]
    EmptyField(Box<str>),

    #[error("failed to delete keys: {0:?}")]
    MassDeletion(Vec<(String, Box<tonic::Status>)>),

    #[error("gRPC transport error")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error("TTRPC error")]
    Ttrpc(#[from] ::ttrpc::Error),

    #[error("failed gRPC call")]
    TonicStatus(#[from] Box<tonic::Status>),

    #[error("JSON error: {msg}")]
    Json {
        msg: Box<str>,
        #[source]
        source: ::serde_json::Error,
    },

    #[error("I/O error: {msg}")]
    Io {
        msg: String,
        #[source]
        source: ::std::io::Error,
    },

    #[error("unexpected OCI media type '{0}'")]
    UnexpectedMediaType(String),

    #[error("no OCI manifest for this platform was found in the OCI index")]
    ManifestNotFound(Vec<::oci_spec::image::Descriptor>),

    #[error("failed to read content blob from containerd: {0}")]
    ReadContent(String),

    /// Returned by:
    /// - [`Vm::load_from_snapshot`],
    /// - [`Vm::set_snapshot_memory_file`] and [`Vm::set_snapshot_state_file`],
    ///
    /// when no snapshot file paths are (already) tracked/stored by the [`Vm`] at hand, while there
    /// should (for the call to succeed).
    ///
    /// [`Vm`]: crate::vm::Vm
    /// [`Vm::load_from_snapshot`]: crate::vm::Vm::load_from_snapshot
    /// [`Vm::set_snapshot_memory_file`]: crate::vm::Vm::set_snapshot_memory_file
    /// [`Vm::set_snapshot_state_file`]: crate::vm::Vm::set_snapshot_state_file
    #[error("no snapshot found")]
    NoSnapshot,

    #[error("no '{0}' field found in OCI Image Configuration")]
    OciImageConfigMissingField(Box<str>),
}
