mod error;
#[cfg(feature = "__toy")]
pub mod toy;

pub use error::ConstructResponseError;
pub use error::Error;

use std::{borrow::Cow, collections::HashMap};

use async_trait::async_trait;
use prost::bytes::Bytes;
use tokio::sync::{broadcast, mpsc};

use crate::{FunctionId, InvocationId};

/// The [`Response`] counterpart of [`HEADER_KEY_SOURCE_TIMESTAMP_NS`].
///
/// The system does not really use this, but [`ResponseSink`] implementations optionally could.
pub const HEADER_KEY_SINK_TIMESTAMP_NS: &str = "snaplace-sink-timestamp";

/// Any type representing an outgoing Function response must implement this trait to be handled by
/// `snaplace-rs`.
pub trait Response: Clone + Send + 'static {
    /// Constructor used internally by [`Worker`]s.
    ///
    /// # Returns
    ///
    /// A correctly initialized `Self`, or a [`ConstructResponseError`] in case of failure.
    ///
    /// [`ConstructResponseError`]: crate::response::ConstructResponseError
    fn try_from_parts(
        invocation_id: &InvocationId,
        function_id: &FunctionId,
        status_code: ::tonic::Code,
        metadata_map: ::tonic::metadata::MetadataMap,
        payload: Bytes,
    ) -> Result<Self, ConstructResponseError>;

    /// Returns a unique identifier for the invocation that this request is part of.
    fn invocation_id(&self) -> Cow<'_, str>;

    ///// Returns a unique identifier for the Function targeted by this request.
    //// NOTE: We probably do not need the FunctionId in the Response at all
    //fn function_id(&self) -> Cow<'_, str>;

    /// The gRPC status code, accessible through [`tonic::Status`].
    ///
    /// [`tonic::Status`]: ::tonic::Status
    fn status_code(&self) -> i32;

    /// This is the [`MetadataMap`] accessible through [`tonic::Status`], which can be (cheaply)
    /// converted into a [`http::header::HeaderMap`], which can be converted and serialized as a
    /// [`HashMap`].
    ///
    /// [`http::header::HeaderMap`]: ::http::header::HeaderMap
    /// [`tonic::Status`]: ::tonic::Status
    /// [`MetadataMap`]: ::tonic::metadata::MetadataMap
    // FIXME: I don't think we really need to send this to the user?!
    fn metadata_map(&mut self) -> &mut HashMap<String, String>;

    /// Returns the actual user's serialized payload to be handed to the Function.
    fn payload(&self) -> Bytes;
}

#[async_trait]
pub trait Sink: Send + 'static {
    type Response: Response;

    fn tx(&self) -> mpsc::Sender<Self::Response>;

    async fn run(self, quit_rx: broadcast::Receiver<()>) -> Result<(), Error>;
}
