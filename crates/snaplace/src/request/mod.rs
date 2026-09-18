mod error;
#[cfg(feature = "__toy")]
pub mod toy;

pub use error::Error;

use std::{borrow::Cow, collections::HashMap, fmt::Debug};

use async_trait::async_trait;
use prost::bytes::Bytes;
use tokio::sync::{broadcast, mpsc};

/// If a header with this key is found in an incoming [`Request`], the system assumes that it is
/// some sort of timestamp originating from the [`RequestSource`], and therefore adds it as a
/// header to the respective [`Response`].
pub const HEADER_KEY_SOURCE_TIMESTAMP_NS: &str = "snaplace-source-timestamp";

/// Any type representing an incoming Function request must implement this trait to be handled by
/// `snaplace-rs`.
pub trait Request: Clone + Debug + Send + 'static {
    /// Returns a unique identifier for the invocation that this request is part of.
    fn invocation_id(&self) -> Cow<'_, str>;

    /// Returns a unique identifier for the Function targeted by this request.
    fn function_id(&self) -> Cow<'_, str>;

    /// This is the [`MetadataMap`] accessible through [`tonic::Request`], which can be (cheaply)
    /// converted into a [`http::header::HeaderMap`], which can be converted and serialized as a
    /// [`HashMap`].
    ///
    /// [`http::header::HeaderMap`]: ::http::header::HeaderMap
    /// [`tonic::Request`]: ::tonic::Request
    /// [`MetadataMap`]: ::tonic::metadata::MetadataMap
    fn metadata_map(&self) -> &HashMap<String, String>;

    /// Returns the actual user's serialized payload to be handed to the Function.
    fn payload(&self) -> Bytes;

    /// Consumes the [`Request`] returning its metadata map and payload.
    fn into_parts(self) -> (HashMap<String, String>, Bytes);
}

#[async_trait]
pub trait Source: Send + 'static {
    type Request: Request;

    async fn run(
        self,
        to_dispatcher: mpsc::Sender<Self::Request>,
        quit_rx: broadcast::Receiver<()>,
    ) -> Result<(), Error>;
}
