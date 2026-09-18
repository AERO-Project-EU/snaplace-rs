use tokio::sync::broadcast;

/// Error returned when failing to construct a new [`Response`].
///
/// [`Response`]: crate::response::Response
#[derive(Debug, ::thiserror::Error)]
#[error("error constructing new snaplace::Response; custom message: {msg:?}")]
pub struct ConstructResponseError {
    msg: Option<Box<str>>,
    #[source]
    source: Box<dyn ::std::error::Error + Send + Sync + 'static>,
}

impl ConstructResponseError {
    /// Wrap an existing error into a `ConstructResponseError`.
    #[inline]
    pub fn new(source: Box<dyn ::std::error::Error + Send + Sync + 'static>) -> Self {
        Self { msg: None, source }
    }

    /// Wrap an existing error into a `ConstructResponseError`, along with a custom description.
    #[inline]
    pub fn with_message(
        source: Box<dyn ::std::error::Error + Send + Sync + 'static>,
        message: Box<str>,
    ) -> Self {
        Self {
            msg: Some(message),
            source,
        }
    }
}

/// Error type returned by [`Sink`]s.
///
/// [`Sink`]: crate::response::Sink
#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    /// Failure while receiving a [`broadcast`] message (e.g., from the quit channel).
    ///
    /// [`broadcast`]: ::tokio::sync::broadcast
    #[error("response sink: {msg}")]
    Receive {
        msg: Box<str>,
        #[source]
        source: broadcast::error::RecvError,
    },

    /// Generic failure while running the [`response::Sink`].
    ///
    /// [`response::Sink`]: crate::response::Sink
    #[error("response sink: {msg}")]
    Runtime {
        msg: Box<str>,
        #[source]
        source: Box<dyn ::std::error::Error + Send + Sync + 'static>,
    },
}
