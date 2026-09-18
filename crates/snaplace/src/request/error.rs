use tokio::sync::broadcast;

/// Error type returned by [`Source`]s.
#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    /// Failure while forwarding a request as a message to the (internal) channel.
    #[error("request source: {msg}")]
    Send { msg: Box<str> },

    /// Failure while receiving a [`broadcast`] message (e.g., from the quit channel).
    #[error("request source: {msg}")]
    Receive {
        msg: Box<str>,
        #[source]
        source: broadcast::error::RecvError,
    },

    /// Generic failure while running the request [`Source`].
    #[error("request source: {msg}")]
    Runtime {
        msg: Box<str>,
        #[source]
        source: Box<dyn ::std::error::Error + Send + Sync + 'static>,
    },
}
