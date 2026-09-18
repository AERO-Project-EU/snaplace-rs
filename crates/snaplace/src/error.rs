pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("failed to initialize orchestrator: {msg}")]
    OrchestratorInit {
        msg: String,
        #[source]
        source: Box<dyn ::std::error::Error + Send + Sync + 'static>,
    },

    #[error("I/O error: {msg}")]
    Io {
        msg: String,
        #[source]
        source: ::std::io::Error,
    },

    #[error("tonic(gRPC) transport error")]
    Tonic(#[source] ::tonic::transport::Error),
}
