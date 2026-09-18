use crate::metadata::registration;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("I/O error: {msg}")]
    Io {
        msg: Box<str>,
        #[source]
        source: ::std::io::Error,
    },

    #[error("registration service error: {msg}")]
    Registration {
        msg: Box<str>,
        #[source]
        source: registration::Error,
    },

    // TODO
    #[error("tonic(gRPC) transport error")]
    Tonic(#[source] ::tonic::transport::Error),
}
