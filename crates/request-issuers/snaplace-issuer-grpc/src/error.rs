use std::time::Duration;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("failed to connect to '{endpoint}' after {duration:?}")]
    Connection {
        endpoint: Box<str>,
        duration: Duration,
        #[source]
        source: ::tonic::transport::Error,
    },

    #[error("failed to encapsulate Function's response into a ::snaplace::Response")]
    ResponseEncapsulation(#[source] ::snaplace::response::ConstructResponseError),
}
