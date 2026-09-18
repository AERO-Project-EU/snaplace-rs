pub(super) type Result<T> = ::std::result::Result<T, self::Error>;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("channel error: {msg}")]
    Channel {
        msg: Box<str>,
        #[source]
        source: Box<dyn ::std::error::Error + Send + Sync + 'static>,
    },

    #[error("snapshot placement policy error: {msg}")]
    Placement {
        msg: Box<str>,
        #[source]
        source: Box<dyn ::std::error::Error + Send + Sync + 'static>,
    },
}
