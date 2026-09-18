#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("TODO")]
    Http(#[source] ::http::Error),

    /// Return on failure to parse HTTP header value.
    ///
    /// # Notes
    ///
    /// According to [these] [posts], RFC 7230 specifies that HTTP/1.1 contains only visible ASCII
    /// characters in header values.
    ///
    /// [these]: https://stackoverflow.com/q/69493006
    /// [posts]: https://stackoverflow.com/a/48138818
    #[error("error parsing HTTP header value")]
    HttpHeader(#[source] ::http::header::ToStrError),

    #[error("TODO")]
    Json(#[source] ::serde_json::Error),
    //
    // TODO
    //
}
