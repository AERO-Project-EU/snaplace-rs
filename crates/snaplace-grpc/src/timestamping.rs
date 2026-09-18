use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};

use tokio_stream::Stream;

pub(crate) trait MetadataStamp {
    fn metadata_map_mut(&mut self) -> &mut HashMap<String, String>;
}

pub(crate) struct TimestampingStream<S> {
    inner: S,
    header_key: &'static str,
}

impl<S> TimestampingStream<S> {
    pub(crate) fn new(inner: S, header_key: &'static str) -> Self {
        Self { inner, header_key }
    }
}

impl<S, T, E> Stream for TimestampingStream<S>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    T: MetadataStamp,
{
    type Item = Result<T, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(mut item))) => {
                let epoch_ns = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                let _ = item
                    .metadata_map_mut()
                    .insert(self.header_key.to_string(), epoch_ns.to_string());
                Poll::Ready(Some(Ok(item)))
            }
            other => other,
        }
    }
}

#[cfg(feature = "source")]
impl MetadataStamp for crate::Request {
    #[inline]
    fn metadata_map_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata_map
    }
}

#[cfg(feature = "sink")]
impl MetadataStamp for crate::Response {
    #[inline]
    fn metadata_map_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata_map
    }
}
