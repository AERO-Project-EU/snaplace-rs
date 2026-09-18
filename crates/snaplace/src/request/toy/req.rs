use std::{borrow::Cow, collections::HashMap, time::SystemTime};

use prost::bytes::Bytes;

use crate::{request::HEADER_KEY_SOURCE_TIMESTAMP_NS, Request};

#[derive(Debug, Clone)]
pub struct ToyStringRequest(String, HashMap<String, String>);

impl ::std::fmt::Display for ToyStringRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<S: AsRef<str>> From<S> for ToyStringRequest {
    fn from(s: S) -> Self {
        Self(
            s.as_ref().to_string(),
            [(
                HEADER_KEY_SOURCE_TIMESTAMP_NS.to_owned(),
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
                    .to_string(),
            )]
            .into(),
        )
    }
}

impl Request for ToyStringRequest {
    fn invocation_id(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
    }

    fn function_id(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
    }

    fn metadata_map(&self) -> &HashMap<String, String> {
        &self.1
    }

    fn payload(&self) -> Bytes {
        self.0.clone().into()
    }

    fn into_parts(self) -> (HashMap<String, String>, Bytes) {
        (self.1, self.0.into())
    }
}
