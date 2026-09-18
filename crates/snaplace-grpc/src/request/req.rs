use std::{borrow::Cow, collections::HashMap};

use prost::bytes::Bytes;

use crate::{Error, Request};

impl ::snaplace::Request for Request {
    #[inline]
    fn invocation_id(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.invocation_id.as_str())
    }

    #[inline]
    fn function_id(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.function_id)
    }

    #[inline]
    fn payload(&self) -> Bytes {
        self.payload.clone()
    }

    #[inline]
    fn metadata_map(&self) -> &HashMap<String, String> {
        &self.metadata_map
    }

    #[inline]
    fn into_parts(self) -> (HashMap<String, String>, Bytes) {
        (self.metadata_map, self.payload)
    }
}

impl<I, F> TryFrom<(I, F, &::http::HeaderMap, Bytes)> for Request
where
    I: Into<String>,
    F: Into<String>,
{
    type Error = Error;

    fn try_from(
        (invocation_id, function_id, http_headers, bytes): (I, F, &::http::HeaderMap, Bytes),
    ) -> Result<Self, Self::Error> {
        let metadata_map = http_headers
            .iter()
            .flat_map(|(name, val)| {
                Ok::<_, Self::Error>((
                    name.as_str().to_owned(),
                    val.to_str().map_err(Error::HttpHeader)?.to_owned(),
                ))
            })
            .collect();
        Ok(Self {
            invocation_id: invocation_id.into(),
            function_id: function_id.into(),
            metadata_map,
            payload: bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use http::HeaderMap;

    use crate::{Error, Request};

    #[test]
    fn t01() -> Result<(), Error> {
        let payload = ::serde_json::to_vec(
            &[(String::from("a"), 1), (String::from("b"), 2)]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        )
        .expect("serde_json");

        let mut req = Request::try_from(("invID", "funcID", &HeaderMap::new(), payload.into()))?;
        eprintln!("{req:#?}");

        req.invocation_id = String::from("skata");
        eprintln!("{req:#?}");

        Ok(())
    }
}
