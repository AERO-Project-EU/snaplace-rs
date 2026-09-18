use std::{borrow::Cow, collections::HashMap};

use prost::bytes::Bytes;
use tonic::metadata::MetadataMap;

use snaplace::{response::ConstructResponseError, FunctionId, InvocationId};

use crate::Response;

impl ::snaplace::Response for Response {
    // Fallible implementation that gives up if any value cannot be converted to `str`:
    fn try_from_parts(
        invocation_id: &InvocationId,
        _function_id: &FunctionId,
        status_code: ::tonic::Code,
        metadata_map: MetadataMap,
        payload: Bytes,
    ) -> Result<Self, ConstructResponseError> {
        let metadata_map = metadata_map
            .into_headers()
            .iter()
            .flat_map(|(name, val)| {
                Ok::<_, ConstructResponseError>((
                    name.as_str().to_owned(),
                    val.to_str()
                        .map_err(|err| {
                            ConstructResponseError::with_message(
                                Box::new(err),
                                format!("error converting {val:?} to str").into_boxed_str(),
                            )
                        })?
                        .to_owned(),
                ))
            })
            .collect();
        Ok(Self {
            invocation_id: String::from(invocation_id.as_str()),
            status_code: status_code as i32,
            metadata_map,
            payload,
        })
    }
    //
    // Infallible implementation that omits pairs whose value cannot be converted to `str`:
    //fn try_from_parts(
    //    invocation_id: &InvocationId,
    //    _function_id: &FunctionId,
    //    status_code: ::tonic::Code,
    //    metadata_map: MetadataMap,
    //    payload: Bytes,
    //) -> Result<Self, ConstructResponseError> {
    //    let metadata_map = metadata_map
    //        .into_headers()
    //        .iter()
    //        .filter_map(|(name, val)| {
    //            val.to_str()
    //                .ok()
    //                .map(|val| (name.as_str().to_owned(), val.to_owned()))
    //        })
    //        .collect();
    //    Ok(Self {
    //        invocation_id: String::from(invocation_id.as_str()),
    //        status_code: status_code as i32,
    //        metadata_map,
    //        payload,
    //    })
    //}

    #[inline]
    fn invocation_id(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.invocation_id)
    }

    #[inline]
    fn status_code(&self) -> i32 {
        self.status_code
    }

    #[inline]
    fn metadata_map(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata_map
    }

    #[inline]
    fn payload(&self) -> Bytes {
        self.payload.clone()
    }
}
