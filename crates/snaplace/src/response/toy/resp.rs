use std::{borrow::Cow, collections::HashMap, convert::Infallible, fmt::Display};

use prost::bytes::Bytes;

use crate::{response::error::ConstructResponseError, FunctionId, InvocationId, Response};

#[derive(Debug, Clone)]
pub struct ToyStringResponse(String, HashMap<String, String>);

impl Display for ToyStringResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ToyStringResponse {
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[allow(clippy::infallible_try_from)]
impl<'b> TryFrom<&'b [u8]> for ToyStringResponse {
    type Error = Infallible;

    #[inline]
    fn try_from(bytes: &'b [u8]) -> Result<Self, Self::Error> {
        Ok(ToyStringResponse(
            String::from_utf8_lossy(bytes).into_owned(),
            Default::default(),
        ))
    }
}

#[allow(clippy::infallible_try_from)]
impl<'i, 'f, 'b>
    TryFrom<(
        &'i InvocationId,
        &'f FunctionId,
        ::tonic::metadata::MetadataMap,
        &'b [u8],
    )> for ToyStringResponse
{
    type Error = Infallible;

    fn try_from(
        (_iid, _fid, _metadata_map, bytes): (
            &'i InvocationId,
            &'f FunctionId,
            ::tonic::metadata::MetadataMap,
            &'b [u8],
        ),
    ) -> Result<Self, Self::Error> {
        Ok(ToyStringResponse(
            String::from_utf8_lossy(bytes).into_owned(),
            Default::default(),
        ))
    }
}

#[allow(clippy::infallible_try_from)]
impl TryFrom<(InvocationId, FunctionId, Vec<u8>)> for ToyStringResponse {
    type Error = Infallible;

    fn try_from(
        (_iid, _fid, bytes): (InvocationId, FunctionId, Vec<u8>),
    ) -> Result<Self, Self::Error> {
        Ok(ToyStringResponse(
            String::from_utf8_lossy(&bytes).into_owned(),
            Default::default(),
        ))
    }
}

impl Response for ToyStringResponse {
    fn try_from_parts(
        _invocation_id: &InvocationId,
        _function_id: &FunctionId,
        _status_code: ::tonic::Code,
        _metadata_map: tonic::metadata::MetadataMap,
        payload: Bytes,
    ) -> Result<Self, ConstructResponseError> {
        Ok(ToyStringResponse(
            String::from_utf8_lossy(payload.as_ref()).into_owned(),
            Default::default(),
        ))
    }

    #[inline]
    fn invocation_id(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
    }

    //#[inline]
    //fn function_id(&self) -> Cow<'_, str> {
    //    Cow::Borrowed(&self.0)
    //}

    #[inline]
    fn status_code(&self) -> i32 {
        ::tonic::Code::Ok as i32
    }

    #[inline]
    fn metadata_map(&mut self) -> &mut HashMap<String, String> {
        &mut self.1
    }

    #[inline]
    fn payload(&self) -> Bytes {
        self.0.clone().into()
    }
}
