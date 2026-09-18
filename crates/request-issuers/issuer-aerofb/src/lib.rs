use std::{marker::PhantomData, net::Ipv4Addr, time::Duration};

use backon::Retryable;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use tokio::time::Instant;
use tracing::{debug, instrument, trace, Level};
use ubyte::ToByteUnit;

use snaplace::{
    utils::backoff::chained::ChainedBuilder,
    worker::{issuer::IssuerResponse, RequestIssuer, SandboxStateRef},
    FunctionId, InvocationId,
};

/// Maximum acceptable size of a Function response body, in bytes.
pub const MAX_RESP_BODY_SIZE: usize = 1 << 25;

pub const FUNCTION_PORT: u16 = 8000;

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    #[error("failed to build HTTP request")]
    RequestBuilder(#[source] ::http::Error),
    #[error(transparent)]
    InvalidHeaderName(::http::header::InvalidHeaderName),
    #[error(transparent)]
    InvalidHeaderValue(::http::header::InvalidHeaderValue),
    #[error(transparent)]
    InvalidUri(::http::uri::InvalidUri),

    #[error("function response body greater than {}", MAX_RESP_BODY_SIZE.bytes())]
    ResponseBodyTooBig(#[source] Box<dyn ::std::error::Error + Send + Sync + 'static>),

    #[error("failed to issue HTTP request to '{endpoint}' after {duration:?}")]
    HyperClient {
        endpoint: Box<str>,
        duration: Duration,
        source: ::hyper_util::client::legacy::Error,
    },
    #[error("failed to encapsulate Function's response into a ::snaplace::Response")]
    ResponseEncapsulation(#[source] ::snaplace::response::ConstructResponseError),
}

#[derive(Debug, Clone, Copy)]
pub struct AeroFbRequestIssuer<Req, Resp> {
    phantom: PhantomData<fn() -> (Req, Resp)>,
}

impl<Req, Resp> RequestIssuer<Req, Resp> for AeroFbRequestIssuer<Req, Resp>
where
    Req: ::snaplace::Request,
    Resp: ::snaplace::Response,
{
    #[inline(always)]
    fn new() -> Result<Self, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        Ok(Self {
            phantom: PhantomData,
        })
    }

    #[instrument(level = Level::TRACE, skip_all, fields(%ip_addr, ?sandbox_state))]
    async fn issue_request(
        &mut self,
        ip_addr: Ipv4Addr,
        sandbox_state: SandboxStateRef,
        req: Req,
    ) -> Result<IssuerResponse<Resp>, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let backoff: ChainedBuilder<::backon::ConstantBuilder, ::backon::ConstantBuilder> =
            if let SandboxStateRef::Nonexistent = sandbox_state {
                // gives up after ~5000-10000 ms
                ChainedBuilder::single(
                    ::backon::ConstantBuilder::default()
                        .with_delay(Duration::from_millis(25))
                        .with_max_times(200)
                        .with_jitter(), // > 5000ms, < 10000ms
                )
            } else {
                // gives up after ~375-750 ms
                ChainedBuilder::single(
                    ::backon::ConstantBuilder::default()
                        .with_delay(Duration::from_millis(5))
                        .with_max_times(75)
                        .with_jitter(), // > 375ms, < 750ms
                )
            };

        let function_id = FunctionId::from(req.function_id());
        let invocation_id = InvocationId::from(req.invocation_id());

        let endpoint = format!("http://{ip_addr}:{FUNCTION_PORT}/invoke");
        let func_req = snaplace_to_http_req(req, &endpoint)?;

        let func_client = Client::builder(TokioExecutor::new())
            .http1_allow_spaces_after_header_name_in_responses(true)
            .http1_ignore_invalid_headers_in_responses(true)
            .build_http();

        // Retry, as the sandbox may have just been created and not accept connections yet
        let mut attempt = 0;
        let conn_and_issue_start = Instant::now();
        let (parts, inc) = (|| func_client.request(func_req.clone()))
            .retry(backoff)
            .notify(|err, next_delay| {
                trace!(attempt, ?next_delay, error = ?err);
                attempt += 1;
            })
            .await
            .map_err(|err| {
                if err.is_connect() {
                    debug!(error = ?err, "Failed to connect to HTTP server in sandbox");
                }
                Error::HyperClient {
                    endpoint: endpoint.into_boxed_str(),
                    duration: conn_and_issue_start.elapsed(),
                    source: err,
                }
            })?
            .into_parts();
        trace!(?parts, "Just received HTTP Response");

        let body = Limited::new(inc, MAX_RESP_BODY_SIZE)
            .collect()
            .await
            .map_err(Error::ResponseBodyTooBig)?;
        let invocation_duration = conn_and_issue_start.elapsed();
        let payload = body.to_bytes();

        Ok(IssuerResponse {
            resp: Resp::try_from_parts(
                &invocation_id,
                &function_id,
                http_to_grpc_code(parts.status.as_u16()),
                ::tonic::metadata::MetadataMap::from_headers(parts.headers),
                payload,
            )
            .map_err(Error::ResponseEncapsulation)?,
            connection_duration: Duration::ZERO,
            invocation_duration,
        })
    }
}

pub fn snaplace_to_http_req(
    sreq: impl ::snaplace::Request,
    endpoint: &str,
) -> Result<::http::Request<Full<Bytes>>, Error> {
    let (metadata_map, payload) = sreq.into_parts();

    let mut builder = ::http::Request::builder();
    let headers = builder
        .headers_mut()
        .expect("freshly constructed Builder has no errors");
    for (k, v) in metadata_map {
        let header_name =
            ::http::HeaderName::from_bytes(k.as_bytes()).map_err(Error::InvalidHeaderName)?;
        let header_value =
            ::http::HeaderValue::from_bytes(v.as_bytes()).map_err(Error::InvalidHeaderValue)?;
        let _old_header_value = headers.insert(header_name, header_value);
        debug_assert!(_old_header_value.is_none(), "empty builder has no headers");
        // FIXME:  ^^  Can `::snaplace::Request`'s `metadata_map` contain duplicates though?
    }

    builder
        .version(::http::Version::HTTP_11)
        .method(::http::Method::POST)
        .uri(endpoint.parse::<::http::Uri>().map_err(Error::InvalidUri)?)
        .body(Full::new(payload))
        .map_err(Error::RequestBuilder)
}

/// Also see:
/// https://github.com/googleapis/googleapis/blob/da24f941319741abf7a9f6fc8451b636bcb61a20/google/rpc/code.proto
fn http_to_grpc_code(http_status_code: u16) -> ::tonic::Code {
    match http_status_code {
        200 => ::tonic::Code::Ok,
        400 => ::tonic::Code::InvalidArgument, // also: FailedPrecondition, OutOfRange
        401 => ::tonic::Code::Unauthenticated,
        403 => ::tonic::Code::PermissionDenied,
        404 => ::tonic::Code::NotFound,
        409 => ::tonic::Code::AlreadyExists, // also: Aborted
        429 => ::tonic::Code::ResourceExhausted,
        499 => ::tonic::Code::Cancelled,
        500 => ::tonic::Code::Internal, // also: Unknown, DataLoss
        503 => ::tonic::Code::Unavailable,
        504 => ::tonic::Code::DeadlineExceeded,
        _ => ::tonic::Code::Unknown,
    }
}

::static_assertions::assert_impl_all!(Error: Send, Sync);
#[cfg(test)]
::static_assertions::assert_impl_all!(
    AeroFbRequestIssuer<::snaplace_grpc::Request, ::snaplace_grpc::Response>: Send, Sync
);
