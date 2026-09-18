use std::{marker::PhantomData, net::Ipv4Addr, time::Duration};

use backon::Retryable;
use tokio::time::Instant;
use tracing::{debug, instrument, trace, Level};

use snaplace::{
    utils::backoff::chained::ChainedBuilder,
    worker::{
        issuer::{IssuerResponse, RequestIssuer},
        SandboxStateRef,
    },
    FunctionId, InvocationId,
};

use crate::{Error, FunctionClient, FunctionRequest, FUNCTION_PORT};

/// TODO: doc
///
/// # Note
///
/// Hard-coded `http` scheme.
#[derive(Debug, Clone)]
pub struct GrpcRequestIssuer<Req, Resp> {
    phantom: PhantomData<fn() -> (Req, Resp)>,
}

impl<Req, Resp> RequestIssuer<Req, Resp> for GrpcRequestIssuer<Req, Resp>
where
    Req: ::snaplace::Request,
    Resp: ::snaplace::Response,
{
    #[inline]
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
        let func_req = FunctionRequest::from(req);

        let endpoint = format!("http://{ip_addr}:{FUNCTION_PORT}");
        // Retry, as the sandbox may have just been created and not accept connections yet
        let mut attempt = 0;
        let conn_start = Instant::now();
        let mut func_client = (|| FunctionClient::connect(endpoint.clone()))
            // FIXME(ckatsak): timeouts?
            .retry(backoff)
            .notify(|err, next_delay| {
                trace!(attempt, ?next_delay, error = ?err);
                attempt += 1;
            })
            .await
            .map_err(|err| Error::Connection {
                endpoint: endpoint.into_boxed_str(),
                duration: conn_start.elapsed(),
                source: err,
            })?;
        let connection_duration = conn_start.elapsed();
        trace!(?connection_duration);

        let issue_start = Instant::now();
        let issue_result = func_client.issue(func_req).await;
        // FIXME(ckatsak): Should we retry the RPC too (given that connection already succeeded)?
        // FIXME(ckatsak): timeout?
        let invocation_duration = issue_start.elapsed();
        trace!(?invocation_duration);

        let resp = match issue_result {
            Ok(func_resp) => {
                let (metadata_map, func_resp, _ext) = func_resp.into_parts();
                Resp::try_from_parts(
                    &invocation_id,
                    &function_id,
                    ::tonic::Code::Ok,
                    metadata_map,
                    func_resp.payload,
                )
            }
            Err(status) => {
                debug!(?status, "Function invocation failed");
                Resp::try_from_parts(
                    &invocation_id,
                    &function_id,
                    status.code(),
                    status.metadata().clone(),
                    status.message().to_owned().into(),
                )
            }
        }
        .map_err(Error::ResponseEncapsulation)?;

        Ok(IssuerResponse {
            resp,
            connection_duration,
            invocation_duration,
        })
    }
}
