use std::net::Ipv4Addr;

use tracing::{instrument, Level};

use crate::{
    worker::{issuer::IssuerResponse, RequestIssuer, SandboxStateRef},
    FunctionId, InvocationId, Request, Response,
};

#[derive(Debug, Clone)]
pub struct ToyIssuer {}

impl<Req: Request, Resp: Response> RequestIssuer<Req, Resp> for ToyIssuer {
    #[inline(always)]
    fn new() -> Result<Self, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        Ok(Self {})
    }

    #[instrument(
        level = Level::INFO,
        skip_all,
        fields(
            %_ip_addr,
            ?_sandbox_state,
            toy_payload = String::from_utf8_lossy(req.payload().as_ref()).into_owned(),
        ),
    )]
    async fn issue_request(
        &mut self,
        _ip_addr: Ipv4Addr,
        _sandbox_state: SandboxStateRef,
        req: Req,
    ) -> Result<IssuerResponse<Resp>, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let function_id = FunctionId::from(req.function_id());
        let invocation_id = InvocationId::from(req.invocation_id());
        // Consume the request, and build a response, trying to simulate the real case
        let payload = req.payload();
        drop(req);

        let resp = Resp::try_from_parts(
            &invocation_id,
            &function_id,
            ::tonic::Code::Ok,
            ::tonic::metadata::MetadataMap::new(),
            payload,
        )
        .expect("TryFrom is infallible in '__toy'");

        Ok(IssuerResponse {
            resp,
            connection_duration: Default::default(),
            invocation_duration: Default::default(),
        })
    }
}
