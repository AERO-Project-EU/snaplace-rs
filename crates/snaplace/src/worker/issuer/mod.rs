#[cfg(feature = "__toy")]
pub mod toy;

use std::{net::Ipv4Addr, time::Duration};

use crate::worker::SandboxStateRef;

pub const HEADER_KEY_RESPONSE_DURATION: &str = "response-duration-ns";
pub const HEADER_KEY_HANDLER_DURATION: &str = "handler-duration-ns";

#[derive(Debug)]
pub struct IssuerResponse<Resp> {
    pub resp: Resp,

    pub connection_duration: Duration,
    pub invocation_duration: Duration,
}

pub trait RequestIssuer<Req, Resp>
where
    Self: Sized + Send + Sync + 'static,
{
    fn new() -> Result<Self, Box<dyn ::std::error::Error + Send + Sync + 'static>>;

    fn issue_request(
        &mut self,
        ip_addr: Ipv4Addr,
        sandbox_state: SandboxStateRef,
        req: Req,
    ) -> impl Future<
        Output = Result<IssuerResponse<Resp>, Box<dyn ::std::error::Error + Send + Sync + 'static>>,
    > + Send;
}
