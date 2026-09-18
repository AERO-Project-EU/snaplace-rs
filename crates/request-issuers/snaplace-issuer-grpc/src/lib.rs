mod error;
mod issuer;
#[path = "snaplace.internal.worker.rs"]
pub mod pb;

pub use error::Error;
pub use issuer::GrpcRequestIssuer;
pub use pb::function_client::FunctionClient;
pub use pb::FunctionRequest;
pub use pb::FunctionResponse;

pub const FUNCTION_PORT: u16 = 50052;

impl<Req: ::snaplace::Request> From<Req> for FunctionRequest {
    #[inline]
    fn from(req: Req) -> Self {
        let (metadata_map, payload) = req.into_parts();
        Self {
            metadata_map,
            payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use snaplace::worker::RequestIssuer;
    use tracing::debug;

    use super::GrpcRequestIssuer;

    #[::tokio::test]
    #[::tracing_test::traced_test]
    #[should_panic]
    async fn issuer_nonexistent_server() {
        const IP_ADDR: Ipv4Addr = Ipv4Addr::new(147, 102, 4, 81);

        let mut iss =
            GrpcRequestIssuer::<::snaplace_grpc::Request, ::snaplace_grpc::Response>::new()
                .expect("failed to instantiate GrpcRequestIssuer");
        let res = iss
            .issue_request(
                IP_ADDR,
                snaplace::worker::SandboxStateRef::Nonexistent,
                Default::default(),
            )
            .await;
        debug!("{res:#?}");
        res.expect("failed to issue request");
    }
}
