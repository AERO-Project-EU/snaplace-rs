mod common;

use bytes::Bytes;
use const_format::formatcp;
use tracing_test::traced_test;

use crate::common::{check_request, check_response, issue_cold, issue_warm};

const NAME: &str = "aerofb_helloworld";

#[::tokio::test]
#[traced_test]
async fn aerofb_helloworld() -> ::anyhow::Result<()> {
    //
    // TODO(ckatsak): fork/exec:
    //      `docker run --rm -it --pull=always -p 8000:8000 ghcr.io/aero-project-eu/aerofb-helloworld:0.0.1`
    // or just do it manually before running the test?
    //

    // Create request
    let mut sreq = ::snaplace_grpc::Request {
        invocation_id: formatcp!("{NAME}-cold").into(),
        function_id: NAME.into(),
        metadata_map: Default::default(),
        payload: Bytes::from_static(b"{}"),
    };
    check_request(sreq.clone())?;

    // Issue request & retrieve response
    //let IssuerResponse {
    //    resp: snaplace_response,
    //    invocation_duration,
    //    ..
    //}: IssuerResponse<::snaplace_grpc::Response> = issuer
    //    .issue_request(Ipv4Addr::LOCALHOST, SandboxStateRef::Nonexistent, sreq)
    //    .await
    //    .inspect(|result| trace!(?result))
    //    .map_err(::anyhow::Error::from_boxed)?;
    //info!(?invocation_duration, ?snaplace_response);
    //let snaplace_response = issue::<::snaplace_grpc::Response>(sreq).await?;
    let sresp = issue_cold::<::snaplace_grpc::Response>(sreq.clone()).await?;
    check_response(sresp)?;

    sreq.invocation_id = formatcp!("{NAME}-warm").into();
    let sresp = issue_warm::<::snaplace_grpc::Response>(sreq).await?;
    check_response(sresp)?;

    Ok(())
}
