mod common;

use std::collections::HashMap;

use anyhow::Context;
use bytes::Bytes;
use const_format::formatcp;
use tracing_test::traced_test;

use crate::common::{
    check_request, check_response, issue_cold, issue_warm, MINIO_ADDRESS, MINIO_BUCKET_NAME,
};

const NAME: &str = "aerofb_rnn_serving";

#[::tokio::test]
#[traced_test]
async fn aerofb_rnn_serving() -> ::anyhow::Result<()> {
    //
    // TODO(ckatsak): fork/exec:
    //      `docker run --rm -it --pull=always -p 8000:8000 ghcr.io/aero-project-eu/aerofb-rnn_serving:0.0.1`
    // or just do it manually before running the test?
    //

    // Create request
    let handler_payload = ::serde_json::to_vec::<HashMap<_, _>>(&HashMap::from_iter([
        ("minio_address", MINIO_ADDRESS),
        ("bucket_name", MINIO_BUCKET_NAME),
        ("language", "Greek"),
        ("start_letters", "QRSTUVWXYZABCDEF"),
        ("model_parameter_object_key", "rnn_params.pkl"),
        ("model_object_key", "rnn_model.pth"),
    ]))
    .context("failed to serialize handler's payload")?;
    let agent_payload =
        ::serde_json::to_vec::<HashMap<_, _>>(&HashMap::from_iter([("payload", handler_payload)]))
            .context("failed to serialize agent's payload")?;
    let mut sreq = ::snaplace_grpc::Request {
        invocation_id: formatcp!("{NAME}-cold").into(),
        function_id: NAME.into(),
        metadata_map: Default::default(),
        payload: Bytes::from(agent_payload),
    };
    //let mut sreq = common::create_snaplace_grpc_request(NAME, &handler_payload)?;
    check_request(sreq.clone())?;

    let sresp = issue_cold::<::snaplace_grpc::Response>(sreq.clone()).await?;
    check_response(sresp)?;

    sreq.invocation_id = formatcp!("{NAME}-warm").into();
    let sresp = issue_warm::<::snaplace_grpc::Response>(sreq).await?;
    check_response(sresp)?;

    Ok(())
}
