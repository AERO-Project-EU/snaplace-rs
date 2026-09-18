//! TODO: docs
//!
//! NOTE(ckatsak): Integration tests run **serially** according to [the docs][cargo_ref].
//!
//! [cargo_ref]: https://github.com/rust-lang/cargo/blob/rust-1.87.0/src/doc/src/reference/cargo-targets.md#integration-tests

use std::{fmt::Debug, net::Ipv4Addr};

use anyhow::{bail, Context, Result};
use const_format::formatcp;
use serde_json::Value as serdeValue;
use snaplace::worker::{issuer::IssuerResponse, RequestIssuer, SandboxStateRef};
use tracing::{debug, error, info, instrument, trace, warn};

use issuer_aerofb::{snaplace_to_http_req, AeroFbRequestIssuer, FUNCTION_PORT};

pub const OUT_KEY_DURATION: &str = "duration_ns";
pub const OUT_KEY_OUTPUT: &str = "output";

#[allow(dead_code)]
pub const MINIO_ADDRESS: &str = "icy1.cslab.ece.ntua.gr:59000";
#[allow(dead_code)]
pub const MINIO_BUCKET_NAME: &str = "snaplace-fbpml";

#[instrument(skip(snaplace_request))]
pub fn check_request(snaplace_request: impl ::snaplace::Request) -> Result<()> {
    let type_name = ::std::any::type_name_of_val(&snaplace_request);
    trace!(%type_name, ?snaplace_request);
    let http_request = snaplace_to_http_req(
        snaplace_request,
        formatcp!("http://localhost:{FUNCTION_PORT}/invoke"),
    )
    .with_context(|| {
        format!("failed conversion: <{type_name} as ::snaplace::Request> --> ::http::Request")
    })?;
    trace!(?http_request);

    Ok(())
}

#[instrument(skip(sresp))]
pub fn check_response(sresp: impl ::snaplace::Response) -> Result<()> {
    let code = ::tonic::Code::from(sresp.status_code());
    if code != ::tonic::Code::Ok {
        warn!(?code, "UNsuccessful gRPC status");
    } else {
        debug!(?code, "successful gRPC status");
    }

    let out = ::serde_json::from_slice::<serdeValue>(sresp.payload().as_ref())
        .inspect_err(|err| error!(error = ?err, "JSON deserialization of payload failed: {err:#}"))
        .context("JSON deserialization of payload failed")?;
    debug!("{out:?}");

    let serdeValue::Object(ref m) = out else {
        bail!("Unexpected function output: function top-level output is not JSON object: {out:?}");
    };
    let serdeValue::Number(nanosec) = m.get(OUT_KEY_DURATION).with_context(|| {
        format!("Unexpected function output: missing key '{OUT_KEY_DURATION}': {out:?}")
    })?
    else {
        bail!("Unexpected function output: key '{OUT_KEY_DURATION}' does not contain Num: {out:?}");
    };
    let _dur = nanosec
        .as_u64()
        .with_context(|| format!("failed to parse {nanosec:?} as u64"))?;
    let serdeValue::String(function_output) = m.get(OUT_KEY_OUTPUT).with_context(|| {
        format!("Unexpected function output: missing key '{OUT_KEY_OUTPUT}': {out:?}")
    })?
    else {
        bail!("Unexpected function output: key '{OUT_KEY_OUTPUT}' does not contain Str: {out:?}");
    };
    debug!(?function_output);

    Ok(())
}

#[instrument(skip(sreq))]
#[inline]
pub async fn issue_cold<Resp: ::snaplace::Response + Debug>(
    sreq: impl ::snaplace::Request,
) -> ::anyhow::Result<Resp> {
    issue(sreq, SandboxStateRef::Nonexistent).await
}

#[instrument(skip(sreq))]
#[inline]
pub async fn issue_warm<Resp: ::snaplace::Response + Debug>(
    sreq: impl ::snaplace::Request,
) -> ::anyhow::Result<Resp> {
    issue(sreq, SandboxStateRef::Paused).await
}

async fn issue<Resp: ::snaplace::Response + Debug>(
    sreq: impl ::snaplace::Request,
    sandbox_state: SandboxStateRef,
) -> ::anyhow::Result<Resp> {
    let mut issuer = AeroFbRequestIssuer::new().map_err(::anyhow::Error::from_boxed)?;

    let IssuerResponse {
        resp: snaplace_response,
        invocation_duration,
        ..
    } = issuer
        .issue_request(Ipv4Addr::LOCALHOST, sandbox_state, sreq)
        .await
        .inspect(|result| trace!(?result))
        .map_err(::anyhow::Error::from_boxed)?;
    info!(?invocation_duration, ?snaplace_response);

    Ok(snaplace_response)
}

//pub fn create_snaplace_grpc_request(
//    name: &'static str,
//    function_handler_payload: &[u8],
//) -> ::anyhow::Result<::snaplace_grpc::Request> {
//    use std::collections::HashMap;
//    use bytes::Bytes;
//
//    let agent_payload = ::serde_json::to_vec::<HashMap<_, _>>(&HashMap::from_iter([(
//        "payload",
//        function_handler_payload,
//    )]))
//    .context("failed to serialize agent's payload")?;
//
//    Ok(::snaplace_grpc::Request {
//        invocation_id: format!("{name}-cold").into(),
//        function_id: name.into(),
//        metadata_map: Default::default(),
//        payload: Bytes::from(agent_payload),
//    })
//}
