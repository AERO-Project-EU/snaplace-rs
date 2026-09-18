use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{self, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use argh::FromArgs;
use dashmap::DashMap;
use futures::StreamExt;
use serde::Deserialize;
use tokio::{sync::mpsc, time::Instant};
use tracing::{error, info, trace, warn};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

/// snaplace-grpc client
#[derive(Debug, FromArgs)]
struct Cli {
    /// path to file containing JSON-serialized snaplace-grpc requests
    #[argh(option, short = 'r')]
    requests: PathBuf,

    /// the `IP_ADDR:PORT` that the gRPC RequestSource should listen on
    #[argh(option, default = "String::from(\"0.0.0.0:60051\")")]
    source_addr: String,

    /// the `IP_ADDR:PORT` that the gRPC ResponseSink should listen on
    #[argh(option, default = "String::from(\"0.0.0.0:60052\")")]
    sink_addr: String,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
struct JsonSnaplaceGrpcRequest {
    invocation_id: Option<String>,
    function_id: String,
    headers: Option<HashMap<String, String>>,
    payload: ::serde_json::Value,
}

impl TryFrom<JsonSnaplaceGrpcRequest> for ::snaplace_grpc::Request {
    type Error = ::anyhow::Error;

    fn try_from(jreq: JsonSnaplaceGrpcRequest) -> Result<Self> {
        Ok(::snaplace_grpc::Request {
            invocation_id: jreq.invocation_id.context("InvocationID still unset")?,
            function_id: jreq.function_id,
            metadata_map: jreq.headers.unwrap_or_default(),
            payload: match jreq.payload {
                ::serde_json::Value::Object(m) => ::serde_json::to_vec(&m)
                    .with_context(|| format!("failed to serialize object '{m:?}'"))?
                    .into(),
                ::serde_json::Value::String(s) => s.into_bytes().into(),
                ::serde_json::Value::Array(arr) => {
                    // this only really makes sense if it's already a byte array
                    let arr = arr
                        .iter()
                        .map(|v| {
                            Ok::<_, ::anyhow::Error>(v.as_u64().with_context(|| {
                                format!("JSON value '{v}' in payload array is not unsigned integer")
                            })? as u8)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let ret = String::from_utf8(arr.clone()).with_context(|| {
                        format!("failed to convert byte array '{arr:?}' to String")
                    })?;
                    if arr.len() != ret.len() {
                        warn!(
                            input.array = ?arr, serialized.array = ?ret,
                            "input array length == {} != {} == serialized array length",
                            arr.len(), ret.len(),
                        );
                        Err(anyhow!(
                            "input array length == {} != {} == serialized array length",
                            arr.len(),
                            ret.len()
                        ))?;
                    }
                    ret.into()
                }
                ::serde_json::Value::Null => "".into(),
                val => Err(anyhow!("invalid payload '{val:?}'"))?,
            },
        })
    }
}

fn json_snap_reqs(arg: &str) -> ::std::result::Result<Vec<::snaplace_grpc::Request>, String> {
    parse_requests(arg)
        .map_err(|err| format!("error while parsing requests: {err:#}"))?
        .into_iter()
        .enumerate()
        .map(|(i, mut jreq)| {
            if jreq.invocation_id.is_none() {
                jreq.invocation_id = Some(format!("{i:020}"));
            }
            jreq.try_into().map_err(|err| format!("{err:#}"))
        })
        .collect::<Result<_, _>>()
}

fn parse_requests(path: impl AsRef<Path>) -> Result<Vec<JsonSnaplaceGrpcRequest>> {
    let mut br = BufReader::with_capacity(
        1 << 14,
        OpenOptions::new().read(true).open(&path).with_context(|| {
            format!("failed to open requests file '{}'", path.as_ref().display())
        })?,
    );
    ::serde_json::from_reader(&mut br).with_context(|| {
        format!(
            "failed to deserialize JSON requests at '{}'",
            path.as_ref().display()
        )
    })
}

#[::tokio::main]
async fn main() -> Result<()> {
    ::tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_thread_ids(true)
        .with_line_number(true)
        //.with_thread_names(true)
        .try_init()
        .map_err(|err| anyhow!("failed to initialize tracing subscriber: {err:#}"))?;

    let cli = ::argh::from_env::<Cli>();

    let reqs = json_snap_reqs(cli.requests.to_str().with_context(|| {
        format!(
            "failed to convert path '{}' to UTF-8 string",
            cli.requests.display()
        )
    })?)
    .map_err(|err| anyhow!(err))?;

    let ts_tx = Arc::new(DashMap::with_capacity(reqs.len()));
    let ts_rx = Arc::new(DashMap::with_capacity(reqs.len()));

    //
    // Source
    //
    let mut src = ::snaplace_grpc::SourceClient::connect(format!("http://{}", &cli.source_addr))
        .await
        .context("failed to connect to RequestSource")?;
    let src = ::tokio::spawn({
        let ts_tx = Arc::clone(&ts_tx);
        async move {
            match src
                .snaplace_requests(::tokio_stream::iter(reqs.into_iter()).inspect(move |req| {
                    ts_tx.insert(req.invocation_id.clone(), Instant::now());
                }))
                .await
            {
                Ok(resp) => {
                    info!(?resp, "Client-side streaming ended");
                    Ok(())
                }
                Err(status) => {
                    error!(?status, "Client-side streaming failed: {status:#}");
                    Err(anyhow!("Client-side streaming failed with: {status:?}"))
                }
            }
        }
    });

    let (ltx, mut lrx) = mpsc::channel(ts_rx.capacity());

    //
    // Sink
    //
    let mut snk = ::snaplace_grpc::SinkClient::connect(format!("http://{}", &cli.sink_addr))
        .await
        .context("failed to connect to ResponseSink")?;
    let snk = ::tokio::spawn({
        let ts_rx = Arc::clone(&ts_rx);
        async move {
            match snk.snaplace_responses(::snaplace_grpc::Subscribe {}).await {
                Ok(resp) => {
                    let mut stream = resp.into_inner();
                    while let Some(resp) = stream
                        .message()
                        .await
                        .context("failed to receive next stream message")?
                    {
                        ts_rx.insert(resp.invocation_id.clone(), Instant::now());
                        ltx.send(resp)
                            .await
                            .context("failed to forward Response to logging task")?;
                    }
                }
                Err(status) => error!(?status, "Server-side streaming failed"),
            }
            Ok::<_, ::anyhow::Error>(())
        }
    });

    //
    // Logger
    //
    let log = ::tokio::spawn(async move {
        while let Some(resp) = lrx.recv().await {
            let cpd = if let Some(ts_rx) = ts_rx.get(&resp.invocation_id)
                && let Some(ts_tx) = ts_tx.get(&resp.invocation_id)
            {
                ts_rx.duration_since(*ts_tx)
            } else {
                Duration::MAX
            };
            info!(%resp.invocation_id, %resp.status_code, ?resp.metadata_map, client.perceived.latency = ?cpd);

            match ::std::str::from_utf8(&resp.payload) {
                Ok(payload) => trace!(?payload),
                Err(err) => warn!("Received payload is not valid UTF-8: {err}"),
            }
        }
    });

    // TODO(ckatsak): graceful shut down FIXME

    let (src, snk, log) = ::tokio::join!(src, snk, log);
    src.context("error joining source client")?
        .context("error in source client")?;
    snk.context("error joining sink client")?
        .context("error in sink client")?;
    log.context("error joining logging task")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::{Context, Result};
    use pretty_assertions::assert_eq;
    use tracing::debug;
    use tracing_test::traced_test;

    use crate::JsonSnaplaceGrpcRequest;

    const IN_STR: &str = r#"{
        "function_id": "chameleon-61939efde6a3f9",
        "invocation_id": "skata",
        "payload": "{\"ncol\":76,\"nrow\":76}"
    }"#;
    const IN_OBJ: &str = r#"{
        "function_id": "chameleon-61939efde6a3f9",
        "invocation_id": "skata",
        "payload": { "ncol": 76, "nrow": 76 }
    }"#;
    const IN_BYTES: &str = r#"{
        "function_id": "chameleon-61939efde6a3f9",
        "invocation_id": "skata",
        "payload": [123, 34, 110, 99, 111, 108, 34, 58, 55, 54, 44, 34, 110, 114, 111, 119, 34, 58, 55, 54, 125]
    }"#;
    //"payload": [123, 34, 110, 99, 111, 108, 34, 58, 55, 54, 44, 34, 110, 114, 111, 119, 34, 58, 55, 54, 125]             // NO whitespace
    //"payload": [123, 34, 110, 114, 111, 119, 34, 58, 32, 55, 54, 44, 32, 34, 110, 99, 111, 108, 34, 58, 32, 55, 54, 125] // with whitespace

    #[test]
    #[traced_test]
    fn test_parse_json_snap_reqs() -> Result<()> {
        let jreq_str = JsonSnaplaceGrpcRequest {
            function_id: String::from("chameleon-61939efde6a3f9"),
            invocation_id: Some(String::from("skata")),
            headers: None,
            payload: ::serde_json::Value::String(String::from("{\"ncol\":76,\"nrow\":76}")),
        };

        let input_str = ::serde_json::from_str::<JsonSnaplaceGrpcRequest>(IN_STR)
            .context("failed to deserialize IN_STR")?;
        debug!("input_str = {input_str:?}");
        assert_eq!(input_str, jreq_str);
        assert_eq!(input_str.payload, jreq_str.payload);

        let sreq_str = TryInto::<::snaplace_grpc::Request>::try_into(jreq_str)
            .context("failed to convert JsonSnaplaceGrpcRequest to ::snaplace_grpc::Request")?;
        debug!("sreq_str = {sreq_str:?}");

        let jreq_obj = ::serde_json::from_str::<JsonSnaplaceGrpcRequest>(IN_OBJ)
            .context("failed to deserialize IN_OBJ")?;
        debug!("jreq_obj = {jreq_obj:?}");
        let sreq_obj = TryInto::<::snaplace_grpc::Request>::try_into(jreq_obj)
            .context("failed to convert JsonSnaplaceGrpcRequest to ::snaplace_grpc::Request")?;
        debug!("sreq_obj = {sreq_obj:?}");
        assert_eq!(sreq_obj, sreq_str);

        let jreq_bytes = ::serde_json::from_str::<JsonSnaplaceGrpcRequest>(IN_BYTES)
            .context("failed to deserialize IN_BYTES")?;
        debug!("jreq_bytes = {jreq_bytes:?}");
        let sreq_bytes = TryInto::<::snaplace_grpc::Request>::try_into(jreq_bytes)
            .context("failed to convert JsonSnaplaceGrpcRequest to ::snaplace_grpc::Request")?;
        debug!("sreq_bytes = {sreq_bytes:?}");
        assert_eq!(sreq_bytes, sreq_str);

        Ok(())
    }
}
