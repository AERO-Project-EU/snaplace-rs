use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{self, stdout, BufReader},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use argh::FromArgs;
use prost::bytes::Bytes;
use serde::Deserialize;
use tokio::time::Instant;
use tonic::transport::Uri;
use tracing::{debug, error, trace};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use snaplace::worker::issuer::{HEADER_KEY_HANDLER_DURATION, HEADER_KEY_RESPONSE_DURATION};
use snaplace_issuer_grpc::{FunctionClient, FunctionRequest};

#[derive(Debug, Deserialize, Default)]
struct JsonFunctionRequest {
    payload: Vec<u8>,
    metadata_map: HashMap<String, String>,
}

impl From<JsonFunctionRequest> for FunctionRequest {
    fn from(jfreq: JsonFunctionRequest) -> Self {
        Self {
            payload: Bytes::from(jfreq.payload),
            metadata_map: jfreq.metadata_map,
        }
    }
}

/// Cli to issue FunctionRequests to snaplace Functions, similar to how snaplace Workers do so.
#[derive(Debug, FromArgs)]
struct Cli {
    /// JSON-serialized `FunctionRequest` to be used as input. Prepend a '@' to indicate a
    /// filesystem path with the input.
    #[argh(option, short = 'i', from_str_fn(json_func_req))]
    input: JsonFunctionRequest,

    /// gRPC server address
    #[argh(positional, from_str_fn(parse_uri))]
    addr: Uri,
}

fn parse_uri(arg: &str) -> ::std::result::Result<Uri, String> {
    arg.parse::<Uri>().map_err(|err| {
        format!("failed to parse URI from the provided gRPC server address: {err:#}")
    })
}

fn json_func_req(arg: &str) -> ::std::result::Result<JsonFunctionRequest, String> {
    if matches!(arg.chars().next(), Some('@')) {
        func_req_from_file(arg.split_at(1).1)
    } else {
        func_req_from_cmd(arg)
    }
}

#[inline]
fn func_req_from_file(filepath: &str) -> ::std::result::Result<JsonFunctionRequest, String> {
    let mut br = BufReader::with_capacity(
        1 << 14, // 4 pages
        OpenOptions::new()
            .read(true)
            .open(filepath)
            .map_err(|err| format!("failed to open (JSON?) file '{filepath}': {err:#}"))?,
    );
    ::serde_json::from_reader(&mut br)
        .map_err(|err| format!("failed to (JSON-)deserialize FunctionRequest: {err:#}"))
}

#[inline]
fn func_req_from_cmd(data: &str) -> ::std::result::Result<JsonFunctionRequest, String> {
    ::serde_json::from_str(data)
        .map_err(|err| format!("failed to (JSON-)deserialize FunctionRequest: {err:#}"))
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
    trace!("{cli:#?}");

    let t_conn_start = Instant::now();
    let mut client = FunctionClient::connect(cli.addr)
        .await
        .with_context(|| "failed to connect to gRPC server")?;
    let conn_dur = t_conn_start.elapsed();

    let t_client_start = Instant::now();
    let resp = client.issue(FunctionRequest::from(cli.input)).await;
    let client_dur = t_client_start.elapsed();
    let resp = match resp {
        Ok(resp) => resp,
        Err(status) => {
            error!("{status:#?}");
            bail!("FAILURE");
        }
    };
    trace!("{resp:#?}");

    let metadata_map = resp.metadata().clone().into_headers();
    debug!("{metadata_map:#?}");
    let parse_dur = |hkey| {
        metadata_map
            .get(hkey)
            .and_then(|hval| hval.to_str().ok())
            .and_then(|ns_str| ns_str.parse().ok())
            .map(Duration::from_nanos)
            .unwrap_or_default()
    };
    let response_dur = parse_dur(HEADER_KEY_RESPONSE_DURATION);
    let handler_dur = parse_dur(HEADER_KEY_HANDLER_DURATION);

    let fresp = resp.into_inner();
    debug!("payload: {:?}", fresp.payload);
    //let payload = ::serde_json::from_slice::<HashMap<String, String>>(&fresp.payload)
    //    .with_context(|| "failed to (JSON-)deserialize response's payload")?;
    //debug!("{payload:?}");

    //println!(
    //    "{},{},{},{}",
    //    conn_dur.as_millis(),
    //    client_dur.as_millis(),
    //    response_dur.as_millis(),
    //    handler_dur.as_millis()
    //);
    let stdout = stdout().lock();
    ::serde_json::to_writer(
        stdout,
        &[
            ("conn_ms", conn_dur.as_millis()),
            ("client_ms", client_dur.as_millis()),
            ("response_ms", response_dur.as_millis()),
            ("handler_ms", handler_dur.as_millis()),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>(),
    )
    .with_context(|| "failed to JSON-serialize output")?;

    Ok(())
}
