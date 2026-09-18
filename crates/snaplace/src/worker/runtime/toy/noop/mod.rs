use std::{
    borrow::Cow,
    net::Ipv4Addr,
    sync::atomic::{AtomicU32, Ordering},
};

use anyhow::{anyhow, Context};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use prost::Message;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::{debug, instrument, trace, Level};
use ubyte::{ByteUnit, ToByteUnit};

use crate::{
    metadata::{
        registration::pb::{function_info::RuntimeSpecific, RegisterFunctionRequest},
        FunctionInfo, SandboxStats,
    },
    metrics::Timing,
    worker::{
        runtime::{DestroySandboxRuntimeError, SnapshotState},
        Runtime, Sandbox,
    },
    FunctionId, SandboxId,
};

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// Error
//
///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, ::thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] ::anyhow::Error);

impl From<Error> for ::tonic::Status {
    fn from(err: Error) -> Self {
        ::tonic::Status::internal(format!("{err:?}: {err:#}"))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// FunctionInfo
//
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Function metadata needed by [`NoOpRt`] for `snaplace` bookkeeping.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct NoOpFunctionInfo {
    id: FunctionId,
    #[serde(deserialize_with = "crate::utils::ser_de::deserialize_byteunit")]
    memory: ByteUnit,
}

impl FunctionInfo for NoOpFunctionInfo {
    fn id(&self) -> &FunctionId {
        &self.id
    }

    fn memory(&self) -> ByteUnit {
        self.memory
    }
}

/// Minimal structural view of runtime-specific registration payloads accepted by
/// `NoOpRt`.
///
/// `faasctl` currently registers real-runtime function metadata as a
/// `prost_types::Any` containing either the `FunctionInfo` proto of either
/// `rt-fc` or `rt-fcctrd`.
/// `NoOpRt` does not need image names, vCPU counts, entrypoints, snapshot
/// hints, or any other runtime-specific data. It only needs the fields that
/// `snaplace` itself uses for admission/pool bookkeeping: `function_id` and
/// requested memory.
///
/// This works because the supported real-runtime protos intentionally share the
/// same wire layout for those fields:
/// - tag 1: `function_id: string`
/// - tag 2: `memory_mib: uint32`
///
/// Protobuf decoding ignores unknown fields, so this smaller message can decode
/// both payloads while discarding the rest. That keeps `__toy` independent from
/// `rt-fc` / `rt-fcctrd` feature flags and avoids coupling the profiling runtime
/// to concrete production runtime types.
///
/// # Maintenance Note
///
/// <div class="warning">
/// This is a wire-layout compatibility assumption, not a semantic conversion
/// through the real runtime structs. If future registration payloads move or
/// or rename these fields, change their protobuf tags, or encode memory in
/// different units, this decoder must be updated accordingly.
/// </div>
///
/// If future runtimes need different runtime-specific metadata, keep this no-op
/// decoder focused on the common fields `snaplace` itself needs. A broader fix
/// would be to make registration carry `function_id` and memory in a
/// runtime-neutral part of the request, leaving only truly runtime-specific
/// data inside the `Any` payload.
#[derive(Clone, PartialEq, ::prost::Message)]
struct RuntimeInfoForNoOp {
    #[prost(string, tag = "1")]
    function_id: String,

    #[prost(uint32, tag = "2")]
    memory_mib: u32,
}

impl TryFrom<RegisterFunctionRequest> for NoOpFunctionInfo {
    type Error = Error;

    fn try_from(req: RegisterFunctionRequest) -> Result<Self, Self::Error> {
        trace!(?req);
        let fi = req.function_info.ok_or_else(|| {
            anyhow!("RegisterFunctionRequest -> NoOpFunctionInfo: empty request".to_owned())
        })?;

        match fi.runtime_specific {
            Some(RuntimeSpecific::B64Json(base64_bytes)) => {
                let json_bytes = BASE64
                    .decode(base64_bytes)
                    .map_err(|err| Error(anyhow!("failed to decode base64: {err:#}")))?;
                trace!(base64.decoded.data = ?String::from_utf8_lossy(&json_bytes));
                let fi = ::serde_json::from_slice(&json_bytes)
                    .map_err(|err| Error(anyhow!("failed to deserialize JSON: {err:#}")))?;
                Ok(fi)
            }
            Some(RuntimeSpecific::Proto3(any)) => {
                debug!(?any, "received protobuf");
                let fi_noop = RuntimeInfoForNoOp::decode(any.value.as_slice()).context(
                    "pb::function_info::RuntimeSpecific::Proto3 -> RuntimeInfoForNoOp: decode failed",
                )?;
                Ok(Self {
                    id: FunctionId::from(fi_noop.function_id),
                    memory: fi_noop.memory_mib.mebibytes(),
                })
            }
            None => Err(Error(anyhow!(
                "RegisterFunctionRequest -> NoOpFunctionInfo: empty request"
            ))),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// Sandbox & SnapshotState
//
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Synthetic [`Sandbox`] used by [`NoOpRt`].
///
/// It carries only the identity and stats required by `snaplace`; it has
/// no process, VM, network endpoint, or restorable snapshot behind it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoOpSandbox {
    id: SandboxId,
    function_id: FunctionId,
    stats: SandboxStats,
}

impl Sandbox for NoOpSandbox {
    type SnapshotState = Self;

    fn id(&self) -> &str {
        &self.id
    }

    fn function_id(&self) -> FunctionId {
        self.function_id.clone()
    }

    fn ip_addr(&self) -> Ipv4Addr {
        Ipv4Addr::UNSPECIFIED
    }

    fn has_snapshot(&self) -> bool {
        false
    }

    fn monitor_targets(&self) -> Vec<crate::snapman::MonitorTarget> {
        Vec::new()
    }

    fn stats(&self) -> &SandboxStats {
        &self.stats
    }

    fn stats_mut(&mut self) -> &mut SandboxStats {
        &mut self.stats
    }
}

impl SnapshotState for NoOpSandbox {
    fn id(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.id)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// Runtime
//
///////////////////////////////////////////////////////////////////////////////////////////////////

static NEXT_ID: AtomicU32 = AtomicU32::new(0);

/// Runtime implementation that exercises `snaplace` without external execution.
///
/// `NoOpRt` accepts registrations and creates [`NoOpSandbox`]es, but performs
/// no VM, container, network, snapshot, or function invocation work. It is meant
/// for profiling `snaplace`-side overheads with realistic control/request flows.
#[derive(Debug)]
pub struct NoOpRt {
    //sandbox_id: Option<SandboxId>,
}

impl Runtime for NoOpRt {
    // `NoOpRt` ignores Config anyway, while accepts:
    // - object, array, string, number, bool
    // - null (as `None`)
    // - missing "workers.runtime": likely accepted as `None`?
    type Config = Option<::serde_json::Value>;
    type Sandbox = NoOpSandbox;
    type NetResource = ();
    type FunctionInfo = NoOpFunctionInfo;

    const NAME: &'static str = "noop";

    fn new(
        _config: &Self::Config,
        _function_info: &Self::FunctionInfo,
        _sandbox: Option<&Self::Sandbox>,
    ) -> Result<Self, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        Ok(Self {
            //sandbox_id: _sandbox.map(|s| s.id.clone()),
        })
    }

    fn init(
        &mut self,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send
    {
        ::futures::future::ready(Ok(()))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn create_sandbox(
        &mut self,
        function_id: &FunctionId,
        _net_resource: Self::NetResource,
        timings: &mut enum_map::EnumMap<crate::metrics::Timing, crate::metrics::Nanoseconds>,
    ) -> Result<Self::Sandbox, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let create_sandbox_start = Instant::now();
        defer! {
            timings[Timing::CreateSandbox] = create_sandbox_start.elapsed().as_nanos() as _;
        }
        Ok(NoOpSandbox {
            id: ::compact_str::format_compact!("{:024}", NEXT_ID.fetch_add(1, Ordering::Relaxed)),
            function_id: function_id.clone(),
            stats: Default::default(),
        })
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    fn load_sandbox(
        &mut self,
        _sandbox: &mut Self::Sandbox,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send
    {
        ::futures::future::ready(Ok(()))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    fn pause_sandbox(
        &mut self,
        _sandbox: &mut Self::Sandbox,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send
    {
        ::futures::future::ready(Ok(()))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    fn resume_sandbox(
        &mut self,
        _sandbox: &mut Self::Sandbox,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send
    {
        ::futures::future::ready(Ok(()))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    fn create_snapshot(
        &mut self,
        _sandbox: &mut Self::Sandbox,
        _state_file_path: impl AsRef<std::path::Path> + Send + Sync + std::fmt::Debug,
        _memory_file_path: impl AsRef<std::path::Path> + Send + Sync + std::fmt::Debug,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send
    {
        ::futures::future::ready(Ok(()))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    fn shutdown_sandbox(
        &mut self,
        _sandbox: &mut Self::Sandbox,
    ) -> impl Future<Output = Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>>> + Send
    {
        ::futures::future::ready(Ok(()))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    fn destroy_sandbox(
        &mut self,
        _sandbox: Self::Sandbox,
    ) -> impl Future<Output = Result<Self::NetResource, DestroySandboxRuntimeError<Self::Sandbox>>> + Send
    {
        ::futures::future::ready(Ok(()))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    fn register_function(
        _config: &Self::Config,
        _function_info: &mut Self::FunctionInfo,
    ) -> impl Future<Output = Result<(), crate::metadata::registration::Error>> + Send {
        ::futures::future::ready(Ok(()))
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    fn deregister_function(
        _config: &Self::Config,
        _function_info: &Self::FunctionInfo,
    ) -> impl Future<Output = Result<(), crate::metadata::registration::Error>> + Send {
        ::futures::future::ready(Ok(()))
    }
}
