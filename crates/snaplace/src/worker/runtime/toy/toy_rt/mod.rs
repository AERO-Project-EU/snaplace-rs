use std::{fmt::Debug, net::Ipv4Addr, path::Path};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use enum_map::EnumMap;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::{debug, instrument, trace, Level};

use crate::{
    metadata::{
        registration::{
            self,
            pb::{function_info::RuntimeSpecific, RegisterFunctionRequest},
        },
        FunctionInfo, SandboxStats,
    },
    metrics::{Nanoseconds, Timing},
    network::Tap,
    snapman::MonitorTarget,
    worker::{runtime::DestroySandboxRuntimeError, Runtime, Sandbox},
    FunctionId,
};

#[derive(Debug)]
pub struct ToySandbox {
    stats: SandboxStats,
    function_id: FunctionId,
    tap: Tap,
}

impl Sandbox for ToySandbox {
    type SnapshotState = ();

    #[inline(always)]
    fn id(&self) -> &str {
        self.tap.name()
    }

    #[inline(always)]
    fn stats(&self) -> &SandboxStats {
        &self.stats
    }

    #[inline(always)]
    fn stats_mut(&mut self) -> &mut SandboxStats {
        &mut self.stats
    }

    #[inline(always)]
    fn ip_addr(&self) -> Ipv4Addr {
        self.tap.ip_addr()
    }

    #[inline(always)]
    fn function_id(&self) -> FunctionId {
        self.function_id.clone()
    }

    #[inline(always)]
    fn has_snapshot(&self) -> bool {
        false
    }

    #[inline(always)]
    fn monitor_targets(&self) -> Vec<MonitorTarget> {
        Vec::new()
    }
}

pub struct ToyRt {}

impl Runtime for ToyRt {
    type Config = ();
    type Sandbox = ToySandbox;
    type NetResource = Tap;
    type FunctionInfo = ToyFunctionInfo;

    const NAME: &'static str = "toy";

    #[inline(always)]
    fn new(
        _config: &Self::Config,
        _fi: &Self::FunctionInfo,
        _sb: Option<&Self::Sandbox>,
    ) -> Result<Self, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        Ok(Self {})
    }

    #[inline(always)]
    async fn init(&mut self) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }

    #[instrument(level = Level::INFO, skip_all)]
    #[inline]
    async fn create_sandbox(
        &mut self,
        function_id: &FunctionId,
        tap: Self::NetResource,
        timings: &mut EnumMap<Timing, Nanoseconds>,
    ) -> Result<Self::Sandbox, Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        let create_sandbox_start = Instant::now();
        defer! {
            timings[Timing::CreateSandbox] = create_sandbox_start.elapsed().as_nanos() as _;
        }
        Ok(ToySandbox {
            stats: SandboxStats::default(),
            function_id: function_id.clone(),
            tap,
        })
    }

    #[instrument(level = Level::INFO, skip_all)]
    #[inline]
    async fn load_sandbox(
        &mut self,
        _sandbox: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }

    #[instrument(level = Level::INFO, skip_all)]
    #[inline]
    async fn pause_sandbox(
        &mut self,
        _sandbox: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }

    #[instrument(level = Level::INFO, skip_all)]
    #[inline]
    async fn resume_sandbox(
        &mut self,
        _sandbox: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }

    #[instrument(level = Level::INFO, skip_all)]
    #[inline]
    async fn create_snapshot(
        &mut self,
        _sandbox: &mut Self::Sandbox,
        _state_file_path: impl AsRef<Path> + Send + Sync + Debug,
        _memory_file_path: impl AsRef<Path> + Send + Sync + Debug,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }

    #[instrument(level = Level::INFO, skip_all)]
    #[inline]
    async fn shutdown_sandbox(
        &mut self,
        sandbox: &mut Self::Sandbox,
    ) -> Result<(), Box<dyn ::std::error::Error + Send + Sync + 'static>> {
        debug!(sandbox = ?sandbox);
        Ok(())
    }

    #[instrument(level = Level::INFO, skip_all)]
    #[inline]
    async fn destroy_sandbox(
        &mut self,
        sandbox: Self::Sandbox,
    ) -> Result<Self::NetResource, DestroySandboxRuntimeError<Self::Sandbox>> {
        Ok(sandbox.tap)
    }

    #[instrument(level = Level::INFO, skip_all)]
    #[inline]
    async fn register_function(
        _config: &Self::Config,
        _function_info: &mut Self::FunctionInfo,
    ) -> Result<(), registration::Error> {
        Ok(())
    }

    #[instrument(level = Level::INFO, skip_all)]
    #[inline]
    async fn deregister_function(
        _config: &Self::Config,
        _function_info: &Self::FunctionInfo,
    ) -> Result<(), registration::Error> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ToyFunctionInfo {
    pub id: FunctionId,
    pub memory: ::ubyte::ByteUnit,
    pub image_ref: String,
}

impl FunctionInfo for ToyFunctionInfo {
    #[inline]
    fn id(&self) -> &FunctionId {
        &self.id
    }
    #[inline]
    fn memory(&self) -> ::ubyte::ByteUnit {
        self.memory
    }
}

#[derive(Debug, ::thiserror::Error)]
#[error("toy error: {0}")]
pub struct ToyError(String);

impl From<ToyError> for ::tonic::Status {
    fn from(err: ToyError) -> Self {
        Self::unknown(err.to_string())
    }
}

impl TryFrom<RegisterFunctionRequest> for ToyFunctionInfo {
    type Error = ToyError;

    fn try_from(req: RegisterFunctionRequest) -> Result<Self, Self::Error> {
        trace!(?req);
        let fi = req.function_info.ok_or_else(|| {
            ToyError("RegisterFunctionRequest -> ToyFunctionInfo: empty request".to_owned())
        })?;

        match fi.runtime_specific {
            Some(RuntimeSpecific::B64Json(base64_bytes)) => {
                let json_bytes = BASE64
                    .decode(base64_bytes)
                    .map_err(|err| ToyError(format!("failed to decode base64: {err:#}")))?;
                trace!(base64.decoded.data = ?String::from_utf8_lossy(&json_bytes));
                let fi = ::serde_json::from_slice(&json_bytes)
                    .map_err(|err| ToyError(format!("failed to deserialize JSON: {err:#}")))?;
                Ok(fi)
            }
            Some(RuntimeSpecific::Proto3(any)) => {
                debug!(?any, "received protobuf");
                Err(ToyError(
                    "RegisterFunctionRequest -> ToyFunctionInfo: protobuf not supported".to_owned(),
                ))
            }
            None => Err(ToyError(
                "RegisterFunctionRequest -> ToyFunctionInfo: empty request".to_owned(),
            )),
        }
    }
}
