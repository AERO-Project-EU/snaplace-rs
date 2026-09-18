use std::{
    fmt::Debug,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use argh::FromArgs;
use tonic::{transport::Channel, Code};
use tracing::{error, info_span, instrument, trace, warn, Instrument, Level};

use snaplace::{
    metadata::registration::{
        self,
        pb::{
            function_info::RuntimeSpecific,
            function_registration_client::FunctionRegistrationClient, RegisterFunctionRequest,
        },
    },
    worker::runtime::{fc, fcctrd},
};

use crate::{
    funcreg::{get_client, get_functions, FunctionsCmd},
    Cli, Runtime, SubCmd,
};

/// Register new Function(s) with FaaSCell.
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "register")]
pub struct RegisterCmd {
    /// path to JSON file with Functions to be registered
    #[argh(option, short = 'f')]
    functions: PathBuf,
}

impl RegisterCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Functions(ref fcmd @ FunctionsCmd { .. }) = cli.cmd else {
            unreachable!("should not have been routed here");
        };

        let client = get_client(&fcmd.addr).await?;

        match cli.runtime {
            Runtime::FirecrackerContainerd => {
                register_rt::<fcctrd::FcctrdFunctionInfo, fcctrd::pb::FunctionInfo>(
                    &self.functions,
                    client,
                )
                .await
            }
            Runtime::Firecracker => {
                register_rt::<fc::FcFunctionInfo, fc::pb::FunctionInfo>(&self.functions, client)
                    .await
            }
        }
    }
}

// - RtFi: `<RUNTIME>::FunctionInfo`
//   E.g.: `::snaplace::worker::runtime::fcctrd::FcctrdFunctionInfo`
// - RtPbFi: `<RUNTIME>::pb::FunctionInfo`
//   E.g.: `::snaplace::worker::runtime::fcctrd::pb::FunctionInfo`
#[instrument(level = Level::DEBUG, skip_all)]
pub async fn register_rt<RtFi, RtPbFi>(
    path: &Path,
    mut c: FunctionRegistrationClient<Channel>,
) -> Result<()>
where
    RtFi: ::snaplace::metadata::FunctionInfo,
    RtPbFi: for<'a> TryFrom<&'a RtFi, Error: ::std::error::Error + Send + Sync + 'static>
        + ::prost::Name
        + Debug,
{
    for fri in get_functions(path).await? {
        #[allow(clippy::unnecessary_fallible_conversions)] // NOTE(ckatsak): future proof?
        let rtpbfi = RtPbFi::try_from(&fri.function_info)
            .with_context(|| format!("failed to FunctionInfo -> pb::FunctionInfo for: {fri:?}"))?;

        let admission = fri
            .admission
            .as_ref()
            .map(TryInto::try_into)
            .transpose()
            .with_context(|| {
                format!(
                    "failed to FunctionAdmissionOverrides -> pb::AdmissionOverrides for {fri:?}"
                )
            })?;

        register_function(rtpbfi, admission, &mut c)
            .instrument(info_span!("register_function", function_id = %fri.function_info.id()))
            .await?;
    }
    Ok(())
}

// RtPbFi: `<RUNTIME>::pb::FunctionInfo`
// E.g.: `::snaplace::worker::runtime::fcctrd::pb::FunctionInfo`
async fn register_function<RtPbFi: ::prost::Name + Debug>(
    rtpbfi: RtPbFi,
    admission: Option<registration::pb::AdmissionOverrides>,
    client: &mut FunctionRegistrationClient<Channel>,
) -> Result<()> {
    let any = ::prost_types::Any::from_msg(&rtpbfi).with_context(|| {
        format!("failed to format into google.protobuf.Any the following: {rtpbfi:?}")
    })?;
    let genfi = registration::pb::FunctionInfo {
        runtime_specific: Some(RuntimeSpecific::Proto3(any)),
    };
    let req = RegisterFunctionRequest {
        function_info: Some(genfi),
        admission,
    };

    match client.register_function(req).await {
        Ok(response) => {
            trace!(?response);
            Ok(())
        }
        Err(status) if status.code() == Code::AlreadyExists => {
            trace!(?status);
            warn!("Function already registered; to overwrite, deregister and retry");
            Ok(())
        }
        Err(status) => {
            error!(?status);
            Err(::anyhow::Error::new(status))
        }
    }
}
