use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use argh::FromArgs;
use tonic::{transport::Channel, Code};
use tracing::{error, instrument, trace, warn, Level};

use snaplace::{
    metadata::registration::pb::{
        function_registration_client::FunctionRegistrationClient, DeregisterFunctionRequest,
    },
    worker::runtime::{fc, fcctrd},
};

use crate::{
    funcreg::{get_client, get_functions, FunctionsCmd},
    Cli, Runtime, SubCmd,
};

/// Deregister Function(s) in FaaSCell.
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "deregister")]
pub struct DeregisterCmd {
    /// path to JSON file with Functions to be deregistered.
    ///
    /// Requires the `--runtime` flag to be specified too, and causes any single `function_id`
    /// provided to be ignored.
    #[argh(option, short = 'f')]
    functions: Option<PathBuf>,

    /// the (unique) ID of the Function to be deregistered.
    ///
    /// Ignored if the `--functions` and `--runtime` flags are provided
    #[argh(positional)]
    function_id: Option<String>,
}

impl DeregisterCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Functions(ref fcmd @ FunctionsCmd { .. }) = cli.cmd else {
            unreachable!("should not have been routed here");
        };

        let mut client = get_client(&fcmd.addr).await?;

        if self.functions.is_some() {
            self.deregister_all(&cli.runtime, client).await
        } else if let Some(function_id) = &self.function_id {
            deregister_function(function_id, &mut client).await
        } else {
            bail!("Neither Function ID nor `--functions <FILE_PATH>` provided");
        }
    }

    async fn deregister_all(
        &self,
        runtime: &Runtime,
        client: FunctionRegistrationClient<Channel>,
    ) -> Result<()> {
        let path = self
            .functions
            .as_ref()
            .expect("deregister_all() called only when `--functions` is provided");

        match runtime {
            Runtime::FirecrackerContainerd => {
                deregister_all_rt::<fcctrd::FcctrdFunctionInfo>(path, client).await
            }
            Runtime::Firecracker => deregister_all_rt::<fc::FcFunctionInfo>(path, client).await,
        }
    }
}

// - RtFi: `<RUNTIME>::FunctionInfo`
//   E.g.: `::snaplace::worker::runtime::fcctrd::FcctrdFunctionInfo`
#[instrument(level = Level::DEBUG, skip_all)]
async fn deregister_all_rt<RtFi: ::snaplace::metadata::FunctionInfo>(
    path: &Path,
    mut client: FunctionRegistrationClient<Channel>,
) -> Result<()> {
    for fri in get_functions::<RtFi>(path).await? {
        deregister_function(fri.function_info.id(), &mut client).await?;
    }
    Ok(())
}

#[instrument(level = Level::INFO, skip(client))]
async fn deregister_function(
    function_id: &str,
    client: &mut FunctionRegistrationClient<Channel>,
) -> Result<()> {
    let req = DeregisterFunctionRequest {
        function_id: function_id.to_owned(),
    };
    match client.deregister_function(req).await {
        Ok(response) => {
            trace!(?response);
            Ok(())
        }
        Err(status) if status.code() == Code::NotFound => {
            trace!(?status);
            warn!("No Function '{function_id}' registered");
            Ok(())
        }
        Err(status) => {
            error!(?status);
            Err(::anyhow::Error::new(status))
        }
    }
}
