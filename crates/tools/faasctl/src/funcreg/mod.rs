use std::{fs::OpenOptions, io::BufReader, path::Path};

use anyhow::{Context, Result};
use argh::FromArgs;
use serde::Deserialize;
use tonic::transport::{Channel, Endpoint};
use tracing::{instrument, Level};

use snaplace::{
    conf::Address,
    metadata::{
        registration::{
            pb::function_registration_client::FunctionRegistrationClient,
            FunctionAdmissionOverrides,
        },
        FunctionInfo,
    },
};

mod dereg;
mod reg;

pub(super) use dereg::DeregisterCmd;
pub(super) use reg::RegisterCmd;

use crate::{Cli, SubCmd};

/// Function registration & deregistration
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "func", help_triggers("-h", "--help", "help"))]
pub struct FunctionsCmd {
    /// address of control plane API gRPC server.
    ///
    /// This should only be one of:
    /// (a) `"HOST:PORT"`, to bind a TCP socket to;
    /// (b) `"/path/to/unix/domain.socket"`, to bind a Unix Domain Socket to.
    #[argh(option)]
    addr: Address,

    #[argh(subcommand)]
    cmd: FunctionsSubCmd,
}

#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand)]
enum FunctionsSubCmd {
    Register(RegisterCmd),
    Deregister(DeregisterCmd),
}

impl FunctionsCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Functions(FunctionsCmd { ref cmd, .. }) = cli.cmd else {
            unreachable!("should not have been routed here");
        };

        match cmd {
            FunctionsSubCmd::Register(reg_cmd) => reg_cmd.run(cli).await,
            FunctionsSubCmd::Deregister(dreg_cmd) => dreg_cmd.run(cli).await,
        }
    }
}

async fn get_client(addr: &Address) -> Result<FunctionRegistrationClient<Channel>> {
    match addr {
        Address::Net(addr) => {
            let channel = Endpoint::from_shared(format!("http://{addr}"))
                .with_context(|| format!("failed to parse gRPC endpoint from '{addr}'"))?;
            FunctionRegistrationClient::connect(channel)
                .await
                .with_context(|| format!("failed to connect to gRPC server at '{addr}'"))
        }
        Address::Uds(path) => {
            let path = format!(
                "unix:{}{}",
                // See: <https://github.com/grpc/grpc/blob/v1.75.0-pre1/doc/naming.md>
                if path.is_absolute() { "//" } else { "" },
                path.display()
            );
            FunctionRegistrationClient::connect(path.clone())
                .await
                .with_context(|| format!("failed to connect to gRPC server at '{path}'"))
        }
    }
}

// TODO: docs
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct FunctionRegistrationInput<RtFi> {
    #[serde(flatten)]
    function_info: RtFi,

    #[serde(default)]
    admission: Option<FunctionAdmissionOverrides>,
}

/// Read and deserialize all Functions in the JSON file at the provided `path`.
#[instrument(level = Level::TRACE, skip_all)]
fn sync_get_functions<FuncInfo: FunctionInfo>(
    path: impl AsRef<Path>,
) -> Result<Vec<FunctionRegistrationInput<FuncInfo>>> {
    ::serde_json::from_reader({
        BufReader::with_capacity(
            1 << 14,
            OpenOptions::new()
                .read(true)
                .open(&path)
                .with_context(|| format!("failed to open file '{}'", path.as_ref().display()))?,
        )
    })
    .with_context(|| {
        format!(
            "failed to deserialize functions from '{}'",
            path.as_ref().display()
        )
    })
}

/// Read and deserialize all Functions in the JSON file at the provided `path`.
#[instrument(level = Level::TRACE, skip_all)]
async fn get_functions<FuncInfo: FunctionInfo>(
    path: &Path,
) -> Result<Vec<FunctionRegistrationInput<FuncInfo>>> {
    ::tokio::task::spawn_blocking({
        let p = path.to_owned();
        || sync_get_functions(p)
    })
    .await
    .context("failed to join tokio task")?
    .with_context(|| format!("failed to read functions from '{}'", path.display()))
}
