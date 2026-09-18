use anyhow::{Context, Result};
use argh::FromArgs;
use tonic::transport::{Channel, Endpoint};

use snaplace::{
    conf::Address, control::sandbox::pb::sandbox_lifecycle_client::SandboxLifecycleClient,
};

mod destroy;
mod prepare;
mod seed;
mod snapshot;

use self::{destroy::DestroyCmd, prepare::PrepareCmd, seed::SeedCmd, snapshot::SnapshotCmd};
use crate::{Cli, SubCmd};

/// Sandbox lifecycle manipulation
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "sandbox", help_triggers("-h", "--help", "help"))]
pub struct SandboxesCmd {
    /// address of control plane API gRPC server.
    ///
    /// This should only be one of:
    /// (a) `"HOST:PORT"`, to connect to a TCP socket;
    /// (b) `"/path/to/unix/domain.socket"`, to connect to a Unix Domain Socket.
    #[argh(option)]
    addr: Address,

    #[argh(subcommand)]
    cmd: SandboxesSubCmd,
}

#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand)]
enum SandboxesSubCmd {
    Prepare(PrepareCmd),
    Destroy(DestroyCmd),
    Snapshot(SnapshotCmd),
    Seed(SeedCmd),
}

impl SandboxesCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Sandboxes(SandboxesCmd { ref cmd, .. }) = cli.cmd else {
            unreachable!("should not have been routed here")
        };

        match cmd {
            SandboxesSubCmd::Prepare(cmd) => cmd.run(cli).await,
            SandboxesSubCmd::Destroy(cmd) => cmd.run(cli).await,
            SandboxesSubCmd::Snapshot(cmd) => cmd.run(cli).await,
            SandboxesSubCmd::Seed(cmd) => cmd.run(cli).await,
        }
    }
}

async fn get_client(addr: &Address) -> Result<SandboxLifecycleClient<Channel>> {
    match addr {
        Address::Net(addr) => {
            let channel = Endpoint::from_shared(format!("http://{addr}"))
                .with_context(|| format!("failed to parse gRPC endpoint from '{addr}'"))?;
            SandboxLifecycleClient::connect(channel)
                .await
                .with_context(|| format!("failed to connect to gRPC server at '{addr}'"))
        }
        Address::Uds(path) => {
            let path = format!(
                "unix:{}{}",
                // See: <https://github.com/grpc/grpc/blob/v1.80.0/doc/naming.md>
                if path.is_absolute() { "//" } else { "" },
                path.display()
            );
            SandboxLifecycleClient::connect(path.clone())
                .await
                .with_context(|| format!("failed to connect to gRPC server at '{path}'"))
        }
    }
}
