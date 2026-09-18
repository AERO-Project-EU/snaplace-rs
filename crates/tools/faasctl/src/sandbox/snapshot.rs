use anyhow::{anyhow, Result};
use argh::FromArgs;
use tonic::transport::Channel;
use tracing::{error, info, trace};

use snaplace::{
    control::sandbox::pb::{
        sandbox_lifecycle_client::SandboxLifecycleClient, CreateSnapshotRequest,
        CreateSnapshotResponse,
    },
    SandboxId,
};

use crate::{sandbox::SandboxesCmd, Cli, SubCmd};

/// Create a snapshot of a Sandbox
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "snapshot", help_triggers("-h", "--help", "help"))]
pub struct SnapshotCmd {
    /// the unique ID of the Sandbox to snapshot
    #[argh(positional)]
    sandbox_id: SandboxId,
}

impl SnapshotCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Sandboxes(ref scmd @ SandboxesCmd { .. }) = cli.cmd else {
            unreachable!("should not have been routed here")
        };

        match super::get_client(&scmd.addr)
            .await?
            .create_snapshot(CreateSnapshotRequest {
                sandbox_id: self.sandbox_id.as_str().into(),
            })
            .await
        {
            Ok(resp) => {
                let CreateSnapshotResponse { sandbox_id } = resp.into_inner();
                assert_eq!(sandbox_id.as_str(), self.sandbox_id.as_str());
                info!(sandbox.id = %self.sandbox_id, "Successfully created Sandbox snapshot");
                Ok(())
            }
            Err(status) => {
                error!(
                    sandbox.id = %self.sandbox_id, %status,
                    "Failed to create snapshot from the Sandbox"
                );
                Err(anyhow!(status))
            }
        }
    }
}

pub async fn create_snapshot(
    c: &mut SandboxLifecycleClient<Channel>,
    sandbox_id: &str,
) -> Result<()> {
    match c
        .create_snapshot(CreateSnapshotRequest {
            sandbox_id: sandbox_id.into(),
        })
        .await
    {
        Ok(resp) => {
            trace!(%sandbox_id, ?resp, "Successfully created Sandbox snapshot");
            Ok(())
        }
        Err(status) => {
            error!(sandbox.id = %sandbox_id, %status, "Failed to create snapshot from the Sandbox");
            Err(anyhow!(status))
        }
    }
}
