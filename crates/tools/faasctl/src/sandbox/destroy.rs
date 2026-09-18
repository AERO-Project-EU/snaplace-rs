use anyhow::{anyhow, Result};
use argh::FromArgs;
use tonic::transport::Channel;
use tracing::{error, info, trace, warn};

use snaplace::{
    control::sandbox::pb::{
        sandbox_lifecycle_client::SandboxLifecycleClient, DestroySandboxRequest,
        DestroySandboxResponse,
    },
    SandboxId,
};

use crate::{sandbox::SandboxesCmd, Cli, SubCmd};

/// Destroy a Sandbox, and/or possibly its snapshot
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "destroy", help_triggers("-h", "--help", "help"))]
pub struct DestroyCmd {
    /// reject the request if the target Sandbox is currently active (default: false)
    #[argh(switch)]
    no_active: bool,
    /// also remove Sandbox's snapshot (default: false)
    #[argh(switch, short = 's')]
    remove_snapshot: bool,
    /// the unique ID of the Sandbox to destroy
    #[argh(positional)]
    sandbox_id: SandboxId,
}

impl DestroyCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Sandboxes(ref scmd @ SandboxesCmd { .. }) = cli.cmd else {
            unreachable!("should not have been routed here")
        };

        match super::get_client(&scmd.addr)
            .await?
            .destroy_sandbox(DestroySandboxRequest {
                sandbox_id: self.sandbox_id.as_str().into(),
                allow_if_active: self.no_active.then_some(false),
                remove_persisted_snapshot: self.remove_snapshot.then_some(true),
            })
            .await
        {
            Ok(resp) => {
                let DestroySandboxResponse {
                    persistent_snapshot_exists,
                } = resp.into_inner();
                info!(sandbox.id = %self.sandbox_id, "Sandbox successfully destroyed");
                if self.remove_snapshot && persistent_snapshot_exists {
                    warn!(sandbox.id = %self.sandbox_id, "Sandbox's snapshot still exists");
                } else if self.remove_snapshot && !persistent_snapshot_exists {
                    info!(sandbox.id = %self.sandbox_id, "Sandbox's snapshot successfully destroyed");
                }
                Ok(())
            }
            Err(status) => {
                error!(
                    sandbox.id = %self.sandbox_id, %status,
                    "Failed to destroy Sandbox and/or its snapshot"
                );
                Err(anyhow!(status))
            }
        }
    }
}

pub async fn destroy_sandbox(
    c: &mut SandboxLifecycleClient<Channel>,
    allow_if_active: bool,
    remove_persisted_snapshot: bool,
    sandbox_id: &str,
) -> Result<()> {
    match c
        .destroy_sandbox(DestroySandboxRequest {
            sandbox_id: sandbox_id.into(),
            allow_if_active: Some(allow_if_active),
            remove_persisted_snapshot: Some(remove_persisted_snapshot),
        })
        .await
    {
        Ok(resp) => {
            let DestroySandboxResponse {
                persistent_snapshot_exists,
            } = resp.into_inner();
            trace!(%sandbox_id, "Sandbox successfully destroyed");
            if remove_persisted_snapshot && persistent_snapshot_exists {
                warn!(%sandbox_id, "Sandbox's snapshot still exists");
            } else if remove_persisted_snapshot && !persistent_snapshot_exists {
                trace!(%sandbox_id, "Sandbox's snapshot successfully destroyed");
            }
            Ok(())
        }
        Err(status) => {
            error!(%sandbox_id, %status, "Failed to destroy Sandbox and/or its snapshot");
            Err(anyhow!(status))
        }
    }
}
