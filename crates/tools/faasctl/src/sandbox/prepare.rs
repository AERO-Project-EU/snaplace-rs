use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use argh::{FromArgValue, FromArgs};
use tonic::transport::Channel;
use tracing::{error, info, trace};

use snaplace::{
    control::sandbox::pb::{
        sandbox_lifecycle_client::SandboxLifecycleClient, PrepareSandboxRequest,
        PrepareSandboxResponse, SandboxInfo, SandboxProvisioningMode,
    },
    FunctionId,
};

use crate::{sandbox::SandboxesCmd, Cli, SubCmd};

/// Prepare a new Sandbox
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "prepare", help_triggers("-h", "--help", "help"))]
pub struct PrepareCmd {
    /// the unique ID of the Function to prerare a new Sandbox for
    #[argh(option, short = 'f')]
    function_id: FunctionId,
    /// describes the sandbox provisioning mode; can be one of:
    /// "prefer_snapshot" (default), "require_snapshot", "force_fresh"
    #[argh(option, short = 'm', default = "Default::default()")]
    mode: ProvisioningMode,
    /// optional suggestion (best-effort) for an initial keep-alive duration
    /// for the freshly prepared Sandbox
    #[argh(option, short = 'k')]
    initial_keepalive_hint: Option<::humantime::Duration>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, FromArgValue)]
enum ProvisioningMode {
    #[default]
    PreferSnapshot,
    RequireSnapshot,
    ForceFresh,
}

impl From<ProvisioningMode> for SandboxProvisioningMode {
    fn from(mode: ProvisioningMode) -> Self {
        match mode {
            ProvisioningMode::PreferSnapshot => SandboxProvisioningMode::PreferSnapshot,
            ProvisioningMode::RequireSnapshot => SandboxProvisioningMode::RequireSnapshot,
            ProvisioningMode::ForceFresh => SandboxProvisioningMode::ForceFresh,
        }
    }
}

impl PrepareCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Sandboxes(ref scmd @ SandboxesCmd { .. }) = cli.cmd else {
            unreachable!("should not have been routed here")
        };

        match super::get_client(&scmd.addr)
            .await?
            .prepare_sandbox(PrepareSandboxRequest {
                function_id: self.function_id.as_str().into(),
                mode: SandboxProvisioningMode::from(self.mode).into(),
                initial_keepalive_hint: self
                    .initial_keepalive_hint
                    .map(|dur| {
                        Duration::from(dur)
                            .try_into()
                            .with_context(|| format!("failed to parse duration '{dur}'"))
                    })
                    .transpose()?,
            })
            .await
        {
            Ok(resp) => {
                let PrepareSandboxResponse { info } = resp.into_inner();
                info!(?info, "Sandbox prepared");
                Ok(())
            }
            Err(status) => {
                error!(%status, "Failed to prepare new Sandbox");
                Err(anyhow!(status))
            }
        }
    }
}

pub async fn prepare_sandbox(
    c: &mut SandboxLifecycleClient<Channel>,
    function_id: &FunctionId,
    mode: SandboxProvisioningMode,
    initial_keepalive_hint: Option<Duration>,
) -> Result<Option<SandboxInfo>> {
    match c
        .prepare_sandbox(PrepareSandboxRequest {
            function_id: function_id.as_str().into(),
            mode: mode.into(),
            initial_keepalive_hint: initial_keepalive_hint
                .map(|dur| {
                    dur.try_into()
                        .with_context(|| format!("failed to parse duration '{dur:?}'"))
                })
                .transpose()?,
        })
        .await
    {
        Ok(resp) => {
            let PrepareSandboxResponse { info } = resp.into_inner();
            trace!(?info, "Sandbox prepared");
            Ok(info)
        }
        Err(status) => {
            error!(%status, "Failed to prepare new Sandbox");
            Err(anyhow!(status))
        }
    }
}
