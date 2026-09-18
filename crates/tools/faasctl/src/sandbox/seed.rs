use std::{collections::HashMap, path::PathBuf, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use argh::FromArgs;
use tokio::{task::JoinSet, time::sleep};
use tonic::{transport::Channel, Status};
use tracing::{debug, error, instrument, trace, Level};

use snaplace::{
    control::sandbox::pb::{
        sandbox_lifecycle_client::SandboxLifecycleClient, SandboxInfo, SandboxProvisioningMode,
    },
    utils::backoff::FibonacciBackoff,
    FunctionId,
};

use crate::{
    sandbox::{
        destroy::destroy_sandbox, prepare::prepare_sandbox, snapshot::create_snapshot,
        SandboxesCmd, SandboxesSubCmd,
    },
    Cli, SubCmd,
};

/// Bulk-create (and possibly snapshot) a number of Sandboxes per Function
#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand, name = "seed", help_triggers("-h", "--help", "help"))]
pub struct SeedCmd {
    /// path to JSON file with Function counts
    #[argh(option, short = 'c')]
    func_counts: Option<PathBuf>,

    /// path to JSON file with Function counts
    #[argh(option, short = 's')]
    sb_stats: Option<PathBuf>,

    /// optionally also snapshot every newly created Sandbox
    #[argh(switch)]
    also_snapshot: bool,

    /// do not force fresh Sandbox creation
    #[argh(switch)]
    no_force_fresh: bool,

    /// optional suggestion (best-effort) for an initial keep-alive duration
    /// for the freshly prepared Sandbox
    #[argh(option, short = 'k')]
    initial_keepalive_hint: Option<::humantime::Duration>,
}

impl SeedCmd {
    pub async fn run(&self, cli: &Cli) -> Result<()> {
        let SubCmd::Sandboxes(ref scmd @ SandboxesCmd { .. }) = cli.cmd else {
            unreachable!("should not have been routed here")
        };

        self.validate_args().context("CLI arguments error")?;
        let SandboxesSubCmd::Seed(SeedCmd {
            func_counts,
            sb_stats,
            also_snapshot,
            no_force_fresh,
            initial_keepalive_hint,
        }) = &scmd.cmd
        else {
            unreachable!("should not have been routed here")
        };

        let func_counts = if let Some(f) = func_counts {
            let fcnts = ::tokio::fs::read_to_string(&f)
                .await
                .with_context(|| format!("failed to read file '{}'", f.display()))?;
            let mut ret = HashMap::new();
            for line in fcnts.lines().map(str::trim) {
                let entry: HashMap<FunctionId, usize> =
                    ::serde_json::from_str(line).with_context(|| {
                        format!(
                            "failed to JSON-deserialize line in '{}': '{line}'",
                            f.display()
                        )
                    })?;
                ret.extend(entry);
            }
            ret
        } else if let Some(f) = sb_stats {
            let sb_stats = ::tokio::fs::read_to_string(&f)
                .await
                .with_context(|| format!("failed to read file '{}'", f.display()))?;
            let mut ret = HashMap::new();
            for line in sb_stats.lines().map(str::trim) {
                let ::serde_json::Value::Object(map) =
                    ::serde_json::from_str(line).with_context(|| {
                        format!(
                            "failed to JSON-deserialize line in '{}': '{line}'",
                            f.display()
                        )
                    })?
                else {
                    bail!("Expected JSON object; found line: '{line}'");
                };
                let Some(::serde_json::Value::String(s)) = map.get("fid") else {
                    bail!("Function ID key 'fid' not found in line: '{line}'");
                };
                ret.entry(FunctionId::from(s))
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
            }
            ret
        } else {
            unreachable!("validation should have ensured one of `--func-counts` and `--sb-stats`")
        };

        let mut c = super::get_client(&scmd.addr).await?;

        let mut sandboxes_prepared = 0;
        let mut num_incomplete = 0;
        for (function_id, &count) in &func_counts {
            let sandboxes = seed_function(
                &mut c,
                count,
                function_id,
                *also_snapshot,
                *no_force_fresh,
                *initial_keepalive_hint,
            )
            .await;

            sandboxes_prepared += sandboxes.len();
            if sandboxes.len() < count {
                error!(%function_id, "Prepared only {} out of the {count} requested Sandboxes", sandboxes.len());
                num_incomplete += 1;
            }
            for sandbox in &sandboxes {
                trace!(?sandbox);
            }
        }

        if num_incomplete == 0 {
            Ok(())
        } else {
            error!(
                num.functions = func_counts.len(),
                num.incomplete = num_incomplete,
                sandboxes.requested = func_counts.values().sum::<usize>(),
                sandboxes.prepared = %sandboxes_prepared
            );
            bail!("Partial or total failure for {num_incomplete} Functions")
        }
    }

    fn validate_args(&self) -> Result<()> {
        if self.func_counts.is_some() && self.sb_stats.is_some() {
            bail!("Both function counts and sandbox stats JSON files provided; choose one")
        }
        if self.func_counts.is_none() && self.sb_stats.is_none() {
            bail!("Provide either a function counts or a sandbox stats JSON files")
        }

        if let Some(f) = &self.func_counts
            && !f
                .metadata()
                .with_context(|| format!("failed to stat(2) path '{}'", f.display()))?
                .is_file()
        {
            bail!("`--func-counts` does not point to a normal file")
        }
        if let Some(f) = &self.sb_stats
            && !f
                .metadata()
                .with_context(|| format!("failed to stat(2) path '{}'", f.display()))?
                .is_file()
        {
            bail!("`--sb-stats` does not point to a normal file")
        }

        Ok(())
    }
}

#[instrument(level = Level::DEBUG, skip_all, fields(function.id = %function_id, %count))]
async fn seed_function(
    c: &mut SandboxLifecycleClient<Channel>,
    count: usize,
    function_id: &FunctionId,
    also_snapshot: bool,
    no_force_fresh: bool,
    initial_keepalive_hint: Option<::humantime::Duration>,
) -> Vec<SandboxInfo> {
    let mut clients = (0..count)
        .map(|_| {
            let mut c = c.clone();
            let function_id = function_id.clone();
            async move {
                create_snapshot_destroy(
                    &mut c,
                    &function_id,
                    no_force_fresh,
                    also_snapshot,
                    initial_keepalive_hint,
                )
                .await
            }
        })
        .collect::<JoinSet<_>>();

    let mut ret = Vec::with_capacity(count);
    while let Some(res) = clients.join_next().await {
        match res {
            Ok(Ok(info)) => {
                trace!(?info, "Successful Sandbox preparation");
                ret.push(info);
            }
            Ok(Err(err)) => debug!(error = ?err, "Failed to prepare Sandbox: {err:#}"),
            Err(jerr) => error!(error = ?jerr, "Failed to join tokio preparator task: {jerr:#}"),
        }
    }
    ret
}

#[instrument(level = Level::DEBUG, skip_all, fields(sandbox.id = ::tracing::field::Empty), ret)]
async fn create_snapshot_destroy(
    c: &mut SandboxLifecycleClient<Channel>,
    function_id: &FunctionId,
    no_force_fresh: bool,
    also_snapshot: bool,
    initial_keepalive_hint: Option<::humantime::Duration>,
) -> Result<SandboxInfo> {
    //
    // Create a new sandbox
    //
    let mut backoff = FibonacciBackoff::new(Duration::from_millis(500));
    let mut attempts = 3;
    let info = loop {
        match prepare_sandbox(
            c,
            function_id,
            if no_force_fresh {
                SandboxProvisioningMode::PreferSnapshot
            } else {
                SandboxProvisioningMode::ForceFresh
            },
            initial_keepalive_hint.map(Into::into),
        )
        .await
        {
            Ok(Some(info)) => break info,
            Err(err) => match err.downcast::<Status>() {
                Ok(status) => match status.code() {
                    ::tonic::Code::Unavailable if attempts > 0 => {
                        attempts -= 1;
                        sleep(backoff.next().expect("never-ending iterator")).await;
                    }
                    status => {
                        error!(%status, ?function_id, ?attempts, ?backoff, "Failed to create new Sandbox");
                        return Err(anyhow!(status));
                    }
                },
                Err(err) => {
                    error!(error = ?err, "Unexpected error while creating new sandbox: {err:#}");
                    return Err(err);
                }
            },
            Ok(None) => unreachable!("SandboxInfo should always be present"),
        }
    };
    ::tracing::Span::current().record("sandbox.id", &info.sandbox_id);
    trace!(sandbox.info = ?info);

    //
    // Create a new snapshot for the sandbox
    //
    if also_snapshot {
        let mut snap_err = None;

        let mut backoff = FibonacciBackoff::new(Duration::from_millis(500));
        let mut attempts = 3;
        while let Err(err) = create_snapshot(c, &info.sandbox_id).await {
            attempts -= 1;
            if attempts <= 0 {
                error!(error = ?err, ?info, "Failed to create snapshot: {err:#}");
                snap_err = Some(err);
                break;
            }
            sleep(backoff.next().expect("never-ending iterator")).await;
        }

        //
        // Destroy the sandbox, retaining its snapshot
        //
        if let Err(err) = destroy_sandbox(c, false, false, &info.sandbox_id).await {
            error!(error = ?err, ?info, "Failed to destroy sandbox");
        }

        if let Some(err) = snap_err {
            return Err(err);
        }
    }

    Ok(info)
}
