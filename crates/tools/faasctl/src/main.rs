use std::str::FromStr;

use anyhow::{bail, Context, Result};
use argh::FromArgs;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

mod funcreg;
use funcreg::FunctionsCmd;

mod snapshots;
use snapshots::SnapshotsCmd;

mod sandbox;
use sandbox::SandboxesCmd;

/// Client for FaaSCell orchestrator's control plane API.
#[derive(Debug, FromArgs)]
#[argh(help_triggers("-h", "--help", "help"))]
struct Cli {
    /// runtime used by FaaSCell
    #[argh(option, short = 'r', default = "Runtime::FirecrackerContainerd")]
    runtime: Runtime,

    #[argh(subcommand)]
    cmd: SubCmd,
}

#[derive(Debug, PartialEq, FromArgs)]
#[argh(subcommand)]
enum SubCmd {
    Functions(FunctionsCmd),
    Snapshots(SnapshotsCmd),
    Sandboxes(SandboxesCmd),
}

#[derive(Debug, PartialEq, Eq)]
enum Runtime {
    FirecrackerContainerd,
    Firecracker,
}

impl FromStr for Runtime {
    type Err = ::anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "fcctrd" | "firecracker-containerd" => Ok(Self::FirecrackerContainerd),
            "fc" => Ok(Self::Firecracker),
            _ => bail!("unknown faascell runtime '{s}'"),
        }
    }
}

#[::tokio::main]
async fn main() -> Result<()> {
    ::tracing_subscriber::fmt()
        .with_writer(::std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_thread_ids(true)
        .with_line_number(true)
        //.with_thread_names(true)
        .try_init()
        .map_err(::anyhow::Error::from_boxed)
        .context("failed to initialize tracing subscriber")?;

    let cli = ::argh::from_env::<Cli>();
    match cli.cmd {
        SubCmd::Functions(ref cmd) => cmd.run(&cli).await,
        SubCmd::Snapshots(ref cmd) => cmd.run(&cli).await,
        SubCmd::Sandboxes(ref cmd) => cmd.run(&cli).await,
    }
}
