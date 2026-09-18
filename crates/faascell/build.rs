use anyhow::{Context, Result};

fn main() -> Result<()> {
    emit_vergen_info()
}

fn emit_vergen_info() -> Result<()> {
    ::vergen_gitcl::Emitter::default()
        .add_instructions(
            &::vergen_gitcl::BuildBuilder::default()
                .build_timestamp(true)
                .use_local(true)
                .build()
                .context("failed to construct vergen build information")?,
        )
        .context("failed to add build information to vergen")?
        .add_instructions(
            &::vergen_gitcl::CargoBuilder::all_cargo()
                .context("failed to construct vergen cargo information")?,
        )
        .context("failed to add cargo information to vergen")?
        .add_instructions(
            &::vergen_gitcl::GitclBuilder::default()
                .branch(true)
                .sha(true)
                //.commit_count(true)
                .commit_timestamp(true)
                .use_local(true)
                .dirty(false)
                //.describe(true, true, Some("v*"))
                .build()
                .context("failed to construct vergen git information")?,
        )
        .context("failed to add git information to vergen")?
        .add_instructions(
            &::vergen_gitcl::RustcBuilder::default()
                .channel(true)
                .semver(true)
                .host_triple(true)
                .build()
                .context("failed to construct vergen rustc information")?,
        )
        .context("failed to add rustc information to vergen")?
        .add_instructions(
            &::vergen_gitcl::SysinfoBuilder::default()
                .os_version(true)
                .user(true)
                .cpu_brand(true)
                .build()
                .context("failed to construct vergen sysinfo")?,
        )
        .context("failed to add sysinfo to vergen")?
        .fail_on_error()
        .emit()
        .context("failed to emit vergen compile-time information")
}
