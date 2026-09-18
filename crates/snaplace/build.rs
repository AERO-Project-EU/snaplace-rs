use anyhow::{Context, Result};

fn main() -> Result<()> {
    protoc_registration()?;
    protoc_sandbox()?;
    #[cfg(feature = "rt-fcctrd")]
    protoc_runtime_fcctrd()?;
    #[cfg(feature = "rt-fc")]
    protoc_runtime_fc()?;

    emit_vergen_info()?;

    Ok(())
}

fn protoc_registration() -> Result<()> {
    let mut config = ::tonic_prost_build::Config::new();
    config.bytes(["."]);
    ::tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("src/metadata/registration/pb")
        .compile_with_config(config, &["proto/registration.proto"], &["proto"])
        .context("code generation for snaplace.registration failed")
}

fn protoc_sandbox() -> Result<()> {
    let mut config = ::tonic_prost_build::Config::new();
    config.bytes(["."]);
    ::tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("src/control/sandbox/pb")
        .compile_with_config(config, &["proto/sandbox.proto"], &["proto"])
        .context("code generation for snaplace.sandbox failed")
}

#[cfg(feature = "rt-fcctrd")]
fn protoc_runtime_fcctrd() -> Result<()> {
    let mut config = ::tonic_prost_build::Config::new();
    config
        .bytes(["."])
        .enable_type_names()
        .message_attribute(
            "snaplace.runtime.fcctrd.FunctionInfo",
            "#[derive(::serde::Serialize, ::serde::Deserialize)]",
        )
        .field_attribute(
            "snaplace.runtime.fcctrd.FunctionInfo.image_ref",
            "#[serde(rename = \"image\")]",
        );
    ::tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("src/worker/runtime/fcctrd")
        .compile_with_config(config, &["proto/runtime/fcctrd.proto"], &["proto"])
        .context("code generation for snaplace.runtime.fcctrd failed")
}

#[cfg(feature = "rt-fc")]
fn protoc_runtime_fc() -> Result<()> {
    let mut config = ::tonic_prost_build::Config::new();
    config.bytes(["."]).enable_type_names().message_attribute(
        "snaplace.runtime.fc.FunctionInfo",
        "#[derive(::serde::Serialize, ::serde::Deserialize)]",
    );
    ::tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("src/worker/runtime/fc")
        .compile_with_config(config, &["proto/runtime/fc.proto"], &["proto"])
        .context("code generation for snaplace.runtime.fc failed")
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
