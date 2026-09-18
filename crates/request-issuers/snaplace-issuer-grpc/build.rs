use anyhow::{Context, Result};

fn main() -> Result<()> {
    let mut config = ::tonic_prost_build::Config::new();
    config.bytes(["."]);
    ::tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src")
        .compile_with_config(config, &["proto/function.proto"], &["proto"])
        .with_context(|| "code generation for snaplace-issuer-grpc failed")?;

    Ok(())
}
