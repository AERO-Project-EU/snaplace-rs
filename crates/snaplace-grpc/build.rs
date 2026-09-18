use anyhow::{Context, Result};

fn main() -> Result<()> {
    #[cfg(feature = "source")]
    {
        let mut config = ::tonic_prost_build::Config::new();
        config
            .bytes(["."])
            .message_attribute(
                "snaplace.grpc.request.Request",
                "#[derive(::serde::Serialize, ::serde::Deserialize)]",
            )
            .field_attribute(
                "snaplace.grpc.request.Request.invocation_id",
                "#[serde(default)]",
            );
        ::tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .out_dir("src/pb")
            .compile_with_config(config, &["proto/req.proto"], &["proto"])
            .with_context(|| "code generation for source failed")?;
    }

    #[cfg(feature = "sink")]
    {
        let mut config = ::tonic_prost_build::Config::new();
        config
            .bytes(["."])
            .message_attribute(
                "snaplace.grpc.response.Response",
                "#[derive(::serde::Serialize, ::serde::Deserialize)]",
            )
            .field_attribute(
                "snaplace.grpc.response.Response.invocation_id",
                "#[serde(default)]",
            );
        ::tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .out_dir("src/pb")
            .compile_with_config(config, &["proto/resp.proto"], &["proto"])
            .with_context(|| "code generation for sink failed")?;
    }

    Ok(())
}
