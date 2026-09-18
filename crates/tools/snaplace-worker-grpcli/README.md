# grpcli

Run the server locally and:

```console
$ RUST_LOG='grpcli=trace,snaplace=trace' cargo r -- -i '{"payload": [0, 0, 0], "metadata_map": {"a": "1", "b": "2"}}'
```

or

```console
$ cargo b
$ RUST_LOG='grpcli=trace,snaplace=trace' ../../target/debug/grpcli -i @test-input/skata http://localhost:50052
```

Or spawn a container (e.g., `ckatsak/snaplace-fbpml-chameleon:v0.0.2-dev`) and:

```console
$ cargo b
$ RUST_LOG='grpcli=trace,snaplace=trace' ../../target/debug/grpcli -i @test-input/chameleon.json http://172.17.0.2:50052
```
