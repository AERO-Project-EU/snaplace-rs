# FaaSCell

## Quick Build

### Standalone

```console
$ cargo build --package faascell --bin faascell --no-default-features --features=fmd-store-dash,rt-fc,sched-setaffinity,sbnet-tap-pool,uncache,pol-all-fixed-evlru --profile=release
```

### firecracker-containerd

```console
$ cargo build --package faascell --bin faascell --no-default-features --features=fmd-store-dash,rt-fcctrd,sched-setaffinity,sbnet-tap-pool,uncache,pol-all-fixed-evlru --profile=release
```

## Run

### Standalone

Verbose debug build:
```console
# RUST_LOG='faascell=trace,snaplace=trace,snaplace_grpc=trace' target/debug/faascell -c artifacts/faascell/config/config_fc.json
```

Debug build + parsable full logging:
```console
# NO_COLOR=1 RUST_LOG='faascell=trace,snaplace=trace,snaplace_grpc=trace,fc.fifo=trace,fc.stdio=trace' target/debug/faascell -c artifacts/faascell/config/config_fc.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log" --span-events
```

Verbose release build:
```console
# RUST_LOG='faascell=trace,snaplace=trace,snaplace_grpc=trace,firecracker_containerd_client=trace' target/release/faascell -c artifacts/faascell/config/config_fc.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log"
```

Release build:
```console
# NO_COLOR=1 RUST_LOG='faascell=info,snaplace=info,snaplace_grpc=info' target/release/faascell -c artifacts/faascell/config/config_fc.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log"
```

### firecracker-containerd

```console
# RUST_LOG='faascell=trace,snaplace=trace,snaplace_grpc=trace,firecracker_containerd_client=debug' target/debug/faascell -c artifacts/faascell/config/config_fcctrd.json
```
```console
# NO_COLOR=1 RUST_LOG='faascell=trace,snaplace=trace,snaplace_grpc=trace,firecracker_containerd_client=trace' target/debug/faascell -c artifacts/faascell/config/config_fcctrd.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log" --span-events
```
```console
# RUST_LOG='faascell=trace,snaplace=trace,snaplace_grpc=trace,firecracker_containerd_client=trace' target/release/faascell -c artifacts/faascell/config/config_fcctrd.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log"
```
```console
# NO_COLOR=1 RUST_LOG='faascell=info,snaplace=info,snaplace_grpc=info,firecracker_containerd_client=info' target/release/faascell -c artifacts/faascell/config/config_fcctrd.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log"
```

## Flamegraphs

0. Install `cargo-flamegraph`.
1. Modify the [top-level Cargo.toml](../../Cargo.toml) to include debugging
   symbols in release builds.

### Run process through `flamegraph`

```console
# RUST_LOG='faascell=trace,snaplace=trace,snaplace_grpc=trace,firecracker_containerd_client=debug' flamegraph -F 7999 -o flamegraph.svg --ignore-status -- target/release/faascell -c artifacts/faascell/config/config.json
```

### Attach `flamegraph` to process

First run `faascell` and wait until it's ready (e.g., until static Function
registration is over), to avoid measuring initialization stuff.
So:

```console
# RUST_LOG='faascell=trace,snaplace=trace,snaplace_grpc=trace,firecracker_containerd_client=debug' target/release/faascell -c artifacts/faascell/config/config.json
```

When `faascell` is ready, attach `flamegraph` to it using its PID:

```console
# flamegraph -F 9949 --pid $(pidof faascell) -o flamegraph.svg --ignore-status
```

You may now send requests and `faascell` will be profiled while handling them.

When the request stream ends, you may stop (<kbd>Ctrl</kbd> + <kbd>C</kbd>)
profiling `faascell` to avoid profiling it while shutting down.
