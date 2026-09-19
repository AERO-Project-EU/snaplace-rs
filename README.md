# FaaSCell

## Prerequisites

### Build

Make sure that the `protoc` binary either can be found in `PATH` or is pointed
to by the `PROTOC` environment variable; e.g.:
```console
$ $PROTOC --version
libprotoc 25.3
```

Moreover, `faascell` by default relies on `mold` linker, so either make sure
it is installed and reachable via `$PATH`, or modify Cargo's
[config.toml](.cargo/config.toml) accordingly to use the default (or the
preferred) linker.

### Run

Make sure experiment's configuration is correct (some examples are provided
[here](artifacts/faascell/config/)).

> [!WARNING]
> For now, correct operation of FaaSCell requires root privileges.

> [!TIP]
> Setting the `NO_COLOR` environment variable to any value other than the empty
> string disables ANSI characters in logs (which is likely a good idea if they
> need to be parsed or otherwise processed later).
> Also see <https://no-color.org/>.

---

## Standalone FaaSCell

### Build

Depends on `faascell`'s cargo feature `rt-fc`.

To build with [default features](crates/faascell/Cargo.toml) enabled:
```console
$ cargo build --package faascell --bin faascell --features=rt-fc --profile=release
```

To build for the experiments with "static placement":
```console
$ cargo build --package faascell --bin faascell --no-default-features --features=fmd-store-dash,mimalloc,sched-setaffinity,sbnet-tap-pool,pol-sd-static,uncache,rt-fc --profile=release
```

To build for the experiments in the paper (fixed keep-alive and snapshot
placement policies, LRU eviction policy, CPU pinning, + `uncache`):
```console
$ cargo build --package faascell --bin faascell --no-default-features --features=fmd-store-dash,mimalloc,sched-setaffinity,sbnet-tap-pool,pol-all-fixed-evlru,uncache,rt-fc --profile=release
```

To build with _disabled_ CPU pinning (using `sched_setaffinity(2)`):
```console
$ cargo build --package faascell --bin faascell --no-default-features --features=fmd-store-dash,mimalloc,sbnet-tap-pool,pol-all-fixed-evlru,uncache,rt-fc --profile=release
```

### Run

Run FaaSCell providing the appropriate command line options and environment
variables; e.g.:
```console
# NO_COLOR=1 RUST_LOG='faascell=info,snaplace=info,snaplace_grpc=info' target/release/faascell -c artifacts/faascell/config/config_fc.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log"
```

Run the debug build, with full and parsing-friendly logging, also emitting all
Firecracker processes' output to the logs:
```console
# NO_COLOR=1 RUST_LOG='faascell=trace,snaplace=trace,snaplace_grpc=trace,fc.fifo=trace,fc.stdio=trace' target/debug/faascell -c artifacts/faascell/config/config_fc.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log" --span-events
```

---

## FaaSCell + firecracker-containerd

### Build

Depends on `faascell`'s cargo feature `rt-fcctrd`.

> [!NOTE]
> Currently, FaaSCell can run with `firecracker-containerd` as its underlying
> runtime only using our fork. Make sure it is correctly deployed as well.

To build with [default features](crates/faascell/Cargo.toml) enabled:
```console
$ cargo build --package faascell --bin faascell --features=rt-fcctrd --profile=release
```

To build for the experiments with "static placement":
```console
$ cargo build --package faascell --bin faascell --no-default-features --features=fmd-store-dash,mimalloc,sched-setaffinity,sbnet-tap-pool,pol-sd-static,uncache,rt-fcctrd --profile=release
```

To build for the experiments in the paper (fixed keep-alive and snapshot
placement policies, LRU eviction policy, CPU pinning, + `uncache`):
```console
$ cargo build --package faascell --bin faascell --no-default-features --features=fmd-store-dash,mimalloc,sched-setaffinity,sbnet-tap-pool,pol-all-fixed-evlru,uncache,rt-fcctrd --profile=release
```

To build with _disabled_ CPU pinning (using `sched_setaffinity(2)`):
```console
$ cargo build --package faascell --bin faascell --no-default-features --features=fmd-store-dash,mimalloc,sbnet-tap-pool,pol-all-fixed-evlru,uncache,rt-fcctrd --profile=release
```

### Run

Then run FaaSCell providing the appropriate command line options; e.g.:
```console
# NO_COLOR=1 RUST_LOG='faascell=info,snaplace=info,snaplace_grpc=info,firecracker_containerd_client=info' target/release/faascell -c artifacts/faascell/config/config_fcctrd.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log"
```

---

## FaaSRail integration

To build FaaSCell's plugin for the
[FaaSRail](https://github.com/cslab-ntua/faasrail) load generator (binary
`faasrail-snaplace-grpc`):
```console
cargo build --package faasrail-snaplace-grpc --bin faasrail-snaplace-grpc --profile=release
```

## Command line options

Check `faascell --help`.

Also check `README.md` files of
- [`faascell`](crates/faascell/README.md)
- [`faasrail-snaplace-grpc`](crates/faasrail-snaplace-grpc/README.md)
- [`faasctl`](crates/tools/faasctl/README.md)
- [`noop-faascell`](crates/tools/toy/README.md)

---

## License

This project is license under the terms of the European Union Public Licence
(EUPL), version 1.2.

For more information consult the included [LICENSE](LICENSE) file.
