# snaplace-toy

## `noop-faascell`

Build:
```console
$ cargo b --package snaplace-toy --bin noop-faascell --features=mimalloc --profile=release
```
(Similar for `debug` and `profiling` builds.)

Run debug build with detailed logging:
```console
$ RUST_LOG='noop_faascell=trace,snaplace=trace,snaplace_grpc=trace' target/debug/noop-faascell -c artifacts/faascell/config/config_fc.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log" --span-events
```

Run release build with limited logging:
```console
$ NO_COLOR=1 RUST_LOG='noop_faascell=info,snaplace=info,snaplace_grpc=info' target/release/noop-faascell -c artifacts/faascell/config/config_fc.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log"
```

### CPU profiling

> [!NOTE]
> For CPU profiling, mind to `echo -1 >/proc/sys/kernel/perf_event_paranoid`.

> [!TIP]
> Be mindful of the "observer effect" when setting the sampling frequency.
> - 10kHz is probably too much already;
>   [100Hz or 1kHz should be fine](https://www.brendangregg.com/blog/2014-06-22/perf-cpu-sample.html).
> - max available specified at `/proc/sys/kernel/perf_event_max_sample_rate`.

Boot the `profiling` build of `faascell`:
```console
$ NO_COLOR=1 RUST_LOG='noop_faascell=info,snaplace=info,snaplace_grpc=info' target/profiling/noop-faascell -c artifacts/faascell/config/config_fc.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log"
```

#### `[cargo-]flamegraph`

Once `noop-faascell` is done with registration, attach `flamegraph` to it:
```console
$ flamegraph -F 997 --pid $(pidof noop-faascell) -o cpu-flamegraph.svg --ignore-status
```

Now send requests to profile `noop-faascell` while it handles them.

When the request stream ends, you may stop (<kbd>Ctrl</kbd> + <kbd>C</kbd>)
profiling `noop-faascell` to avoid profiling it while shutting down.

#### `perf` + `inferno-flamegraph`

Once it's done with registration, attach `perf` to it:
```console
$ perf record -g -F 997 -p $(pidof noop-faascell)
```

Now send requests to profile `noop-faascell` while it handles them.

When the request stream ends, you may stop (<kbd>Ctrl</kbd> + <kbd>C</kbd>)
profiling `noop-faascell` to avoid profiling it while shutting down.

Then script the perf data:
```console
$ perf script >perf.script
```
Use `inferno` to collapse the stack traces:
```console
$ inferno-collapse-perf perf.script >collapsed.txt
```
And generate the flamegraph:
```console
$ inferno-flamegraph --deterministic --minwidth 0 collapsed.txt >flamegraph.svg
```

#### `samply` + Firefox profiler or perfetto

> [!WARNING]
> Symbols are not resolved correctly under `profiling` builds with this one.
> Perhaps generating more debug symbols would solve this. Till then, prefer
> the previous workflows.

Once `noop-faascell` is done with registration, attach `samply` to it:
```console
$ samply record --save-only -r 997 -p $(pidof noop-faascell)
```

Now send requests to profile `noop-faascell` while it handles them.

When the request stream ends, you may stop (<kbd>Ctrl</kbd> + <kbd>C</kbd>)
profiling `noop-faascell` to avoid profiling it while shutting down.

Then `gunzip` the file (probably named `profile.json.gz`) and load
it either on [Firefox profiler](https://profiler.firefox.com/) or
on [perfetto](https://ui.perfetto.dev/) to examine it.

### `tracing-flame`

Run release build with full tracing enabled:
```console
$ NO_COLOR=1 RUST_LOG='trace,noop_faascell=trace,snaplace=trace,snaplace_grpc=trace' target/release/noop-faascell -c artifacts/faascell/config/config_fc.json --log-file "/tmp/tmpfs/faascell.$(date '+%Y%m%d%H%M').log" --flame-file /tmp/noop-faascell.folded
```

Then use `inferno-flamegraph` to visualize:
```console
$ cat noop-faascell.folded | inferno-flamegraph --deterministic --minwidth 0 >tracing-flamegraph.svg
```
```console
$ cat noop-faascell.folded | inferno-flamegraph --flamechart --deterministic --minwidth 0.001 >tracing-flamechart.svg
```

> [!TIP]
> You'll likely want to filter out some long-running spans, to better render and
> observe shorter ones; e.g.:
> ```console
> $ rg -v 'metrics_writer|snaplace_responses' noop-faascell.folded | inferno-flamegraph --deterministic --minwidth 0 >tracing-flamegraph.svg
> ```

---

## `snaplace-toy`

```console
# RUST_LOG='toy=trace,snaplace=trace,firecracker_containerd_client=debug' cargo r --bin snaplace-toy -- --config config2.json
```

or

```console
$ cargo b
# RUST_LOG='toy=trace,snaplace=trace,firecracker_containerd_client=debug' ../../target/debug/snaplace-toy --config config2.json
```
