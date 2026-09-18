# warmup-snapshots

All options:

```console
$ target/release/warmup-snapshots --help
```

My typical usage (though mind the defaults):

```console
$ RUST_LOG='info,faasrail_snaplace_grpc=trace,faasrail_loadgen=trace,snaplace_grpc=trace' numactl -N1 -l target/release/warmup-snapshots --source-address '147.102.4.82:60051' --sink-address '147.102.4.82:60052' --minio-address 'icy1.cslab.ece.ntua.gr:59000' --csv artifacts/faasrail/azure_spec_rps20_min30.csv --max-concurr-requests 16
```
