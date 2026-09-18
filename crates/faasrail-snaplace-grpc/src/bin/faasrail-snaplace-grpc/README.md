# faasrail-snaplace-grpc

All options:
```console
$ target/release/faasrail-snaplace-grpc --help
```

My typical usage:
```console
$ RUST_LOG='faasrail_snaplace_grpc=trace,faasrail_loadgen=trace,snaplace_grpc=trace' numactl -N1 -l target/release/faasrail-snaplace-grpc --source-address '147.102.4.82:60051' --sink-address '147.102.4.82:60052' --minio-address 'icy1.cslab.ece.ntua.gr:59000' -o /tmp/tmpfs/sink1.out --inv-log /tmp/tmpfs/inv_log1.json --invoc-id 100000 --csv crates/reqgen/artifacts/rgv3/azure__rps20__min30.csv --seed 0
```
