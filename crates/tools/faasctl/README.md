# faasctl

Mind to check `faasctl --help` output (for each subcommand too).

> [!IMPORTANT]
> Mind that the Unix socket needs to have been created by the running FaaSCell
> instance to avoid `EADDRINUSE`. In other words, remember to
> `rm -vf /path/to/unix.socket` before running `faascell`, if you want to
> interact with it via `faasctl`.

> [!TIP]
> For now, `--runtime` defaults to `firecracker-containerd` when omitted.


## Register Functions

> [!TIP]
> The Function information necessary for the registration depends on the
> underlying runtime (`rt-fcctrd` or `rt-fc` for now). Therefore:
> - mind the `--runtime <RT>` CLI flag;
> - make sure the `--functions <FILE>` points to the correct data.

Example, when served over TCP, for `firecracker-containerd`-based runtime
(i.e., `rt-fcctrd`):
```console
$ RUST_LOG='faasctl=trace,snaplace=trace' numactl -N1 -l target/release/faasctl --runtime firecracker-containerd func --addr 'localhost:55555' register --functions artifacts/faascell/functions/functions_shrinkray.json
```

Example, when served over Unix socket, for standalone FaaSCell (i.e., `rt-fc`):
```console
$ RUST_LOG='faasctl=trace,snaplace=trace' numactl -N1 -l target/release/faasctl --runtime fc func --addr '/tmp/tmpfs/faascell.sock' register --functions artifacts/faascell/functions/functions_fc_shrinkray.json
```


## Snapshots

`faasctl snap` provides utilities to facilitate snapshot files manipulation.

> [!CAUTION]
> They utilities are generally meant to work offline; i.e., they may need to
> modify FaaSCell's database, hence they may need its write lock.

### List

List all existing snapshots according to the metadata currently stored in
FaaSCell's database at `/tmp/tmpfs/faascell.redb` (output will be shown in logs,
at INFO level):
```console
$ RUST_LOG='faasctl=trace,snaplace=trace' target/release/faasctl snap --db-path /tmp/tmpfs/faascell.redb ls
```

Same as above, but as newline-delimited JSON-formatted output at stdout (and
logs redirected to `/dev/null` (by the shell)):
```console
$ target/release/faasctl snap --db-path /tmp/tmpfs/faascell.redb ls -J 2>/dev/null
```

For finer-grained output, snapshots of only specific Functions can be listed,
by specifying any or both of the following options:
- `--func <FUNCTION_ID>` (possibly multiple times),
- `--from-func-file <PATH>` (at most once).

In this case any snapshots corresponding to Function IDs not included in the
union of the specified options, will be omitted from the output.
For example:

List snapshots stored in FaaSCell's database (at `/tmp/tmpfs/faascell.redb`)
only if they correspond to Function IDs included in the provided `.funcs` file,
NDJSON-formatted to stdout, and then pipe that to `jq` to extract the
corresponding Sandbox IDs into a new (text) file:
```console
$ RUST_LOG=debug target/release/faasctl snap --db-path /tmp/tmpfs/faascell.redb ls --from-func-file /tmp/cold_b16__running_slot__mean.10.sdstatic.funcs -J | jq '.sid' >"cold_uvm_sids.txt"
```

List snapshots associated with Function ID `lr_training-76dbbdb25bea` stored
in FaaSCell's database (at `/tmp/tmpfs/faascell.redb`) to the log (INFO level):
```console
$ RUST_LOG=info target/release/faasctl snap --db-path /tmp/tmpfs/faascell.redb ls --func 'lr_training-76dbbdb25bea'
```

### Move

> [!WARNING]
> `faasctl snap mv <DST> [<SRC>...]`; i.e., the first argument is always the
> destination directory, and all other subsequent arguments are the "source"
> files to be moved into it.

Move all snapshot files found under `/opt/ckatsak/snapshots/` and verified to be
valid in the database at `/tmp/tmpfs/faascell.redb` into `/tmp/snapshots/`:
```console
$ RUST_LOG='faasctl=debug,snaplace=trace' target/release/faasctl snap --db-path /tmp/tmpfs/faascell.redb mv /tmp/snapshots /opt/ckatsak/snapshots/*
```

Move all snapshot files with the `".memory"` suffix (glob resolved by shell)
found under `/tmp/snapshots/` and verified to be valid in the database at
`/tmp/tmpfs/faascell.redb` into `/mnt/pmem0/snapshots/`:
```console
$ RUST_LOG='faasctl=debug,snaplace=trace' target/release/faasctl snap --db-path /tmp/tmpfs/faascell.redb mv /mnt/pmem0/ckatsak/snapshots /tmp/snapshots/*.memory
```

Move all snapshot files that refer to Functions whose Function ID is included in
newline-delimited file `/tmp/flash_funcs.sdstatic` to `/opt/ckatsak/snapshots`
(as long as their metadata can also be found in database file `faascell.redb`):
```console
$ RUST_LOG='faasctl=debug,snaplace=trace' target/release/faasctl snap --db-path /tmp/tmpfs/faascell.redb mv --from-func-file /tmp/flash_funcs.sdstatic /opt/ckatsak/snapshots
```

### Forget

Show (notice the `--dry-run` option) all snapshot entries that would be deleted
from FaaSCell database at `/tmp/faascell.redb` based on Function IDs provided
through file `cold_b16__running_slot__mean`, without actually deleting them:
```console
$ RUST_LOG=debug target/release/faasctl snap --db-path /tmp/faascell.redb forget --dry-run --from-func-file cold_b16__running_slot__mean.10.sdstatic.funcs
```
Removing the `--dry-run` (or `-n`) option would actually delete them.

### Sandbox Stats

#### List

Lists all tracked Sandbox stats for all snapshots stored in the database,
sorted in descending order, and prints them either in logs (by default) or
JSONL-formatted on stdout.

Example:
```console
$ RUST_LOG=info target/release/faasctl snap --db-path /tmp/tmpfs/faascell.redb stats ls -J | jq
```

#### Zero-out

Zeroes out all tracked Sandbox stats for all snapshots tracked in the database.

Example:
```console
$ RUST_LOG=trace target/release/faasctl snap --db-path /tmp/faascell.redb stats zero-out
```

### Sync

Checks which of the snapshots stored in the database are actually valid (i.e.,
their corresponding snapshot files exist in the filesystem paths stored in the
database), printing out the invalid ones, and optionally removes the invalid
ones (those which files are no longer in the filesystem, essentially syncing
the database with the filesystem state). Note: output in log, at INFO level.

Check if any such "incomplete" snapshots exist in database file
`/tmp/faascell.redb`, and dump their Sandbox IDs to a new text file:
```console
$ RUST_LOG=info target/release/faasctl snap --db-path /tmp/faascell.redb sync -J | jq '.sid' >uvms_to_remove.$(date '+%Y%m%d%H%M').txt
```

Check for (and print in NDJSON format) any "incomplete" snapshots, and also
remove any such entry from the input database file `/tmp/faascell.redb`:
```console
$ RUST_LOG=info target/release/faasctl snap --db-path /tmp/tmpfs/faascell.redb sync -J --fix
```
