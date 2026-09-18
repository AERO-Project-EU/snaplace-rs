# quick-cginfo

```console
$ numactl -N1 -l target/release/quick-cginfo --period-ms 200 --cgroup /sys/fs/cgroup/faascell >cginfo.csv
```

## Setup cgroup for monitoring

First, check:
```console
$ grep -E 'cpu|memory' /sys/fs/cgroup/cgroup.controllers
```

Create a new cgroup:
```console
# mkdir -v /sys/fs/cgroup/faascell
```
Enable the CPU and memory controllers for our parent's children:
```console
# echo '+cpu +cpuset +memory' | tee /sys/fs/cgroup/cgroup.subtree_control
```

Optionally, configure the cpuset:
```console
# echo '0-15' | tee /sys/fs/cgroup/faascell/cpuset.cpus
```
Optionally, also restrict memory allocations to a specific NUMA node:
```console
# echo '0' | tee /sys/fs/cgroup/faascell/cpuset.mems
```

Optionally, set a hard cap for memory:
```console
# echo '30G' | tee /sys/fs/cgroup/faascell/memory.max
```
Optionally, limit swap (`0` disables it):
```console
# echo 0 | tee /sys/fs/cgroup/faascell/memory.swap.max
```
Optionally, deprioritize orchestrating processes for OOM killings:
```console
# echo -990 >"/proc/$(pidof firecracker-containerd)/oom_score_adj"
# echo -990 >"/proc/$(pidof faascell)/oom_score_adj"
```

Add processes to the cgroup:
```console
# echo "$(pidof firecracker-containerd)" | tee /sys/fs/cgroup/faascell/cgroup.procs
# echo "$(pidof faascell)" | tee /sys/fs/cgroup/faascell/cgroup.procs
```

Finally, monitor CPU and memory usage with `quick-cginfo`, as demonstrated in
the example at the top of this document.

## Tear down cgroup

First, make sure there are no running processes in the cgroup.
Either kill them or move them to the parent cgroup if this is the case.

```console
# rmdir -v /sys/fs/cgroup/faascell
```

On `EBUSY`, check who might be still using it:
```console
# lsof +D /sys/fs/cgroup/faascell
```
```console
# fuser -v /sys/fs/cgroup/faascell
```
