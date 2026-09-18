# Network & `udevd` Configuration

## TAP devices

Deploy on:
* All `systemd` hosts:
  - [`/etc/systemd/network/10-snaplace.link`](systemd/network/10-snaplace.link)
* `ifupdown`-based hosts:
  - [`/etc/udev/rules.d/80-ifupdown.rules`](udev/rules.d/80-ifupdown.rules)
  - maybe also [`zy-snaplace-uvm-no-run.rules`](udev/rules.d/zy-snaplace-uvm-no-run.rules)
    if `systemd-sysctl` churn exists
* `systemd-networkd`-based hosts:
  - [`/etc/systemd/network/01-snaplace-uvm-unmanaged.network`](systemd/network/01-snaplace-uvm-unmanaged.network)
  - [`/etc/udev/rules.d/zz-snaplace-uvm-no-run.rules`](udev/rules.d/zy-snaplace-uvm-no-run.rules)
